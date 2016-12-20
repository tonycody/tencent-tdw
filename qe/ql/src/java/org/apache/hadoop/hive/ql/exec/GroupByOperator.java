/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec;

import java.util.Arrays;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.IllegalAccessException;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.aggregationDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.groupByDesc;
import org.apache.hadoop.hive.ql.plan.groupByDesc.Mode;
import org.apache.hadoop.hive.serde2.lazy.LazyUnion;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.Text;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCardinalityEstimation;

public class GroupByOperator extends Operator<groupByDesc> implements
    Serializable {

  static final private Log LOG = LogFactory.getLog(GroupByOperator.class
      .getName());

  private static final long serialVersionUID = 1L;
  private static final int NUMROWSESTIMATESIZE = 1000;

  transient protected ExprNodeEvaluator[] keyFields;
  transient protected ObjectInspector[] keyObjectInspectors;
  transient protected Object[] keyObjects;

  transient protected ExprNodeEvaluator[][] aggregationParameterFields;
  transient protected ObjectInspector[][] aggregationParameterObjectInspectors;
  transient protected ObjectInspector[][] aggregationParameterStandardObjectInspectors;
  transient protected Object[][] aggregationParameterObjects;
  transient protected boolean[] aggregationIsDistinct;
  transient protected Map<Integer, Set<Integer>> distinctKeyAggrs = new HashMap<Integer, Set<Integer>>();
  transient protected Map<Integer, Set<Integer>> nonDistinctKeyAggrs = new HashMap<Integer, Set<Integer>>();
  transient protected List<Integer> nonDistinctAggrs = new ArrayList<Integer>();
  transient ExprNodeEvaluator unionExprEval = null;

  transient GenericUDAFEvaluator[] aggregationEvaluators;

  transient protected ArrayList<ObjectInspector> objectInspectors;
  transient ArrayList<String> fieldNames;

  transient protected ArrayList<Object> currentKeys;
  transient protected ArrayList<Object> newKeys;
  transient protected AggregationBuffer[] aggregations;
  transient protected Object[][] aggregationsParametersLastInvoke;

  transient protected HashMap<ArrayList<Object>, AggregationBuffer[]> hashAggregations;

  transient protected HashSet<ArrayList<Object>> keysCurrentGroup;

  transient boolean firstRow;
  transient long totalMemory;
  transient boolean hashAggr;
  transient boolean groupKeyIsNotReduceKey;
  transient boolean firstRowInGroup;
  transient long numRowsInput;
  transient long numRowsHashTbl;
  transient int groupbyMapAggrInterval;
  transient long numRowsCompareHashAggr;
  transient float minReductionHashAggr;
  
  transient boolean containEstDistinct = false;
  //static long totalEstDistBlockNum = 0;

  transient protected ObjectInspector[] currentKeyObjectInspectors;
  transient StructObjectInspector newKeyObjectInspector;
  transient StructObjectInspector currentKeyObjectInspector;

  class varLenFields {
    int aggrPos;
    List<Field> fields;

    varLenFields(int aggrPos, List<Field> fields) {
      this.aggrPos = aggrPos;
      this.fields = fields;
    }

    int getAggrPos() {
      return aggrPos;
    }

    List<Field> getFields() {
      return fields;
    }
  };

  transient List<Integer> keyPositionsSize;
  transient List<Integer> aggrPositionsSize;

  transient List<varLenFields> aggrPositions;

  transient int fixedRowSize;
  transient long maxHashTblMemory;
  transient int totalVariableSize;
  transient int numEntriesVarSize;
  transient int numEntriesHashTable;

  protected void initializeOp(Configuration hconf) throws HiveException {

    if (conf.isUsenewgroupby()) {
      initializeOpNewGroupBy(hconf);
      return;
    }

    totalMemory = Runtime.getRuntime().totalMemory();
    numRowsInput = 0;
    numRowsHashTbl = 0;

    assert (inputObjInspectors.length == 1);
    ObjectInspector rowInspector = inputObjInspectors[0];

    keyFields = new ExprNodeEvaluator[conf.getKeys().size()];
    keyObjectInspectors = new ObjectInspector[conf.getKeys().size()];
    currentKeyObjectInspectors = new ObjectInspector[conf.getKeys().size()];
    keyObjects = new Object[conf.getKeys().size()];
    for (int i = 0; i < keyFields.length; i++) {
      keyFields[i] = ExprNodeEvaluatorFactory.get(conf.getKeys().get(i));
      keyObjectInspectors[i] = keyFields[i].initialize(rowInspector);
      currentKeyObjectInspectors[i] = ObjectInspectorUtils
          .getStandardObjectInspector(keyObjectInspectors[i],
              ObjectInspectorCopyOption.WRITABLE);
      keyObjects[i] = null;
    }
    newKeys = new ArrayList<Object>(keyFields.length);
    List<? extends StructField> sfs = ((StandardStructObjectInspector) rowInspector)
        .getAllStructFieldRefs();

    if (sfs.size() > 0) {
      StructField keyField = sfs.get(0);
      if (keyField.getFieldName().toUpperCase()
          .equals(Utilities.ReduceField.KEY.name())) {
        ObjectInspector keyObjInspector = keyField.getFieldObjectInspector();
        if (keyObjInspector instanceof StandardStructObjectInspector) {
          List<? extends StructField> keysfs = ((StandardStructObjectInspector) keyObjInspector)
              .getAllStructFieldRefs();
          if (keysfs.size() > 0) {
            StructField sf = keysfs.get(keysfs.size() - 1);
            if (sf.getFieldObjectInspector().getCategory()
                .equals(ObjectInspector.Category.UNION)) {
              unionExprEval = ExprNodeEvaluatorFactory
                  .get(new exprNodeColumnDesc(TypeInfoUtils
                      .getTypeInfoFromObjectInspector(sf
                          .getFieldObjectInspector()), keyField.getFieldName()
                      + "." + sf.getFieldName(), null, false));
              unionExprEval.initialize(rowInspector);
            }
          }
        }
      }
    }

    ArrayList<aggregationDesc> aggrs = conf.getAggregators();
    aggregationParameterFields = new ExprNodeEvaluator[aggrs.size()][];
    aggregationParameterObjectInspectors = new ObjectInspector[aggrs.size()][];
    aggregationParameterStandardObjectInspectors = new ObjectInspector[aggrs
        .size()][];
    aggregationParameterObjects = new Object[aggrs.size()][];
    aggregationIsDistinct = new boolean[aggrs.size()];
    for (int i = 0; i < aggrs.size(); i++) {
      aggregationDesc aggr = aggrs.get(i);
      ArrayList<exprNodeDesc> parameters = aggr.getParameters();
      aggregationParameterFields[i] = new ExprNodeEvaluator[parameters.size()];
      aggregationParameterObjectInspectors[i] = new ObjectInspector[parameters
          .size()];
      aggregationParameterStandardObjectInspectors[i] = new ObjectInspector[parameters
          .size()];
      aggregationParameterObjects[i] = new Object[parameters.size()];
      for (int j = 0; j < parameters.size(); j++) {
        aggregationParameterFields[i][j] = ExprNodeEvaluatorFactory
            .get(parameters.get(j));
        aggregationParameterObjectInspectors[i][j] = aggregationParameterFields[i][j]
            .initialize(rowInspector);
        if (unionExprEval != null) {
          String[] names = parameters.get(j).getExprString().split("\\.");
          if (Utilities.ReduceField.KEY.name().equals(names[0])) {
            if (names.length >= 3) {
              String name = names[names.length - 2];
              int tag = Integer.parseInt(name.split("\\:")[1]);
              if (aggr.getDistinct()) {
                Set<Integer> set = distinctKeyAggrs.get(tag);
                if (null == set) {
                  set = new HashSet<Integer>();
                  distinctKeyAggrs.put(tag, set);
                }
                if (!set.contains(i)) {
                  set.add(i);
                }
              } else {
                Set<Integer> set = nonDistinctKeyAggrs.get(tag);
                if (null == set) {
                  set = new HashSet<Integer>();
                  nonDistinctKeyAggrs.put(tag, set);
                }
                if (!set.contains(i)) {
                  set.add(i);
                }
              }
            }
          } else {
            if (!nonDistinctAggrs.contains(i)) {
              nonDistinctAggrs.add(i);
            }
          }
        } else {
          if (aggr.getDistinct()) {
            aggregationIsDistinct[i] = true;
          }
        }

        aggregationParameterStandardObjectInspectors[i][j] = ObjectInspectorUtils
            .getStandardObjectInspector(
                aggregationParameterObjectInspectors[i][j],
                ObjectInspectorCopyOption.WRITABLE);
        aggregationParameterObjects[i][j] = null;
      }
      if (parameters.size() == 0) {
        if (!nonDistinctAggrs.contains(i)) {
          nonDistinctAggrs.add(i);
        }
      }

    }

    aggregationEvaluators = new GenericUDAFEvaluator[conf.getAggregators()
        .size()];
    for (int i = 0; i < aggregationEvaluators.length; i++) {
      aggregationDesc agg = conf.getAggregators().get(i);
      aggregationEvaluators[i] = agg.createGenericUDAFEvaluator();
    }

    int totalFields = keyFields.length + aggregationEvaluators.length;
    objectInspectors = new ArrayList<ObjectInspector>(totalFields);
    for (int i = 0; i < keyFields.length; i++) {
      objectInspectors.add(null);
    }
    for (int i = 0; i < aggregationEvaluators.length; i++) {
      ObjectInspector roi = aggregationEvaluators[i].init(conf.getAggregators()
          .get(i).getMode(), aggregationParameterObjectInspectors[i]);
      objectInspectors.add(roi);
    }

    aggregationsParametersLastInvoke = new Object[conf.getAggregators().size()][];
    if (conf.getMode() != groupByDesc.Mode.HASH) {
      aggregations = newAggregations();
      hashAggr = false;
    } else {
      hashAggregations = new HashMap<ArrayList<Object>, AggregationBuffer[]>();
      aggregations = newAggregations();
      hashAggr = true;
      keyPositionsSize = new ArrayList<Integer>();
      aggrPositions = new ArrayList<varLenFields>();
      groupbyMapAggrInterval = HiveConf.getIntVar(hconf,
          HiveConf.ConfVars.HIVEGROUPBYMAPINTERVAL);

      numRowsCompareHashAggr = groupbyMapAggrInterval;
      minReductionHashAggr = HiveConf.getFloatVar(hconf,
          HiveConf.ConfVars.HIVEMAPAGGRHASHMINREDUCTION);
      groupKeyIsNotReduceKey = conf.getGroupKeyNotReductionKey();
      if (groupKeyIsNotReduceKey)
        keysCurrentGroup = new HashSet<ArrayList<Object>>();
    }

    fieldNames = conf.getOutputColumnNames();

    for (int i = 0; i < keyFields.length; i++) {
      objectInspectors.set(i, currentKeyObjectInspectors[i]);
    }

    ArrayList<String> keyNames = new ArrayList<String>(keyFields.length);
    for (int i = 0; i < keyFields.length; i++) {
      keyNames.add(fieldNames.get(i));
    }
    newKeyObjectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(keyNames,
            Arrays.asList(keyObjectInspectors));
    currentKeyObjectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(keyNames,
            Arrays.asList(currentKeyObjectInspectors));

    outputObjInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(fieldNames, objectInspectors);

    firstRow = true;
    if (conf.getMode() == groupByDesc.Mode.HASH)
      computeMaxEntriesHashAggr(hconf);
    initializeChildren(hconf);
  }

  private void computeMaxEntriesHashAggr(Configuration hconf)
      throws HiveException {
    maxHashTblMemory = (long) (HiveConf.getFloatVar(hconf,
        HiveConf.ConfVars.HIVEMAPAGGRHASHMEMORY) * Runtime.getRuntime()
        .maxMemory());
    if (maxHashTblMemory > 256l * 1024 * 1024) {
      maxHashTblMemory = 256l * 1024 * 1024;
    }
    LOG.info("maxHashTblMemory:\t" + maxHashTblMemory);
    estimateRowSize();
  }

  private static final int javaObjectOverHead = 64;
  private static final int javaHashEntryOverHead = 64;
  private static final int javaSizePrimitiveType = 16;
  private static final int javaSizeUnknownType = 256;

  private int getSize(int pos, PrimitiveCategory category) {
    switch (category) {
    case VOID:
    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case FLOAT:
    case DOUBLE: {
      return javaSizePrimitiveType;
    }
    case STRING: {
      keyPositionsSize.add(new Integer(pos));
      return javaObjectOverHead;
    }
    default: {
      return javaSizeUnknownType;
    }
    }
  }

  private int getSize(int pos, Class<?> c, Field f) {
    if (c.isPrimitive() || c.isInstance(new Boolean(true))
        || c.isInstance(new Byte((byte) 0))
        || c.isInstance(new Short((short) 0)) || c.isInstance(new Integer(0))
        || c.isInstance(new Long(0)) || c.isInstance(new Float(0))
        || c.isInstance(new Double(0)))
      return javaSizePrimitiveType;

    if (c.isInstance(new String())) {
      int idx = 0;
      varLenFields v = null;
      for (idx = 0; idx < aggrPositions.size(); idx++) {
        v = aggrPositions.get(idx);
        if (v.getAggrPos() == pos)
          break;
      }

      if (idx == aggrPositions.size()) {
        v = new varLenFields(pos, new ArrayList<Field>());
        aggrPositions.add(v);
      }

      v.getFields().add(f);
      return javaObjectOverHead;
    }

    return javaSizeUnknownType;
  }

  private int getSize(int pos, TypeInfo typeInfo) {
    if (typeInfo instanceof PrimitiveTypeInfo)
      return getSize(pos, ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory());
    return javaSizeUnknownType;
  }

  private void estimateRowSize() throws HiveException {
    fixedRowSize = javaHashEntryOverHead;

    ArrayList<exprNodeDesc> keys = conf.getKeys();

    for (int pos = 0; pos < keys.size(); pos++)
      fixedRowSize += getSize(pos, keys.get(pos).getTypeInfo());

    for (int i = 0; i < aggregationEvaluators.length; i++) {

      fixedRowSize += javaObjectOverHead;
      Class<? extends AggregationBuffer> agg = aggregationEvaluators[i]
          .getNewAggregationBuffer().getClass();
      Field[] fArr = ObjectInspectorUtils.getDeclaredNonStaticFields(agg);
      for (Field f : fArr) {
        fixedRowSize += getSize(i, f.getType(), f);
      }
    }
    LOG.info("fixedRowSize:\t" + fixedRowSize);
  }

  protected AggregationBuffer[] newAggregations() throws HiveException {
    AggregationBuffer[] aggs = new AggregationBuffer[aggregationEvaluators.length];
    for (int i = 0; i < aggregationEvaluators.length; i++) {
      aggs[i] = aggregationEvaluators[i].getNewAggregationBuffer();
    }
    return aggs;
  }

  protected void resetAggregations(AggregationBuffer[] aggs)
      throws HiveException {
    for (int i = 0; i < aggs.length; i++) {
      aggregationEvaluators[i].reset(aggs[i]);
    }
  }

  protected void updateAggregations1(AggregationBuffer[] aggs, Object row,
      ObjectInspector rowInspector, boolean hashAggr,
      boolean newEntryForHashAggr, Object[][] lastInvoke) throws HiveException {

    for (int ai = 0; ai < aggs.length; ai++) {

      Object[] o = new Object[aggregationParameterFields[ai].length];
      for (int pi = 0; pi < aggregationParameterFields[ai].length; pi++) {
        o[pi] = aggregationParameterFields[ai][pi].evaluate(row);
      }

      if (aggregationIsDistinct[ai]) {
        if (hashAggr) {
          if (newEntryForHashAggr) {
            aggregationEvaluators[ai].aggregate(aggs[ai], o);
          }
        } else {
          if (lastInvoke[ai] == null) {
            lastInvoke[ai] = new Object[o.length];
          }
          if (ObjectInspectorUtils.compare(o,
              aggregationParameterObjectInspectors[ai], lastInvoke[ai],
              aggregationParameterStandardObjectInspectors[ai]) != 0) {
            aggregationEvaluators[ai].aggregate(aggs[ai], o);
            for (int pi = 0; pi < o.length; pi++) {
              lastInvoke[ai][pi] = ObjectInspectorUtils.copyToStandardObject(
                  o[pi], aggregationParameterObjectInspectors[ai][pi],
                  ObjectInspectorCopyOption.WRITABLE);
            }
          }
        }
      } else {
        aggregationEvaluators[ai].aggregate(aggs[ai], o);
      }
    }
  }

  protected void updateAggregations(AggregationBuffer[] aggs, Object row,
      ObjectInspector rowInspector, boolean hashAggr,
      boolean newEntryForHashAggr, Object[][] lastInvoke) throws HiveException {
    if (unionExprEval == null) {
      for (int ai = 0; ai < aggs.length; ai++) {
        Object[] o = new Object[aggregationParameterFields[ai].length];
        for (int pi = 0; pi < aggregationParameterFields[ai].length; pi++) {
          o[pi] = aggregationParameterFields[ai][pi].evaluate(row);
        }
        if (aggregationIsDistinct[ai]) {
          if (hashAggr) {
            if (newEntryForHashAggr) {
              aggregationEvaluators[ai].aggregate(aggs[ai], o);
            }
          } else {
            if (lastInvoke[ai] == null) {
              lastInvoke[ai] = new Object[o.length];
            }
            if (ObjectInspectorUtils.compare(o,
                aggregationParameterObjectInspectors[ai], lastInvoke[ai],
                aggregationParameterStandardObjectInspectors[ai]) != 0) {
              aggregationEvaluators[ai].aggregate(aggs[ai], o);
              for (int pi = 0; pi < o.length; pi++) {
                lastInvoke[ai][pi] = ObjectInspectorUtils.copyToStandardObject(
                    o[pi], aggregationParameterObjectInspectors[ai][pi],
                    ObjectInspectorCopyOption.WRITABLE);
              }
            }
          }
        } else {
          aggregationEvaluators[ai].aggregate(aggs[ai], o);
        }
      }
      return;
    }

    if (distinctKeyAggrs.size() > 0) {
      UnionObject uo = (UnionObject) (unionExprEval.evaluate(row));
      int unionTag = uo.getTag();
      if (nonDistinctKeyAggrs.get(unionTag) != null) {
        for (int pos : nonDistinctKeyAggrs.get(unionTag)) {
          Object[] o = new Object[aggregationParameterFields[pos].length];
          for (int pi = 0; pi < aggregationParameterFields[pos].length; pi++) {
            o[pi] = aggregationParameterFields[pos][pi].evaluate(row);
          }
          aggregationEvaluators[pos].aggregate(aggs[pos], o);
        }
      }
      if (distinctKeyAggrs.get(unionTag) != null) {
        for (int i : distinctKeyAggrs.get(unionTag)) {
          Object[] o = new Object[aggregationParameterFields[i].length];
          for (int pi = 0; pi < aggregationParameterFields[i].length; pi++) {
            o[pi] = aggregationParameterFields[i][pi].evaluate(row);
          }
          if (hashAggr) {
            if (newEntryForHashAggr) {
              aggregationEvaluators[i].aggregate(aggs[i], o);
            }
          } else {
            if (lastInvoke[i] == null) {
              lastInvoke[i] = new Object[o.length];
            }
            if (ObjectInspectorUtils.compare(o,
                aggregationParameterObjectInspectors[i], lastInvoke[i],
                aggregationParameterStandardObjectInspectors[i]) != 0) {
              aggregationEvaluators[i].aggregate(aggs[i], o);
              for (int pi = 0; pi < o.length; pi++) {
                lastInvoke[i][pi] = ObjectInspectorUtils.copyToStandardObject(
                    o[pi], aggregationParameterObjectInspectors[i][pi],
                    ObjectInspectorCopyOption.WRITABLE);
              }
            }
          }
        }
      }

      if (unionTag == 0) {
        for (int pos : nonDistinctAggrs) {
          Object[] o = new Object[aggregationParameterFields[pos].length];
          for (int pi = 0; pi < aggregationParameterFields[pos].length; pi++) {
            o[pi] = aggregationParameterFields[pos][pi].evaluate(row);
          }
          aggregationEvaluators[pos].aggregate(aggs[pos], o);
        }
      }
    } else {
      for (int ai = 0; ai < aggs.length; ai++) {
        Object[] o = new Object[aggregationParameterFields[ai].length];
        for (int pi = 0; pi < aggregationParameterFields[ai].length; pi++) {
          o[pi] = aggregationParameterFields[ai][pi].evaluate(row);
        }
        aggregationEvaluators[ai].aggregate(aggs[ai], o);
      }
    }
  }

  public void startGroup() throws HiveException {
    firstRowInGroup = true;
  }

  public void endGroup() throws HiveException {
    if (groupKeyIsNotReduceKey)
      keysCurrentGroup.clear();
  }

  public void process(Object row, int tag) throws HiveException {

    if (usenewgroupby) {
      processNewGroupBy(row, tag);
      return;
    }

    firstRow = false;
    ObjectInspector rowInspector = inputObjInspectors[tag];
    if (hashAggr && !groupKeyIsNotReduceKey) {
      numRowsInput++;
      if (numRowsInput == numRowsCompareHashAggr) {
        numRowsCompareHashAggr += groupbyMapAggrInterval;
        if (numRowsHashTbl > numRowsInput * minReductionHashAggr) {
          LOG.warn("Disable Hash Aggr: #hash table = " + numRowsHashTbl
              + " #total = " + numRowsInput + " reduction = " + 1.0
              * (numRowsHashTbl / numRowsInput) + " minReduction = "
              + minReductionHashAggr);
          flush(true);
          hashAggr = false;
        } else {
          LOG.trace("Hash Aggr Enabled: #hash table = " + numRowsHashTbl
              + " #total = " + numRowsInput + " reduction = " + 1.0
              * (numRowsHashTbl / numRowsInput) + " minReduction = "
              + minReductionHashAggr);
        }
      }
    }

    try {
      newKeys.clear();
      for (int i = 0; i < keyFields.length; i++) {
        if (keyObjectInspectors[i] == null) {
          keyObjectInspectors[i] = keyFields[i].initialize(rowInspector);
        }
        keyObjects[i] = keyFields[i].evaluate(row);
        newKeys.add(keyObjects[i]);
      }

      if (hashAggr)
        processHashAggr(row, rowInspector, newKeys);
      else
        processAggr(row, rowInspector, newKeys);
      firstRowInGroup = false;
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  private static ArrayList<Object> deepCopyElements(Object[] keys,
      ObjectInspector[] keyObjectInspectors,
      ObjectInspectorCopyOption copyOption) {
    ArrayList<Object> result = new ArrayList<Object>(keys.length);
    deepCopyElements(keys, keyObjectInspectors, result, copyOption);
    return result;
  }

  private static void deepCopyElements(Object[] keys,
      ObjectInspector[] keyObjectInspectors, ArrayList<Object> result,
      ObjectInspectorCopyOption copyOption) {
    result.clear();
    for (int i = 0; i < keys.length; i++) {
      result.add(ObjectInspectorUtils.copyToStandardObject(keys[i],
          keyObjectInspectors[i], copyOption));
    }
  }

  private void processHashAggr(Object row, ObjectInspector rowInspector,
      ArrayList<Object> newKeys) throws HiveException {
    AggregationBuffer[] aggs = null;
    boolean newEntryForHashAggr = false;

    ArrayList<Object> newDefaultKeys = deepCopyElements(keyObjects,
        keyObjectInspectors, ObjectInspectorCopyOption.WRITABLE);
    aggs = hashAggregations.get(newDefaultKeys);
    if (aggs == null) {
      aggs = newAggregations();
      hashAggregations.put(newDefaultKeys, aggs);
      newEntryForHashAggr = true;
      numRowsHashTbl++;
    }

    if (groupKeyIsNotReduceKey) {
      newEntryForHashAggr = keysCurrentGroup.add(newDefaultKeys);
    }

    updateAggregations(aggs, row, rowInspector, true, newEntryForHashAggr, null);

    if ((!groupKeyIsNotReduceKey || firstRowInGroup)
        && shouldBeFlushed(newDefaultKeys)) {
      flush(false);
    }
  }

  private void processAggr(Object row, ObjectInspector rowInspector,
      ArrayList<Object> newKeys) throws HiveException {
    AggregationBuffer[] aggs = null;
    Object[][] lastInvoke = null;
    boolean keysAreEqual = ObjectInspectorUtils.compare(newKeys,
        newKeyObjectInspector, currentKeys, currentKeyObjectInspector) == 0;

    if (currentKeys != null && !keysAreEqual)
      forward(currentKeys, aggregations);

    if (currentKeys == null || !keysAreEqual) {
      if (currentKeys == null) {
        currentKeys = new ArrayList<Object>(keyFields.length);
      }
      deepCopyElements(keyObjects, keyObjectInspectors, currentKeys,
          ObjectInspectorCopyOption.WRITABLE);

      resetAggregations(aggregations);

      for (int i = 0; i < aggregationsParametersLastInvoke.length; i++)
        aggregationsParametersLastInvoke[i] = null;
    }

    aggs = aggregations;

    lastInvoke = aggregationsParametersLastInvoke;

    updateAggregations(aggs, row, rowInspector, false, false, lastInvoke);
  }

  private boolean shouldBeFlushed(ArrayList<Object> newKeys) {
    int numEntries = hashAggregations.size();

    if ((numEntriesHashTable == 0) || ((numEntries % NUMROWSESTIMATESIZE) == 0)) {
      for (Integer pos : keyPositionsSize) {
        Object key = newKeys.get(pos.intValue());
        if (key != null) {
          if (key instanceof String) {
            totalVariableSize += ((String) key).length();
          } else if (key instanceof Text) {
            totalVariableSize += ((Text) key).getLength();
          }
        }
      }

      AggregationBuffer[] aggs = null;
      if (aggrPositions.size() > 0)
        aggs = hashAggregations.get(newKeys);

      for (varLenFields v : aggrPositions) {
        int aggrPos = v.getAggrPos();
        List<Field> fieldsVarLen = v.getFields();
        AggregationBuffer agg = aggs[aggrPos];

        try {
          for (Field f : fieldsVarLen)
            totalVariableSize += ((String) f.get(agg)).length();
        } catch (IllegalAccessException e) {
          assert false;
        }
      }

      numEntriesVarSize++;

      numEntriesHashTable = (int) (maxHashTblMemory / (fixedRowSize + ((int) totalVariableSize / numEntriesVarSize)));
      LOG.trace("Hash Aggr: #hash table = " + numEntries
          + " #max in hash table = " + numEntriesHashTable);
    }

    if (numEntries >= numEntriesHashTable)
      return true;
    return false;
  }

  private void flush(boolean complete) throws HiveException {

    if (complete) {
      Iterator<Map.Entry<ArrayList<Object>, AggregationBuffer[]>> iter = hashAggregations
          .entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<ArrayList<Object>, AggregationBuffer[]> m = iter.next();
        forward(m.getKey(), m.getValue());
      }
      hashAggregations.clear();
      hashAggregations = null;
      LOG.warn("Hash Table completed flushed");
      return;
    }

    int oldSize = hashAggregations.size();
    LOG.warn("Hash Tbl flush: #hash table = " + oldSize);
    Iterator<Map.Entry<ArrayList<Object>, AggregationBuffer[]>> iter = hashAggregations
        .entrySet().iterator();
    int numDel = 0;
    while (iter.hasNext()) {
      Map.Entry<ArrayList<Object>, AggregationBuffer[]> m = iter.next();
      forward(m.getKey(), m.getValue());
      iter.remove();
      numDel++;
      if (numDel * 10 >= oldSize) {
        LOG.warn("Hash Table flushed: new size = " + hashAggregations.size());
        return;
      }
    }
  }

  transient Object[] forwardCache;

  protected void forward(ArrayList<Object> keys, AggregationBuffer[] aggs)
      throws HiveException {
    int totalFields = keys.size() + aggs.length;
    if (forwardCache == null) {
      forwardCache = new Object[totalFields];
    }
    for (int i = 0; i < keys.size(); i++) {
      forwardCache[i] = keys.get(i);
    }
    for (int i = 0; i < aggs.length; i++) {
      forwardCache[keys.size() + i] = aggregationEvaluators[i]
          .evaluate(aggs[i]);
    }
    forward(forwardCache, outputObjInspector);
  }

  public void closeOp(boolean abort) throws HiveException {
    if (this.usenewgroupby) {
      closeOpNewGroupBy(abort);
      return;
    }
    if (!abort) {
      try {
        if (firstRow && (keyFields.length == 0)) {
          firstRow = false;

          for (int ai = 0; ai < aggregations.length; ai++) {

            Object[] o;
            if (aggregationParameterFields[ai].length > 0) {
              o = new Object[aggregationParameterFields[ai].length];
            } else {
              o = null;
            }

            for (int pi = 0; pi < aggregationParameterFields[ai].length; pi++) {
              o[pi] = null;
            }
            aggregationEvaluators[ai].aggregate(aggregations[ai], o);
          }

          forward(new ArrayList<Object>(0), aggregations);
        } else {
          if (hashAggregations != null) {
            LOG.warn("Begin Hash Table flush at close: size = "
                + hashAggregations.size());
            Iterator<Map.Entry<ArrayList<Object>, AggregationBuffer[]>> iter = hashAggregations
                .entrySet().iterator();
            while (iter.hasNext()) {
              Map.Entry<ArrayList<Object>, AggregationBuffer[]> m = (Map.Entry<ArrayList<Object>, AggregationBuffer[]>) iter
                  .next();
              forward(m.getKey(), m.getValue());
              iter.remove();
            }
            hashAggregations.clear();
          } else if (aggregations != null) {
            if (currentKeys != null) {
              forward(currentKeys, aggregations);
            }
            currentKeys = null;
          } else {
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        throw new HiveException(e);
      }
    }
  }

  public List<String> genColLists(
      HashMap<Operator<? extends Serializable>, OpParseContext> opParseCtx) {
    List<String> colLists = new ArrayList<String>();
    ArrayList<exprNodeDesc> keys = conf.getKeys();
    for (exprNodeDesc key : keys)
      colLists = Utilities.mergeUniqElems(colLists, key.getCols());

    ArrayList<aggregationDesc> aggrs = conf.getAggregators();
    for (aggregationDesc aggr : aggrs) {
      ArrayList<exprNodeDesc> params = aggr.getParameters();
      for (exprNodeDesc param : params)
        colLists = Utilities.mergeUniqElems(colLists, param.getCols());
    }

    return colLists;
  }

  @Override
  public String getName() {
    return new String("GBY");
  }

  static class TagedAggregationBuffer {
    int tag;
    AggregationBuffer[] aggs;

    public TagedAggregationBuffer(int tag, AggregationBuffer[] aggs) {
      this.tag = tag;
      this.aggs = aggs;
    }
  }

  transient protected boolean usenewgroupby = false;
  transient protected ArrayList<ArrayList<Integer>> tag2AggrPos;
  transient protected boolean containsfunctions = false;
  transient protected boolean hashmode = true;
  transient protected boolean finalmode = true;
  transient protected boolean mergemode = true;
  transient protected boolean lastgby = false;

  transient protected ArrayList<String> outputColumnNames = null;
  transient protected int gbykeylength = 0;
  transient protected ExprNodeEvaluator[] keyFieldsNew;
  transient protected Object[] keyObjectsNew;

  transient protected HashMap<ArrayList<Object>, AggregationBuffer[]> newHashAggregationsForNondist = null;
  transient protected ArrayList<HashSet<ArrayList<Object>>> newHashAggregationsForDist = null;
  transient protected TagedAggregationBuffer[] newAggregations = null;

  transient protected StructObjectInspector currKeyCompareObjectInspector;
  transient protected StructObjectInspector newKeyCompareObjectInspector;

  transient protected ExprNodeEvaluator[][] aggrParamFieldsHash;
  transient protected ObjectInspector[][] aggrParamObjectInspectorsHash;
  transient protected ObjectInspector[][] aggrParamObjectInspectorsHashStandard;
  transient protected ObjectInspector[] aggrParamObjectInspectorUOHash;
  transient protected Object[][] cachedAggrParamHash;

  transient protected Object[][] cachedAggrParamObjects;

  transient ExprNodeEvaluator unionExprEvalAggr = null;
  transient protected ObjectInspector aggrParamObjectInspector;
  transient protected ObjectInspector aggrParamObjectInspectorStandard;

  transient protected ObjectInspector[] aggregationParameterObjectInspectorsUnion;
  transient protected ObjectInspector[] aggrParamObjectInspectorUOHashStandard;

  transient protected ExprNodeEvaluator[][] aggregationParameterFieldsNew;
  transient protected ObjectInspector[][] aggregationParameterObjectInspectorsNew;
  transient protected GenericUDAFEvaluator[] aggregationEvaluatorsNew;

  transient protected ArrayList<Object> newDefaultKeys = null;
  transient protected ArrayList<Object> newDefaultKeys1 = null;
  transient protected ArrayList<Object> groupbyKeysHash = null;

  transient protected int hashFlushMinSize = 0;

  private void initializeOpNewGroupBy(Configuration hconf) throws HiveException {
    usenewgroupby = true;
    totalMemory = Runtime.getRuntime().totalMemory();
    numRowsInput = 0;
    numRowsHashTbl = 0;

    outputColumnNames = conf.getOutputColumnNames();
    assert (inputObjInspectors.length == 1);
    ObjectInspector rowInspector = inputObjInspectors[0];
    System.out.println("gbykey:ioi:"
        + ObjectInspectorUtils.getStandardObjectInspector(rowInspector,
            ObjectInspectorCopyOption.WRITABLE));

    Mode mode = conf.getMode();
    LOG.info("use new group by gbyop: mode :\t" + mode);

    hashFlushMinSize = hconf.getInt("hashFlushMinSize", 1000);

    hashmode = mode == Mode.HASH;
    finalmode = mode == Mode.FINAL;
    mergemode = !hashmode && !finalmode;
    lastgby = mode == Mode.COMPLETE || mode == Mode.FINAL
        || mode == Mode.MERGEPARTIAL;
    tag2AggrPos = conf.getTag2AggrPos();

    containsfunctions = tag2AggrPos != null && tag2AggrPos.size() > 0;
    boolean containsdistinctfunctions = containsfunctions
        && tag2AggrPos.size() > 1;

    gbykeylength = conf.getKeys().size();
    keyFieldsNew = new ExprNodeEvaluator[conf.getKeys().size()];
    keyObjectInspectors = new ObjectInspector[conf.getKeys().size()];
    keyObjectsNew = new Object[conf.getKeys().size()];

    ObjectInspector[] currKeyObjectInspectors = new ObjectInspector[conf
        .getKeys().size()];
    for (int i = 0; i < keyFieldsNew.length; i++) {
      keyFieldsNew[i] = ExprNodeEvaluatorFactory.get(conf.getKeys().get(i));
      keyObjectInspectors[i] = keyFieldsNew[i].initialize(rowInspector);
      keyObjectsNew[i] = null;
      currKeyObjectInspectors[i] = ObjectInspectorUtils
          .getStandardObjectInspector(keyObjectInspectors[i],
              ObjectInspectorCopyOption.WRITABLE);
    }

    ArrayList<ArrayList<exprNodeDesc>> tag2AggrParamExpr = conf
        .getTag2AggrParamExpr();
    if (hashmode) {
      aggrParamFieldsHash = new ExprNodeEvaluator[tag2AggrParamExpr.size()][];
      cachedAggrParamHash = new Object[tag2AggrParamExpr.size()][];
      aggrParamObjectInspectorsHash = new ObjectInspector[tag2AggrParamExpr
          .size()][];
      aggrParamObjectInspectorsHashStandard = new ObjectInspector[tag2AggrParamExpr
          .size()][];

      aggrParamObjectInspectorUOHash = new ObjectInspector[tag2AggrParamExpr
          .size()];
      aggrParamObjectInspectorUOHashStandard = new ObjectInspector[tag2AggrParamExpr
          .size()];
      for (int tag = 0; tag < aggrParamFieldsHash.length; tag++) {
        ArrayList<exprNodeDesc> distexprs = tag2AggrParamExpr.get(tag);
        aggrParamFieldsHash[tag] = new ExprNodeEvaluator[distexprs.size()];
        aggrParamObjectInspectorsHash[tag] = new ObjectInspector[distexprs
            .size()];
        aggrParamObjectInspectorsHashStandard[tag] = new ObjectInspector[distexprs
            .size()];

        cachedAggrParamHash[tag] = new Object[distexprs.size()];
        List<String> names = new ArrayList<String>();
        List<ObjectInspector> eois = new ArrayList<ObjectInspector>();
        List<ObjectInspector> eoisStandard = new ArrayList<ObjectInspector>();
        for (int j = 0; j < distexprs.size(); j++) {
          aggrParamFieldsHash[tag][j] = ExprNodeEvaluatorFactory.get(distexprs
              .get(j));
          aggrParamObjectInspectorsHash[tag][j] = aggrParamFieldsHash[tag][j]
              .initialize(rowInspector);
          aggrParamObjectInspectorsHashStandard[tag][j] = ObjectInspectorUtils
              .getStandardObjectInspector(aggrParamObjectInspectorsHash[tag][j]);

          names.add(HiveConf.getColumnInternalName(j));
          eois.add(aggrParamObjectInspectorsHash[tag][j]);
          eoisStandard.add(aggrParamObjectInspectorsHashStandard[tag][j]);
          cachedAggrParamHash[tag][j] = null;
        }
        aggrParamObjectInspectorUOHash[tag] = ObjectInspectorFactory
            .getStandardStructObjectInspector(names, eois);
        aggrParamObjectInspectorUOHashStandard[tag] = ObjectInspectorFactory
            .getStandardStructObjectInspector(names, eoisStandard);
      }
    } else {
      if (containsfunctions) {

        List<? extends StructField> sfs = ((StandardStructObjectInspector) rowInspector)
            .getAllStructFieldRefs();
        if (mergemode) {

          StructField keyField = sfs.get(0);
          ObjectInspector keyObjInspector = keyField.getFieldObjectInspector();
          List<? extends StructField> keysfs = ((StandardStructObjectInspector) keyObjInspector)
              .getAllStructFieldRefs();
          StructField sf = keysfs.get(keysfs.size() - 1);
          unionExprEvalAggr = ExprNodeEvaluatorFactory
              .get(new exprNodeColumnDesc(
                  TypeInfoUtils.getTypeInfoFromObjectInspector(sf
                      .getFieldObjectInspector()), keyField.getFieldName()
                      + "." + sf.getFieldName(), null, false));
          aggrParamObjectInspector = unionExprEvalAggr.initialize(rowInspector);
          aggrParamObjectInspectorStandard = ObjectInspectorUtils
              .getStandardObjectInspector(aggrParamObjectInspector);
          System.out.println("gby:unionExprEvalAggr:"
              + unionExprEvalAggr.toString());
          System.out.println("gby:aggrParamObjectInspector:"
              + aggrParamObjectInspector);

        } else {
          StructField valueField = sfs.get(1);
          ObjectInspector valueObjInspector = valueField
              .getFieldObjectInspector();
          List<? extends StructField> valuesfs = ((StructObjectInspector) valueObjInspector)
              .getAllStructFieldRefs();
          StructField sf = valuesfs.get(valuesfs.size() - 1);
          unionExprEvalAggr = ExprNodeEvaluatorFactory
              .get(new exprNodeColumnDesc(
                  TypeInfoUtils.getTypeInfoFromObjectInspector(sf
                      .getFieldObjectInspector()), valueField.getFieldName()
                      + "." + sf.getFieldName(), null, false));
          aggrParamObjectInspector = unionExprEvalAggr.initialize(rowInspector);
          aggrParamObjectInspectorStandard = ObjectInspectorUtils
              .getStandardObjectInspector(aggrParamObjectInspector);

          System.out.println("gby:unionExprEvalAggr:"
              + unionExprEvalAggr.toString());
          System.out.println("gby:aggrParamObjectInspector:"
              + aggrParamObjectInspector);
        }
      }
    }

    ArrayList<aggregationDesc> aggrs = conf.getAggregators();
    aggregationParameterFieldsNew = new ExprNodeEvaluator[aggrs.size()][];
    cachedAggrParamObjects = new Object[aggrs.size()][];
    aggregationParameterObjectInspectorsNew = new ObjectInspector[aggrs.size()][];
    aggregationEvaluatorsNew = new GenericUDAFEvaluator[conf.getAggregators()
        .size()];
    aggregationParameterObjectInspectorsUnion = new ObjectInspector[conf
        .getAggregators().size()];
    for (int pos = 0; pos < aggrs.size(); pos++) {
      aggregationDesc aggr = aggrs.get(pos);
      if (aggr != null) {
        ArrayList<exprNodeDesc> parameters = aggr.getParameters();
        aggregationEvaluatorsNew[pos] = aggr.createGenericUDAFEvaluator();
        aggregationParameterFieldsNew[pos] = new ExprNodeEvaluator[parameters
            .size()];
        cachedAggrParamObjects[pos] = new Object[parameters.size()];

        aggregationParameterObjectInspectorsNew[pos] = new ObjectInspector[parameters
            .size()];
        for (int j = 0; j < parameters.size(); j++) {
          aggregationParameterFieldsNew[pos][j] = ExprNodeEvaluatorFactory
              .get(parameters.get(j));
          cachedAggrParamObjects[pos][j] = null;
          aggregationParameterObjectInspectorsNew[pos][j] = aggregationParameterFieldsNew[pos][j]
              .initialize(rowInspector);
        }
        aggregationParameterObjectInspectorsUnion[pos] = aggregationEvaluatorsNew[pos]
            .init(conf.getAggregators().get(pos).getMode(),
                aggregationParameterObjectInspectorsNew[pos]);
      } else {
        aggregationEvaluatorsNew[pos] = null;
        aggregationParameterFieldsNew[pos] = null;
        aggregationParameterObjectInspectorsNew[pos] = null;
        cachedAggrParamObjects[pos] = null;
      }
    }

    newAggregations = new TagedAggregationBuffer[tag2AggrPos.size()];
    for (int tag = 0; tag < tag2AggrPos.size(); tag++) {
      if (hashmode && tag != 0) {
        continue;
      }
      AggregationBuffer[] aggs = new AggregationBuffer[tag2AggrPos.get(tag)
          .size()];
      for (int j = 0; j < aggs.length; j++) {
        aggs[j] = aggregationEvaluatorsNew[this.tag2AggrPos.get(tag).get(j)]
            .getNewAggregationBuffer();
      }
      newAggregations[tag] = new TagedAggregationBuffer(tag, aggs);
    }

    objectInspectors = new ArrayList<ObjectInspector>();
    for (int i = 0; i < keyFieldsNew.length; i++) {
      objectInspectors.add(currKeyObjectInspectors[i]);
    }
    if (containsfunctions) {
      if (hashmode) {
        List<ObjectInspector> uois = new ArrayList<ObjectInspector>();

        List<String> names = new ArrayList<String>();
        List<ObjectInspector> eois = new ArrayList<ObjectInspector>();
        for (int i = 0; i < tag2AggrPos.get(0).size(); i++) {
          int pos = tag2AggrPos.get(0).get(i);
          names.add(HiveConf.getColumnInternalName(i));
          eois.add(ObjectInspectorUtils
              .getStandardObjectInspector(aggregationParameterObjectInspectorsUnion[pos]));
        }
        uois.add(ObjectInspectorFactory.getStandardStructObjectInspector(names,
            eois));

        for (int i = 1; i < aggrParamObjectInspectorUOHashStandard.length; i++) {
          uois.add(aggrParamObjectInspectorUOHashStandard[i]);
        }

        objectInspectors.add(ObjectInspectorFactory
            .getStandardUnionObjectInspector(uois));
      } else if (!lastgby) {
        List<ObjectInspector> uois = new ArrayList<ObjectInspector>();
        for (ArrayList<Integer> poses : tag2AggrPos) {
          List<String> names = new ArrayList<String>();
          List<ObjectInspector> eois = new ArrayList<ObjectInspector>();
          int posoff = 0;
          for (Integer pos : poses) {
            names.add(HiveConf.getColumnInternalName(posoff++));
            ObjectInspector roi = aggregationEvaluatorsNew[pos].init(conf
                .getAggregators().get(pos).getMode(),
                aggregationParameterObjectInspectorsNew[pos]);
            eois.add(roi);
          }
          uois.add(ObjectInspectorFactory.getStandardStructObjectInspector(
              names, eois));
        }
        objectInspectors.add(ObjectInspectorFactory
            .getStandardUnionObjectInspector(uois));

      } else {
        for (int pos = 0; pos < aggregationEvaluatorsNew.length; pos++) {
          ObjectInspector roi = aggregationEvaluatorsNew[pos].init(conf
              .getAggregators().get(pos).getMode(),
              aggregationParameterObjectInspectorsNew[pos]);
          objectInspectors.add(roi);
        }

      }
    }

    fieldNames = conf.getOutputColumnNames();
    outputObjInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(fieldNames, objectInspectors);
    System.out.println("gbykey:ooi:" + this.outputObjInspector);

    ArrayList<String> keyNames = new ArrayList<String>(keyFieldsNew.length);
    for (int i = 0; i < keyFieldsNew.length; i++) {
      keyNames.add(fieldNames.get(i));
    }

    ArrayList<ObjectInspector> comprareCurrKeyOIs = new ArrayList<ObjectInspector>();
    comprareCurrKeyOIs.addAll(Arrays.asList(currKeyObjectInspectors));
    currKeyCompareObjectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(keyNames, comprareCurrKeyOIs);
    newKeyCompareObjectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(keyNames,
            Arrays.asList(keyObjectInspectors));

    if (!hashmode) {
      hashAggr = false;
    } else {
      hashAggr = true;
      newHashAggregationsForNondist = new HashMap<ArrayList<Object>, AggregationBuffer[]>();
      newHashAggregationsForDist = new ArrayList<HashSet<ArrayList<Object>>>(
          conf.getTag2AggrParamExpr().size());
      for (int i = 0; i < conf.getTag2AggrParamExpr().size(); i++) {
        newHashAggregationsForDist.add(new HashSet<ArrayList<Object>>());
      }
      keyPositionsSize = new ArrayList<Integer>();
      aggrPositionsSize = new ArrayList<Integer>();
      aggrPositions = new ArrayList<varLenFields>();

      groupbyMapAggrInterval = HiveConf.getIntVar(hconf,
          HiveConf.ConfVars.HIVEGROUPBYMAPINTERVAL);

      numRowsCompareHashAggr = groupbyMapAggrInterval;
      minReductionHashAggr = HiveConf.getFloatVar(hconf,
          HiveConf.ConfVars.HIVEMAPAGGRHASHMINREDUCTION);
      groupKeyIsNotReduceKey = conf.getGroupKeyNotReductionKey();
      if (groupKeyIsNotReduceKey)
        keysCurrentGroup = new HashSet<ArrayList<Object>>();
      computeMaxEntriesHashAggrNewGroupBy(hconf);
    }

    compareKeys = new ArrayList<Object>(gbykeylength);
    currAggrUOs = new ArrayList<Object>(tag2AggrPos.size());
    for (int i = 0; i < tag2AggrPos.size(); i++) {
      currAggrUOs.add(null);
    }

    if (containsdistinctfunctions) {
      newDefaultKeys = new ArrayList<Object>(gbykeylength + 1);
      groupbyKeysHash = new ArrayList<Object>(gbykeylength + 1);
    } else {
      newDefaultKeys = new ArrayList<Object>(gbykeylength);
      groupbyKeysHash = new ArrayList<Object>(gbykeylength);
    }

    forwardCacheArray = new Object[this.outputColumnNames.size()];

    firstRow = true;

    initializeChildren(hconf);
  }

  ArrayList<Object> currKeys = null;

  ArrayList<Object> compareKeys = null;

  ArrayList<Object> currAggrUOs = null;

  StandardUnion compareAggrUO = new StandardUnion();

  private void processNewGroupBy(Object row, int tag) throws HiveException {
    firstRow = false;

    if (hashAggr && !groupKeyIsNotReduceKey) {
      numRowsInput++;
      if (numRowsInput == numRowsCompareHashAggr) {
        numRowsCompareHashAggr += groupbyMapAggrInterval;
        if (numRowsHashTbl > numRowsInput * Math.max(tag2AggrPos.size(), 1)
            * minReductionHashAggr) {
          LOG.warn("Disable Hash Aggr: #hash table = "
              + numRowsHashTbl
              + " #total = "
              + numRowsInput
              + " reduction = "
              + (1.0 * numRowsHashTbl / numRowsInput / Math.max(
                  tag2AggrPos.size(), 1)) + " minReduction = "
              + minReductionHashAggr);
          flushHashNewGroupBy(true);
          hashAggr = false;
        } else {
          LOG.trace("Hash Aggr Enabled: #hash table = "
              + numRowsHashTbl
              + " #total = "
              + numRowsInput
              + " reduction = "
              + (1.0 * numRowsHashTbl / numRowsInput / Math.max(
                  tag2AggrPos.size(), 1)) + " minReduction = "
              + minReductionHashAggr);
        }
      }
    }

    try {
      for (int i = 0; i < keyFieldsNew.length; i++) {
        keyObjectsNew[i] = keyFieldsNew[i].evaluate(row);
      }

      if (hashmode) {
        deepCopyElements(keyObjectsNew, keyObjectInspectors, groupbyKeysHash,
            ObjectInspectorCopyOption.WRITABLE);

        if (containsfunctions) {
          int starttag = tag2AggrPos.get(0).size() == 0 ? 1 : 0;
          for (int disttag = starttag; disttag < this.aggrParamFieldsHash.length; disttag++) {
            for (int j = 0; j < aggrParamFieldsHash[disttag].length; j++) {
              cachedAggrParamHash[disttag][j] = this.aggrParamFieldsHash[disttag][j]
                  .evaluate(row);
            }

            if (hashAggr) {
              processHashAggrNewGroupBy(row, disttag);
            } else {
              processHashAggrNewGroupByAggr(row, disttag);
            }
          }
        } else {
          if (hashAggr) {
            processHashAggrNewGroupByNofunctions(row);
          } else {
            processHashAggrNewGroupByAggr(row, 0);
          }
        }
      } else if (finalmode) {
        if (containsfunctions) {
          LazyUnion lazyUO = (LazyUnion) unionExprEvalAggr.evaluate(row);
          processAggrNewGroupByFinal(row,
              (lazyUO == null) ? 0 : lazyUO.getTag());
        } else {
          processAggrNewGroupByFinal(row, 0);
        }
      } else {
        if (containsfunctions) {
          UnionObject aggrUO = (UnionObject) unionExprEvalAggr.evaluate(row);
          processAggrNewGroupBy(row, aggrUO,
              (aggrUO == null) ? 0 : aggrUO.getTag());
        } else {
          processAggrNewGroupBy(row, null, 0);
        }
      }
      firstRowInGroup = false;
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }

  }

  private void processHashAggrNewGroupBy(Object row, int disttag)
      throws HiveException {
    boolean newKeysIncomming = false;
    if (disttag == 0) {
      AggregationBuffer[] aggs = newHashAggregationsForNondist
          .get(groupbyKeysHash);
      if (aggs == null) {
        newDefaultKeys.clear();
        for (int i = 0; i < keyObjectsNew.length; i++) {
          newDefaultKeys.add(groupbyKeysHash.get(i));
        }

        aggs = newAggregations(0).aggs;
        newHashAggregationsForNondist.put(newDefaultKeys, aggs);
        newDefaultKeys1 = newDefaultKeys;
        newDefaultKeys = new ArrayList<Object>(gbykeylength);
        numRowsHashTbl++;
        newKeysIncomming = true;
      }

      updateAggregationsNewGroupByHash(row, aggs);

    } else {
      newDefaultKeys.clear();
      for (int i = 0; i < groupbyKeysHash.size(); i++) {
        newDefaultKeys.add(groupbyKeysHash.get(i));
      }

      newDefaultKeys.add(ObjectInspectorUtils
          .copyToStandardObject(cachedAggrParamHash[disttag],
              aggrParamObjectInspectorUOHash[disttag]));
      if (!newHashAggregationsForDist.get(disttag).contains(newDefaultKeys)) {
        newHashAggregationsForDist.get(disttag).add(newDefaultKeys);
        newDefaultKeys1 = newDefaultKeys;
        newDefaultKeys = new ArrayList<Object>(gbykeylength + 1);
        numRowsHashTbl++;
        newKeysIncomming = true;
      }
    }

    if (newKeysIncomming && shouldBeFlushedNewGroupBy(newDefaultKeys1, disttag)) {
      flushHashNewGroupBy(false);
    }
  }

  private void processHashAggrNewGroupByAggr(Object row, int disttag)
      throws HiveException {

    for (int i = 0; i < gbykeylength; i++) {
      forwardCacheArray[i] = groupbyKeysHash.get(i);
    }

    if (containsfunctions) {
      tmpAggrUO.setTag((byte) disttag);
      if (disttag == 0) {
        resetNewAggregations(newAggregations[disttag]);
        updateAggregationsNewGroupByHash(row, newAggregations[disttag].aggs);
        Object[] aggValues = new Object[newAggregations[disttag].aggs.length];
        for (int j = 0; j < aggValues.length; j++) {
          aggValues[j] = this.aggregationEvaluatorsNew[tag2AggrPos.get(disttag)
              .get(j)].evaluate(newAggregations[disttag].aggs[j]);
        }
        tmpAggrUO.setObject(aggValues);

      } else {

        tmpAggrUO.setObject(ObjectInspectorUtils.copyToStandardObject(
            cachedAggrParamHash[disttag],
            aggrParamObjectInspectorUOHash[disttag]));

      }
      forwardCacheArray[gbykeylength] = tmpAggrUO;
    }

    forward(forwardCacheArray, outputObjInspector);
  }

  private void processHashAggrNewGroupByNofunctions(Object row)
      throws HiveException {
    boolean newKeysIncomming = false;
    if (!newHashAggregationsForNondist.containsKey(groupbyKeysHash)) {
      newDefaultKeys.clear();
      for (int i = 0; i < keyObjectsNew.length; i++) {
        newDefaultKeys.add(groupbyKeysHash.get(i));
      }
      newHashAggregationsForNondist.put(newDefaultKeys, null);
      newDefaultKeys1 = newDefaultKeys;
      newDefaultKeys = new ArrayList<Object>(gbykeylength);
      numRowsHashTbl++;
      newKeysIncomming = true;
    }

    if (newKeysIncomming && shouldBeFlushedNewGroupBy(newDefaultKeys1, 0)) {
      flushHashNewGroupBy(false);
    }
  }

  private void processAggrNewGroupBy(Object row, UnionObject aggrUO, int disttag)
      throws HiveException {

    compareKeys.clear();
    for (int i = 0; i < keyObjectsNew.length; i++) {
      compareKeys.add(keyObjectsNew[i]);
    }

    boolean keysAreEqual = ObjectInspectorUtils.compare(compareKeys,
        newKeyCompareObjectInspector, currKeys, currKeyCompareObjectInspector) == 0;

    if (currKeys != null && !keysAreEqual) {
      forwardAll();
    }

    if (currKeys == null || !keysAreEqual) {

      if (currKeys == null) {
        currKeys = new ArrayList<Object>(gbykeylength);
        keysAreEqual = true;
      }
      currKeys.clear();
      deepCopyElements(keyObjectsNew, keyObjectInspectors, currKeys,
          ObjectInspectorCopyOption.WRITABLE);

      for (int i = 0; i < currAggrUOs.size(); i++) {
        currAggrUOs.set(i, null);
      }

      resetNewAggregations();
    }

    if (containsfunctions) {

      if (disttag == 0) {
        updateAggregationsNewGroupBy(newAggregations[disttag].aggs, row, false,
            disttag);
      } else if (keysAreEqual) {
        compareAggrUO.setTag((byte) disttag);
        compareAggrUO.setObject(currAggrUOs.get(disttag));
        if (ObjectInspectorUtils.compare(aggrUO, aggrParamObjectInspector,
            compareAggrUO, aggrParamObjectInspectorStandard) != 0) {
          Object newUnionObject = ObjectInspectorUtils.copyToStandardObject(
              aggrUO, aggrParamObjectInspector);
          updateAggregationsNewGroupBy(newAggregations[disttag].aggs, row,
              false, disttag);
          currAggrUOs.set(disttag, newUnionObject);
        }
      } else {
        Object newUnionObject = ObjectInspectorUtils.copyToStandardObject(
            aggrUO, aggrParamObjectInspector);
        updateAggregationsNewGroupBy(newAggregations[disttag].aggs, row, false,
            disttag);
        currAggrUOs.set(disttag, newUnionObject);
      }
    }
  }

  private void processAggrNewGroupByFinal(Object row, int disttag)
      throws HiveException {

    compareKeys.clear();
    for (int i = 0; i < keyObjectsNew.length; i++) {
      compareKeys.add(keyObjectsNew[i]);
    }

    boolean keysAreEqual = ObjectInspectorUtils.compare(compareKeys,
        newKeyCompareObjectInspector, currKeys, currKeyCompareObjectInspector) == 0;

    if (currKeys != null && !keysAreEqual) {
      forwardAggrFinal();
    }

    if (currKeys == null || !keysAreEqual) {

      if (currKeys == null) {
        currKeys = new ArrayList<Object>(gbykeylength);
      }
      currKeys.clear();
      deepCopyElements(keyObjectsNew, keyObjectInspectors, currKeys,
          ObjectInspectorCopyOption.WRITABLE);

      resetNewAggregations();
    }

    if (containsfunctions) {
      updateAggregationsNewGroupByFinal(newAggregations[disttag].aggs, row,
          disttag);
    }
  }

  private void updateAggregationsNewGroupByFinal(AggregationBuffer[] aggs,
      Object row, int disttag) throws HiveException {
    for (int ai = 0; ai < aggs.length; ai++) {
      int pos = this.tag2AggrPos.get(disttag).get(ai);
      cachedAggrParamObjects[pos][0] = aggregationParameterFieldsNew[pos][0]
          .evaluate(row);
      aggregationEvaluatorsNew[pos].aggregate(aggs[ai],
          cachedAggrParamObjects[pos]);
    }
  }

  private void updateAggregationsNewGroupBy(AggregationBuffer[] aggs,
      Object row, boolean hashAggr, int disttag) throws HiveException {
    if (disttag == 0) {
      for (int ai = 0; ai < aggs.length; ai++) {
        int pos = this.tag2AggrPos.get(disttag).get(ai);
        cachedAggrParamObjects[pos][0] = aggregationParameterFieldsNew[pos][0]
            .evaluate(row);
        aggregationEvaluatorsNew[pos].aggregate(aggs[ai],
            cachedAggrParamObjects[pos]);
      }
    } else {
      int fpos = this.tag2AggrPos.get(disttag).get(0);
      for (int pi = 0; pi < aggregationParameterFieldsNew[fpos].length; pi++) {
        cachedAggrParamObjects[fpos][pi] = aggregationParameterFieldsNew[fpos][pi]
            .evaluate(row);
      }
      for (int ai = 0; ai < aggs.length; ai++) {
        int pos = this.tag2AggrPos.get(disttag).get(ai);
        aggregationEvaluatorsNew[pos].aggregate(aggs[ai],
            cachedAggrParamObjects[fpos]);
      }
    }
  }

  private void updateAggregationsNewGroupByHash(Object row,
      AggregationBuffer[] aggs) throws HiveException {

    for (int ai = 0; ai < aggs.length; ai++) {
      int pos = this.tag2AggrPos.get(0).get(ai);
      for (int pi = 0; pi < aggregationParameterFieldsNew[pos].length; pi++) {
        cachedAggrParamObjects[pos][pi] = aggregationParameterFieldsNew[pos][pi]
            .evaluate(row);
      }
      aggregationEvaluatorsNew[pos].aggregate(aggs[ai],
          cachedAggrParamObjects[pos]);
    }
  }

  private void flushHashNewGroupBy(boolean complete) throws HiveException {

    if (complete) {
      Iterator<Map.Entry<ArrayList<Object>, AggregationBuffer[]>> iter = newHashAggregationsForNondist
          .entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<ArrayList<Object>, AggregationBuffer[]> m = iter.next();
        forwardHashDistTag0(m.getKey(), m.getValue());
      }
      newHashAggregationsForNondist.clear();
      newHashAggregationsForNondist = null;

      for (int disttag = 1; disttag < newHashAggregationsForDist.size(); disttag++) {
        for (ArrayList<Object> keys : newHashAggregationsForDist.get(disttag)) {
          forwardHash(keys, null, disttag);
        }
        newHashAggregationsForDist.get(disttag).clear();
        newHashAggregationsForDist.set(disttag, null);
      }
      newHashAggregationsForDist.clear();
      newHashAggregationsForDist = null;

      LOG.warn("Hash Table completed flushed");
      return;
    }

    int maxsize = newHashAggregationsForNondist.size();
    int maxsizetag = 0;
    for (int disttag = 1; disttag < newHashAggregationsForDist.size(); disttag++) {
      if (newHashAggregationsForDist.get(disttag).size() > maxsize) {
        maxsize = newHashAggregationsForDist.get(disttag).size();
        maxsizetag = disttag;
      }
    }

    int oldSize = newHashAggregationsForNondist.size();
    if (oldSize > hashFlushMinSize || maxsizetag == 0) {
      LOG.warn("Hash Tbl flush: tag 0 #hash table = " + oldSize);
      Iterator<Map.Entry<ArrayList<Object>, AggregationBuffer[]>> iter = newHashAggregationsForNondist
          .entrySet().iterator();
      int numDel = 0;
      while (iter.hasNext()) {
        Map.Entry<ArrayList<Object>, AggregationBuffer[]> m = iter.next();
        forwardHashDistTag0(m.getKey(), m.getValue());
        iter.remove();
        numDel++;
        if (numDel * 10 >= oldSize) {
          LOG.warn("Hash Table flushed: tag 0 new size = "
              + newHashAggregationsForNondist.size());
          break;
        }
      }
    }

    for (int disttag = 1; disttag < newHashAggregationsForDist.size(); disttag++) {
      oldSize = newHashAggregationsForDist.get(disttag).size();
      if (oldSize > hashFlushMinSize || maxsizetag == disttag) {

        LOG.warn("Hash Tbl flush: tag " + disttag + " #hash table = " + oldSize);
        Iterator<ArrayList<Object>> iter1 = newHashAggregationsForDist.get(
            disttag).iterator();
        int numDel = 0;
        while (iter1.hasNext()) {
          forwardHash(iter1.next(), null, disttag);
          iter1.remove();
          numDel++;
          if (numDel * 10 >= oldSize) {
            LOG.warn("Hash Table flushed: tag " + disttag + " new size = "
                + newHashAggregationsForDist.get(disttag).size());
            break;
          }
        }
      }
    }
  }

  StandardUnion tmpAggrUO = new StandardUnion();

  transient Object[] forwardCacheArray = null;

  private void forwardHashDistTag0(ArrayList<Object> keys,
      AggregationBuffer[] aggs) throws HiveException {

    for (int i = 0; i < gbykeylength; i++) {
      forwardCacheArray[i] = keys.get(i);
    }

    if (containsfunctions) {
      tmpAggrUO.setTag((byte) 0);
      Object[] aggValues = new Object[aggs.length];
      for (int j = 0; j < aggValues.length; j++) {
        aggValues[j] = this.aggregationEvaluatorsNew[tag2AggrPos.get(0).get(j)]
            .evaluate(aggs[j]);
      }
      tmpAggrUO.setObject(aggValues);
      forwardCacheArray[gbykeylength] = tmpAggrUO;
    }

    forward(forwardCacheArray, outputObjInspector);
  }

  private void forwardHash(ArrayList<Object> keys, AggregationBuffer[] aggs,
      int disttag) throws HiveException {

    for (int i = 0; i < gbykeylength; i++) {
      forwardCacheArray[i] = keys.get(i);
    }

    if (containsfunctions) {
      tmpAggrUO.setTag((byte) disttag);
      tmpAggrUO.setObject(keys.get(gbykeylength));
      forwardCacheArray[gbykeylength] = tmpAggrUO;
    }

    forward(forwardCacheArray, outputObjInspector);
  }

  private void forwardAggrFinal() throws HiveException {
    for (int i = 0; i < gbykeylength; i++) {
      forwardCacheArray[i] = currKeys.get(i);
    }

    if (containsfunctions) {
      for (int disttag = 0; disttag < tag2AggrPos.size(); disttag++) {
        int posoff = 0;
        for (Integer pos : tag2AggrPos.get(disttag)) {
          forwardCacheArray[gbykeylength + pos] = this.aggregationEvaluatorsNew[pos]
              .evaluate(newAggregations[disttag].aggs[posoff++]);
        }
      }
    }
    forward(forwardCacheArray, outputObjInspector);
  }

  private void forward(int disttag) throws HiveException {

    for (int i = 0; i < gbykeylength; i++) {
      forwardCacheArray[i] = currKeys.get(i);
    }

    tmpAggrUO.setTag((byte) disttag);
    if (mergemode || disttag == 0) {
      Object[] aggValues = new Object[newAggregations[disttag].aggs.length];
      for (int j = 0; j < aggValues.length; j++) {
        aggValues[j] = this.aggregationEvaluatorsNew[tag2AggrPos.get(disttag)
            .get(j)].evaluate(newAggregations[disttag].aggs[j]);
      }
      tmpAggrUO.setObject(aggValues);
    } else {
      tmpAggrUO.setObject(currAggrUOs.get(disttag));
    }

    forwardCacheArray[gbykeylength] = tmpAggrUO;

    forward(forwardCacheArray, outputObjInspector);

  }

  private void forwardAll() throws HiveException {
    if (containsfunctions) {
      if (lastgby) {

        for (int i = 0; i < gbykeylength; i++) {
          forwardCacheArray[i] = currKeys.get(i);
        }

        for (int disttag = 0; disttag < tag2AggrPos.size(); disttag++) {
          int posoff = 0;
          for (Integer pos : tag2AggrPos.get(disttag)) {
            forwardCacheArray[gbykeylength + pos] = this.aggregationEvaluatorsNew[pos]
                .evaluate(newAggregations[disttag].aggs[posoff++]);
          }
        }

        forward(forwardCacheArray, outputObjInspector);
      } else {
        if (tag2AggrPos.get(0).size() > 0) {
          forward(0);
        }
        for (int disttag = 1; disttag < this.tag2AggrPos.size(); disttag++) {
          forward(disttag);
        }
      }
    } else {

      for (int i = 0; i < gbykeylength; i++) {
        forwardCacheArray[i] = currKeys.get(i);
      }

      forward(forwardCacheArray, outputObjInspector);
    }
  }

  @SuppressWarnings("unchecked")
  private boolean shouldBeFlushedNewGroupBy(ArrayList<Object> newKeys,
      int disttag) {
    int numEntries = newHashAggregationsForNondist.size();
    for (HashSet<ArrayList<Object>> hs : newHashAggregationsForDist) {
      numEntries += hs.size();
    }
    
    if(containEstDistinct){
      
      //LOG.info("----------------------est distinct block num is " + GenericUDAFCardinalityEstimation.totalEstDistBlockNum);
      
      if ((numEntriesHashTable == 0)
          || ((numEntries % (NUMROWSESTIMATESIZE/10)) <= tag2AggrPos.size())) {
        for (Integer pos : keyPositionsSize) {
          Object key = newKeys.get(pos.intValue());
          if (key != null) {
            if (key instanceof String) {
              totalVariableSize += ((String) key).length();
            } else if (key instanceof Text) {
              totalVariableSize += ((Text) key).getLength();
            }
          }
        }

        if (disttag == 0) {
          for (varLenFields v : aggrPositions) {
            int aggrPos = v.getAggrPos();
            if (!this.tag2AggrPos.get(disttag).contains(aggrPos)) {
              continue;
            }
            List<Field> fieldsVarLen = v.getFields();
            AggregationBuffer agg = newHashAggregationsForNondist.get(newKeys)[aggrPos];

            try {
              for (Field f : fieldsVarLen)
                totalVariableSize += ((String) f.get(agg)).length();
            } catch (IllegalAccessException e) {
              assert false;
            }
          }
        } else {
          Object uo = newKeys.get(gbykeylength);
          for (int aggrpos : aggrPositionsSize) {
            if ((aggrpos >> 16) == disttag) {
              int posoff = aggrpos & 0x0ffff;
              if (uo instanceof ArrayList<?>) {
                Object obj = ((ArrayList<Object>) uo).get(posoff);
                if (obj instanceof Text) {
                  totalVariableSize += ((Text) obj).getLength();
                } else if (obj instanceof String) {
                  totalVariableSize += ((String) obj).length();
                } else {
                  totalVariableSize += javaSizeUnknownType;
                }
              } else {
                totalVariableSize += ((String) ((Object[]) newKeys
                    .get(gbykeylength))[posoff]).length();
              }
            }
          }
        }

        numEntriesVarSize++;
        
        numEntriesHashTable = (int) (maxHashTblMemory / (fixedRowSize
              + fixAggrPartSize[disttag]
              + GenericUDAFCardinalityEstimation.totalEstDistBlockNum * GenericUDAFCardinalityEstimation.buffSize / numEntries
              + ((int) totalVariableSize / numEntriesVarSize)));

        LOG.info("-----------------------Hash Aggr: #hash table = " + numEntries
            + " #max in hash table = " + numEntriesHashTable);
      }
      
    }
    else{
      if ((numEntriesHashTable == 0)
          || ((numEntries % NUMROWSESTIMATESIZE) <= tag2AggrPos.size())) {
        for (Integer pos : keyPositionsSize) {
          Object key = newKeys.get(pos.intValue());
          if (key != null) {
            if (key instanceof String) {
              totalVariableSize += ((String) key).length();
            } else if (key instanceof Text) {
              totalVariableSize += ((Text) key).getLength();
            }
          }
        }

        if (disttag == 0) {
          for (varLenFields v : aggrPositions) {
            int aggrPos = v.getAggrPos();
            if (!this.tag2AggrPos.get(disttag).contains(aggrPos)) {
              continue;
            }
            List<Field> fieldsVarLen = v.getFields();
            AggregationBuffer agg = newHashAggregationsForNondist.get(newKeys)[aggrPos];

            try {
              for (Field f : fieldsVarLen)
                totalVariableSize += ((String) f.get(agg)).length();
            } catch (IllegalAccessException e) {
              assert false;
            }
          }
        } else {
          Object uo = newKeys.get(gbykeylength);
          for (int aggrpos : aggrPositionsSize) {
            if ((aggrpos >> 16) == disttag) {
              int posoff = aggrpos & 0x0ffff;
              if (uo instanceof ArrayList<?>) {
                Object obj = ((ArrayList<Object>) uo).get(posoff);
                if (obj instanceof Text) {
                  totalVariableSize += ((Text) obj).getLength();
                } else if (obj instanceof String) {
                  totalVariableSize += ((String) obj).length();
                } else {
                  totalVariableSize += javaSizeUnknownType;
                }
              } else {
                totalVariableSize += ((String) ((Object[]) newKeys
                    .get(gbykeylength))[posoff]).length();
              }
            }
          }
        }

        numEntriesVarSize++;
        
        numEntriesHashTable = (int) (maxHashTblMemory / (fixedRowSize
              + fixAggrPartSize[disttag]
              + ((int) totalVariableSize / numEntriesVarSize)));

        LOG.trace("Hash Aggr: #hash table = " + numEntries
            + " #max in hash table = " + numEntriesHashTable);
      }
    }

    


    if (numEntries >= numEntriesHashTable)
      return true;
    return false;
  }

  private void resetNewAggregations() throws HiveException {
    for (TagedAggregationBuffer aggrbuffer : this.newAggregations) {
      if (aggrbuffer != null)
        this.resetNewAggregations(aggrbuffer);
    }
  }

  private void resetNewAggregations(TagedAggregationBuffer tagedaggs)
      throws HiveException {
    for (int i = 0; i < tagedaggs.aggs.length; i++) {
      int pos = this.tag2AggrPos.get(tagedaggs.tag).get(i);
      aggregationEvaluatorsNew[pos].reset(tagedaggs.aggs[i]);
    }
  }

  private TagedAggregationBuffer newAggregations(int disttag)
      throws HiveException {
    if (!this.containsfunctions) {
      return new TagedAggregationBuffer(0, new AggregationBuffer[0]);
    }
    int num = tag2AggrPos.get(disttag).size();
    AggregationBuffer[] aggs = new AggregationBuffer[num];
    int ii = 0;
    for (int pos : tag2AggrPos.get(disttag)) {
      aggs[ii++] = aggregationEvaluatorsNew[pos].getNewAggregationBuffer();
    }
    return new TagedAggregationBuffer(disttag, aggs);
  }

  Object[] nullobj = { null, null, null, null, null, null, null, null };

  public void closeOpNewGroupBy(boolean abort) throws HiveException {
    if (!abort) {
      try {
        if (firstRow && (keyFieldsNew.length == 0)) {
          firstRow = false;

          if (this.containsfunctions) {
            hashAggr = false;
            for (int i = 0; i < newAggregations.length; i++) {
              TagedAggregationBuffer aggbuffer = newAggregations[i];
              if (aggbuffer == null) {
                continue;
              }
              for (int j = 0; j < aggbuffer.aggs.length; j++) {
                aggregationEvaluatorsNew[this.tag2AggrPos.get(i).get(j)]
                    .aggregate(aggbuffer.aggs[j], nullobj);
              }
            }
            forwardAll();
          }
        } else {
          if (hashmode) {
            if (hashAggr) {
              flushHashNewGroupBy(true);
            }
          } else {
            if (currKeys != null) {
              forwardAll();
            }
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        throw new HiveException(e);
      }
    }
  }

  int[] fixAggrPartSize = null;

  private void computeMaxEntriesHashAggrNewGroupBy(Configuration hconf)
      throws HiveException {
    maxHashTblMemory = (long) (HiveConf.getFloatVar(hconf,
        HiveConf.ConfVars.HIVEMAPAGGRHASHMEMORY) * Runtime.getRuntime()
        .maxMemory());
    long maxmem = HiveConf.getLongVar(hconf,
        HiveConf.ConfVars.HIVEMAPAGGRHASHMEMORY_MAX);
    if (maxHashTblMemory > maxmem) {
      maxHashTblMemory = maxmem;
    }
    LOG.info("maxHashTblMemory:\t" + maxHashTblMemory);

    fixedRowSize = javaHashEntryOverHead;

    ArrayList<exprNodeDesc> keys = conf.getKeys();

    for (int pos = 0; pos < keys.size(); pos++)
      fixedRowSize += getSize(pos, keys.get(pos).getTypeInfo());

    fixAggrPartSize = new int[tag2AggrPos.size()];
    for (int tag = 0; tag < fixAggrPartSize.length; tag++) {
      fixAggrPartSize[tag] = 0;
    }
    if (tag2AggrPos.size() == 0) {
      fixAggrPartSize = new int[1];
      fixAggrPartSize[0] = 0;
    }
    if (containsfunctions) {
      for (int tag = 0; tag < tag2AggrPos.size(); tag++) {
        if (tag == 0) {
          for (int i = 0; i < tag2AggrPos.get(tag).size(); i++) {
            fixAggrPartSize[tag] += javaObjectOverHead;
            AggregationBuffer buf = aggregationEvaluatorsNew[tag2AggrPos.get(tag).get(i)].getNewAggregationBuffer();
            if(buf instanceof GenericUDAFCardinalityEstimation.CardinalityEstimationAgg)
            {
              containEstDistinct = true;
              fixAggrPartSize[tag] +=  
                  + 4 * GenericUDAFCardinalityEstimation.buffNum;
              continue;
            }
            Class<? extends AggregationBuffer> agg = aggregationEvaluatorsNew[tag2AggrPos
                .get(tag).get(i)].getNewAggregationBuffer().getClass();
            Field[] fArr = ObjectInspectorUtils.getDeclaredNonStaticFields(agg);
            for (Field f : fArr) {
              fixAggrPartSize[tag] += getSize((tag << 16) + i, f.getType(), f);
            }
          }
        } else {
          for (int i = 0; i < conf.getTag2AggrParamExpr().get(tag).size(); i++) {
            fixAggrPartSize[tag] += javaObjectOverHead;
            TypeInfo typeinfo = conf.getTag2AggrParamExpr().get(tag).get(i)
                .getTypeInfo();
            fixAggrPartSize[tag] += getSize1((tag << 16) + i, typeinfo);
          }
        }
      }
    }
    LOG.info("estimate fixedRowSize:\t" + fixedRowSize);
    for (int i = 0; i < fixAggrPartSize.length; i++) {
      LOG.info("estimate fixAggrPartSize[" + i + "]:\t" + fixAggrPartSize[i]);
    }
    LOG.info("estimate aggrPositionsSize:\t" + aggrPositionsSize);
  }

  private int getSize1(int pos, TypeInfo typeInfo) {
    if (typeInfo instanceof PrimitiveTypeInfo)
      return getSize1(pos,
          ((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory());
    return javaSizeUnknownType;
  }

  private int getSize1(int pos, PrimitiveCategory category) {
    switch (category) {
    case VOID:
    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case FLOAT:
    case DOUBLE: {
      return javaSizePrimitiveType;
    }
    case STRING: {
      aggrPositionsSize.add(new Integer(pos));
      return javaObjectOverHead;
    }
    default: {
      return javaSizeUnknownType;
    }
    }
  }

}
