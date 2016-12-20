/**
* Tencent is pleased to support the open source community by making TDW available.
* Copyright (C) 2014 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use 
* this file except in compliance with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed 
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS 
* OF ANY KIND, either express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/
package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.persistence.AnalysisBuffer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.analysisDesc;
import org.apache.hadoop.hive.ql.plan.analysisEvaluatorDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDWFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDWFEvaluator.BooleanTrans;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDWFEvaluator.AnalysisEvaluatorBuffer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;

public class AnalysisOperator extends Operator<analysisDesc> implements
    Serializable {

  private static final long serialVersionUID = -8313560832289804735L;
  transient protected Log LOG = LogFactory.getLog(this.getClass().getName());

  transient protected ExprNodeEvaluator[] pkeyFields;
  transient protected ObjectInspector[] pkeyObjectInspectors;
  transient protected ObjectInspector[] pkeyStandardObjectInspectors;
  transient protected Object[] pkeyObjects;

  transient protected ExprNodeEvaluator[] okeyFields;
  transient protected ObjectInspector[] okeyObjectInspectors;
  transient protected ObjectInspector[] okeyStandardObjectInspectors;
  transient protected Object[] okeyObjects;

  transient protected ExprNodeEvaluator[][] analysisParameterFields;
  transient protected ObjectInspector[][] analysisParameterObjectInspectors;
  transient protected Object[][] analysisParameterObjects;
  transient protected boolean[] analysisIsDistinct;

  transient protected ExprNodeEvaluator[] otherColumns;
  transient protected ObjectInspector[] otherColumnsObjectInspectors;
  transient protected Object[] otherColumnsObjects;

  transient protected GenericUDWFEvaluator[] analysisEvaluators;
  transient protected AnalysisEvaluatorBuffer[] aggregations;
  transient protected Object[][] analysisParametersLastInvoke;

  transient protected ArrayList<ObjectInspector> objectInspectors;
  transient protected ArrayList<String> fieldNames;

  transient protected ArrayList<Object> pnewKeys;
  transient protected ArrayList<Object> pcurrentKeys;

  transient StructObjectInspector pnewKeyObjectInspector;
  transient StructObjectInspector pcurrentKeyObjectInspector;

  transient protected boolean[] hasAggregateOrderBy;
  transient protected int hasAggregateOrderByNumber = 0;
  transient protected int[] hasAggregateOrderByIdx;
  transient protected int[] hasAggregateOrderByRevIdx;
  transient ObjectInspector aggregateOrderByObjectInspectorANAStore;

  boolean isDistinct = false;

  AnalysisBuffer<Object> anabuffer;

  int windowlag = 0;
  int windowlead = 0;
  int currentrowid = 0;
  int currentforwardrow = 0;

  enum ForwardMode {
    WHOLEPARTITION, IMMEDIATE
  }

  ObjectInspector rowInspector;
  ObjectInspector standardRowInspector;

  ForwardMode forwardMode = ForwardMode.IMMEDIATE;
  Configuration hconf;
  SerDe anaserde = null;

  protected void initializeOp(Configuration hconf) throws HiveException {
    this.hconf = hconf;
    rowInspector = inputObjInspectors[0];

    standardRowInspector = ObjectInspectorUtils
        .getStandardObjectInspector(rowInspector);

    isDistinct = conf.getDistinct();

    pkeyFields = new ExprNodeEvaluator[conf.getPartitionByKeys().size()];
    pkeyObjectInspectors = new ObjectInspector[pkeyFields.length];
    pkeyObjects = new Object[pkeyFields.length];

    if (pkeyFields.length > 0) {
      for (int i = 0; i < pkeyFields.length; i++) {
        pkeyFields[i] = ExprNodeEvaluatorFactory.get(conf.getPartitionByKeys()
            .get(i));
        pkeyObjectInspectors[i] = pkeyFields[i]
            .initialize(standardRowInspector);
        pkeyObjects[i] = null;
      }
    }

    okeyFields = new ExprNodeEvaluator[conf.getOrderByKeys().size()];
    okeyObjectInspectors = new ObjectInspector[okeyFields.length];
    okeyObjects = new Object[okeyFields.length];
    for (int i = 0; i < okeyFields.length; i++) {
      okeyFields[i] = ExprNodeEvaluatorFactory
          .get(conf.getOrderByKeys().get(i));
      okeyObjectInspectors[i] = okeyFields[i].initialize(standardRowInspector);
      okeyObjects[i] = null;
    }

    hasAggregateOrderBy = new boolean[conf.getAnalysises().size()];
    hasAggregateOrderByRevIdx = new int[conf.getAnalysises().size()];
    analysisParameterFields = new ExprNodeEvaluator[conf.getAnalysises().size()][];
    analysisParameterObjectInspectors = new ObjectInspector[conf
        .getAnalysises().size()][];
    analysisParameterObjects = new Object[conf.getAnalysises().size()][];
    for (int i = 0; i < analysisParameterFields.length; i++) {
      analysisEvaluatorDesc aed = conf.getAnalysises().get(i);
      String udwfname = aed.getGenericUDWFName().toLowerCase();
      ArrayList<exprNodeDesc> parameters = aed.getParameters();
      hasAggregateOrderBy[i] = aed.hasAggregateOrderBy();
      hasAggregateOrderByRevIdx[i] = -1;
      if (hasAggregateOrderBy[i]) {
        hasAggregateOrderByRevIdx[i] = hasAggregateOrderByNumber;
        hasAggregateOrderByNumber++;
      }
      if (udwfname.contains("lag")) {
        int lag = 1;
        if (parameters.size() > 1) {
          lag = (Integer) ((exprNodeConstantDesc) parameters.get(1)).getValue();
        }
        if (lag > windowlag)
          windowlag = lag;
      } else if (udwfname.contains("lead")) {
        int lead = 1;
        if (parameters.size() > 1) {
          lead = (Integer) ((exprNodeConstantDesc) parameters.get(1))
              .getValue();
        }
        if (lead > windowlead)
          windowlead = lead;
      } else if (udwfname.contains("row_number") || udwfname.contains("rank")
          || udwfname.contains("first_value")) {
      } else if (hasAggregateOrderBy[i]) {

      } else {
        this.forwardMode = ForwardMode.WHOLEPARTITION;
      }

      if (udwfname.contains("rank")) {
        parameters = new ArrayList<exprNodeDesc>();
        parameters.addAll(conf.getOrderByKeys());
      }

      analysisParameterFields[i] = new ExprNodeEvaluator[parameters.size()];
      analysisParameterObjectInspectors[i] = new ObjectInspector[parameters
          .size()];
      analysisParameterObjects[i] = new Object[parameters.size()];
      for (int j = 0; j < parameters.size(); j++) {
        analysisParameterFields[i][j] = ExprNodeEvaluatorFactory.get(parameters
            .get(j));
        analysisParameterObjectInspectors[i][j] = analysisParameterFields[i][j]
            .initialize(standardRowInspector);
        analysisParameterObjects[i][j] = null;
      }
    }

    hasAggregateOrderByIdx = new int[this.hasAggregateOrderByNumber];
    int numm = 0;
    for (int j = 0; j < this.hasAggregateOrderBy.length; j++) {
      if (this.hasAggregateOrderBy[j]) {
        this.hasAggregateOrderByIdx[numm++] = j;
      }
    }

    otherColumns = new ExprNodeEvaluator[conf.getOtherColumns().size()];
    otherColumnsObjectInspectors = new ObjectInspector[otherColumns.length];
    otherColumnsObjects = new Object[otherColumns.length];

    for (int i = 0; i < otherColumns.length; i++) {
      otherColumns[i] = ExprNodeEvaluatorFactory.get(conf.getOtherColumns()
          .get(i));
      otherColumnsObjectInspectors[i] = otherColumns[i]
          .initialize(standardRowInspector);
      otherColumnsObjects[i] = null;
    }

    analysisIsDistinct = new boolean[conf.getAnalysises().size()];
    for (int i = 0; i < analysisIsDistinct.length; i++) {
      analysisIsDistinct[i] = conf.getAnalysises().get(i).getDistinct();
    }

    analysisEvaluators = new GenericUDWFEvaluator[conf.getAnalysises().size()];
    for (int i = 0; i < analysisEvaluators.length; i++) {
      analysisEvaluatorDesc agg = conf.getAnalysises().get(i);
      analysisEvaluators[i] = agg.getGenericUDWFEvaluator();
    }
    int totalFields = pkeyFields.length + okeyFields.length
        + analysisEvaluators.length + otherColumns.length;
    objectInspectors = new ArrayList<ObjectInspector>(totalFields);
    for (int i = 0; i < pkeyFields.length; i++) {
      objectInspectors.add(pkeyObjectInspectors[i]);
    }

    for (int i = 0; i < okeyFields.length; i++) {
      objectInspectors.add(okeyObjectInspectors[i]);
    }

    ArrayList<ObjectInspector> aggregateOrderByObjectInspectors = new ArrayList<ObjectInspector>();
    ArrayList<String> anaStoredName = new ArrayList<String>();
    for (int i = 0; i < analysisEvaluators.length; i++) {
      ObjectInspector roi = analysisEvaluators[i]
          .init(analysisParameterObjectInspectors[i]);
      objectInspectors.add(roi);
      if (hasAggregateOrderBy[i]) {
        anaStoredName.add("aggr" + i);
        aggregateOrderByObjectInspectors.add(roi);
      }
    }

    for (int i = 0; i < otherColumns.length; i++) {
      objectInspectors.add(otherColumnsObjectInspectors[i]);
    }

    outputObjInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(conf.getOutputColumnNames(),
            objectInspectors);

    anaStoredName.add(0, "rowobj");
    aggregateOrderByObjectInspectors.add(0, standardRowInspector);
    aggregateOrderByObjectInspectorANAStore = ObjectInspectorFactory
        .getStandardStructObjectInspector(anaStoredName,
            aggregateOrderByObjectInspectors);

    ArrayList<String> keyNames = new ArrayList<String>(pkeyFields.length);
    for (int i = 0; i < pkeyFields.length; i++) {
      keyNames.add(conf.getOutputColumnNames().get(i));
    }

    pcurrentKeyObjectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(keyNames,
            Arrays.asList(pkeyObjectInspectors));

    pnewKeyObjectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(keyNames,
            Arrays.asList(pkeyObjectInspectors));

    analysisParametersLastInvoke = new Object[conf.getAnalysises().size()][];

    aggregations = newAggregations();

    pnewKeys = new ArrayList<Object>();

    StringBuffer colNames = new StringBuffer();
    StringBuffer colTypes = new StringBuffer();

    StructObjectInspector soi = (StructObjectInspector) aggregateOrderByObjectInspectorANAStore;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();

    for (int k = 0; k < fields.size(); k++) {
      String newColName = "_VALUE_" + k;
      colNames.append(newColName);
      colNames.append(',');
      colTypes.append(fields.get(k).getFieldObjectInspector().getTypeName());
      colTypes.append(',');
    }
    colNames.setLength(colNames.length() - 1);
    colTypes.setLength(colTypes.length() - 1);

    Properties properties = Utilities.makeProperties(
        org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, ""
            + Utilities.ctrlaCode,
        org.apache.hadoop.hive.serde.Constants.LIST_COLUMNS,
        colNames.toString(),
        org.apache.hadoop.hive.serde.Constants.LIST_COLUMN_TYPES,
        colTypes.toString());

    try {
      anaserde = LazyBinarySerDe.class.newInstance();
      anaserde.initialize(hconf, properties);
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (SerDeException e) {
      e.printStackTrace();
    }

    anabuffer = new AnalysisBuffer<Object>(anaserde,
        this.aggregateOrderByObjectInspectorANAStore, hconf);

    System.out.println("hasAggregateOrderByNumber\t"
        + hasAggregateOrderByNumber);
    for (int i = 0; i < hasAggregateOrderBy.length; i++) {
      System.out.print(hasAggregateOrderBy[i] + "\t");
    }
    System.out.println();
    for (int i = 0; i < hasAggregateOrderByIdx.length; i++) {
      System.out.print(hasAggregateOrderByIdx[i] + "\t");
    }
    System.out.println();
    for (int i = 0; i < hasAggregateOrderByRevIdx.length; i++) {
      System.out.print(hasAggregateOrderByRevIdx[i] + "\t");
    }
    System.out.println();

    initializeChildren(hconf);
  }

  protected AnalysisEvaluatorBuffer[] newAggregations() throws HiveException {
    AnalysisEvaluatorBuffer[] aggs = new AnalysisEvaluatorBuffer[analysisEvaluators.length];
    for (int i = 0; i < analysisEvaluators.length; i++) {
      aggs[i] = analysisEvaluators[i].getNewAnalysisEvaBuffer();
    }
    return aggs;
  }

  @Override
  public void process(Object row, int tag) throws HiveException {

    Object obj = ObjectInspectorUtils.copyToStandardObject(row, rowInspector,
        ObjectInspectorCopyOption.DEFAULT);

    pnewKeys.clear();
    for (int i = 0; i < pkeyFields.length; i++) {
      if (pkeyObjectInspectors[i] == null) {
        pkeyObjectInspectors[i] = pkeyFields[i]
            .initialize(standardRowInspector);
      }
      pkeyObjects[i] = pkeyFields[i].evaluate(obj);
      pnewKeys.add(pkeyObjects[i]);
    }

    for (int i = 0; i < okeyFields.length; i++) {
      if (okeyObjectInspectors[i] == null) {
        okeyObjectInspectors[i] = okeyFields[i]
            .initialize(standardRowInspector);
      }
      okeyObjects[i] = okeyFields[i].evaluate(obj);
    }

    boolean keysAreEqual = ObjectInspectorUtils.compare(pnewKeys,
        pnewKeyObjectInspector, pcurrentKeys, pcurrentKeyObjectInspector) == 0;

    if (pcurrentKeys != null && !keysAreEqual) {
      forwardPartition();
    }

    if (pcurrentKeys == null || !keysAreEqual) {
      if (pcurrentKeys == null) {
        pcurrentKeys = new ArrayList<Object>(pkeyFields.length);
      }
      deepCopyElements(pkeyObjects, pkeyObjectInspectors, pcurrentKeys,
          ObjectInspectorCopyOption.WRITABLE);

      resetAggregations(aggregations);

      for (int i = 0; i < analysisParametersLastInvoke.length; i++)
        analysisParametersLastInvoke[i] = null;
      anabuffer.reset();

      currentrowid = 0;
      currentforwardrow = 0;
    }

    updateAggregations(obj);

    ArrayList<Object> store1 = new ArrayList<Object>();
    store1.add(obj);
    if (hasAggregateOrderByNumber > 0) {
      for (int i = 0; i < hasAggregateOrderByNumber; i++) {
        Object paramobj = analysisParameterFields[this.hasAggregateOrderByIdx[i]][0]
            .evaluate(obj);
        store1.add(this.analysisEvaluators[this.hasAggregateOrderByIdx[i]]
            .terminateCurrent(
                this.aggregations[this.hasAggregateOrderByIdx[i]], paramobj));
      }
    }

    anabuffer.add(store1);

    if (forwardMode == ForwardMode.IMMEDIATE) {
      forwardimmediate();
    }
    currentrowid++;
  }

  private void forwardimmediate() throws HiveException {

    if (currentforwardrow < windowlag
        && currentrowid >= currentforwardrow + windowlead) {
      forward(anabuffer, anabuffer.getByRowid(currentforwardrow),
          currentforwardrow++, true);
    } else if (currentforwardrow >= windowlag) {
      if (forward(anabuffer, anabuffer.getByRowid(currentforwardrow),
          currentforwardrow, false)) {
        currentforwardrow++;
        anabuffer.removeFirst(false);
      }
    }
  }

  private void forwardPartition() throws HiveException {
    if (!anabuffer.seek(currentforwardrow))
      return;
    Object row;
    while ((row = anabuffer.next()) != null) {
      forward(anabuffer, row, currentforwardrow++, true);
    }
  }

  transient Object[] forwardCache;

  protected boolean forward(AnalysisBuffer<Object> analysisBuffer, Object row,
      int rowid, boolean absolute) throws HiveException {
    if (row == null)
      return false;
    ArrayList<Object> rowfull = ((ArrayList<Object>) row);
    row = rowfull.get(0);
    if (row == null)
      return false;
    BooleanTrans canternimate = new BooleanTrans();
    int totalFields = this.pcurrentKeys.size() + this.okeyFields.length
        + aggregations.length + this.otherColumns.length;
    if (forwardCache == null) {
      forwardCache = new Object[totalFields];
    }
    int ii = 0;
    for (int i = 0; i < pcurrentKeys.size(); i++) {
      forwardCache[ii] = pcurrentKeys.get(i);
      ii++;
    }

    for (int i = 0; i < okeyFields.length; i++) {
      forwardCache[ii] = okeyFields[i].evaluate(row);
      ii++;
    }

    for (int i = 0; i < aggregations.length; i++) {
      if (this.hasAggregateOrderBy[i]) {
        forwardCache[ii] = rowfull.get(this.hasAggregateOrderByRevIdx[i] + 1);
        canternimate.set(true);
      } else {
        forwardCache[ii] = analysisEvaluators[i].terminate(aggregations[i],
            analysisParameterFields[i], analysisBuffer, rowid, absolute,
            canternimate);
      }
      if (!canternimate.get())
        return false;
      ii++;
    }

    for (int i = 0; i < otherColumns.length; i++) {
      forwardCache[ii] = otherColumns[i].evaluate(row);
      ii++;
    }

    forward(forwardCache, outputObjInspector);
    return true;
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

  protected void resetAggregations(AnalysisEvaluatorBuffer[] aggs)
      throws HiveException {
    for (int i = 0; i < aggs.length; i++) {
      analysisEvaluators[i].reset(aggs[i]);
    }
  }

  protected void updateAggregations(Object obj) throws HiveException {

    for (int ai = 0; ai < aggregations.length; ai++) {

      Object[] o = new Object[analysisParameterFields[ai].length];
      for (int pi = 0; pi < analysisParameterFields[ai].length; pi++) {
        o[pi] = analysisParameterFields[ai][pi].evaluate(obj);
      }

      if (analysisIsDistinct[ai]) {

        if (analysisParametersLastInvoke[ai] == null) {
          analysisParametersLastInvoke[ai] = new Object[o.length];
        }
        if (ObjectInspectorUtils.compare(o,
            analysisParameterObjectInspectors[ai],
            analysisParametersLastInvoke[ai],
            analysisParameterObjectInspectors[ai]) != 0) {
          analysisEvaluators[ai].analysis(aggregations[ai], o);
          for (int pi = 0; pi < o.length; pi++) {
            analysisParametersLastInvoke[ai][pi] = ObjectInspectorUtils
                .copyToStandardObject(o[pi],
                    analysisParameterObjectInspectors[ai][pi],
                    ObjectInspectorCopyOption.WRITABLE);
          }
        }

      } else {
        analysisEvaluators[ai].analysis(aggregations[ai], o);
      }
    }
  }

  protected void closeOp(boolean abort) throws HiveException {
    forwardPartition();
    anabuffer.close();
  }

  public String getName() {
    return new String("ANA");
  }

}
