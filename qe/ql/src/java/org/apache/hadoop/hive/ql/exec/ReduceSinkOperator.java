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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.reduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.lazy.LazyUnion;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyUnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObject;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ReduceSinkOperator extends TerminalOperator<reduceSinkDesc>
    implements Serializable {

  private static final long serialVersionUID = 1L;

  transient protected ExprNodeEvaluator[] keyEval;

  transient protected ExprNodeEvaluator[] valueEval;

  transient protected ExprNodeEvaluator[] partitionEval;

  transient Serializer keySerializer;
  transient boolean keyIsText;
  transient Serializer valueSerializer;
  transient int tag;
  transient byte[] tagByte = new byte[1];
  transient protected int numDistributionKeys;
  transient protected int numDistinctExprs;

  transient boolean partitionByGbkeyanddistinctkey;

  protected void initializeOp(Configuration hconf) throws HiveException {

    try {

      if (conf.isUsenewgroupby()) {
        initializeOpNewGroupBy(hconf);
        return;
      }

      this.partitionByGbkeyanddistinctkey = conf
          .getPartitionByGbkeyanddistinctkey();
      keyEval = new ExprNodeEvaluator[conf.getKeyCols().size()];
      int i = 0;
      for (exprNodeDesc e : conf.getKeyCols()) {
        keyEval[i++] = ExprNodeEvaluatorFactory.get(e);
      }

      numDistributionKeys = conf.getNumDistributionKeys();
      distinctColIndices = conf.getDistinctColumnIndices();
      numDistinctExprs = distinctColIndices.size();

      valueEval = new ExprNodeEvaluator[conf.getValueCols().size()];
      i = 0;
      for (exprNodeDesc e : conf.getValueCols()) {
        valueEval[i++] = ExprNodeEvaluatorFactory.get(e);
      }

      partitionEval = new ExprNodeEvaluator[conf.getPartitionCols().size()];
      i = 0;
      for (exprNodeDesc e : conf.getPartitionCols()) {
        partitionEval[i++] = ExprNodeEvaluatorFactory.get(e);
      }

      tag = conf.getTag();
      tagByte[0] = (byte) tag;
      LOG.info("Using tag = " + tag);

      tableDesc keyTableDesc = conf.getKeySerializeInfo();
      keySerializer = (Serializer) keyTableDesc.getDeserializerClass()
          .newInstance();
      keySerializer.initialize(null, keyTableDesc.getProperties());
      keyIsText = keySerializer.getSerializedClass().equals(Text.class);

      tableDesc valueTableDesc = conf.getValueSerializeInfo();
      valueSerializer = (Serializer) valueTableDesc.getDeserializerClass()
          .newInstance();
      valueSerializer.initialize(null, valueTableDesc.getProperties());

      firstRow = true;
      initializeChildren(hconf);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  transient InspectableObject tempInspectableObject = new InspectableObject();
  transient HiveKey keyWritable = new HiveKey();
  transient Writable value;

  transient StructObjectInspector keyObjectInspector;
  transient StructObjectInspector valueObjectInspector;
  transient ObjectInspector[] partitionObjectInspectors;
  transient ObjectInspector[] keyPartitionObjectInspectors;

  transient Object[][] cachedKeys;
  transient Object[] cachedValues;
  transient List<List<Integer>> distinctColIndices;

  boolean firstRow;

  transient Random random;

  protected static StructObjectInspector initEvaluatorsAndReturnStruct(
      ExprNodeEvaluator[] evals, List<List<Integer>> distinctColIndices,
      List<String> outputColNames, int length, ObjectInspector rowInspector)
      throws HiveException {
    int inspectorLen = evals.length > length ? length + 1 : evals.length;
    List<ObjectInspector> sois = new ArrayList<ObjectInspector>(inspectorLen);

    ObjectInspector[] fieldObjectInspectors = initEvaluators(evals, 0, length,
        rowInspector);
    sois.addAll(Arrays.asList(fieldObjectInspectors));

    List<ObjectInspector> uois = new ArrayList<ObjectInspector>();
    for (List<Integer> distinctCols : distinctColIndices) {
      List<String> names = new ArrayList<String>();
      List<ObjectInspector> eois = new ArrayList<ObjectInspector>();
      int numExprs = 0;
      for (int i : distinctCols) {
        names.add(HiveConf.getColumnInternalName(numExprs));
        eois.add(evals[i].initialize(rowInspector));
        numExprs++;
      }
      uois.add(ObjectInspectorFactory.getStandardStructObjectInspector(names,
          eois));
    }
    if (!uois.isEmpty()) {
      UnionObjectInspector uoi = ObjectInspectorFactory
          .getStandardUnionObjectInspector(uois);
      sois.add(uoi);
    }
    return ObjectInspectorFactory.getStandardStructObjectInspector(
        outputColNames, sois);
  }

  public void process(Object row, int tag) throws HiveException {
    if (this.useNewGroupBy) {
      processNewGroupBy(row, tag);
      return;
    }
    try {
      ObjectInspector rowInspector = inputObjInspectors[tag];
      if (firstRow) {
        firstRow = false;
        keyObjectInspector = initEvaluatorsAndReturnStruct(keyEval,
            distinctColIndices, conf.getOutputKeyColumnNames(),
            numDistributionKeys, rowInspector);
        valueObjectInspector = initEvaluatorsAndReturnStruct(valueEval,
            conf.getOutputValueColumnNames(), rowInspector);
        partitionObjectInspectors = initEvaluators(partitionEval, rowInspector);
        keyPartitionObjectInspectors = initEvaluators(keyEval, rowInspector);

        int numKeys = numDistinctExprs > 0 ? numDistinctExprs : 1;
        int keyLen = numDistinctExprs > 0 ? numDistributionKeys + 1
            : numDistributionKeys;
        cachedKeys = new Object[numKeys][keyLen];
        cachedValues = new Object[valueEval.length];
      }

      int keyHashCode = 0;
      if (partitionEval.length == 0) {
        if (random == null) {
          random = new Random(12345);
        }
        keyHashCode = random.nextInt();
      } else {
        for (int i = 0; i < partitionEval.length; i++) {
          Object o = partitionEval[i].evaluate(row);
          keyHashCode = keyHashCode * 31
              + ObjectInspectorUtils.hashCode(o, partitionObjectInspectors[i]);
        }
      }
      keyWritable.setHashCode(keyHashCode);

      for (int i = 0; i < valueEval.length; i++) {
        cachedValues[i] = valueEval[i].evaluate(row);
      }
      value = valueSerializer.serialize(cachedValues, valueObjectInspector);

      Object[] distributionKeys = new Object[numDistributionKeys];
      for (int i = 0; i < numDistributionKeys; i++) {
        distributionKeys[i] = keyEval[i].evaluate(row);
      }

      int keyHashCodes[];
      keyHashCodes = new int[numDistinctExprs];
      if (numDistinctExprs > 0) {

        int keyHashCodeG = 0;
        for (int i = 0; i < numDistributionKeys; i++) {
          keyHashCodeG = keyHashCodeG
              * 31
              + ObjectInspectorUtils.hashCode(distributionKeys[i],
                  keyPartitionObjectInspectors[i]);
        }

        for (int i = 0; i < numDistinctExprs; i++) {
          System.arraycopy(distributionKeys, 0, cachedKeys[i], 0,
              numDistributionKeys);
          Object[] distinctParameters = new Object[distinctColIndices.get(i)
              .size()];
          keyHashCodes[i] = keyHashCodeG;
          for (int j = 0; j < distinctParameters.length; j++) {
            int dx = distinctColIndices.get(i).get(j);
            distinctParameters[j] = keyEval[dx].evaluate(row);
            keyHashCodes[i] = keyHashCodes[i]
                * 31
                + ObjectInspectorUtils.hashCode(distinctParameters[j],
                    keyPartitionObjectInspectors[dx]);
          }
          cachedKeys[i][numDistributionKeys] = new StandardUnion((byte) i,
              distinctParameters);
        }
      } else {
        System.arraycopy(distributionKeys, 0, cachedKeys[0], 0,
            numDistributionKeys);

      }
      for (int i = 0; i < cachedKeys.length; i++) {
        if (keyIsText) {
          Text key = (Text) keySerializer.serialize(cachedKeys[i],
              keyObjectInspector);
          if (tag == -1) {
            keyWritable.set(key.getBytes(), 0, key.getLength());
          } else {
            int keyLength = key.getLength();
            keyWritable.setSize(keyLength + 1);
            System
                .arraycopy(key.getBytes(), 0, keyWritable.get(), 0, keyLength);
            keyWritable.get()[keyLength] = tagByte[0];
          }
        } else {
          BytesWritable key = (BytesWritable) keySerializer.serialize(
              cachedKeys[i], keyObjectInspector);
          if (tag == -1) {
            keyWritable.set(key.getBytes(), 0, key.getLength());
          } else {
            int keyLength = key.getLength();
            keyWritable.setSize(keyLength + 1);
            System
                .arraycopy(key.getBytes(), 0, keyWritable.get(), 0, keyLength);
            keyWritable.get()[keyLength] = tagByte[0];
          }
        }
        if (numDistinctExprs > 0 && partitionByGbkeyanddistinctkey) {
          keyWritable.setHashCode(keyHashCodes[i]);
        } else
          keyWritable.setHashCode(keyHashCode);

        if (out != null) {
          out.collect(keyWritable, value);
        }
      }
    } catch (SerDeException e) {
      throw new HiveException(e);
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  public String getName() {
    return new String("RS");
  }

  transient protected boolean useNewGroupBy = false;

  transient protected ObjectInspector[] keyObjectInspectors;

  transient protected ExprNodeEvaluator[][] aggrParamFields;
  transient protected Object[][] cachedAggrParam;
  transient protected ObjectInspector[] aggrTagObjectInspectors;

  transient protected ExprNodeEvaluator distAggrParamEval = null;

  transient protected boolean mapAggrDone;
  transient protected boolean isFirstReduce = false;
  transient protected boolean containsfunctions = false;

  transient protected boolean lazyUOmode = false;
  transient protected ObjectInspector distAggrParamObjectInspector = null;

  private void initializeOpNewGroupBy(Configuration hconf) {
    this.useNewGroupBy = true;

    try {
      LOG.info("use new group by reduceop ");

      ObjectInspector rowInspector = inputObjInspectors[0];
      System.out.println("red:ioi:"
          + ObjectInspectorUtils.getStandardObjectInspector(rowInspector,
              ObjectInspectorCopyOption.WRITABLE));

      partitionByGbkeyanddistinctkey = conf.getPartitionByGbkeyanddistinctkey();
      mapAggrDone = conf.isMapAggrDone();
      isFirstReduce = conf.isFirstReduce();
      ArrayList<ArrayList<exprNodeDesc>> tag2AggrParamExpr = conf
          .getTag2AggrParamORValueExpr();

      containsfunctions = tag2AggrParamExpr != null
          || conf.getAggrPartExpr() != null;

      tableDesc keyTableDesc = conf.getKeySerializeInfo();
      keySerializer = (Serializer) keyTableDesc.getDeserializerClass()
          .newInstance();
      keySerializer.initialize(null, keyTableDesc.getProperties());
      keyIsText = keySerializer.getSerializedClass().equals(Text.class);

      tableDesc valueTableDesc = conf.getValueSerializeInfo();
      valueSerializer = (Serializer) valueTableDesc.getDeserializerClass()
          .newInstance();
      valueSerializer.initialize(null, valueTableDesc.getProperties());

      keyEval = new ExprNodeEvaluator[conf.getKeyCols().size()];
      int ii = 0;
      for (exprNodeDesc e : conf.getKeyCols()) {
        keyEval[ii++] = ExprNodeEvaluatorFactory.get(e);
      }

      if (containsfunctions) {
        if (!mapAggrDone) {
          aggrParamFields = new ExprNodeEvaluator[tag2AggrParamExpr.size()][];
          cachedAggrParam = new Object[tag2AggrParamExpr.size()][];
          ObjectInspector[] aggrTagObjectInspectorsStandard;
          aggrTagObjectInspectorsStandard = new ObjectInspector[tag2AggrParamExpr
              .size()];
          aggrTagObjectInspectors = new ObjectInspector[tag2AggrParamExpr
              .size()];
          List<ObjectInspector> uois = new ArrayList<ObjectInspector>();
          for (int tag = 0; tag < aggrParamFields.length; tag++) {
            ArrayList<exprNodeDesc> distexprs = tag2AggrParamExpr.get(tag);
            aggrParamFields[tag] = new ExprNodeEvaluator[distexprs.size()];
            cachedAggrParam[tag] = new Object[distexprs.size()];
            List<String> names = new ArrayList<String>();
            List<ObjectInspector> eois = new ArrayList<ObjectInspector>();
            List<ObjectInspector> eoisStandard = new ArrayList<ObjectInspector>();
            for (int j = 0; j < distexprs.size(); j++) {
              aggrParamFields[tag][j] = ExprNodeEvaluatorFactory.get(distexprs
                  .get(j));
              names.add(HiveConf.getColumnInternalName(j));
              ObjectInspector oi = aggrParamFields[tag][j]
                  .initialize(rowInspector);
              eois.add(oi);
              eoisStandard.add(ObjectInspectorUtils
                  .getStandardObjectInspector(oi));
              cachedAggrParam[tag][j] = null;
            }
            aggrTagObjectInspectors[tag] = ObjectInspectorFactory
                .getStandardStructObjectInspector(names, eois);
            aggrTagObjectInspectorsStandard[tag] = ObjectInspectorFactory
                .getStandardStructObjectInspector(names, eoisStandard);
            uois.add(aggrTagObjectInspectorsStandard[tag]);
          }

          distAggrParamObjectInspector = ObjectInspectorFactory
              .getStandardUnionObjectInspector(uois);
          System.out.println("red:distAggrParamObjectInspector:"
              + ObjectInspectorUtils.getStandardObjectInspector(
                  distAggrParamObjectInspector,
                  ObjectInspectorCopyOption.WRITABLE));

        } else {
          exprNodeDesc distAggrPartExpr = conf.getAggrPartExpr();

          distAggrParamEval = ExprNodeEvaluatorFactory.get(distAggrPartExpr);
          System.out.println("red:distAggrPartExpr:"
              + distAggrPartExpr.getExprString());
          distAggrParamObjectInspector = distAggrParamEval
              .initialize(rowInspector);
          if (distAggrParamObjectInspector instanceof LazyUnionObjectInspector) {
            lazyUOmode = true;
          }
          System.out.println("red:distAggrParamObjectInspector11:"
              + ObjectInspectorUtils.getStandardObjectInspector(
                  distAggrParamObjectInspector,
                  ObjectInspectorCopyOption.WRITABLE));
        }
      }

      List<ObjectInspector> rksois = new ArrayList<ObjectInspector>();
      List<ObjectInspector> rvsois = new ArrayList<ObjectInspector>();

      keyObjectInspectors = initEvaluators(keyEval, rowInspector);
      if (keyEval.length > 0)
        rksois.addAll(Arrays.asList(keyObjectInspectors));

      if (containsfunctions) {
        if (isFirstReduce) {
          rksois.add(distAggrParamObjectInspector);
        } else {
          rvsois.add(distAggrParamObjectInspector);
        }
      }

      keyObjectInspector = ObjectInspectorFactory
          .getStandardStructObjectInspector(conf.getOutputKeyColumnNames(),
              rksois);
      valueObjectInspector = ObjectInspectorFactory
          .getStandardStructObjectInspector(conf.getOutputValueColumnNames(),
              rvsois);

      distributionKeys = new Object[isFirstReduce ? keyEval.length + 1
          : keyEval.length];

      forwardValue = new Object[1];

      random = new Random(12345);
      initializeChildren(hconf);
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  Object[] distributionKeys = null;
  Object[] forwardValue = null;
  StandardUnion aggrUO = new StandardUnion();
  LazyUnion lazyAggrUO = null;
  Object valuenull = new ArrayList<Object>();

  private void processNewGroupBy(Object row, int tag) throws HiveException {

    try {

      int keyHashCode = 0;
      for (int i = 0; i < keyEval.length; i++) {
        distributionKeys[i] = keyEval[i].evaluate(row);
        keyHashCode = keyHashCode
            * 31
            + ObjectInspectorUtils.hashCode(distributionKeys[i],
                keyObjectInspectors[i]);
      }

      if (containsfunctions) {
        if (!mapAggrDone) {
          for (byte disttag = 0; disttag < this.aggrParamFields.length; disttag++) {
            for (int j = 0; j < aggrParamFields[disttag].length; j++) {
              cachedAggrParam[disttag][j] = this.aggrParamFields[disttag][j]
                  .evaluate(row);
            }
            if (disttag == 0 && aggrParamFields[disttag].length == 0) {
              continue;
            }
            aggrUO.setTag(disttag);
            aggrUO.setObject(ObjectInspectorUtils.copyToStandardObject(
                cachedAggrParam[disttag], aggrTagObjectInspectors[disttag]));
            forwardNoMapGby(keyHashCode);
          }
        } else if (isFirstReduce) {
          byte disttag = -1;
          if (lazyUOmode) {
            lazyAggrUO = (LazyUnion) distAggrParamEval.evaluate(row);
            forwardFirstReduceLazy(keyHashCode);
          } else {
            UnionObject uo = (UnionObject) distAggrParamEval.evaluate(row);
            disttag = uo.getTag();
            aggrUO.setTag(disttag);
            aggrUO.setObject(uo.getObject());
            forwardFirstReduce(keyHashCode);
          }
        } else {
          lazyAggrUO = (LazyUnion) distAggrParamEval.evaluate(row);
          forwardLazy(keyHashCode);
        }
      } else {
        forwardNoFunctions(keyHashCode);
      }

    } catch (SerDeException e) {
      throw new HiveException(e);
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private void forwardNoMapGby(int keyHashCode) throws IOException,
      SerDeException {

    int disttag = aggrUO.getTag();
    if (partitionByGbkeyanddistinctkey) {
      if (disttag != 0) {
        keyHashCode = keyHashCode * 31 + aggrUO.hashCode();
      } else {
        keyHashCode = keyHashCode * 31 + random.nextInt();
      }
    }

    keyWritable.setHashCode(keyHashCode);
    distributionKeys[distributionKeys.length - 1] = aggrUO;

    if (keyIsText) {
      Text key = (Text) keySerializer.serialize(distributionKeys,
          keyObjectInspector);
      keyWritable.set(key.getBytes(), 0, key.getLength());
    } else {
      BytesWritable key = (BytesWritable) keySerializer.serialize(
          distributionKeys, keyObjectInspector);
      keyWritable.set(key.getBytes(), 0, key.getLength());
    }

    forwardValue[0] = valuenull;
    value = valueSerializer.serialize(forwardValue, valueObjectInspector);

    if (out != null) {
      out.collect(keyWritable, value);
    }
  }

  @SuppressWarnings("unchecked")
  private void forwardFirstReduce(int keyHashCode) throws SerDeException,
      IOException {

    int disttag = aggrUO.getTag();

    if (partitionByGbkeyanddistinctkey) {
      if (disttag != 0) {
        keyHashCode = keyHashCode * 31 + aggrUO.hashCode();
      } else {
        keyHashCode = keyHashCode * 31 + random.nextInt();
      }
    }

    keyWritable.setHashCode(keyHashCode);
    distributionKeys[distributionKeys.length - 1] = aggrUO;

    if (keyIsText) {
      Text key = (Text) keySerializer.serialize(distributionKeys,
          keyObjectInspector);
      keyWritable.set(key.getBytes(), 0, key.getLength());
    } else {
      BytesWritable key = (BytesWritable) keySerializer.serialize(
          distributionKeys, keyObjectInspector);
      keyWritable.set(key.getBytes(), 0, key.getLength());
    }
    forwardValue[0] = valuenull;
    value = valueSerializer.serialize(forwardValue, valueObjectInspector);

    if (out != null) {
      out.collect(keyWritable, value);
    }
  }

  @SuppressWarnings("unchecked")
  private void forwardFirstReduceLazy(int keyHashCode) throws IOException,
      SerDeException {

    int disttag = lazyAggrUO.getTag();
    if (partitionByGbkeyanddistinctkey) {
      if (disttag != 0) {
        keyHashCode = keyHashCode
            * 31
            + ObjectInspectorUtils.hashCode(lazyAggrUO.getField(),
                distAggrParamObjectInspector);

      } else {
        keyHashCode = keyHashCode * 31 + random.nextInt();
      }
    }

    keyWritable.setHashCode(keyHashCode);
    distributionKeys[distributionKeys.length - 1] = lazyAggrUO;

    if (keyIsText) {
      Text key = (Text) keySerializer.serialize(distributionKeys,
          keyObjectInspector);
      keyWritable.set(key.getBytes(), 0, key.getLength());
    } else {
      BytesWritable key = (BytesWritable) keySerializer.serialize(
          distributionKeys, keyObjectInspector);
      keyWritable.set(key.getBytes(), 0, key.getLength());
    }

    forwardValue[0] = valuenull;
    value = valueSerializer.serialize(forwardValue, valueObjectInspector);

    if (out != null) {
      out.collect(keyWritable, value);
    }
  }

  @SuppressWarnings("unchecked")
  private void forwardLazy(int keyHashCode) throws SerDeException, IOException {

    keyWritable.setHashCode(keyHashCode);

    if (keyIsText) {
      Text key = (Text) keySerializer.serialize(distributionKeys,
          keyObjectInspector);
      keyWritable.set(key.getBytes(), 0, key.getLength());
    } else {
      BytesWritable key = (BytesWritable) keySerializer.serialize(
          distributionKeys, keyObjectInspector);
      keyWritable.set(key.getBytes(), 0, key.getLength());
    }

    forwardValue[0] = lazyAggrUO;
    value = valueSerializer.serialize(forwardValue, valueObjectInspector);

    if (out != null) {
      out.collect(keyWritable, value);
    }
  }

  @SuppressWarnings("unchecked")
  private void forwardNoFunctions(int keyHashCode) throws SerDeException,
      IOException {

    keyWritable.setHashCode(keyHashCode);

    if (keyIsText) {
      Text key = (Text) keySerializer.serialize(distributionKeys,
          keyObjectInspector);
      keyWritable.set(key.getBytes(), 0, key.getLength());
    } else {
      BytesWritable key = (BytesWritable) keySerializer.serialize(
          distributionKeys, keyObjectInspector);
      keyWritable.set(key.getBytes(), 0, key.getLength());
    }
    value = valueSerializer.serialize(null, valueObjectInspector);

    if (out != null) {
      out.collect(keyWritable, value);
    }
  }

}
