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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@explain(displayName = "Reduce Output Operator")
public class reduceSinkDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  private java.util.ArrayList<exprNodeDesc> keyCols;
  private java.util.ArrayList<java.lang.String> outputKeyColumnNames;

  private java.util.ArrayList<exprNodeDesc> valueCols;
  private java.util.ArrayList<java.lang.String> outputValueColumnNames;
  private List<List<Integer>> distinctColumnIndices;

  private tableDesc keySerializeInfo;

  private tableDesc valueSerializeInfo;

  private int tag;
  private int numDistributionKeys;

  private boolean partitionByGbkeyanddistinctkey;

  private java.util.ArrayList<exprNodeDesc> partitionCols;

  private int numReducers;

  private int reduceNumFactor = 1;

  public reduceSinkDesc() {
  }

  public reduceSinkDesc(java.util.ArrayList<exprNodeDesc> keyCols,
      int numDistributionKeys, java.util.ArrayList<exprNodeDesc> valueCols,
      java.util.ArrayList<java.lang.String> outputKeyColumnNames,
      List<List<Integer>> distinctColumnIndices,
      java.util.ArrayList<java.lang.String> outputValueColumnNames, int tag,
      java.util.ArrayList<exprNodeDesc> partitionCols, int numReducers,
      final tableDesc keySerializeInfo, final tableDesc valueSerializeInfo,
      boolean partitionByGbkeyanddistinctkey) {
    this.keyCols = keyCols;
    this.numDistributionKeys = numDistributionKeys;
    this.valueCols = valueCols;
    this.outputKeyColumnNames = outputKeyColumnNames;
    this.outputValueColumnNames = outputValueColumnNames;
    this.tag = tag;
    this.numReducers = numReducers;
    this.partitionCols = partitionCols;
    this.keySerializeInfo = keySerializeInfo;
    this.valueSerializeInfo = valueSerializeInfo;
    this.distinctColumnIndices = distinctColumnIndices;
    this.partitionByGbkeyanddistinctkey = partitionByGbkeyanddistinctkey;
  }

  @explain(displayName = "output key names")
  public java.util.ArrayList<java.lang.String> getOutputKeyColumnNames() {
    return outputKeyColumnNames;
  }

  public void setOutputKeyColumnNames(
      java.util.ArrayList<java.lang.String> outputKeyColumnNames) {
    this.outputKeyColumnNames = outputKeyColumnNames;
  }

  @explain(displayName = "output value names")
  public java.util.ArrayList<java.lang.String> getOutputValueColumnNames() {
    return outputValueColumnNames;
  }

  public void setOutputValueColumnNames(
      java.util.ArrayList<java.lang.String> outputValueColumnNames) {
    this.outputValueColumnNames = outputValueColumnNames;
  }

  @explain(displayName = "key expressions")
  public java.util.ArrayList<exprNodeDesc> getKeyCols() {
    return this.keyCols;
  }

  public void setKeyCols(final java.util.ArrayList<exprNodeDesc> keyCols) {
    this.keyCols = keyCols;
  }

  public int getNumDistributionKeys() {
    return this.numDistributionKeys;
  }

  public void setNumDistributionKeys(int numKeys) {
    this.numDistributionKeys = numKeys;
  }

  @explain(displayName = "value expressions")
  public java.util.ArrayList<exprNodeDesc> getValueCols() {
    return this.valueCols;
  }

  public void setValueCols(final java.util.ArrayList<exprNodeDesc> valueCols) {
    this.valueCols = valueCols;
  }

  @explain(displayName = "Map-reduce partition columns")
  public java.util.ArrayList<exprNodeDesc> getPartitionCols() {
    return this.partitionCols;
  }

  public void setPartitionCols(
      final java.util.ArrayList<exprNodeDesc> partitionCols) {
    this.partitionCols = partitionCols;
  }

  @explain(displayName = "tag")
  public int getTag() {
    return this.tag;
  }

  public void setTag(int tag) {
    this.tag = tag;
  }

  public int getNumReducers() {
    return this.numReducers;
  }

  public void setNumReducers(int numReducers) {
    this.numReducers = numReducers;
  }

  @explain(displayName = "key serialize infos")
  public tableDesc getKeySerializeInfo() {
    return keySerializeInfo;
  }

  public void setKeySerializeInfo(tableDesc keySerializeInfo) {
    this.keySerializeInfo = keySerializeInfo;
  }

  public tableDesc getValueSerializeInfo() {
    return valueSerializeInfo;
  }

  public void setValueSerializeInfo(tableDesc valueSerializeInfo) {
    this.valueSerializeInfo = valueSerializeInfo;
  }

  @explain(displayName = "sort order")
  public String getOrder() {
    return keySerializeInfo.getProperties().getProperty(
        org.apache.hadoop.hive.serde.Constants.SERIALIZATION_SORT_ORDER);
  }

  public List<List<Integer>> getDistinctColumnIndices() {
    return distinctColumnIndices;
  }

  public void setDistinctColumnIndices(List<List<Integer>> distinctColumnIndices) {
    this.distinctColumnIndices = distinctColumnIndices;
  }

  public boolean getPartitionByGbkeyanddistinctkey() {
    return this.partitionByGbkeyanddistinctkey;
  }

  public void setPartitionByGbkeyanddistinctkey(
      boolean partitionByGbkeyanddistinctkey) {
    this.partitionByGbkeyanddistinctkey = partitionByGbkeyanddistinctkey;
  }

  private boolean usenewgroupby = false;
  private boolean outputdistinctaggrparam = false;
  private boolean outputvalues = false;
  private exprNodeDesc aggrPartExpr = null;
  private boolean mapAggrDone = false;

  public reduceSinkDesc(java.util.ArrayList<exprNodeDesc> reduceKeys,
      java.util.ArrayList<exprNodeDesc> reduceValues,
      java.util.ArrayList<String> outputKeyColumnNames,
      java.util.ArrayList<String> outputValueColumnNames, int tag,
      int numReducers, boolean mapAggrDone, tableDesc keyTable,
      tableDesc valueTable, boolean outputdistinctaggrparam,
      boolean partitionByGbkeyanddistinctkey, boolean outputvalues,
      exprNodeDesc distAggrPartExpr) {
    this.setUsenewgroupby(true);
    this.keyCols = reduceKeys;
    this.numDistributionKeys = keyCols.size();
    this.valueCols = reduceValues;
    this.outputKeyColumnNames = outputKeyColumnNames;
    this.outputValueColumnNames = outputValueColumnNames;
    this.tag = tag;
    this.numReducers = numReducers;
    this.partitionCols = keyCols;
    this.keySerializeInfo = keyTable;
    this.valueSerializeInfo = valueTable;
    this.distinctColumnIndices = null;
    this.partitionByGbkeyanddistinctkey = partitionByGbkeyanddistinctkey;
    this.setOutputdistinctaggrparam(outputdistinctaggrparam);
    this.setOutputvalues(outputvalues);
    this.setAggrPartExpr(distAggrPartExpr);
    this.setMapAggrDone(mapAggrDone);
  }

  private boolean isFirstReduce = false;
  private ArrayList<ArrayList<exprNodeDesc>> tag2AggrParamORValueExpr = null;

  public reduceSinkDesc(ArrayList<exprNodeDesc> reduceKeys,
      ArrayList<exprNodeDesc> reduceValues, ArrayList<String> outputKeyCols,
      ArrayList<String> outputValCols, int tag, int numReducers,
      tableDesc keyTable, tableDesc valueTable, boolean mapAggrDone,
      boolean partitionByGbkeyanddistinctkey, boolean isFirstReduce,
      exprNodeDesc aggrPartExpr,
      ArrayList<ArrayList<exprNodeDesc>> tag2AggrParamORValueExpr) {
    this.setUsenewgroupby(true);
    this.setKeyCols(reduceKeys);
    this.setValueCols(reduceValues);
    this.setOutputKeyColumnNames(outputKeyCols);
    this.setOutputValueColumnNames(outputValCols);
    this.setTag(tag);
    this.setNumReducers(numReducers);
    this.setKeySerializeInfo(keyTable);
    this.setValueSerializeInfo(valueTable);
    this.setMapAggrDone(mapAggrDone);
    this.setPartitionByGbkeyanddistinctkey(partitionByGbkeyanddistinctkey);
    this.setFirstReduce(isFirstReduce);
    this.setAggrPartExpr(aggrPartExpr);
    this.setTag2AggrParamORValueExpr(tag2AggrParamORValueExpr);
  }

  public void setUsenewgroupby(boolean usenewgroupby) {
    this.usenewgroupby = usenewgroupby;
  }

  public boolean isUsenewgroupby() {
    return usenewgroupby;
  }

  public void setOutputdistinctaggrparam(boolean outputdistinctaggrparam) {
    this.outputdistinctaggrparam = outputdistinctaggrparam;
  }

  public boolean isOutputdistinctaggrparam() {
    return outputdistinctaggrparam;
  }

  public void setOutputvalues(boolean outputvalues) {
    this.outputvalues = outputvalues;
  }

  public boolean isOutputvalues() {
    return outputvalues;
  }

  public void setAggrPartExpr(exprNodeDesc distAggrPartExpr) {
    this.aggrPartExpr = distAggrPartExpr;
  }

  public exprNodeDesc getAggrPartExpr() {
    return aggrPartExpr;
  }

  public void setMapAggrDone(boolean mapAggrDone) {
    this.mapAggrDone = mapAggrDone;
  }

  public boolean isMapAggrDone() {
    return mapAggrDone;
  }

  public void setFirstReduce(boolean isFirstReduce) {
    this.isFirstReduce = isFirstReduce;
  }

  public boolean isFirstReduce() {
    return isFirstReduce;
  }

  public void setTag2AggrParamORValueExpr(
      ArrayList<ArrayList<exprNodeDesc>> tag2AggrParamORValueExpr) {
    this.tag2AggrParamORValueExpr = tag2AggrParamORValueExpr;
  }

  public ArrayList<ArrayList<exprNodeDesc>> getTag2AggrParamORValueExpr() {
    return tag2AggrParamORValueExpr;
  }

  public void setReduceNumFactor(int reduceNumFactor) {
    this.reduceNumFactor = reduceNumFactor;
  }

  public int getReduceNumFactor() {
    return reduceNumFactor;
  }

}
