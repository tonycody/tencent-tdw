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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

@explain(displayName = "RowExtend Operator")
public class rowExtendDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  private ArrayList<Integer> groupingColOffs;
  private java.util.ArrayList<org.apache.hadoop.hive.ql.plan.exprNodeDesc> groupByKeys;
  private java.util.ArrayList<java.lang.String> outputColumnNames;
  private ArrayList<Long> tagids;
  private HashMap<Integer, Integer> gbkIdx2compactIdx;
  private ArrayList<exprNodeDesc> values;

  public rowExtendDesc() {
  }

  public rowExtendDesc(
      ArrayList<Integer> groupingColOffs,
      final java.util.ArrayList<org.apache.hadoop.hive.ql.plan.exprNodeDesc> groupByKeys,
      ArrayList<Long> tagids, HashMap<Integer, Integer> gbkIdx2compactIdx,
      ArrayList<exprNodeDesc> values,
      final java.util.ArrayList<java.lang.String> outputColumnNames) {
    this.groupingColOffs = groupingColOffs;
    this.groupByKeys = groupByKeys;
    this.tagids = tagids;
    this.gbkIdx2compactIdx = gbkIdx2compactIdx;
    this.values = values;
    this.outputColumnNames = outputColumnNames;
  }

  @explain(displayName = "outputColumnNames")
  public java.util.ArrayList<java.lang.String> getOutputColumnNames() {
    return outputColumnNames;
  }

  public void setOutputColumnNames(
      java.util.ArrayList<java.lang.String> outputColumnNames) {
    this.outputColumnNames = outputColumnNames;
  }

  @explain(displayName = "expressions")
  public java.util.ArrayList<org.apache.hadoop.hive.ql.plan.exprNodeDesc> getGroupByKeys() {
    return groupByKeys;
  }

  public void setGroupByKeys(
      java.util.ArrayList<org.apache.hadoop.hive.ql.plan.exprNodeDesc> groupByKeys) {
    this.groupByKeys = groupByKeys;
  }

  public ArrayList<Long> getTagids() {
    return tagids;
  }

  public void setTagids(ArrayList<Long> tagids) {
    this.tagids = tagids;
  }

  @explain(displayName = "values")
  public ArrayList<exprNodeDesc> getValues() {
    return values;
  }

  public void setValues(ArrayList<exprNodeDesc> values) {
    this.values = values;
  }

  public void setGroupingColOffs(ArrayList<Integer> groupingColOffs) {
    this.groupingColOffs = groupingColOffs;
  }

  public ArrayList<Integer> getGroupingColOffs() {
    return groupingColOffs;
  }

  public void setGbkIdx2compactIdx(HashMap<Integer, Integer> gbkIdx2compactIdx) {
    this.gbkIdx2compactIdx = gbkIdx2compactIdx;
  }

  public HashMap<Integer, Integer> getGbkIdx2compactIdx() {
    return gbkIdx2compactIdx;
  }

}
