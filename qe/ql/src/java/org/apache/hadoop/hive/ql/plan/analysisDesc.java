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

@explain(displayName = "Analysis Operator")
public class analysisDesc implements Serializable {

  private static final long serialVersionUID = 6519546919272066933L;

  private ArrayList<String> outputColumnNames;
  private ArrayList<exprNodeDesc> partitionByKeys;
  private ArrayList<exprNodeDesc> orderByKeys;
  private ArrayList<analysisEvaluatorDesc> analysises;
  private ArrayList<exprNodeDesc> otherColumns;

  private boolean hasDistinct;

  public analysisDesc() {
  }

  public analysisDesc(final ArrayList<String> outputColumnNames,
      final ArrayList<exprNodeDesc> partitionByKeys,
      final ArrayList<exprNodeDesc> orderByKeys, final boolean hasDistinct,
      final ArrayList<analysisEvaluatorDesc> analysises,
      final ArrayList<exprNodeDesc> otherColumns) {

    this.outputColumnNames = outputColumnNames;
    this.partitionByKeys = partitionByKeys;
    this.orderByKeys = orderByKeys;
    this.hasDistinct = hasDistinct;
    this.analysises = analysises;
    this.otherColumns = otherColumns;
  }

  public void setPartitionByKeys(final ArrayList<exprNodeDesc> partitionByKeys) {
    this.partitionByKeys = partitionByKeys;
  }

  @explain(displayName = "PartitionByKeys")
  public ArrayList<exprNodeDesc> getPartitionByKeys() {
    return this.partitionByKeys;
  }

  public void setOrderByKeys(final ArrayList<exprNodeDesc> orderByKeys) {
    this.orderByKeys = orderByKeys;
  }

  @explain(displayName = "OrderByKeys")
  public ArrayList<exprNodeDesc> getOrderByKeys() {
    return this.orderByKeys;
  }

  public void setOtherColumns(final ArrayList<exprNodeDesc> otherColumns) {
    this.otherColumns = otherColumns;
  }

  @explain(displayName = "OtherColumns")
  public ArrayList<exprNodeDesc> getOtherColumns() {
    return this.otherColumns;
  }

  @explain(displayName = "Distinct")
  public boolean getDistinct() {
    return this.hasDistinct;
  }

  public void setDistinct(final boolean hasDistinct) {
    this.hasDistinct = hasDistinct;
  }

  @explain(displayName = "Analysises")
  public ArrayList<analysisEvaluatorDesc> getAnalysises() {
    return this.analysises;
  }

  public void setAnalysises(final ArrayList<analysisEvaluatorDesc> analysises) {
    this.analysises = analysises;
  }

  @explain(displayName = "OutputColumnNames")
  public ArrayList<String> getOutputColumnNames() {
    return outputColumnNames;
  }

  public void setOutputColumnNames(final ArrayList<String> outputColumnNames) {
    this.outputColumnNames = outputColumnNames;
  }

  @explain(displayName = "expr")
  public String getExprString() {
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < analysises.size() - 1; i++) {
      sb.append(analysises.get(i).getExprString());
      sb.append(", ");
    }

    sb.append(analysises.get(analysises.size() - 1).getExprString());

    return sb.toString();
  }
}
