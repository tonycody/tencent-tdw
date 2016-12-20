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

public class sampleDesc implements Serializable {
  static final long serialVersionUID = 1L;

  java.util.ArrayList<org.apache.hadoop.hive.ql.plan.exprNodeDesc> colList;
  java.util.ArrayList<java.lang.String> outputColumnNames;
  ArrayList<String> _als;
  ArrayList<Integer> _switch_;
  boolean selectStar;
  boolean selStarNoCompute;

  int estimatedSplitSize;
  int num_sample_splits;
  int num_sample_records;
  float samplingFactor;
  String tableName;

  public sampleDesc() {
  }

  public ArrayList<Integer> getSwitch() {
    return this._switch_;
  }

  public void setSwitch(ArrayList<Integer> _copy) {
    this._switch_ = _copy;
  }

  public ArrayList<String> getTableFieldNames() {
    return this._als;
  }

  public void setTableFieldNames(ArrayList<String> _copy) {
    this._als = _copy;
  }

  @explain(displayName = "expressions")
  public java.util.ArrayList<org.apache.hadoop.hive.ql.plan.exprNodeDesc> getColList() {
    return this.colList;
  }

  public void setColList(
      final java.util.ArrayList<org.apache.hadoop.hive.ql.plan.exprNodeDesc> colList) {
    this.colList = colList;
  }

  @explain(displayName = "outputColumnNames")
  public java.util.ArrayList<java.lang.String> getOutputColumnNames() {
    return outputColumnNames;
  }

  public void setOutputColumnNames(
      java.util.ArrayList<java.lang.String> outputColumnNames) {
    this.outputColumnNames = outputColumnNames;
  }

  public void setTableName(String tblName) {
    this.tableName = tblName;
  }

  public String getTableName() {
    return this.tableName;
  }

  public void setNumSampleRecords(int _para) {
    this.num_sample_records = _para;
  }

  public int getNumSampleRecords() {
    return this.num_sample_records;
  }

  public void setNumSampleSplits(int _para) {
    this.num_sample_splits = _para;
  }

  public int getNumSampleSplits() {
    return this.num_sample_splits;
  }

  public void setEstimatedSplitSize(int para) {
    this.estimatedSplitSize = para;
  }

  public int getEstimatedSplitSize() {
    return this.estimatedSplitSize;
  }

  public void setSamplingFactor(float _para) {
    this.samplingFactor = _para;
  }

  public float getSamplingFactor() {
    return this.samplingFactor;
  }

}
