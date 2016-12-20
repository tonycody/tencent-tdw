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

public class histogramDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  String dirNameString;
  String _tableName_;
  ArrayList<String> _as;
  ArrayList<Integer> _switch_;
  java.util.ArrayList<org.apache.hadoop.hive.ql.plan.exprNodeDesc> colList;

  final int windowSize = 256;

  public histogramDesc() {
  }

  public int getWindowSize() {
    return windowSize;
  }

  public void setDirName(String para) {
    this.dirNameString = para;
  }

  public String getDirName() {
    return dirNameString;
  }

  public void setTableName(String _para) {
    this._tableName_ = _para;
  }

  public String getTableName() {
    return _tableName_;
  }

  public void setFieldNames(java.util.ArrayList<String> _para) {
    this._as = _para;
  }

  public java.util.ArrayList<String> getFieldNames() {
    return this._as;
  }

  public void setSwitch(java.util.ArrayList<Integer> _para) {
    this._switch_ = _para;
  }

  public java.util.ArrayList<Integer> getSwitch() {
    return this._switch_;
  }

  public java.util.ArrayList<org.apache.hadoop.hive.ql.plan.exprNodeDesc> getColList() {
    return this.colList;
  }

  public void setColList(
      final java.util.ArrayList<org.apache.hadoop.hive.ql.plan.exprNodeDesc> colList) {
    this.colList = colList;
  }

}
