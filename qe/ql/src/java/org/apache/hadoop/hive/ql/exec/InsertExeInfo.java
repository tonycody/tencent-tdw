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

public class InsertExeInfo {
  private long fsSuccessNum = 0;
  private long fsRejectNum = 0;
  private String queryID = null;
  private String tblName = null;
  private boolean isMultiInsert = false;

  public InsertExeInfo() {

  }

  public void setFsSuccessNum(long num) {
    fsSuccessNum = num;
  }

  public void setFsRejectNum(long num) {
    fsRejectNum = num;
  }

  public void setQueryID(String id) {
    queryID = id;
  }

  public long getFsSuccessNum() {
    return fsSuccessNum;
  }

  public long getFsRejectNum() {
    return fsRejectNum;
  }

  public String getQueryID() {
    return queryID;
  }

  public void setDestTable(String name) {
    tblName = name;
  }

  public String getDestTable() {
    return tblName;
  }

  public boolean getIsMultiInsert() {
    return isMultiInsert;
  }

  public void setIsMultiInsert(boolean is) {
    isMultiInsert = is;
  }
}
