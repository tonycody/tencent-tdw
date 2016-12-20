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

public class addDefaultPartitionDesc extends ddlDesc implements Serializable {
  private String dbName = null;
  private String tblName = null;
  private boolean isSub = false;

  public addDefaultPartitionDesc(String dbName, String tblName, boolean isSub) {
    super();
    this.dbName = dbName;
    this.tblName = tblName;
    this.isSub = isSub;
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public String getTblName() {
    return tblName;
  }

  public void setTblName(String tblName) {
    this.tblName = tblName;
  }

  public boolean isSub() {
    return isSub;
  }

  public void setSub(boolean isSub) {
    this.isSub = isSub;
  }

}
