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
import org.apache.hadoop.fs.Path;

@explain(displayName = "Show Indexs")
public class showIndexDesc extends ddlDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  public static enum showIndexTypes {
    SHOWTABLEINDEX, SHOWALLINDEX
  };

  String dbName;
  String tblName;
  showIndexTypes op;

  Path resFile;

  public showIndexDesc(String dbName, String tblName, showIndexTypes op) {
    this.tblName = tblName;
    this.dbName = dbName;
    this.op = op;
  }

  @explain(displayName = "table")
  public String getTblName() {
    return tblName;
  }

  public void setTblName(String tblName) {
    this.tblName = tblName;
  }

  @explain(displayName = "db")
  public String getDBName() {
    return dbName;
  }

  public void setDBName(String dbName) {
    this.dbName = dbName;
  }

  public showIndexTypes getOP() {
    return op;
  }
}
