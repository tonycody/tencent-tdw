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

public class truncatePartitionDesc extends ddlDesc implements Serializable {
  private String dbName;
  private String tblName;
  private String priPartName;
  private String subPartName;

  public truncatePartitionDesc(String dbName, String tblName,
      String priPartName, String subPartNmae) {
    super();
    this.dbName = dbName;
    this.tblName = tblName;
    this.priPartName = priPartName;
    this.subPartName = subPartNmae;
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

  public String getPriPartName() {
    return priPartName;
  }

  public void setPriPartName(String priPartName) {
    this.priPartName = priPartName;
  }

  public String getSubPartName() {
    return subPartName;
  }

  public void setSubPartName(String subPartNmae) {
    this.subPartName = subPartNmae;
  }

}
