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
import java.util.HashMap;
import java.util.Vector;

import org.apache.hadoop.fs.Path;

public class showDatabaseSizeDesc extends ddlDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  Path resFile;
  String dbname;
  boolean isextended = false;

  private final String schema = "table_name,size#string:string";

  public String getSchema() {
    return schema;
  }

  public String getDBName() {
    return dbname;
  }

  public boolean getExtended() {
    return isextended;
  }

  public showDatabaseSizeDesc(Path resFile, String db) {
    this.resFile = resFile;
    dbname = db;
  }

  public showDatabaseSizeDesc(Path resFile, String db, boolean ext) {
    this.resFile = resFile;
    dbname = db;
    isextended = ext;
  }

  public Path getResFile() {
    return resFile;
  }

  public String getResFileString() {
    return getResFile().getName();
  }

  public void setResFile(Path resFile) {
    this.resFile = resFile;
  }
}
