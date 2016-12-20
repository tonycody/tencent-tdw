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

public class updateTableDesc implements Serializable {

  private static final long serialVersionUID = 1L;
  private String deletepath;
  private String tmpdir_new;
  private String tmpdir_old;
  private String dbname;
  private String tablename;

  public updateTableDesc(String dbname, String tablename, String tablepath,
      String tmpdirnew, String tmpdirold) {
    this.dbname = dbname;
    this.tablename = tablename;
    this.deletepath = tablepath;
    this.tmpdir_new = tmpdirnew;
    this.tmpdir_old = tmpdirold;
  }

  public void setTablepath(String deletepath) {
    this.deletepath = deletepath;
  }

  public String getDeletepath() {
    return deletepath;
  }

  public void setTmpdir_new(String newdir) {
    this.tmpdir_new = newdir;
  }

  public String getTmpdir_new() {
    return tmpdir_new;
  }

  public void setTmpdir_old(String olddir) {
    this.tmpdir_old = olddir;
  }

  public String getTmpdir_old() {
    return tmpdir_old;
  }

  @explain(displayName = "Database")
  public String getDbname() {
    return dbname;
  }

  public void setDbname(String dbname) {
    this.dbname = dbname;
  }

  @explain(displayName = "Table")
  public String getTablename() {
    return tablename;
  }

  public void setTablename(String tablename) {
    this.tablename = tablename;
  }
}
