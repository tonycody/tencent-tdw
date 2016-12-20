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
import java.util.Properties;

import org.apache.hadoop.fs.Path;

public class execExtSQLDesc extends ddlDesc implements Serializable {

  public String getSchema() {
    return schema;
  }

  private static final long serialVersionUID = 1L;

  private String sql = null;
  private String dbtype = null;
  private Properties prop = null;
  private final String schema = "result#string";
  Path resFile;

  public execExtSQLDesc(String sql, String dbtype, Properties prop, Path path) {
    super();
    this.sql = sql;
    this.dbtype = dbtype;
    this.prop = prop;
    this.resFile = path;
  }

  public Path getResFile() {
    return resFile;
  }

  public void setResFile(Path resFile) {
    this.resFile = resFile;
  }

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  public String getDbtype() {
    return dbtype;
  }

  public void setDbtype(String dbtype) {
    this.dbtype = dbtype;
  }

  public Properties getProp() {
    return prop;
  }

  public void setProp(Properties prop) {
    this.prop = prop;
  }

}
