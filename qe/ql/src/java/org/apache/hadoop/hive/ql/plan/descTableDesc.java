/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;

@explain(displayName = "Describe Table")
public class descTableDesc extends ddlDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  String tableName;
  String dbName;
  String partName;
  String pattern;
  Path resFile;
  boolean isExt;

  private final String table = "describe";

  private final String schema = "col_name,data_type,comment#string:string:string";

  public String getTable() {
    return table;
  }

  public String getSchema() {
    return schema;
  }

  public descTableDesc(Path resFile, String tableName, String partName,
      String pattern, boolean isExt) {
    this.isExt = isExt;
    this.partName = partName;
    this.pattern = pattern;
    this.resFile = resFile;
    this.tableName = tableName;
    this.dbName = null;
  }

  public descTableDesc(Path resFile, String dbname, String tableName,
      String partName, String pattern, boolean isExt) {
    this.isExt = isExt;
    this.partName = partName;
    this.pattern = pattern;
    this.resFile = resFile;
    this.tableName = tableName;
    this.dbName = dbname;
  }

  public boolean isExt() {
    return isExt;
  }

  public void setExt(boolean isExt) {
    this.isExt = isExt;
  }

  @explain(displayName = "table")
  public String getTableName() {
    return tableName;
  }

  public String getDBName() {
    return dbName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void setDBName(String dbname) {
    this.dbName = dbname;
  }

  public String getPattern() {
    return pattern;
  }

  public void setPattern(String pattern) {
    this.pattern = pattern;
  }

  public String getPartName() {
    return partName;
  }

  public void setPartName(String partName) {
    this.partName = partName;
  }

  public Path getResFile() {
    return resFile;
  }

  @explain(displayName = "result file", normalExplain = false)
  public String getResFileString() {
    return getResFile().getName();
  }

  public void setResFile(Path resFile) {
    this.resFile = resFile;
  }
}
