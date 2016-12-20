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

package org.apache.hadoop.hive.jdbc;

import java.sql.SQLException;

public class JdbcTable {
  private String tableCatalog;
  private String tableName;
  private String type;
  private String comment;
  private boolean isExt;

  public JdbcTable(String tableCatalog, String tableName, String type,
      String comment, boolean isExt) {
    this.tableCatalog = tableCatalog;
    this.tableName = tableName;
    this.type = type;
    this.comment = comment;
    this.isExt = isExt;
  }

  public JdbcTable(String tableCatalog, String tableName, String type,
      String comment) {
    this.tableCatalog = tableCatalog;
    this.tableName = tableName;
    this.type = type;
    this.comment = comment;
  }

  public JdbcTable(String tableCatalog, String tableName, String comment) {
    this.tableCatalog = tableCatalog;
    this.tableName = tableName;
    this.comment = comment;
  }

  public boolean isExt() {
    return isExt;
  }

  public void setExt(boolean isExt) {
    this.isExt = isExt;
  }

  public String getTableCatalog() {
    return tableCatalog;
  }

  public String getTableName() {
    return tableName;
  }

  public String getType() {
    return type;
  }

  public String getSqlTableType() throws SQLException {
    return HiveDatabaseMetaData.toJdbcTableType(type);
  }

  public String getComment() {
    return comment;
  }
}
