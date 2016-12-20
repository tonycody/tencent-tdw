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
import java.util.Map;

import org.apache.hadoop.hive.ql.parse.PartitionDesc;

public class AddPartitionDesc extends ddlDesc implements Serializable {

  String tableName;
  String dbName;
  Boolean isSubPartition;
  PartitionDesc partDesc;

  public AddPartitionDesc(String dbName, String tableName, PartitionDesc pd,
      Boolean isSub) {
    super();
    this.dbName = dbName;
    this.tableName = tableName;
    this.partDesc = pd;
    this.isSubPartition = isSub;
  }

  public Boolean getIsSubPartition() {
    return isSubPartition;
  }

  public void setIsSubPartition(Boolean isSubPartition) {
    this.isSubPartition = isSubPartition;
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public PartitionDesc getPartDesc() {
    return partDesc;
  }

  public void setPartDesc(PartitionDesc partDesc) {
    this.partDesc = partDesc;
  }

}
