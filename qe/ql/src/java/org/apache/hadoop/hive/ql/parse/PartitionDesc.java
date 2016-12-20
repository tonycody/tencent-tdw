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
package org.apache.hadoop.hive.ql.parse;

import java.util.*;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

public class PartitionDesc {

  private String dbName;
  private String tableName;
  private Boolean isSub;
  private PartitionType partitionType;
  private FieldSchema partColumn;
  private LinkedHashMap<String, List<String>> partitionSpaces;
  private PartitionDesc subPartition;

  PartitionDesc() {

  }

  public PartitionDesc(PartitionType pt, FieldSchema pc,
      LinkedHashMap<String, List<String>> ps, PartitionDesc subpd) {
    partColumn = pc;
    partitionType = pt;
    partitionSpaces = ps;
    subPartition = subpd;

  }

  public PartitionDesc(String dbname, String tblname, PartitionType pt,
      FieldSchema pc, LinkedHashMap<String, List<String>> ps,
      PartitionDesc subpd) {
    dbName = dbname;
    tableName = tblname;
    partColumn = pc;
    partitionType = pt;
    partitionSpaces = ps;
    subPartition = subpd;

  }

  public PartitionType getPartitionType() {
    return partitionType;
  }

  public void setPartitionType(PartitionType pt) {

    partitionType = pt;
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public LinkedHashMap<String, List<String>> getPartitionSpaces() {
    return partitionSpaces;
  }

  public void setPartitionSpaces(
      LinkedHashMap<String, List<String>> partitionSpaces) {
    this.partitionSpaces = partitionSpaces;
  }

  public PartitionDesc getSubPartition() {
    return subPartition;
  }

  public void setSubPartition(PartitionDesc subpd) {
    subPartition = subpd;
  }

  public FieldSchema getPartColumn() {
    return partColumn;
  }

  public void setPartColumn(FieldSchema partColumn) {
    this.partColumn = partColumn;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public Boolean getIsSub() {
    return isSub;
  }

  public void setIsSub(Boolean isSub) {
    this.isSub = isSub;
  }

}
