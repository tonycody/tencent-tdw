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

package org.apache.hadoop.hive.metastore.model;

import java.util.Map;

public class MTable {

  private String tableName;
  private MDatabase database;
  private MStorageDescriptor sd;
  private String owner;
  private int createTime;
  private int lastAccessTime;
  private int retention;

  private MPartition priPartition;
  private MPartition subPartition;

  private Map<String, String> parameters;

  private String viewOriginalText;
  private String viewExpandedText;
  private String tableType;
  private String vtables;

  public String getVtables() {
    return vtables;
  }

  public void setVtables(String vtables) {
    this.vtables = vtables;
  }

  public MTable() {
  }

  public MTable(String tableName, MDatabase database, MStorageDescriptor sd,
      String owner, int createTime, int lastAccessTime, int retention,
      MPartition priPartition, MPartition subPartition,
      Map<String, String> parameters, String viewOriginalText,
      String viewExpandedText, String tableType, String vtables) {
    this.tableName = tableName;
    this.database = database;
    this.sd = sd;
    this.owner = owner;
    this.createTime = createTime;
    this.setLastAccessTime(lastAccessTime);
    this.retention = retention;
    this.priPartition = priPartition;
    this.subPartition = subPartition;
    this.parameters = parameters;
    this.viewOriginalText = viewOriginalText;
    this.viewExpandedText = viewExpandedText;
    this.tableType = tableType;
    this.vtables = vtables;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public MStorageDescriptor getSd() {
    return sd;
  }

  public void setSd(MStorageDescriptor sd) {
    this.sd = sd;
  }

  public MPartition getPriPartition() {
    return priPartition;
  }

  public void setPriPartition(MPartition priPartition) {
    this.priPartition = priPartition;
  }

  public MPartition getSubPartition() {
    return subPartition;
  }

  public void setSubPartition(MPartition subPartition) {
    this.subPartition = subPartition;
  }

  public Map<String, String> getParameters() {
    return parameters;
  }

  public void setParameters(Map<String, String> parameters) {
    this.parameters = parameters;
  }

  public String getViewOriginalText() {
    return viewOriginalText;
  }

  public void setViewOriginalText(String viewOriginalText) {
    this.viewOriginalText = viewOriginalText;
  }

  public String getViewExpandedText() {
    return viewExpandedText;
  }

  public void setViewExpandedText(String viewExpandedText) {
    this.viewExpandedText = viewExpandedText;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  public int getCreateTime() {
    return createTime;
  }

  public void setCreateTime(int createTime) {
    this.createTime = createTime;
  }

  public MDatabase getDatabase() {
    return database;
  }

  public void setDatabase(MDatabase database) {
    this.database = database;
  }

  public int getRetention() {
    return retention;
  }

  public void setRetention(int retention) {
    this.retention = retention;
  }

  public void setLastAccessTime(int lastAccessTime) {
    this.lastAccessTime = lastAccessTime;
  }

  public int getLastAccessTime() {
    return lastAccessTime;
  }

  public void setTableType(String tableType) {
    this.tableType = tableType;
  }

  public String getTableType() {
    return tableType;
  }

}
