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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@explain(displayName = "Drop Table")
public class dropTableDesc extends ddlDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  String dbName;

  String tableName;
  Boolean isSub;

  ArrayList<String> partitionNames;

  Boolean isDropPartition;
  Boolean reserveData;

  boolean expectView;

  public dropTableDesc(String tableName, boolean expectView) {
    this.tableName = tableName;
    this.isDropPartition = false;
    this.reserveData = false;
    this.expectView = expectView;
  }

  public dropTableDesc(String tableName, Boolean reserveData, boolean expectView) {
    this.tableName = tableName;
    this.isDropPartition = false;
    this.reserveData = reserveData;
    this.expectView = expectView;
  }

  public dropTableDesc(String dbName, String tableName, Boolean isSub,
      ArrayList<String> partNames, Boolean dropPart) {
    this.dbName = dbName;
    this.tableName = tableName;
    this.isSub = isSub;
    this.partitionNames = partNames;
    this.isDropPartition = dropPart;
    this.expectView = false;
  }

  public Boolean getIsDropPartition() {
    return isDropPartition;
  }

  public void setIsDropPartition(Boolean isDropPartition) {
    this.isDropPartition = isDropPartition;
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  @explain(displayName = "table")
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

  public Boolean getIsReserveData() {
    return reserveData;
  }

  public ArrayList<String> getPartitionNames() {
    return partitionNames;
  }

  public void setPartitionNames(ArrayList<String> partitionNames) {
    this.partitionNames = partitionNames;
  }

  public boolean getExpectView() {
    return expectView;
  }

  public void setExpectView(boolean expectView) {
    this.expectView = expectView;
  }
}
