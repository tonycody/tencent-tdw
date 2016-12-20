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
package org.apache.hadoop.hive.ql.metadata;

import java.util.ArrayList;
import java.util.List;

public class CheckResult {

  private List<String> tablesNotOnFs = new ArrayList<String>();
  private List<String> tablesNotInMs = new ArrayList<String>();
  private List<PartitionResult> partitionsNotOnFs = new ArrayList<PartitionResult>();
  private List<PartitionResult> partitionsNotInMs = new ArrayList<PartitionResult>();

  public List<String> getTablesNotOnFs() {
    return tablesNotOnFs;
  }

  public void setTablesNotOnFs(List<String> tablesNotOnFs) {
    this.tablesNotOnFs = tablesNotOnFs;
  }

  public List<String> getTablesNotInMs() {
    return tablesNotInMs;
  }

  public void setTablesNotInMs(List<String> tablesNotInMs) {
    this.tablesNotInMs = tablesNotInMs;
  }

  public List<PartitionResult> getPartitionsNotOnFs() {
    return partitionsNotOnFs;
  }

  public void setPartitionsNotOnFs(List<PartitionResult> partitionsNotOnFs) {
    this.partitionsNotOnFs = partitionsNotOnFs;
  }

  public List<PartitionResult> getPartitionsNotInMs() {
    return partitionsNotInMs;
  }

  public void setPartitionsNotInMs(List<PartitionResult> partitionsNotInMs) {
    this.partitionsNotInMs = partitionsNotInMs;
  }

  static class PartitionResult {
    private String partitionName;
    private String tableName;
    private boolean isSub;
    private String path = null;

    public String getPath() {
      return path;
    }

    public void setPath(String path) {
      this.path = path;
    }

    public String getPartitionName() {
      return partitionName;
    }

    public boolean isSub() {
      return isSub;
    }

    public void setSub(boolean isSub) {
      this.isSub = isSub;
    }

    public void setPartitionName(String partitionName) {
      this.partitionName = partitionName;
    }

    public String getTableName() {
      return tableName;
    }

    public void setTableName(String tableName) {
      this.tableName = tableName;
    }

    public String toString() {
      return tableName + " : "
          + (partitionName == null ? "unknown" : partitionName) + " ,PATH: "
          + path;
    }
  }

}
