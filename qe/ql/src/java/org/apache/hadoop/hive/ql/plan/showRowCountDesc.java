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

public class showRowCountDesc extends ddlDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  Path resFile;
  String tablename = null;
  String dbname = null;
  Vector<String> partitions = null;
  Vector<String> subpartitions = null;
  Vector<String> pairpartitions = null;
  boolean isExtended;
  boolean isPartition = false;

  private final String schema = "table_partition_name,size#string:string";

  public boolean getIsExtended() {
    return isExtended;
  }

  public void setIsPartition(boolean in) {
    isPartition = in;
  }

  public boolean getIsPartition() {
    return isPartition;
  }

  public String getSchema() {
    return schema;
  }

  public String getTableName() {
    return tablename;
  }

  public String getDBName() {
    return dbname;
  }

  public Vector<String> getPartitions() {
    return partitions;
  }

  public Vector<String> getSubpartitions() {
    return subpartitions;
  }

  public Vector<String> getPairpartitions() {
    return pairpartitions;
  }

  public showRowCountDesc(Path resFile, String tname, boolean isext,
      Vector<String> ipartitions, Vector<String> isubpartitions,
      Vector<String> ipairpatitions) {
    this.resFile = resFile;
    tablename = tname;
    isExtended = isext;
    partitions = ipartitions;
    subpartitions = isubpartitions;
    pairpartitions = ipairpatitions;
    isPartition = true;
  }

  public showRowCountDesc(Path resFile, String dbName, String tname,
      boolean isext, Vector<String> ipartitions, Vector<String> isubpartitions,
      Vector<String> ipairpatitions) {
    this.resFile = resFile;
    tablename = tname;
    isExtended = isext;
    partitions = ipartitions;
    subpartitions = isubpartitions;
    pairpartitions = ipairpatitions;
    isPartition = true;
    this.dbname = dbName;
  }

  public showRowCountDesc(Path resFile, String tname, boolean isext) {
    this.resFile = resFile;
    tablename = tname;
    isExtended = isext;
  }

  public showRowCountDesc(Path resFile, String dbName, String tname,
      boolean isext) {
    this.resFile = resFile;
    tablename = tname;
    isExtended = isext;
    this.dbname = dbName;
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
