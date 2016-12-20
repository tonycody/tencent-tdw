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

public class Mtdw_sys_table_statistics {

  private String stat_table_name;
  private String stat_db_name;
  private int stat_num_records;
  private int stat_num_units;
  private int stat_total_size;
  private int stat_num_files;
  private int stat_num_blocks;

  public String getStat_db_name() {
    return stat_db_name;
  }

  public void setStat_db_name(String dbName) {
    stat_db_name = dbName;
  }

  public String getStat_table_name() {
    return stat_table_name;
  }

  public void setStat_table_name(String statTableName) {
    stat_table_name = statTableName;
  }

  public int getStat_num_records() {
    return stat_num_records;
  }

  public void setStat_num_records(int statNumRecords) {
    stat_num_records = statNumRecords;
  }

  public int getStat_num_units() {
    return stat_num_units;
  }

  public void setStat_num_units(int statNumUnits) {
    stat_num_units = statNumUnits;
  }

  public int getStat_total_size() {
    return stat_total_size;
  }

  public void setStat_total_size(int statTotalSize) {
    stat_total_size = statTotalSize;
  }

  public int getStat_num_files() {
    return stat_num_files;
  }

  public void setStat_num_files(int statNumFiles) {
    stat_num_files = statNumFiles;
  }

  public int getStat_num_blocks() {
    return stat_num_blocks;
  }

  public void setStat_num_blocks(int statNumBlocks) {
    stat_num_blocks = statNumBlocks;
  }

  public Mtdw_sys_table_statistics() {
  }

  public Mtdw_sys_table_statistics(String stat_table_name, String stat_db_name,
      int stat_num_records, int stat_num_units, int stat_total_size,
      int stat_num_files, int stat_num_blocks) {
    this.stat_table_name = stat_table_name;
    this.stat_db_name = stat_db_name;
    this.stat_num_records = stat_num_records;
    this.stat_num_units = stat_num_units;
    this.stat_total_size = stat_total_size;
    this.stat_num_files = stat_num_files;
    this.stat_num_blocks = stat_num_blocks;
  }

}
