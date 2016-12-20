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

public class Mtdw_sys_fields_statistics {

  private String stat_table_name;
  private String stat_db_name;
  private String stat_field_name;
  private double stat_nullfac;
  private int stat_avg_field_width;
  private double stat_distinct_values;
  private String stat_values_1;
  private String stat_numbers_1;
  private String stat_values_2;
  private String stat_numbers_2;
  private String stat_values_3;
  private String stat_numbers_3;
  private int stat_number_1_type;
  private int stat_number_2_type;
  private int stat_number_3_type;

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

  public String getStat_field_name() {
    return stat_field_name;
  }

  public void setStat_field_name(String statFieldName) {
    stat_field_name = statFieldName;
  }

  public double getStat_nullfac() {
    return stat_nullfac;
  }

  public void setStat_nullfac(double statNullfac) {
    stat_nullfac = statNullfac;
  }

  public int getStat_avg_field_width() {
    return stat_avg_field_width;
  }

  public void setStat_avg_field_width(int statAvgFieldWidth) {
    stat_avg_field_width = statAvgFieldWidth;
  }

  public double getStat_distinct_values() {
    return stat_distinct_values;
  }

  public void setStat_distinct_values(double statDistinctValues) {
    stat_distinct_values = statDistinctValues;
  }

  public String getStat_values_1() {
    return stat_values_1;
  }

  public void setStat_values_1(String statValues_1) {
    stat_values_1 = statValues_1;
  }

  public String getStat_numbers_1() {
    return stat_numbers_1;
  }

  public void setStat_numbers_1(String statNumbers_1) {
    stat_numbers_1 = statNumbers_1;
  }

  public String getStat_values_2() {
    return stat_values_2;
  }

  public void setStat_values_2(String statValues_2) {
    stat_values_2 = statValues_2;
  }

  public String getStat_numbers_2() {
    return stat_numbers_2;
  }

  public void setStat_numbers_2(String statNumbers_2) {
    stat_numbers_2 = statNumbers_2;
  }

  public String getStat_values_3() {
    return stat_values_3;
  }

  public void setStat_values_3(String statValues_3) {
    stat_values_3 = statValues_3;
  }

  public String getStat_numbers_3() {
    return stat_numbers_3;
  }

  public void setStat_numbers_3(String statNumbers_3) {
    stat_numbers_3 = statNumbers_3;
  }

  public int getStat_number_1_type() {
    return stat_number_1_type;
  }

  public void setStat_number_1_type(int statNumber_1Type) {
    stat_number_1_type = statNumber_1Type;
  }

  public int getStat_number_2_type() {
    return stat_number_2_type;
  }

  public void setStat_number_2_type(int statNumber_2Type) {
    stat_number_2_type = statNumber_2Type;
  }

  public int getStat_number_3_type() {
    return stat_number_3_type;
  }

  public void setStat_number_3_type(int statNumber_3Type) {
    stat_number_3_type = statNumber_3Type;
  }

  public Mtdw_sys_fields_statistics() {
  }

  public Mtdw_sys_fields_statistics(String stat_table_name,
      String stat_field_name, String stat_db_name, double stat_nullfac,
      int stat_avg_field_width, double stat_distinct_values,
      String stat_values_1, String stat_numbers_1, String stat_values_2,
      String stat_numbers_2, String stat_values_3, String stat_numbers_3,
      int stat_number_1_type, int stat_number_2_type, int stat_number_3_type) {
    this.stat_table_name = stat_table_name;
    this.stat_field_name = stat_field_name;
    this.stat_db_name = stat_db_name;
    this.stat_nullfac = stat_nullfac;
    this.stat_avg_field_width = stat_avg_field_width;
    this.stat_distinct_values = stat_distinct_values;
    this.stat_values_1 = stat_values_1;
    this.stat_numbers_1 = stat_numbers_1;
    this.stat_values_2 = stat_values_2;
    this.stat_numbers_2 = stat_numbers_2;
    this.stat_values_3 = stat_values_3;
    this.stat_numbers_3 = stat_numbers_3;
    this.stat_number_1_type = stat_number_1_type;
    this.stat_number_2_type = stat_number_2_type;
    this.stat_number_3_type = stat_number_3_type;
  }

}
