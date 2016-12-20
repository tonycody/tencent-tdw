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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.parse.PartitionDesc;

@explain(displayName = "Create Table")
public class createTableDesc extends ddlDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  String tableName;
  boolean isExternal;
  List<FieldSchema> cols;

  PartitionDesc partdesc;
  List<String> bucketCols;
  List<Order> sortCols;
  int numBuckets;
  String fieldDelim;
  String fieldEscape;
  String collItemDelim;
  String mapKeyDelim;
  String lineDelim;
  String comment;
  String inputFormat;
  String outputFormat;
  String location;
  String serName;
  Map<String, String> mapProp;
  Map<String, String> mapPropOther;

  boolean ifNotExists;

  boolean ifCompressed;
  ArrayList<ArrayList<String>> projectionInfos;
  int tblType;
  indexInfoDesc indexInfo;

  String pb_file_path;
  String pb_outer_name;

  public createTableDesc(String tableName, boolean isExternal,
      List<FieldSchema> cols, PartitionDesc ptdc, List<String> bucketCols,
      List<Order> sortCols, int numBuckets, String fieldDelim,
      String fieldEscape, String collItemDelim, String mapKeyDelim,
      String lineDelim, String comment, String inputFormat,
      String outputFormat, String location, String serName,
      Map<String, String> mapProp, Map<String, String> mapPropOther,
      boolean ifNotExists, boolean ifCompressed, int tblType,
      ArrayList<ArrayList<String>> projectionInfos, indexInfoDesc indexInfo,
      String pb_file_path, String pb_msg_outer_name) {
    this.tableName = tableName;
    this.isExternal = isExternal;
    this.bucketCols = bucketCols;
    this.sortCols = sortCols;
    this.collItemDelim = collItemDelim;
    this.cols = cols;
    this.comment = comment;
    this.fieldDelim = fieldDelim;
    this.fieldEscape = fieldEscape;
    this.inputFormat = inputFormat;
    this.outputFormat = outputFormat;
    this.lineDelim = lineDelim;
    this.location = location;
    this.mapKeyDelim = mapKeyDelim;
    this.numBuckets = numBuckets;
    this.partdesc = ptdc;
    this.serName = serName;
    this.mapProp = mapProp;
    this.mapPropOther = mapPropOther;
    this.ifNotExists = ifNotExists;

    this.ifCompressed = ifCompressed;
    this.projectionInfos = projectionInfos;
    this.tblType = tblType;
    this.indexInfo = indexInfo;

    this.pb_file_path = pb_file_path;
    this.pb_outer_name = pb_msg_outer_name;
  }

  @explain(displayName = "if compressed")
  public boolean getIfCompressed() {
    return ifCompressed;
  }

  public void setIfCompressed(boolean ifCompressed) {
    this.ifCompressed = ifCompressed;
  }

  @explain(displayName = "projection infos")
  public ArrayList<ArrayList<String>> getProjectionInfos() {
    return projectionInfos;
  }

  public void setProjectionInfos(ArrayList<ArrayList<String>> projectionInfos) {
    this.projectionInfos = projectionInfos;
  }

  public int getTblType() {
    return tblType;
  }

  public void setTblType(int tblType) {
    this.tblType = tblType;
  }

  public indexInfoDesc getIndexInfo() {
    return indexInfo;
  }

  public String getPb_file_path() {
    return pb_file_path;
  }

  public void setPb_file_path(String pbFilePath) {
    pb_file_path = pbFilePath;
  }

  public String getPb_outer_name() {
    return pb_outer_name;
  }

  public void setPb_outer_name(String pbOuterName) {
    pb_outer_name = pbOuterName;
  }

  @explain(displayName = "if not exists")
  public boolean getIfNotExists() {
    return ifNotExists;
  }

  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }

  @explain(displayName = "name")
  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public List<FieldSchema> getCols() {
    return cols;
  }

  @explain(displayName = "columns")
  public List<String> getColsString() {
    return Utilities.getFieldSchemaString(getCols());
  }

  public void setCols(List<FieldSchema> cols) {
    this.cols = cols;
  }

  @explain(displayName = "bucket columns")
  public List<String> getBucketCols() {
    return bucketCols;
  }

  public PartitionDesc getPartdesc() {
    return partdesc;
  }

  public void setPartdesc(PartitionDesc partdesc) {
    this.partdesc = partdesc;
  }

  public void setBucketCols(List<String> bucketCols) {
    this.bucketCols = bucketCols;
  }

  @explain(displayName = "# buckets")
  public int getNumBuckets() {
    return numBuckets;
  }

  public void setNumBuckets(int numBuckets) {
    this.numBuckets = numBuckets;
  }

  @explain(displayName = "field delimiter")
  public String getFieldDelim() {
    return fieldDelim;
  }

  public void setFieldDelim(String fieldDelim) {
    this.fieldDelim = fieldDelim;
  }

  @explain(displayName = "field escape")
  public String getFieldEscape() {
    return fieldEscape;
  }

  public void setFieldEscape(String fieldEscape) {
    this.fieldEscape = fieldEscape;
  }

  @explain(displayName = "collection delimiter")
  public String getCollItemDelim() {
    return collItemDelim;
  }

  public void setCollItemDelim(String collItemDelim) {
    this.collItemDelim = collItemDelim;
  }

  @explain(displayName = "map key delimiter")
  public String getMapKeyDelim() {
    return mapKeyDelim;
  }

  public void setMapKeyDelim(String mapKeyDelim) {
    this.mapKeyDelim = mapKeyDelim;
  }

  @explain(displayName = "line delimiter")
  public String getLineDelim() {
    return lineDelim;
  }

  public void setLineDelim(String lineDelim) {
    this.lineDelim = lineDelim;
  }

  @explain(displayName = "comment")
  public String getComment() {
    return comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  @explain(displayName = "input format")
  public String getInputFormat() {
    return inputFormat;
  }

  public void setInputFormat(String inputFormat) {
    this.inputFormat = inputFormat;
  }

  @explain(displayName = "output format")
  public String getOutputFormat() {
    return outputFormat;
  }

  public void setOutputFormat(String outputFormat) {
    this.outputFormat = outputFormat;
  }

  @explain(displayName = "location")
  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  @explain(displayName = "isExternal")
  public boolean isExternal() {
    return isExternal;
  }

  public void setExternal(boolean isExternal) {
    this.isExternal = isExternal;
  }

  @explain(displayName = "sort columns")
  public List<Order> getSortCols() {
    return sortCols;
  }

  public void setSortCols(List<Order> sortCols) {
    this.sortCols = sortCols;
  }

  @explain(displayName = "serde name")
  public String getSerName() {
    return serName;
  }

  public void setSerName(String serName) {
    this.serName = serName;
  }

  @explain(displayName = "serde properties")
  public Map<String, String> getMapProp() {
    return mapProp;
  }

  public void setMapProp(Map<String, String> mapProp) {
    this.mapProp = mapProp;
  }

  public Map<String, String> getMapPropOther() {
    return mapPropOther;
  }

  public void setMapPropOther(Map<String, String> mapPropOther) {
    this.mapPropOther = mapPropOther;
  }
}
