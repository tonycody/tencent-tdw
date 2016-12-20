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

import java.util.List;
import java.util.Map;

public class MStorageDescriptor {
  private List<MFieldSchema> cols;
  private String location;
  private String inputFormat;
  private String outputFormat;
  private boolean isCompressed = false;
  private int numBuckets = 1;
  private MSerDeInfo serDeInfo;
  private List<String> bucketCols;
  private List<MOrder> sortCols;
  private Map<String, String> parameters;

  public MStorageDescriptor() {
  }

  public MStorageDescriptor(List<MFieldSchema> cols, String location,
      String inputFormat, String outputFormat, boolean isCompressed,
      int numBuckets, MSerDeInfo serDeInfo, List<String> bucketCols,
      List<MOrder> sortOrder, Map<String, String> parameters) {
    this.cols = cols;
    this.location = location;
    this.inputFormat = inputFormat;
    this.outputFormat = outputFormat;
    this.isCompressed = isCompressed;
    this.numBuckets = numBuckets;
    this.serDeInfo = serDeInfo;
    this.bucketCols = bucketCols;
    this.sortCols = sortOrder;
    this.parameters = parameters;
  }

  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public boolean isCompressed() {
    return isCompressed;
  }

  public void setCompressed(boolean isCompressed) {
    this.isCompressed = isCompressed;
  }

  public int getNumBuckets() {
    return numBuckets;
  }

  public void setNumBuckets(int numBuckets) {
    this.numBuckets = numBuckets;
  }

  public List<String> getBucketCols() {
    return bucketCols;
  }

  public void setBucketCols(List<String> bucketCols) {
    this.bucketCols = bucketCols;
  }

  public Map<String, String> getParameters() {
    return parameters;
  }

  public void setParameters(Map<String, String> parameters) {
    this.parameters = parameters;
  }

  public String getInputFormat() {
    return inputFormat;
  }

  public void setInputFormat(String inputFormat) {
    this.inputFormat = inputFormat;
  }

  public String getOutputFormat() {
    return outputFormat;
  }

  public void setOutputFormat(String outputFormat) {
    this.outputFormat = outputFormat;
  }

  public List<MFieldSchema> getCols() {
    return cols;
  }

  public void setCols(List<MFieldSchema> cols) {
    this.cols = cols;
  }

  public MSerDeInfo getSerDeInfo() {
    return serDeInfo;
  }

  public void setSerDeInfo(MSerDeInfo serDe) {
    this.serDeInfo = serDe;
  }

  public void setSortCols(List<MOrder> sortOrder) {
    this.sortCols = sortOrder;
  }

  public List<MOrder> getSortCols() {
    return sortCols;
  }
}
