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
package org.apache.hadoop.hive.ql.metadata;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import javax.swing.plaf.metal.MetalComboBoxButton;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.parse.PartitionDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.mapred.InputFormat;

public class TablePartition {
  private String alias;
  private String PriPartName;
  private String SubPartName;
  private ArrayList<Path> paths;

  private Table tbl;

  public TablePartition(String alias, String priPartName, String subPartName,
      Table tbl) throws HiveException, MetaException {

    this.alias = alias;
    PriPartName = priPartName;
    SubPartName = subPartName;
    this.tbl = tbl;

    this.paths = new ArrayList<Path>();

    if (PriPartName != null && SubPartName != null && !PriPartName.isEmpty()
        && !SubPartName.isEmpty()) {
      this.paths.add(Warehouse.getPartitionPath(tbl.getPath(), priPartName,
          subPartName));
    } else if (PriPartName != null && !PriPartName.isEmpty()) {
      this.paths.addAll(Warehouse.getPriPartitionPaths(tbl.getPath(),
          priPartName, tbl.getTTable().getSubPartition() != null ? tbl
              .getTTable().getSubPartition().getParSpaces().keySet() : null));
    } else if (SubPartName != null && !SubPartName.isEmpty()) {
      this.paths.addAll(Warehouse.getSubPartitionPaths(tbl.getPath(), tbl
          .getTTable().getPriPartition().getParSpaces().keySet(), subPartName));
    } else if (tbl.isPartitioned()) {
      System.out.println("table is partitioned,get all partitions!");
      this.paths.addAll(Warehouse.getPartitionPaths(tbl.getPath(), tbl
          .getTTable().getPriPartition().getParSpaces().keySet(), tbl
          .getTTable().getSubPartition() != null ? tbl.getTTable()
          .getSubPartition().getParSpaces().keySet() : null));
    } else
      this.paths.add(tbl.getPath());
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public String getPriPartName() {
    return PriPartName;
  }

  public void setPriPartName(String priPartName) {
    PriPartName = priPartName;
  }

  public String getSubPartName() {
    return SubPartName;
  }

  public void setSubPartName(String subPartName) {
    SubPartName = subPartName;
  }

  public ArrayList<Path> getPaths() {
    return paths;
  }
  
  public void setPaths(List<Path> ps) {
    paths = (ArrayList<Path>) ps;
    return;
  }

  public Table getTbl() {
    return tbl;
  }

  public void setTbl(Table tbl) {
    this.tbl = tbl;
  }

  public void checkValidity() throws HiveException {
    tbl.checkValidity();
  }

  public boolean equals(Object obj) {
    return tbl.equals(obj);
  }

  public List<FieldSchema> getAllCols() {
    return tbl.getAllCols();
  }

  public List<String> getBucketCols() {
    return tbl.getBucketCols();
  }

  public String getBucketingDimensionId() {
    return tbl.getBucketingDimensionId();
  }

  public List<FieldSchema> getCols() {
    return tbl.getCols();
  }

  public final URI getDataLocation() {
    return tbl.getDataLocation();
  }

  public String getDbName() {
    return tbl.getDbName();
  }

  public final Deserializer getDeserializer() {
    return tbl.getDeserializer();
  }

  public StructField getField(String fld) {
    return tbl.getField(fld);
  }

  public Vector<StructField> getFields() {
    return tbl.getFields();
  }

  public Boolean getHasSubPartition() {
    return tbl.getHasSubPartition();
  }

  public final Class<? extends org.apache.hadoop.mapred.InputFormat> getInputFormatClass() {
    return tbl.getInputFormatClass();
  }

  public final String getName() {
    return tbl.getName();
  }

  public int getNumBuckets() {
    return tbl.getNumBuckets();
  }

  public final Class<? extends HiveOutputFormat> getOutputFormatClass() {
    return tbl.getOutputFormatClass();
  }

  public String getOwner() {
    return tbl.getOwner();
  }

  public Map<String, String> getParameters() {
    return tbl.getParameters();
  }

  public List<FieldSchema> getPartCols() {
    return tbl.getPartCols();
  }

  public final Path getPath() {
    return tbl.getPath();
  }

  public String getProperty(String name) {
    return tbl.getProperty(name);
  }

  public int getRetention() {
    return tbl.getRetention();
  }

  public final Properties getSchema() {
    return tbl.getSchema();
  }

  public String getSerdeParam(String param) {
    return tbl.getSerdeParam(param);
  }

  public String getSerializationLib() {
    return tbl.getSerializationLib();
  }

  public List<Order> getSortCols() {
    return tbl.getSortCols();
  }

  public org.apache.hadoop.hive.metastore.api.Table getTTable() {
    return tbl.getTTable();
  }

  public int hashCode() {
    return tbl.hashCode();
  }

  public boolean isPartitioned() {
    return tbl.isPartitioned();
  }

  public boolean isPartitionKey(String colName) {
    return tbl.isPartitionKey(colName);
  }

  public final boolean isValidSpec(Map<String, String> spec)
      throws HiveException {
    return tbl.isValidSpec(spec);
  }

  public void reinitSerDe() throws HiveException {
    tbl.reinitSerDe();
  }

  public void setBucketCols(List<String> bucketCols) throws HiveException {
    tbl.setBucketCols(bucketCols);
  }

  public void setDataLocation(URI uri2) {
    tbl.setDataLocation(uri2);
  }

  public void setDeserializer(Deserializer deserializer) {
    tbl.setDeserializer(deserializer);
  }

  public void setFields(List<FieldSchema> fields) {
    tbl.setFields(fields);
  }

  public void setHasSubPartition(Boolean hasSubPartition) {
    tbl.setHasSubPartition(hasSubPartition);
  }

  public void setInputFormatClass(Class<? extends InputFormat> inputFormatClass) {
    tbl.setInputFormatClass(inputFormatClass);
  }

  public void setInputFormatClass(String name) throws HiveException {
    tbl.setInputFormatClass(name);
  }

  public void setNumBuckets(int nb) {
    tbl.setNumBuckets(nb);
  }

  public void setOutputFormatClass(Class<?> class1) {
    tbl.setOutputFormatClass(class1);
  }

  public void setOutputFormatClass(String name) throws HiveException {
    tbl.setOutputFormatClass(name);
  }

  public void setOwner(String owner) {
    tbl.setOwner(owner);
  }

  public void setPartitions(PartitionDesc pd) {
    tbl.setPartitions(pd);
  }

  public void setProperty(String name, String value) {
    tbl.setProperty(name, value);
  }

  public void setRetention(int retention) {
    tbl.setRetention(retention);
  }

  public void setSchema(Properties schema) {
    tbl.setSchema(schema);
  }

  public String setSerdeParam(String param, String value) {
    return tbl.setSerdeParam(param, value);
  }

  public void setSerializationLib(String lib) {
    tbl.setSerializationLib(lib);
  }

  public void setSortCols(List<Order> sortOrder) throws HiveException {
    tbl.setSortCols(sortOrder);
  }

  public String toString() {
    return tbl.toString();
  }

}
