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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.parse.PartitionDesc;
import org.apache.hadoop.hive.ql.parse.PartitionType;
import org.apache.hadoop.hive.ql.parse.QB.PartRefType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;

public class Table {

  static final private Log LOG = LogFactory.getLog("hive.ql.metadata.Table");

  private Properties schema;
  private Deserializer deserializer;
  private Boolean hasSubPartition = false;

  private URI uri;
  private Class<? extends InputFormat> inputFormatClass;
  private Class<? extends HiveOutputFormat> outputFormatClass;
  private org.apache.hadoop.hive.metastore.api.Table tTable;
  private ArrayList<String> colComments;

  protected Table() throws HiveException {
  }

  public Table(String name, Properties schema, Deserializer deserializer,
      Class<? extends InputFormat<?, ?>> inputFormatClass,
      Class<?> outputFormatClass, URI dataLocation, Hive hive)
      throws HiveException {
    initEmpty();
    getTTable().setDbName(MetaStoreUtils.DEFAULT_DATABASE_NAME);
    this.schema = schema;
    this.deserializer = deserializer;
    this.getTTable().getSd().getSerdeInfo()
        .setSerializationLib(deserializer.getClass().getName());
    getTTable().setTableName(name);
    getSerdeInfo().setSerializationLib(deserializer.getClass().getName());
    setInputFormatClass(inputFormatClass);
    setOutputFormatClass(HiveFileFormatUtils
        .getOutputFormatSubstitute(outputFormatClass));
    setDataLocation(dataLocation);
  }

  public Table(String name) {
    initEmpty();
    getTTable().setDbName(MetaStoreUtils.DEFAULT_DATABASE_NAME);
    getTTable().setTableName(name);
    getTTable().setDbName(SessionState.get().getDbName());
    getSerdeInfo().setSerializationLib(
        MetadataTypedColumnsetSerDe.class.getName());
    getSerdeInfo().getParameters().put(Constants.SERIALIZATION_FORMAT, "1");
  }

  void initEmpty() {
    setTTable(new org.apache.hadoop.hive.metastore.api.Table());
    getTTable().setSd(new StorageDescriptor());
    getTTable().setParameters(new HashMap<String, String>());

    StorageDescriptor sd = getTTable().getSd();
    sd.setSerdeInfo(new SerDeInfo());
    sd.setNumBuckets(-1);
    sd.setBucketCols(new ArrayList<String>());
    sd.setCols(new ArrayList<FieldSchema>());
    sd.setParameters(new HashMap<String, String>());
    sd.setSortCols(new ArrayList<Order>());

    sd.getSerdeInfo().setParameters(new HashMap<String, String>());

    setTableType(TableType.MANAGED_TABLE);
  }

  public void reinitSerDe() throws HiveException {
    try {
      deserializer = MetaStoreUtils.getDeserializer(Hive.get().getConf(),
          this.getTTable());
    } catch (MetaException e) {
      throw new HiveException(e);
    }
  }

  public Boolean getHasSubPartition() {
    return hasSubPartition;
  }

  public void setHasSubPartition(Boolean hasSubPartition) {
    this.hasSubPartition = hasSubPartition;
  }

  protected void initSerDe() throws HiveException {
    if (deserializer == null) {
      try {
        deserializer = MetaStoreUtils.getDeserializer(Hive.get().getConf(),
            this.getTTable());
      } catch (MetaException e) {
        throw new HiveException(e);
      }
    }
  }

  public void checkValidity() throws HiveException {
    String name = getTTable().getTableName();
    if (null == name || name.length() == 0
        || !MetaStoreUtils.validateName(name)) {
      throw new HiveException("[" + name + "]: is not a valid table name");
    }
    if (0 == getCols().size()) {
      throw new HiveException(
          "at least one column must be specified for the table");
    }
    if (!isView()) {
      if (null == getDeserializer()) {
        throw new HiveException("must specify a non-null serDe");
      }
      if (null == getInputFormatClass()) {
        throw new HiveException("must specify an InputFormat class");
      }
      if (null == getOutputFormatClass()) {
        throw new HiveException("must specify an OutputFormat class");
      }
    }

    if (isView()) {
      assert (getViewOriginalText() != null);
      assert (getViewExpandedText() != null);
    } else {
      assert (getViewOriginalText() == null);
      assert (getViewExpandedText() == null);
    }

    Iterator<FieldSchema> iterCols = getCols().iterator();
    List<String> colNames = new ArrayList<String>();
    while (iterCols.hasNext()) {
      String colName = iterCols.next().getName();
      Iterator<String> iter = colNames.iterator();
      while (iter.hasNext()) {
        String oldColName = iter.next();
        if (colName.equalsIgnoreCase(oldColName))
          throw new HiveException("Duplicate column name " + colName
              + " in the table definition.");
      }
      colNames.add(colName.toLowerCase());
    }

    if (isPartitioned()) {

      if (getPartCols().size() == 2
          && getPartCols().get(0).equals(getPartCols().get(1))) {
        throw new HiveException(
            "primary partition and subpartition should not be the same");
      }

      List<FieldSchema> fss = getCols();
      List<FieldSchema> pfs = getPartCols();
      for (FieldSchema fs : pfs) {
        boolean findIt = false;
        for (FieldSchema f : fss) {
          if (f.getName().equalsIgnoreCase(fs.getName())) {
            if (f.getType().equalsIgnoreCase(fs.getType())) {
              findIt = true;
              break;
            } else
              throw new HiveException(
                  "the partition column's type is not consistent!");
          }
        }

        if (!findIt)
          throw new HiveException(
              "find some partition columns missed in all the table columns");
      }

    }

    return;
  }

  public void setInputFormatClass(
      Class<? extends org.apache.hadoop.mapred.InputFormat> inputFormatClass) {
    this.inputFormatClass = inputFormatClass;
    tTable.getSd().setInputFormat(inputFormatClass.getName());
  }

  public void setOutputFormatClass(Class<?> class1) {
    this.outputFormatClass = HiveFileFormatUtils
        .getOutputFormatSubstitute(class1);
    tTable.getSd().setOutputFormat(class1.getName());
  }

  final public Properties getSchema() {
    return schema;
  }

  final public Path getPath() {
    return new Path(getTTable().getSd().getLocation());
  }

  final public String getName() {
    return getTTable().getTableName();
  }

  final public URI getDataLocation() {
    return uri;
  }

  final public Deserializer getDeserializer() {
    if (deserializer == null) {
      try {
        initSerDe();
      } catch (HiveException e) {
        LOG.error("Error in initializing serde.", e);
      }
    }
    return deserializer;
  }

  final public Class<? extends InputFormat> getInputFormatClass() {
    return inputFormatClass;
  }

  final public Class<? extends HiveOutputFormat> getOutputFormatClass() {
    return outputFormatClass;
  }

  final public boolean isValidSpec(Map<String, String> spec)
      throws HiveException {

    return true;
  }

  public void setProperty(String name, String value) {
    getTTable().getParameters().put(name, value);
  }

  public String getProperty(String name) {
    return getTTable().getParameters().get(name);
  }

  public void setTableType(TableType tableType) {
    getTTable().setTableType(tableType.toString());
  }

  public TableType getTableType() {
    return Enum.valueOf(TableType.class, getTTable().getTableType());
  }

  public Vector<StructField> getFields() {

    Vector<StructField> fields = new Vector<StructField>();
    try {
      Deserializer decoder = getDeserializer();

      StructObjectInspector structObjectInspector = (StructObjectInspector) decoder
          .getObjectInspector();
      List<? extends StructField> fld_lst = structObjectInspector
          .getAllStructFieldRefs();
      for (StructField field : fld_lst) {
        fields.add(field);
      }
    } catch (SerDeException e) {
      throw new RuntimeException(e);
    }
    return fields;
  }

  public StructField getField(String fld) {
    try {
      StructObjectInspector structObjectInspector = (StructObjectInspector) getDeserializer()
          .getObjectInspector();
      return structObjectInspector.getStructFieldRef(fld);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void setSchema(Properties schema) {
    this.schema = schema;
  }

  public void setDeserializer(Deserializer deserializer) {
    this.deserializer = deserializer;
  }

  public String toString() {
    return getTTable().getTableName();
  }

  public List<FieldSchema> getPartCols() {
    if (getTTable().getPriPartition() == null)
      return null;
    List<FieldSchema> partKeys = new ArrayList<FieldSchema>();
    if (getTTable().getPriPartition() != null)
      partKeys.add(getTTable().getPriPartition().getParKey());
    if (getTTable().getSubPartition() != null) {
      partKeys.add(getTTable().getSubPartition().getParKey());
    }

    return partKeys;
  }

  public boolean isPartitionKey(String colName) {
    List<FieldSchema> partCols = getPartCols();
    if (partCols == null)
      return false;
    for (FieldSchema key : partCols) {
      if (key.getName().toLowerCase().equals(colName)) {
        return true;
      }
    }
    return false;
  }

  public String getBucketingDimensionId() {
    List<String> bcols = getTTable().getSd().getBucketCols();
    if (bcols == null || bcols.size() == 0) {
      return null;
    }

    if (bcols.size() > 1) {
      LOG.warn(this
          + " table has more than one dimensions which aren't supported yet");
    }

    return bcols.get(0);
  }

  public org.apache.hadoop.hive.metastore.api.Table getTTable() {
    return tTable;
  }

  public void setTTable(org.apache.hadoop.hive.metastore.api.Table table) {
    tTable = table;
  }

  public void setDataLocation(URI uri2) {
    uri = uri2;
    getTTable().getSd().setLocation(uri2.toString());
  }

  public void setBucketCols(List<String> bucketCols) throws HiveException {
    if (bucketCols == null) {
      return;
    }

    for (String col : bucketCols) {
      if (!isField(col))
        throw new HiveException("Bucket columns " + col
            + " is not part of the table columns");
    }
    getTTable().getSd().setBucketCols(bucketCols);
  }

  public void setSortCols(List<Order> sortOrder) throws HiveException {
    getTTable().getSd().setSortCols(sortOrder);
  }

  protected boolean isField(String col) {
    for (FieldSchema field : getCols()) {
      if (field.getName().equals(col)) {
        return true;
      }
    }
    return false;
  }

  public List<FieldSchema> getCols() {
    boolean getColsFromSerDe = SerDeUtils
        .shouldGetColsFromSerDe(getSerializationLib());
    if (!getColsFromSerDe)
      return getTTable().getSd().getCols();
    else {
      try {
        return Hive.getFieldsFromDeserializer(getName(), getDeserializer());
      } catch (HiveException e) {
        LOG.error("Unable to get field from serde: " + getSerializationLib(), e);
      }
      return new ArrayList<FieldSchema>();
    }
  }

  public List<FieldSchema> getAllCols() {
    ArrayList<FieldSchema> f_list = new ArrayList<FieldSchema>();
    f_list.addAll(getCols());
    return f_list;
  }

  public String getDbName() {
    return getTTable().getDbName();
  }

  public int getNumBuckets() {
    return getTTable().getSd().getNumBuckets();
  }

  protected void replaceFiles(Path srcf, Path tmpd) throws HiveException {
    FileSystem fs;
    try {
      fs = FileSystem.get(getDataLocation(), Hive.get().getConf());
      Hive.replaceFiles(srcf, new Path(getDataLocation().getPath()), fs, tmpd);
      if (this.isPartitioned()) {
        Set<String> priPartNames = getTTable().getPriPartition().getParSpaces()
            .keySet();
        Set<String> subPartNames = null;
        if (getTTable().getSubPartition() != null) {
          subPartNames = getTTable().getSubPartition().getParSpaces().keySet();
        }
        if (subPartNames == null) {
          for (String priPartName : priPartNames) {
            Path partPath = new Path(getDataLocation().getPath(), priPartName);
            try {
              if (!fs.exists(partPath))
                fs.mkdirs(partPath);
            } catch (IOException ie) {
            }
          }
        } else {
          for (String priPartName : priPartNames) {
            for (String subPartName : subPartNames) {
              Path partPath = new Path(getDataLocation().getPath(), priPartName
                  + "/" + subPartName);
              try {
                if (!fs.exists(partPath))
                  fs.mkdirs(partPath);
              } catch (IOException ie) {
              }
            }
          }
        }
      }
    } catch (IOException e) {
      throw new HiveException("addFiles: filesystem error in check phase", e);
    }
  }

  protected void replaceFiles(Path srcf, PartRefType prt, String partName,
      String subPartName, Path tmpd) throws HiveException {
    FileSystem fs;
    try {
      fs = FileSystem.get(getDataLocation(), Hive.get().getConf());
      Path tabPath = new Path(getDataLocation().getPath());
      Path destPath = null;
      Path srcPath = null;

      switch (prt) {
      case PRI:
        destPath = new Path(tabPath, partName);
        srcPath = new Path(srcf, partName);
        break;
      case COMP:
        Path tmpPath1 = new Path(tabPath, partName);
        destPath = new Path(tmpPath1, subPartName);
        Path tmpPath2 = new Path(srcf, partName);
        srcPath = new Path(tmpPath2, subPartName);
        break;

      case SUB:
        throw new HiveException("you should set a pri-partition first");
      }

      Hive.replaceFiles(srcPath, destPath, fs, tmpd);
    } catch (IOException e) {
      throw new HiveException("addFiles: filesystem error in check phase", e);
    }
  }

  protected void copyFiles(Path srcf) throws HiveException {
    FileSystem fs;
    try {
      fs = FileSystem.get(getDataLocation(), Hive.get().getConf());
      Hive.copyFiles(srcf, new Path(getDataLocation().getPath()), fs);
    } catch (IOException e) {
      throw new HiveException("addFiles: filesystem error in check phase", e);
    }
  }

  public void setInputFormatClass(String name) throws HiveException {
    try {
      setInputFormatClass((Class<? extends InputFormat<WritableComparable, Writable>>) Class
          .forName(name, true, JavaUtils.getClassLoader()));
    } catch (ClassNotFoundException e) {
      throw new HiveException("Class not found: " + name, e);
    }
  }

  public void setOutputFormatClass(String name) throws HiveException {
    try {
      Class<?> origin = Class.forName(name, true, JavaUtils.getClassLoader());
      setOutputFormatClass(HiveFileFormatUtils
          .getOutputFormatSubstitute(origin));
    } catch (ClassNotFoundException e) {
      throw new HiveException("Class not found: " + name, e);
    }
  }

  public boolean isPartitioned() {
    return (getTTable().getPriPartition() != null);

  }

  public void setFields(List<FieldSchema> fields) {
    getTTable().getSd().setCols(fields);
  }

  public void setNumBuckets(int nb) {
    getTTable().getSd().setNumBuckets(nb);
  }

  public String getOwner() {
    return tTable.getOwner();
  }

  public Map<String, String> getParameters() {
    return tTable.getParameters();
  }

  public int getRetention() {
    return tTable.getRetention();
  }

  public void setOwner(String owner) {
    tTable.setOwner(owner);
  }

  public void setRetention(int retention) {
    tTable.setRetention(retention);
  }

  protected SerDeInfo getSerdeInfo() {
    return getTTable().getSd().getSerdeInfo();
  }

  public void setSerializationLib(String lib) {
    getSerdeInfo().setSerializationLib(lib);
  }

  public String getSerializationLib() {
    return getSerdeInfo().getSerializationLib();
  }

  public String getSerdeParam(String param) {
    return getSerdeInfo().getParameters().get(param);
  }

  public String setSerdeParam(String param, String value) {
    return getSerdeInfo().getParameters().put(param, value);
  }

  public List<String> getBucketCols() {
    return getTTable().getSd().getBucketCols();
  }

  public List<Order> getSortCols() {
    return getTTable().getSd().getSortCols();
  }

  public String getViewOriginalText() {
    return getTTable().getViewOriginalText();
  }

  public void setViewOriginalText(String viewOriginalText) {
    getTTable().setViewOriginalText(viewOriginalText);
  }

  public String getViewTables() {
    return getTTable().getVtables();
  }

  public void setViewTables(String viewTables) {
    getTTable().setVtables(viewTables);
  }

  public String getViewExpandedText() {
    return getTTable().getViewExpandedText();
  }

  public void setViewExpandedText(String viewExpandedText) {
    getTTable().setViewExpandedText(viewExpandedText);
  }

  public boolean isView() {

    return TableType.VIRTUAL_VIEW.equals(getTableType());
  }

  public void setPartitions(PartitionDesc pd) {

    Partition part = new Partition();

    part.setLevel(0);
    part.setParKey(pd.getPartColumn());

    if (pd.getPartitionType() == PartitionType.LIST_PARTITION)
      part.setParType("list");
    else if (pd.getPartitionType() == PartitionType.RANGE_PARTITION)
      part.setParType("range");
    else
      part.setParType("hash");

    part.setTableName(pd.getTableName());
    part.setDbName(pd.getDbName());
    part.setParSpaces(pd.getPartitionSpaces());

    getTTable().setPriPartition(part);
    if (pd.getSubPartition() != null) {
      part = new Partition();

      part.setLevel(1);
      part.setParKey(pd.getSubPartition().getPartColumn());
      if (pd.getSubPartition().getPartitionType() == PartitionType.LIST_PARTITION)
        part.setParType("list");
      else if (pd.getSubPartition().getPartitionType() == PartitionType.RANGE_PARTITION)
        part.setParType("range");
      else
        part.setParType("hash");
      part.setTableName(pd.getSubPartition().getTableName());
      part.setDbName(pd.getSubPartition().getDbName());
      part.setParSpaces(pd.getSubPartition().getPartitionSpaces());

      getTTable().setSubPartition(part);
    }
  }

  public void setCompressed(boolean compressed) {
    getTTable().getSd().setCompressed(compressed);
  }

  public boolean isCompressed() {
    return getTTable().getSd().isCompressed();
  }

  public void setTableStorageType(String type) {
    getTTable().getParameters().put("type", type);
  }

  public String getTableStorgeType() {
    String type = getTTable().getParameters().get("type");
    if (type == null) {
      type = "";
    }
    return type;
  }

  public void setProjection(String projection) {
    getTTable().getParameters().put("projection", projection);

  }

  public String getProjection() {
    String projection = getTTable().getParameters().get("projection");
    if (projection == null) {
      projection = "";
    }
    return projection;
  }

  public void setIndexNum(String num) {
    getTTable().getParameters().put("indexNum", num);
  }

  public void setIndexInfo(int count, String info) {
    String key = "index" + count;
    getTTable().getParameters().put(key, info);
  }

  public ArrayList<String> getColComments() {
    return colComments;
  }

  public void setColComments(ArrayList<String> colComments) {
    this.colComments = colComments;
  }
};
