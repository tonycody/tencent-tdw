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
package org.apache.hadoop.hive.metastore.api;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class StorageDescriptor implements
    org.apache.thrift.TBase<StorageDescriptor, StorageDescriptor._Fields>,
    java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct(
      "StorageDescriptor");

  private static final org.apache.thrift.protocol.TField COLS_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "cols", org.apache.thrift.protocol.TType.LIST, (short) 1);
  private static final org.apache.thrift.protocol.TField LOCATION_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "location", org.apache.thrift.protocol.TType.STRING, (short) 2);
  private static final org.apache.thrift.protocol.TField INPUT_FORMAT_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "inputFormat", org.apache.thrift.protocol.TType.STRING, (short) 3);
  private static final org.apache.thrift.protocol.TField OUTPUT_FORMAT_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "outputFormat", org.apache.thrift.protocol.TType.STRING, (short) 4);
  private static final org.apache.thrift.protocol.TField COMPRESSED_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "compressed", org.apache.thrift.protocol.TType.BOOL, (short) 5);
  private static final org.apache.thrift.protocol.TField NUM_BUCKETS_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "numBuckets", org.apache.thrift.protocol.TType.I32, (short) 6);
  private static final org.apache.thrift.protocol.TField SERDE_INFO_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "serdeInfo", org.apache.thrift.protocol.TType.STRUCT, (short) 7);
  private static final org.apache.thrift.protocol.TField BUCKET_COLS_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "bucketCols", org.apache.thrift.protocol.TType.LIST, (short) 8);
  private static final org.apache.thrift.protocol.TField SORT_COLS_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "sortCols", org.apache.thrift.protocol.TType.LIST, (short) 9);
  private static final org.apache.thrift.protocol.TField PARAMETERS_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "parameters", org.apache.thrift.protocol.TType.MAP, (short) 10);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class,
        new StorageDescriptorStandardSchemeFactory());
    schemes.put(TupleScheme.class, new StorageDescriptorTupleSchemeFactory());
  }

  private List<FieldSchema> cols;
  private String location;
  private String inputFormat;
  private String outputFormat;
  private boolean compressed;
  private int numBuckets;
  private SerDeInfo serdeInfo;
  private List<String> bucketCols;
  private List<Order> sortCols;
  private Map<String, String> parameters;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    COLS((short) 1, "cols"), LOCATION((short) 2, "location"), INPUT_FORMAT(
        (short) 3, "inputFormat"), OUTPUT_FORMAT((short) 4, "outputFormat"), COMPRESSED(
        (short) 5, "compressed"), NUM_BUCKETS((short) 6, "numBuckets"), SERDE_INFO(
        (short) 7, "serdeInfo"), BUCKET_COLS((short) 8, "bucketCols"), SORT_COLS(
        (short) 9, "sortCols"), PARAMETERS((short) 10, "parameters");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    public static _Fields findByThriftId(int fieldId) {
      switch (fieldId) {
      case 1:
        return COLS;
      case 2:
        return LOCATION;
      case 3:
        return INPUT_FORMAT;
      case 4:
        return OUTPUT_FORMAT;
      case 5:
        return COMPRESSED;
      case 6:
        return NUM_BUCKETS;
      case 7:
        return SERDE_INFO;
      case 8:
        return BUCKET_COLS;
      case 9:
        return SORT_COLS;
      case 10:
        return PARAMETERS;
      default:
        return null;
      }
    }

    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null)
        throw new IllegalArgumentException("Field " + fieldId
            + " doesn't exist!");
      return fields;
    }

    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  private static final int __COMPRESSED_ISSET_ID = 0;
  private static final int __NUMBUCKETS_ISSET_ID = 1;
  private BitSet __isset_bit_vector = new BitSet(2);
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(
        _Fields.class);
    tmpMap.put(_Fields.COLS, new org.apache.thrift.meta_data.FieldMetaData(
        "cols", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.ListMetaData(
            org.apache.thrift.protocol.TType.LIST,
            new org.apache.thrift.meta_data.StructMetaData(
                org.apache.thrift.protocol.TType.STRUCT, FieldSchema.class))));
    tmpMap.put(_Fields.LOCATION, new org.apache.thrift.meta_data.FieldMetaData(
        "location", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.INPUT_FORMAT,
        new org.apache.thrift.meta_data.FieldMetaData("inputFormat",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.OUTPUT_FORMAT,
        new org.apache.thrift.meta_data.FieldMetaData("outputFormat",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.COMPRESSED,
        new org.apache.thrift.meta_data.FieldMetaData("compressed",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.NUM_BUCKETS,
        new org.apache.thrift.meta_data.FieldMetaData("numBuckets",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.SERDE_INFO,
        new org.apache.thrift.meta_data.FieldMetaData("serdeInfo",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.StructMetaData(
                org.apache.thrift.protocol.TType.STRUCT, SerDeInfo.class)));
    tmpMap.put(_Fields.BUCKET_COLS,
        new org.apache.thrift.meta_data.FieldMetaData("bucketCols",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.ListMetaData(
                org.apache.thrift.protocol.TType.LIST,
                new org.apache.thrift.meta_data.FieldValueMetaData(
                    org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.SORT_COLS,
        new org.apache.thrift.meta_data.FieldMetaData("sortCols",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.ListMetaData(
                org.apache.thrift.protocol.TType.LIST,
                new org.apache.thrift.meta_data.StructMetaData(
                    org.apache.thrift.protocol.TType.STRUCT, Order.class))));
    tmpMap.put(_Fields.PARAMETERS,
        new org.apache.thrift.meta_data.FieldMetaData("parameters",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.MapMetaData(
                org.apache.thrift.protocol.TType.MAP,
                new org.apache.thrift.meta_data.FieldValueMetaData(
                    org.apache.thrift.protocol.TType.STRING),
                new org.apache.thrift.meta_data.FieldValueMetaData(
                    org.apache.thrift.protocol.TType.STRING))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(
        StorageDescriptor.class, metaDataMap);
  }

  public StorageDescriptor() {
  }

  public StorageDescriptor(List<FieldSchema> cols, String location,
      String inputFormat, String outputFormat, boolean compressed,
      int numBuckets, SerDeInfo serdeInfo, List<String> bucketCols,
      List<Order> sortCols, Map<String, String> parameters) {
    this();
    this.cols = cols;
    this.location = location;
    this.inputFormat = inputFormat;
    this.outputFormat = outputFormat;
    this.compressed = compressed;
    setCompressedIsSet(true);
    this.numBuckets = numBuckets;
    setNumBucketsIsSet(true);
    this.serdeInfo = serdeInfo;
    this.bucketCols = bucketCols;
    this.sortCols = sortCols;
    this.parameters = parameters;
  }

  public StorageDescriptor(StorageDescriptor other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    if (other.isSetCols()) {
      List<FieldSchema> __this__cols = new ArrayList<FieldSchema>();
      for (FieldSchema other_element : other.cols) {
        __this__cols.add(new FieldSchema(other_element));
      }
      this.cols = __this__cols;
    }
    if (other.isSetLocation()) {
      this.location = other.location;
    }
    if (other.isSetInputFormat()) {
      this.inputFormat = other.inputFormat;
    }
    if (other.isSetOutputFormat()) {
      this.outputFormat = other.outputFormat;
    }
    this.compressed = other.compressed;
    this.numBuckets = other.numBuckets;
    if (other.isSetSerdeInfo()) {
      this.serdeInfo = new SerDeInfo(other.serdeInfo);
    }
    if (other.isSetBucketCols()) {
      List<String> __this__bucketCols = new ArrayList<String>();
      for (String other_element : other.bucketCols) {
        __this__bucketCols.add(other_element);
      }
      this.bucketCols = __this__bucketCols;
    }
    if (other.isSetSortCols()) {
      List<Order> __this__sortCols = new ArrayList<Order>();
      for (Order other_element : other.sortCols) {
        __this__sortCols.add(new Order(other_element));
      }
      this.sortCols = __this__sortCols;
    }
    if (other.isSetParameters()) {
      Map<String, String> __this__parameters = new HashMap<String, String>();
      for (Map.Entry<String, String> other_element : other.parameters
          .entrySet()) {

        String other_element_key = other_element.getKey();
        String other_element_value = other_element.getValue();

        String __this__parameters_copy_key = other_element_key;

        String __this__parameters_copy_value = other_element_value;

        __this__parameters.put(__this__parameters_copy_key,
            __this__parameters_copy_value);
      }
      this.parameters = __this__parameters;
    }
  }

  public StorageDescriptor deepCopy() {
    return new StorageDescriptor(this);
  }

  @Override
  public void clear() {
    this.cols = null;
    this.location = null;
    this.inputFormat = null;
    this.outputFormat = null;
    setCompressedIsSet(false);
    this.compressed = false;
    setNumBucketsIsSet(false);
    this.numBuckets = 0;
    this.serdeInfo = null;
    this.bucketCols = null;
    this.sortCols = null;
    this.parameters = null;
  }

  public int getColsSize() {
    return (this.cols == null) ? 0 : this.cols.size();
  }

  public java.util.Iterator<FieldSchema> getColsIterator() {
    return (this.cols == null) ? null : this.cols.iterator();
  }

  public void addToCols(FieldSchema elem) {
    if (this.cols == null) {
      this.cols = new ArrayList<FieldSchema>();
    }
    this.cols.add(elem);
  }

  public List<FieldSchema> getCols() {
    return this.cols;
  }

  public void setCols(List<FieldSchema> cols) {
    this.cols = cols;
  }

  public void unsetCols() {
    this.cols = null;
  }

  public boolean isSetCols() {
    return this.cols != null;
  }

  public void setColsIsSet(boolean value) {
    if (!value) {
      this.cols = null;
    }
  }

  public String getLocation() {
    return this.location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public void unsetLocation() {
    this.location = null;
  }

  public boolean isSetLocation() {
    return this.location != null;
  }

  public void setLocationIsSet(boolean value) {
    if (!value) {
      this.location = null;
    }
  }

  public String getInputFormat() {
    return this.inputFormat;
  }

  public void setInputFormat(String inputFormat) {
    this.inputFormat = inputFormat;
  }

  public void unsetInputFormat() {
    this.inputFormat = null;
  }

  public boolean isSetInputFormat() {
    return this.inputFormat != null;
  }

  public void setInputFormatIsSet(boolean value) {
    if (!value) {
      this.inputFormat = null;
    }
  }

  public String getOutputFormat() {
    return this.outputFormat;
  }

  public void setOutputFormat(String outputFormat) {
    this.outputFormat = outputFormat;
  }

  public void unsetOutputFormat() {
    this.outputFormat = null;
  }

  public boolean isSetOutputFormat() {
    return this.outputFormat != null;
  }

  public void setOutputFormatIsSet(boolean value) {
    if (!value) {
      this.outputFormat = null;
    }
  }

  public boolean isCompressed() {
    return this.compressed;
  }

  public void setCompressed(boolean compressed) {
    this.compressed = compressed;
    setCompressedIsSet(true);
  }

  public void unsetCompressed() {
    __isset_bit_vector.clear(__COMPRESSED_ISSET_ID);
  }

  public boolean isSetCompressed() {
    return __isset_bit_vector.get(__COMPRESSED_ISSET_ID);
  }

  public void setCompressedIsSet(boolean value) {
    __isset_bit_vector.set(__COMPRESSED_ISSET_ID, value);
  }

  public int getNumBuckets() {
    return this.numBuckets;
  }

  public void setNumBuckets(int numBuckets) {
    this.numBuckets = numBuckets;
    setNumBucketsIsSet(true);
  }

  public void unsetNumBuckets() {
    __isset_bit_vector.clear(__NUMBUCKETS_ISSET_ID);
  }

  public boolean isSetNumBuckets() {
    return __isset_bit_vector.get(__NUMBUCKETS_ISSET_ID);
  }

  public void setNumBucketsIsSet(boolean value) {
    __isset_bit_vector.set(__NUMBUCKETS_ISSET_ID, value);
  }

  public SerDeInfo getSerdeInfo() {
    return this.serdeInfo;
  }

  public void setSerdeInfo(SerDeInfo serdeInfo) {
    this.serdeInfo = serdeInfo;
  }

  public void unsetSerdeInfo() {
    this.serdeInfo = null;
  }

  public boolean isSetSerdeInfo() {
    return this.serdeInfo != null;
  }

  public void setSerdeInfoIsSet(boolean value) {
    if (!value) {
      this.serdeInfo = null;
    }
  }

  public int getBucketColsSize() {
    return (this.bucketCols == null) ? 0 : this.bucketCols.size();
  }

  public java.util.Iterator<String> getBucketColsIterator() {
    return (this.bucketCols == null) ? null : this.bucketCols.iterator();
  }

  public void addToBucketCols(String elem) {
    if (this.bucketCols == null) {
      this.bucketCols = new ArrayList<String>();
    }
    this.bucketCols.add(elem);
  }

  public List<String> getBucketCols() {
    return this.bucketCols;
  }

  public void setBucketCols(List<String> bucketCols) {
    this.bucketCols = bucketCols;
  }

  public void unsetBucketCols() {
    this.bucketCols = null;
  }

  public boolean isSetBucketCols() {
    return this.bucketCols != null;
  }

  public void setBucketColsIsSet(boolean value) {
    if (!value) {
      this.bucketCols = null;
    }
  }

  public int getSortColsSize() {
    return (this.sortCols == null) ? 0 : this.sortCols.size();
  }

  public java.util.Iterator<Order> getSortColsIterator() {
    return (this.sortCols == null) ? null : this.sortCols.iterator();
  }

  public void addToSortCols(Order elem) {
    if (this.sortCols == null) {
      this.sortCols = new ArrayList<Order>();
    }
    this.sortCols.add(elem);
  }

  public List<Order> getSortCols() {
    return this.sortCols;
  }

  public void setSortCols(List<Order> sortCols) {
    this.sortCols = sortCols;
  }

  public void unsetSortCols() {
    this.sortCols = null;
  }

  public boolean isSetSortCols() {
    return this.sortCols != null;
  }

  public void setSortColsIsSet(boolean value) {
    if (!value) {
      this.sortCols = null;
    }
  }

  public int getParametersSize() {
    return (this.parameters == null) ? 0 : this.parameters.size();
  }

  public void putToParameters(String key, String val) {
    if (this.parameters == null) {
      this.parameters = new HashMap<String, String>();
    }
    this.parameters.put(key, val);
  }

  public Map<String, String> getParameters() {
    return this.parameters;
  }

  public void setParameters(Map<String, String> parameters) {
    this.parameters = parameters;
  }

  public void unsetParameters() {
    this.parameters = null;
  }

  public boolean isSetParameters() {
    return this.parameters != null;
  }

  public void setParametersIsSet(boolean value) {
    if (!value) {
      this.parameters = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case COLS:
      if (value == null) {
        unsetCols();
      } else {
        setCols((List<FieldSchema>) value);
      }
      break;

    case LOCATION:
      if (value == null) {
        unsetLocation();
      } else {
        setLocation((String) value);
      }
      break;

    case INPUT_FORMAT:
      if (value == null) {
        unsetInputFormat();
      } else {
        setInputFormat((String) value);
      }
      break;

    case OUTPUT_FORMAT:
      if (value == null) {
        unsetOutputFormat();
      } else {
        setOutputFormat((String) value);
      }
      break;

    case COMPRESSED:
      if (value == null) {
        unsetCompressed();
      } else {
        setCompressed((Boolean) value);
      }
      break;

    case NUM_BUCKETS:
      if (value == null) {
        unsetNumBuckets();
      } else {
        setNumBuckets((Integer) value);
      }
      break;

    case SERDE_INFO:
      if (value == null) {
        unsetSerdeInfo();
      } else {
        setSerdeInfo((SerDeInfo) value);
      }
      break;

    case BUCKET_COLS:
      if (value == null) {
        unsetBucketCols();
      } else {
        setBucketCols((List<String>) value);
      }
      break;

    case SORT_COLS:
      if (value == null) {
        unsetSortCols();
      } else {
        setSortCols((List<Order>) value);
      }
      break;

    case PARAMETERS:
      if (value == null) {
        unsetParameters();
      } else {
        setParameters((Map<String, String>) value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case COLS:
      return getCols();

    case LOCATION:
      return getLocation();

    case INPUT_FORMAT:
      return getInputFormat();

    case OUTPUT_FORMAT:
      return getOutputFormat();

    case COMPRESSED:
      return Boolean.valueOf(isCompressed());

    case NUM_BUCKETS:
      return Integer.valueOf(getNumBuckets());

    case SERDE_INFO:
      return getSerdeInfo();

    case BUCKET_COLS:
      return getBucketCols();

    case SORT_COLS:
      return getSortCols();

    case PARAMETERS:
      return getParameters();

    }
    throw new IllegalStateException();
  }

  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case COLS:
      return isSetCols();
    case LOCATION:
      return isSetLocation();
    case INPUT_FORMAT:
      return isSetInputFormat();
    case OUTPUT_FORMAT:
      return isSetOutputFormat();
    case COMPRESSED:
      return isSetCompressed();
    case NUM_BUCKETS:
      return isSetNumBuckets();
    case SERDE_INFO:
      return isSetSerdeInfo();
    case BUCKET_COLS:
      return isSetBucketCols();
    case SORT_COLS:
      return isSetSortCols();
    case PARAMETERS:
      return isSetParameters();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof StorageDescriptor)
      return this.equals((StorageDescriptor) that);
    return false;
  }

  public boolean equals(StorageDescriptor that) {
    if (that == null)
      return false;

    boolean this_present_cols = true && this.isSetCols();
    boolean that_present_cols = true && that.isSetCols();
    if (this_present_cols || that_present_cols) {
      if (!(this_present_cols && that_present_cols))
        return false;
      if (!this.cols.equals(that.cols))
        return false;
    }

    boolean this_present_location = true && this.isSetLocation();
    boolean that_present_location = true && that.isSetLocation();
    if (this_present_location || that_present_location) {
      if (!(this_present_location && that_present_location))
        return false;
      if (!this.location.equals(that.location))
        return false;
    }

    boolean this_present_inputFormat = true && this.isSetInputFormat();
    boolean that_present_inputFormat = true && that.isSetInputFormat();
    if (this_present_inputFormat || that_present_inputFormat) {
      if (!(this_present_inputFormat && that_present_inputFormat))
        return false;
      if (!this.inputFormat.equals(that.inputFormat))
        return false;
    }

    boolean this_present_outputFormat = true && this.isSetOutputFormat();
    boolean that_present_outputFormat = true && that.isSetOutputFormat();
    if (this_present_outputFormat || that_present_outputFormat) {
      if (!(this_present_outputFormat && that_present_outputFormat))
        return false;
      if (!this.outputFormat.equals(that.outputFormat))
        return false;
    }

    boolean this_present_compressed = true;
    boolean that_present_compressed = true;
    if (this_present_compressed || that_present_compressed) {
      if (!(this_present_compressed && that_present_compressed))
        return false;
      if (this.compressed != that.compressed)
        return false;
    }

    boolean this_present_numBuckets = true;
    boolean that_present_numBuckets = true;
    if (this_present_numBuckets || that_present_numBuckets) {
      if (!(this_present_numBuckets && that_present_numBuckets))
        return false;
      if (this.numBuckets != that.numBuckets)
        return false;
    }

    boolean this_present_serdeInfo = true && this.isSetSerdeInfo();
    boolean that_present_serdeInfo = true && that.isSetSerdeInfo();
    if (this_present_serdeInfo || that_present_serdeInfo) {
      if (!(this_present_serdeInfo && that_present_serdeInfo))
        return false;
      if (!this.serdeInfo.equals(that.serdeInfo))
        return false;
    }

    boolean this_present_bucketCols = true && this.isSetBucketCols();
    boolean that_present_bucketCols = true && that.isSetBucketCols();
    if (this_present_bucketCols || that_present_bucketCols) {
      if (!(this_present_bucketCols && that_present_bucketCols))
        return false;
      if (!this.bucketCols.equals(that.bucketCols))
        return false;
    }

    boolean this_present_sortCols = true && this.isSetSortCols();
    boolean that_present_sortCols = true && that.isSetSortCols();
    if (this_present_sortCols || that_present_sortCols) {
      if (!(this_present_sortCols && that_present_sortCols))
        return false;
      if (!this.sortCols.equals(that.sortCols))
        return false;
    }

    boolean this_present_parameters = true && this.isSetParameters();
    boolean that_present_parameters = true && that.isSetParameters();
    if (this_present_parameters || that_present_parameters) {
      if (!(this_present_parameters && that_present_parameters))
        return false;
      if (!this.parameters.equals(that.parameters))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(StorageDescriptor other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    StorageDescriptor typedOther = (StorageDescriptor) other;

    lastComparison = Boolean.valueOf(isSetCols()).compareTo(
        typedOther.isSetCols());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCols()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.cols,
          typedOther.cols);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetLocation()).compareTo(
        typedOther.isSetLocation());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLocation()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.location,
          typedOther.location);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetInputFormat()).compareTo(
        typedOther.isSetInputFormat());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetInputFormat()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.inputFormat, typedOther.inputFormat);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetOutputFormat()).compareTo(
        typedOther.isSetOutputFormat());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetOutputFormat()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.outputFormat, typedOther.outputFormat);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetCompressed()).compareTo(
        typedOther.isSetCompressed());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCompressed()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.compressed,
          typedOther.compressed);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetNumBuckets()).compareTo(
        typedOther.isSetNumBuckets());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetNumBuckets()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.numBuckets,
          typedOther.numBuckets);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetSerdeInfo()).compareTo(
        typedOther.isSetSerdeInfo());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSerdeInfo()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.serdeInfo,
          typedOther.serdeInfo);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetBucketCols()).compareTo(
        typedOther.isSetBucketCols());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetBucketCols()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.bucketCols,
          typedOther.bucketCols);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetSortCols()).compareTo(
        typedOther.isSetSortCols());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSortCols()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.sortCols,
          typedOther.sortCols);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetParameters()).compareTo(
        typedOther.isSetParameters());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetParameters()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.parameters,
          typedOther.parameters);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot)
      throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot)
      throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("StorageDescriptor(");
    boolean first = true;

    sb.append("cols:");
    if (this.cols == null) {
      sb.append("null");
    } else {
      sb.append(this.cols);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("location:");
    if (this.location == null) {
      sb.append("null");
    } else {
      sb.append(this.location);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("inputFormat:");
    if (this.inputFormat == null) {
      sb.append("null");
    } else {
      sb.append(this.inputFormat);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("outputFormat:");
    if (this.outputFormat == null) {
      sb.append("null");
    } else {
      sb.append(this.outputFormat);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("compressed:");
    sb.append(this.compressed);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("numBuckets:");
    sb.append(this.numBuckets);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("serdeInfo:");
    if (this.serdeInfo == null) {
      sb.append("null");
    } else {
      sb.append(this.serdeInfo);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("bucketCols:");
    if (this.bucketCols == null) {
      sb.append("null");
    } else {
      sb.append(this.bucketCols);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("sortCols:");
    if (this.sortCols == null) {
      sb.append("null");
    } else {
      sb.append(this.sortCols);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("parameters:");
    if (this.parameters == null) {
      sb.append("null");
    } else {
      sb.append(this.parameters);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
  }

  private void writeObject(java.io.ObjectOutputStream out)
      throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(
          new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in)
      throws java.io.IOException, ClassNotFoundException {
    try {
      __isset_bit_vector = new BitSet(1);
      read(new org.apache.thrift.protocol.TCompactProtocol(
          new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class StorageDescriptorStandardSchemeFactory implements
      SchemeFactory {
    public StorageDescriptorStandardScheme getScheme() {
      return new StorageDescriptorStandardScheme();
    }
  }

  private static class StorageDescriptorStandardScheme extends
      StandardScheme<StorageDescriptor> {

    public void read(org.apache.thrift.protocol.TProtocol iprot,
        StorageDescriptor struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true) {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) {
          break;
        }
        switch (schemeField.id) {
        case 1:
          if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
            {
              org.apache.thrift.protocol.TList _list18 = iprot.readListBegin();
              struct.cols = new ArrayList<FieldSchema>(_list18.size);
              for (int _i19 = 0; _i19 < _list18.size; ++_i19) {
                FieldSchema _elem20;
                _elem20 = new FieldSchema();
                _elem20.read(iprot);
                struct.cols.add(_elem20);
              }
              iprot.readListEnd();
            }
            struct.setColsIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 2:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.location = iprot.readString();
            struct.setLocationIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 3:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.inputFormat = iprot.readString();
            struct.setInputFormatIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 4:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.outputFormat = iprot.readString();
            struct.setOutputFormatIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 5:
          if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
            struct.compressed = iprot.readBool();
            struct.setCompressedIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 6:
          if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
            struct.numBuckets = iprot.readI32();
            struct.setNumBucketsIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 7:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
            struct.serdeInfo = new SerDeInfo();
            struct.serdeInfo.read(iprot);
            struct.setSerdeInfoIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 8:
          if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
            {
              org.apache.thrift.protocol.TList _list21 = iprot.readListBegin();
              struct.bucketCols = new ArrayList<String>(_list21.size);
              for (int _i22 = 0; _i22 < _list21.size; ++_i22) {
                String _elem23;
                _elem23 = iprot.readString();
                struct.bucketCols.add(_elem23);
              }
              iprot.readListEnd();
            }
            struct.setBucketColsIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 9:
          if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
            {
              org.apache.thrift.protocol.TList _list24 = iprot.readListBegin();
              struct.sortCols = new ArrayList<Order>(_list24.size);
              for (int _i25 = 0; _i25 < _list24.size; ++_i25) {
                Order _elem26;
                _elem26 = new Order();
                _elem26.read(iprot);
                struct.sortCols.add(_elem26);
              }
              iprot.readListEnd();
            }
            struct.setSortColsIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 10:
          if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
            {
              org.apache.thrift.protocol.TMap _map27 = iprot.readMapBegin();
              struct.parameters = new HashMap<String, String>(2 * _map27.size);
              for (int _i28 = 0; _i28 < _map27.size; ++_i28) {
                String _key29;
                String _val30;
                _key29 = iprot.readString();
                _val30 = iprot.readString();
                struct.parameters.put(_key29, _val30);
              }
              iprot.readMapEnd();
            }
            struct.setParametersIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        default:
          org.apache.thrift.protocol.TProtocolUtil
              .skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot,
        StorageDescriptor struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.cols != null) {
        oprot.writeFieldBegin(COLS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(
              org.apache.thrift.protocol.TType.STRUCT, struct.cols.size()));
          for (FieldSchema _iter31 : struct.cols) {
            _iter31.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.location != null) {
        oprot.writeFieldBegin(LOCATION_FIELD_DESC);
        oprot.writeString(struct.location);
        oprot.writeFieldEnd();
      }
      if (struct.inputFormat != null) {
        oprot.writeFieldBegin(INPUT_FORMAT_FIELD_DESC);
        oprot.writeString(struct.inputFormat);
        oprot.writeFieldEnd();
      }
      if (struct.outputFormat != null) {
        oprot.writeFieldBegin(OUTPUT_FORMAT_FIELD_DESC);
        oprot.writeString(struct.outputFormat);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(COMPRESSED_FIELD_DESC);
      oprot.writeBool(struct.compressed);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(NUM_BUCKETS_FIELD_DESC);
      oprot.writeI32(struct.numBuckets);
      oprot.writeFieldEnd();
      if (struct.serdeInfo != null) {
        oprot.writeFieldBegin(SERDE_INFO_FIELD_DESC);
        struct.serdeInfo.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.bucketCols != null) {
        oprot.writeFieldBegin(BUCKET_COLS_FIELD_DESC);
        {
          oprot
              .writeListBegin(new org.apache.thrift.protocol.TList(
                  org.apache.thrift.protocol.TType.STRING, struct.bucketCols
                      .size()));
          for (String _iter32 : struct.bucketCols) {
            oprot.writeString(_iter32);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.sortCols != null) {
        oprot.writeFieldBegin(SORT_COLS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(
              org.apache.thrift.protocol.TType.STRUCT, struct.sortCols.size()));
          for (Order _iter33 : struct.sortCols) {
            _iter33.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.parameters != null) {
        oprot.writeFieldBegin(PARAMETERS_FIELD_DESC);
        {
          oprot
              .writeMapBegin(new org.apache.thrift.protocol.TMap(
                  org.apache.thrift.protocol.TType.STRING,
                  org.apache.thrift.protocol.TType.STRING, struct.parameters
                      .size()));
          for (Map.Entry<String, String> _iter34 : struct.parameters.entrySet()) {
            oprot.writeString(_iter34.getKey());
            oprot.writeString(_iter34.getValue());
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class StorageDescriptorTupleSchemeFactory implements
      SchemeFactory {
    public StorageDescriptorTupleScheme getScheme() {
      return new StorageDescriptorTupleScheme();
    }
  }

  private static class StorageDescriptorTupleScheme extends
      TupleScheme<StorageDescriptor> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot,
        StorageDescriptor struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetCols()) {
        optionals.set(0);
      }
      if (struct.isSetLocation()) {
        optionals.set(1);
      }
      if (struct.isSetInputFormat()) {
        optionals.set(2);
      }
      if (struct.isSetOutputFormat()) {
        optionals.set(3);
      }
      if (struct.isSetCompressed()) {
        optionals.set(4);
      }
      if (struct.isSetNumBuckets()) {
        optionals.set(5);
      }
      if (struct.isSetSerdeInfo()) {
        optionals.set(6);
      }
      if (struct.isSetBucketCols()) {
        optionals.set(7);
      }
      if (struct.isSetSortCols()) {
        optionals.set(8);
      }
      if (struct.isSetParameters()) {
        optionals.set(9);
      }
      oprot.writeBitSet(optionals, 10);
      if (struct.isSetCols()) {
        {
          oprot.writeI32(struct.cols.size());
          for (FieldSchema _iter35 : struct.cols) {
            _iter35.write(oprot);
          }
        }
      }
      if (struct.isSetLocation()) {
        oprot.writeString(struct.location);
      }
      if (struct.isSetInputFormat()) {
        oprot.writeString(struct.inputFormat);
      }
      if (struct.isSetOutputFormat()) {
        oprot.writeString(struct.outputFormat);
      }
      if (struct.isSetCompressed()) {
        oprot.writeBool(struct.compressed);
      }
      if (struct.isSetNumBuckets()) {
        oprot.writeI32(struct.numBuckets);
      }
      if (struct.isSetSerdeInfo()) {
        struct.serdeInfo.write(oprot);
      }
      if (struct.isSetBucketCols()) {
        {
          oprot.writeI32(struct.bucketCols.size());
          for (String _iter36 : struct.bucketCols) {
            oprot.writeString(_iter36);
          }
        }
      }
      if (struct.isSetSortCols()) {
        {
          oprot.writeI32(struct.sortCols.size());
          for (Order _iter37 : struct.sortCols) {
            _iter37.write(oprot);
          }
        }
      }
      if (struct.isSetParameters()) {
        {
          oprot.writeI32(struct.parameters.size());
          for (Map.Entry<String, String> _iter38 : struct.parameters.entrySet()) {
            oprot.writeString(_iter38.getKey());
            oprot.writeString(_iter38.getValue());
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot,
        StorageDescriptor struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(10);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TList _list39 = new org.apache.thrift.protocol.TList(
              org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.cols = new ArrayList<FieldSchema>(_list39.size);
          for (int _i40 = 0; _i40 < _list39.size; ++_i40) {
            FieldSchema _elem41;
            _elem41 = new FieldSchema();
            _elem41.read(iprot);
            struct.cols.add(_elem41);
          }
        }
        struct.setColsIsSet(true);
      }
      if (incoming.get(1)) {
        struct.location = iprot.readString();
        struct.setLocationIsSet(true);
      }
      if (incoming.get(2)) {
        struct.inputFormat = iprot.readString();
        struct.setInputFormatIsSet(true);
      }
      if (incoming.get(3)) {
        struct.outputFormat = iprot.readString();
        struct.setOutputFormatIsSet(true);
      }
      if (incoming.get(4)) {
        struct.compressed = iprot.readBool();
        struct.setCompressedIsSet(true);
      }
      if (incoming.get(5)) {
        struct.numBuckets = iprot.readI32();
        struct.setNumBucketsIsSet(true);
      }
      if (incoming.get(6)) {
        struct.serdeInfo = new SerDeInfo();
        struct.serdeInfo.read(iprot);
        struct.setSerdeInfoIsSet(true);
      }
      if (incoming.get(7)) {
        {
          org.apache.thrift.protocol.TList _list42 = new org.apache.thrift.protocol.TList(
              org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.bucketCols = new ArrayList<String>(_list42.size);
          for (int _i43 = 0; _i43 < _list42.size; ++_i43) {
            String _elem44;
            _elem44 = iprot.readString();
            struct.bucketCols.add(_elem44);
          }
        }
        struct.setBucketColsIsSet(true);
      }
      if (incoming.get(8)) {
        {
          org.apache.thrift.protocol.TList _list45 = new org.apache.thrift.protocol.TList(
              org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.sortCols = new ArrayList<Order>(_list45.size);
          for (int _i46 = 0; _i46 < _list45.size; ++_i46) {
            Order _elem47;
            _elem47 = new Order();
            _elem47.read(iprot);
            struct.sortCols.add(_elem47);
          }
        }
        struct.setSortColsIsSet(true);
      }
      if (incoming.get(9)) {
        {
          org.apache.thrift.protocol.TMap _map48 = new org.apache.thrift.protocol.TMap(
              org.apache.thrift.protocol.TType.STRING,
              org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.parameters = new HashMap<String, String>(2 * _map48.size);
          for (int _i49 = 0; _i49 < _map48.size; ++_i49) {
            String _key50;
            String _val51;
            _key50 = iprot.readString();
            _val51 = iprot.readString();
            struct.parameters.put(_key50, _val51);
          }
        }
        struct.setParametersIsSet(true);
      }
    }
  }

}
