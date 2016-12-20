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

public class Index implements org.apache.thrift.TBase<Index, Index._Fields>,
    java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct(
      "Index");

  private static final org.apache.thrift.protocol.TField INDEX_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "indexName", org.apache.thrift.protocol.TType.STRING, (short) 1);
  private static final org.apache.thrift.protocol.TField INDEX_TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "indexType", org.apache.thrift.protocol.TType.I32, (short) 2);
  private static final org.apache.thrift.protocol.TField TABLE_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "tableName", org.apache.thrift.protocol.TType.STRING, (short) 3);
  private static final org.apache.thrift.protocol.TField DB_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "dbName", org.apache.thrift.protocol.TType.STRING, (short) 4);
  private static final org.apache.thrift.protocol.TField COL_NAMES_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "colNames", org.apache.thrift.protocol.TType.LIST, (short) 5);
  private static final org.apache.thrift.protocol.TField PART_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "partName", org.apache.thrift.protocol.TType.STRING, (short) 6);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new IndexStandardSchemeFactory());
    schemes.put(TupleScheme.class, new IndexTupleSchemeFactory());
  }

  private String indexName;
  private int indexType;
  private String tableName;
  private String dbName;
  private List<String> colNames;
  private String partName;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    INDEX_NAME((short) 1, "indexName"), INDEX_TYPE((short) 2, "indexType"), TABLE_NAME(
        (short) 3, "tableName"), DB_NAME((short) 4, "dbName"), COL_NAMES(
        (short) 5, "colNames"), PART_NAME((short) 6, "partName");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    public static _Fields findByThriftId(int fieldId) {
      switch (fieldId) {
      case 1:
        return INDEX_NAME;
      case 2:
        return INDEX_TYPE;
      case 3:
        return TABLE_NAME;
      case 4:
        return DB_NAME;
      case 5:
        return COL_NAMES;
      case 6:
        return PART_NAME;
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

  private static final int __INDEXTYPE_ISSET_ID = 0;
  private BitSet __isset_bit_vector = new BitSet(1);
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(
        _Fields.class);
    tmpMap.put(_Fields.INDEX_NAME,
        new org.apache.thrift.meta_data.FieldMetaData("indexName",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.INDEX_TYPE,
        new org.apache.thrift.meta_data.FieldMetaData("indexType",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.TABLE_NAME,
        new org.apache.thrift.meta_data.FieldMetaData("tableName",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.DB_NAME, new org.apache.thrift.meta_data.FieldMetaData(
        "dbName", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.COL_NAMES,
        new org.apache.thrift.meta_data.FieldMetaData("colNames",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.ListMetaData(
                org.apache.thrift.protocol.TType.LIST,
                new org.apache.thrift.meta_data.FieldValueMetaData(
                    org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.PART_NAME,
        new org.apache.thrift.meta_data.FieldMetaData("partName",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(Index.class,
        metaDataMap);
  }

  public Index() {
  }

  public Index(String indexName, int indexType, String tableName,
      String dbName, List<String> colNames, String partName) {
    this();
    this.indexName = indexName;
    this.indexType = indexType;
    setIndexTypeIsSet(true);
    this.tableName = tableName;
    this.dbName = dbName;
    this.colNames = colNames;
    this.partName = partName;
  }

  public Index(Index other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    if (other.isSetIndexName()) {
      this.indexName = other.indexName;
    }
    this.indexType = other.indexType;
    if (other.isSetTableName()) {
      this.tableName = other.tableName;
    }
    if (other.isSetDbName()) {
      this.dbName = other.dbName;
    }
    if (other.isSetColNames()) {
      List<String> __this__colNames = new ArrayList<String>();
      for (String other_element : other.colNames) {
        __this__colNames.add(other_element);
      }
      this.colNames = __this__colNames;
    }
    if (other.isSetPartName()) {
      this.partName = other.partName;
    }
  }

  public Index deepCopy() {
    return new Index(this);
  }

  @Override
  public void clear() {
    this.indexName = null;
    setIndexTypeIsSet(false);
    this.indexType = 0;
    this.tableName = null;
    this.dbName = null;
    this.colNames = null;
    this.partName = null;
  }

  public String getIndexName() {
    return this.indexName;
  }

  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  public void unsetIndexName() {
    this.indexName = null;
  }

  public boolean isSetIndexName() {
    return this.indexName != null;
  }

  public void setIndexNameIsSet(boolean value) {
    if (!value) {
      this.indexName = null;
    }
  }

  public int getIndexType() {
    return this.indexType;
  }

  public void setIndexType(int indexType) {
    this.indexType = indexType;
    setIndexTypeIsSet(true);
  }

  public void unsetIndexType() {
    __isset_bit_vector.clear(__INDEXTYPE_ISSET_ID);
  }

  public boolean isSetIndexType() {
    return __isset_bit_vector.get(__INDEXTYPE_ISSET_ID);
  }

  public void setIndexTypeIsSet(boolean value) {
    __isset_bit_vector.set(__INDEXTYPE_ISSET_ID, value);
  }

  public String getTableName() {
    return this.tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void unsetTableName() {
    this.tableName = null;
  }

  public boolean isSetTableName() {
    return this.tableName != null;
  }

  public void setTableNameIsSet(boolean value) {
    if (!value) {
      this.tableName = null;
    }
  }

  public String getDbName() {
    return this.dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public void unsetDbName() {
    this.dbName = null;
  }

  public boolean isSetDbName() {
    return this.dbName != null;
  }

  public void setDbNameIsSet(boolean value) {
    if (!value) {
      this.dbName = null;
    }
  }

  public int getColNamesSize() {
    return (this.colNames == null) ? 0 : this.colNames.size();
  }

  public java.util.Iterator<String> getColNamesIterator() {
    return (this.colNames == null) ? null : this.colNames.iterator();
  }

  public void addToColNames(String elem) {
    if (this.colNames == null) {
      this.colNames = new ArrayList<String>();
    }
    this.colNames.add(elem);
  }

  public List<String> getColNames() {
    return this.colNames;
  }

  public void setColNames(List<String> colNames) {
    this.colNames = colNames;
  }

  public void unsetColNames() {
    this.colNames = null;
  }

  public boolean isSetColNames() {
    return this.colNames != null;
  }

  public void setColNamesIsSet(boolean value) {
    if (!value) {
      this.colNames = null;
    }
  }

  public String getPartName() {
    return this.partName;
  }

  public void setPartName(String partName) {
    this.partName = partName;
  }

  public void unsetPartName() {
    this.partName = null;
  }

  public boolean isSetPartName() {
    return this.partName != null;
  }

  public void setPartNameIsSet(boolean value) {
    if (!value) {
      this.partName = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case INDEX_NAME:
      if (value == null) {
        unsetIndexName();
      } else {
        setIndexName((String) value);
      }
      break;

    case INDEX_TYPE:
      if (value == null) {
        unsetIndexType();
      } else {
        setIndexType((Integer) value);
      }
      break;

    case TABLE_NAME:
      if (value == null) {
        unsetTableName();
      } else {
        setTableName((String) value);
      }
      break;

    case DB_NAME:
      if (value == null) {
        unsetDbName();
      } else {
        setDbName((String) value);
      }
      break;

    case COL_NAMES:
      if (value == null) {
        unsetColNames();
      } else {
        setColNames((List<String>) value);
      }
      break;

    case PART_NAME:
      if (value == null) {
        unsetPartName();
      } else {
        setPartName((String) value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case INDEX_NAME:
      return getIndexName();

    case INDEX_TYPE:
      return Integer.valueOf(getIndexType());

    case TABLE_NAME:
      return getTableName();

    case DB_NAME:
      return getDbName();

    case COL_NAMES:
      return getColNames();

    case PART_NAME:
      return getPartName();

    }
    throw new IllegalStateException();
  }

  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case INDEX_NAME:
      return isSetIndexName();
    case INDEX_TYPE:
      return isSetIndexType();
    case TABLE_NAME:
      return isSetTableName();
    case DB_NAME:
      return isSetDbName();
    case COL_NAMES:
      return isSetColNames();
    case PART_NAME:
      return isSetPartName();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof Index)
      return this.equals((Index) that);
    return false;
  }

  public boolean equals(Index that) {
    if (that == null)
      return false;

    boolean this_present_indexName = true && this.isSetIndexName();
    boolean that_present_indexName = true && that.isSetIndexName();
    if (this_present_indexName || that_present_indexName) {
      if (!(this_present_indexName && that_present_indexName))
        return false;
      if (!this.indexName.equals(that.indexName))
        return false;
    }

    boolean this_present_indexType = true;
    boolean that_present_indexType = true;
    if (this_present_indexType || that_present_indexType) {
      if (!(this_present_indexType && that_present_indexType))
        return false;
      if (this.indexType != that.indexType)
        return false;
    }

    boolean this_present_tableName = true && this.isSetTableName();
    boolean that_present_tableName = true && that.isSetTableName();
    if (this_present_tableName || that_present_tableName) {
      if (!(this_present_tableName && that_present_tableName))
        return false;
      if (!this.tableName.equals(that.tableName))
        return false;
    }

    boolean this_present_dbName = true && this.isSetDbName();
    boolean that_present_dbName = true && that.isSetDbName();
    if (this_present_dbName || that_present_dbName) {
      if (!(this_present_dbName && that_present_dbName))
        return false;
      if (!this.dbName.equals(that.dbName))
        return false;
    }

    boolean this_present_colNames = true && this.isSetColNames();
    boolean that_present_colNames = true && that.isSetColNames();
    if (this_present_colNames || that_present_colNames) {
      if (!(this_present_colNames && that_present_colNames))
        return false;
      if (!this.colNames.equals(that.colNames))
        return false;
    }

    boolean this_present_partName = true && this.isSetPartName();
    boolean that_present_partName = true && that.isSetPartName();
    if (this_present_partName || that_present_partName) {
      if (!(this_present_partName && that_present_partName))
        return false;
      if (!this.partName.equals(that.partName))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(Index other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    Index typedOther = (Index) other;

    lastComparison = Boolean.valueOf(isSetIndexName()).compareTo(
        typedOther.isSetIndexName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetIndexName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.indexName,
          typedOther.indexName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetIndexType()).compareTo(
        typedOther.isSetIndexType());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetIndexType()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.indexType,
          typedOther.indexType);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTableName()).compareTo(
        typedOther.isSetTableName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTableName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.tableName,
          typedOther.tableName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetDbName()).compareTo(
        typedOther.isSetDbName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDbName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.dbName,
          typedOther.dbName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetColNames()).compareTo(
        typedOther.isSetColNames());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetColNames()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.colNames,
          typedOther.colNames);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetPartName()).compareTo(
        typedOther.isSetPartName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPartName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.partName,
          typedOther.partName);
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
    StringBuilder sb = new StringBuilder("Index(");
    boolean first = true;

    sb.append("indexName:");
    if (this.indexName == null) {
      sb.append("null");
    } else {
      sb.append(this.indexName);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("indexType:");
    sb.append(this.indexType);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("tableName:");
    if (this.tableName == null) {
      sb.append("null");
    } else {
      sb.append(this.tableName);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("dbName:");
    if (this.dbName == null) {
      sb.append("null");
    } else {
      sb.append(this.dbName);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("colNames:");
    if (this.colNames == null) {
      sb.append("null");
    } else {
      sb.append(this.colNames);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("partName:");
    if (this.partName == null) {
      sb.append("null");
    } else {
      sb.append(this.partName);
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

  private static class IndexStandardSchemeFactory implements SchemeFactory {
    public IndexStandardScheme getScheme() {
      return new IndexStandardScheme();
    }
  }

  private static class IndexStandardScheme extends StandardScheme<Index> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, Index struct)
        throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true) {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) {
          break;
        }
        switch (schemeField.id) {
        case 1:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.indexName = iprot.readString();
            struct.setIndexNameIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 2:
          if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
            struct.indexType = iprot.readI32();
            struct.setIndexTypeIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 3:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.tableName = iprot.readString();
            struct.setTableNameIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 4:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.dbName = iprot.readString();
            struct.setDbNameIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 5:
          if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
            {
              org.apache.thrift.protocol.TList _list80 = iprot.readListBegin();
              struct.colNames = new ArrayList<String>(_list80.size);
              for (int _i81 = 0; _i81 < _list80.size; ++_i81) {
                String _elem82;
                _elem82 = iprot.readString();
                struct.colNames.add(_elem82);
              }
              iprot.readListEnd();
            }
            struct.setColNamesIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 6:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.partName = iprot.readString();
            struct.setPartNameIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, Index struct)
        throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.indexName != null) {
        oprot.writeFieldBegin(INDEX_NAME_FIELD_DESC);
        oprot.writeString(struct.indexName);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(INDEX_TYPE_FIELD_DESC);
      oprot.writeI32(struct.indexType);
      oprot.writeFieldEnd();
      if (struct.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeString(struct.tableName);
        oprot.writeFieldEnd();
      }
      if (struct.dbName != null) {
        oprot.writeFieldBegin(DB_NAME_FIELD_DESC);
        oprot.writeString(struct.dbName);
        oprot.writeFieldEnd();
      }
      if (struct.colNames != null) {
        oprot.writeFieldBegin(COL_NAMES_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(
              org.apache.thrift.protocol.TType.STRING, struct.colNames.size()));
          for (String _iter83 : struct.colNames) {
            oprot.writeString(_iter83);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.partName != null) {
        oprot.writeFieldBegin(PART_NAME_FIELD_DESC);
        oprot.writeString(struct.partName);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class IndexTupleSchemeFactory implements SchemeFactory {
    public IndexTupleScheme getScheme() {
      return new IndexTupleScheme();
    }
  }

  private static class IndexTupleScheme extends TupleScheme<Index> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, Index struct)
        throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetIndexName()) {
        optionals.set(0);
      }
      if (struct.isSetIndexType()) {
        optionals.set(1);
      }
      if (struct.isSetTableName()) {
        optionals.set(2);
      }
      if (struct.isSetDbName()) {
        optionals.set(3);
      }
      if (struct.isSetColNames()) {
        optionals.set(4);
      }
      if (struct.isSetPartName()) {
        optionals.set(5);
      }
      oprot.writeBitSet(optionals, 6);
      if (struct.isSetIndexName()) {
        oprot.writeString(struct.indexName);
      }
      if (struct.isSetIndexType()) {
        oprot.writeI32(struct.indexType);
      }
      if (struct.isSetTableName()) {
        oprot.writeString(struct.tableName);
      }
      if (struct.isSetDbName()) {
        oprot.writeString(struct.dbName);
      }
      if (struct.isSetColNames()) {
        {
          oprot.writeI32(struct.colNames.size());
          for (String _iter84 : struct.colNames) {
            oprot.writeString(_iter84);
          }
        }
      }
      if (struct.isSetPartName()) {
        oprot.writeString(struct.partName);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, Index struct)
        throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(6);
      if (incoming.get(0)) {
        struct.indexName = iprot.readString();
        struct.setIndexNameIsSet(true);
      }
      if (incoming.get(1)) {
        struct.indexType = iprot.readI32();
        struct.setIndexTypeIsSet(true);
      }
      if (incoming.get(2)) {
        struct.tableName = iprot.readString();
        struct.setTableNameIsSet(true);
      }
      if (incoming.get(3)) {
        struct.dbName = iprot.readString();
        struct.setDbNameIsSet(true);
      }
      if (incoming.get(4)) {
        {
          org.apache.thrift.protocol.TList _list85 = new org.apache.thrift.protocol.TList(
              org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.colNames = new ArrayList<String>(_list85.size);
          for (int _i86 = 0; _i86 < _list85.size; ++_i86) {
            String _elem87;
            _elem87 = iprot.readString();
            struct.colNames.add(_elem87);
          }
        }
        struct.setColNamesIsSet(true);
      }
      if (incoming.get(5)) {
        struct.partName = iprot.readString();
        struct.setPartNameIsSet(true);
      }
    }
  }

}
