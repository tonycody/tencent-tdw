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

public class TableInfo implements
    org.apache.thrift.TBase<TableInfo, TableInfo._Fields>,
    java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct(
      "TableInfo");

  private static final org.apache.thrift.protocol.TField COMMENT_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "comment", org.apache.thrift.protocol.TType.STRING, (short) 1);
  private static final org.apache.thrift.protocol.TField OWNER_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "owner", org.apache.thrift.protocol.TType.STRING, (short) 2);
  private static final org.apache.thrift.protocol.TField CREATE_TIME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "createTime", org.apache.thrift.protocol.TType.STRING, (short) 3);
  private static final org.apache.thrift.protocol.TField COLS_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "cols", org.apache.thrift.protocol.TType.LIST, (short) 4);
  private static final org.apache.thrift.protocol.TField TBL_PARAMS_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "tblParams", org.apache.thrift.protocol.TType.MAP, (short) 5);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TableInfoStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TableInfoTupleSchemeFactory());
  }

  private String comment;
  private String owner;
  private String createTime;
  private List<ColumnInfo> cols;
  private Map<String, String> tblParams;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    COMMENT((short) 1, "comment"), OWNER((short) 2, "owner"), CREATE_TIME(
        (short) 3, "createTime"), COLS((short) 4, "cols"), TBL_PARAMS(
        (short) 5, "tblParams");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    public static _Fields findByThriftId(int fieldId) {
      switch (fieldId) {
      case 1:
        return COMMENT;
      case 2:
        return OWNER;
      case 3:
        return CREATE_TIME;
      case 4:
        return COLS;
      case 5:
        return TBL_PARAMS;
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

  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(
        _Fields.class);
    tmpMap.put(_Fields.COMMENT, new org.apache.thrift.meta_data.FieldMetaData(
        "comment", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.OWNER, new org.apache.thrift.meta_data.FieldMetaData(
        "owner", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.CREATE_TIME,
        new org.apache.thrift.meta_data.FieldMetaData("createTime",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.COLS, new org.apache.thrift.meta_data.FieldMetaData(
        "cols", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.ListMetaData(
            org.apache.thrift.protocol.TType.LIST,
            new org.apache.thrift.meta_data.StructMetaData(
                org.apache.thrift.protocol.TType.STRUCT, ColumnInfo.class))));
    tmpMap.put(_Fields.TBL_PARAMS,
        new org.apache.thrift.meta_data.FieldMetaData("tblParams",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.MapMetaData(
                org.apache.thrift.protocol.TType.MAP,
                new org.apache.thrift.meta_data.FieldValueMetaData(
                    org.apache.thrift.protocol.TType.STRING),
                new org.apache.thrift.meta_data.FieldValueMetaData(
                    org.apache.thrift.protocol.TType.STRING))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(
        TableInfo.class, metaDataMap);
  }

  public TableInfo() {
  }

  public TableInfo(String comment, String owner, String createTime,
      List<ColumnInfo> cols, Map<String, String> tblParams) {
    this();
    this.comment = comment;
    this.owner = owner;
    this.createTime = createTime;
    this.cols = cols;
    this.tblParams = tblParams;
  }

  public TableInfo(TableInfo other) {
    if (other.isSetComment()) {
      this.comment = other.comment;
    }
    if (other.isSetOwner()) {
      this.owner = other.owner;
    }
    if (other.isSetCreateTime()) {
      this.createTime = other.createTime;
    }
    if (other.isSetCols()) {
      List<ColumnInfo> __this__cols = new ArrayList<ColumnInfo>();
      for (ColumnInfo other_element : other.cols) {
        __this__cols.add(new ColumnInfo(other_element));
      }
      this.cols = __this__cols;
    }
    if (other.isSetTblParams()) {
      Map<String, String> __this__tblParams = new HashMap<String, String>();
      for (Map.Entry<String, String> other_element : other.tblParams.entrySet()) {

        String other_element_key = other_element.getKey();
        String other_element_value = other_element.getValue();

        String __this__tblParams_copy_key = other_element_key;

        String __this__tblParams_copy_value = other_element_value;

        __this__tblParams.put(__this__tblParams_copy_key,
            __this__tblParams_copy_value);
      }
      this.tblParams = __this__tblParams;
    }
  }

  public TableInfo deepCopy() {
    return new TableInfo(this);
  }

  @Override
  public void clear() {
    this.comment = null;
    this.owner = null;
    this.createTime = null;
    this.cols = null;
    this.tblParams = null;
  }

  public String getComment() {
    return this.comment;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public void unsetComment() {
    this.comment = null;
  }

  public boolean isSetComment() {
    return this.comment != null;
  }

  public void setCommentIsSet(boolean value) {
    if (!value) {
      this.comment = null;
    }
  }

  public String getOwner() {
    return this.owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  public void unsetOwner() {
    this.owner = null;
  }

  public boolean isSetOwner() {
    return this.owner != null;
  }

  public void setOwnerIsSet(boolean value) {
    if (!value) {
      this.owner = null;
    }
  }

  public String getCreateTime() {
    return this.createTime;
  }

  public void setCreateTime(String createTime) {
    this.createTime = createTime;
  }

  public void unsetCreateTime() {
    this.createTime = null;
  }

  public boolean isSetCreateTime() {
    return this.createTime != null;
  }

  public void setCreateTimeIsSet(boolean value) {
    if (!value) {
      this.createTime = null;
    }
  }

  public int getColsSize() {
    return (this.cols == null) ? 0 : this.cols.size();
  }

  public java.util.Iterator<ColumnInfo> getColsIterator() {
    return (this.cols == null) ? null : this.cols.iterator();
  }

  public void addToCols(ColumnInfo elem) {
    if (this.cols == null) {
      this.cols = new ArrayList<ColumnInfo>();
    }
    this.cols.add(elem);
  }

  public List<ColumnInfo> getCols() {
    return this.cols;
  }

  public void setCols(List<ColumnInfo> cols) {
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

  public int getTblParamsSize() {
    return (this.tblParams == null) ? 0 : this.tblParams.size();
  }

  public void putToTblParams(String key, String val) {
    if (this.tblParams == null) {
      this.tblParams = new HashMap<String, String>();
    }
    this.tblParams.put(key, val);
  }

  public Map<String, String> getTblParams() {
    return this.tblParams;
  }

  public void setTblParams(Map<String, String> tblParams) {
    this.tblParams = tblParams;
  }

  public void unsetTblParams() {
    this.tblParams = null;
  }

  public boolean isSetTblParams() {
    return this.tblParams != null;
  }

  public void setTblParamsIsSet(boolean value) {
    if (!value) {
      this.tblParams = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case COMMENT:
      if (value == null) {
        unsetComment();
      } else {
        setComment((String) value);
      }
      break;

    case OWNER:
      if (value == null) {
        unsetOwner();
      } else {
        setOwner((String) value);
      }
      break;

    case CREATE_TIME:
      if (value == null) {
        unsetCreateTime();
      } else {
        setCreateTime((String) value);
      }
      break;

    case COLS:
      if (value == null) {
        unsetCols();
      } else {
        setCols((List<ColumnInfo>) value);
      }
      break;

    case TBL_PARAMS:
      if (value == null) {
        unsetTblParams();
      } else {
        setTblParams((Map<String, String>) value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case COMMENT:
      return getComment();

    case OWNER:
      return getOwner();

    case CREATE_TIME:
      return getCreateTime();

    case COLS:
      return getCols();

    case TBL_PARAMS:
      return getTblParams();

    }
    throw new IllegalStateException();
  }

  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case COMMENT:
      return isSetComment();
    case OWNER:
      return isSetOwner();
    case CREATE_TIME:
      return isSetCreateTime();
    case COLS:
      return isSetCols();
    case TBL_PARAMS:
      return isSetTblParams();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TableInfo)
      return this.equals((TableInfo) that);
    return false;
  }

  public boolean equals(TableInfo that) {
    if (that == null)
      return false;

    boolean this_present_comment = true && this.isSetComment();
    boolean that_present_comment = true && that.isSetComment();
    if (this_present_comment || that_present_comment) {
      if (!(this_present_comment && that_present_comment))
        return false;
      if (!this.comment.equals(that.comment))
        return false;
    }

    boolean this_present_owner = true && this.isSetOwner();
    boolean that_present_owner = true && that.isSetOwner();
    if (this_present_owner || that_present_owner) {
      if (!(this_present_owner && that_present_owner))
        return false;
      if (!this.owner.equals(that.owner))
        return false;
    }

    boolean this_present_createTime = true && this.isSetCreateTime();
    boolean that_present_createTime = true && that.isSetCreateTime();
    if (this_present_createTime || that_present_createTime) {
      if (!(this_present_createTime && that_present_createTime))
        return false;
      if (!this.createTime.equals(that.createTime))
        return false;
    }

    boolean this_present_cols = true && this.isSetCols();
    boolean that_present_cols = true && that.isSetCols();
    if (this_present_cols || that_present_cols) {
      if (!(this_present_cols && that_present_cols))
        return false;
      if (!this.cols.equals(that.cols))
        return false;
    }

    boolean this_present_tblParams = true && this.isSetTblParams();
    boolean that_present_tblParams = true && that.isSetTblParams();
    if (this_present_tblParams || that_present_tblParams) {
      if (!(this_present_tblParams && that_present_tblParams))
        return false;
      if (!this.tblParams.equals(that.tblParams))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(TableInfo other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    TableInfo typedOther = (TableInfo) other;

    lastComparison = Boolean.valueOf(isSetComment()).compareTo(
        typedOther.isSetComment());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetComment()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.comment,
          typedOther.comment);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetOwner()).compareTo(
        typedOther.isSetOwner());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetOwner()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.owner,
          typedOther.owner);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetCreateTime()).compareTo(
        typedOther.isSetCreateTime());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCreateTime()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.createTime,
          typedOther.createTime);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
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
    lastComparison = Boolean.valueOf(isSetTblParams()).compareTo(
        typedOther.isSetTblParams());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTblParams()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.tblParams,
          typedOther.tblParams);
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
    StringBuilder sb = new StringBuilder("TableInfo(");
    boolean first = true;

    sb.append("comment:");
    if (this.comment == null) {
      sb.append("null");
    } else {
      sb.append(this.comment);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("owner:");
    if (this.owner == null) {
      sb.append("null");
    } else {
      sb.append(this.owner);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("createTime:");
    if (this.createTime == null) {
      sb.append("null");
    } else {
      sb.append(this.createTime);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("cols:");
    if (this.cols == null) {
      sb.append("null");
    } else {
      sb.append(this.cols);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("tblParams:");
    if (this.tblParams == null) {
      sb.append("null");
    } else {
      sb.append(this.tblParams);
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
      read(new org.apache.thrift.protocol.TCompactProtocol(
          new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TableInfoStandardSchemeFactory implements SchemeFactory {
    public TableInfoStandardScheme getScheme() {
      return new TableInfoStandardScheme();
    }
  }

  private static class TableInfoStandardScheme extends
      StandardScheme<TableInfo> {

    public void read(org.apache.thrift.protocol.TProtocol iprot,
        TableInfo struct) throws org.apache.thrift.TException {
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
            struct.comment = iprot.readString();
            struct.setCommentIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 2:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.owner = iprot.readString();
            struct.setOwnerIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 3:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.createTime = iprot.readString();
            struct.setCreateTimeIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 4:
          if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
            {
              org.apache.thrift.protocol.TList _list166 = iprot.readListBegin();
              struct.cols = new ArrayList<ColumnInfo>(_list166.size);
              for (int _i167 = 0; _i167 < _list166.size; ++_i167) {
                ColumnInfo _elem168;
                _elem168 = new ColumnInfo();
                _elem168.read(iprot);
                struct.cols.add(_elem168);
              }
              iprot.readListEnd();
            }
            struct.setColsIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 5:
          if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
            {
              org.apache.thrift.protocol.TMap _map169 = iprot.readMapBegin();
              struct.tblParams = new HashMap<String, String>(2 * _map169.size);
              for (int _i170 = 0; _i170 < _map169.size; ++_i170) {
                String _key171;
                String _val172;
                _key171 = iprot.readString();
                _val172 = iprot.readString();
                struct.tblParams.put(_key171, _val172);
              }
              iprot.readMapEnd();
            }
            struct.setTblParamsIsSet(true);
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
        TableInfo struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.comment != null) {
        oprot.writeFieldBegin(COMMENT_FIELD_DESC);
        oprot.writeString(struct.comment);
        oprot.writeFieldEnd();
      }
      if (struct.owner != null) {
        oprot.writeFieldBegin(OWNER_FIELD_DESC);
        oprot.writeString(struct.owner);
        oprot.writeFieldEnd();
      }
      if (struct.createTime != null) {
        oprot.writeFieldBegin(CREATE_TIME_FIELD_DESC);
        oprot.writeString(struct.createTime);
        oprot.writeFieldEnd();
      }
      if (struct.cols != null) {
        oprot.writeFieldBegin(COLS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(
              org.apache.thrift.protocol.TType.STRUCT, struct.cols.size()));
          for (ColumnInfo _iter173 : struct.cols) {
            _iter173.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.tblParams != null) {
        oprot.writeFieldBegin(TBL_PARAMS_FIELD_DESC);
        {
          oprot
              .writeMapBegin(new org.apache.thrift.protocol.TMap(
                  org.apache.thrift.protocol.TType.STRING,
                  org.apache.thrift.protocol.TType.STRING, struct.tblParams
                      .size()));
          for (Map.Entry<String, String> _iter174 : struct.tblParams.entrySet()) {
            oprot.writeString(_iter174.getKey());
            oprot.writeString(_iter174.getValue());
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TableInfoTupleSchemeFactory implements SchemeFactory {
    public TableInfoTupleScheme getScheme() {
      return new TableInfoTupleScheme();
    }
  }

  private static class TableInfoTupleScheme extends TupleScheme<TableInfo> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot,
        TableInfo struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetComment()) {
        optionals.set(0);
      }
      if (struct.isSetOwner()) {
        optionals.set(1);
      }
      if (struct.isSetCreateTime()) {
        optionals.set(2);
      }
      if (struct.isSetCols()) {
        optionals.set(3);
      }
      if (struct.isSetTblParams()) {
        optionals.set(4);
      }
      oprot.writeBitSet(optionals, 5);
      if (struct.isSetComment()) {
        oprot.writeString(struct.comment);
      }
      if (struct.isSetOwner()) {
        oprot.writeString(struct.owner);
      }
      if (struct.isSetCreateTime()) {
        oprot.writeString(struct.createTime);
      }
      if (struct.isSetCols()) {
        {
          oprot.writeI32(struct.cols.size());
          for (ColumnInfo _iter175 : struct.cols) {
            _iter175.write(oprot);
          }
        }
      }
      if (struct.isSetTblParams()) {
        {
          oprot.writeI32(struct.tblParams.size());
          for (Map.Entry<String, String> _iter176 : struct.tblParams.entrySet()) {
            oprot.writeString(_iter176.getKey());
            oprot.writeString(_iter176.getValue());
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TableInfo struct)
        throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(5);
      if (incoming.get(0)) {
        struct.comment = iprot.readString();
        struct.setCommentIsSet(true);
      }
      if (incoming.get(1)) {
        struct.owner = iprot.readString();
        struct.setOwnerIsSet(true);
      }
      if (incoming.get(2)) {
        struct.createTime = iprot.readString();
        struct.setCreateTimeIsSet(true);
      }
      if (incoming.get(3)) {
        {
          org.apache.thrift.protocol.TList _list177 = new org.apache.thrift.protocol.TList(
              org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.cols = new ArrayList<ColumnInfo>(_list177.size);
          for (int _i178 = 0; _i178 < _list177.size; ++_i178) {
            ColumnInfo _elem179;
            _elem179 = new ColumnInfo();
            _elem179.read(iprot);
            struct.cols.add(_elem179);
          }
        }
        struct.setColsIsSet(true);
      }
      if (incoming.get(4)) {
        {
          org.apache.thrift.protocol.TMap _map180 = new org.apache.thrift.protocol.TMap(
              org.apache.thrift.protocol.TType.STRING,
              org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.tblParams = new HashMap<String, String>(2 * _map180.size);
          for (int _i181 = 0; _i181 < _map180.size; ++_i181) {
            String _key182;
            String _val183;
            _key182 = iprot.readString();
            _val183 = iprot.readString();
            struct.tblParams.put(_key182, _val183);
          }
        }
        struct.setTblParamsIsSet(true);
      }
    }
  }

}
