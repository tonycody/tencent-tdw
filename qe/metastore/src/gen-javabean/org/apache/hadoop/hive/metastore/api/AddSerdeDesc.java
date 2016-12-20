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

public class AddSerdeDesc implements
    org.apache.thrift.TBase<AddSerdeDesc, AddSerdeDesc._Fields>,
    java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct(
      "AddSerdeDesc");

  private static final org.apache.thrift.protocol.TField DB_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "dbName", org.apache.thrift.protocol.TType.STRING, (short) 1);
  private static final org.apache.thrift.protocol.TField TABLE_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "tableName", org.apache.thrift.protocol.TType.STRING, (short) 2);
  private static final org.apache.thrift.protocol.TField USER_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "user", org.apache.thrift.protocol.TType.STRING, (short) 3);
  private static final org.apache.thrift.protocol.TField SERDE_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "serdeName", org.apache.thrift.protocol.TType.STRING, (short) 4);
  private static final org.apache.thrift.protocol.TField PROPS_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "props", org.apache.thrift.protocol.TType.MAP, (short) 5);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new AddSerdeDescStandardSchemeFactory());
    schemes.put(TupleScheme.class, new AddSerdeDescTupleSchemeFactory());
  }

  private String dbName;
  private String tableName;
  private String user;
  private String serdeName;
  private Map<String, String> props;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    DB_NAME((short) 1, "dbName"), TABLE_NAME((short) 2, "tableName"), USER(
        (short) 3, "user"), SERDE_NAME((short) 4, "serdeName"), PROPS(
        (short) 5, "props");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    public static _Fields findByThriftId(int fieldId) {
      switch (fieldId) {
      case 1:
        return DB_NAME;
      case 2:
        return TABLE_NAME;
      case 3:
        return USER;
      case 4:
        return SERDE_NAME;
      case 5:
        return PROPS;
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
    tmpMap.put(_Fields.DB_NAME, new org.apache.thrift.meta_data.FieldMetaData(
        "dbName", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.TABLE_NAME,
        new org.apache.thrift.meta_data.FieldMetaData("tableName",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.USER, new org.apache.thrift.meta_data.FieldMetaData(
        "user", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.SERDE_NAME,
        new org.apache.thrift.meta_data.FieldMetaData("serdeName",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.PROPS, new org.apache.thrift.meta_data.FieldMetaData(
        "props", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.MapMetaData(
            org.apache.thrift.protocol.TType.MAP,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING),
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(
        AddSerdeDesc.class, metaDataMap);
  }

  public AddSerdeDesc() {
  }

  public AddSerdeDesc(String dbName, String tableName, String user,
      String serdeName, Map<String, String> props) {
    this();
    this.dbName = dbName;
    this.tableName = tableName;
    this.user = user;
    this.serdeName = serdeName;
    this.props = props;
  }

  public AddSerdeDesc(AddSerdeDesc other) {
    if (other.isSetDbName()) {
      this.dbName = other.dbName;
    }
    if (other.isSetTableName()) {
      this.tableName = other.tableName;
    }
    if (other.isSetUser()) {
      this.user = other.user;
    }
    if (other.isSetSerdeName()) {
      this.serdeName = other.serdeName;
    }
    if (other.isSetProps()) {
      Map<String, String> __this__props = new HashMap<String, String>();
      for (Map.Entry<String, String> other_element : other.props.entrySet()) {

        String other_element_key = other_element.getKey();
        String other_element_value = other_element.getValue();

        String __this__props_copy_key = other_element_key;

        String __this__props_copy_value = other_element_value;

        __this__props.put(__this__props_copy_key, __this__props_copy_value);
      }
      this.props = __this__props;
    }
  }

  public AddSerdeDesc deepCopy() {
    return new AddSerdeDesc(this);
  }

  @Override
  public void clear() {
    this.dbName = null;
    this.tableName = null;
    this.user = null;
    this.serdeName = null;
    this.props = null;
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

  public String getUser() {
    return this.user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public void unsetUser() {
    this.user = null;
  }

  public boolean isSetUser() {
    return this.user != null;
  }

  public void setUserIsSet(boolean value) {
    if (!value) {
      this.user = null;
    }
  }

  public String getSerdeName() {
    return this.serdeName;
  }

  public void setSerdeName(String serdeName) {
    this.serdeName = serdeName;
  }

  public void unsetSerdeName() {
    this.serdeName = null;
  }

  public boolean isSetSerdeName() {
    return this.serdeName != null;
  }

  public void setSerdeNameIsSet(boolean value) {
    if (!value) {
      this.serdeName = null;
    }
  }

  public int getPropsSize() {
    return (this.props == null) ? 0 : this.props.size();
  }

  public void putToProps(String key, String val) {
    if (this.props == null) {
      this.props = new HashMap<String, String>();
    }
    this.props.put(key, val);
  }

  public Map<String, String> getProps() {
    return this.props;
  }

  public void setProps(Map<String, String> props) {
    this.props = props;
  }

  public void unsetProps() {
    this.props = null;
  }

  public boolean isSetProps() {
    return this.props != null;
  }

  public void setPropsIsSet(boolean value) {
    if (!value) {
      this.props = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case DB_NAME:
      if (value == null) {
        unsetDbName();
      } else {
        setDbName((String) value);
      }
      break;

    case TABLE_NAME:
      if (value == null) {
        unsetTableName();
      } else {
        setTableName((String) value);
      }
      break;

    case USER:
      if (value == null) {
        unsetUser();
      } else {
        setUser((String) value);
      }
      break;

    case SERDE_NAME:
      if (value == null) {
        unsetSerdeName();
      } else {
        setSerdeName((String) value);
      }
      break;

    case PROPS:
      if (value == null) {
        unsetProps();
      } else {
        setProps((Map<String, String>) value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case DB_NAME:
      return getDbName();

    case TABLE_NAME:
      return getTableName();

    case USER:
      return getUser();

    case SERDE_NAME:
      return getSerdeName();

    case PROPS:
      return getProps();

    }
    throw new IllegalStateException();
  }

  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case DB_NAME:
      return isSetDbName();
    case TABLE_NAME:
      return isSetTableName();
    case USER:
      return isSetUser();
    case SERDE_NAME:
      return isSetSerdeName();
    case PROPS:
      return isSetProps();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof AddSerdeDesc)
      return this.equals((AddSerdeDesc) that);
    return false;
  }

  public boolean equals(AddSerdeDesc that) {
    if (that == null)
      return false;

    boolean this_present_dbName = true && this.isSetDbName();
    boolean that_present_dbName = true && that.isSetDbName();
    if (this_present_dbName || that_present_dbName) {
      if (!(this_present_dbName && that_present_dbName))
        return false;
      if (!this.dbName.equals(that.dbName))
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

    boolean this_present_user = true && this.isSetUser();
    boolean that_present_user = true && that.isSetUser();
    if (this_present_user || that_present_user) {
      if (!(this_present_user && that_present_user))
        return false;
      if (!this.user.equals(that.user))
        return false;
    }

    boolean this_present_serdeName = true && this.isSetSerdeName();
    boolean that_present_serdeName = true && that.isSetSerdeName();
    if (this_present_serdeName || that_present_serdeName) {
      if (!(this_present_serdeName && that_present_serdeName))
        return false;
      if (!this.serdeName.equals(that.serdeName))
        return false;
    }

    boolean this_present_props = true && this.isSetProps();
    boolean that_present_props = true && that.isSetProps();
    if (this_present_props || that_present_props) {
      if (!(this_present_props && that_present_props))
        return false;
      if (!this.props.equals(that.props))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(AddSerdeDesc other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    AddSerdeDesc typedOther = (AddSerdeDesc) other;

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
    lastComparison = Boolean.valueOf(isSetUser()).compareTo(
        typedOther.isSetUser());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetUser()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.user,
          typedOther.user);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetSerdeName()).compareTo(
        typedOther.isSetSerdeName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSerdeName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.serdeName,
          typedOther.serdeName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetProps()).compareTo(
        typedOther.isSetProps());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetProps()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.props,
          typedOther.props);
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
    StringBuilder sb = new StringBuilder("AddSerdeDesc(");
    boolean first = true;

    sb.append("dbName:");
    if (this.dbName == null) {
      sb.append("null");
    } else {
      sb.append(this.dbName);
    }
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
    sb.append("user:");
    if (this.user == null) {
      sb.append("null");
    } else {
      sb.append(this.user);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("serdeName:");
    if (this.serdeName == null) {
      sb.append("null");
    } else {
      sb.append(this.serdeName);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("props:");
    if (this.props == null) {
      sb.append("null");
    } else {
      sb.append(this.props);
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

  private static class AddSerdeDescStandardSchemeFactory implements
      SchemeFactory {
    public AddSerdeDescStandardScheme getScheme() {
      return new AddSerdeDescStandardScheme();
    }
  }

  private static class AddSerdeDescStandardScheme extends
      StandardScheme<AddSerdeDesc> {

    public void read(org.apache.thrift.protocol.TProtocol iprot,
        AddSerdeDesc struct) throws org.apache.thrift.TException {
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
            struct.dbName = iprot.readString();
            struct.setDbNameIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 2:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.tableName = iprot.readString();
            struct.setTableNameIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 3:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.user = iprot.readString();
            struct.setUserIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 4:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.serdeName = iprot.readString();
            struct.setSerdeNameIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 5:
          if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
            {
              org.apache.thrift.protocol.TMap _map156 = iprot.readMapBegin();
              struct.props = new HashMap<String, String>(2 * _map156.size);
              for (int _i157 = 0; _i157 < _map156.size; ++_i157) {
                String _key158;
                String _val159;
                _key158 = iprot.readString();
                _val159 = iprot.readString();
                struct.props.put(_key158, _val159);
              }
              iprot.readMapEnd();
            }
            struct.setPropsIsSet(true);
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
        AddSerdeDesc struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.dbName != null) {
        oprot.writeFieldBegin(DB_NAME_FIELD_DESC);
        oprot.writeString(struct.dbName);
        oprot.writeFieldEnd();
      }
      if (struct.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeString(struct.tableName);
        oprot.writeFieldEnd();
      }
      if (struct.user != null) {
        oprot.writeFieldBegin(USER_FIELD_DESC);
        oprot.writeString(struct.user);
        oprot.writeFieldEnd();
      }
      if (struct.serdeName != null) {
        oprot.writeFieldBegin(SERDE_NAME_FIELD_DESC);
        oprot.writeString(struct.serdeName);
        oprot.writeFieldEnd();
      }
      if (struct.props != null) {
        oprot.writeFieldBegin(PROPS_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(
              org.apache.thrift.protocol.TType.STRING,
              org.apache.thrift.protocol.TType.STRING, struct.props.size()));
          for (Map.Entry<String, String> _iter160 : struct.props.entrySet()) {
            oprot.writeString(_iter160.getKey());
            oprot.writeString(_iter160.getValue());
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class AddSerdeDescTupleSchemeFactory implements SchemeFactory {
    public AddSerdeDescTupleScheme getScheme() {
      return new AddSerdeDescTupleScheme();
    }
  }

  private static class AddSerdeDescTupleScheme extends
      TupleScheme<AddSerdeDesc> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot,
        AddSerdeDesc struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetDbName()) {
        optionals.set(0);
      }
      if (struct.isSetTableName()) {
        optionals.set(1);
      }
      if (struct.isSetUser()) {
        optionals.set(2);
      }
      if (struct.isSetSerdeName()) {
        optionals.set(3);
      }
      if (struct.isSetProps()) {
        optionals.set(4);
      }
      oprot.writeBitSet(optionals, 5);
      if (struct.isSetDbName()) {
        oprot.writeString(struct.dbName);
      }
      if (struct.isSetTableName()) {
        oprot.writeString(struct.tableName);
      }
      if (struct.isSetUser()) {
        oprot.writeString(struct.user);
      }
      if (struct.isSetSerdeName()) {
        oprot.writeString(struct.serdeName);
      }
      if (struct.isSetProps()) {
        {
          oprot.writeI32(struct.props.size());
          for (Map.Entry<String, String> _iter161 : struct.props.entrySet()) {
            oprot.writeString(_iter161.getKey());
            oprot.writeString(_iter161.getValue());
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot,
        AddSerdeDesc struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(5);
      if (incoming.get(0)) {
        struct.dbName = iprot.readString();
        struct.setDbNameIsSet(true);
      }
      if (incoming.get(1)) {
        struct.tableName = iprot.readString();
        struct.setTableNameIsSet(true);
      }
      if (incoming.get(2)) {
        struct.user = iprot.readString();
        struct.setUserIsSet(true);
      }
      if (incoming.get(3)) {
        struct.serdeName = iprot.readString();
        struct.setSerdeNameIsSet(true);
      }
      if (incoming.get(4)) {
        {
          org.apache.thrift.protocol.TMap _map162 = new org.apache.thrift.protocol.TMap(
              org.apache.thrift.protocol.TType.STRING,
              org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.props = new HashMap<String, String>(2 * _map162.size);
          for (int _i163 = 0; _i163 < _map162.size; ++_i163) {
            String _key164;
            String _val165;
            _key164 = iprot.readString();
            _val165 = iprot.readString();
            struct.props.put(_key164, _val165);
          }
        }
        struct.setPropsIsSet(true);
      }
    }
  }

}
