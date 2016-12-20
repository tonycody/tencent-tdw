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

public class AddPartitionDesc implements
    org.apache.thrift.TBase<AddPartitionDesc, AddPartitionDesc._Fields>,
    java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct(
      "AddPartitionDesc");

  private static final org.apache.thrift.protocol.TField DB_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "dbName", org.apache.thrift.protocol.TType.STRING, (short) 1);
  private static final org.apache.thrift.protocol.TField TABLE_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "tableName", org.apache.thrift.protocol.TType.STRING, (short) 2);
  private static final org.apache.thrift.protocol.TField USER_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "user", org.apache.thrift.protocol.TType.STRING, (short) 3);
  private static final org.apache.thrift.protocol.TField LEVEL_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "level", org.apache.thrift.protocol.TType.I32, (short) 4);
  private static final org.apache.thrift.protocol.TField PART_TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "partType", org.apache.thrift.protocol.TType.STRING, (short) 5);
  private static final org.apache.thrift.protocol.TField PAR_SPACES_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "parSpaces", org.apache.thrift.protocol.TType.MAP, (short) 6);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class,
        new AddPartitionDescStandardSchemeFactory());
    schemes.put(TupleScheme.class, new AddPartitionDescTupleSchemeFactory());
  }

  private String dbName;
  private String tableName;
  private String user;
  private int level;
  private String partType;
  private Map<String, List<String>> parSpaces;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    DB_NAME((short) 1, "dbName"), TABLE_NAME((short) 2, "tableName"), USER(
        (short) 3, "user"), LEVEL((short) 4, "level"), PART_TYPE((short) 5,
        "partType"), PAR_SPACES((short) 6, "parSpaces");

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
        return LEVEL;
      case 5:
        return PART_TYPE;
      case 6:
        return PAR_SPACES;
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

  private static final int __LEVEL_ISSET_ID = 0;
  private BitSet __isset_bit_vector = new BitSet(1);
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
    tmpMap.put(_Fields.LEVEL, new org.apache.thrift.meta_data.FieldMetaData(
        "level", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.PART_TYPE,
        new org.apache.thrift.meta_data.FieldMetaData("partType",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.PAR_SPACES,
        new org.apache.thrift.meta_data.FieldMetaData("parSpaces",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.MapMetaData(
                org.apache.thrift.protocol.TType.MAP,
                new org.apache.thrift.meta_data.FieldValueMetaData(
                    org.apache.thrift.protocol.TType.STRING),
                new org.apache.thrift.meta_data.ListMetaData(
                    org.apache.thrift.protocol.TType.LIST,
                    new org.apache.thrift.meta_data.FieldValueMetaData(
                        org.apache.thrift.protocol.TType.STRING)))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(
        AddPartitionDesc.class, metaDataMap);
  }

  public AddPartitionDesc() {
  }

  public AddPartitionDesc(String dbName, String tableName, String user,
      int level, String partType, Map<String, List<String>> parSpaces) {
    this();
    this.dbName = dbName;
    this.tableName = tableName;
    this.user = user;
    this.level = level;
    setLevelIsSet(true);
    this.partType = partType;
    this.parSpaces = parSpaces;
  }

  public AddPartitionDesc(AddPartitionDesc other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    if (other.isSetDbName()) {
      this.dbName = other.dbName;
    }
    if (other.isSetTableName()) {
      this.tableName = other.tableName;
    }
    if (other.isSetUser()) {
      this.user = other.user;
    }
    this.level = other.level;
    if (other.isSetPartType()) {
      this.partType = other.partType;
    }
    if (other.isSetParSpaces()) {
      Map<String, List<String>> __this__parSpaces = new HashMap<String, List<String>>();
      for (Map.Entry<String, List<String>> other_element : other.parSpaces
          .entrySet()) {

        String other_element_key = other_element.getKey();
        List<String> other_element_value = other_element.getValue();

        String __this__parSpaces_copy_key = other_element_key;

        List<String> __this__parSpaces_copy_value = new ArrayList<String>();
        for (String other_element_value_element : other_element_value) {
          __this__parSpaces_copy_value.add(other_element_value_element);
        }

        __this__parSpaces.put(__this__parSpaces_copy_key,
            __this__parSpaces_copy_value);
      }
      this.parSpaces = __this__parSpaces;
    }
  }

  public AddPartitionDesc deepCopy() {
    return new AddPartitionDesc(this);
  }

  @Override
  public void clear() {
    this.dbName = null;
    this.tableName = null;
    this.user = null;
    setLevelIsSet(false);
    this.level = 0;
    this.partType = null;
    this.parSpaces = null;
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

  public int getLevel() {
    return this.level;
  }

  public void setLevel(int level) {
    this.level = level;
    setLevelIsSet(true);
  }

  public void unsetLevel() {
    __isset_bit_vector.clear(__LEVEL_ISSET_ID);
  }

  public boolean isSetLevel() {
    return __isset_bit_vector.get(__LEVEL_ISSET_ID);
  }

  public void setLevelIsSet(boolean value) {
    __isset_bit_vector.set(__LEVEL_ISSET_ID, value);
  }

  public String getPartType() {
    return this.partType;
  }

  public void setPartType(String partType) {
    this.partType = partType;
  }

  public void unsetPartType() {
    this.partType = null;
  }

  public boolean isSetPartType() {
    return this.partType != null;
  }

  public void setPartTypeIsSet(boolean value) {
    if (!value) {
      this.partType = null;
    }
  }

  public int getParSpacesSize() {
    return (this.parSpaces == null) ? 0 : this.parSpaces.size();
  }

  public void putToParSpaces(String key, List<String> val) {
    if (this.parSpaces == null) {
      this.parSpaces = new HashMap<String, List<String>>();
    }
    this.parSpaces.put(key, val);
  }

  public Map<String, List<String>> getParSpaces() {
    return this.parSpaces;
  }

  public void setParSpaces(Map<String, List<String>> parSpaces) {
    this.parSpaces = parSpaces;
  }

  public void unsetParSpaces() {
    this.parSpaces = null;
  }

  public boolean isSetParSpaces() {
    return this.parSpaces != null;
  }

  public void setParSpacesIsSet(boolean value) {
    if (!value) {
      this.parSpaces = null;
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

    case LEVEL:
      if (value == null) {
        unsetLevel();
      } else {
        setLevel((Integer) value);
      }
      break;

    case PART_TYPE:
      if (value == null) {
        unsetPartType();
      } else {
        setPartType((String) value);
      }
      break;

    case PAR_SPACES:
      if (value == null) {
        unsetParSpaces();
      } else {
        setParSpaces((Map<String, List<String>>) value);
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

    case LEVEL:
      return Integer.valueOf(getLevel());

    case PART_TYPE:
      return getPartType();

    case PAR_SPACES:
      return getParSpaces();

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
    case LEVEL:
      return isSetLevel();
    case PART_TYPE:
      return isSetPartType();
    case PAR_SPACES:
      return isSetParSpaces();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof AddPartitionDesc)
      return this.equals((AddPartitionDesc) that);
    return false;
  }

  public boolean equals(AddPartitionDesc that) {
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

    boolean this_present_level = true;
    boolean that_present_level = true;
    if (this_present_level || that_present_level) {
      if (!(this_present_level && that_present_level))
        return false;
      if (this.level != that.level)
        return false;
    }

    boolean this_present_partType = true && this.isSetPartType();
    boolean that_present_partType = true && that.isSetPartType();
    if (this_present_partType || that_present_partType) {
      if (!(this_present_partType && that_present_partType))
        return false;
      if (!this.partType.equals(that.partType))
        return false;
    }

    boolean this_present_parSpaces = true && this.isSetParSpaces();
    boolean that_present_parSpaces = true && that.isSetParSpaces();
    if (this_present_parSpaces || that_present_parSpaces) {
      if (!(this_present_parSpaces && that_present_parSpaces))
        return false;
      if (!this.parSpaces.equals(that.parSpaces))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(AddPartitionDesc other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    AddPartitionDesc typedOther = (AddPartitionDesc) other;

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
    lastComparison = Boolean.valueOf(isSetLevel()).compareTo(
        typedOther.isSetLevel());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLevel()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.level,
          typedOther.level);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetPartType()).compareTo(
        typedOther.isSetPartType());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPartType()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.partType,
          typedOther.partType);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetParSpaces()).compareTo(
        typedOther.isSetParSpaces());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetParSpaces()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.parSpaces,
          typedOther.parSpaces);
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
    StringBuilder sb = new StringBuilder("AddPartitionDesc(");
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
    sb.append("level:");
    sb.append(this.level);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("partType:");
    if (this.partType == null) {
      sb.append("null");
    } else {
      sb.append(this.partType);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("parSpaces:");
    if (this.parSpaces == null) {
      sb.append("null");
    } else {
      sb.append(this.parSpaces);
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

  private static class AddPartitionDescStandardSchemeFactory implements
      SchemeFactory {
    public AddPartitionDescStandardScheme getScheme() {
      return new AddPartitionDescStandardScheme();
    }
  }

  private static class AddPartitionDescStandardScheme extends
      StandardScheme<AddPartitionDesc> {

    public void read(org.apache.thrift.protocol.TProtocol iprot,
        AddPartitionDesc struct) throws org.apache.thrift.TException {
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
          if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
            struct.level = iprot.readI32();
            struct.setLevelIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 5:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.partType = iprot.readString();
            struct.setPartTypeIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 6:
          if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
            {
              org.apache.thrift.protocol.TMap _map130 = iprot.readMapBegin();
              struct.parSpaces = new HashMap<String, List<String>>(
                  2 * _map130.size);
              for (int _i131 = 0; _i131 < _map130.size; ++_i131) {
                String _key132;
                List<String> _val133;
                _key132 = iprot.readString();
                {
                  org.apache.thrift.protocol.TList _list134 = iprot
                      .readListBegin();
                  _val133 = new ArrayList<String>(_list134.size);
                  for (int _i135 = 0; _i135 < _list134.size; ++_i135) {
                    String _elem136;
                    _elem136 = iprot.readString();
                    _val133.add(_elem136);
                  }
                  iprot.readListEnd();
                }
                struct.parSpaces.put(_key132, _val133);
              }
              iprot.readMapEnd();
            }
            struct.setParSpacesIsSet(true);
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
        AddPartitionDesc struct) throws org.apache.thrift.TException {
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
      oprot.writeFieldBegin(LEVEL_FIELD_DESC);
      oprot.writeI32(struct.level);
      oprot.writeFieldEnd();
      if (struct.partType != null) {
        oprot.writeFieldBegin(PART_TYPE_FIELD_DESC);
        oprot.writeString(struct.partType);
        oprot.writeFieldEnd();
      }
      if (struct.parSpaces != null) {
        oprot.writeFieldBegin(PAR_SPACES_FIELD_DESC);
        {
          oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(
              org.apache.thrift.protocol.TType.STRING,
              org.apache.thrift.protocol.TType.LIST, struct.parSpaces.size()));
          for (Map.Entry<String, List<String>> _iter137 : struct.parSpaces
              .entrySet()) {
            oprot.writeString(_iter137.getKey());
            {
              oprot.writeListBegin(new org.apache.thrift.protocol.TList(
                  org.apache.thrift.protocol.TType.STRING, _iter137.getValue()
                      .size()));
              for (String _iter138 : _iter137.getValue()) {
                oprot.writeString(_iter138);
              }
              oprot.writeListEnd();
            }
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class AddPartitionDescTupleSchemeFactory implements
      SchemeFactory {
    public AddPartitionDescTupleScheme getScheme() {
      return new AddPartitionDescTupleScheme();
    }
  }

  private static class AddPartitionDescTupleScheme extends
      TupleScheme<AddPartitionDesc> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot,
        AddPartitionDesc struct) throws org.apache.thrift.TException {
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
      if (struct.isSetLevel()) {
        optionals.set(3);
      }
      if (struct.isSetPartType()) {
        optionals.set(4);
      }
      if (struct.isSetParSpaces()) {
        optionals.set(5);
      }
      oprot.writeBitSet(optionals, 6);
      if (struct.isSetDbName()) {
        oprot.writeString(struct.dbName);
      }
      if (struct.isSetTableName()) {
        oprot.writeString(struct.tableName);
      }
      if (struct.isSetUser()) {
        oprot.writeString(struct.user);
      }
      if (struct.isSetLevel()) {
        oprot.writeI32(struct.level);
      }
      if (struct.isSetPartType()) {
        oprot.writeString(struct.partType);
      }
      if (struct.isSetParSpaces()) {
        {
          oprot.writeI32(struct.parSpaces.size());
          for (Map.Entry<String, List<String>> _iter139 : struct.parSpaces
              .entrySet()) {
            oprot.writeString(_iter139.getKey());
            {
              oprot.writeI32(_iter139.getValue().size());
              for (String _iter140 : _iter139.getValue()) {
                oprot.writeString(_iter140);
              }
            }
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot,
        AddPartitionDesc struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(6);
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
        struct.level = iprot.readI32();
        struct.setLevelIsSet(true);
      }
      if (incoming.get(4)) {
        struct.partType = iprot.readString();
        struct.setPartTypeIsSet(true);
      }
      if (incoming.get(5)) {
        {
          org.apache.thrift.protocol.TMap _map141 = new org.apache.thrift.protocol.TMap(
              org.apache.thrift.protocol.TType.STRING,
              org.apache.thrift.protocol.TType.LIST, iprot.readI32());
          struct.parSpaces = new HashMap<String, List<String>>(2 * _map141.size);
          for (int _i142 = 0; _i142 < _map141.size; ++_i142) {
            String _key143;
            List<String> _val144;
            _key143 = iprot.readString();
            {
              org.apache.thrift.protocol.TList _list145 = new org.apache.thrift.protocol.TList(
                  org.apache.thrift.protocol.TType.STRING, iprot.readI32());
              _val144 = new ArrayList<String>(_list145.size);
              for (int _i146 = 0; _i146 < _list145.size; ++_i146) {
                String _elem147;
                _elem147 = iprot.readString();
                _val144.add(_elem147);
              }
            }
            struct.parSpaces.put(_key143, _val144);
          }
        }
        struct.setParSpacesIsSet(true);
      }
    }
  }

}
