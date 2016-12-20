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

public class Database implements
    org.apache.thrift.TBase<Database, Database._Fields>, java.io.Serializable,
    Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct(
      "Database");

  private static final org.apache.thrift.protocol.TField NAME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "name", org.apache.thrift.protocol.TType.STRING, (short) 1);
  private static final org.apache.thrift.protocol.TField DESCRIPTION_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "description", org.apache.thrift.protocol.TType.STRING, (short) 2);
  private static final org.apache.thrift.protocol.TField HDFSSCHEME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "hdfsscheme", org.apache.thrift.protocol.TType.STRING, (short) 3);
  private static final org.apache.thrift.protocol.TField METASTORE_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "metastore", org.apache.thrift.protocol.TType.STRING, (short) 4);
  private static final org.apache.thrift.protocol.TField OWNER_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "owner", org.apache.thrift.protocol.TType.STRING, (short) 5);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new DatabaseStandardSchemeFactory());
    schemes.put(TupleScheme.class, new DatabaseTupleSchemeFactory());
  }

  private String name;
  private String description;
  private String hdfsscheme;
  private String metastore;
  private String owner;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    NAME((short) 1, "name"), DESCRIPTION((short) 2, "description"), HDFSSCHEME(
        (short) 3, "hdfsscheme"), METASTORE((short) 4, "metastore"), OWNER(
        (short) 5, "owner");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    public static _Fields findByThriftId(int fieldId) {
      switch (fieldId) {
      case 1:
        return NAME;
      case 2:
        return DESCRIPTION;
      case 3:
        return HDFSSCHEME;
      case 4:
        return METASTORE;
      case 5:
        return OWNER;
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
    tmpMap.put(_Fields.NAME, new org.apache.thrift.meta_data.FieldMetaData(
        "name", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.DESCRIPTION,
        new org.apache.thrift.meta_data.FieldMetaData("description",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.HDFSSCHEME,
        new org.apache.thrift.meta_data.FieldMetaData("hdfsscheme",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.METASTORE,
        new org.apache.thrift.meta_data.FieldMetaData("metastore",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.OWNER, new org.apache.thrift.meta_data.FieldMetaData(
        "owner", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(
        Database.class, metaDataMap);
  }

  public Database() {
  }

  public Database(String name, String description, String hdfsscheme,
      String metastore, String owner) {
    this();
    this.name = name;
    this.description = description;
    this.hdfsscheme = hdfsscheme;
    this.metastore = metastore;
    this.owner = owner;
  }

  public Database(Database other) {
    if (other.isSetName()) {
      this.name = other.name;
    }
    if (other.isSetDescription()) {
      this.description = other.description;
    }
    if (other.isSetHdfsscheme()) {
      this.hdfsscheme = other.hdfsscheme;
    }
    if (other.isSetMetastore()) {
      this.metastore = other.metastore;
    }
    if (other.isSetOwner()) {
      this.owner = other.owner;
    }
  }

  public Database deepCopy() {
    return new Database(this);
  }

  @Override
  public void clear() {
    this.name = null;
    this.description = null;
    this.hdfsscheme = null;
    this.metastore = null;
    this.owner = null;
  }

  public String getName() {
    return this.name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void unsetName() {
    this.name = null;
  }

  public boolean isSetName() {
    return this.name != null;
  }

  public void setNameIsSet(boolean value) {
    if (!value) {
      this.name = null;
    }
  }

  public String getDescription() {
    return this.description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public void unsetDescription() {
    this.description = null;
  }

  public boolean isSetDescription() {
    return this.description != null;
  }

  public void setDescriptionIsSet(boolean value) {
    if (!value) {
      this.description = null;
    }
  }

  public String getHdfsscheme() {
    return this.hdfsscheme;
  }

  public void setHdfsscheme(String hdfsscheme) {
    this.hdfsscheme = hdfsscheme;
  }

  public void unsetHdfsscheme() {
    this.hdfsscheme = null;
  }

  public boolean isSetHdfsscheme() {
    return this.hdfsscheme != null;
  }

  public void setHdfsschemeIsSet(boolean value) {
    if (!value) {
      this.hdfsscheme = null;
    }
  }

  public String getMetastore() {
    return this.metastore;
  }

  public void setMetastore(String metastore) {
    this.metastore = metastore;
  }

  public void unsetMetastore() {
    this.metastore = null;
  }

  public boolean isSetMetastore() {
    return this.metastore != null;
  }

  public void setMetastoreIsSet(boolean value) {
    if (!value) {
      this.metastore = null;
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

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case NAME:
      if (value == null) {
        unsetName();
      } else {
        setName((String) value);
      }
      break;

    case DESCRIPTION:
      if (value == null) {
        unsetDescription();
      } else {
        setDescription((String) value);
      }
      break;

    case HDFSSCHEME:
      if (value == null) {
        unsetHdfsscheme();
      } else {
        setHdfsscheme((String) value);
      }
      break;

    case METASTORE:
      if (value == null) {
        unsetMetastore();
      } else {
        setMetastore((String) value);
      }
      break;

    case OWNER:
      if (value == null) {
        unsetOwner();
      } else {
        setOwner((String) value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case NAME:
      return getName();

    case DESCRIPTION:
      return getDescription();

    case HDFSSCHEME:
      return getHdfsscheme();

    case METASTORE:
      return getMetastore();

    case OWNER:
      return getOwner();

    }
    throw new IllegalStateException();
  }

  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case NAME:
      return isSetName();
    case DESCRIPTION:
      return isSetDescription();
    case HDFSSCHEME:
      return isSetHdfsscheme();
    case METASTORE:
      return isSetMetastore();
    case OWNER:
      return isSetOwner();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof Database)
      return this.equals((Database) that);
    return false;
  }

  public boolean equals(Database that) {
    if (that == null)
      return false;

    boolean this_present_name = true && this.isSetName();
    boolean that_present_name = true && that.isSetName();
    if (this_present_name || that_present_name) {
      if (!(this_present_name && that_present_name))
        return false;
      if (!this.name.equals(that.name))
        return false;
    }

    boolean this_present_description = true && this.isSetDescription();
    boolean that_present_description = true && that.isSetDescription();
    if (this_present_description || that_present_description) {
      if (!(this_present_description && that_present_description))
        return false;
      if (!this.description.equals(that.description))
        return false;
    }

    boolean this_present_hdfsscheme = true && this.isSetHdfsscheme();
    boolean that_present_hdfsscheme = true && that.isSetHdfsscheme();
    if (this_present_hdfsscheme || that_present_hdfsscheme) {
      if (!(this_present_hdfsscheme && that_present_hdfsscheme))
        return false;
      if (!this.hdfsscheme.equals(that.hdfsscheme))
        return false;
    }

    boolean this_present_metastore = true && this.isSetMetastore();
    boolean that_present_metastore = true && that.isSetMetastore();
    if (this_present_metastore || that_present_metastore) {
      if (!(this_present_metastore && that_present_metastore))
        return false;
      if (!this.metastore.equals(that.metastore))
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

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(Database other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    Database typedOther = (Database) other;

    lastComparison = Boolean.valueOf(isSetName()).compareTo(
        typedOther.isSetName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.name,
          typedOther.name);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetDescription()).compareTo(
        typedOther.isSetDescription());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDescription()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.description, typedOther.description);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetHdfsscheme()).compareTo(
        typedOther.isSetHdfsscheme());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetHdfsscheme()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.hdfsscheme,
          typedOther.hdfsscheme);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetMetastore()).compareTo(
        typedOther.isSetMetastore());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMetastore()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.metastore,
          typedOther.metastore);
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
    StringBuilder sb = new StringBuilder("Database(");
    boolean first = true;

    sb.append("name:");
    if (this.name == null) {
      sb.append("null");
    } else {
      sb.append(this.name);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("description:");
    if (this.description == null) {
      sb.append("null");
    } else {
      sb.append(this.description);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("hdfsscheme:");
    if (this.hdfsscheme == null) {
      sb.append("null");
    } else {
      sb.append(this.hdfsscheme);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("metastore:");
    if (this.metastore == null) {
      sb.append("null");
    } else {
      sb.append(this.metastore);
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

  private static class DatabaseStandardSchemeFactory implements SchemeFactory {
    public DatabaseStandardScheme getScheme() {
      return new DatabaseStandardScheme();
    }
  }

  private static class DatabaseStandardScheme extends StandardScheme<Database> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, Database struct)
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
            struct.name = iprot.readString();
            struct.setNameIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 2:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.description = iprot.readString();
            struct.setDescriptionIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 3:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.hdfsscheme = iprot.readString();
            struct.setHdfsschemeIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 4:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.metastore = iprot.readString();
            struct.setMetastoreIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 5:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.owner = iprot.readString();
            struct.setOwnerIsSet(true);
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
        Database struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.name != null) {
        oprot.writeFieldBegin(NAME_FIELD_DESC);
        oprot.writeString(struct.name);
        oprot.writeFieldEnd();
      }
      if (struct.description != null) {
        oprot.writeFieldBegin(DESCRIPTION_FIELD_DESC);
        oprot.writeString(struct.description);
        oprot.writeFieldEnd();
      }
      if (struct.hdfsscheme != null) {
        oprot.writeFieldBegin(HDFSSCHEME_FIELD_DESC);
        oprot.writeString(struct.hdfsscheme);
        oprot.writeFieldEnd();
      }
      if (struct.metastore != null) {
        oprot.writeFieldBegin(METASTORE_FIELD_DESC);
        oprot.writeString(struct.metastore);
        oprot.writeFieldEnd();
      }
      if (struct.owner != null) {
        oprot.writeFieldBegin(OWNER_FIELD_DESC);
        oprot.writeString(struct.owner);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class DatabaseTupleSchemeFactory implements SchemeFactory {
    public DatabaseTupleScheme getScheme() {
      return new DatabaseTupleScheme();
    }
  }

  private static class DatabaseTupleScheme extends TupleScheme<Database> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, Database struct)
        throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetName()) {
        optionals.set(0);
      }
      if (struct.isSetDescription()) {
        optionals.set(1);
      }
      if (struct.isSetHdfsscheme()) {
        optionals.set(2);
      }
      if (struct.isSetMetastore()) {
        optionals.set(3);
      }
      if (struct.isSetOwner()) {
        optionals.set(4);
      }
      oprot.writeBitSet(optionals, 5);
      if (struct.isSetName()) {
        oprot.writeString(struct.name);
      }
      if (struct.isSetDescription()) {
        oprot.writeString(struct.description);
      }
      if (struct.isSetHdfsscheme()) {
        oprot.writeString(struct.hdfsscheme);
      }
      if (struct.isSetMetastore()) {
        oprot.writeString(struct.metastore);
      }
      if (struct.isSetOwner()) {
        oprot.writeString(struct.owner);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, Database struct)
        throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(5);
      if (incoming.get(0)) {
        struct.name = iprot.readString();
        struct.setNameIsSet(true);
      }
      if (incoming.get(1)) {
        struct.description = iprot.readString();
        struct.setDescriptionIsSet(true);
      }
      if (incoming.get(2)) {
        struct.hdfsscheme = iprot.readString();
        struct.setHdfsschemeIsSet(true);
      }
      if (incoming.get(3)) {
        struct.metastore = iprot.readString();
        struct.setMetastoreIsSet(true);
      }
      if (incoming.get(4)) {
        struct.owner = iprot.readString();
        struct.setOwnerIsSet(true);
      }
    }
  }

}
