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

public class SerDeInfo implements
    org.apache.thrift.TBase<SerDeInfo, SerDeInfo._Fields>,
    java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct(
      "SerDeInfo");

  private static final org.apache.thrift.protocol.TField NAME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "name", org.apache.thrift.protocol.TType.STRING, (short) 1);
  private static final org.apache.thrift.protocol.TField SERIALIZATION_LIB_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "serializationLib", org.apache.thrift.protocol.TType.STRING, (short) 2);
  private static final org.apache.thrift.protocol.TField PARAMETERS_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "parameters", org.apache.thrift.protocol.TType.MAP, (short) 3);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new SerDeInfoStandardSchemeFactory());
    schemes.put(TupleScheme.class, new SerDeInfoTupleSchemeFactory());
  }

  private String name;
  private String serializationLib;
  private Map<String, String> parameters;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    NAME((short) 1, "name"), SERIALIZATION_LIB((short) 2, "serializationLib"), PARAMETERS(
        (short) 3, "parameters");

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
        return SERIALIZATION_LIB;
      case 3:
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

  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(
        _Fields.class);
    tmpMap.put(_Fields.NAME, new org.apache.thrift.meta_data.FieldMetaData(
        "name", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.SERIALIZATION_LIB,
        new org.apache.thrift.meta_data.FieldMetaData("serializationLib",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
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
        SerDeInfo.class, metaDataMap);
  }

  public SerDeInfo() {
  }

  public SerDeInfo(String name, String serializationLib,
      Map<String, String> parameters) {
    this();
    this.name = name;
    this.serializationLib = serializationLib;
    this.parameters = parameters;
  }

  public SerDeInfo(SerDeInfo other) {
    if (other.isSetName()) {
      this.name = other.name;
    }
    if (other.isSetSerializationLib()) {
      this.serializationLib = other.serializationLib;
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

  public SerDeInfo deepCopy() {
    return new SerDeInfo(this);
  }

  @Override
  public void clear() {
    this.name = null;
    this.serializationLib = null;
    this.parameters = null;
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

  public String getSerializationLib() {
    return this.serializationLib;
  }

  public void setSerializationLib(String serializationLib) {
    this.serializationLib = serializationLib;
  }

  public void unsetSerializationLib() {
    this.serializationLib = null;
  }

  public boolean isSetSerializationLib() {
    return this.serializationLib != null;
  }

  public void setSerializationLibIsSet(boolean value) {
    if (!value) {
      this.serializationLib = null;
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
    case NAME:
      if (value == null) {
        unsetName();
      } else {
        setName((String) value);
      }
      break;

    case SERIALIZATION_LIB:
      if (value == null) {
        unsetSerializationLib();
      } else {
        setSerializationLib((String) value);
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
    case NAME:
      return getName();

    case SERIALIZATION_LIB:
      return getSerializationLib();

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
    case NAME:
      return isSetName();
    case SERIALIZATION_LIB:
      return isSetSerializationLib();
    case PARAMETERS:
      return isSetParameters();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof SerDeInfo)
      return this.equals((SerDeInfo) that);
    return false;
  }

  public boolean equals(SerDeInfo that) {
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

    boolean this_present_serializationLib = true && this
        .isSetSerializationLib();
    boolean that_present_serializationLib = true && that
        .isSetSerializationLib();
    if (this_present_serializationLib || that_present_serializationLib) {
      if (!(this_present_serializationLib && that_present_serializationLib))
        return false;
      if (!this.serializationLib.equals(that.serializationLib))
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

  public int compareTo(SerDeInfo other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    SerDeInfo typedOther = (SerDeInfo) other;

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
    lastComparison = Boolean.valueOf(isSetSerializationLib()).compareTo(
        typedOther.isSetSerializationLib());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSerializationLib()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.serializationLib, typedOther.serializationLib);
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
    StringBuilder sb = new StringBuilder("SerDeInfo(");
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
    sb.append("serializationLib:");
    if (this.serializationLib == null) {
      sb.append("null");
    } else {
      sb.append(this.serializationLib);
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
      read(new org.apache.thrift.protocol.TCompactProtocol(
          new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class SerDeInfoStandardSchemeFactory implements SchemeFactory {
    public SerDeInfoStandardScheme getScheme() {
      return new SerDeInfoStandardScheme();
    }
  }

  private static class SerDeInfoStandardScheme extends
      StandardScheme<SerDeInfo> {

    public void read(org.apache.thrift.protocol.TProtocol iprot,
        SerDeInfo struct) throws org.apache.thrift.TException {
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
            struct.serializationLib = iprot.readString();
            struct.setSerializationLibIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 3:
          if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
            {
              org.apache.thrift.protocol.TMap _map8 = iprot.readMapBegin();
              struct.parameters = new HashMap<String, String>(2 * _map8.size);
              for (int _i9 = 0; _i9 < _map8.size; ++_i9) {
                String _key10;
                String _val11;
                _key10 = iprot.readString();
                _val11 = iprot.readString();
                struct.parameters.put(_key10, _val11);
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
        SerDeInfo struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.name != null) {
        oprot.writeFieldBegin(NAME_FIELD_DESC);
        oprot.writeString(struct.name);
        oprot.writeFieldEnd();
      }
      if (struct.serializationLib != null) {
        oprot.writeFieldBegin(SERIALIZATION_LIB_FIELD_DESC);
        oprot.writeString(struct.serializationLib);
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
          for (Map.Entry<String, String> _iter12 : struct.parameters.entrySet()) {
            oprot.writeString(_iter12.getKey());
            oprot.writeString(_iter12.getValue());
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class SerDeInfoTupleSchemeFactory implements SchemeFactory {
    public SerDeInfoTupleScheme getScheme() {
      return new SerDeInfoTupleScheme();
    }
  }

  private static class SerDeInfoTupleScheme extends TupleScheme<SerDeInfo> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot,
        SerDeInfo struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetName()) {
        optionals.set(0);
      }
      if (struct.isSetSerializationLib()) {
        optionals.set(1);
      }
      if (struct.isSetParameters()) {
        optionals.set(2);
      }
      oprot.writeBitSet(optionals, 3);
      if (struct.isSetName()) {
        oprot.writeString(struct.name);
      }
      if (struct.isSetSerializationLib()) {
        oprot.writeString(struct.serializationLib);
      }
      if (struct.isSetParameters()) {
        {
          oprot.writeI32(struct.parameters.size());
          for (Map.Entry<String, String> _iter13 : struct.parameters.entrySet()) {
            oprot.writeString(_iter13.getKey());
            oprot.writeString(_iter13.getValue());
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, SerDeInfo struct)
        throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(3);
      if (incoming.get(0)) {
        struct.name = iprot.readString();
        struct.setNameIsSet(true);
      }
      if (incoming.get(1)) {
        struct.serializationLib = iprot.readString();
        struct.setSerializationLibIsSet(true);
      }
      if (incoming.get(2)) {
        {
          org.apache.thrift.protocol.TMap _map14 = new org.apache.thrift.protocol.TMap(
              org.apache.thrift.protocol.TType.STRING,
              org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.parameters = new HashMap<String, String>(2 * _map14.size);
          for (int _i15 = 0; _i15 < _map14.size; ++_i15) {
            String _key16;
            String _val17;
            _key16 = iprot.readString();
            _val17 = iprot.readString();
            struct.parameters.put(_key16, _val17);
          }
        }
        struct.setParametersIsSet(true);
      }
    }
  }

}
