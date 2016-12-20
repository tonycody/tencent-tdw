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

public class group implements org.apache.thrift.TBase<group, group._Fields>,
    java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct(
      "group");

  private static final org.apache.thrift.protocol.TField GROUP_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "groupName", org.apache.thrift.protocol.TType.STRING, (short) 1);
  private static final org.apache.thrift.protocol.TField CREATOR_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "creator", org.apache.thrift.protocol.TType.STRING, (short) 2);
  private static final org.apache.thrift.protocol.TField USER_LIST_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "userList", org.apache.thrift.protocol.TType.STRING, (short) 3);
  private static final org.apache.thrift.protocol.TField USER_NUM_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "userNum", org.apache.thrift.protocol.TType.I32, (short) 4);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new groupStandardSchemeFactory());
    schemes.put(TupleScheme.class, new groupTupleSchemeFactory());
  }

  private String groupName;
  private String creator;
  private String userList;
  private int userNum;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    GROUP_NAME((short) 1, "groupName"), CREATOR((short) 2, "creator"), USER_LIST(
        (short) 3, "userList"), USER_NUM((short) 4, "userNum");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    public static _Fields findByThriftId(int fieldId) {
      switch (fieldId) {
      case 1:
        return GROUP_NAME;
      case 2:
        return CREATOR;
      case 3:
        return USER_LIST;
      case 4:
        return USER_NUM;
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

  private static final int __USERNUM_ISSET_ID = 0;
  private BitSet __isset_bit_vector = new BitSet(1);
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(
        _Fields.class);
    tmpMap.put(_Fields.GROUP_NAME,
        new org.apache.thrift.meta_data.FieldMetaData("groupName",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.CREATOR, new org.apache.thrift.meta_data.FieldMetaData(
        "creator", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.USER_LIST,
        new org.apache.thrift.meta_data.FieldMetaData("userList",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.USER_NUM, new org.apache.thrift.meta_data.FieldMetaData(
        "userNum", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.I32)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(group.class,
        metaDataMap);
  }

  public group() {
  }

  public group(String groupName, String creator, String userList, int userNum) {
    this();
    this.groupName = groupName;
    this.creator = creator;
    this.userList = userList;
    this.userNum = userNum;
    setUserNumIsSet(true);
  }

  public group(group other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    if (other.isSetGroupName()) {
      this.groupName = other.groupName;
    }
    if (other.isSetCreator()) {
      this.creator = other.creator;
    }
    if (other.isSetUserList()) {
      this.userList = other.userList;
    }
    this.userNum = other.userNum;
  }

  public group deepCopy() {
    return new group(this);
  }

  @Override
  public void clear() {
    this.groupName = null;
    this.creator = null;
    this.userList = null;
    setUserNumIsSet(false);
    this.userNum = 0;
  }

  public String getGroupName() {
    return this.groupName;
  }

  public void setGroupName(String groupName) {
    this.groupName = groupName;
  }

  public void unsetGroupName() {
    this.groupName = null;
  }

  public boolean isSetGroupName() {
    return this.groupName != null;
  }

  public void setGroupNameIsSet(boolean value) {
    if (!value) {
      this.groupName = null;
    }
  }

  public String getCreator() {
    return this.creator;
  }

  public void setCreator(String creator) {
    this.creator = creator;
  }

  public void unsetCreator() {
    this.creator = null;
  }

  public boolean isSetCreator() {
    return this.creator != null;
  }

  public void setCreatorIsSet(boolean value) {
    if (!value) {
      this.creator = null;
    }
  }

  public String getUserList() {
    return this.userList;
  }

  public void setUserList(String userList) {
    this.userList = userList;
  }

  public void unsetUserList() {
    this.userList = null;
  }

  public boolean isSetUserList() {
    return this.userList != null;
  }

  public void setUserListIsSet(boolean value) {
    if (!value) {
      this.userList = null;
    }
  }

  public int getUserNum() {
    return this.userNum;
  }

  public void setUserNum(int userNum) {
    this.userNum = userNum;
    setUserNumIsSet(true);
  }

  public void unsetUserNum() {
    __isset_bit_vector.clear(__USERNUM_ISSET_ID);
  }

  public boolean isSetUserNum() {
    return __isset_bit_vector.get(__USERNUM_ISSET_ID);
  }

  public void setUserNumIsSet(boolean value) {
    __isset_bit_vector.set(__USERNUM_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case GROUP_NAME:
      if (value == null) {
        unsetGroupName();
      } else {
        setGroupName((String) value);
      }
      break;

    case CREATOR:
      if (value == null) {
        unsetCreator();
      } else {
        setCreator((String) value);
      }
      break;

    case USER_LIST:
      if (value == null) {
        unsetUserList();
      } else {
        setUserList((String) value);
      }
      break;

    case USER_NUM:
      if (value == null) {
        unsetUserNum();
      } else {
        setUserNum((Integer) value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case GROUP_NAME:
      return getGroupName();

    case CREATOR:
      return getCreator();

    case USER_LIST:
      return getUserList();

    case USER_NUM:
      return Integer.valueOf(getUserNum());

    }
    throw new IllegalStateException();
  }

  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case GROUP_NAME:
      return isSetGroupName();
    case CREATOR:
      return isSetCreator();
    case USER_LIST:
      return isSetUserList();
    case USER_NUM:
      return isSetUserNum();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof group)
      return this.equals((group) that);
    return false;
  }

  public boolean equals(group that) {
    if (that == null)
      return false;

    boolean this_present_groupName = true && this.isSetGroupName();
    boolean that_present_groupName = true && that.isSetGroupName();
    if (this_present_groupName || that_present_groupName) {
      if (!(this_present_groupName && that_present_groupName))
        return false;
      if (!this.groupName.equals(that.groupName))
        return false;
    }

    boolean this_present_creator = true && this.isSetCreator();
    boolean that_present_creator = true && that.isSetCreator();
    if (this_present_creator || that_present_creator) {
      if (!(this_present_creator && that_present_creator))
        return false;
      if (!this.creator.equals(that.creator))
        return false;
    }

    boolean this_present_userList = true && this.isSetUserList();
    boolean that_present_userList = true && that.isSetUserList();
    if (this_present_userList || that_present_userList) {
      if (!(this_present_userList && that_present_userList))
        return false;
      if (!this.userList.equals(that.userList))
        return false;
    }

    boolean this_present_userNum = true;
    boolean that_present_userNum = true;
    if (this_present_userNum || that_present_userNum) {
      if (!(this_present_userNum && that_present_userNum))
        return false;
      if (this.userNum != that.userNum)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(group other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    group typedOther = (group) other;

    lastComparison = Boolean.valueOf(isSetGroupName()).compareTo(
        typedOther.isSetGroupName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetGroupName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.groupName,
          typedOther.groupName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetCreator()).compareTo(
        typedOther.isSetCreator());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCreator()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.creator,
          typedOther.creator);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetUserList()).compareTo(
        typedOther.isSetUserList());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetUserList()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.userList,
          typedOther.userList);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetUserNum()).compareTo(
        typedOther.isSetUserNum());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetUserNum()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.userNum,
          typedOther.userNum);
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
    StringBuilder sb = new StringBuilder("group(");
    boolean first = true;

    sb.append("groupName:");
    if (this.groupName == null) {
      sb.append("null");
    } else {
      sb.append(this.groupName);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("creator:");
    if (this.creator == null) {
      sb.append("null");
    } else {
      sb.append(this.creator);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("userList:");
    if (this.userList == null) {
      sb.append("null");
    } else {
      sb.append(this.userList);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("userNum:");
    sb.append(this.userNum);
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

  private static class groupStandardSchemeFactory implements SchemeFactory {
    public groupStandardScheme getScheme() {
      return new groupStandardScheme();
    }
  }

  private static class groupStandardScheme extends StandardScheme<group> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, group struct)
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
            struct.groupName = iprot.readString();
            struct.setGroupNameIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 2:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.creator = iprot.readString();
            struct.setCreatorIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 3:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.userList = iprot.readString();
            struct.setUserListIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 4:
          if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
            struct.userNum = iprot.readI32();
            struct.setUserNumIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, group struct)
        throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.groupName != null) {
        oprot.writeFieldBegin(GROUP_NAME_FIELD_DESC);
        oprot.writeString(struct.groupName);
        oprot.writeFieldEnd();
      }
      if (struct.creator != null) {
        oprot.writeFieldBegin(CREATOR_FIELD_DESC);
        oprot.writeString(struct.creator);
        oprot.writeFieldEnd();
      }
      if (struct.userList != null) {
        oprot.writeFieldBegin(USER_LIST_FIELD_DESC);
        oprot.writeString(struct.userList);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(USER_NUM_FIELD_DESC);
      oprot.writeI32(struct.userNum);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class groupTupleSchemeFactory implements SchemeFactory {
    public groupTupleScheme getScheme() {
      return new groupTupleScheme();
    }
  }

  private static class groupTupleScheme extends TupleScheme<group> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, group struct)
        throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetGroupName()) {
        optionals.set(0);
      }
      if (struct.isSetCreator()) {
        optionals.set(1);
      }
      if (struct.isSetUserList()) {
        optionals.set(2);
      }
      if (struct.isSetUserNum()) {
        optionals.set(3);
      }
      oprot.writeBitSet(optionals, 4);
      if (struct.isSetGroupName()) {
        oprot.writeString(struct.groupName);
      }
      if (struct.isSetCreator()) {
        oprot.writeString(struct.creator);
      }
      if (struct.isSetUserList()) {
        oprot.writeString(struct.userList);
      }
      if (struct.isSetUserNum()) {
        oprot.writeI32(struct.userNum);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, group struct)
        throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(4);
      if (incoming.get(0)) {
        struct.groupName = iprot.readString();
        struct.setGroupNameIsSet(true);
      }
      if (incoming.get(1)) {
        struct.creator = iprot.readString();
        struct.setCreatorIsSet(true);
      }
      if (incoming.get(2)) {
        struct.userList = iprot.readString();
        struct.setUserListIsSet(true);
      }
      if (incoming.get(3)) {
        struct.userNum = iprot.readI32();
        struct.setUserNumIsSet(true);
      }
    }
  }

}
