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

public class RenameColDesc implements
    org.apache.thrift.TBase<RenameColDesc, RenameColDesc._Fields>,
    java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct(
      "RenameColDesc");

  private static final org.apache.thrift.protocol.TField DB_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "dbName", org.apache.thrift.protocol.TType.STRING, (short) 1);
  private static final org.apache.thrift.protocol.TField TABLE_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "tableName", org.apache.thrift.protocol.TType.STRING, (short) 2);
  private static final org.apache.thrift.protocol.TField USER_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "user", org.apache.thrift.protocol.TType.STRING, (short) 3);
  private static final org.apache.thrift.protocol.TField OLD_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "oldName", org.apache.thrift.protocol.TType.STRING, (short) 4);
  private static final org.apache.thrift.protocol.TField NEW_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "newName", org.apache.thrift.protocol.TType.STRING, (short) 5);
  private static final org.apache.thrift.protocol.TField TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "type", org.apache.thrift.protocol.TType.STRING, (short) 6);
  private static final org.apache.thrift.protocol.TField COMMENT_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "comment", org.apache.thrift.protocol.TType.STRING, (short) 7);
  private static final org.apache.thrift.protocol.TField IS_FIRST_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "isFirst", org.apache.thrift.protocol.TType.BOOL, (short) 8);
  private static final org.apache.thrift.protocol.TField AFTER_COL_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "afterCol", org.apache.thrift.protocol.TType.STRING, (short) 9);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new RenameColDescStandardSchemeFactory());
    schemes.put(TupleScheme.class, new RenameColDescTupleSchemeFactory());
  }

  private String dbName;
  private String tableName;
  private String user;
  private String oldName;
  private String newName;
  private String type;
  private String comment;
  private boolean isFirst;
  private String afterCol;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    DB_NAME((short) 1, "dbName"), TABLE_NAME((short) 2, "tableName"), USER(
        (short) 3, "user"), OLD_NAME((short) 4, "oldName"), NEW_NAME((short) 5,
        "newName"), TYPE((short) 6, "type"), COMMENT((short) 7, "comment"), IS_FIRST(
        (short) 8, "isFirst"), AFTER_COL((short) 9, "afterCol");

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
        return OLD_NAME;
      case 5:
        return NEW_NAME;
      case 6:
        return TYPE;
      case 7:
        return COMMENT;
      case 8:
        return IS_FIRST;
      case 9:
        return AFTER_COL;
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

  private static final int __ISFIRST_ISSET_ID = 0;
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
    tmpMap.put(_Fields.OLD_NAME, new org.apache.thrift.meta_data.FieldMetaData(
        "oldName", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.NEW_NAME, new org.apache.thrift.meta_data.FieldMetaData(
        "newName", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.TYPE, new org.apache.thrift.meta_data.FieldMetaData(
        "type", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.COMMENT, new org.apache.thrift.meta_data.FieldMetaData(
        "comment", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.IS_FIRST, new org.apache.thrift.meta_data.FieldMetaData(
        "isFirst", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.AFTER_COL,
        new org.apache.thrift.meta_data.FieldMetaData("afterCol",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(
        RenameColDesc.class, metaDataMap);
  }

  public RenameColDesc() {
  }

  public RenameColDesc(String dbName, String tableName, String user,
      String oldName, String newName, String type, String comment,
      boolean isFirst, String afterCol) {
    this();
    this.dbName = dbName;
    this.tableName = tableName;
    this.user = user;
    this.oldName = oldName;
    this.newName = newName;
    this.type = type;
    this.comment = comment;
    this.isFirst = isFirst;
    setIsFirstIsSet(true);
    this.afterCol = afterCol;
  }

  public RenameColDesc(RenameColDesc other) {
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
    if (other.isSetOldName()) {
      this.oldName = other.oldName;
    }
    if (other.isSetNewName()) {
      this.newName = other.newName;
    }
    if (other.isSetType()) {
      this.type = other.type;
    }
    if (other.isSetComment()) {
      this.comment = other.comment;
    }
    this.isFirst = other.isFirst;
    if (other.isSetAfterCol()) {
      this.afterCol = other.afterCol;
    }
  }

  public RenameColDesc deepCopy() {
    return new RenameColDesc(this);
  }

  @Override
  public void clear() {
    this.dbName = null;
    this.tableName = null;
    this.user = null;
    this.oldName = null;
    this.newName = null;
    this.type = null;
    this.comment = null;
    setIsFirstIsSet(false);
    this.isFirst = false;
    this.afterCol = null;
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

  public String getOldName() {
    return this.oldName;
  }

  public void setOldName(String oldName) {
    this.oldName = oldName;
  }

  public void unsetOldName() {
    this.oldName = null;
  }

  public boolean isSetOldName() {
    return this.oldName != null;
  }

  public void setOldNameIsSet(boolean value) {
    if (!value) {
      this.oldName = null;
    }
  }

  public String getNewName() {
    return this.newName;
  }

  public void setNewName(String newName) {
    this.newName = newName;
  }

  public void unsetNewName() {
    this.newName = null;
  }

  public boolean isSetNewName() {
    return this.newName != null;
  }

  public void setNewNameIsSet(boolean value) {
    if (!value) {
      this.newName = null;
    }
  }

  public String getType() {
    return this.type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public void unsetType() {
    this.type = null;
  }

  public boolean isSetType() {
    return this.type != null;
  }

  public void setTypeIsSet(boolean value) {
    if (!value) {
      this.type = null;
    }
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

  public boolean isIsFirst() {
    return this.isFirst;
  }

  public void setIsFirst(boolean isFirst) {
    this.isFirst = isFirst;
    setIsFirstIsSet(true);
  }

  public void unsetIsFirst() {
    __isset_bit_vector.clear(__ISFIRST_ISSET_ID);
  }

  public boolean isSetIsFirst() {
    return __isset_bit_vector.get(__ISFIRST_ISSET_ID);
  }

  public void setIsFirstIsSet(boolean value) {
    __isset_bit_vector.set(__ISFIRST_ISSET_ID, value);
  }

  public String getAfterCol() {
    return this.afterCol;
  }

  public void setAfterCol(String afterCol) {
    this.afterCol = afterCol;
  }

  public void unsetAfterCol() {
    this.afterCol = null;
  }

  public boolean isSetAfterCol() {
    return this.afterCol != null;
  }

  public void setAfterColIsSet(boolean value) {
    if (!value) {
      this.afterCol = null;
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

    case OLD_NAME:
      if (value == null) {
        unsetOldName();
      } else {
        setOldName((String) value);
      }
      break;

    case NEW_NAME:
      if (value == null) {
        unsetNewName();
      } else {
        setNewName((String) value);
      }
      break;

    case TYPE:
      if (value == null) {
        unsetType();
      } else {
        setType((String) value);
      }
      break;

    case COMMENT:
      if (value == null) {
        unsetComment();
      } else {
        setComment((String) value);
      }
      break;

    case IS_FIRST:
      if (value == null) {
        unsetIsFirst();
      } else {
        setIsFirst((Boolean) value);
      }
      break;

    case AFTER_COL:
      if (value == null) {
        unsetAfterCol();
      } else {
        setAfterCol((String) value);
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

    case OLD_NAME:
      return getOldName();

    case NEW_NAME:
      return getNewName();

    case TYPE:
      return getType();

    case COMMENT:
      return getComment();

    case IS_FIRST:
      return Boolean.valueOf(isIsFirst());

    case AFTER_COL:
      return getAfterCol();

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
    case OLD_NAME:
      return isSetOldName();
    case NEW_NAME:
      return isSetNewName();
    case TYPE:
      return isSetType();
    case COMMENT:
      return isSetComment();
    case IS_FIRST:
      return isSetIsFirst();
    case AFTER_COL:
      return isSetAfterCol();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof RenameColDesc)
      return this.equals((RenameColDesc) that);
    return false;
  }

  public boolean equals(RenameColDesc that) {
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

    boolean this_present_oldName = true && this.isSetOldName();
    boolean that_present_oldName = true && that.isSetOldName();
    if (this_present_oldName || that_present_oldName) {
      if (!(this_present_oldName && that_present_oldName))
        return false;
      if (!this.oldName.equals(that.oldName))
        return false;
    }

    boolean this_present_newName = true && this.isSetNewName();
    boolean that_present_newName = true && that.isSetNewName();
    if (this_present_newName || that_present_newName) {
      if (!(this_present_newName && that_present_newName))
        return false;
      if (!this.newName.equals(that.newName))
        return false;
    }

    boolean this_present_type = true && this.isSetType();
    boolean that_present_type = true && that.isSetType();
    if (this_present_type || that_present_type) {
      if (!(this_present_type && that_present_type))
        return false;
      if (!this.type.equals(that.type))
        return false;
    }

    boolean this_present_comment = true && this.isSetComment();
    boolean that_present_comment = true && that.isSetComment();
    if (this_present_comment || that_present_comment) {
      if (!(this_present_comment && that_present_comment))
        return false;
      if (!this.comment.equals(that.comment))
        return false;
    }

    boolean this_present_isFirst = true;
    boolean that_present_isFirst = true;
    if (this_present_isFirst || that_present_isFirst) {
      if (!(this_present_isFirst && that_present_isFirst))
        return false;
      if (this.isFirst != that.isFirst)
        return false;
    }

    boolean this_present_afterCol = true && this.isSetAfterCol();
    boolean that_present_afterCol = true && that.isSetAfterCol();
    if (this_present_afterCol || that_present_afterCol) {
      if (!(this_present_afterCol && that_present_afterCol))
        return false;
      if (!this.afterCol.equals(that.afterCol))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(RenameColDesc other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    RenameColDesc typedOther = (RenameColDesc) other;

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
    lastComparison = Boolean.valueOf(isSetOldName()).compareTo(
        typedOther.isSetOldName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetOldName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.oldName,
          typedOther.oldName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetNewName()).compareTo(
        typedOther.isSetNewName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetNewName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.newName,
          typedOther.newName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetType()).compareTo(
        typedOther.isSetType());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetType()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.type,
          typedOther.type);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
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
    lastComparison = Boolean.valueOf(isSetIsFirst()).compareTo(
        typedOther.isSetIsFirst());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetIsFirst()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.isFirst,
          typedOther.isFirst);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetAfterCol()).compareTo(
        typedOther.isSetAfterCol());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetAfterCol()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.afterCol,
          typedOther.afterCol);
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
    StringBuilder sb = new StringBuilder("RenameColDesc(");
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
    sb.append("oldName:");
    if (this.oldName == null) {
      sb.append("null");
    } else {
      sb.append(this.oldName);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("newName:");
    if (this.newName == null) {
      sb.append("null");
    } else {
      sb.append(this.newName);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("type:");
    if (this.type == null) {
      sb.append("null");
    } else {
      sb.append(this.type);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("comment:");
    if (this.comment == null) {
      sb.append("null");
    } else {
      sb.append(this.comment);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("isFirst:");
    sb.append(this.isFirst);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("afterCol:");
    if (this.afterCol == null) {
      sb.append("null");
    } else {
      sb.append(this.afterCol);
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

  private static class RenameColDescStandardSchemeFactory implements
      SchemeFactory {
    public RenameColDescStandardScheme getScheme() {
      return new RenameColDescStandardScheme();
    }
  }

  private static class RenameColDescStandardScheme extends
      StandardScheme<RenameColDesc> {

    public void read(org.apache.thrift.protocol.TProtocol iprot,
        RenameColDesc struct) throws org.apache.thrift.TException {
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
            struct.oldName = iprot.readString();
            struct.setOldNameIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 5:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.newName = iprot.readString();
            struct.setNewNameIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 6:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.type = iprot.readString();
            struct.setTypeIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 7:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.comment = iprot.readString();
            struct.setCommentIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 8:
          if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
            struct.isFirst = iprot.readBool();
            struct.setIsFirstIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 9:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.afterCol = iprot.readString();
            struct.setAfterColIsSet(true);
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
        RenameColDesc struct) throws org.apache.thrift.TException {
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
      if (struct.oldName != null) {
        oprot.writeFieldBegin(OLD_NAME_FIELD_DESC);
        oprot.writeString(struct.oldName);
        oprot.writeFieldEnd();
      }
      if (struct.newName != null) {
        oprot.writeFieldBegin(NEW_NAME_FIELD_DESC);
        oprot.writeString(struct.newName);
        oprot.writeFieldEnd();
      }
      if (struct.type != null) {
        oprot.writeFieldBegin(TYPE_FIELD_DESC);
        oprot.writeString(struct.type);
        oprot.writeFieldEnd();
      }
      if (struct.comment != null) {
        oprot.writeFieldBegin(COMMENT_FIELD_DESC);
        oprot.writeString(struct.comment);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(IS_FIRST_FIELD_DESC);
      oprot.writeBool(struct.isFirst);
      oprot.writeFieldEnd();
      if (struct.afterCol != null) {
        oprot.writeFieldBegin(AFTER_COL_FIELD_DESC);
        oprot.writeString(struct.afterCol);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class RenameColDescTupleSchemeFactory implements SchemeFactory {
    public RenameColDescTupleScheme getScheme() {
      return new RenameColDescTupleScheme();
    }
  }

  private static class RenameColDescTupleScheme extends
      TupleScheme<RenameColDesc> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot,
        RenameColDesc struct) throws org.apache.thrift.TException {
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
      if (struct.isSetOldName()) {
        optionals.set(3);
      }
      if (struct.isSetNewName()) {
        optionals.set(4);
      }
      if (struct.isSetType()) {
        optionals.set(5);
      }
      if (struct.isSetComment()) {
        optionals.set(6);
      }
      if (struct.isSetIsFirst()) {
        optionals.set(7);
      }
      if (struct.isSetAfterCol()) {
        optionals.set(8);
      }
      oprot.writeBitSet(optionals, 9);
      if (struct.isSetDbName()) {
        oprot.writeString(struct.dbName);
      }
      if (struct.isSetTableName()) {
        oprot.writeString(struct.tableName);
      }
      if (struct.isSetUser()) {
        oprot.writeString(struct.user);
      }
      if (struct.isSetOldName()) {
        oprot.writeString(struct.oldName);
      }
      if (struct.isSetNewName()) {
        oprot.writeString(struct.newName);
      }
      if (struct.isSetType()) {
        oprot.writeString(struct.type);
      }
      if (struct.isSetComment()) {
        oprot.writeString(struct.comment);
      }
      if (struct.isSetIsFirst()) {
        oprot.writeBool(struct.isFirst);
      }
      if (struct.isSetAfterCol()) {
        oprot.writeString(struct.afterCol);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot,
        RenameColDesc struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(9);
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
        struct.oldName = iprot.readString();
        struct.setOldNameIsSet(true);
      }
      if (incoming.get(4)) {
        struct.newName = iprot.readString();
        struct.setNewNameIsSet(true);
      }
      if (incoming.get(5)) {
        struct.type = iprot.readString();
        struct.setTypeIsSet(true);
      }
      if (incoming.get(6)) {
        struct.comment = iprot.readString();
        struct.setCommentIsSet(true);
      }
      if (incoming.get(7)) {
        struct.isFirst = iprot.readBool();
        struct.setIsFirstIsSet(true);
      }
      if (incoming.get(8)) {
        struct.afterCol = iprot.readString();
        struct.setAfterColIsSet(true);
      }
    }
  }

}
