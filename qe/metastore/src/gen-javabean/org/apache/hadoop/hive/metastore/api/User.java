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

public class User implements org.apache.thrift.TBase<User, User._Fields>,
    java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct(
      "User");

  private static final org.apache.thrift.protocol.TField USER_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "userName", org.apache.thrift.protocol.TType.STRING, (short) 1);
  private static final org.apache.thrift.protocol.TField PLAY_ROLES_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "playRoles", org.apache.thrift.protocol.TType.LIST, (short) 2);
  private static final org.apache.thrift.protocol.TField SELECT_PRIV_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "selectPriv", org.apache.thrift.protocol.TType.BOOL, (short) 3);
  private static final org.apache.thrift.protocol.TField INSERT_PRIV_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "insertPriv", org.apache.thrift.protocol.TType.BOOL, (short) 4);
  private static final org.apache.thrift.protocol.TField INDEX_PRIV_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "indexPriv", org.apache.thrift.protocol.TType.BOOL, (short) 5);
  private static final org.apache.thrift.protocol.TField CREATE_PRIV_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "createPriv", org.apache.thrift.protocol.TType.BOOL, (short) 6);
  private static final org.apache.thrift.protocol.TField DROP_PRIV_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "dropPriv", org.apache.thrift.protocol.TType.BOOL, (short) 7);
  private static final org.apache.thrift.protocol.TField DELETE_PRIV_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "deletePriv", org.apache.thrift.protocol.TType.BOOL, (short) 8);
  private static final org.apache.thrift.protocol.TField ALTER_PRIV_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "alterPriv", org.apache.thrift.protocol.TType.BOOL, (short) 9);
  private static final org.apache.thrift.protocol.TField UPDATE_PRIV_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "updatePriv", org.apache.thrift.protocol.TType.BOOL, (short) 10);
  private static final org.apache.thrift.protocol.TField CREATEVIEW_PRIV_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "createviewPriv", org.apache.thrift.protocol.TType.BOOL, (short) 11);
  private static final org.apache.thrift.protocol.TField SHOWVIEW_PRIV_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "showviewPriv", org.apache.thrift.protocol.TType.BOOL, (short) 12);
  private static final org.apache.thrift.protocol.TField DBA_PRIV_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "dbaPriv", org.apache.thrift.protocol.TType.BOOL, (short) 13);
  private static final org.apache.thrift.protocol.TField GROUP_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "groupName", org.apache.thrift.protocol.TType.STRING, (short) 14);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new UserStandardSchemeFactory());
    schemes.put(TupleScheme.class, new UserTupleSchemeFactory());
  }

  private String userName;
  private List<String> playRoles;
  private boolean selectPriv;
  private boolean insertPriv;
  private boolean indexPriv;
  private boolean createPriv;
  private boolean dropPriv;
  private boolean deletePriv;
  private boolean alterPriv;
  private boolean updatePriv;
  private boolean createviewPriv;
  private boolean showviewPriv;
  private boolean dbaPriv;
  private String groupName;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    USER_NAME((short) 1, "userName"), PLAY_ROLES((short) 2, "playRoles"), SELECT_PRIV(
        (short) 3, "selectPriv"), INSERT_PRIV((short) 4, "insertPriv"), INDEX_PRIV(
        (short) 5, "indexPriv"), CREATE_PRIV((short) 6, "createPriv"), DROP_PRIV(
        (short) 7, "dropPriv"), DELETE_PRIV((short) 8, "deletePriv"), ALTER_PRIV(
        (short) 9, "alterPriv"), UPDATE_PRIV((short) 10, "updatePriv"), CREATEVIEW_PRIV(
        (short) 11, "createviewPriv"), SHOWVIEW_PRIV((short) 12, "showviewPriv"), DBA_PRIV(
        (short) 13, "dbaPriv"), GROUP_NAME((short) 14, "groupName");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    public static _Fields findByThriftId(int fieldId) {
      switch (fieldId) {
      case 1:
        return USER_NAME;
      case 2:
        return PLAY_ROLES;
      case 3:
        return SELECT_PRIV;
      case 4:
        return INSERT_PRIV;
      case 5:
        return INDEX_PRIV;
      case 6:
        return CREATE_PRIV;
      case 7:
        return DROP_PRIV;
      case 8:
        return DELETE_PRIV;
      case 9:
        return ALTER_PRIV;
      case 10:
        return UPDATE_PRIV;
      case 11:
        return CREATEVIEW_PRIV;
      case 12:
        return SHOWVIEW_PRIV;
      case 13:
        return DBA_PRIV;
      case 14:
        return GROUP_NAME;
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

  private static final int __SELECTPRIV_ISSET_ID = 0;
  private static final int __INSERTPRIV_ISSET_ID = 1;
  private static final int __INDEXPRIV_ISSET_ID = 2;
  private static final int __CREATEPRIV_ISSET_ID = 3;
  private static final int __DROPPRIV_ISSET_ID = 4;
  private static final int __DELETEPRIV_ISSET_ID = 5;
  private static final int __ALTERPRIV_ISSET_ID = 6;
  private static final int __UPDATEPRIV_ISSET_ID = 7;
  private static final int __CREATEVIEWPRIV_ISSET_ID = 8;
  private static final int __SHOWVIEWPRIV_ISSET_ID = 9;
  private static final int __DBAPRIV_ISSET_ID = 10;
  private BitSet __isset_bit_vector = new BitSet(11);
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(
        _Fields.class);
    tmpMap.put(_Fields.USER_NAME,
        new org.apache.thrift.meta_data.FieldMetaData("userName",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.PLAY_ROLES,
        new org.apache.thrift.meta_data.FieldMetaData("playRoles",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.ListMetaData(
                org.apache.thrift.protocol.TType.LIST,
                new org.apache.thrift.meta_data.FieldValueMetaData(
                    org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.SELECT_PRIV,
        new org.apache.thrift.meta_data.FieldMetaData("selectPriv",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.INSERT_PRIV,
        new org.apache.thrift.meta_data.FieldMetaData("insertPriv",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.INDEX_PRIV,
        new org.apache.thrift.meta_data.FieldMetaData("indexPriv",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.CREATE_PRIV,
        new org.apache.thrift.meta_data.FieldMetaData("createPriv",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.DROP_PRIV,
        new org.apache.thrift.meta_data.FieldMetaData("dropPriv",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.DELETE_PRIV,
        new org.apache.thrift.meta_data.FieldMetaData("deletePriv",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.ALTER_PRIV,
        new org.apache.thrift.meta_data.FieldMetaData("alterPriv",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.UPDATE_PRIV,
        new org.apache.thrift.meta_data.FieldMetaData("updatePriv",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.CREATEVIEW_PRIV,
        new org.apache.thrift.meta_data.FieldMetaData("createviewPriv",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.SHOWVIEW_PRIV,
        new org.apache.thrift.meta_data.FieldMetaData("showviewPriv",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.DBA_PRIV, new org.apache.thrift.meta_data.FieldMetaData(
        "dbaPriv", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.GROUP_NAME,
        new org.apache.thrift.meta_data.FieldMetaData("groupName",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(User.class,
        metaDataMap);
  }

  public User() {
  }

  public User(String userName, List<String> playRoles, boolean selectPriv,
      boolean insertPriv, boolean indexPriv, boolean createPriv,
      boolean dropPriv, boolean deletePriv, boolean alterPriv,
      boolean updatePriv, boolean createviewPriv, boolean showviewPriv,
      boolean dbaPriv, String groupName) {
    this();
    this.userName = userName;
    this.playRoles = playRoles;
    this.selectPriv = selectPriv;
    setSelectPrivIsSet(true);
    this.insertPriv = insertPriv;
    setInsertPrivIsSet(true);
    this.indexPriv = indexPriv;
    setIndexPrivIsSet(true);
    this.createPriv = createPriv;
    setCreatePrivIsSet(true);
    this.dropPriv = dropPriv;
    setDropPrivIsSet(true);
    this.deletePriv = deletePriv;
    setDeletePrivIsSet(true);
    this.alterPriv = alterPriv;
    setAlterPrivIsSet(true);
    this.updatePriv = updatePriv;
    setUpdatePrivIsSet(true);
    this.createviewPriv = createviewPriv;
    setCreateviewPrivIsSet(true);
    this.showviewPriv = showviewPriv;
    setShowviewPrivIsSet(true);
    this.dbaPriv = dbaPriv;
    setDbaPrivIsSet(true);
    this.groupName = groupName;
  }

  public User(User other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    if (other.isSetUserName()) {
      this.userName = other.userName;
    }
    if (other.isSetPlayRoles()) {
      List<String> __this__playRoles = new ArrayList<String>();
      for (String other_element : other.playRoles) {
        __this__playRoles.add(other_element);
      }
      this.playRoles = __this__playRoles;
    }
    this.selectPriv = other.selectPriv;
    this.insertPriv = other.insertPriv;
    this.indexPriv = other.indexPriv;
    this.createPriv = other.createPriv;
    this.dropPriv = other.dropPriv;
    this.deletePriv = other.deletePriv;
    this.alterPriv = other.alterPriv;
    this.updatePriv = other.updatePriv;
    this.createviewPriv = other.createviewPriv;
    this.showviewPriv = other.showviewPriv;
    this.dbaPriv = other.dbaPriv;
    if (other.isSetGroupName()) {
      this.groupName = other.groupName;
    }
  }

  public User deepCopy() {
    return new User(this);
  }

  @Override
  public void clear() {
    this.userName = null;
    this.playRoles = null;
    setSelectPrivIsSet(false);
    this.selectPriv = false;
    setInsertPrivIsSet(false);
    this.insertPriv = false;
    setIndexPrivIsSet(false);
    this.indexPriv = false;
    setCreatePrivIsSet(false);
    this.createPriv = false;
    setDropPrivIsSet(false);
    this.dropPriv = false;
    setDeletePrivIsSet(false);
    this.deletePriv = false;
    setAlterPrivIsSet(false);
    this.alterPriv = false;
    setUpdatePrivIsSet(false);
    this.updatePriv = false;
    setCreateviewPrivIsSet(false);
    this.createviewPriv = false;
    setShowviewPrivIsSet(false);
    this.showviewPriv = false;
    setDbaPrivIsSet(false);
    this.dbaPriv = false;
    this.groupName = null;
  }

  public String getUserName() {
    return this.userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public void unsetUserName() {
    this.userName = null;
  }

  public boolean isSetUserName() {
    return this.userName != null;
  }

  public void setUserNameIsSet(boolean value) {
    if (!value) {
      this.userName = null;
    }
  }

  public int getPlayRolesSize() {
    return (this.playRoles == null) ? 0 : this.playRoles.size();
  }

  public java.util.Iterator<String> getPlayRolesIterator() {
    return (this.playRoles == null) ? null : this.playRoles.iterator();
  }

  public void addToPlayRoles(String elem) {
    if (this.playRoles == null) {
      this.playRoles = new ArrayList<String>();
    }
    this.playRoles.add(elem);
  }

  public List<String> getPlayRoles() {
    return this.playRoles;
  }

  public void setPlayRoles(List<String> playRoles) {
    this.playRoles = playRoles;
  }

  public void unsetPlayRoles() {
    this.playRoles = null;
  }

  public boolean isSetPlayRoles() {
    return this.playRoles != null;
  }

  public void setPlayRolesIsSet(boolean value) {
    if (!value) {
      this.playRoles = null;
    }
  }

  public boolean isSelectPriv() {
    return this.selectPriv;
  }

  public void setSelectPriv(boolean selectPriv) {
    this.selectPriv = selectPriv;
    setSelectPrivIsSet(true);
  }

  public void unsetSelectPriv() {
    __isset_bit_vector.clear(__SELECTPRIV_ISSET_ID);
  }

  public boolean isSetSelectPriv() {
    return __isset_bit_vector.get(__SELECTPRIV_ISSET_ID);
  }

  public void setSelectPrivIsSet(boolean value) {
    __isset_bit_vector.set(__SELECTPRIV_ISSET_ID, value);
  }

  public boolean isInsertPriv() {
    return this.insertPriv;
  }

  public void setInsertPriv(boolean insertPriv) {
    this.insertPriv = insertPriv;
    setInsertPrivIsSet(true);
  }

  public void unsetInsertPriv() {
    __isset_bit_vector.clear(__INSERTPRIV_ISSET_ID);
  }

  public boolean isSetInsertPriv() {
    return __isset_bit_vector.get(__INSERTPRIV_ISSET_ID);
  }

  public void setInsertPrivIsSet(boolean value) {
    __isset_bit_vector.set(__INSERTPRIV_ISSET_ID, value);
  }

  public boolean isIndexPriv() {
    return this.indexPriv;
  }

  public void setIndexPriv(boolean indexPriv) {
    this.indexPriv = indexPriv;
    setIndexPrivIsSet(true);
  }

  public void unsetIndexPriv() {
    __isset_bit_vector.clear(__INDEXPRIV_ISSET_ID);
  }

  public boolean isSetIndexPriv() {
    return __isset_bit_vector.get(__INDEXPRIV_ISSET_ID);
  }

  public void setIndexPrivIsSet(boolean value) {
    __isset_bit_vector.set(__INDEXPRIV_ISSET_ID, value);
  }

  public boolean isCreatePriv() {
    return this.createPriv;
  }

  public void setCreatePriv(boolean createPriv) {
    this.createPriv = createPriv;
    setCreatePrivIsSet(true);
  }

  public void unsetCreatePriv() {
    __isset_bit_vector.clear(__CREATEPRIV_ISSET_ID);
  }

  public boolean isSetCreatePriv() {
    return __isset_bit_vector.get(__CREATEPRIV_ISSET_ID);
  }

  public void setCreatePrivIsSet(boolean value) {
    __isset_bit_vector.set(__CREATEPRIV_ISSET_ID, value);
  }

  public boolean isDropPriv() {
    return this.dropPriv;
  }

  public void setDropPriv(boolean dropPriv) {
    this.dropPriv = dropPriv;
    setDropPrivIsSet(true);
  }

  public void unsetDropPriv() {
    __isset_bit_vector.clear(__DROPPRIV_ISSET_ID);
  }

  public boolean isSetDropPriv() {
    return __isset_bit_vector.get(__DROPPRIV_ISSET_ID);
  }

  public void setDropPrivIsSet(boolean value) {
    __isset_bit_vector.set(__DROPPRIV_ISSET_ID, value);
  }

  public boolean isDeletePriv() {
    return this.deletePriv;
  }

  public void setDeletePriv(boolean deletePriv) {
    this.deletePriv = deletePriv;
    setDeletePrivIsSet(true);
  }

  public void unsetDeletePriv() {
    __isset_bit_vector.clear(__DELETEPRIV_ISSET_ID);
  }

  public boolean isSetDeletePriv() {
    return __isset_bit_vector.get(__DELETEPRIV_ISSET_ID);
  }

  public void setDeletePrivIsSet(boolean value) {
    __isset_bit_vector.set(__DELETEPRIV_ISSET_ID, value);
  }

  public boolean isAlterPriv() {
    return this.alterPriv;
  }

  public void setAlterPriv(boolean alterPriv) {
    this.alterPriv = alterPriv;
    setAlterPrivIsSet(true);
  }

  public void unsetAlterPriv() {
    __isset_bit_vector.clear(__ALTERPRIV_ISSET_ID);
  }

  public boolean isSetAlterPriv() {
    return __isset_bit_vector.get(__ALTERPRIV_ISSET_ID);
  }

  public void setAlterPrivIsSet(boolean value) {
    __isset_bit_vector.set(__ALTERPRIV_ISSET_ID, value);
  }

  public boolean isUpdatePriv() {
    return this.updatePriv;
  }

  public void setUpdatePriv(boolean updatePriv) {
    this.updatePriv = updatePriv;
    setUpdatePrivIsSet(true);
  }

  public void unsetUpdatePriv() {
    __isset_bit_vector.clear(__UPDATEPRIV_ISSET_ID);
  }

  public boolean isSetUpdatePriv() {
    return __isset_bit_vector.get(__UPDATEPRIV_ISSET_ID);
  }

  public void setUpdatePrivIsSet(boolean value) {
    __isset_bit_vector.set(__UPDATEPRIV_ISSET_ID, value);
  }

  public boolean isCreateviewPriv() {
    return this.createviewPriv;
  }

  public void setCreateviewPriv(boolean createviewPriv) {
    this.createviewPriv = createviewPriv;
    setCreateviewPrivIsSet(true);
  }

  public void unsetCreateviewPriv() {
    __isset_bit_vector.clear(__CREATEVIEWPRIV_ISSET_ID);
  }

  public boolean isSetCreateviewPriv() {
    return __isset_bit_vector.get(__CREATEVIEWPRIV_ISSET_ID);
  }

  public void setCreateviewPrivIsSet(boolean value) {
    __isset_bit_vector.set(__CREATEVIEWPRIV_ISSET_ID, value);
  }

  public boolean isShowviewPriv() {
    return this.showviewPriv;
  }

  public void setShowviewPriv(boolean showviewPriv) {
    this.showviewPriv = showviewPriv;
    setShowviewPrivIsSet(true);
  }

  public void unsetShowviewPriv() {
    __isset_bit_vector.clear(__SHOWVIEWPRIV_ISSET_ID);
  }

  public boolean isSetShowviewPriv() {
    return __isset_bit_vector.get(__SHOWVIEWPRIV_ISSET_ID);
  }

  public void setShowviewPrivIsSet(boolean value) {
    __isset_bit_vector.set(__SHOWVIEWPRIV_ISSET_ID, value);
  }

  public boolean isDbaPriv() {
    return this.dbaPriv;
  }

  public void setDbaPriv(boolean dbaPriv) {
    this.dbaPriv = dbaPriv;
    setDbaPrivIsSet(true);
  }

  public void unsetDbaPriv() {
    __isset_bit_vector.clear(__DBAPRIV_ISSET_ID);
  }

  public boolean isSetDbaPriv() {
    return __isset_bit_vector.get(__DBAPRIV_ISSET_ID);
  }

  public void setDbaPrivIsSet(boolean value) {
    __isset_bit_vector.set(__DBAPRIV_ISSET_ID, value);
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

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case USER_NAME:
      if (value == null) {
        unsetUserName();
      } else {
        setUserName((String) value);
      }
      break;

    case PLAY_ROLES:
      if (value == null) {
        unsetPlayRoles();
      } else {
        setPlayRoles((List<String>) value);
      }
      break;

    case SELECT_PRIV:
      if (value == null) {
        unsetSelectPriv();
      } else {
        setSelectPriv((Boolean) value);
      }
      break;

    case INSERT_PRIV:
      if (value == null) {
        unsetInsertPriv();
      } else {
        setInsertPriv((Boolean) value);
      }
      break;

    case INDEX_PRIV:
      if (value == null) {
        unsetIndexPriv();
      } else {
        setIndexPriv((Boolean) value);
      }
      break;

    case CREATE_PRIV:
      if (value == null) {
        unsetCreatePriv();
      } else {
        setCreatePriv((Boolean) value);
      }
      break;

    case DROP_PRIV:
      if (value == null) {
        unsetDropPriv();
      } else {
        setDropPriv((Boolean) value);
      }
      break;

    case DELETE_PRIV:
      if (value == null) {
        unsetDeletePriv();
      } else {
        setDeletePriv((Boolean) value);
      }
      break;

    case ALTER_PRIV:
      if (value == null) {
        unsetAlterPriv();
      } else {
        setAlterPriv((Boolean) value);
      }
      break;

    case UPDATE_PRIV:
      if (value == null) {
        unsetUpdatePriv();
      } else {
        setUpdatePriv((Boolean) value);
      }
      break;

    case CREATEVIEW_PRIV:
      if (value == null) {
        unsetCreateviewPriv();
      } else {
        setCreateviewPriv((Boolean) value);
      }
      break;

    case SHOWVIEW_PRIV:
      if (value == null) {
        unsetShowviewPriv();
      } else {
        setShowviewPriv((Boolean) value);
      }
      break;

    case DBA_PRIV:
      if (value == null) {
        unsetDbaPriv();
      } else {
        setDbaPriv((Boolean) value);
      }
      break;

    case GROUP_NAME:
      if (value == null) {
        unsetGroupName();
      } else {
        setGroupName((String) value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case USER_NAME:
      return getUserName();

    case PLAY_ROLES:
      return getPlayRoles();

    case SELECT_PRIV:
      return Boolean.valueOf(isSelectPriv());

    case INSERT_PRIV:
      return Boolean.valueOf(isInsertPriv());

    case INDEX_PRIV:
      return Boolean.valueOf(isIndexPriv());

    case CREATE_PRIV:
      return Boolean.valueOf(isCreatePriv());

    case DROP_PRIV:
      return Boolean.valueOf(isDropPriv());

    case DELETE_PRIV:
      return Boolean.valueOf(isDeletePriv());

    case ALTER_PRIV:
      return Boolean.valueOf(isAlterPriv());

    case UPDATE_PRIV:
      return Boolean.valueOf(isUpdatePriv());

    case CREATEVIEW_PRIV:
      return Boolean.valueOf(isCreateviewPriv());

    case SHOWVIEW_PRIV:
      return Boolean.valueOf(isShowviewPriv());

    case DBA_PRIV:
      return Boolean.valueOf(isDbaPriv());

    case GROUP_NAME:
      return getGroupName();

    }
    throw new IllegalStateException();
  }

  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case USER_NAME:
      return isSetUserName();
    case PLAY_ROLES:
      return isSetPlayRoles();
    case SELECT_PRIV:
      return isSetSelectPriv();
    case INSERT_PRIV:
      return isSetInsertPriv();
    case INDEX_PRIV:
      return isSetIndexPriv();
    case CREATE_PRIV:
      return isSetCreatePriv();
    case DROP_PRIV:
      return isSetDropPriv();
    case DELETE_PRIV:
      return isSetDeletePriv();
    case ALTER_PRIV:
      return isSetAlterPriv();
    case UPDATE_PRIV:
      return isSetUpdatePriv();
    case CREATEVIEW_PRIV:
      return isSetCreateviewPriv();
    case SHOWVIEW_PRIV:
      return isSetShowviewPriv();
    case DBA_PRIV:
      return isSetDbaPriv();
    case GROUP_NAME:
      return isSetGroupName();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof User)
      return this.equals((User) that);
    return false;
  }

  public boolean equals(User that) {
    if (that == null)
      return false;

    boolean this_present_userName = true && this.isSetUserName();
    boolean that_present_userName = true && that.isSetUserName();
    if (this_present_userName || that_present_userName) {
      if (!(this_present_userName && that_present_userName))
        return false;
      if (!this.userName.equals(that.userName))
        return false;
    }

    boolean this_present_playRoles = true && this.isSetPlayRoles();
    boolean that_present_playRoles = true && that.isSetPlayRoles();
    if (this_present_playRoles || that_present_playRoles) {
      if (!(this_present_playRoles && that_present_playRoles))
        return false;
      if (!this.playRoles.equals(that.playRoles))
        return false;
    }

    boolean this_present_selectPriv = true;
    boolean that_present_selectPriv = true;
    if (this_present_selectPriv || that_present_selectPriv) {
      if (!(this_present_selectPriv && that_present_selectPriv))
        return false;
      if (this.selectPriv != that.selectPriv)
        return false;
    }

    boolean this_present_insertPriv = true;
    boolean that_present_insertPriv = true;
    if (this_present_insertPriv || that_present_insertPriv) {
      if (!(this_present_insertPriv && that_present_insertPriv))
        return false;
      if (this.insertPriv != that.insertPriv)
        return false;
    }

    boolean this_present_indexPriv = true;
    boolean that_present_indexPriv = true;
    if (this_present_indexPriv || that_present_indexPriv) {
      if (!(this_present_indexPriv && that_present_indexPriv))
        return false;
      if (this.indexPriv != that.indexPriv)
        return false;
    }

    boolean this_present_createPriv = true;
    boolean that_present_createPriv = true;
    if (this_present_createPriv || that_present_createPriv) {
      if (!(this_present_createPriv && that_present_createPriv))
        return false;
      if (this.createPriv != that.createPriv)
        return false;
    }

    boolean this_present_dropPriv = true;
    boolean that_present_dropPriv = true;
    if (this_present_dropPriv || that_present_dropPriv) {
      if (!(this_present_dropPriv && that_present_dropPriv))
        return false;
      if (this.dropPriv != that.dropPriv)
        return false;
    }

    boolean this_present_deletePriv = true;
    boolean that_present_deletePriv = true;
    if (this_present_deletePriv || that_present_deletePriv) {
      if (!(this_present_deletePriv && that_present_deletePriv))
        return false;
      if (this.deletePriv != that.deletePriv)
        return false;
    }

    boolean this_present_alterPriv = true;
    boolean that_present_alterPriv = true;
    if (this_present_alterPriv || that_present_alterPriv) {
      if (!(this_present_alterPriv && that_present_alterPriv))
        return false;
      if (this.alterPriv != that.alterPriv)
        return false;
    }

    boolean this_present_updatePriv = true;
    boolean that_present_updatePriv = true;
    if (this_present_updatePriv || that_present_updatePriv) {
      if (!(this_present_updatePriv && that_present_updatePriv))
        return false;
      if (this.updatePriv != that.updatePriv)
        return false;
    }

    boolean this_present_createviewPriv = true;
    boolean that_present_createviewPriv = true;
    if (this_present_createviewPriv || that_present_createviewPriv) {
      if (!(this_present_createviewPriv && that_present_createviewPriv))
        return false;
      if (this.createviewPriv != that.createviewPriv)
        return false;
    }

    boolean this_present_showviewPriv = true;
    boolean that_present_showviewPriv = true;
    if (this_present_showviewPriv || that_present_showviewPriv) {
      if (!(this_present_showviewPriv && that_present_showviewPriv))
        return false;
      if (this.showviewPriv != that.showviewPriv)
        return false;
    }

    boolean this_present_dbaPriv = true;
    boolean that_present_dbaPriv = true;
    if (this_present_dbaPriv || that_present_dbaPriv) {
      if (!(this_present_dbaPriv && that_present_dbaPriv))
        return false;
      if (this.dbaPriv != that.dbaPriv)
        return false;
    }

    boolean this_present_groupName = true && this.isSetGroupName();
    boolean that_present_groupName = true && that.isSetGroupName();
    if (this_present_groupName || that_present_groupName) {
      if (!(this_present_groupName && that_present_groupName))
        return false;
      if (!this.groupName.equals(that.groupName))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(User other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    User typedOther = (User) other;

    lastComparison = Boolean.valueOf(isSetUserName()).compareTo(
        typedOther.isSetUserName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetUserName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.userName,
          typedOther.userName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetPlayRoles()).compareTo(
        typedOther.isSetPlayRoles());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPlayRoles()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.playRoles,
          typedOther.playRoles);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetSelectPriv()).compareTo(
        typedOther.isSetSelectPriv());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSelectPriv()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.selectPriv,
          typedOther.selectPriv);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetInsertPriv()).compareTo(
        typedOther.isSetInsertPriv());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetInsertPriv()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.insertPriv,
          typedOther.insertPriv);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetIndexPriv()).compareTo(
        typedOther.isSetIndexPriv());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetIndexPriv()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.indexPriv,
          typedOther.indexPriv);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetCreatePriv()).compareTo(
        typedOther.isSetCreatePriv());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCreatePriv()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.createPriv,
          typedOther.createPriv);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetDropPriv()).compareTo(
        typedOther.isSetDropPriv());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDropPriv()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.dropPriv,
          typedOther.dropPriv);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetDeletePriv()).compareTo(
        typedOther.isSetDeletePriv());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDeletePriv()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.deletePriv,
          typedOther.deletePriv);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetAlterPriv()).compareTo(
        typedOther.isSetAlterPriv());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetAlterPriv()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.alterPriv,
          typedOther.alterPriv);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetUpdatePriv()).compareTo(
        typedOther.isSetUpdatePriv());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetUpdatePriv()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.updatePriv,
          typedOther.updatePriv);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetCreateviewPriv()).compareTo(
        typedOther.isSetCreateviewPriv());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCreateviewPriv()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.createviewPriv, typedOther.createviewPriv);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetShowviewPriv()).compareTo(
        typedOther.isSetShowviewPriv());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetShowviewPriv()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.showviewPriv, typedOther.showviewPriv);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetDbaPriv()).compareTo(
        typedOther.isSetDbaPriv());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDbaPriv()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.dbaPriv,
          typedOther.dbaPriv);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
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
    StringBuilder sb = new StringBuilder("User(");
    boolean first = true;

    sb.append("userName:");
    if (this.userName == null) {
      sb.append("null");
    } else {
      sb.append(this.userName);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("playRoles:");
    if (this.playRoles == null) {
      sb.append("null");
    } else {
      sb.append(this.playRoles);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("selectPriv:");
    sb.append(this.selectPriv);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("insertPriv:");
    sb.append(this.insertPriv);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("indexPriv:");
    sb.append(this.indexPriv);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("createPriv:");
    sb.append(this.createPriv);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("dropPriv:");
    sb.append(this.dropPriv);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("deletePriv:");
    sb.append(this.deletePriv);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("alterPriv:");
    sb.append(this.alterPriv);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("updatePriv:");
    sb.append(this.updatePriv);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("createviewPriv:");
    sb.append(this.createviewPriv);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("showviewPriv:");
    sb.append(this.showviewPriv);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("dbaPriv:");
    sb.append(this.dbaPriv);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("groupName:");
    if (this.groupName == null) {
      sb.append("null");
    } else {
      sb.append(this.groupName);
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

  private static class UserStandardSchemeFactory implements SchemeFactory {
    public UserStandardScheme getScheme() {
      return new UserStandardScheme();
    }
  }

  private static class UserStandardScheme extends StandardScheme<User> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, User struct)
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
            struct.userName = iprot.readString();
            struct.setUserNameIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 2:
          if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
            {
              org.apache.thrift.protocol.TList _list106 = iprot.readListBegin();
              struct.playRoles = new ArrayList<String>(_list106.size);
              for (int _i107 = 0; _i107 < _list106.size; ++_i107) {
                String _elem108;
                _elem108 = iprot.readString();
                struct.playRoles.add(_elem108);
              }
              iprot.readListEnd();
            }
            struct.setPlayRolesIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 3:
          if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
            struct.selectPriv = iprot.readBool();
            struct.setSelectPrivIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 4:
          if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
            struct.insertPriv = iprot.readBool();
            struct.setInsertPrivIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 5:
          if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
            struct.indexPriv = iprot.readBool();
            struct.setIndexPrivIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 6:
          if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
            struct.createPriv = iprot.readBool();
            struct.setCreatePrivIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 7:
          if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
            struct.dropPriv = iprot.readBool();
            struct.setDropPrivIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 8:
          if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
            struct.deletePriv = iprot.readBool();
            struct.setDeletePrivIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 9:
          if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
            struct.alterPriv = iprot.readBool();
            struct.setAlterPrivIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 10:
          if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
            struct.updatePriv = iprot.readBool();
            struct.setUpdatePrivIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 11:
          if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
            struct.createviewPriv = iprot.readBool();
            struct.setCreateviewPrivIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 12:
          if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
            struct.showviewPriv = iprot.readBool();
            struct.setShowviewPrivIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 13:
          if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
            struct.dbaPriv = iprot.readBool();
            struct.setDbaPrivIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 14:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.groupName = iprot.readString();
            struct.setGroupNameIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, User struct)
        throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.userName != null) {
        oprot.writeFieldBegin(USER_NAME_FIELD_DESC);
        oprot.writeString(struct.userName);
        oprot.writeFieldEnd();
      }
      if (struct.playRoles != null) {
        oprot.writeFieldBegin(PLAY_ROLES_FIELD_DESC);
        {
          oprot
              .writeListBegin(new org.apache.thrift.protocol.TList(
                  org.apache.thrift.protocol.TType.STRING, struct.playRoles
                      .size()));
          for (String _iter109 : struct.playRoles) {
            oprot.writeString(_iter109);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(SELECT_PRIV_FIELD_DESC);
      oprot.writeBool(struct.selectPriv);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(INSERT_PRIV_FIELD_DESC);
      oprot.writeBool(struct.insertPriv);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(INDEX_PRIV_FIELD_DESC);
      oprot.writeBool(struct.indexPriv);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(CREATE_PRIV_FIELD_DESC);
      oprot.writeBool(struct.createPriv);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(DROP_PRIV_FIELD_DESC);
      oprot.writeBool(struct.dropPriv);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(DELETE_PRIV_FIELD_DESC);
      oprot.writeBool(struct.deletePriv);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(ALTER_PRIV_FIELD_DESC);
      oprot.writeBool(struct.alterPriv);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(UPDATE_PRIV_FIELD_DESC);
      oprot.writeBool(struct.updatePriv);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(CREATEVIEW_PRIV_FIELD_DESC);
      oprot.writeBool(struct.createviewPriv);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(SHOWVIEW_PRIV_FIELD_DESC);
      oprot.writeBool(struct.showviewPriv);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(DBA_PRIV_FIELD_DESC);
      oprot.writeBool(struct.dbaPriv);
      oprot.writeFieldEnd();
      if (struct.groupName != null) {
        oprot.writeFieldBegin(GROUP_NAME_FIELD_DESC);
        oprot.writeString(struct.groupName);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class UserTupleSchemeFactory implements SchemeFactory {
    public UserTupleScheme getScheme() {
      return new UserTupleScheme();
    }
  }

  private static class UserTupleScheme extends TupleScheme<User> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, User struct)
        throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetUserName()) {
        optionals.set(0);
      }
      if (struct.isSetPlayRoles()) {
        optionals.set(1);
      }
      if (struct.isSetSelectPriv()) {
        optionals.set(2);
      }
      if (struct.isSetInsertPriv()) {
        optionals.set(3);
      }
      if (struct.isSetIndexPriv()) {
        optionals.set(4);
      }
      if (struct.isSetCreatePriv()) {
        optionals.set(5);
      }
      if (struct.isSetDropPriv()) {
        optionals.set(6);
      }
      if (struct.isSetDeletePriv()) {
        optionals.set(7);
      }
      if (struct.isSetAlterPriv()) {
        optionals.set(8);
      }
      if (struct.isSetUpdatePriv()) {
        optionals.set(9);
      }
      if (struct.isSetCreateviewPriv()) {
        optionals.set(10);
      }
      if (struct.isSetShowviewPriv()) {
        optionals.set(11);
      }
      if (struct.isSetDbaPriv()) {
        optionals.set(12);
      }
      if (struct.isSetGroupName()) {
        optionals.set(13);
      }
      oprot.writeBitSet(optionals, 14);
      if (struct.isSetUserName()) {
        oprot.writeString(struct.userName);
      }
      if (struct.isSetPlayRoles()) {
        {
          oprot.writeI32(struct.playRoles.size());
          for (String _iter110 : struct.playRoles) {
            oprot.writeString(_iter110);
          }
        }
      }
      if (struct.isSetSelectPriv()) {
        oprot.writeBool(struct.selectPriv);
      }
      if (struct.isSetInsertPriv()) {
        oprot.writeBool(struct.insertPriv);
      }
      if (struct.isSetIndexPriv()) {
        oprot.writeBool(struct.indexPriv);
      }
      if (struct.isSetCreatePriv()) {
        oprot.writeBool(struct.createPriv);
      }
      if (struct.isSetDropPriv()) {
        oprot.writeBool(struct.dropPriv);
      }
      if (struct.isSetDeletePriv()) {
        oprot.writeBool(struct.deletePriv);
      }
      if (struct.isSetAlterPriv()) {
        oprot.writeBool(struct.alterPriv);
      }
      if (struct.isSetUpdatePriv()) {
        oprot.writeBool(struct.updatePriv);
      }
      if (struct.isSetCreateviewPriv()) {
        oprot.writeBool(struct.createviewPriv);
      }
      if (struct.isSetShowviewPriv()) {
        oprot.writeBool(struct.showviewPriv);
      }
      if (struct.isSetDbaPriv()) {
        oprot.writeBool(struct.dbaPriv);
      }
      if (struct.isSetGroupName()) {
        oprot.writeString(struct.groupName);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, User struct)
        throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(14);
      if (incoming.get(0)) {
        struct.userName = iprot.readString();
        struct.setUserNameIsSet(true);
      }
      if (incoming.get(1)) {
        {
          org.apache.thrift.protocol.TList _list111 = new org.apache.thrift.protocol.TList(
              org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.playRoles = new ArrayList<String>(_list111.size);
          for (int _i112 = 0; _i112 < _list111.size; ++_i112) {
            String _elem113;
            _elem113 = iprot.readString();
            struct.playRoles.add(_elem113);
          }
        }
        struct.setPlayRolesIsSet(true);
      }
      if (incoming.get(2)) {
        struct.selectPriv = iprot.readBool();
        struct.setSelectPrivIsSet(true);
      }
      if (incoming.get(3)) {
        struct.insertPriv = iprot.readBool();
        struct.setInsertPrivIsSet(true);
      }
      if (incoming.get(4)) {
        struct.indexPriv = iprot.readBool();
        struct.setIndexPrivIsSet(true);
      }
      if (incoming.get(5)) {
        struct.createPriv = iprot.readBool();
        struct.setCreatePrivIsSet(true);
      }
      if (incoming.get(6)) {
        struct.dropPriv = iprot.readBool();
        struct.setDropPrivIsSet(true);
      }
      if (incoming.get(7)) {
        struct.deletePriv = iprot.readBool();
        struct.setDeletePrivIsSet(true);
      }
      if (incoming.get(8)) {
        struct.alterPriv = iprot.readBool();
        struct.setAlterPrivIsSet(true);
      }
      if (incoming.get(9)) {
        struct.updatePriv = iprot.readBool();
        struct.setUpdatePrivIsSet(true);
      }
      if (incoming.get(10)) {
        struct.createviewPriv = iprot.readBool();
        struct.setCreateviewPrivIsSet(true);
      }
      if (incoming.get(11)) {
        struct.showviewPriv = iprot.readBool();
        struct.setShowviewPrivIsSet(true);
      }
      if (incoming.get(12)) {
        struct.dbaPriv = iprot.readBool();
        struct.setDbaPrivIsSet(true);
      }
      if (incoming.get(13)) {
        struct.groupName = iprot.readString();
        struct.setGroupNameIsSet(true);
      }
    }
  }

}
