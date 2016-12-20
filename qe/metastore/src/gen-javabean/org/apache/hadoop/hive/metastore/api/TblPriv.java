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

public class TblPriv implements
    org.apache.thrift.TBase<TblPriv, TblPriv._Fields>, java.io.Serializable,
    Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct(
      "TblPriv");

  private static final org.apache.thrift.protocol.TField DB_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "db", org.apache.thrift.protocol.TType.STRING, (short) 1);
  private static final org.apache.thrift.protocol.TField TBL_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "tbl", org.apache.thrift.protocol.TType.STRING, (short) 2);
  private static final org.apache.thrift.protocol.TField USER_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "user", org.apache.thrift.protocol.TType.STRING, (short) 3);
  private static final org.apache.thrift.protocol.TField SELECT_PRIV_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "selectPriv", org.apache.thrift.protocol.TType.BOOL, (short) 4);
  private static final org.apache.thrift.protocol.TField INSERT_PRIV_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "insertPriv", org.apache.thrift.protocol.TType.BOOL, (short) 5);
  private static final org.apache.thrift.protocol.TField INDEX_PRIV_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "indexPriv", org.apache.thrift.protocol.TType.BOOL, (short) 6);
  private static final org.apache.thrift.protocol.TField CREATE_PRIV_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "createPriv", org.apache.thrift.protocol.TType.BOOL, (short) 7);
  private static final org.apache.thrift.protocol.TField DROP_PRIV_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "dropPriv", org.apache.thrift.protocol.TType.BOOL, (short) 8);
  private static final org.apache.thrift.protocol.TField DELETE_PRIV_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "deletePriv", org.apache.thrift.protocol.TType.BOOL, (short) 9);
  private static final org.apache.thrift.protocol.TField ALTER_PRIV_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "alterPriv", org.apache.thrift.protocol.TType.BOOL, (short) 10);
  private static final org.apache.thrift.protocol.TField UPDATE_PRIV_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "updatePriv", org.apache.thrift.protocol.TType.BOOL, (short) 11);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TblPrivStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TblPrivTupleSchemeFactory());
  }

  private String db;
  private String tbl;
  private String user;
  private boolean selectPriv;
  private boolean insertPriv;
  private boolean indexPriv;
  private boolean createPriv;
  private boolean dropPriv;
  private boolean deletePriv;
  private boolean alterPriv;
  private boolean updatePriv;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    DB((short) 1, "db"), TBL((short) 2, "tbl"), USER((short) 3, "user"), SELECT_PRIV(
        (short) 4, "selectPriv"), INSERT_PRIV((short) 5, "insertPriv"), INDEX_PRIV(
        (short) 6, "indexPriv"), CREATE_PRIV((short) 7, "createPriv"), DROP_PRIV(
        (short) 8, "dropPriv"), DELETE_PRIV((short) 9, "deletePriv"), ALTER_PRIV(
        (short) 10, "alterPriv"), UPDATE_PRIV((short) 11, "updatePriv");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    public static _Fields findByThriftId(int fieldId) {
      switch (fieldId) {
      case 1:
        return DB;
      case 2:
        return TBL;
      case 3:
        return USER;
      case 4:
        return SELECT_PRIV;
      case 5:
        return INSERT_PRIV;
      case 6:
        return INDEX_PRIV;
      case 7:
        return CREATE_PRIV;
      case 8:
        return DROP_PRIV;
      case 9:
        return DELETE_PRIV;
      case 10:
        return ALTER_PRIV;
      case 11:
        return UPDATE_PRIV;
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
  private BitSet __isset_bit_vector = new BitSet(8);
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(
        _Fields.class);
    tmpMap.put(_Fields.DB, new org.apache.thrift.meta_data.FieldMetaData("db",
        org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.TBL, new org.apache.thrift.meta_data.FieldMetaData(
        "tbl", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.USER, new org.apache.thrift.meta_data.FieldMetaData(
        "user", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.STRING)));
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
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(
        TblPriv.class, metaDataMap);
  }

  public TblPriv() {
  }

  public TblPriv(String db, String tbl, String user, boolean selectPriv,
      boolean insertPriv, boolean indexPriv, boolean createPriv,
      boolean dropPriv, boolean deletePriv, boolean alterPriv,
      boolean updatePriv) {
    this();
    this.db = db;
    this.tbl = tbl;
    this.user = user;
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
  }

  public TblPriv(TblPriv other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    if (other.isSetDb()) {
      this.db = other.db;
    }
    if (other.isSetTbl()) {
      this.tbl = other.tbl;
    }
    if (other.isSetUser()) {
      this.user = other.user;
    }
    this.selectPriv = other.selectPriv;
    this.insertPriv = other.insertPriv;
    this.indexPriv = other.indexPriv;
    this.createPriv = other.createPriv;
    this.dropPriv = other.dropPriv;
    this.deletePriv = other.deletePriv;
    this.alterPriv = other.alterPriv;
    this.updatePriv = other.updatePriv;
  }

  public TblPriv deepCopy() {
    return new TblPriv(this);
  }

  @Override
  public void clear() {
    this.db = null;
    this.tbl = null;
    this.user = null;
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
  }

  public String getDb() {
    return this.db;
  }

  public void setDb(String db) {
    this.db = db;
  }

  public void unsetDb() {
    this.db = null;
  }

  public boolean isSetDb() {
    return this.db != null;
  }

  public void setDbIsSet(boolean value) {
    if (!value) {
      this.db = null;
    }
  }

  public String getTbl() {
    return this.tbl;
  }

  public void setTbl(String tbl) {
    this.tbl = tbl;
  }

  public void unsetTbl() {
    this.tbl = null;
  }

  public boolean isSetTbl() {
    return this.tbl != null;
  }

  public void setTblIsSet(boolean value) {
    if (!value) {
      this.tbl = null;
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

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case DB:
      if (value == null) {
        unsetDb();
      } else {
        setDb((String) value);
      }
      break;

    case TBL:
      if (value == null) {
        unsetTbl();
      } else {
        setTbl((String) value);
      }
      break;

    case USER:
      if (value == null) {
        unsetUser();
      } else {
        setUser((String) value);
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

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case DB:
      return getDb();

    case TBL:
      return getTbl();

    case USER:
      return getUser();

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

    }
    throw new IllegalStateException();
  }

  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case DB:
      return isSetDb();
    case TBL:
      return isSetTbl();
    case USER:
      return isSetUser();
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
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TblPriv)
      return this.equals((TblPriv) that);
    return false;
  }

  public boolean equals(TblPriv that) {
    if (that == null)
      return false;

    boolean this_present_db = true && this.isSetDb();
    boolean that_present_db = true && that.isSetDb();
    if (this_present_db || that_present_db) {
      if (!(this_present_db && that_present_db))
        return false;
      if (!this.db.equals(that.db))
        return false;
    }

    boolean this_present_tbl = true && this.isSetTbl();
    boolean that_present_tbl = true && that.isSetTbl();
    if (this_present_tbl || that_present_tbl) {
      if (!(this_present_tbl && that_present_tbl))
        return false;
      if (!this.tbl.equals(that.tbl))
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

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(TblPriv other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    TblPriv typedOther = (TblPriv) other;

    lastComparison = Boolean.valueOf(isSetDb()).compareTo(typedOther.isSetDb());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDb()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.db,
          typedOther.db);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTbl()).compareTo(
        typedOther.isSetTbl());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTbl()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.tbl,
          typedOther.tbl);
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
    StringBuilder sb = new StringBuilder("TblPriv(");
    boolean first = true;

    sb.append("db:");
    if (this.db == null) {
      sb.append("null");
    } else {
      sb.append(this.db);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("tbl:");
    if (this.tbl == null) {
      sb.append("null");
    } else {
      sb.append(this.tbl);
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

  private static class TblPrivStandardSchemeFactory implements SchemeFactory {
    public TblPrivStandardScheme getScheme() {
      return new TblPrivStandardScheme();
    }
  }

  private static class TblPrivStandardScheme extends StandardScheme<TblPriv> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TblPriv struct)
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
            struct.db = iprot.readString();
            struct.setDbIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 2:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.tbl = iprot.readString();
            struct.setTblIsSet(true);
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
          if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
            struct.selectPriv = iprot.readBool();
            struct.setSelectPrivIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 5:
          if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
            struct.insertPriv = iprot.readBool();
            struct.setInsertPrivIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 6:
          if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
            struct.indexPriv = iprot.readBool();
            struct.setIndexPrivIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 7:
          if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
            struct.createPriv = iprot.readBool();
            struct.setCreatePrivIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 8:
          if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
            struct.dropPriv = iprot.readBool();
            struct.setDropPrivIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 9:
          if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
            struct.deletePriv = iprot.readBool();
            struct.setDeletePrivIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 10:
          if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
            struct.alterPriv = iprot.readBool();
            struct.setAlterPrivIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 11:
          if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
            struct.updatePriv = iprot.readBool();
            struct.setUpdatePrivIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, TblPriv struct)
        throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.db != null) {
        oprot.writeFieldBegin(DB_FIELD_DESC);
        oprot.writeString(struct.db);
        oprot.writeFieldEnd();
      }
      if (struct.tbl != null) {
        oprot.writeFieldBegin(TBL_FIELD_DESC);
        oprot.writeString(struct.tbl);
        oprot.writeFieldEnd();
      }
      if (struct.user != null) {
        oprot.writeFieldBegin(USER_FIELD_DESC);
        oprot.writeString(struct.user);
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
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TblPrivTupleSchemeFactory implements SchemeFactory {
    public TblPrivTupleScheme getScheme() {
      return new TblPrivTupleScheme();
    }
  }

  private static class TblPrivTupleScheme extends TupleScheme<TblPriv> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TblPriv struct)
        throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetDb()) {
        optionals.set(0);
      }
      if (struct.isSetTbl()) {
        optionals.set(1);
      }
      if (struct.isSetUser()) {
        optionals.set(2);
      }
      if (struct.isSetSelectPriv()) {
        optionals.set(3);
      }
      if (struct.isSetInsertPriv()) {
        optionals.set(4);
      }
      if (struct.isSetIndexPriv()) {
        optionals.set(5);
      }
      if (struct.isSetCreatePriv()) {
        optionals.set(6);
      }
      if (struct.isSetDropPriv()) {
        optionals.set(7);
      }
      if (struct.isSetDeletePriv()) {
        optionals.set(8);
      }
      if (struct.isSetAlterPriv()) {
        optionals.set(9);
      }
      if (struct.isSetUpdatePriv()) {
        optionals.set(10);
      }
      oprot.writeBitSet(optionals, 11);
      if (struct.isSetDb()) {
        oprot.writeString(struct.db);
      }
      if (struct.isSetTbl()) {
        oprot.writeString(struct.tbl);
      }
      if (struct.isSetUser()) {
        oprot.writeString(struct.user);
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
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TblPriv struct)
        throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(11);
      if (incoming.get(0)) {
        struct.db = iprot.readString();
        struct.setDbIsSet(true);
      }
      if (incoming.get(1)) {
        struct.tbl = iprot.readString();
        struct.setTblIsSet(true);
      }
      if (incoming.get(2)) {
        struct.user = iprot.readString();
        struct.setUserIsSet(true);
      }
      if (incoming.get(3)) {
        struct.selectPriv = iprot.readBool();
        struct.setSelectPrivIsSet(true);
      }
      if (incoming.get(4)) {
        struct.insertPriv = iprot.readBool();
        struct.setInsertPrivIsSet(true);
      }
      if (incoming.get(5)) {
        struct.indexPriv = iprot.readBool();
        struct.setIndexPrivIsSet(true);
      }
      if (incoming.get(6)) {
        struct.createPriv = iprot.readBool();
        struct.setCreatePrivIsSet(true);
      }
      if (incoming.get(7)) {
        struct.dropPriv = iprot.readBool();
        struct.setDropPrivIsSet(true);
      }
      if (incoming.get(8)) {
        struct.deletePriv = iprot.readBool();
        struct.setDeletePrivIsSet(true);
      }
      if (incoming.get(9)) {
        struct.alterPriv = iprot.readBool();
        struct.setAlterPrivIsSet(true);
      }
      if (incoming.get(10)) {
        struct.updatePriv = iprot.readBool();
        struct.setUpdatePrivIsSet(true);
      }
    }
  }

}
