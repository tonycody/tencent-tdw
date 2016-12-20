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

public class IndexItem implements
    org.apache.thrift.TBase<IndexItem, IndexItem._Fields>,
    java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct(
      "IndexItem");

  private static final org.apache.thrift.protocol.TField DB_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "db", org.apache.thrift.protocol.TType.STRING, (short) 1);
  private static final org.apache.thrift.protocol.TField TBL_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "tbl", org.apache.thrift.protocol.TType.STRING, (short) 2);
  private static final org.apache.thrift.protocol.TField NAME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "name", org.apache.thrift.protocol.TType.STRING, (short) 3);
  private static final org.apache.thrift.protocol.TField FIELD_LIST_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "fieldList", org.apache.thrift.protocol.TType.STRING, (short) 4);
  private static final org.apache.thrift.protocol.TField LOCATION_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "location", org.apache.thrift.protocol.TType.STRING, (short) 5);
  private static final org.apache.thrift.protocol.TField INDEX_PATH_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "indexPath", org.apache.thrift.protocol.TType.STRING, (short) 6);
  private static final org.apache.thrift.protocol.TField PART_PATH_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "partPath", org.apache.thrift.protocol.TType.SET, (short) 7);
  private static final org.apache.thrift.protocol.TField TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "type", org.apache.thrift.protocol.TType.I32, (short) 8);
  private static final org.apache.thrift.protocol.TField STATUS_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "status", org.apache.thrift.protocol.TType.I32, (short) 9);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new IndexItemStandardSchemeFactory());
    schemes.put(TupleScheme.class, new IndexItemTupleSchemeFactory());
  }

  private String db;
  private String tbl;
  private String name;
  private String fieldList;
  private String location;
  private String indexPath;
  private Set<String> partPath;
  private int type;
  private int status;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    DB((short) 1, "db"), TBL((short) 2, "tbl"), NAME((short) 3, "name"), FIELD_LIST(
        (short) 4, "fieldList"), LOCATION((short) 5, "location"), INDEX_PATH(
        (short) 6, "indexPath"), PART_PATH((short) 7, "partPath"), TYPE(
        (short) 8, "type"), STATUS((short) 9, "status");

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
        return NAME;
      case 4:
        return FIELD_LIST;
      case 5:
        return LOCATION;
      case 6:
        return INDEX_PATH;
      case 7:
        return PART_PATH;
      case 8:
        return TYPE;
      case 9:
        return STATUS;
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

  private static final int __TYPE_ISSET_ID = 0;
  private static final int __STATUS_ISSET_ID = 1;
  private BitSet __isset_bit_vector = new BitSet(2);
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
    tmpMap.put(_Fields.NAME, new org.apache.thrift.meta_data.FieldMetaData(
        "name", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.FIELD_LIST,
        new org.apache.thrift.meta_data.FieldMetaData("fieldList",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.LOCATION, new org.apache.thrift.meta_data.FieldMetaData(
        "location", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.INDEX_PATH,
        new org.apache.thrift.meta_data.FieldMetaData("indexPath",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.PART_PATH,
        new org.apache.thrift.meta_data.FieldMetaData("partPath",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.SetMetaData(
                org.apache.thrift.protocol.TType.SET,
                new org.apache.thrift.meta_data.FieldValueMetaData(
                    org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.TYPE, new org.apache.thrift.meta_data.FieldMetaData(
        "type", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.STATUS, new org.apache.thrift.meta_data.FieldMetaData(
        "status", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.I32)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(
        IndexItem.class, metaDataMap);
  }

  public IndexItem() {
  }

  public IndexItem(String db, String tbl, String name, String fieldList,
      String location, String indexPath, Set<String> partPath, int type,
      int status) {
    this();
    this.db = db;
    this.tbl = tbl;
    this.name = name;
    this.fieldList = fieldList;
    this.location = location;
    this.indexPath = indexPath;
    this.partPath = partPath;
    this.type = type;
    setTypeIsSet(true);
    this.status = status;
    setStatusIsSet(true);
  }

  public IndexItem(IndexItem other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    if (other.isSetDb()) {
      this.db = other.db;
    }
    if (other.isSetTbl()) {
      this.tbl = other.tbl;
    }
    if (other.isSetName()) {
      this.name = other.name;
    }
    if (other.isSetFieldList()) {
      this.fieldList = other.fieldList;
    }
    if (other.isSetLocation()) {
      this.location = other.location;
    }
    if (other.isSetIndexPath()) {
      this.indexPath = other.indexPath;
    }
    if (other.isSetPartPath()) {
      Set<String> __this__partPath = new HashSet<String>();
      for (String other_element : other.partPath) {
        __this__partPath.add(other_element);
      }
      this.partPath = __this__partPath;
    }
    this.type = other.type;
    this.status = other.status;
  }

  public IndexItem deepCopy() {
    return new IndexItem(this);
  }

  @Override
  public void clear() {
    this.db = null;
    this.tbl = null;
    this.name = null;
    this.fieldList = null;
    this.location = null;
    this.indexPath = null;
    this.partPath = null;
    setTypeIsSet(false);
    this.type = 0;
    setStatusIsSet(false);
    this.status = 0;
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

  public String getFieldList() {
    return this.fieldList;
  }

  public void setFieldList(String fieldList) {
    this.fieldList = fieldList;
  }

  public void unsetFieldList() {
    this.fieldList = null;
  }

  public boolean isSetFieldList() {
    return this.fieldList != null;
  }

  public void setFieldListIsSet(boolean value) {
    if (!value) {
      this.fieldList = null;
    }
  }

  public String getLocation() {
    return this.location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public void unsetLocation() {
    this.location = null;
  }

  public boolean isSetLocation() {
    return this.location != null;
  }

  public void setLocationIsSet(boolean value) {
    if (!value) {
      this.location = null;
    }
  }

  public String getIndexPath() {
    return this.indexPath;
  }

  public void setIndexPath(String indexPath) {
    this.indexPath = indexPath;
  }

  public void unsetIndexPath() {
    this.indexPath = null;
  }

  public boolean isSetIndexPath() {
    return this.indexPath != null;
  }

  public void setIndexPathIsSet(boolean value) {
    if (!value) {
      this.indexPath = null;
    }
  }

  public int getPartPathSize() {
    return (this.partPath == null) ? 0 : this.partPath.size();
  }

  public java.util.Iterator<String> getPartPathIterator() {
    return (this.partPath == null) ? null : this.partPath.iterator();
  }

  public void addToPartPath(String elem) {
    if (this.partPath == null) {
      this.partPath = new HashSet<String>();
    }
    this.partPath.add(elem);
  }

  public Set<String> getPartPath() {
    return this.partPath;
  }

  public void setPartPath(Set<String> partPath) {
    this.partPath = partPath;
  }

  public void unsetPartPath() {
    this.partPath = null;
  }

  public boolean isSetPartPath() {
    return this.partPath != null;
  }

  public void setPartPathIsSet(boolean value) {
    if (!value) {
      this.partPath = null;
    }
  }

  public int getType() {
    return this.type;
  }

  public void setType(int type) {
    this.type = type;
    setTypeIsSet(true);
  }

  public void unsetType() {
    __isset_bit_vector.clear(__TYPE_ISSET_ID);
  }

  public boolean isSetType() {
    return __isset_bit_vector.get(__TYPE_ISSET_ID);
  }

  public void setTypeIsSet(boolean value) {
    __isset_bit_vector.set(__TYPE_ISSET_ID, value);
  }

  public int getStatus() {
    return this.status;
  }

  public void setStatus(int status) {
    this.status = status;
    setStatusIsSet(true);
  }

  public void unsetStatus() {
    __isset_bit_vector.clear(__STATUS_ISSET_ID);
  }

  public boolean isSetStatus() {
    return __isset_bit_vector.get(__STATUS_ISSET_ID);
  }

  public void setStatusIsSet(boolean value) {
    __isset_bit_vector.set(__STATUS_ISSET_ID, value);
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

    case NAME:
      if (value == null) {
        unsetName();
      } else {
        setName((String) value);
      }
      break;

    case FIELD_LIST:
      if (value == null) {
        unsetFieldList();
      } else {
        setFieldList((String) value);
      }
      break;

    case LOCATION:
      if (value == null) {
        unsetLocation();
      } else {
        setLocation((String) value);
      }
      break;

    case INDEX_PATH:
      if (value == null) {
        unsetIndexPath();
      } else {
        setIndexPath((String) value);
      }
      break;

    case PART_PATH:
      if (value == null) {
        unsetPartPath();
      } else {
        setPartPath((Set<String>) value);
      }
      break;

    case TYPE:
      if (value == null) {
        unsetType();
      } else {
        setType((Integer) value);
      }
      break;

    case STATUS:
      if (value == null) {
        unsetStatus();
      } else {
        setStatus((Integer) value);
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

    case NAME:
      return getName();

    case FIELD_LIST:
      return getFieldList();

    case LOCATION:
      return getLocation();

    case INDEX_PATH:
      return getIndexPath();

    case PART_PATH:
      return getPartPath();

    case TYPE:
      return Integer.valueOf(getType());

    case STATUS:
      return Integer.valueOf(getStatus());

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
    case NAME:
      return isSetName();
    case FIELD_LIST:
      return isSetFieldList();
    case LOCATION:
      return isSetLocation();
    case INDEX_PATH:
      return isSetIndexPath();
    case PART_PATH:
      return isSetPartPath();
    case TYPE:
      return isSetType();
    case STATUS:
      return isSetStatus();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof IndexItem)
      return this.equals((IndexItem) that);
    return false;
  }

  public boolean equals(IndexItem that) {
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

    boolean this_present_name = true && this.isSetName();
    boolean that_present_name = true && that.isSetName();
    if (this_present_name || that_present_name) {
      if (!(this_present_name && that_present_name))
        return false;
      if (!this.name.equals(that.name))
        return false;
    }

    boolean this_present_fieldList = true && this.isSetFieldList();
    boolean that_present_fieldList = true && that.isSetFieldList();
    if (this_present_fieldList || that_present_fieldList) {
      if (!(this_present_fieldList && that_present_fieldList))
        return false;
      if (!this.fieldList.equals(that.fieldList))
        return false;
    }

    boolean this_present_location = true && this.isSetLocation();
    boolean that_present_location = true && that.isSetLocation();
    if (this_present_location || that_present_location) {
      if (!(this_present_location && that_present_location))
        return false;
      if (!this.location.equals(that.location))
        return false;
    }

    boolean this_present_indexPath = true && this.isSetIndexPath();
    boolean that_present_indexPath = true && that.isSetIndexPath();
    if (this_present_indexPath || that_present_indexPath) {
      if (!(this_present_indexPath && that_present_indexPath))
        return false;
      if (!this.indexPath.equals(that.indexPath))
        return false;
    }

    boolean this_present_partPath = true && this.isSetPartPath();
    boolean that_present_partPath = true && that.isSetPartPath();
    if (this_present_partPath || that_present_partPath) {
      if (!(this_present_partPath && that_present_partPath))
        return false;
      if (!this.partPath.equals(that.partPath))
        return false;
    }

    boolean this_present_type = true;
    boolean that_present_type = true;
    if (this_present_type || that_present_type) {
      if (!(this_present_type && that_present_type))
        return false;
      if (this.type != that.type)
        return false;
    }

    boolean this_present_status = true;
    boolean that_present_status = true;
    if (this_present_status || that_present_status) {
      if (!(this_present_status && that_present_status))
        return false;
      if (this.status != that.status)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(IndexItem other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    IndexItem typedOther = (IndexItem) other;

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
    lastComparison = Boolean.valueOf(isSetFieldList()).compareTo(
        typedOther.isSetFieldList());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetFieldList()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.fieldList,
          typedOther.fieldList);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetLocation()).compareTo(
        typedOther.isSetLocation());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLocation()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.location,
          typedOther.location);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetIndexPath()).compareTo(
        typedOther.isSetIndexPath());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetIndexPath()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.indexPath,
          typedOther.indexPath);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetPartPath()).compareTo(
        typedOther.isSetPartPath());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPartPath()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.partPath,
          typedOther.partPath);
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
    lastComparison = Boolean.valueOf(isSetStatus()).compareTo(
        typedOther.isSetStatus());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStatus()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.status,
          typedOther.status);
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
    StringBuilder sb = new StringBuilder("IndexItem(");
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
    sb.append("name:");
    if (this.name == null) {
      sb.append("null");
    } else {
      sb.append(this.name);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("fieldList:");
    if (this.fieldList == null) {
      sb.append("null");
    } else {
      sb.append(this.fieldList);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("location:");
    if (this.location == null) {
      sb.append("null");
    } else {
      sb.append(this.location);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("indexPath:");
    if (this.indexPath == null) {
      sb.append("null");
    } else {
      sb.append(this.indexPath);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("partPath:");
    if (this.partPath == null) {
      sb.append("null");
    } else {
      sb.append(this.partPath);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("type:");
    sb.append(this.type);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("status:");
    sb.append(this.status);
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

  private static class IndexItemStandardSchemeFactory implements SchemeFactory {
    public IndexItemStandardScheme getScheme() {
      return new IndexItemStandardScheme();
    }
  }

  private static class IndexItemStandardScheme extends
      StandardScheme<IndexItem> {

    public void read(org.apache.thrift.protocol.TProtocol iprot,
        IndexItem struct) throws org.apache.thrift.TException {
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
            struct.name = iprot.readString();
            struct.setNameIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 4:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.fieldList = iprot.readString();
            struct.setFieldListIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 5:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.location = iprot.readString();
            struct.setLocationIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 6:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.indexPath = iprot.readString();
            struct.setIndexPathIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 7:
          if (schemeField.type == org.apache.thrift.protocol.TType.SET) {
            {
              org.apache.thrift.protocol.TSet _set122 = iprot.readSetBegin();
              struct.partPath = new HashSet<String>(2 * _set122.size);
              for (int _i123 = 0; _i123 < _set122.size; ++_i123) {
                String _elem124;
                _elem124 = iprot.readString();
                struct.partPath.add(_elem124);
              }
              iprot.readSetEnd();
            }
            struct.setPartPathIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 8:
          if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
            struct.type = iprot.readI32();
            struct.setTypeIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 9:
          if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
            struct.status = iprot.readI32();
            struct.setStatusIsSet(true);
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
        IndexItem struct) throws org.apache.thrift.TException {
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
      if (struct.name != null) {
        oprot.writeFieldBegin(NAME_FIELD_DESC);
        oprot.writeString(struct.name);
        oprot.writeFieldEnd();
      }
      if (struct.fieldList != null) {
        oprot.writeFieldBegin(FIELD_LIST_FIELD_DESC);
        oprot.writeString(struct.fieldList);
        oprot.writeFieldEnd();
      }
      if (struct.location != null) {
        oprot.writeFieldBegin(LOCATION_FIELD_DESC);
        oprot.writeString(struct.location);
        oprot.writeFieldEnd();
      }
      if (struct.indexPath != null) {
        oprot.writeFieldBegin(INDEX_PATH_FIELD_DESC);
        oprot.writeString(struct.indexPath);
        oprot.writeFieldEnd();
      }
      if (struct.partPath != null) {
        oprot.writeFieldBegin(PART_PATH_FIELD_DESC);
        {
          oprot.writeSetBegin(new org.apache.thrift.protocol.TSet(
              org.apache.thrift.protocol.TType.STRING, struct.partPath.size()));
          for (String _iter125 : struct.partPath) {
            oprot.writeString(_iter125);
          }
          oprot.writeSetEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(TYPE_FIELD_DESC);
      oprot.writeI32(struct.type);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(STATUS_FIELD_DESC);
      oprot.writeI32(struct.status);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class IndexItemTupleSchemeFactory implements SchemeFactory {
    public IndexItemTupleScheme getScheme() {
      return new IndexItemTupleScheme();
    }
  }

  private static class IndexItemTupleScheme extends TupleScheme<IndexItem> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot,
        IndexItem struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetDb()) {
        optionals.set(0);
      }
      if (struct.isSetTbl()) {
        optionals.set(1);
      }
      if (struct.isSetName()) {
        optionals.set(2);
      }
      if (struct.isSetFieldList()) {
        optionals.set(3);
      }
      if (struct.isSetLocation()) {
        optionals.set(4);
      }
      if (struct.isSetIndexPath()) {
        optionals.set(5);
      }
      if (struct.isSetPartPath()) {
        optionals.set(6);
      }
      if (struct.isSetType()) {
        optionals.set(7);
      }
      if (struct.isSetStatus()) {
        optionals.set(8);
      }
      oprot.writeBitSet(optionals, 9);
      if (struct.isSetDb()) {
        oprot.writeString(struct.db);
      }
      if (struct.isSetTbl()) {
        oprot.writeString(struct.tbl);
      }
      if (struct.isSetName()) {
        oprot.writeString(struct.name);
      }
      if (struct.isSetFieldList()) {
        oprot.writeString(struct.fieldList);
      }
      if (struct.isSetLocation()) {
        oprot.writeString(struct.location);
      }
      if (struct.isSetIndexPath()) {
        oprot.writeString(struct.indexPath);
      }
      if (struct.isSetPartPath()) {
        {
          oprot.writeI32(struct.partPath.size());
          for (String _iter126 : struct.partPath) {
            oprot.writeString(_iter126);
          }
        }
      }
      if (struct.isSetType()) {
        oprot.writeI32(struct.type);
      }
      if (struct.isSetStatus()) {
        oprot.writeI32(struct.status);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, IndexItem struct)
        throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(9);
      if (incoming.get(0)) {
        struct.db = iprot.readString();
        struct.setDbIsSet(true);
      }
      if (incoming.get(1)) {
        struct.tbl = iprot.readString();
        struct.setTblIsSet(true);
      }
      if (incoming.get(2)) {
        struct.name = iprot.readString();
        struct.setNameIsSet(true);
      }
      if (incoming.get(3)) {
        struct.fieldList = iprot.readString();
        struct.setFieldListIsSet(true);
      }
      if (incoming.get(4)) {
        struct.location = iprot.readString();
        struct.setLocationIsSet(true);
      }
      if (incoming.get(5)) {
        struct.indexPath = iprot.readString();
        struct.setIndexPathIsSet(true);
      }
      if (incoming.get(6)) {
        {
          org.apache.thrift.protocol.TSet _set127 = new org.apache.thrift.protocol.TSet(
              org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.partPath = new HashSet<String>(2 * _set127.size);
          for (int _i128 = 0; _i128 < _set127.size; ++_i128) {
            String _elem129;
            _elem129 = iprot.readString();
            struct.partPath.add(_elem129);
          }
        }
        struct.setPartPathIsSet(true);
      }
      if (incoming.get(7)) {
        struct.type = iprot.readI32();
        struct.setTypeIsSet(true);
      }
      if (incoming.get(8)) {
        struct.status = iprot.readI32();
        struct.setStatusIsSet(true);
      }
    }
  }

}
