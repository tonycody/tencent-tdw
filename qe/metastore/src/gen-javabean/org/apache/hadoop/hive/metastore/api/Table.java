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

public class Table implements org.apache.thrift.TBase<Table, Table._Fields>,
    java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct(
      "Table");

  private static final org.apache.thrift.protocol.TField TABLE_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "tableName", org.apache.thrift.protocol.TType.STRING, (short) 1);
  private static final org.apache.thrift.protocol.TField DB_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "dbName", org.apache.thrift.protocol.TType.STRING, (short) 2);
  private static final org.apache.thrift.protocol.TField OWNER_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "owner", org.apache.thrift.protocol.TType.STRING, (short) 3);
  private static final org.apache.thrift.protocol.TField CREATE_TIME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "createTime", org.apache.thrift.protocol.TType.I32, (short) 4);
  private static final org.apache.thrift.protocol.TField LAST_ACCESS_TIME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "lastAccessTime", org.apache.thrift.protocol.TType.I32, (short) 5);
  private static final org.apache.thrift.protocol.TField RETENTION_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "retention", org.apache.thrift.protocol.TType.I32, (short) 6);
  private static final org.apache.thrift.protocol.TField SD_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "sd", org.apache.thrift.protocol.TType.STRUCT, (short) 7);
  private static final org.apache.thrift.protocol.TField PRI_PARTITION_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "priPartition", org.apache.thrift.protocol.TType.STRUCT, (short) 8);
  private static final org.apache.thrift.protocol.TField SUB_PARTITION_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "subPartition", org.apache.thrift.protocol.TType.STRUCT, (short) 9);
  private static final org.apache.thrift.protocol.TField PARAMETERS_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "parameters", org.apache.thrift.protocol.TType.MAP, (short) 10);
  private static final org.apache.thrift.protocol.TField VIEW_ORIGINAL_TEXT_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "viewOriginalText", org.apache.thrift.protocol.TType.STRING, (short) 11);
  private static final org.apache.thrift.protocol.TField VIEW_EXPANDED_TEXT_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "viewExpandedText", org.apache.thrift.protocol.TType.STRING, (short) 12);
  private static final org.apache.thrift.protocol.TField TABLE_TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "tableType", org.apache.thrift.protocol.TType.STRING, (short) 13);
  private static final org.apache.thrift.protocol.TField VTABLES_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "vtables", org.apache.thrift.protocol.TType.STRING, (short) 14);
  private static final org.apache.thrift.protocol.TField IS_REPLACE_ON_EXIT_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "isReplaceOnExit", org.apache.thrift.protocol.TType.BOOL, (short) 15);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TableStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TableTupleSchemeFactory());
  }

  private String tableName;
  private String dbName;
  private String owner;
  private int createTime;
  private int lastAccessTime;
  private int retention;
  private StorageDescriptor sd;
  private Partition priPartition;
  private Partition subPartition;
  private Map<String, String> parameters;
  private String viewOriginalText;
  private String viewExpandedText;
  private String tableType;
  private String vtables;
  private boolean isReplaceOnExit;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    TABLE_NAME((short) 1, "tableName"), DB_NAME((short) 2, "dbName"), OWNER(
        (short) 3, "owner"), CREATE_TIME((short) 4, "createTime"), LAST_ACCESS_TIME(
        (short) 5, "lastAccessTime"), RETENTION((short) 6, "retention"), SD(
        (short) 7, "sd"), PRI_PARTITION((short) 8, "priPartition"), SUB_PARTITION(
        (short) 9, "subPartition"), PARAMETERS((short) 10, "parameters"), VIEW_ORIGINAL_TEXT(
        (short) 11, "viewOriginalText"), VIEW_EXPANDED_TEXT((short) 12,
        "viewExpandedText"), TABLE_TYPE((short) 13, "tableType"), VTABLES(
        (short) 14, "vtables"), IS_REPLACE_ON_EXIT((short) 15,
        "isReplaceOnExit");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    public static _Fields findByThriftId(int fieldId) {
      switch (fieldId) {
      case 1:
        return TABLE_NAME;
      case 2:
        return DB_NAME;
      case 3:
        return OWNER;
      case 4:
        return CREATE_TIME;
      case 5:
        return LAST_ACCESS_TIME;
      case 6:
        return RETENTION;
      case 7:
        return SD;
      case 8:
        return PRI_PARTITION;
      case 9:
        return SUB_PARTITION;
      case 10:
        return PARAMETERS;
      case 11:
        return VIEW_ORIGINAL_TEXT;
      case 12:
        return VIEW_EXPANDED_TEXT;
      case 13:
        return TABLE_TYPE;
      case 14:
        return VTABLES;
      case 15:
        return IS_REPLACE_ON_EXIT;
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

  private static final int __CREATETIME_ISSET_ID = 0;
  private static final int __LASTACCESSTIME_ISSET_ID = 1;
  private static final int __RETENTION_ISSET_ID = 2;
  private static final int __ISREPLACEONEXIT_ISSET_ID = 3;
  private BitSet __isset_bit_vector = new BitSet(4);
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(
        _Fields.class);
    tmpMap.put(_Fields.TABLE_NAME,
        new org.apache.thrift.meta_data.FieldMetaData("tableName",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.DB_NAME, new org.apache.thrift.meta_data.FieldMetaData(
        "dbName", org.apache.thrift.TFieldRequirementType.DEFAULT,
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
                org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.LAST_ACCESS_TIME,
        new org.apache.thrift.meta_data.FieldMetaData("lastAccessTime",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.RETENTION,
        new org.apache.thrift.meta_data.FieldMetaData("retention",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.SD, new org.apache.thrift.meta_data.FieldMetaData("sd",
        org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.StructMetaData(
            org.apache.thrift.protocol.TType.STRUCT, StorageDescriptor.class)));
    tmpMap.put(_Fields.PRI_PARTITION,
        new org.apache.thrift.meta_data.FieldMetaData("priPartition",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.StructMetaData(
                org.apache.thrift.protocol.TType.STRUCT, Partition.class)));
    tmpMap.put(_Fields.SUB_PARTITION,
        new org.apache.thrift.meta_data.FieldMetaData("subPartition",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.StructMetaData(
                org.apache.thrift.protocol.TType.STRUCT, Partition.class)));
    tmpMap.put(_Fields.PARAMETERS,
        new org.apache.thrift.meta_data.FieldMetaData("parameters",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.MapMetaData(
                org.apache.thrift.protocol.TType.MAP,
                new org.apache.thrift.meta_data.FieldValueMetaData(
                    org.apache.thrift.protocol.TType.STRING),
                new org.apache.thrift.meta_data.FieldValueMetaData(
                    org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.VIEW_ORIGINAL_TEXT,
        new org.apache.thrift.meta_data.FieldMetaData("viewOriginalText",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.VIEW_EXPANDED_TEXT,
        new org.apache.thrift.meta_data.FieldMetaData("viewExpandedText",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.TABLE_TYPE,
        new org.apache.thrift.meta_data.FieldMetaData("tableType",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.VTABLES, new org.apache.thrift.meta_data.FieldMetaData(
        "vtables", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.IS_REPLACE_ON_EXIT,
        new org.apache.thrift.meta_data.FieldMetaData("isReplaceOnExit",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.BOOL)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(Table.class,
        metaDataMap);
  }

  public Table() {
  }

  public Table(String tableName, String dbName, String owner, int createTime,
      int lastAccessTime, int retention, StorageDescriptor sd,
      Partition priPartition, Partition subPartition,
      Map<String, String> parameters, String viewOriginalText,
      String viewExpandedText, String tableType, String vtables,
      boolean isReplaceOnExit) {
    this();
    this.tableName = tableName;
    this.dbName = dbName;
    this.owner = owner;
    this.createTime = createTime;
    setCreateTimeIsSet(true);
    this.lastAccessTime = lastAccessTime;
    setLastAccessTimeIsSet(true);
    this.retention = retention;
    setRetentionIsSet(true);
    this.sd = sd;
    this.priPartition = priPartition;
    this.subPartition = subPartition;
    this.parameters = parameters;
    this.viewOriginalText = viewOriginalText;
    this.viewExpandedText = viewExpandedText;
    this.tableType = tableType;
    this.vtables = vtables;
    this.isReplaceOnExit = isReplaceOnExit;
    setIsReplaceOnExitIsSet(true);
  }

  public Table(Table other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    if (other.isSetTableName()) {
      this.tableName = other.tableName;
    }
    if (other.isSetDbName()) {
      this.dbName = other.dbName;
    }
    if (other.isSetOwner()) {
      this.owner = other.owner;
    }
    this.createTime = other.createTime;
    this.lastAccessTime = other.lastAccessTime;
    this.retention = other.retention;
    if (other.isSetSd()) {
      this.sd = new StorageDescriptor(other.sd);
    }
    if (other.isSetPriPartition()) {
      this.priPartition = new Partition(other.priPartition);
    }
    if (other.isSetSubPartition()) {
      this.subPartition = new Partition(other.subPartition);
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
    if (other.isSetViewOriginalText()) {
      this.viewOriginalText = other.viewOriginalText;
    }
    if (other.isSetViewExpandedText()) {
      this.viewExpandedText = other.viewExpandedText;
    }
    if (other.isSetTableType()) {
      this.tableType = other.tableType;
    }
    if (other.isSetVtables()) {
      this.vtables = other.vtables;
    }
    this.isReplaceOnExit = other.isReplaceOnExit;
  }

  public Table deepCopy() {
    return new Table(this);
  }

  @Override
  public void clear() {
    this.tableName = null;
    this.dbName = null;
    this.owner = null;
    setCreateTimeIsSet(false);
    this.createTime = 0;
    setLastAccessTimeIsSet(false);
    this.lastAccessTime = 0;
    setRetentionIsSet(false);
    this.retention = 0;
    this.sd = null;
    this.priPartition = null;
    this.subPartition = null;
    this.parameters = null;
    this.viewOriginalText = null;
    this.viewExpandedText = null;
    this.tableType = null;
    this.vtables = null;
    setIsReplaceOnExitIsSet(false);
    this.isReplaceOnExit = false;
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

  public int getCreateTime() {
    return this.createTime;
  }

  public void setCreateTime(int createTime) {
    this.createTime = createTime;
    setCreateTimeIsSet(true);
  }

  public void unsetCreateTime() {
    __isset_bit_vector.clear(__CREATETIME_ISSET_ID);
  }

  public boolean isSetCreateTime() {
    return __isset_bit_vector.get(__CREATETIME_ISSET_ID);
  }

  public void setCreateTimeIsSet(boolean value) {
    __isset_bit_vector.set(__CREATETIME_ISSET_ID, value);
  }

  public int getLastAccessTime() {
    return this.lastAccessTime;
  }

  public void setLastAccessTime(int lastAccessTime) {
    this.lastAccessTime = lastAccessTime;
    setLastAccessTimeIsSet(true);
  }

  public void unsetLastAccessTime() {
    __isset_bit_vector.clear(__LASTACCESSTIME_ISSET_ID);
  }

  public boolean isSetLastAccessTime() {
    return __isset_bit_vector.get(__LASTACCESSTIME_ISSET_ID);
  }

  public void setLastAccessTimeIsSet(boolean value) {
    __isset_bit_vector.set(__LASTACCESSTIME_ISSET_ID, value);
  }

  public int getRetention() {
    return this.retention;
  }

  public void setRetention(int retention) {
    this.retention = retention;
    setRetentionIsSet(true);
  }

  public void unsetRetention() {
    __isset_bit_vector.clear(__RETENTION_ISSET_ID);
  }

  public boolean isSetRetention() {
    return __isset_bit_vector.get(__RETENTION_ISSET_ID);
  }

  public void setRetentionIsSet(boolean value) {
    __isset_bit_vector.set(__RETENTION_ISSET_ID, value);
  }

  public StorageDescriptor getSd() {
    return this.sd;
  }

  public void setSd(StorageDescriptor sd) {
    this.sd = sd;
  }

  public void unsetSd() {
    this.sd = null;
  }

  public boolean isSetSd() {
    return this.sd != null;
  }

  public void setSdIsSet(boolean value) {
    if (!value) {
      this.sd = null;
    }
  }

  public Partition getPriPartition() {
    return this.priPartition;
  }

  public void setPriPartition(Partition priPartition) {
    this.priPartition = priPartition;
  }

  public void unsetPriPartition() {
    this.priPartition = null;
  }

  public boolean isSetPriPartition() {
    return this.priPartition != null;
  }

  public void setPriPartitionIsSet(boolean value) {
    if (!value) {
      this.priPartition = null;
    }
  }

  public Partition getSubPartition() {
    return this.subPartition;
  }

  public void setSubPartition(Partition subPartition) {
    this.subPartition = subPartition;
  }

  public void unsetSubPartition() {
    this.subPartition = null;
  }

  public boolean isSetSubPartition() {
    return this.subPartition != null;
  }

  public void setSubPartitionIsSet(boolean value) {
    if (!value) {
      this.subPartition = null;
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

  public String getViewOriginalText() {
    return this.viewOriginalText;
  }

  public void setViewOriginalText(String viewOriginalText) {
    this.viewOriginalText = viewOriginalText;
  }

  public void unsetViewOriginalText() {
    this.viewOriginalText = null;
  }

  public boolean isSetViewOriginalText() {
    return this.viewOriginalText != null;
  }

  public void setViewOriginalTextIsSet(boolean value) {
    if (!value) {
      this.viewOriginalText = null;
    }
  }

  public String getViewExpandedText() {
    return this.viewExpandedText;
  }

  public void setViewExpandedText(String viewExpandedText) {
    this.viewExpandedText = viewExpandedText;
  }

  public void unsetViewExpandedText() {
    this.viewExpandedText = null;
  }

  public boolean isSetViewExpandedText() {
    return this.viewExpandedText != null;
  }

  public void setViewExpandedTextIsSet(boolean value) {
    if (!value) {
      this.viewExpandedText = null;
    }
  }

  public String getTableType() {
    return this.tableType;
  }

  public void setTableType(String tableType) {
    this.tableType = tableType;
  }

  public void unsetTableType() {
    this.tableType = null;
  }

  public boolean isSetTableType() {
    return this.tableType != null;
  }

  public void setTableTypeIsSet(boolean value) {
    if (!value) {
      this.tableType = null;
    }
  }

  public String getVtables() {
    return this.vtables;
  }

  public void setVtables(String vtables) {
    this.vtables = vtables;
  }

  public void unsetVtables() {
    this.vtables = null;
  }

  public boolean isSetVtables() {
    return this.vtables != null;
  }

  public void setVtablesIsSet(boolean value) {
    if (!value) {
      this.vtables = null;
    }
  }

  public boolean isIsReplaceOnExit() {
    return this.isReplaceOnExit;
  }

  public void setIsReplaceOnExit(boolean isReplaceOnExit) {
    this.isReplaceOnExit = isReplaceOnExit;
    setIsReplaceOnExitIsSet(true);
  }

  public void unsetIsReplaceOnExit() {
    __isset_bit_vector.clear(__ISREPLACEONEXIT_ISSET_ID);
  }

  public boolean isSetIsReplaceOnExit() {
    return __isset_bit_vector.get(__ISREPLACEONEXIT_ISSET_ID);
  }

  public void setIsReplaceOnExitIsSet(boolean value) {
    __isset_bit_vector.set(__ISREPLACEONEXIT_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case TABLE_NAME:
      if (value == null) {
        unsetTableName();
      } else {
        setTableName((String) value);
      }
      break;

    case DB_NAME:
      if (value == null) {
        unsetDbName();
      } else {
        setDbName((String) value);
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
        setCreateTime((Integer) value);
      }
      break;

    case LAST_ACCESS_TIME:
      if (value == null) {
        unsetLastAccessTime();
      } else {
        setLastAccessTime((Integer) value);
      }
      break;

    case RETENTION:
      if (value == null) {
        unsetRetention();
      } else {
        setRetention((Integer) value);
      }
      break;

    case SD:
      if (value == null) {
        unsetSd();
      } else {
        setSd((StorageDescriptor) value);
      }
      break;

    case PRI_PARTITION:
      if (value == null) {
        unsetPriPartition();
      } else {
        setPriPartition((Partition) value);
      }
      break;

    case SUB_PARTITION:
      if (value == null) {
        unsetSubPartition();
      } else {
        setSubPartition((Partition) value);
      }
      break;

    case PARAMETERS:
      if (value == null) {
        unsetParameters();
      } else {
        setParameters((Map<String, String>) value);
      }
      break;

    case VIEW_ORIGINAL_TEXT:
      if (value == null) {
        unsetViewOriginalText();
      } else {
        setViewOriginalText((String) value);
      }
      break;

    case VIEW_EXPANDED_TEXT:
      if (value == null) {
        unsetViewExpandedText();
      } else {
        setViewExpandedText((String) value);
      }
      break;

    case TABLE_TYPE:
      if (value == null) {
        unsetTableType();
      } else {
        setTableType((String) value);
      }
      break;

    case VTABLES:
      if (value == null) {
        unsetVtables();
      } else {
        setVtables((String) value);
      }
      break;

    case IS_REPLACE_ON_EXIT:
      if (value == null) {
        unsetIsReplaceOnExit();
      } else {
        setIsReplaceOnExit((Boolean) value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case TABLE_NAME:
      return getTableName();

    case DB_NAME:
      return getDbName();

    case OWNER:
      return getOwner();

    case CREATE_TIME:
      return Integer.valueOf(getCreateTime());

    case LAST_ACCESS_TIME:
      return Integer.valueOf(getLastAccessTime());

    case RETENTION:
      return Integer.valueOf(getRetention());

    case SD:
      return getSd();

    case PRI_PARTITION:
      return getPriPartition();

    case SUB_PARTITION:
      return getSubPartition();

    case PARAMETERS:
      return getParameters();

    case VIEW_ORIGINAL_TEXT:
      return getViewOriginalText();

    case VIEW_EXPANDED_TEXT:
      return getViewExpandedText();

    case TABLE_TYPE:
      return getTableType();

    case VTABLES:
      return getVtables();

    case IS_REPLACE_ON_EXIT:
      return Boolean.valueOf(isIsReplaceOnExit());

    }
    throw new IllegalStateException();
  }

  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case TABLE_NAME:
      return isSetTableName();
    case DB_NAME:
      return isSetDbName();
    case OWNER:
      return isSetOwner();
    case CREATE_TIME:
      return isSetCreateTime();
    case LAST_ACCESS_TIME:
      return isSetLastAccessTime();
    case RETENTION:
      return isSetRetention();
    case SD:
      return isSetSd();
    case PRI_PARTITION:
      return isSetPriPartition();
    case SUB_PARTITION:
      return isSetSubPartition();
    case PARAMETERS:
      return isSetParameters();
    case VIEW_ORIGINAL_TEXT:
      return isSetViewOriginalText();
    case VIEW_EXPANDED_TEXT:
      return isSetViewExpandedText();
    case TABLE_TYPE:
      return isSetTableType();
    case VTABLES:
      return isSetVtables();
    case IS_REPLACE_ON_EXIT:
      return isSetIsReplaceOnExit();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof Table)
      return this.equals((Table) that);
    return false;
  }

  public boolean equals(Table that) {
    if (that == null)
      return false;

    boolean this_present_tableName = true && this.isSetTableName();
    boolean that_present_tableName = true && that.isSetTableName();
    if (this_present_tableName || that_present_tableName) {
      if (!(this_present_tableName && that_present_tableName))
        return false;
      if (!this.tableName.equals(that.tableName))
        return false;
    }

    boolean this_present_dbName = true && this.isSetDbName();
    boolean that_present_dbName = true && that.isSetDbName();
    if (this_present_dbName || that_present_dbName) {
      if (!(this_present_dbName && that_present_dbName))
        return false;
      if (!this.dbName.equals(that.dbName))
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

    boolean this_present_createTime = true;

    boolean this_present_retention = true;
    boolean that_present_retention = true;
    if (this_present_retention || that_present_retention) {
      if (!(this_present_retention && that_present_retention))
        return false;
      if (this.retention != that.retention)
        return false;
    }

    boolean this_present_sd = true && this.isSetSd();
    boolean that_present_sd = true && that.isSetSd();
    if (this_present_sd || that_present_sd) {
      if (!(this_present_sd && that_present_sd))
        return false;
      if (!this.sd.equals(that.sd))
        return false;
    }

    boolean this_present_priPartition = true && this.isSetPriPartition();
    boolean that_present_priPartition = true && that.isSetPriPartition();
    if (this_present_priPartition || that_present_priPartition) {
      if (!(this_present_priPartition && that_present_priPartition))
        return false;
      if (!this.priPartition.equals(that.priPartition))
        return false;
    }

    boolean this_present_subPartition = true && this.isSetSubPartition();
    boolean that_present_subPartition = true && that.isSetSubPartition();
    if (this_present_subPartition || that_present_subPartition) {
      if (!(this_present_subPartition && that_present_subPartition))
        return false;
      if (!this.subPartition.equals(that.subPartition))
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

    boolean this_present_viewOriginalText = true && this
        .isSetViewOriginalText();
    boolean that_present_viewOriginalText = true && that
        .isSetViewOriginalText();
    if (this_present_viewOriginalText || that_present_viewOriginalText) {
      if (!(this_present_viewOriginalText && that_present_viewOriginalText))
        return false;
      if (!this.viewOriginalText.equals(that.viewOriginalText))
        return false;
    }

    boolean this_present_viewExpandedText = true && this
        .isSetViewExpandedText();
    boolean that_present_viewExpandedText = true && that
        .isSetViewExpandedText();
    if (this_present_viewExpandedText || that_present_viewExpandedText) {
      if (!(this_present_viewExpandedText && that_present_viewExpandedText))
        return false;
      if (!this.viewExpandedText.equals(that.viewExpandedText))
        return false;
    }

    boolean this_present_tableType = true && this.isSetTableType();
    boolean that_present_tableType = true && that.isSetTableType();
    if (this_present_tableType || that_present_tableType) {
      if (!(this_present_tableType && that_present_tableType))
        return false;
      if (!this.tableType.equals(that.tableType))
        return false;
    }

    boolean this_present_vtables = true && this.isSetVtables();
    boolean that_present_vtables = true && that.isSetVtables();
    if (this_present_vtables || that_present_vtables) {
      if (!(this_present_vtables && that_present_vtables))
        return false;
      if (!this.vtables.equals(that.vtables))
        return false;
    }

    boolean this_present_isReplaceOnExit = true;
    boolean that_present_isReplaceOnExit = true;
    if (this_present_isReplaceOnExit || that_present_isReplaceOnExit) {
      if (!(this_present_isReplaceOnExit && that_present_isReplaceOnExit))
        return false;
      if (this.isReplaceOnExit != that.isReplaceOnExit)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(Table other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    Table typedOther = (Table) other;

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
    lastComparison = Boolean.valueOf(isSetLastAccessTime()).compareTo(
        typedOther.isSetLastAccessTime());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLastAccessTime()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.lastAccessTime, typedOther.lastAccessTime);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetRetention()).compareTo(
        typedOther.isSetRetention());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetRetention()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.retention,
          typedOther.retention);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetSd()).compareTo(typedOther.isSetSd());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSd()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.sd,
          typedOther.sd);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetPriPartition()).compareTo(
        typedOther.isSetPriPartition());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPriPartition()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.priPartition, typedOther.priPartition);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetSubPartition()).compareTo(
        typedOther.isSetSubPartition());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSubPartition()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.subPartition, typedOther.subPartition);
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
    lastComparison = Boolean.valueOf(isSetViewOriginalText()).compareTo(
        typedOther.isSetViewOriginalText());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetViewOriginalText()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.viewOriginalText, typedOther.viewOriginalText);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetViewExpandedText()).compareTo(
        typedOther.isSetViewExpandedText());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetViewExpandedText()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.viewExpandedText, typedOther.viewExpandedText);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTableType()).compareTo(
        typedOther.isSetTableType());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTableType()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.tableType,
          typedOther.tableType);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetVtables()).compareTo(
        typedOther.isSetVtables());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetVtables()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.vtables,
          typedOther.vtables);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetIsReplaceOnExit()).compareTo(
        typedOther.isSetIsReplaceOnExit());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetIsReplaceOnExit()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.isReplaceOnExit, typedOther.isReplaceOnExit);
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
    StringBuilder sb = new StringBuilder("Table(");
    boolean first = true;

    sb.append("tableName:");
    if (this.tableName == null) {
      sb.append("null");
    } else {
      sb.append(this.tableName);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("dbName:");
    if (this.dbName == null) {
      sb.append("null");
    } else {
      sb.append(this.dbName);
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
    sb.append(this.createTime);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("lastAccessTime:");
    sb.append(this.lastAccessTime);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("retention:");
    sb.append(this.retention);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("sd:");
    if (this.sd == null) {
      sb.append("null");
    } else {
      sb.append(this.sd);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("priPartition:");
    if (this.priPartition == null) {
      sb.append("null");
    } else {
      sb.append(this.priPartition);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("subPartition:");
    if (this.subPartition == null) {
      sb.append("null");
    } else {
      sb.append(this.subPartition);
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
    if (!first)
      sb.append(", ");
    sb.append("viewOriginalText:");
    if (this.viewOriginalText == null) {
      sb.append("null");
    } else {
      sb.append(this.viewOriginalText);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("viewExpandedText:");
    if (this.viewExpandedText == null) {
      sb.append("null");
    } else {
      sb.append(this.viewExpandedText);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("tableType:");
    if (this.tableType == null) {
      sb.append("null");
    } else {
      sb.append(this.tableType);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("vtables:");
    if (this.vtables == null) {
      sb.append("null");
    } else {
      sb.append(this.vtables);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("isReplaceOnExit:");
    sb.append(this.isReplaceOnExit);
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

  private static class TableStandardSchemeFactory implements SchemeFactory {
    public TableStandardScheme getScheme() {
      return new TableStandardScheme();
    }
  }

  private static class TableStandardScheme extends StandardScheme<Table> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, Table struct)
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
            struct.tableName = iprot.readString();
            struct.setTableNameIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 2:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.dbName = iprot.readString();
            struct.setDbNameIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 3:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.owner = iprot.readString();
            struct.setOwnerIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 4:
          if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
            struct.createTime = iprot.readI32();
            struct.setCreateTimeIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 5:
          if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
            struct.lastAccessTime = iprot.readI32();
            struct.setLastAccessTimeIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 6:
          if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
            struct.retention = iprot.readI32();
            struct.setRetentionIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 7:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
            struct.sd = new StorageDescriptor();
            struct.sd.read(iprot);
            struct.setSdIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 8:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
            struct.priPartition = new Partition();
            struct.priPartition.read(iprot);
            struct.setPriPartitionIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 9:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
            struct.subPartition = new Partition();
            struct.subPartition.read(iprot);
            struct.setSubPartitionIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 10:
          if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
            {
              org.apache.thrift.protocol.TMap _map70 = iprot.readMapBegin();
              struct.parameters = new HashMap<String, String>(2 * _map70.size);
              for (int _i71 = 0; _i71 < _map70.size; ++_i71) {
                String _key72;
                String _val73;
                _key72 = iprot.readString();
                _val73 = iprot.readString();
                struct.parameters.put(_key72, _val73);
              }
              iprot.readMapEnd();
            }
            struct.setParametersIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 11:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.viewOriginalText = iprot.readString();
            struct.setViewOriginalTextIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 12:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.viewExpandedText = iprot.readString();
            struct.setViewExpandedTextIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 13:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.tableType = iprot.readString();
            struct.setTableTypeIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 14:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.vtables = iprot.readString();
            struct.setVtablesIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 15:
          if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
            struct.isReplaceOnExit = iprot.readBool();
            struct.setIsReplaceOnExitIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, Table struct)
        throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.tableName != null) {
        oprot.writeFieldBegin(TABLE_NAME_FIELD_DESC);
        oprot.writeString(struct.tableName);
        oprot.writeFieldEnd();
      }
      if (struct.dbName != null) {
        oprot.writeFieldBegin(DB_NAME_FIELD_DESC);
        oprot.writeString(struct.dbName);
        oprot.writeFieldEnd();
      }
      if (struct.owner != null) {
        oprot.writeFieldBegin(OWNER_FIELD_DESC);
        oprot.writeString(struct.owner);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(CREATE_TIME_FIELD_DESC);
      oprot.writeI32(struct.createTime);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(LAST_ACCESS_TIME_FIELD_DESC);
      oprot.writeI32(struct.lastAccessTime);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(RETENTION_FIELD_DESC);
      oprot.writeI32(struct.retention);
      oprot.writeFieldEnd();
      if (struct.sd != null) {
        oprot.writeFieldBegin(SD_FIELD_DESC);
        struct.sd.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.priPartition != null) {
        oprot.writeFieldBegin(PRI_PARTITION_FIELD_DESC);
        struct.priPartition.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.subPartition != null) {
        oprot.writeFieldBegin(SUB_PARTITION_FIELD_DESC);
        struct.subPartition.write(oprot);
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
          for (Map.Entry<String, String> _iter74 : struct.parameters.entrySet()) {
            oprot.writeString(_iter74.getKey());
            oprot.writeString(_iter74.getValue());
          }
          oprot.writeMapEnd();
        }
        oprot.writeFieldEnd();
      }
      if (struct.viewOriginalText != null) {
        oprot.writeFieldBegin(VIEW_ORIGINAL_TEXT_FIELD_DESC);
        oprot.writeString(struct.viewOriginalText);
        oprot.writeFieldEnd();
      }
      if (struct.viewExpandedText != null) {
        oprot.writeFieldBegin(VIEW_EXPANDED_TEXT_FIELD_DESC);
        oprot.writeString(struct.viewExpandedText);
        oprot.writeFieldEnd();
      }
      if (struct.tableType != null) {
        oprot.writeFieldBegin(TABLE_TYPE_FIELD_DESC);
        oprot.writeString(struct.tableType);
        oprot.writeFieldEnd();
      }
      if (struct.vtables != null) {
        oprot.writeFieldBegin(VTABLES_FIELD_DESC);
        oprot.writeString(struct.vtables);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(IS_REPLACE_ON_EXIT_FIELD_DESC);
      oprot.writeBool(struct.isReplaceOnExit);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TableTupleSchemeFactory implements SchemeFactory {
    public TableTupleScheme getScheme() {
      return new TableTupleScheme();
    }
  }

  private static class TableTupleScheme extends TupleScheme<Table> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, Table struct)
        throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetTableName()) {
        optionals.set(0);
      }
      if (struct.isSetDbName()) {
        optionals.set(1);
      }
      if (struct.isSetOwner()) {
        optionals.set(2);
      }
      if (struct.isSetCreateTime()) {
        optionals.set(3);
      }
      if (struct.isSetLastAccessTime()) {
        optionals.set(4);
      }
      if (struct.isSetRetention()) {
        optionals.set(5);
      }
      if (struct.isSetSd()) {
        optionals.set(6);
      }
      if (struct.isSetPriPartition()) {
        optionals.set(7);
      }
      if (struct.isSetSubPartition()) {
        optionals.set(8);
      }
      if (struct.isSetParameters()) {
        optionals.set(9);
      }
      if (struct.isSetViewOriginalText()) {
        optionals.set(10);
      }
      if (struct.isSetViewExpandedText()) {
        optionals.set(11);
      }
      if (struct.isSetTableType()) {
        optionals.set(12);
      }
      if (struct.isSetVtables()) {
        optionals.set(13);
      }
      if (struct.isSetIsReplaceOnExit()) {
        optionals.set(14);
      }
      oprot.writeBitSet(optionals, 15);
      if (struct.isSetTableName()) {
        oprot.writeString(struct.tableName);
      }
      if (struct.isSetDbName()) {
        oprot.writeString(struct.dbName);
      }
      if (struct.isSetOwner()) {
        oprot.writeString(struct.owner);
      }
      if (struct.isSetCreateTime()) {
        oprot.writeI32(struct.createTime);
      }
      if (struct.isSetLastAccessTime()) {
        oprot.writeI32(struct.lastAccessTime);
      }
      if (struct.isSetRetention()) {
        oprot.writeI32(struct.retention);
      }
      if (struct.isSetSd()) {
        struct.sd.write(oprot);
      }
      if (struct.isSetPriPartition()) {
        struct.priPartition.write(oprot);
      }
      if (struct.isSetSubPartition()) {
        struct.subPartition.write(oprot);
      }
      if (struct.isSetParameters()) {
        {
          oprot.writeI32(struct.parameters.size());
          for (Map.Entry<String, String> _iter75 : struct.parameters.entrySet()) {
            oprot.writeString(_iter75.getKey());
            oprot.writeString(_iter75.getValue());
          }
        }
      }
      if (struct.isSetViewOriginalText()) {
        oprot.writeString(struct.viewOriginalText);
      }
      if (struct.isSetViewExpandedText()) {
        oprot.writeString(struct.viewExpandedText);
      }
      if (struct.isSetTableType()) {
        oprot.writeString(struct.tableType);
      }
      if (struct.isSetVtables()) {
        oprot.writeString(struct.vtables);
      }
      if (struct.isSetIsReplaceOnExit()) {
        oprot.writeBool(struct.isReplaceOnExit);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, Table struct)
        throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(15);
      if (incoming.get(0)) {
        struct.tableName = iprot.readString();
        struct.setTableNameIsSet(true);
      }
      if (incoming.get(1)) {
        struct.dbName = iprot.readString();
        struct.setDbNameIsSet(true);
      }
      if (incoming.get(2)) {
        struct.owner = iprot.readString();
        struct.setOwnerIsSet(true);
      }
      if (incoming.get(3)) {
        struct.createTime = iprot.readI32();
        struct.setCreateTimeIsSet(true);
      }
      if (incoming.get(4)) {
        struct.lastAccessTime = iprot.readI32();
        struct.setLastAccessTimeIsSet(true);
      }
      if (incoming.get(5)) {
        struct.retention = iprot.readI32();
        struct.setRetentionIsSet(true);
      }
      if (incoming.get(6)) {
        struct.sd = new StorageDescriptor();
        struct.sd.read(iprot);
        struct.setSdIsSet(true);
      }
      if (incoming.get(7)) {
        struct.priPartition = new Partition();
        struct.priPartition.read(iprot);
        struct.setPriPartitionIsSet(true);
      }
      if (incoming.get(8)) {
        struct.subPartition = new Partition();
        struct.subPartition.read(iprot);
        struct.setSubPartitionIsSet(true);
      }
      if (incoming.get(9)) {
        {
          org.apache.thrift.protocol.TMap _map76 = new org.apache.thrift.protocol.TMap(
              org.apache.thrift.protocol.TType.STRING,
              org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.parameters = new HashMap<String, String>(2 * _map76.size);
          for (int _i77 = 0; _i77 < _map76.size; ++_i77) {
            String _key78;
            String _val79;
            _key78 = iprot.readString();
            _val79 = iprot.readString();
            struct.parameters.put(_key78, _val79);
          }
        }
        struct.setParametersIsSet(true);
      }
      if (incoming.get(10)) {
        struct.viewOriginalText = iprot.readString();
        struct.setViewOriginalTextIsSet(true);
      }
      if (incoming.get(11)) {
        struct.viewExpandedText = iprot.readString();
        struct.setViewExpandedTextIsSet(true);
      }
      if (incoming.get(12)) {
        struct.tableType = iprot.readString();
        struct.setTableTypeIsSet(true);
      }
      if (incoming.get(13)) {
        struct.vtables = iprot.readString();
        struct.setVtablesIsSet(true);
      }
      if (incoming.get(14)) {
        struct.isReplaceOnExit = iprot.readBool();
        struct.setIsReplaceOnExitIsSet(true);
      }
    }
  }

}
