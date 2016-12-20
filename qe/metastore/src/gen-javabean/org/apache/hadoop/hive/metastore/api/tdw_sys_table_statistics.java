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

public class tdw_sys_table_statistics
    implements
    org.apache.thrift.TBase<tdw_sys_table_statistics, tdw_sys_table_statistics._Fields>,
    java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct(
      "tdw_sys_table_statistics");

  private static final org.apache.thrift.protocol.TField STAT_TABLE_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "stat_table_name", org.apache.thrift.protocol.TType.STRING, (short) 1);
  private static final org.apache.thrift.protocol.TField STAT_DB_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "stat_db_name", org.apache.thrift.protocol.TType.STRING, (short) 2);
  private static final org.apache.thrift.protocol.TField STAT_NUM_RECORDS_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "stat_num_records", org.apache.thrift.protocol.TType.I32, (short) 3);
  private static final org.apache.thrift.protocol.TField STAT_NUM_UNITS_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "stat_num_units", org.apache.thrift.protocol.TType.I32, (short) 4);
  private static final org.apache.thrift.protocol.TField STAT_TOTAL_SIZE_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "stat_total_size", org.apache.thrift.protocol.TType.I32, (short) 5);
  private static final org.apache.thrift.protocol.TField STAT_NUM_FILES_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "stat_num_files", org.apache.thrift.protocol.TType.I32, (short) 6);
  private static final org.apache.thrift.protocol.TField STAT_NUM_BLOCKS_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "stat_num_blocks", org.apache.thrift.protocol.TType.I32, (short) 7);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class,
        new tdw_sys_table_statisticsStandardSchemeFactory());
    schemes.put(TupleScheme.class,
        new tdw_sys_table_statisticsTupleSchemeFactory());
  }

  private String stat_table_name;
  private String stat_db_name;
  private int stat_num_records;
  private int stat_num_units;
  private int stat_total_size;
  private int stat_num_files;
  private int stat_num_blocks;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    STAT_TABLE_NAME((short) 1, "stat_table_name"), STAT_DB_NAME((short) 2,
        "stat_db_name"), STAT_NUM_RECORDS((short) 3, "stat_num_records"), STAT_NUM_UNITS(
        (short) 4, "stat_num_units"), STAT_TOTAL_SIZE((short) 5,
        "stat_total_size"), STAT_NUM_FILES((short) 6, "stat_num_files"), STAT_NUM_BLOCKS(
        (short) 7, "stat_num_blocks");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    public static _Fields findByThriftId(int fieldId) {
      switch (fieldId) {
      case 1:
        return STAT_TABLE_NAME;
      case 2:
        return STAT_DB_NAME;
      case 3:
        return STAT_NUM_RECORDS;
      case 4:
        return STAT_NUM_UNITS;
      case 5:
        return STAT_TOTAL_SIZE;
      case 6:
        return STAT_NUM_FILES;
      case 7:
        return STAT_NUM_BLOCKS;
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

  private static final int __STAT_NUM_RECORDS_ISSET_ID = 0;
  private static final int __STAT_NUM_UNITS_ISSET_ID = 1;
  private static final int __STAT_TOTAL_SIZE_ISSET_ID = 2;
  private static final int __STAT_NUM_FILES_ISSET_ID = 3;
  private static final int __STAT_NUM_BLOCKS_ISSET_ID = 4;
  private BitSet __isset_bit_vector = new BitSet(5);
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(
        _Fields.class);
    tmpMap.put(_Fields.STAT_TABLE_NAME,
        new org.apache.thrift.meta_data.FieldMetaData("stat_table_name",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.STAT_DB_NAME,
        new org.apache.thrift.meta_data.FieldMetaData("stat_db_name",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.STAT_NUM_RECORDS,
        new org.apache.thrift.meta_data.FieldMetaData("stat_num_records",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.STAT_NUM_UNITS,
        new org.apache.thrift.meta_data.FieldMetaData("stat_num_units",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.STAT_TOTAL_SIZE,
        new org.apache.thrift.meta_data.FieldMetaData("stat_total_size",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.STAT_NUM_FILES,
        new org.apache.thrift.meta_data.FieldMetaData("stat_num_files",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.STAT_NUM_BLOCKS,
        new org.apache.thrift.meta_data.FieldMetaData("stat_num_blocks",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.I32)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(
        tdw_sys_table_statistics.class, metaDataMap);
  }

  public tdw_sys_table_statistics() {
  }

  public tdw_sys_table_statistics(String stat_table_name, String stat_db_name,
      int stat_num_records, int stat_num_units, int stat_total_size,
      int stat_num_files, int stat_num_blocks) {
    this();
    this.stat_table_name = stat_table_name;
    this.stat_db_name = stat_db_name;
    this.stat_num_records = stat_num_records;
    setStat_num_recordsIsSet(true);
    this.stat_num_units = stat_num_units;
    setStat_num_unitsIsSet(true);
    this.stat_total_size = stat_total_size;
    setStat_total_sizeIsSet(true);
    this.stat_num_files = stat_num_files;
    setStat_num_filesIsSet(true);
    this.stat_num_blocks = stat_num_blocks;
    setStat_num_blocksIsSet(true);
  }

  public tdw_sys_table_statistics(tdw_sys_table_statistics other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    if (other.isSetStat_table_name()) {
      this.stat_table_name = other.stat_table_name;
    }
    if (other.isSetStat_db_name()) {
      this.stat_db_name = other.stat_db_name;
    }
    this.stat_num_records = other.stat_num_records;
    this.stat_num_units = other.stat_num_units;
    this.stat_total_size = other.stat_total_size;
    this.stat_num_files = other.stat_num_files;
    this.stat_num_blocks = other.stat_num_blocks;
  }

  public tdw_sys_table_statistics deepCopy() {
    return new tdw_sys_table_statistics(this);
  }

  @Override
  public void clear() {
    this.stat_table_name = null;
    this.stat_db_name = null;
    setStat_num_recordsIsSet(false);
    this.stat_num_records = 0;
    setStat_num_unitsIsSet(false);
    this.stat_num_units = 0;
    setStat_total_sizeIsSet(false);
    this.stat_total_size = 0;
    setStat_num_filesIsSet(false);
    this.stat_num_files = 0;
    setStat_num_blocksIsSet(false);
    this.stat_num_blocks = 0;
  }

  public String getStat_table_name() {
    return this.stat_table_name;
  }

  public void setStat_table_name(String stat_table_name) {
    this.stat_table_name = stat_table_name;
  }

  public void unsetStat_table_name() {
    this.stat_table_name = null;
  }

  public boolean isSetStat_table_name() {
    return this.stat_table_name != null;
  }

  public void setStat_table_nameIsSet(boolean value) {
    if (!value) {
      this.stat_table_name = null;
    }
  }

  public String getStat_db_name() {
    return this.stat_db_name;
  }

  public void setStat_db_name(String stat_db_name) {
    this.stat_db_name = stat_db_name;
  }

  public void unsetStat_db_name() {
    this.stat_db_name = null;
  }

  public boolean isSetStat_db_name() {
    return this.stat_db_name != null;
  }

  public void setStat_db_nameIsSet(boolean value) {
    if (!value) {
      this.stat_db_name = null;
    }
  }

  public int getStat_num_records() {
    return this.stat_num_records;
  }

  public void setStat_num_records(int stat_num_records) {
    this.stat_num_records = stat_num_records;
    setStat_num_recordsIsSet(true);
  }

  public void unsetStat_num_records() {
    __isset_bit_vector.clear(__STAT_NUM_RECORDS_ISSET_ID);
  }

  public boolean isSetStat_num_records() {
    return __isset_bit_vector.get(__STAT_NUM_RECORDS_ISSET_ID);
  }

  public void setStat_num_recordsIsSet(boolean value) {
    __isset_bit_vector.set(__STAT_NUM_RECORDS_ISSET_ID, value);
  }

  public int getStat_num_units() {
    return this.stat_num_units;
  }

  public void setStat_num_units(int stat_num_units) {
    this.stat_num_units = stat_num_units;
    setStat_num_unitsIsSet(true);
  }

  public void unsetStat_num_units() {
    __isset_bit_vector.clear(__STAT_NUM_UNITS_ISSET_ID);
  }

  public boolean isSetStat_num_units() {
    return __isset_bit_vector.get(__STAT_NUM_UNITS_ISSET_ID);
  }

  public void setStat_num_unitsIsSet(boolean value) {
    __isset_bit_vector.set(__STAT_NUM_UNITS_ISSET_ID, value);
  }

  public int getStat_total_size() {
    return this.stat_total_size;
  }

  public void setStat_total_size(int stat_total_size) {
    this.stat_total_size = stat_total_size;
    setStat_total_sizeIsSet(true);
  }

  public void unsetStat_total_size() {
    __isset_bit_vector.clear(__STAT_TOTAL_SIZE_ISSET_ID);
  }

  public boolean isSetStat_total_size() {
    return __isset_bit_vector.get(__STAT_TOTAL_SIZE_ISSET_ID);
  }

  public void setStat_total_sizeIsSet(boolean value) {
    __isset_bit_vector.set(__STAT_TOTAL_SIZE_ISSET_ID, value);
  }

  public int getStat_num_files() {
    return this.stat_num_files;
  }

  public void setStat_num_files(int stat_num_files) {
    this.stat_num_files = stat_num_files;
    setStat_num_filesIsSet(true);
  }

  public void unsetStat_num_files() {
    __isset_bit_vector.clear(__STAT_NUM_FILES_ISSET_ID);
  }

  public boolean isSetStat_num_files() {
    return __isset_bit_vector.get(__STAT_NUM_FILES_ISSET_ID);
  }

  public void setStat_num_filesIsSet(boolean value) {
    __isset_bit_vector.set(__STAT_NUM_FILES_ISSET_ID, value);
  }

  public int getStat_num_blocks() {
    return this.stat_num_blocks;
  }

  public void setStat_num_blocks(int stat_num_blocks) {
    this.stat_num_blocks = stat_num_blocks;
    setStat_num_blocksIsSet(true);
  }

  public void unsetStat_num_blocks() {
    __isset_bit_vector.clear(__STAT_NUM_BLOCKS_ISSET_ID);
  }

  public boolean isSetStat_num_blocks() {
    return __isset_bit_vector.get(__STAT_NUM_BLOCKS_ISSET_ID);
  }

  public void setStat_num_blocksIsSet(boolean value) {
    __isset_bit_vector.set(__STAT_NUM_BLOCKS_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case STAT_TABLE_NAME:
      if (value == null) {
        unsetStat_table_name();
      } else {
        setStat_table_name((String) value);
      }
      break;

    case STAT_DB_NAME:
      if (value == null) {
        unsetStat_db_name();
      } else {
        setStat_db_name((String) value);
      }
      break;

    case STAT_NUM_RECORDS:
      if (value == null) {
        unsetStat_num_records();
      } else {
        setStat_num_records((Integer) value);
      }
      break;

    case STAT_NUM_UNITS:
      if (value == null) {
        unsetStat_num_units();
      } else {
        setStat_num_units((Integer) value);
      }
      break;

    case STAT_TOTAL_SIZE:
      if (value == null) {
        unsetStat_total_size();
      } else {
        setStat_total_size((Integer) value);
      }
      break;

    case STAT_NUM_FILES:
      if (value == null) {
        unsetStat_num_files();
      } else {
        setStat_num_files((Integer) value);
      }
      break;

    case STAT_NUM_BLOCKS:
      if (value == null) {
        unsetStat_num_blocks();
      } else {
        setStat_num_blocks((Integer) value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case STAT_TABLE_NAME:
      return getStat_table_name();

    case STAT_DB_NAME:
      return getStat_db_name();

    case STAT_NUM_RECORDS:
      return Integer.valueOf(getStat_num_records());

    case STAT_NUM_UNITS:
      return Integer.valueOf(getStat_num_units());

    case STAT_TOTAL_SIZE:
      return Integer.valueOf(getStat_total_size());

    case STAT_NUM_FILES:
      return Integer.valueOf(getStat_num_files());

    case STAT_NUM_BLOCKS:
      return Integer.valueOf(getStat_num_blocks());

    }
    throw new IllegalStateException();
  }

  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case STAT_TABLE_NAME:
      return isSetStat_table_name();
    case STAT_DB_NAME:
      return isSetStat_db_name();
    case STAT_NUM_RECORDS:
      return isSetStat_num_records();
    case STAT_NUM_UNITS:
      return isSetStat_num_units();
    case STAT_TOTAL_SIZE:
      return isSetStat_total_size();
    case STAT_NUM_FILES:
      return isSetStat_num_files();
    case STAT_NUM_BLOCKS:
      return isSetStat_num_blocks();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof tdw_sys_table_statistics)
      return this.equals((tdw_sys_table_statistics) that);
    return false;
  }

  public boolean equals(tdw_sys_table_statistics that) {
    if (that == null)
      return false;

    boolean this_present_stat_table_name = true && this.isSetStat_table_name();
    boolean that_present_stat_table_name = true && that.isSetStat_table_name();
    if (this_present_stat_table_name || that_present_stat_table_name) {
      if (!(this_present_stat_table_name && that_present_stat_table_name))
        return false;
      if (!this.stat_table_name.equals(that.stat_table_name))
        return false;
    }

    boolean this_present_stat_db_name = true && this.isSetStat_db_name();
    boolean that_present_stat_db_name = true && that.isSetStat_db_name();
    if (this_present_stat_db_name || that_present_stat_db_name) {
      if (!(this_present_stat_db_name && that_present_stat_db_name))
        return false;
      if (!this.stat_db_name.equals(that.stat_db_name))
        return false;
    }

    boolean this_present_stat_num_records = true;
    boolean that_present_stat_num_records = true;
    if (this_present_stat_num_records || that_present_stat_num_records) {
      if (!(this_present_stat_num_records && that_present_stat_num_records))
        return false;
      if (this.stat_num_records != that.stat_num_records)
        return false;
    }

    boolean this_present_stat_num_units = true;
    boolean that_present_stat_num_units = true;
    if (this_present_stat_num_units || that_present_stat_num_units) {
      if (!(this_present_stat_num_units && that_present_stat_num_units))
        return false;
      if (this.stat_num_units != that.stat_num_units)
        return false;
    }

    boolean this_present_stat_total_size = true;
    boolean that_present_stat_total_size = true;
    if (this_present_stat_total_size || that_present_stat_total_size) {
      if (!(this_present_stat_total_size && that_present_stat_total_size))
        return false;
      if (this.stat_total_size != that.stat_total_size)
        return false;
    }

    boolean this_present_stat_num_files = true;
    boolean that_present_stat_num_files = true;
    if (this_present_stat_num_files || that_present_stat_num_files) {
      if (!(this_present_stat_num_files && that_present_stat_num_files))
        return false;
      if (this.stat_num_files != that.stat_num_files)
        return false;
    }

    boolean this_present_stat_num_blocks = true;
    boolean that_present_stat_num_blocks = true;
    if (this_present_stat_num_blocks || that_present_stat_num_blocks) {
      if (!(this_present_stat_num_blocks && that_present_stat_num_blocks))
        return false;
      if (this.stat_num_blocks != that.stat_num_blocks)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(tdw_sys_table_statistics other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    tdw_sys_table_statistics typedOther = (tdw_sys_table_statistics) other;

    lastComparison = Boolean.valueOf(isSetStat_table_name()).compareTo(
        typedOther.isSetStat_table_name());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStat_table_name()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.stat_table_name, typedOther.stat_table_name);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStat_db_name()).compareTo(
        typedOther.isSetStat_db_name());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStat_db_name()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.stat_db_name, typedOther.stat_db_name);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStat_num_records()).compareTo(
        typedOther.isSetStat_num_records());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStat_num_records()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.stat_num_records, typedOther.stat_num_records);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStat_num_units()).compareTo(
        typedOther.isSetStat_num_units());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStat_num_units()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.stat_num_units, typedOther.stat_num_units);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStat_total_size()).compareTo(
        typedOther.isSetStat_total_size());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStat_total_size()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.stat_total_size, typedOther.stat_total_size);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStat_num_files()).compareTo(
        typedOther.isSetStat_num_files());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStat_num_files()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.stat_num_files, typedOther.stat_num_files);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStat_num_blocks()).compareTo(
        typedOther.isSetStat_num_blocks());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStat_num_blocks()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.stat_num_blocks, typedOther.stat_num_blocks);
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
    StringBuilder sb = new StringBuilder("tdw_sys_table_statistics(");
    boolean first = true;

    sb.append("stat_table_name:");
    if (this.stat_table_name == null) {
      sb.append("null");
    } else {
      sb.append(this.stat_table_name);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("stat_db_name:");
    if (this.stat_db_name == null) {
      sb.append("null");
    } else {
      sb.append(this.stat_db_name);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("stat_num_records:");
    sb.append(this.stat_num_records);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("stat_num_units:");
    sb.append(this.stat_num_units);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("stat_total_size:");
    sb.append(this.stat_total_size);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("stat_num_files:");
    sb.append(this.stat_num_files);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("stat_num_blocks:");
    sb.append(this.stat_num_blocks);
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

  private static class tdw_sys_table_statisticsStandardSchemeFactory implements
      SchemeFactory {
    public tdw_sys_table_statisticsStandardScheme getScheme() {
      return new tdw_sys_table_statisticsStandardScheme();
    }
  }

  private static class tdw_sys_table_statisticsStandardScheme extends
      StandardScheme<tdw_sys_table_statistics> {

    public void read(org.apache.thrift.protocol.TProtocol iprot,
        tdw_sys_table_statistics struct) throws org.apache.thrift.TException {
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
            struct.stat_table_name = iprot.readString();
            struct.setStat_table_nameIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 2:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.stat_db_name = iprot.readString();
            struct.setStat_db_nameIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 3:
          if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
            struct.stat_num_records = iprot.readI32();
            struct.setStat_num_recordsIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 4:
          if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
            struct.stat_num_units = iprot.readI32();
            struct.setStat_num_unitsIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 5:
          if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
            struct.stat_total_size = iprot.readI32();
            struct.setStat_total_sizeIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 6:
          if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
            struct.stat_num_files = iprot.readI32();
            struct.setStat_num_filesIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 7:
          if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
            struct.stat_num_blocks = iprot.readI32();
            struct.setStat_num_blocksIsSet(true);
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
        tdw_sys_table_statistics struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.stat_table_name != null) {
        oprot.writeFieldBegin(STAT_TABLE_NAME_FIELD_DESC);
        oprot.writeString(struct.stat_table_name);
        oprot.writeFieldEnd();
      }
      if (struct.stat_db_name != null) {
        oprot.writeFieldBegin(STAT_DB_NAME_FIELD_DESC);
        oprot.writeString(struct.stat_db_name);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(STAT_NUM_RECORDS_FIELD_DESC);
      oprot.writeI32(struct.stat_num_records);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(STAT_NUM_UNITS_FIELD_DESC);
      oprot.writeI32(struct.stat_num_units);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(STAT_TOTAL_SIZE_FIELD_DESC);
      oprot.writeI32(struct.stat_total_size);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(STAT_NUM_FILES_FIELD_DESC);
      oprot.writeI32(struct.stat_num_files);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(STAT_NUM_BLOCKS_FIELD_DESC);
      oprot.writeI32(struct.stat_num_blocks);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class tdw_sys_table_statisticsTupleSchemeFactory implements
      SchemeFactory {
    public tdw_sys_table_statisticsTupleScheme getScheme() {
      return new tdw_sys_table_statisticsTupleScheme();
    }
  }

  private static class tdw_sys_table_statisticsTupleScheme extends
      TupleScheme<tdw_sys_table_statistics> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot,
        tdw_sys_table_statistics struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetStat_table_name()) {
        optionals.set(0);
      }
      if (struct.isSetStat_db_name()) {
        optionals.set(1);
      }
      if (struct.isSetStat_num_records()) {
        optionals.set(2);
      }
      if (struct.isSetStat_num_units()) {
        optionals.set(3);
      }
      if (struct.isSetStat_total_size()) {
        optionals.set(4);
      }
      if (struct.isSetStat_num_files()) {
        optionals.set(5);
      }
      if (struct.isSetStat_num_blocks()) {
        optionals.set(6);
      }
      oprot.writeBitSet(optionals, 7);
      if (struct.isSetStat_table_name()) {
        oprot.writeString(struct.stat_table_name);
      }
      if (struct.isSetStat_db_name()) {
        oprot.writeString(struct.stat_db_name);
      }
      if (struct.isSetStat_num_records()) {
        oprot.writeI32(struct.stat_num_records);
      }
      if (struct.isSetStat_num_units()) {
        oprot.writeI32(struct.stat_num_units);
      }
      if (struct.isSetStat_total_size()) {
        oprot.writeI32(struct.stat_total_size);
      }
      if (struct.isSetStat_num_files()) {
        oprot.writeI32(struct.stat_num_files);
      }
      if (struct.isSetStat_num_blocks()) {
        oprot.writeI32(struct.stat_num_blocks);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot,
        tdw_sys_table_statistics struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(7);
      if (incoming.get(0)) {
        struct.stat_table_name = iprot.readString();
        struct.setStat_table_nameIsSet(true);
      }
      if (incoming.get(1)) {
        struct.stat_db_name = iprot.readString();
        struct.setStat_db_nameIsSet(true);
      }
      if (incoming.get(2)) {
        struct.stat_num_records = iprot.readI32();
        struct.setStat_num_recordsIsSet(true);
      }
      if (incoming.get(3)) {
        struct.stat_num_units = iprot.readI32();
        struct.setStat_num_unitsIsSet(true);
      }
      if (incoming.get(4)) {
        struct.stat_total_size = iprot.readI32();
        struct.setStat_total_sizeIsSet(true);
      }
      if (incoming.get(5)) {
        struct.stat_num_files = iprot.readI32();
        struct.setStat_num_filesIsSet(true);
      }
      if (incoming.get(6)) {
        struct.stat_num_blocks = iprot.readI32();
        struct.setStat_num_blocksIsSet(true);
      }
    }
  }

}
