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

public class tdw_sys_fields_statistics
    implements
    org.apache.thrift.TBase<tdw_sys_fields_statistics, tdw_sys_fields_statistics._Fields>,
    java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct(
      "tdw_sys_fields_statistics");

  private static final org.apache.thrift.protocol.TField STAT_TABLE_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "stat_table_name", org.apache.thrift.protocol.TType.STRING, (short) 1);
  private static final org.apache.thrift.protocol.TField STAT_DB_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "stat_db_name", org.apache.thrift.protocol.TType.STRING, (short) 2);
  private static final org.apache.thrift.protocol.TField STAT_FIELD_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "stat_field_name", org.apache.thrift.protocol.TType.STRING, (short) 3);
  private static final org.apache.thrift.protocol.TField STAT_NULLFAC_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "stat_nullfac", org.apache.thrift.protocol.TType.DOUBLE, (short) 4);
  private static final org.apache.thrift.protocol.TField STAT_AVG_FIELD_WIDTH_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "stat_avg_field_width", org.apache.thrift.protocol.TType.I32, (short) 5);
  private static final org.apache.thrift.protocol.TField STAT_DISTINCT_VALUES_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "stat_distinct_values", org.apache.thrift.protocol.TType.DOUBLE,
      (short) 6);
  private static final org.apache.thrift.protocol.TField STAT_VALUES_1_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "stat_values_1", org.apache.thrift.protocol.TType.STRING, (short) 7);
  private static final org.apache.thrift.protocol.TField STAT_NUMBERS_1_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "stat_numbers_1", org.apache.thrift.protocol.TType.STRING, (short) 8);
  private static final org.apache.thrift.protocol.TField STAT_VALUES_2_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "stat_values_2", org.apache.thrift.protocol.TType.STRING, (short) 9);
  private static final org.apache.thrift.protocol.TField STAT_NUMBERS_2_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "stat_numbers_2", org.apache.thrift.protocol.TType.STRING, (short) 10);
  private static final org.apache.thrift.protocol.TField STAT_VALUES_3_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "stat_values_3", org.apache.thrift.protocol.TType.STRING, (short) 11);
  private static final org.apache.thrift.protocol.TField STAT_NUMBERS_3_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "stat_numbers_3", org.apache.thrift.protocol.TType.STRING, (short) 12);
  private static final org.apache.thrift.protocol.TField STAT_NUMBER_1_TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "stat_number_1_type", org.apache.thrift.protocol.TType.I32, (short) 13);
  private static final org.apache.thrift.protocol.TField STAT_NUMBER_2_TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "stat_number_2_type", org.apache.thrift.protocol.TType.I32, (short) 14);
  private static final org.apache.thrift.protocol.TField STAT_NUMBER_3_TYPE_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "stat_number_3_type", org.apache.thrift.protocol.TType.I32, (short) 15);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class,
        new tdw_sys_fields_statisticsStandardSchemeFactory());
    schemes.put(TupleScheme.class,
        new tdw_sys_fields_statisticsTupleSchemeFactory());
  }

  private String stat_table_name;
  private String stat_db_name;
  private String stat_field_name;
  private double stat_nullfac;
  private int stat_avg_field_width;
  private double stat_distinct_values;
  private String stat_values_1;
  private String stat_numbers_1;
  private String stat_values_2;
  private String stat_numbers_2;
  private String stat_values_3;
  private String stat_numbers_3;
  private int stat_number_1_type;
  private int stat_number_2_type;
  private int stat_number_3_type;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    STAT_TABLE_NAME((short) 1, "stat_table_name"), STAT_DB_NAME((short) 2,
        "stat_db_name"), STAT_FIELD_NAME((short) 3, "stat_field_name"), STAT_NULLFAC(
        (short) 4, "stat_nullfac"), STAT_AVG_FIELD_WIDTH((short) 5,
        "stat_avg_field_width"), STAT_DISTINCT_VALUES((short) 6,
        "stat_distinct_values"), STAT_VALUES_1((short) 7, "stat_values_1"), STAT_NUMBERS_1(
        (short) 8, "stat_numbers_1"), STAT_VALUES_2((short) 9, "stat_values_2"), STAT_NUMBERS_2(
        (short) 10, "stat_numbers_2"), STAT_VALUES_3((short) 11,
        "stat_values_3"), STAT_NUMBERS_3((short) 12, "stat_numbers_3"), STAT_NUMBER_1_TYPE(
        (short) 13, "stat_number_1_type"), STAT_NUMBER_2_TYPE((short) 14,
        "stat_number_2_type"), STAT_NUMBER_3_TYPE((short) 15,
        "stat_number_3_type");

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
        return STAT_FIELD_NAME;
      case 4:
        return STAT_NULLFAC;
      case 5:
        return STAT_AVG_FIELD_WIDTH;
      case 6:
        return STAT_DISTINCT_VALUES;
      case 7:
        return STAT_VALUES_1;
      case 8:
        return STAT_NUMBERS_1;
      case 9:
        return STAT_VALUES_2;
      case 10:
        return STAT_NUMBERS_2;
      case 11:
        return STAT_VALUES_3;
      case 12:
        return STAT_NUMBERS_3;
      case 13:
        return STAT_NUMBER_1_TYPE;
      case 14:
        return STAT_NUMBER_2_TYPE;
      case 15:
        return STAT_NUMBER_3_TYPE;
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

  private static final int __STAT_NULLFAC_ISSET_ID = 0;
  private static final int __STAT_AVG_FIELD_WIDTH_ISSET_ID = 1;
  private static final int __STAT_DISTINCT_VALUES_ISSET_ID = 2;
  private static final int __STAT_NUMBER_1_TYPE_ISSET_ID = 3;
  private static final int __STAT_NUMBER_2_TYPE_ISSET_ID = 4;
  private static final int __STAT_NUMBER_3_TYPE_ISSET_ID = 5;
  private BitSet __isset_bit_vector = new BitSet(6);
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
    tmpMap.put(_Fields.STAT_FIELD_NAME,
        new org.apache.thrift.meta_data.FieldMetaData("stat_field_name",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.STAT_NULLFAC,
        new org.apache.thrift.meta_data.FieldMetaData("stat_nullfac",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.DOUBLE)));
    tmpMap.put(_Fields.STAT_AVG_FIELD_WIDTH,
        new org.apache.thrift.meta_data.FieldMetaData("stat_avg_field_width",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.STAT_DISTINCT_VALUES,
        new org.apache.thrift.meta_data.FieldMetaData("stat_distinct_values",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.DOUBLE)));
    tmpMap.put(_Fields.STAT_VALUES_1,
        new org.apache.thrift.meta_data.FieldMetaData("stat_values_1",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.STAT_NUMBERS_1,
        new org.apache.thrift.meta_data.FieldMetaData("stat_numbers_1",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.STAT_VALUES_2,
        new org.apache.thrift.meta_data.FieldMetaData("stat_values_2",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.STAT_NUMBERS_2,
        new org.apache.thrift.meta_data.FieldMetaData("stat_numbers_2",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.STAT_VALUES_3,
        new org.apache.thrift.meta_data.FieldMetaData("stat_values_3",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.STAT_NUMBERS_3,
        new org.apache.thrift.meta_data.FieldMetaData("stat_numbers_3",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.STAT_NUMBER_1_TYPE,
        new org.apache.thrift.meta_data.FieldMetaData("stat_number_1_type",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.STAT_NUMBER_2_TYPE,
        new org.apache.thrift.meta_data.FieldMetaData("stat_number_2_type",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.STAT_NUMBER_3_TYPE,
        new org.apache.thrift.meta_data.FieldMetaData("stat_number_3_type",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.I32)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(
        tdw_sys_fields_statistics.class, metaDataMap);
  }

  public tdw_sys_fields_statistics() {
  }

  public tdw_sys_fields_statistics(String stat_table_name, String stat_db_name,
      String stat_field_name, double stat_nullfac, int stat_avg_field_width,
      double stat_distinct_values, String stat_values_1, String stat_numbers_1,
      String stat_values_2, String stat_numbers_2, String stat_values_3,
      String stat_numbers_3, int stat_number_1_type, int stat_number_2_type,
      int stat_number_3_type) {
    this();
    this.stat_table_name = stat_table_name;
    this.stat_db_name = stat_db_name;
    this.stat_field_name = stat_field_name;
    this.stat_nullfac = stat_nullfac;
    setStat_nullfacIsSet(true);
    this.stat_avg_field_width = stat_avg_field_width;
    setStat_avg_field_widthIsSet(true);
    this.stat_distinct_values = stat_distinct_values;
    setStat_distinct_valuesIsSet(true);
    this.stat_values_1 = stat_values_1;
    this.stat_numbers_1 = stat_numbers_1;
    this.stat_values_2 = stat_values_2;
    this.stat_numbers_2 = stat_numbers_2;
    this.stat_values_3 = stat_values_3;
    this.stat_numbers_3 = stat_numbers_3;
    this.stat_number_1_type = stat_number_1_type;
    setStat_number_1_typeIsSet(true);
    this.stat_number_2_type = stat_number_2_type;
    setStat_number_2_typeIsSet(true);
    this.stat_number_3_type = stat_number_3_type;
    setStat_number_3_typeIsSet(true);
  }

  public tdw_sys_fields_statistics(tdw_sys_fields_statistics other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    if (other.isSetStat_table_name()) {
      this.stat_table_name = other.stat_table_name;
    }
    if (other.isSetStat_db_name()) {
      this.stat_db_name = other.stat_db_name;
    }
    if (other.isSetStat_field_name()) {
      this.stat_field_name = other.stat_field_name;
    }
    this.stat_nullfac = other.stat_nullfac;
    this.stat_avg_field_width = other.stat_avg_field_width;
    this.stat_distinct_values = other.stat_distinct_values;
    if (other.isSetStat_values_1()) {
      this.stat_values_1 = other.stat_values_1;
    }
    if (other.isSetStat_numbers_1()) {
      this.stat_numbers_1 = other.stat_numbers_1;
    }
    if (other.isSetStat_values_2()) {
      this.stat_values_2 = other.stat_values_2;
    }
    if (other.isSetStat_numbers_2()) {
      this.stat_numbers_2 = other.stat_numbers_2;
    }
    if (other.isSetStat_values_3()) {
      this.stat_values_3 = other.stat_values_3;
    }
    if (other.isSetStat_numbers_3()) {
      this.stat_numbers_3 = other.stat_numbers_3;
    }
    this.stat_number_1_type = other.stat_number_1_type;
    this.stat_number_2_type = other.stat_number_2_type;
    this.stat_number_3_type = other.stat_number_3_type;
  }

  public tdw_sys_fields_statistics deepCopy() {
    return new tdw_sys_fields_statistics(this);
  }

  @Override
  public void clear() {
    this.stat_table_name = null;
    this.stat_db_name = null;
    this.stat_field_name = null;
    setStat_nullfacIsSet(false);
    this.stat_nullfac = 0.0;
    setStat_avg_field_widthIsSet(false);
    this.stat_avg_field_width = 0;
    setStat_distinct_valuesIsSet(false);
    this.stat_distinct_values = 0.0;
    this.stat_values_1 = null;
    this.stat_numbers_1 = null;
    this.stat_values_2 = null;
    this.stat_numbers_2 = null;
    this.stat_values_3 = null;
    this.stat_numbers_3 = null;
    setStat_number_1_typeIsSet(false);
    this.stat_number_1_type = 0;
    setStat_number_2_typeIsSet(false);
    this.stat_number_2_type = 0;
    setStat_number_3_typeIsSet(false);
    this.stat_number_3_type = 0;
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

  public String getStat_field_name() {
    return this.stat_field_name;
  }

  public void setStat_field_name(String stat_field_name) {
    this.stat_field_name = stat_field_name;
  }

  public void unsetStat_field_name() {
    this.stat_field_name = null;
  }

  public boolean isSetStat_field_name() {
    return this.stat_field_name != null;
  }

  public void setStat_field_nameIsSet(boolean value) {
    if (!value) {
      this.stat_field_name = null;
    }
  }

  public double getStat_nullfac() {
    return this.stat_nullfac;
  }

  public void setStat_nullfac(double stat_nullfac) {
    this.stat_nullfac = stat_nullfac;
    setStat_nullfacIsSet(true);
  }

  public void unsetStat_nullfac() {
    __isset_bit_vector.clear(__STAT_NULLFAC_ISSET_ID);
  }

  public boolean isSetStat_nullfac() {
    return __isset_bit_vector.get(__STAT_NULLFAC_ISSET_ID);
  }

  public void setStat_nullfacIsSet(boolean value) {
    __isset_bit_vector.set(__STAT_NULLFAC_ISSET_ID, value);
  }

  public int getStat_avg_field_width() {
    return this.stat_avg_field_width;
  }

  public void setStat_avg_field_width(int stat_avg_field_width) {
    this.stat_avg_field_width = stat_avg_field_width;
    setStat_avg_field_widthIsSet(true);
  }

  public void unsetStat_avg_field_width() {
    __isset_bit_vector.clear(__STAT_AVG_FIELD_WIDTH_ISSET_ID);
  }

  public boolean isSetStat_avg_field_width() {
    return __isset_bit_vector.get(__STAT_AVG_FIELD_WIDTH_ISSET_ID);
  }

  public void setStat_avg_field_widthIsSet(boolean value) {
    __isset_bit_vector.set(__STAT_AVG_FIELD_WIDTH_ISSET_ID, value);
  }

  public double getStat_distinct_values() {
    return this.stat_distinct_values;
  }

  public void setStat_distinct_values(double stat_distinct_values) {
    this.stat_distinct_values = stat_distinct_values;
    setStat_distinct_valuesIsSet(true);
  }

  public void unsetStat_distinct_values() {
    __isset_bit_vector.clear(__STAT_DISTINCT_VALUES_ISSET_ID);
  }

  public boolean isSetStat_distinct_values() {
    return __isset_bit_vector.get(__STAT_DISTINCT_VALUES_ISSET_ID);
  }

  public void setStat_distinct_valuesIsSet(boolean value) {
    __isset_bit_vector.set(__STAT_DISTINCT_VALUES_ISSET_ID, value);
  }

  public String getStat_values_1() {
    return this.stat_values_1;
  }

  public void setStat_values_1(String stat_values_1) {
    this.stat_values_1 = stat_values_1;
  }

  public void unsetStat_values_1() {
    this.stat_values_1 = null;
  }

  public boolean isSetStat_values_1() {
    return this.stat_values_1 != null;
  }

  public void setStat_values_1IsSet(boolean value) {
    if (!value) {
      this.stat_values_1 = null;
    }
  }

  public String getStat_numbers_1() {
    return this.stat_numbers_1;
  }

  public void setStat_numbers_1(String stat_numbers_1) {
    this.stat_numbers_1 = stat_numbers_1;
  }

  public void unsetStat_numbers_1() {
    this.stat_numbers_1 = null;
  }

  public boolean isSetStat_numbers_1() {
    return this.stat_numbers_1 != null;
  }

  public void setStat_numbers_1IsSet(boolean value) {
    if (!value) {
      this.stat_numbers_1 = null;
    }
  }

  public String getStat_values_2() {
    return this.stat_values_2;
  }

  public void setStat_values_2(String stat_values_2) {
    this.stat_values_2 = stat_values_2;
  }

  public void unsetStat_values_2() {
    this.stat_values_2 = null;
  }

  public boolean isSetStat_values_2() {
    return this.stat_values_2 != null;
  }

  public void setStat_values_2IsSet(boolean value) {
    if (!value) {
      this.stat_values_2 = null;
    }
  }

  public String getStat_numbers_2() {
    return this.stat_numbers_2;
  }

  public void setStat_numbers_2(String stat_numbers_2) {
    this.stat_numbers_2 = stat_numbers_2;
  }

  public void unsetStat_numbers_2() {
    this.stat_numbers_2 = null;
  }

  public boolean isSetStat_numbers_2() {
    return this.stat_numbers_2 != null;
  }

  public void setStat_numbers_2IsSet(boolean value) {
    if (!value) {
      this.stat_numbers_2 = null;
    }
  }

  public String getStat_values_3() {
    return this.stat_values_3;
  }

  public void setStat_values_3(String stat_values_3) {
    this.stat_values_3 = stat_values_3;
  }

  public void unsetStat_values_3() {
    this.stat_values_3 = null;
  }

  public boolean isSetStat_values_3() {
    return this.stat_values_3 != null;
  }

  public void setStat_values_3IsSet(boolean value) {
    if (!value) {
      this.stat_values_3 = null;
    }
  }

  public String getStat_numbers_3() {
    return this.stat_numbers_3;
  }

  public void setStat_numbers_3(String stat_numbers_3) {
    this.stat_numbers_3 = stat_numbers_3;
  }

  public void unsetStat_numbers_3() {
    this.stat_numbers_3 = null;
  }

  public boolean isSetStat_numbers_3() {
    return this.stat_numbers_3 != null;
  }

  public void setStat_numbers_3IsSet(boolean value) {
    if (!value) {
      this.stat_numbers_3 = null;
    }
  }

  public int getStat_number_1_type() {
    return this.stat_number_1_type;
  }

  public void setStat_number_1_type(int stat_number_1_type) {
    this.stat_number_1_type = stat_number_1_type;
    setStat_number_1_typeIsSet(true);
  }

  public void unsetStat_number_1_type() {
    __isset_bit_vector.clear(__STAT_NUMBER_1_TYPE_ISSET_ID);
  }

  public boolean isSetStat_number_1_type() {
    return __isset_bit_vector.get(__STAT_NUMBER_1_TYPE_ISSET_ID);
  }

  public void setStat_number_1_typeIsSet(boolean value) {
    __isset_bit_vector.set(__STAT_NUMBER_1_TYPE_ISSET_ID, value);
  }

  public int getStat_number_2_type() {
    return this.stat_number_2_type;
  }

  public void setStat_number_2_type(int stat_number_2_type) {
    this.stat_number_2_type = stat_number_2_type;
    setStat_number_2_typeIsSet(true);
  }

  public void unsetStat_number_2_type() {
    __isset_bit_vector.clear(__STAT_NUMBER_2_TYPE_ISSET_ID);
  }

  public boolean isSetStat_number_2_type() {
    return __isset_bit_vector.get(__STAT_NUMBER_2_TYPE_ISSET_ID);
  }

  public void setStat_number_2_typeIsSet(boolean value) {
    __isset_bit_vector.set(__STAT_NUMBER_2_TYPE_ISSET_ID, value);
  }

  public int getStat_number_3_type() {
    return this.stat_number_3_type;
  }

  public void setStat_number_3_type(int stat_number_3_type) {
    this.stat_number_3_type = stat_number_3_type;
    setStat_number_3_typeIsSet(true);
  }

  public void unsetStat_number_3_type() {
    __isset_bit_vector.clear(__STAT_NUMBER_3_TYPE_ISSET_ID);
  }

  public boolean isSetStat_number_3_type() {
    return __isset_bit_vector.get(__STAT_NUMBER_3_TYPE_ISSET_ID);
  }

  public void setStat_number_3_typeIsSet(boolean value) {
    __isset_bit_vector.set(__STAT_NUMBER_3_TYPE_ISSET_ID, value);
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

    case STAT_FIELD_NAME:
      if (value == null) {
        unsetStat_field_name();
      } else {
        setStat_field_name((String) value);
      }
      break;

    case STAT_NULLFAC:
      if (value == null) {
        unsetStat_nullfac();
      } else {
        setStat_nullfac((Double) value);
      }
      break;

    case STAT_AVG_FIELD_WIDTH:
      if (value == null) {
        unsetStat_avg_field_width();
      } else {
        setStat_avg_field_width((Integer) value);
      }
      break;

    case STAT_DISTINCT_VALUES:
      if (value == null) {
        unsetStat_distinct_values();
      } else {
        setStat_distinct_values((Double) value);
      }
      break;

    case STAT_VALUES_1:
      if (value == null) {
        unsetStat_values_1();
      } else {
        setStat_values_1((String) value);
      }
      break;

    case STAT_NUMBERS_1:
      if (value == null) {
        unsetStat_numbers_1();
      } else {
        setStat_numbers_1((String) value);
      }
      break;

    case STAT_VALUES_2:
      if (value == null) {
        unsetStat_values_2();
      } else {
        setStat_values_2((String) value);
      }
      break;

    case STAT_NUMBERS_2:
      if (value == null) {
        unsetStat_numbers_2();
      } else {
        setStat_numbers_2((String) value);
      }
      break;

    case STAT_VALUES_3:
      if (value == null) {
        unsetStat_values_3();
      } else {
        setStat_values_3((String) value);
      }
      break;

    case STAT_NUMBERS_3:
      if (value == null) {
        unsetStat_numbers_3();
      } else {
        setStat_numbers_3((String) value);
      }
      break;

    case STAT_NUMBER_1_TYPE:
      if (value == null) {
        unsetStat_number_1_type();
      } else {
        setStat_number_1_type((Integer) value);
      }
      break;

    case STAT_NUMBER_2_TYPE:
      if (value == null) {
        unsetStat_number_2_type();
      } else {
        setStat_number_2_type((Integer) value);
      }
      break;

    case STAT_NUMBER_3_TYPE:
      if (value == null) {
        unsetStat_number_3_type();
      } else {
        setStat_number_3_type((Integer) value);
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

    case STAT_FIELD_NAME:
      return getStat_field_name();

    case STAT_NULLFAC:
      return Double.valueOf(getStat_nullfac());

    case STAT_AVG_FIELD_WIDTH:
      return Integer.valueOf(getStat_avg_field_width());

    case STAT_DISTINCT_VALUES:
      return Double.valueOf(getStat_distinct_values());

    case STAT_VALUES_1:
      return getStat_values_1();

    case STAT_NUMBERS_1:
      return getStat_numbers_1();

    case STAT_VALUES_2:
      return getStat_values_2();

    case STAT_NUMBERS_2:
      return getStat_numbers_2();

    case STAT_VALUES_3:
      return getStat_values_3();

    case STAT_NUMBERS_3:
      return getStat_numbers_3();

    case STAT_NUMBER_1_TYPE:
      return Integer.valueOf(getStat_number_1_type());

    case STAT_NUMBER_2_TYPE:
      return Integer.valueOf(getStat_number_2_type());

    case STAT_NUMBER_3_TYPE:
      return Integer.valueOf(getStat_number_3_type());

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
    case STAT_FIELD_NAME:
      return isSetStat_field_name();
    case STAT_NULLFAC:
      return isSetStat_nullfac();
    case STAT_AVG_FIELD_WIDTH:
      return isSetStat_avg_field_width();
    case STAT_DISTINCT_VALUES:
      return isSetStat_distinct_values();
    case STAT_VALUES_1:
      return isSetStat_values_1();
    case STAT_NUMBERS_1:
      return isSetStat_numbers_1();
    case STAT_VALUES_2:
      return isSetStat_values_2();
    case STAT_NUMBERS_2:
      return isSetStat_numbers_2();
    case STAT_VALUES_3:
      return isSetStat_values_3();
    case STAT_NUMBERS_3:
      return isSetStat_numbers_3();
    case STAT_NUMBER_1_TYPE:
      return isSetStat_number_1_type();
    case STAT_NUMBER_2_TYPE:
      return isSetStat_number_2_type();
    case STAT_NUMBER_3_TYPE:
      return isSetStat_number_3_type();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof tdw_sys_fields_statistics)
      return this.equals((tdw_sys_fields_statistics) that);
    return false;
  }

  public boolean equals(tdw_sys_fields_statistics that) {
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

    boolean this_present_stat_field_name = true && this.isSetStat_field_name();
    boolean that_present_stat_field_name = true && that.isSetStat_field_name();
    if (this_present_stat_field_name || that_present_stat_field_name) {
      if (!(this_present_stat_field_name && that_present_stat_field_name))
        return false;
      if (!this.stat_field_name.equals(that.stat_field_name))
        return false;
    }

    boolean this_present_stat_nullfac = true;
    boolean that_present_stat_nullfac = true;
    if (this_present_stat_nullfac || that_present_stat_nullfac) {
      if (!(this_present_stat_nullfac && that_present_stat_nullfac))
        return false;
      if (this.stat_nullfac != that.stat_nullfac)
        return false;
    }

    boolean this_present_stat_avg_field_width = true;
    boolean that_present_stat_avg_field_width = true;
    if (this_present_stat_avg_field_width || that_present_stat_avg_field_width) {
      if (!(this_present_stat_avg_field_width && that_present_stat_avg_field_width))
        return false;
      if (this.stat_avg_field_width != that.stat_avg_field_width)
        return false;
    }

    boolean this_present_stat_distinct_values = true;
    boolean that_present_stat_distinct_values = true;
    if (this_present_stat_distinct_values || that_present_stat_distinct_values) {
      if (!(this_present_stat_distinct_values && that_present_stat_distinct_values))
        return false;
      if (this.stat_distinct_values != that.stat_distinct_values)
        return false;
    }

    boolean this_present_stat_values_1 = true && this.isSetStat_values_1();
    boolean that_present_stat_values_1 = true && that.isSetStat_values_1();
    if (this_present_stat_values_1 || that_present_stat_values_1) {
      if (!(this_present_stat_values_1 && that_present_stat_values_1))
        return false;
      if (!this.stat_values_1.equals(that.stat_values_1))
        return false;
    }

    boolean this_present_stat_numbers_1 = true && this.isSetStat_numbers_1();
    boolean that_present_stat_numbers_1 = true && that.isSetStat_numbers_1();
    if (this_present_stat_numbers_1 || that_present_stat_numbers_1) {
      if (!(this_present_stat_numbers_1 && that_present_stat_numbers_1))
        return false;
      if (!this.stat_numbers_1.equals(that.stat_numbers_1))
        return false;
    }

    boolean this_present_stat_values_2 = true && this.isSetStat_values_2();
    boolean that_present_stat_values_2 = true && that.isSetStat_values_2();
    if (this_present_stat_values_2 || that_present_stat_values_2) {
      if (!(this_present_stat_values_2 && that_present_stat_values_2))
        return false;
      if (!this.stat_values_2.equals(that.stat_values_2))
        return false;
    }

    boolean this_present_stat_numbers_2 = true && this.isSetStat_numbers_2();
    boolean that_present_stat_numbers_2 = true && that.isSetStat_numbers_2();
    if (this_present_stat_numbers_2 || that_present_stat_numbers_2) {
      if (!(this_present_stat_numbers_2 && that_present_stat_numbers_2))
        return false;
      if (!this.stat_numbers_2.equals(that.stat_numbers_2))
        return false;
    }

    boolean this_present_stat_values_3 = true && this.isSetStat_values_3();
    boolean that_present_stat_values_3 = true && that.isSetStat_values_3();
    if (this_present_stat_values_3 || that_present_stat_values_3) {
      if (!(this_present_stat_values_3 && that_present_stat_values_3))
        return false;
      if (!this.stat_values_3.equals(that.stat_values_3))
        return false;
    }

    boolean this_present_stat_numbers_3 = true && this.isSetStat_numbers_3();
    boolean that_present_stat_numbers_3 = true && that.isSetStat_numbers_3();
    if (this_present_stat_numbers_3 || that_present_stat_numbers_3) {
      if (!(this_present_stat_numbers_3 && that_present_stat_numbers_3))
        return false;
      if (!this.stat_numbers_3.equals(that.stat_numbers_3))
        return false;
    }

    boolean this_present_stat_number_1_type = true;
    boolean that_present_stat_number_1_type = true;
    if (this_present_stat_number_1_type || that_present_stat_number_1_type) {
      if (!(this_present_stat_number_1_type && that_present_stat_number_1_type))
        return false;
      if (this.stat_number_1_type != that.stat_number_1_type)
        return false;
    }

    boolean this_present_stat_number_2_type = true;
    boolean that_present_stat_number_2_type = true;
    if (this_present_stat_number_2_type || that_present_stat_number_2_type) {
      if (!(this_present_stat_number_2_type && that_present_stat_number_2_type))
        return false;
      if (this.stat_number_2_type != that.stat_number_2_type)
        return false;
    }

    boolean this_present_stat_number_3_type = true;
    boolean that_present_stat_number_3_type = true;
    if (this_present_stat_number_3_type || that_present_stat_number_3_type) {
      if (!(this_present_stat_number_3_type && that_present_stat_number_3_type))
        return false;
      if (this.stat_number_3_type != that.stat_number_3_type)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(tdw_sys_fields_statistics other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    tdw_sys_fields_statistics typedOther = (tdw_sys_fields_statistics) other;

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
    lastComparison = Boolean.valueOf(isSetStat_field_name()).compareTo(
        typedOther.isSetStat_field_name());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStat_field_name()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.stat_field_name, typedOther.stat_field_name);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStat_nullfac()).compareTo(
        typedOther.isSetStat_nullfac());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStat_nullfac()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.stat_nullfac, typedOther.stat_nullfac);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStat_avg_field_width()).compareTo(
        typedOther.isSetStat_avg_field_width());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStat_avg_field_width()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.stat_avg_field_width, typedOther.stat_avg_field_width);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStat_distinct_values()).compareTo(
        typedOther.isSetStat_distinct_values());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStat_distinct_values()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.stat_distinct_values, typedOther.stat_distinct_values);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStat_values_1()).compareTo(
        typedOther.isSetStat_values_1());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStat_values_1()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.stat_values_1, typedOther.stat_values_1);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStat_numbers_1()).compareTo(
        typedOther.isSetStat_numbers_1());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStat_numbers_1()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.stat_numbers_1, typedOther.stat_numbers_1);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStat_values_2()).compareTo(
        typedOther.isSetStat_values_2());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStat_values_2()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.stat_values_2, typedOther.stat_values_2);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStat_numbers_2()).compareTo(
        typedOther.isSetStat_numbers_2());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStat_numbers_2()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.stat_numbers_2, typedOther.stat_numbers_2);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStat_values_3()).compareTo(
        typedOther.isSetStat_values_3());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStat_values_3()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.stat_values_3, typedOther.stat_values_3);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStat_numbers_3()).compareTo(
        typedOther.isSetStat_numbers_3());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStat_numbers_3()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.stat_numbers_3, typedOther.stat_numbers_3);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStat_number_1_type()).compareTo(
        typedOther.isSetStat_number_1_type());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStat_number_1_type()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.stat_number_1_type, typedOther.stat_number_1_type);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStat_number_2_type()).compareTo(
        typedOther.isSetStat_number_2_type());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStat_number_2_type()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.stat_number_2_type, typedOther.stat_number_2_type);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStat_number_3_type()).compareTo(
        typedOther.isSetStat_number_3_type());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStat_number_3_type()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.stat_number_3_type, typedOther.stat_number_3_type);
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
    StringBuilder sb = new StringBuilder("tdw_sys_fields_statistics(");
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
    sb.append("stat_field_name:");
    if (this.stat_field_name == null) {
      sb.append("null");
    } else {
      sb.append(this.stat_field_name);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("stat_nullfac:");
    sb.append(this.stat_nullfac);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("stat_avg_field_width:");
    sb.append(this.stat_avg_field_width);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("stat_distinct_values:");
    sb.append(this.stat_distinct_values);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("stat_values_1:");
    if (this.stat_values_1 == null) {
      sb.append("null");
    } else {
      sb.append(this.stat_values_1);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("stat_numbers_1:");
    if (this.stat_numbers_1 == null) {
      sb.append("null");
    } else {
      sb.append(this.stat_numbers_1);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("stat_values_2:");
    if (this.stat_values_2 == null) {
      sb.append("null");
    } else {
      sb.append(this.stat_values_2);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("stat_numbers_2:");
    if (this.stat_numbers_2 == null) {
      sb.append("null");
    } else {
      sb.append(this.stat_numbers_2);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("stat_values_3:");
    if (this.stat_values_3 == null) {
      sb.append("null");
    } else {
      sb.append(this.stat_values_3);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("stat_numbers_3:");
    if (this.stat_numbers_3 == null) {
      sb.append("null");
    } else {
      sb.append(this.stat_numbers_3);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("stat_number_1_type:");
    sb.append(this.stat_number_1_type);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("stat_number_2_type:");
    sb.append(this.stat_number_2_type);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("stat_number_3_type:");
    sb.append(this.stat_number_3_type);
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

  private static class tdw_sys_fields_statisticsStandardSchemeFactory implements
      SchemeFactory {
    public tdw_sys_fields_statisticsStandardScheme getScheme() {
      return new tdw_sys_fields_statisticsStandardScheme();
    }
  }

  private static class tdw_sys_fields_statisticsStandardScheme extends
      StandardScheme<tdw_sys_fields_statistics> {

    public void read(org.apache.thrift.protocol.TProtocol iprot,
        tdw_sys_fields_statistics struct) throws org.apache.thrift.TException {
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
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.stat_field_name = iprot.readString();
            struct.setStat_field_nameIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 4:
          if (schemeField.type == org.apache.thrift.protocol.TType.DOUBLE) {
            struct.stat_nullfac = iprot.readDouble();
            struct.setStat_nullfacIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 5:
          if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
            struct.stat_avg_field_width = iprot.readI32();
            struct.setStat_avg_field_widthIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 6:
          if (schemeField.type == org.apache.thrift.protocol.TType.DOUBLE) {
            struct.stat_distinct_values = iprot.readDouble();
            struct.setStat_distinct_valuesIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 7:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.stat_values_1 = iprot.readString();
            struct.setStat_values_1IsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 8:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.stat_numbers_1 = iprot.readString();
            struct.setStat_numbers_1IsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 9:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.stat_values_2 = iprot.readString();
            struct.setStat_values_2IsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 10:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.stat_numbers_2 = iprot.readString();
            struct.setStat_numbers_2IsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 11:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.stat_values_3 = iprot.readString();
            struct.setStat_values_3IsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 12:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.stat_numbers_3 = iprot.readString();
            struct.setStat_numbers_3IsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 13:
          if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
            struct.stat_number_1_type = iprot.readI32();
            struct.setStat_number_1_typeIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 14:
          if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
            struct.stat_number_2_type = iprot.readI32();
            struct.setStat_number_2_typeIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 15:
          if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
            struct.stat_number_3_type = iprot.readI32();
            struct.setStat_number_3_typeIsSet(true);
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
        tdw_sys_fields_statistics struct) throws org.apache.thrift.TException {
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
      if (struct.stat_field_name != null) {
        oprot.writeFieldBegin(STAT_FIELD_NAME_FIELD_DESC);
        oprot.writeString(struct.stat_field_name);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(STAT_NULLFAC_FIELD_DESC);
      oprot.writeDouble(struct.stat_nullfac);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(STAT_AVG_FIELD_WIDTH_FIELD_DESC);
      oprot.writeI32(struct.stat_avg_field_width);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(STAT_DISTINCT_VALUES_FIELD_DESC);
      oprot.writeDouble(struct.stat_distinct_values);
      oprot.writeFieldEnd();
      if (struct.stat_values_1 != null) {
        oprot.writeFieldBegin(STAT_VALUES_1_FIELD_DESC);
        oprot.writeString(struct.stat_values_1);
        oprot.writeFieldEnd();
      }
      if (struct.stat_numbers_1 != null) {
        oprot.writeFieldBegin(STAT_NUMBERS_1_FIELD_DESC);
        oprot.writeString(struct.stat_numbers_1);
        oprot.writeFieldEnd();
      }
      if (struct.stat_values_2 != null) {
        oprot.writeFieldBegin(STAT_VALUES_2_FIELD_DESC);
        oprot.writeString(struct.stat_values_2);
        oprot.writeFieldEnd();
      }
      if (struct.stat_numbers_2 != null) {
        oprot.writeFieldBegin(STAT_NUMBERS_2_FIELD_DESC);
        oprot.writeString(struct.stat_numbers_2);
        oprot.writeFieldEnd();
      }
      if (struct.stat_values_3 != null) {
        oprot.writeFieldBegin(STAT_VALUES_3_FIELD_DESC);
        oprot.writeString(struct.stat_values_3);
        oprot.writeFieldEnd();
      }
      if (struct.stat_numbers_3 != null) {
        oprot.writeFieldBegin(STAT_NUMBERS_3_FIELD_DESC);
        oprot.writeString(struct.stat_numbers_3);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(STAT_NUMBER_1_TYPE_FIELD_DESC);
      oprot.writeI32(struct.stat_number_1_type);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(STAT_NUMBER_2_TYPE_FIELD_DESC);
      oprot.writeI32(struct.stat_number_2_type);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(STAT_NUMBER_3_TYPE_FIELD_DESC);
      oprot.writeI32(struct.stat_number_3_type);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class tdw_sys_fields_statisticsTupleSchemeFactory implements
      SchemeFactory {
    public tdw_sys_fields_statisticsTupleScheme getScheme() {
      return new tdw_sys_fields_statisticsTupleScheme();
    }
  }

  private static class tdw_sys_fields_statisticsTupleScheme extends
      TupleScheme<tdw_sys_fields_statistics> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot,
        tdw_sys_fields_statistics struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetStat_table_name()) {
        optionals.set(0);
      }
      if (struct.isSetStat_db_name()) {
        optionals.set(1);
      }
      if (struct.isSetStat_field_name()) {
        optionals.set(2);
      }
      if (struct.isSetStat_nullfac()) {
        optionals.set(3);
      }
      if (struct.isSetStat_avg_field_width()) {
        optionals.set(4);
      }
      if (struct.isSetStat_distinct_values()) {
        optionals.set(5);
      }
      if (struct.isSetStat_values_1()) {
        optionals.set(6);
      }
      if (struct.isSetStat_numbers_1()) {
        optionals.set(7);
      }
      if (struct.isSetStat_values_2()) {
        optionals.set(8);
      }
      if (struct.isSetStat_numbers_2()) {
        optionals.set(9);
      }
      if (struct.isSetStat_values_3()) {
        optionals.set(10);
      }
      if (struct.isSetStat_numbers_3()) {
        optionals.set(11);
      }
      if (struct.isSetStat_number_1_type()) {
        optionals.set(12);
      }
      if (struct.isSetStat_number_2_type()) {
        optionals.set(13);
      }
      if (struct.isSetStat_number_3_type()) {
        optionals.set(14);
      }
      oprot.writeBitSet(optionals, 15);
      if (struct.isSetStat_table_name()) {
        oprot.writeString(struct.stat_table_name);
      }
      if (struct.isSetStat_db_name()) {
        oprot.writeString(struct.stat_db_name);
      }
      if (struct.isSetStat_field_name()) {
        oprot.writeString(struct.stat_field_name);
      }
      if (struct.isSetStat_nullfac()) {
        oprot.writeDouble(struct.stat_nullfac);
      }
      if (struct.isSetStat_avg_field_width()) {
        oprot.writeI32(struct.stat_avg_field_width);
      }
      if (struct.isSetStat_distinct_values()) {
        oprot.writeDouble(struct.stat_distinct_values);
      }
      if (struct.isSetStat_values_1()) {
        oprot.writeString(struct.stat_values_1);
      }
      if (struct.isSetStat_numbers_1()) {
        oprot.writeString(struct.stat_numbers_1);
      }
      if (struct.isSetStat_values_2()) {
        oprot.writeString(struct.stat_values_2);
      }
      if (struct.isSetStat_numbers_2()) {
        oprot.writeString(struct.stat_numbers_2);
      }
      if (struct.isSetStat_values_3()) {
        oprot.writeString(struct.stat_values_3);
      }
      if (struct.isSetStat_numbers_3()) {
        oprot.writeString(struct.stat_numbers_3);
      }
      if (struct.isSetStat_number_1_type()) {
        oprot.writeI32(struct.stat_number_1_type);
      }
      if (struct.isSetStat_number_2_type()) {
        oprot.writeI32(struct.stat_number_2_type);
      }
      if (struct.isSetStat_number_3_type()) {
        oprot.writeI32(struct.stat_number_3_type);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot,
        tdw_sys_fields_statistics struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(15);
      if (incoming.get(0)) {
        struct.stat_table_name = iprot.readString();
        struct.setStat_table_nameIsSet(true);
      }
      if (incoming.get(1)) {
        struct.stat_db_name = iprot.readString();
        struct.setStat_db_nameIsSet(true);
      }
      if (incoming.get(2)) {
        struct.stat_field_name = iprot.readString();
        struct.setStat_field_nameIsSet(true);
      }
      if (incoming.get(3)) {
        struct.stat_nullfac = iprot.readDouble();
        struct.setStat_nullfacIsSet(true);
      }
      if (incoming.get(4)) {
        struct.stat_avg_field_width = iprot.readI32();
        struct.setStat_avg_field_widthIsSet(true);
      }
      if (incoming.get(5)) {
        struct.stat_distinct_values = iprot.readDouble();
        struct.setStat_distinct_valuesIsSet(true);
      }
      if (incoming.get(6)) {
        struct.stat_values_1 = iprot.readString();
        struct.setStat_values_1IsSet(true);
      }
      if (incoming.get(7)) {
        struct.stat_numbers_1 = iprot.readString();
        struct.setStat_numbers_1IsSet(true);
      }
      if (incoming.get(8)) {
        struct.stat_values_2 = iprot.readString();
        struct.setStat_values_2IsSet(true);
      }
      if (incoming.get(9)) {
        struct.stat_numbers_2 = iprot.readString();
        struct.setStat_numbers_2IsSet(true);
      }
      if (incoming.get(10)) {
        struct.stat_values_3 = iprot.readString();
        struct.setStat_values_3IsSet(true);
      }
      if (incoming.get(11)) {
        struct.stat_numbers_3 = iprot.readString();
        struct.setStat_numbers_3IsSet(true);
      }
      if (incoming.get(12)) {
        struct.stat_number_1_type = iprot.readI32();
        struct.setStat_number_1_typeIsSet(true);
      }
      if (incoming.get(13)) {
        struct.stat_number_2_type = iprot.readI32();
        struct.setStat_number_2_typeIsSet(true);
      }
      if (incoming.get(14)) {
        struct.stat_number_3_type = iprot.readI32();
        struct.setStat_number_3_typeIsSet(true);
      }
    }
  }

}
