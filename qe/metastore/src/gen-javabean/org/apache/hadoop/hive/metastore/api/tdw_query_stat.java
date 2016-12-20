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

public class tdw_query_stat implements
    org.apache.thrift.TBase<tdw_query_stat, tdw_query_stat._Fields>,
    java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct(
      "tdw_query_stat");

  private static final org.apache.thrift.protocol.TField QUERY_ID_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "queryId", org.apache.thrift.protocol.TType.STRING, (short) 1);
  private static final org.apache.thrift.protocol.TField CURR_MRINDEX_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "currMRIndex", org.apache.thrift.protocol.TType.I32, (short) 2);
  private static final org.apache.thrift.protocol.TField CURR_MRID_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "currMRId", org.apache.thrift.protocol.TType.STRING, (short) 3);
  private static final org.apache.thrift.protocol.TField CURR_MRSTART_TIME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "currMRStartTime", org.apache.thrift.protocol.TType.STRING, (short) 4);
  private static final org.apache.thrift.protocol.TField CURR_MRFINISH_TIME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "currMRFinishTime", org.apache.thrift.protocol.TType.STRING, (short) 5);
  private static final org.apache.thrift.protocol.TField CURR_MRSTATE_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "currMRState", org.apache.thrift.protocol.TType.STRING, (short) 6);
  private static final org.apache.thrift.protocol.TField MAP_NUM_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "MapNum", org.apache.thrift.protocol.TType.I32, (short) 7);
  private static final org.apache.thrift.protocol.TField REDUCE_NUM_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "ReduceNum", org.apache.thrift.protocol.TType.I32, (short) 8);
  private static final org.apache.thrift.protocol.TField JT_IP_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "jtIP", org.apache.thrift.protocol.TType.STRING, (short) 9);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes
        .put(StandardScheme.class, new tdw_query_statStandardSchemeFactory());
    schemes.put(TupleScheme.class, new tdw_query_statTupleSchemeFactory());
  }

  private String queryId;
  private int currMRIndex;
  private String currMRId;
  private String currMRStartTime;
  private String currMRFinishTime;
  private String currMRState;
  private int MapNum;
  private int ReduceNum;
  private String jtIP;

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    QUERY_ID((short) 1, "queryId"), CURR_MRINDEX((short) 2, "currMRIndex"), CURR_MRID(
        (short) 3, "currMRId"), CURR_MRSTART_TIME((short) 4, "currMRStartTime"), CURR_MRFINISH_TIME(
        (short) 5, "currMRFinishTime"), CURR_MRSTATE((short) 6, "currMRState"), MAP_NUM(
        (short) 7, "MapNum"), REDUCE_NUM((short) 8, "ReduceNum"), JT_IP(
        (short) 9, "jtIP");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    public static _Fields findByThriftId(int fieldId) {
      switch (fieldId) {
      case 1:
        return QUERY_ID;
      case 2:
        return CURR_MRINDEX;
      case 3:
        return CURR_MRID;
      case 4:
        return CURR_MRSTART_TIME;
      case 5:
        return CURR_MRFINISH_TIME;
      case 6:
        return CURR_MRSTATE;
      case 7:
        return MAP_NUM;
      case 8:
        return REDUCE_NUM;
      case 9:
        return JT_IP;
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

  private static final int __CURRMRINDEX_ISSET_ID = 0;
  private static final int __MAPNUM_ISSET_ID = 1;
  private static final int __REDUCENUM_ISSET_ID = 2;
  private BitSet __isset_bit_vector = new BitSet(3);
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(
        _Fields.class);
    tmpMap.put(_Fields.QUERY_ID, new org.apache.thrift.meta_data.FieldMetaData(
        "queryId", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.CURR_MRINDEX,
        new org.apache.thrift.meta_data.FieldMetaData("currMRIndex",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.CURR_MRID,
        new org.apache.thrift.meta_data.FieldMetaData("currMRId",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.CURR_MRSTART_TIME,
        new org.apache.thrift.meta_data.FieldMetaData("currMRStartTime",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.CURR_MRFINISH_TIME,
        new org.apache.thrift.meta_data.FieldMetaData("currMRFinishTime",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.CURR_MRSTATE,
        new org.apache.thrift.meta_data.FieldMetaData("currMRState",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.MAP_NUM, new org.apache.thrift.meta_data.FieldMetaData(
        "MapNum", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.REDUCE_NUM,
        new org.apache.thrift.meta_data.FieldMetaData("ReduceNum",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.JT_IP, new org.apache.thrift.meta_data.FieldMetaData(
        "jtIP", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(
        tdw_query_stat.class, metaDataMap);
  }

  public tdw_query_stat() {
  }

  public tdw_query_stat(String queryId, int currMRIndex, String currMRId,
      String currMRStartTime, String currMRFinishTime, String currMRState,
      int MapNum, int ReduceNum, String jtIP) {
    this();
    this.queryId = queryId;
    this.currMRIndex = currMRIndex;
    setCurrMRIndexIsSet(true);
    this.currMRId = currMRId;
    this.currMRStartTime = currMRStartTime;
    this.currMRFinishTime = currMRFinishTime;
    this.currMRState = currMRState;
    this.MapNum = MapNum;
    setMapNumIsSet(true);
    this.ReduceNum = ReduceNum;
    setReduceNumIsSet(true);
    this.jtIP = jtIP;
  }

  public tdw_query_stat(tdw_query_stat other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    if (other.isSetQueryId()) {
      this.queryId = other.queryId;
    }
    this.currMRIndex = other.currMRIndex;
    if (other.isSetCurrMRId()) {
      this.currMRId = other.currMRId;
    }
    if (other.isSetCurrMRStartTime()) {
      this.currMRStartTime = other.currMRStartTime;
    }
    if (other.isSetCurrMRFinishTime()) {
      this.currMRFinishTime = other.currMRFinishTime;
    }
    if (other.isSetCurrMRState()) {
      this.currMRState = other.currMRState;
    }
    this.MapNum = other.MapNum;
    this.ReduceNum = other.ReduceNum;
    if (other.isSetJtIP()) {
      this.jtIP = other.jtIP;
    }
  }

  public tdw_query_stat deepCopy() {
    return new tdw_query_stat(this);
  }

  @Override
  public void clear() {
    this.queryId = null;
    setCurrMRIndexIsSet(false);
    this.currMRIndex = 0;
    this.currMRId = null;
    this.currMRStartTime = null;
    this.currMRFinishTime = null;
    this.currMRState = null;
    setMapNumIsSet(false);
    this.MapNum = 0;
    setReduceNumIsSet(false);
    this.ReduceNum = 0;
    this.jtIP = null;
  }

  public String getQueryId() {
    return this.queryId;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  public void unsetQueryId() {
    this.queryId = null;
  }

  public boolean isSetQueryId() {
    return this.queryId != null;
  }

  public void setQueryIdIsSet(boolean value) {
    if (!value) {
      this.queryId = null;
    }
  }

  public int getCurrMRIndex() {
    return this.currMRIndex;
  }

  public void setCurrMRIndex(int currMRIndex) {
    this.currMRIndex = currMRIndex;
    setCurrMRIndexIsSet(true);
  }

  public void unsetCurrMRIndex() {
    __isset_bit_vector.clear(__CURRMRINDEX_ISSET_ID);
  }

  public boolean isSetCurrMRIndex() {
    return __isset_bit_vector.get(__CURRMRINDEX_ISSET_ID);
  }

  public void setCurrMRIndexIsSet(boolean value) {
    __isset_bit_vector.set(__CURRMRINDEX_ISSET_ID, value);
  }

  public String getCurrMRId() {
    return this.currMRId;
  }

  public void setCurrMRId(String currMRId) {
    this.currMRId = currMRId;
  }

  public void unsetCurrMRId() {
    this.currMRId = null;
  }

  public boolean isSetCurrMRId() {
    return this.currMRId != null;
  }

  public void setCurrMRIdIsSet(boolean value) {
    if (!value) {
      this.currMRId = null;
    }
  }

  public String getCurrMRStartTime() {
    return this.currMRStartTime;
  }

  public void setCurrMRStartTime(String currMRStartTime) {
    this.currMRStartTime = currMRStartTime;
  }

  public void unsetCurrMRStartTime() {
    this.currMRStartTime = null;
  }

  public boolean isSetCurrMRStartTime() {
    return this.currMRStartTime != null;
  }

  public void setCurrMRStartTimeIsSet(boolean value) {
    if (!value) {
      this.currMRStartTime = null;
    }
  }

  public String getCurrMRFinishTime() {
    return this.currMRFinishTime;
  }

  public void setCurrMRFinishTime(String currMRFinishTime) {
    this.currMRFinishTime = currMRFinishTime;
  }

  public void unsetCurrMRFinishTime() {
    this.currMRFinishTime = null;
  }

  public boolean isSetCurrMRFinishTime() {
    return this.currMRFinishTime != null;
  }

  public void setCurrMRFinishTimeIsSet(boolean value) {
    if (!value) {
      this.currMRFinishTime = null;
    }
  }

  public String getCurrMRState() {
    return this.currMRState;
  }

  public void setCurrMRState(String currMRState) {
    this.currMRState = currMRState;
  }

  public void unsetCurrMRState() {
    this.currMRState = null;
  }

  public boolean isSetCurrMRState() {
    return this.currMRState != null;
  }

  public void setCurrMRStateIsSet(boolean value) {
    if (!value) {
      this.currMRState = null;
    }
  }

  public int getMapNum() {
    return this.MapNum;
  }

  public void setMapNum(int MapNum) {
    this.MapNum = MapNum;
    setMapNumIsSet(true);
  }

  public void unsetMapNum() {
    __isset_bit_vector.clear(__MAPNUM_ISSET_ID);
  }

  public boolean isSetMapNum() {
    return __isset_bit_vector.get(__MAPNUM_ISSET_ID);
  }

  public void setMapNumIsSet(boolean value) {
    __isset_bit_vector.set(__MAPNUM_ISSET_ID, value);
  }

  public int getReduceNum() {
    return this.ReduceNum;
  }

  public void setReduceNum(int ReduceNum) {
    this.ReduceNum = ReduceNum;
    setReduceNumIsSet(true);
  }

  public void unsetReduceNum() {
    __isset_bit_vector.clear(__REDUCENUM_ISSET_ID);
  }

  public boolean isSetReduceNum() {
    return __isset_bit_vector.get(__REDUCENUM_ISSET_ID);
  }

  public void setReduceNumIsSet(boolean value) {
    __isset_bit_vector.set(__REDUCENUM_ISSET_ID, value);
  }

  public String getJtIP() {
    return this.jtIP;
  }

  public void setJtIP(String jtIP) {
    this.jtIP = jtIP;
  }

  public void unsetJtIP() {
    this.jtIP = null;
  }

  public boolean isSetJtIP() {
    return this.jtIP != null;
  }

  public void setJtIPIsSet(boolean value) {
    if (!value) {
      this.jtIP = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case QUERY_ID:
      if (value == null) {
        unsetQueryId();
      } else {
        setQueryId((String) value);
      }
      break;

    case CURR_MRINDEX:
      if (value == null) {
        unsetCurrMRIndex();
      } else {
        setCurrMRIndex((Integer) value);
      }
      break;

    case CURR_MRID:
      if (value == null) {
        unsetCurrMRId();
      } else {
        setCurrMRId((String) value);
      }
      break;

    case CURR_MRSTART_TIME:
      if (value == null) {
        unsetCurrMRStartTime();
      } else {
        setCurrMRStartTime((String) value);
      }
      break;

    case CURR_MRFINISH_TIME:
      if (value == null) {
        unsetCurrMRFinishTime();
      } else {
        setCurrMRFinishTime((String) value);
      }
      break;

    case CURR_MRSTATE:
      if (value == null) {
        unsetCurrMRState();
      } else {
        setCurrMRState((String) value);
      }
      break;

    case MAP_NUM:
      if (value == null) {
        unsetMapNum();
      } else {
        setMapNum((Integer) value);
      }
      break;

    case REDUCE_NUM:
      if (value == null) {
        unsetReduceNum();
      } else {
        setReduceNum((Integer) value);
      }
      break;

    case JT_IP:
      if (value == null) {
        unsetJtIP();
      } else {
        setJtIP((String) value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case QUERY_ID:
      return getQueryId();

    case CURR_MRINDEX:
      return Integer.valueOf(getCurrMRIndex());

    case CURR_MRID:
      return getCurrMRId();

    case CURR_MRSTART_TIME:
      return getCurrMRStartTime();

    case CURR_MRFINISH_TIME:
      return getCurrMRFinishTime();

    case CURR_MRSTATE:
      return getCurrMRState();

    case MAP_NUM:
      return Integer.valueOf(getMapNum());

    case REDUCE_NUM:
      return Integer.valueOf(getReduceNum());

    case JT_IP:
      return getJtIP();

    }
    throw new IllegalStateException();
  }

  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case QUERY_ID:
      return isSetQueryId();
    case CURR_MRINDEX:
      return isSetCurrMRIndex();
    case CURR_MRID:
      return isSetCurrMRId();
    case CURR_MRSTART_TIME:
      return isSetCurrMRStartTime();
    case CURR_MRFINISH_TIME:
      return isSetCurrMRFinishTime();
    case CURR_MRSTATE:
      return isSetCurrMRState();
    case MAP_NUM:
      return isSetMapNum();
    case REDUCE_NUM:
      return isSetReduceNum();
    case JT_IP:
      return isSetJtIP();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof tdw_query_stat)
      return this.equals((tdw_query_stat) that);
    return false;
  }

  public boolean equals(tdw_query_stat that) {
    if (that == null)
      return false;

    boolean this_present_queryId = true && this.isSetQueryId();
    boolean that_present_queryId = true && that.isSetQueryId();
    if (this_present_queryId || that_present_queryId) {
      if (!(this_present_queryId && that_present_queryId))
        return false;
      if (!this.queryId.equals(that.queryId))
        return false;
    }

    boolean this_present_currMRIndex = true;
    boolean that_present_currMRIndex = true;
    if (this_present_currMRIndex || that_present_currMRIndex) {
      if (!(this_present_currMRIndex && that_present_currMRIndex))
        return false;
      if (this.currMRIndex != that.currMRIndex)
        return false;
    }

    boolean this_present_currMRId = true && this.isSetCurrMRId();
    boolean that_present_currMRId = true && that.isSetCurrMRId();
    if (this_present_currMRId || that_present_currMRId) {
      if (!(this_present_currMRId && that_present_currMRId))
        return false;
      if (!this.currMRId.equals(that.currMRId))
        return false;
    }

    boolean this_present_currMRStartTime = true && this.isSetCurrMRStartTime();
    boolean that_present_currMRStartTime = true && that.isSetCurrMRStartTime();
    if (this_present_currMRStartTime || that_present_currMRStartTime) {
      if (!(this_present_currMRStartTime && that_present_currMRStartTime))
        return false;
      if (!this.currMRStartTime.equals(that.currMRStartTime))
        return false;
    }

    boolean this_present_currMRFinishTime = true && this
        .isSetCurrMRFinishTime();
    boolean that_present_currMRFinishTime = true && that
        .isSetCurrMRFinishTime();
    if (this_present_currMRFinishTime || that_present_currMRFinishTime) {
      if (!(this_present_currMRFinishTime && that_present_currMRFinishTime))
        return false;
      if (!this.currMRFinishTime.equals(that.currMRFinishTime))
        return false;
    }

    boolean this_present_currMRState = true && this.isSetCurrMRState();
    boolean that_present_currMRState = true && that.isSetCurrMRState();
    if (this_present_currMRState || that_present_currMRState) {
      if (!(this_present_currMRState && that_present_currMRState))
        return false;
      if (!this.currMRState.equals(that.currMRState))
        return false;
    }

    boolean this_present_MapNum = true;
    boolean that_present_MapNum = true;
    if (this_present_MapNum || that_present_MapNum) {
      if (!(this_present_MapNum && that_present_MapNum))
        return false;
      if (this.MapNum != that.MapNum)
        return false;
    }

    boolean this_present_ReduceNum = true;
    boolean that_present_ReduceNum = true;
    if (this_present_ReduceNum || that_present_ReduceNum) {
      if (!(this_present_ReduceNum && that_present_ReduceNum))
        return false;
      if (this.ReduceNum != that.ReduceNum)
        return false;
    }

    boolean this_present_jtIP = true && this.isSetJtIP();
    boolean that_present_jtIP = true && that.isSetJtIP();
    if (this_present_jtIP || that_present_jtIP) {
      if (!(this_present_jtIP && that_present_jtIP))
        return false;
      if (!this.jtIP.equals(that.jtIP))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(tdw_query_stat other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    tdw_query_stat typedOther = (tdw_query_stat) other;

    lastComparison = Boolean.valueOf(isSetQueryId()).compareTo(
        typedOther.isSetQueryId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetQueryId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.queryId,
          typedOther.queryId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetCurrMRIndex()).compareTo(
        typedOther.isSetCurrMRIndex());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCurrMRIndex()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.currMRIndex, typedOther.currMRIndex);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetCurrMRId()).compareTo(
        typedOther.isSetCurrMRId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCurrMRId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.currMRId,
          typedOther.currMRId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetCurrMRStartTime()).compareTo(
        typedOther.isSetCurrMRStartTime());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCurrMRStartTime()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.currMRStartTime, typedOther.currMRStartTime);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetCurrMRFinishTime()).compareTo(
        typedOther.isSetCurrMRFinishTime());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCurrMRFinishTime()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.currMRFinishTime, typedOther.currMRFinishTime);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetCurrMRState()).compareTo(
        typedOther.isSetCurrMRState());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCurrMRState()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.currMRState, typedOther.currMRState);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetMapNum()).compareTo(
        typedOther.isSetMapNum());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMapNum()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.MapNum,
          typedOther.MapNum);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetReduceNum()).compareTo(
        typedOther.isSetReduceNum());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetReduceNum()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.ReduceNum,
          typedOther.ReduceNum);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetJtIP()).compareTo(
        typedOther.isSetJtIP());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetJtIP()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.jtIP,
          typedOther.jtIP);
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
    StringBuilder sb = new StringBuilder("tdw_query_stat(");
    boolean first = true;

    sb.append("queryId:");
    if (this.queryId == null) {
      sb.append("null");
    } else {
      sb.append(this.queryId);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("currMRIndex:");
    sb.append(this.currMRIndex);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("currMRId:");
    if (this.currMRId == null) {
      sb.append("null");
    } else {
      sb.append(this.currMRId);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("currMRStartTime:");
    if (this.currMRStartTime == null) {
      sb.append("null");
    } else {
      sb.append(this.currMRStartTime);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("currMRFinishTime:");
    if (this.currMRFinishTime == null) {
      sb.append("null");
    } else {
      sb.append(this.currMRFinishTime);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("currMRState:");
    if (this.currMRState == null) {
      sb.append("null");
    } else {
      sb.append(this.currMRState);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("MapNum:");
    sb.append(this.MapNum);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("ReduceNum:");
    sb.append(this.ReduceNum);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("jtIP:");
    if (this.jtIP == null) {
      sb.append("null");
    } else {
      sb.append(this.jtIP);
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

  private static class tdw_query_statStandardSchemeFactory implements
      SchemeFactory {
    public tdw_query_statStandardScheme getScheme() {
      return new tdw_query_statStandardScheme();
    }
  }

  private static class tdw_query_statStandardScheme extends
      StandardScheme<tdw_query_stat> {

    public void read(org.apache.thrift.protocol.TProtocol iprot,
        tdw_query_stat struct) throws org.apache.thrift.TException {
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
            struct.queryId = iprot.readString();
            struct.setQueryIdIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 2:
          if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
            struct.currMRIndex = iprot.readI32();
            struct.setCurrMRIndexIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 3:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.currMRId = iprot.readString();
            struct.setCurrMRIdIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 4:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.currMRStartTime = iprot.readString();
            struct.setCurrMRStartTimeIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 5:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.currMRFinishTime = iprot.readString();
            struct.setCurrMRFinishTimeIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 6:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.currMRState = iprot.readString();
            struct.setCurrMRStateIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 7:
          if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
            struct.MapNum = iprot.readI32();
            struct.setMapNumIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 8:
          if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
            struct.ReduceNum = iprot.readI32();
            struct.setReduceNumIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 9:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.jtIP = iprot.readString();
            struct.setJtIPIsSet(true);
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
        tdw_query_stat struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.queryId != null) {
        oprot.writeFieldBegin(QUERY_ID_FIELD_DESC);
        oprot.writeString(struct.queryId);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(CURR_MRINDEX_FIELD_DESC);
      oprot.writeI32(struct.currMRIndex);
      oprot.writeFieldEnd();
      if (struct.currMRId != null) {
        oprot.writeFieldBegin(CURR_MRID_FIELD_DESC);
        oprot.writeString(struct.currMRId);
        oprot.writeFieldEnd();
      }
      if (struct.currMRStartTime != null) {
        oprot.writeFieldBegin(CURR_MRSTART_TIME_FIELD_DESC);
        oprot.writeString(struct.currMRStartTime);
        oprot.writeFieldEnd();
      }
      if (struct.currMRFinishTime != null) {
        oprot.writeFieldBegin(CURR_MRFINISH_TIME_FIELD_DESC);
        oprot.writeString(struct.currMRFinishTime);
        oprot.writeFieldEnd();
      }
      if (struct.currMRState != null) {
        oprot.writeFieldBegin(CURR_MRSTATE_FIELD_DESC);
        oprot.writeString(struct.currMRState);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(MAP_NUM_FIELD_DESC);
      oprot.writeI32(struct.MapNum);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(REDUCE_NUM_FIELD_DESC);
      oprot.writeI32(struct.ReduceNum);
      oprot.writeFieldEnd();
      if (struct.jtIP != null) {
        oprot.writeFieldBegin(JT_IP_FIELD_DESC);
        oprot.writeString(struct.jtIP);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class tdw_query_statTupleSchemeFactory implements
      SchemeFactory {
    public tdw_query_statTupleScheme getScheme() {
      return new tdw_query_statTupleScheme();
    }
  }

  private static class tdw_query_statTupleScheme extends
      TupleScheme<tdw_query_stat> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot,
        tdw_query_stat struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetQueryId()) {
        optionals.set(0);
      }
      if (struct.isSetCurrMRIndex()) {
        optionals.set(1);
      }
      if (struct.isSetCurrMRId()) {
        optionals.set(2);
      }
      if (struct.isSetCurrMRStartTime()) {
        optionals.set(3);
      }
      if (struct.isSetCurrMRFinishTime()) {
        optionals.set(4);
      }
      if (struct.isSetCurrMRState()) {
        optionals.set(5);
      }
      if (struct.isSetMapNum()) {
        optionals.set(6);
      }
      if (struct.isSetReduceNum()) {
        optionals.set(7);
      }
      if (struct.isSetJtIP()) {
        optionals.set(8);
      }
      oprot.writeBitSet(optionals, 9);
      if (struct.isSetQueryId()) {
        oprot.writeString(struct.queryId);
      }
      if (struct.isSetCurrMRIndex()) {
        oprot.writeI32(struct.currMRIndex);
      }
      if (struct.isSetCurrMRId()) {
        oprot.writeString(struct.currMRId);
      }
      if (struct.isSetCurrMRStartTime()) {
        oprot.writeString(struct.currMRStartTime);
      }
      if (struct.isSetCurrMRFinishTime()) {
        oprot.writeString(struct.currMRFinishTime);
      }
      if (struct.isSetCurrMRState()) {
        oprot.writeString(struct.currMRState);
      }
      if (struct.isSetMapNum()) {
        oprot.writeI32(struct.MapNum);
      }
      if (struct.isSetReduceNum()) {
        oprot.writeI32(struct.ReduceNum);
      }
      if (struct.isSetJtIP()) {
        oprot.writeString(struct.jtIP);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot,
        tdw_query_stat struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(9);
      if (incoming.get(0)) {
        struct.queryId = iprot.readString();
        struct.setQueryIdIsSet(true);
      }
      if (incoming.get(1)) {
        struct.currMRIndex = iprot.readI32();
        struct.setCurrMRIndexIsSet(true);
      }
      if (incoming.get(2)) {
        struct.currMRId = iprot.readString();
        struct.setCurrMRIdIsSet(true);
      }
      if (incoming.get(3)) {
        struct.currMRStartTime = iprot.readString();
        struct.setCurrMRStartTimeIsSet(true);
      }
      if (incoming.get(4)) {
        struct.currMRFinishTime = iprot.readString();
        struct.setCurrMRFinishTimeIsSet(true);
      }
      if (incoming.get(5)) {
        struct.currMRState = iprot.readString();
        struct.setCurrMRStateIsSet(true);
      }
      if (incoming.get(6)) {
        struct.MapNum = iprot.readI32();
        struct.setMapNumIsSet(true);
      }
      if (incoming.get(7)) {
        struct.ReduceNum = iprot.readI32();
        struct.setReduceNumIsSet(true);
      }
      if (incoming.get(8)) {
        struct.jtIP = iprot.readString();
        struct.setJtIPIsSet(true);
      }
    }
  }

}
