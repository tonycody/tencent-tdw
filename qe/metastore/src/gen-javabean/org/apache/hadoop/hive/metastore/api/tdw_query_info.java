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

public class tdw_query_info implements
    org.apache.thrift.TBase<tdw_query_info, tdw_query_info._Fields>,
    java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct(
      "tdw_query_info");

  private static final org.apache.thrift.protocol.TField QUERY_ID_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "queryId", org.apache.thrift.protocol.TType.STRING, (short) 1);
  private static final org.apache.thrift.protocol.TField USER_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "userName", org.apache.thrift.protocol.TType.STRING, (short) 2);
  private static final org.apache.thrift.protocol.TField SESSION_ID_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "sessionId", org.apache.thrift.protocol.TType.STRING, (short) 3);
  private static final org.apache.thrift.protocol.TField START_TIME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "startTime", org.apache.thrift.protocol.TType.STRING, (short) 4);
  private static final org.apache.thrift.protocol.TField FINISH_TIME_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "finishTime", org.apache.thrift.protocol.TType.STRING, (short) 5);
  private static final org.apache.thrift.protocol.TField QUERY_STRING_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "queryString", org.apache.thrift.protocol.TType.STRING, (short) 6);
  private static final org.apache.thrift.protocol.TField MRNUM_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "MRNum", org.apache.thrift.protocol.TType.I32, (short) 7);
  private static final org.apache.thrift.protocol.TField IP_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "ip", org.apache.thrift.protocol.TType.STRING, (short) 8);
  private static final org.apache.thrift.protocol.TField TASKID_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "taskid", org.apache.thrift.protocol.TType.STRING, (short) 9);
  private static final org.apache.thrift.protocol.TField QUERY_STATE_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "queryState", org.apache.thrift.protocol.TType.STRING, (short) 10);
  private static final org.apache.thrift.protocol.TField B_IQUERY_STRING_FIELD_DESC = new org.apache.thrift.protocol.TField(
      "bIQueryString", org.apache.thrift.protocol.TType.STRING, (short) 11);
  private static final org.apache.thrift.protocol.TField PORT_FIELD_DESC = new org.apache.thrift.protocol.TField("port", org.apache.thrift.protocol.TType.STRING, (short)12);
  private static final org.apache.thrift.protocol.TField CLIENT_IP_FIELD_DESC = new org.apache.thrift.protocol.TField("clientIp", org.apache.thrift.protocol.TType.STRING, (short)13);
  private static final org.apache.thrift.protocol.TField DB_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("dbName", org.apache.thrift.protocol.TType.STRING, (short)14);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes
        .put(StandardScheme.class, new tdw_query_infoStandardSchemeFactory());
    schemes.put(TupleScheme.class, new tdw_query_infoTupleSchemeFactory());
  }

  private String queryId;
  private String userName;
  private String sessionId;
  private String startTime;
  private String finishTime;
  private String queryString;
  private int MRNum;
  private String ip;
  private String taskid;
  private String queryState;
  private String bIQueryString;
  private String port; // required
  private String clientIp; // required
  private String dbName; // required

  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    QUERY_ID((short) 1, "queryId"), USER_NAME((short) 2, "userName"), SESSION_ID(
        (short) 3, "sessionId"), START_TIME((short) 4, "startTime"), FINISH_TIME(
        (short) 5, "finishTime"), QUERY_STRING((short) 6, "queryString"), MRNUM(
        (short) 7, "MRNum"), IP((short) 8, "ip"), TASKID((short) 9, "taskid"), QUERY_STATE(
        (short) 10, "queryState"), B_IQUERY_STRING((short) 11, "bIQueryString"),
    PORT((short)12, "port"),
    CLIENT_IP((short)13, "clientIp"),
    DB_NAME((short)14, "dbName");

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
        return USER_NAME;
      case 3:
        return SESSION_ID;
      case 4:
        return START_TIME;
      case 5:
        return FINISH_TIME;
      case 6:
        return QUERY_STRING;
      case 7:
        return MRNUM;
      case 8:
        return IP;
      case 9:
        return TASKID;
      case 10:
        return QUERY_STATE;
      case 11:
        return B_IQUERY_STRING;
        case 12: // PORT
          return PORT;
        case 13: // CLIENT_IP
          return CLIENT_IP;
        case 14: // DB_NAME
          return DB_NAME;
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

  private static final int __MRNUM_ISSET_ID = 0;
  private BitSet __isset_bit_vector = new BitSet(1);
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(
        _Fields.class);
    tmpMap.put(_Fields.QUERY_ID, new org.apache.thrift.meta_data.FieldMetaData(
        "queryId", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.USER_NAME,
        new org.apache.thrift.meta_data.FieldMetaData("userName",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.SESSION_ID,
        new org.apache.thrift.meta_data.FieldMetaData("sessionId",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.START_TIME,
        new org.apache.thrift.meta_data.FieldMetaData("startTime",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.FINISH_TIME,
        new org.apache.thrift.meta_data.FieldMetaData("finishTime",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.QUERY_STRING,
        new org.apache.thrift.meta_data.FieldMetaData("queryString",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.MRNUM, new org.apache.thrift.meta_data.FieldMetaData(
        "MRNum", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.IP, new org.apache.thrift.meta_data.FieldMetaData("ip",
        org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.TASKID, new org.apache.thrift.meta_data.FieldMetaData(
        "taskid", org.apache.thrift.TFieldRequirementType.DEFAULT,
        new org.apache.thrift.meta_data.FieldValueMetaData(
            org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.QUERY_STATE,
        new org.apache.thrift.meta_data.FieldMetaData("queryState",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.B_IQUERY_STRING,
        new org.apache.thrift.meta_data.FieldMetaData("bIQueryString",
            org.apache.thrift.TFieldRequirementType.DEFAULT,
            new org.apache.thrift.meta_data.FieldValueMetaData(
                org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.PORT, new org.apache.thrift.meta_data.FieldMetaData("port", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.CLIENT_IP, new org.apache.thrift.meta_data.FieldMetaData("clientIp", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.DB_NAME, new org.apache.thrift.meta_data.FieldMetaData("dbName", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(
        tdw_query_info.class, metaDataMap);
  }

  public tdw_query_info() {
  }

  public tdw_query_info(
    String queryId,
    String userName,
    String sessionId,
    String startTime,
    String finishTime,
    String queryString,
    int MRNum,
    String ip,
    String taskid,
    String queryState,
    String bIQueryString,
    String port,
    String clientIp,
    String dbName)
  {
    this();
    this.queryId = queryId;
    this.userName = userName;
    this.sessionId = sessionId;
    this.startTime = startTime;
    this.finishTime = finishTime;
    this.queryString = queryString;
    this.MRNum = MRNum;
    setMRNumIsSet(true);
    this.ip = ip;
    this.taskid = taskid;
    this.queryState = queryState;
    this.bIQueryString = bIQueryString;
    this.port = port;
    this.clientIp = clientIp;
    this.dbName = dbName;
  }

  public tdw_query_info(tdw_query_info other) {
    __isset_bit_vector.clear();
    __isset_bit_vector.or(other.__isset_bit_vector);
    if (other.isSetQueryId()) {
      this.queryId = other.queryId;
    }
    if (other.isSetUserName()) {
      this.userName = other.userName;
    }
    if (other.isSetSessionId()) {
      this.sessionId = other.sessionId;
    }
    if (other.isSetStartTime()) {
      this.startTime = other.startTime;
    }
    if (other.isSetFinishTime()) {
      this.finishTime = other.finishTime;
    }
    if (other.isSetQueryString()) {
      this.queryString = other.queryString;
    }
    this.MRNum = other.MRNum;
    if (other.isSetIp()) {
      this.ip = other.ip;
    }
    if (other.isSetTaskid()) {
      this.taskid = other.taskid;
    }
    if (other.isSetQueryState()) {
      this.queryState = other.queryState;
    }
    if (other.isSetBIQueryString()) {
      this.bIQueryString = other.bIQueryString;
    }
    if (other.isSetPort()) {
      this.port = other.port;
    }
    if (other.isSetClientIp()) {
      this.clientIp = other.clientIp;
    }
    if (other.isSetDbName()) {
      this.dbName = other.dbName;
    }
  }

  public tdw_query_info deepCopy() {
    return new tdw_query_info(this);
  }

  @Override
  public void clear() {
    this.queryId = null;
    this.userName = null;
    this.sessionId = null;
    this.startTime = null;
    this.finishTime = null;
    this.queryString = null;
    setMRNumIsSet(false);
    this.MRNum = 0;
    this.ip = null;
    this.taskid = null;
    this.queryState = null;
    this.bIQueryString = null;
    this.port = null;
    this.clientIp = null;
    this.dbName = null;
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

  public String getSessionId() {
    return this.sessionId;
  }

  public void setSessionId(String sessionId) {
    this.sessionId = sessionId;
  }

  public void unsetSessionId() {
    this.sessionId = null;
  }

  public boolean isSetSessionId() {
    return this.sessionId != null;
  }

  public void setSessionIdIsSet(boolean value) {
    if (!value) {
      this.sessionId = null;
    }
  }

  public String getStartTime() {
    return this.startTime;
  }

  public void setStartTime(String startTime) {
    this.startTime = startTime;
  }

  public void unsetStartTime() {
    this.startTime = null;
  }

  public boolean isSetStartTime() {
    return this.startTime != null;
  }

  public void setStartTimeIsSet(boolean value) {
    if (!value) {
      this.startTime = null;
    }
  }

  public String getFinishTime() {
    return this.finishTime;
  }

  public void setFinishTime(String finishTime) {
    this.finishTime = finishTime;
  }

  public void unsetFinishTime() {
    this.finishTime = null;
  }

  public boolean isSetFinishTime() {
    return this.finishTime != null;
  }

  public void setFinishTimeIsSet(boolean value) {
    if (!value) {
      this.finishTime = null;
    }
  }

  public String getQueryString() {
    return this.queryString;
  }

  public void setQueryString(String queryString) {
    this.queryString = queryString;
  }

  public void unsetQueryString() {
    this.queryString = null;
  }

  public boolean isSetQueryString() {
    return this.queryString != null;
  }

  public void setQueryStringIsSet(boolean value) {
    if (!value) {
      this.queryString = null;
    }
  }

  public int getMRNum() {
    return this.MRNum;
  }

  public void setMRNum(int MRNum) {
    this.MRNum = MRNum;
    setMRNumIsSet(true);
  }

  public void unsetMRNum() {
    __isset_bit_vector.clear(__MRNUM_ISSET_ID);
  }

  public boolean isSetMRNum() {
    return __isset_bit_vector.get(__MRNUM_ISSET_ID);
  }

  public void setMRNumIsSet(boolean value) {
    __isset_bit_vector.set(__MRNUM_ISSET_ID, value);
  }

  public String getIp() {
    return this.ip;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }

  public void unsetIp() {
    this.ip = null;
  }

  public boolean isSetIp() {
    return this.ip != null;
  }

  public void setIpIsSet(boolean value) {
    if (!value) {
      this.ip = null;
    }
  }

  public String getTaskid() {
    return this.taskid;
  }

  public void setTaskid(String taskid) {
    this.taskid = taskid;
  }

  public void unsetTaskid() {
    this.taskid = null;
  }

  public boolean isSetTaskid() {
    return this.taskid != null;
  }

  public void setTaskidIsSet(boolean value) {
    if (!value) {
      this.taskid = null;
    }
  }

  public String getQueryState() {
    return this.queryState;
  }

  public void setQueryState(String queryState) {
    this.queryState = queryState;
  }

  public void unsetQueryState() {
    this.queryState = null;
  }

  public boolean isSetQueryState() {
    return this.queryState != null;
  }

  public void setQueryStateIsSet(boolean value) {
    if (!value) {
      this.queryState = null;
    }
  }

  public String getBIQueryString() {
    return this.bIQueryString;
  }

  public void setBIQueryString(String bIQueryString) {
    this.bIQueryString = bIQueryString;
  }

  public void unsetBIQueryString() {
    this.bIQueryString = null;
  }

  public boolean isSetBIQueryString() {
    return this.bIQueryString != null;
  }

  public void setBIQueryStringIsSet(boolean value) {
    if (!value) {
      this.bIQueryString = null;
    }
  }

  public String getPort() {
    return this.port;
  }

  public void setPort(String port) {
    this.port = port;
  }

  public void unsetPort() {
    this.port = null;
  }

  /** Returns true if field port is set (has been assigned a value) and false otherwise */
  public boolean isSetPort() {
    return this.port != null;
  }

  public void setPortIsSet(boolean value) {
    if (!value) {
      this.port = null;
    }
  }

  public String getClientIp() {
    return this.clientIp;
  }

  public void setClientIp(String clientIp) {
    this.clientIp = clientIp;
  }

  public void unsetClientIp() {
    this.clientIp = null;
  }

  /** Returns true if field clientIp is set (has been assigned a value) and false otherwise */
  public boolean isSetClientIp() {
    return this.clientIp != null;
  }

  public void setClientIpIsSet(boolean value) {
    if (!value) {
      this.clientIp = null;
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

  /** Returns true if field dbName is set (has been assigned a value) and false otherwise */
  public boolean isSetDbName() {
    return this.dbName != null;
  }

  public void setDbNameIsSet(boolean value) {
    if (!value) {
      this.dbName = null;
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

    case USER_NAME:
      if (value == null) {
        unsetUserName();
      } else {
        setUserName((String) value);
      }
      break;

    case SESSION_ID:
      if (value == null) {
        unsetSessionId();
      } else {
        setSessionId((String) value);
      }
      break;

    case START_TIME:
      if (value == null) {
        unsetStartTime();
      } else {
        setStartTime((String) value);
      }
      break;

    case FINISH_TIME:
      if (value == null) {
        unsetFinishTime();
      } else {
        setFinishTime((String) value);
      }
      break;

    case QUERY_STRING:
      if (value == null) {
        unsetQueryString();
      } else {
        setQueryString((String) value);
      }
      break;

    case MRNUM:
      if (value == null) {
        unsetMRNum();
      } else {
        setMRNum((Integer) value);
      }
      break;

    case IP:
      if (value == null) {
        unsetIp();
      } else {
        setIp((String) value);
      }
      break;

    case TASKID:
      if (value == null) {
        unsetTaskid();
      } else {
        setTaskid((String) value);
      }
      break;

    case QUERY_STATE:
      if (value == null) {
        unsetQueryState();
      } else {
        setQueryState((String) value);
      }
      break;

    case B_IQUERY_STRING:
      if (value == null) {
        unsetBIQueryString();
      } else {
        setBIQueryString((String) value);
      }
      break;

    case PORT:
      if (value == null) {
        unsetPort();
      } else {
        setPort((String)value);
      }
      break;

    case CLIENT_IP:
      if (value == null) {
        unsetClientIp();
      } else {
        setClientIp((String)value);
      }
      break;

    case DB_NAME:
      if (value == null) {
        unsetDbName();
      } else {
        setDbName((String)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case QUERY_ID:
      return getQueryId();

    case USER_NAME:
      return getUserName();

    case SESSION_ID:
      return getSessionId();

    case START_TIME:
      return getStartTime();

    case FINISH_TIME:
      return getFinishTime();

    case QUERY_STRING:
      return getQueryString();

    case MRNUM:
      return Integer.valueOf(getMRNum());

    case IP:
      return getIp();

    case TASKID:
      return getTaskid();

    case QUERY_STATE:
      return getQueryState();

    case B_IQUERY_STRING:
      return getBIQueryString();

    case PORT:
      return getPort();

    case CLIENT_IP:
      return getClientIp();

    case DB_NAME:
      return getDbName();

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
    case USER_NAME:
      return isSetUserName();
    case SESSION_ID:
      return isSetSessionId();
    case START_TIME:
      return isSetStartTime();
    case FINISH_TIME:
      return isSetFinishTime();
    case QUERY_STRING:
      return isSetQueryString();
    case MRNUM:
      return isSetMRNum();
    case IP:
      return isSetIp();
    case TASKID:
      return isSetTaskid();
    case QUERY_STATE:
      return isSetQueryState();
    case B_IQUERY_STRING:
      return isSetBIQueryString();
    case PORT:
      return isSetPort();
    case CLIENT_IP:
      return isSetClientIp();
    case DB_NAME:
      return isSetDbName();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof tdw_query_info)
      return this.equals((tdw_query_info) that);
    return false;
  }

  public boolean equals(tdw_query_info that) {
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

    boolean this_present_userName = true && this.isSetUserName();
    boolean that_present_userName = true && that.isSetUserName();
    if (this_present_userName || that_present_userName) {
      if (!(this_present_userName && that_present_userName))
        return false;
      if (!this.userName.equals(that.userName))
        return false;
    }

    boolean this_present_sessionId = true && this.isSetSessionId();
    boolean that_present_sessionId = true && that.isSetSessionId();
    if (this_present_sessionId || that_present_sessionId) {
      if (!(this_present_sessionId && that_present_sessionId))
        return false;
      if (!this.sessionId.equals(that.sessionId))
        return false;
    }

    boolean this_present_startTime = true && this.isSetStartTime();
    boolean that_present_startTime = true && that.isSetStartTime();
    if (this_present_startTime || that_present_startTime) {
      if (!(this_present_startTime && that_present_startTime))
        return false;
      if (!this.startTime.equals(that.startTime))
        return false;
    }

    boolean this_present_finishTime = true && this.isSetFinishTime();
    boolean that_present_finishTime = true && that.isSetFinishTime();
    if (this_present_finishTime || that_present_finishTime) {
      if (!(this_present_finishTime && that_present_finishTime))
        return false;
      if (!this.finishTime.equals(that.finishTime))
        return false;
    }

    boolean this_present_queryString = true && this.isSetQueryString();
    boolean that_present_queryString = true && that.isSetQueryString();
    if (this_present_queryString || that_present_queryString) {
      if (!(this_present_queryString && that_present_queryString))
        return false;
      if (!this.queryString.equals(that.queryString))
        return false;
    }

    boolean this_present_MRNum = true;
    boolean that_present_MRNum = true;
    if (this_present_MRNum || that_present_MRNum) {
      if (!(this_present_MRNum && that_present_MRNum))
        return false;
      if (this.MRNum != that.MRNum)
        return false;
    }

    boolean this_present_ip = true && this.isSetIp();
    boolean that_present_ip = true && that.isSetIp();
    if (this_present_ip || that_present_ip) {
      if (!(this_present_ip && that_present_ip))
        return false;
      if (!this.ip.equals(that.ip))
        return false;
    }

    boolean this_present_taskid = true && this.isSetTaskid();
    boolean that_present_taskid = true && that.isSetTaskid();
    if (this_present_taskid || that_present_taskid) {
      if (!(this_present_taskid && that_present_taskid))
        return false;
      if (!this.taskid.equals(that.taskid))
        return false;
    }

    boolean this_present_queryState = true && this.isSetQueryState();
    boolean that_present_queryState = true && that.isSetQueryState();
    if (this_present_queryState || that_present_queryState) {
      if (!(this_present_queryState && that_present_queryState))
        return false;
      if (!this.queryState.equals(that.queryState))
        return false;
    }

    boolean this_present_bIQueryString = true && this.isSetBIQueryString();
    boolean that_present_bIQueryString = true && that.isSetBIQueryString();
    if (this_present_bIQueryString || that_present_bIQueryString) {
      if (!(this_present_bIQueryString && that_present_bIQueryString))
        return false;
      if (!this.bIQueryString.equals(that.bIQueryString))
        return false;
    }

    boolean this_present_port = true && this.isSetPort();
    boolean that_present_port = true && that.isSetPort();
    if (this_present_port || that_present_port) {
      if (!(this_present_port && that_present_port))
        return false;
      if (!this.port.equals(that.port))
        return false;
    }

    boolean this_present_clientIp = true && this.isSetClientIp();
    boolean that_present_clientIp = true && that.isSetClientIp();
    if (this_present_clientIp || that_present_clientIp) {
      if (!(this_present_clientIp && that_present_clientIp))
        return false;
      if (!this.clientIp.equals(that.clientIp))
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

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(tdw_query_info other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    tdw_query_info typedOther = (tdw_query_info) other;

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
    lastComparison = Boolean.valueOf(isSetSessionId()).compareTo(
        typedOther.isSetSessionId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSessionId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.sessionId,
          typedOther.sessionId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetStartTime()).compareTo(
        typedOther.isSetStartTime());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetStartTime()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.startTime,
          typedOther.startTime);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetFinishTime()).compareTo(
        typedOther.isSetFinishTime());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetFinishTime()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.finishTime,
          typedOther.finishTime);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetQueryString()).compareTo(
        typedOther.isSetQueryString());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetQueryString()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.queryString, typedOther.queryString);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetMRNum()).compareTo(
        typedOther.isSetMRNum());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetMRNum()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.MRNum,
          typedOther.MRNum);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetIp()).compareTo(typedOther.isSetIp());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetIp()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.ip,
          typedOther.ip);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetTaskid()).compareTo(
        typedOther.isSetTaskid());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTaskid()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.taskid,
          typedOther.taskid);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetQueryState()).compareTo(
        typedOther.isSetQueryState());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetQueryState()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.queryState,
          typedOther.queryState);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetBIQueryString()).compareTo(
        typedOther.isSetBIQueryString());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetBIQueryString()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(
          this.bIQueryString, typedOther.bIQueryString);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetPort()).compareTo(typedOther.isSetPort());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPort()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.port, typedOther.port);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetClientIp()).compareTo(typedOther.isSetClientIp());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetClientIp()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.clientIp, typedOther.clientIp);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetDbName()).compareTo(typedOther.isSetDbName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDbName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.dbName, typedOther.dbName);
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
    StringBuilder sb = new StringBuilder("tdw_query_info(");
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
    sb.append("userName:");
    if (this.userName == null) {
      sb.append("null");
    } else {
      sb.append(this.userName);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("sessionId:");
    if (this.sessionId == null) {
      sb.append("null");
    } else {
      sb.append(this.sessionId);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("startTime:");
    if (this.startTime == null) {
      sb.append("null");
    } else {
      sb.append(this.startTime);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("finishTime:");
    if (this.finishTime == null) {
      sb.append("null");
    } else {
      sb.append(this.finishTime);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("queryString:");
    if (this.queryString == null) {
      sb.append("null");
    } else {
      sb.append(this.queryString);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("MRNum:");
    sb.append(this.MRNum);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("ip:");
    if (this.ip == null) {
      sb.append("null");
    } else {
      sb.append(this.ip);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("taskid:");
    if (this.taskid == null) {
      sb.append("null");
    } else {
      sb.append(this.taskid);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("queryState:");
    if (this.queryState == null) {
      sb.append("null");
    } else {
      sb.append(this.queryState);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("bIQueryString:");
    if (this.bIQueryString == null) {
      sb.append("null");
    } else {
      sb.append(this.bIQueryString);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("port:");
    if (this.port == null) {
      sb.append("null");
    } else {
      sb.append(this.port);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("clientIp:");
    if (this.clientIp == null) {
      sb.append("null");
    } else {
      sb.append(this.clientIp);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("dbName:");
    if (this.dbName == null) {
      sb.append("null");
    } else {
      sb.append(this.dbName);
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

  private static class tdw_query_infoStandardSchemeFactory implements
      SchemeFactory {
    public tdw_query_infoStandardScheme getScheme() {
      return new tdw_query_infoStandardScheme();
    }
  }

  private static class tdw_query_infoStandardScheme extends
      StandardScheme<tdw_query_info> {

    public void read(org.apache.thrift.protocol.TProtocol iprot,
        tdw_query_info struct) throws org.apache.thrift.TException {
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
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.userName = iprot.readString();
            struct.setUserNameIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 3:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.sessionId = iprot.readString();
            struct.setSessionIdIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 4:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.startTime = iprot.readString();
            struct.setStartTimeIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 5:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.finishTime = iprot.readString();
            struct.setFinishTimeIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 6:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.queryString = iprot.readString();
            struct.setQueryStringIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 7:
          if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
            struct.MRNum = iprot.readI32();
            struct.setMRNumIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 8:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.ip = iprot.readString();
            struct.setIpIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 9:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.taskid = iprot.readString();
            struct.setTaskidIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 10:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.queryState = iprot.readString();
            struct.setQueryStateIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
        case 11:
          if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
            struct.bIQueryString = iprot.readString();
            struct.setBIQueryStringIsSet(true);
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot,
                schemeField.type);
          }
          break;
          case 12: // PORT
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.port = iprot.readString();
              struct.setPortIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 13: // CLIENT_IP
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.clientIp = iprot.readString();
              struct.setClientIpIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 14: // DB_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.dbName = iprot.readString();
              struct.setDbNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot,
        tdw_query_info struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.queryId != null) {
        oprot.writeFieldBegin(QUERY_ID_FIELD_DESC);
        oprot.writeString(struct.queryId);
        oprot.writeFieldEnd();
      }
      if (struct.userName != null) {
        oprot.writeFieldBegin(USER_NAME_FIELD_DESC);
        oprot.writeString(struct.userName);
        oprot.writeFieldEnd();
      }
      if (struct.sessionId != null) {
        oprot.writeFieldBegin(SESSION_ID_FIELD_DESC);
        oprot.writeString(struct.sessionId);
        oprot.writeFieldEnd();
      }
      if (struct.startTime != null) {
        oprot.writeFieldBegin(START_TIME_FIELD_DESC);
        oprot.writeString(struct.startTime);
        oprot.writeFieldEnd();
      }
      if (struct.finishTime != null) {
        oprot.writeFieldBegin(FINISH_TIME_FIELD_DESC);
        oprot.writeString(struct.finishTime);
        oprot.writeFieldEnd();
      }
      if (struct.queryString != null) {
        oprot.writeFieldBegin(QUERY_STRING_FIELD_DESC);
        oprot.writeString(struct.queryString);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(MRNUM_FIELD_DESC);
      oprot.writeI32(struct.MRNum);
      oprot.writeFieldEnd();
      if (struct.ip != null) {
        oprot.writeFieldBegin(IP_FIELD_DESC);
        oprot.writeString(struct.ip);
        oprot.writeFieldEnd();
      }
      if (struct.taskid != null) {
        oprot.writeFieldBegin(TASKID_FIELD_DESC);
        oprot.writeString(struct.taskid);
        oprot.writeFieldEnd();
      }
      if (struct.queryState != null) {
        oprot.writeFieldBegin(QUERY_STATE_FIELD_DESC);
        oprot.writeString(struct.queryState);
        oprot.writeFieldEnd();
      }
      if (struct.bIQueryString != null) {
        oprot.writeFieldBegin(B_IQUERY_STRING_FIELD_DESC);
        oprot.writeString(struct.bIQueryString);
        oprot.writeFieldEnd();
      }
      if (struct.port != null) {
        oprot.writeFieldBegin(PORT_FIELD_DESC);
        oprot.writeString(struct.port);
        oprot.writeFieldEnd();
      }
      if (struct.clientIp != null) {
        oprot.writeFieldBegin(CLIENT_IP_FIELD_DESC);
        oprot.writeString(struct.clientIp);
        oprot.writeFieldEnd();
      }
      if (struct.dbName != null) {
        oprot.writeFieldBegin(DB_NAME_FIELD_DESC);
        oprot.writeString(struct.dbName);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class tdw_query_infoTupleSchemeFactory implements
      SchemeFactory {
    public tdw_query_infoTupleScheme getScheme() {
      return new tdw_query_infoTupleScheme();
    }
  }

  private static class tdw_query_infoTupleScheme extends
      TupleScheme<tdw_query_info> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot,
        tdw_query_info struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetQueryId()) {
        optionals.set(0);
      }
      if (struct.isSetUserName()) {
        optionals.set(1);
      }
      if (struct.isSetSessionId()) {
        optionals.set(2);
      }
      if (struct.isSetStartTime()) {
        optionals.set(3);
      }
      if (struct.isSetFinishTime()) {
        optionals.set(4);
      }
      if (struct.isSetQueryString()) {
        optionals.set(5);
      }
      if (struct.isSetMRNum()) {
        optionals.set(6);
      }
      if (struct.isSetIp()) {
        optionals.set(7);
      }
      if (struct.isSetTaskid()) {
        optionals.set(8);
      }
      if (struct.isSetQueryState()) {
        optionals.set(9);
      }
      if (struct.isSetBIQueryString()) {
        optionals.set(10);
      }
      if (struct.isSetPort()) {
        optionals.set(11);
      }
      if (struct.isSetClientIp()) {
        optionals.set(12);
      }
      if (struct.isSetDbName()) {
        optionals.set(13);
      }
      oprot.writeBitSet(optionals, 14);
      if (struct.isSetQueryId()) {
        oprot.writeString(struct.queryId);
      }
      if (struct.isSetUserName()) {
        oprot.writeString(struct.userName);
      }
      if (struct.isSetSessionId()) {
        oprot.writeString(struct.sessionId);
      }
      if (struct.isSetStartTime()) {
        oprot.writeString(struct.startTime);
      }
      if (struct.isSetFinishTime()) {
        oprot.writeString(struct.finishTime);
      }
      if (struct.isSetQueryString()) {
        oprot.writeString(struct.queryString);
      }
      if (struct.isSetMRNum()) {
        oprot.writeI32(struct.MRNum);
      }
      if (struct.isSetIp()) {
        oprot.writeString(struct.ip);
      }
      if (struct.isSetTaskid()) {
        oprot.writeString(struct.taskid);
      }
      if (struct.isSetQueryState()) {
        oprot.writeString(struct.queryState);
      }
      if (struct.isSetBIQueryString()) {
        oprot.writeString(struct.bIQueryString);
      }
      if (struct.isSetPort()) {
        oprot.writeString(struct.port);
      }
      if (struct.isSetClientIp()) {
        oprot.writeString(struct.clientIp);
      }
      if (struct.isSetDbName()) {
        oprot.writeString(struct.dbName);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot,
        tdw_query_info struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(14);
      if (incoming.get(0)) {
        struct.queryId = iprot.readString();
        struct.setQueryIdIsSet(true);
      }
      if (incoming.get(1)) {
        struct.userName = iprot.readString();
        struct.setUserNameIsSet(true);
      }
      if (incoming.get(2)) {
        struct.sessionId = iprot.readString();
        struct.setSessionIdIsSet(true);
      }
      if (incoming.get(3)) {
        struct.startTime = iprot.readString();
        struct.setStartTimeIsSet(true);
      }
      if (incoming.get(4)) {
        struct.finishTime = iprot.readString();
        struct.setFinishTimeIsSet(true);
      }
      if (incoming.get(5)) {
        struct.queryString = iprot.readString();
        struct.setQueryStringIsSet(true);
      }
      if (incoming.get(6)) {
        struct.MRNum = iprot.readI32();
        struct.setMRNumIsSet(true);
      }
      if (incoming.get(7)) {
        struct.ip = iprot.readString();
        struct.setIpIsSet(true);
      }
      if (incoming.get(8)) {
        struct.taskid = iprot.readString();
        struct.setTaskidIsSet(true);
      }
      if (incoming.get(9)) {
        struct.queryState = iprot.readString();
        struct.setQueryStateIsSet(true);
      }
      if (incoming.get(10)) {
        struct.bIQueryString = iprot.readString();
        struct.setBIQueryStringIsSet(true);
      }
      if (incoming.get(11)) {
        struct.port = iprot.readString();
        struct.setPortIsSet(true);
      }
      if (incoming.get(12)) {
        struct.clientIp = iprot.readString();
        struct.setClientIpIsSet(true);
      }
      if (incoming.get(13)) {
        struct.dbName = iprot.readString();
        struct.setDbNameIsSet(true);
      }
    }
  }

}

