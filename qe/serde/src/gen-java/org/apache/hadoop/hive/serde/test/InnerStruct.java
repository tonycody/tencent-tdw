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
package org.apache.hadoop.hive.serde.test;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import org.apache.log4j.Logger;

import org.apache.thrift.*;
import org.apache.thrift.meta_data.*;
import org.apache.thrift.protocol.*;

public class InnerStruct implements TBase, java.io.Serializable, Cloneable {
  private static final TStruct STRUCT_DESC = new TStruct("InnerStruct");
  private static final TField FIELD0_FIELD_DESC = new TField("field0",
      TType.I32, (short) 1);

  public int field0;
  public static final int FIELD0 = 1;

  private final Isset __isset = new Isset();

  private static final class Isset implements java.io.Serializable {
    public boolean field0 = false;
  }

  public static final Map<Integer, FieldMetaData> metaDataMap = Collections
      .unmodifiableMap(new HashMap<Integer, FieldMetaData>() {
        {
          put(FIELD0, new FieldMetaData("field0",
              TFieldRequirementType.DEFAULT, new FieldValueMetaData(TType.I32)));
        }
      });

  static {
  }

  public InnerStruct() {
  }

  public InnerStruct(int field0) {
    this();
    this.field0 = field0;
    this.__isset.field0 = true;
  }

  public InnerStruct(InnerStruct other) {
    __isset.field0 = other.__isset.field0;
    this.field0 = other.field0;
  }

  @Override
  public InnerStruct clone() {
    return new InnerStruct(this);
  }

  public int getField0() {
    return this.field0;
  }

  public void setField0(int field0) {
    this.field0 = field0;
    this.__isset.field0 = true;
  }

  public void unsetField0() {
    this.__isset.field0 = false;
  }

  public boolean isSetField0() {
    return this.__isset.field0;
  }

  public void setField0IsSet(boolean value) {
    this.__isset.field0 = value;
  }

  public void setFieldValue(int fieldID, Object value) {
    switch (fieldID) {
    case FIELD0:
      if (value == null) {
        unsetField0();
      } else {
        setField0((Integer) value);
      }
      break;

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  public Object getFieldValue(int fieldID) {
    switch (fieldID) {
    case FIELD0:
      return new Integer(getField0());

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  public boolean isSet(int fieldID) {
    switch (fieldID) {
    case FIELD0:
      return isSetField0();
    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof InnerStruct)
      return this.equals((InnerStruct) that);
    return false;
  }

  public boolean equals(InnerStruct that) {
    if (that == null)
      return false;

    boolean this_present_field0 = true;
    boolean that_present_field0 = true;
    if (this_present_field0 || that_present_field0) {
      if (!(this_present_field0 && that_present_field0))
        return false;
      if (this.field0 != that.field0)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public void read(TProtocol iprot) throws TException {
    TField field;
    iprot.readStructBegin();
    while (true) {
      field = iprot.readFieldBegin();
      if (field.type == TType.STOP) {
        break;
      }
      switch (field.id) {
      case FIELD0:
        if (field.type == TType.I32) {
          this.field0 = iprot.readI32();
          this.__isset.field0 = true;
        } else {
          TProtocolUtil.skip(iprot, field.type);
        }
        break;
      default:
        TProtocolUtil.skip(iprot, field.type);
        break;
      }
      iprot.readFieldEnd();
    }
    iprot.readStructEnd();

    validate();
  }

  public void write(TProtocol oprot) throws TException {
    validate();

    oprot.writeStructBegin(STRUCT_DESC);
    oprot.writeFieldBegin(FIELD0_FIELD_DESC);
    oprot.writeI32(this.field0);
    oprot.writeFieldEnd();
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("InnerStruct(");
    boolean first = true;

    sb.append("field0:");
    sb.append(this.field0);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws TException {
  }

  @Override
  public int compareTo(Object arg0) {
    return 0;
  }

  @Override
  public TFieldIdEnum fieldForId(int fieldId) {
    return null;
  }

  @Override
  public boolean isSet(TFieldIdEnum field) {
    return false;
  }

  @Override
  public Object getFieldValue(TFieldIdEnum field) {
    return null;
  }

  @Override
  public void setFieldValue(TFieldIdEnum field, Object value) {

  }

  @Override
  public TBase deepCopy() {
    return null;
  }

  @Override
  public void clear() {

  }

}
