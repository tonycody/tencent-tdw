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
package org.apache.hadoop.hive.serde2.thrift.test;

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

public class IntString implements TBase, java.io.Serializable, Cloneable {
  private static final TStruct STRUCT_DESC = new TStruct("IntString");
  private static final TField MYINT_FIELD_DESC = new TField("myint", TType.I32,
      (short) 1);
  private static final TField MY_STRING_FIELD_DESC = new TField("myString",
      TType.STRING, (short) 2);
  private static final TField UNDERSCORE_INT_FIELD_DESC = new TField(
      "underscore_int", TType.I32, (short) 3);

  public int myint;
  public static final int MYINT = 1;
  public String myString;
  public static final int MYSTRING = 2;
  public int underscore_int;
  public static final int UNDERSCORE_INT = 3;

  private final Isset __isset = new Isset();

  private static final class Isset implements java.io.Serializable {
    public boolean myint = false;
    public boolean underscore_int = false;
  }

  public static final Map<Integer, FieldMetaData> metaDataMap = Collections
      .unmodifiableMap(new HashMap<Integer, FieldMetaData>() {
        {
          put(MYINT, new FieldMetaData("myint", TFieldRequirementType.DEFAULT,
              new FieldValueMetaData(TType.I32)));
          put(MYSTRING, new FieldMetaData("myString",
              TFieldRequirementType.DEFAULT, new FieldValueMetaData(
                  TType.STRING)));
          put(UNDERSCORE_INT, new FieldMetaData("underscore_int",
              TFieldRequirementType.DEFAULT, new FieldValueMetaData(TType.I32)));
        }
      });

  static {
  }

  public IntString() {
  }

  public IntString(int myint, String myString, int underscore_int) {
    this();
    this.myint = myint;
    this.__isset.myint = true;
    this.myString = myString;
    this.underscore_int = underscore_int;
    this.__isset.underscore_int = true;
  }

  public IntString(IntString other) {
    __isset.myint = other.__isset.myint;
    this.myint = other.myint;
    if (other.isSetMyString()) {
      this.myString = other.myString;
    }
    __isset.underscore_int = other.__isset.underscore_int;
    this.underscore_int = other.underscore_int;
  }

  @Override
  public IntString clone() {
    return new IntString(this);
  }

  public int getMyint() {
    return this.myint;
  }

  public void setMyint(int myint) {
    this.myint = myint;
    this.__isset.myint = true;
  }

  public void unsetMyint() {
    this.__isset.myint = false;
  }

  public boolean isSetMyint() {
    return this.__isset.myint;
  }

  public void setMyintIsSet(boolean value) {
    this.__isset.myint = value;
  }

  public String getMyString() {
    return this.myString;
  }

  public void setMyString(String myString) {
    this.myString = myString;
  }

  public void unsetMyString() {
    this.myString = null;
  }

  public boolean isSetMyString() {
    return this.myString != null;
  }

  public void setMyStringIsSet(boolean value) {
    if (!value) {
      this.myString = null;
    }
  }

  public int getUnderscore_int() {
    return this.underscore_int;
  }

  public void setUnderscore_int(int underscore_int) {
    this.underscore_int = underscore_int;
    this.__isset.underscore_int = true;
  }

  public void unsetUnderscore_int() {
    this.__isset.underscore_int = false;
  }

  public boolean isSetUnderscore_int() {
    return this.__isset.underscore_int;
  }

  public void setUnderscore_intIsSet(boolean value) {
    this.__isset.underscore_int = value;
  }

  public void setFieldValue(int fieldID, Object value) {
    switch (fieldID) {
    case MYINT:
      if (value == null) {
        unsetMyint();
      } else {
        setMyint((Integer) value);
      }
      break;

    case MYSTRING:
      if (value == null) {
        unsetMyString();
      } else {
        setMyString((String) value);
      }
      break;

    case UNDERSCORE_INT:
      if (value == null) {
        unsetUnderscore_int();
      } else {
        setUnderscore_int((Integer) value);
      }
      break;

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  public Object getFieldValue(int fieldID) {
    switch (fieldID) {
    case MYINT:
      return new Integer(getMyint());

    case MYSTRING:
      return getMyString();

    case UNDERSCORE_INT:
      return new Integer(getUnderscore_int());

    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  public boolean isSet(int fieldID) {
    switch (fieldID) {
    case MYINT:
      return isSetMyint();
    case MYSTRING:
      return isSetMyString();
    case UNDERSCORE_INT:
      return isSetUnderscore_int();
    default:
      throw new IllegalArgumentException("Field " + fieldID + " doesn't exist!");
    }
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof IntString)
      return this.equals((IntString) that);
    return false;
  }

  public boolean equals(IntString that) {
    if (that == null)
      return false;

    boolean this_present_myint = true;
    boolean that_present_myint = true;
    if (this_present_myint || that_present_myint) {
      if (!(this_present_myint && that_present_myint))
        return false;
      if (this.myint != that.myint)
        return false;
    }

    boolean this_present_myString = true && this.isSetMyString();
    boolean that_present_myString = true && that.isSetMyString();
    if (this_present_myString || that_present_myString) {
      if (!(this_present_myString && that_present_myString))
        return false;
      if (!this.myString.equals(that.myString))
        return false;
    }

    boolean this_present_underscore_int = true;
    boolean that_present_underscore_int = true;
    if (this_present_underscore_int || that_present_underscore_int) {
      if (!(this_present_underscore_int && that_present_underscore_int))
        return false;
      if (this.underscore_int != that.underscore_int)
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
      case MYINT:
        if (field.type == TType.I32) {
          this.myint = iprot.readI32();
          this.__isset.myint = true;
        } else {
          TProtocolUtil.skip(iprot, field.type);
        }
        break;
      case MYSTRING:
        if (field.type == TType.STRING) {
          this.myString = iprot.readString();
        } else {
          TProtocolUtil.skip(iprot, field.type);
        }
        break;
      case UNDERSCORE_INT:
        if (field.type == TType.I32) {
          this.underscore_int = iprot.readI32();
          this.__isset.underscore_int = true;
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
    oprot.writeFieldBegin(MYINT_FIELD_DESC);
    oprot.writeI32(this.myint);
    oprot.writeFieldEnd();
    if (this.myString != null) {
      oprot.writeFieldBegin(MY_STRING_FIELD_DESC);
      oprot.writeString(this.myString);
      oprot.writeFieldEnd();
    }
    oprot.writeFieldBegin(UNDERSCORE_INT_FIELD_DESC);
    oprot.writeI32(this.underscore_int);
    oprot.writeFieldEnd();
    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("IntString(");
    boolean first = true;

    sb.append("myint:");
    sb.append(this.myint);
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("myString:");
    if (this.myString == null) {
      sb.append("null");
    } else {
      sb.append(this.myString);
    }
    first = false;
    if (!first)
      sb.append(", ");
    sb.append("underscore_int:");
    sb.append(this.underscore_int);
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
