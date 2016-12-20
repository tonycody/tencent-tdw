/**
* Tencent is pleased to support the open source community by making TDW available.
* Copyright (C) 2014 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use 
* this file except in compliance with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed 
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS 
* OF ANY KIND, either express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/
package IndexStorage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import Comm.ConstVar;
import Comm.Util;

public class IFieldValue implements Writable, IPersistable,
    Comparable<IFieldValue> {
  private IFieldType fieldType;
  private byte[] value = null;

  public IFieldValue() {
  }

  public IFieldValue(IFieldType fieldType) {
    this.fieldType = fieldType;
  }

  public IFieldValue(IFieldType fieldType, byte[] value) {
    this.fieldType = fieldType;
    this.value = value;
    if (this.fieldType.getType() == ConstVar.FieldType_String)
      this.fieldType.setLen(value.length);
  }

  public IFieldValue(byte type, byte[] value, short index) {
    this(new IFieldType(type, index), value);
  }

  public IFieldValue(byte type, byte[] value, int len, short index) {
    this(new IFieldType(type, index), value);
    this.fieldType.setLen(len);
  }

  public IFieldValue(boolean val, short index) {
    this.fieldType = new IFieldType(ConstVar.FieldType_Byte, index);
    value = new byte[this.fieldType.getLen()];
    value[0] = (byte) (val ? 1 : 0);
  }

  public IFieldValue(byte val, short index) {
    this.fieldType = new IFieldType(ConstVar.FieldType_Byte, index);
    value = new byte[this.fieldType.getLen()];
    value[0] = val;
  }

  public IFieldValue(short val, short index) {
    this.fieldType = new IFieldType(ConstVar.FieldType_Short, index);
    value = new byte[this.fieldType.getLen()];
    Util.short2bytes(value, val);
  }

  public IFieldValue(int val, short index) {
    this.fieldType = new IFieldType(ConstVar.FieldType_Int, index);
    value = new byte[this.fieldType.getLen()];
    Util.int2bytes(value, val);
  }

  public IFieldValue(long val, short index) {
    this.fieldType = new IFieldType(ConstVar.FieldType_Long, index);
    value = new byte[this.fieldType.getLen()];
    Util.long2bytes(value, val);
  }

  public IFieldValue(float val, short index) {
    this.fieldType = new IFieldType(ConstVar.FieldType_Float, index);
    value = new byte[this.fieldType.getLen()];
    Util.float2bytes(value, val);
  }

  public IFieldValue(double val, short index) {
    this.fieldType = new IFieldType(ConstVar.FieldType_Double, index);
    value = new byte[this.fieldType.getLen()];
    Util.double2bytes(value, val);
  }

  public IFieldValue(String val, short index) {
    if (val != null) {
      value = val.getBytes();
    } else {
      value = new byte[0];
    }
    this.fieldType = new IFieldType.IFieldStringType(value.length, index);
  }

  public IFieldValue(boolean val) {
    this(val, (short) -1);
  }

  public IFieldValue(byte val) {
    this(val, (short) -1);
  }

  public IFieldValue(char val) {
    this(val, (short) -1);
  }

  public IFieldValue(short val) {
    this(val, (short) -1);
  }

  public IFieldValue(int val) {
    this(val, (short) -1);
  }

  public IFieldValue(long val) {
    this(val, (short) -1);
  }

  public IFieldValue(float val) {
    this(val, (short) -1);
  }

  public IFieldValue(double val) {
    this(val, (short) -1);
  }

  public IFieldValue(String val) {
    this(val, (short) -1);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.fieldType = new IFieldType();
    fieldType.readFields(in);
    value = new byte[fieldType.getLen()];
    in.readFully(value);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    fieldType.write(out);
    out.write(value, 0, fieldType.getLen());
  }

  @Override
  public void persistent(DataOutput out) throws IOException {
    if (fieldType.getType() == ConstVar.FieldType_String) {
      int len = fieldType.getLen();
      out.writeShort(len);
    }
    out.write(value, 0, fieldType.getLen());
  }

  @Override
  public void unpersistent(DataInput in) throws IOException {
    int len = fieldType.getLen();
    if (fieldType.getType() == ConstVar.FieldType_String) {
      len = in.readShort();
    }
    value = new byte[len];
    in.readFully(value);
  }

  @Override
  public int compareTo(IFieldValue ifv1) {
    switch (this.fieldType.getType()) {
    case ConstVar.FieldType_Byte:
      return this.value[0] - ifv1.value[0];
    case ConstVar.FieldType_Short:
      short s1 = Util.bytes2short(value, 0, ConstVar.Sizeof_Short);
      short s2 = Util.bytes2short(ifv1.value, 0, ConstVar.Sizeof_Short);
      if (s1 == s2)
        return 0;
      return s1 > s2 ? 1 : -1;
    case ConstVar.FieldType_Int:
      int i1 = Util.bytes2int(value, 0, ConstVar.Sizeof_Int);
      int i2 = Util.bytes2int(ifv1.value, 0, ConstVar.Sizeof_Int);
      if (i1 == i2)
        return 0;
      return i1 > i2 ? 1 : -1;
    case ConstVar.FieldType_Long:
      long l1 = Util.bytes2long(value, 0, ConstVar.Sizeof_Long);
      long l2 = Util.bytes2long(ifv1.value, 0, ConstVar.Sizeof_Long);
      if (l1 == l2)
        return 0;
      return l1 > l2 ? 1 : -1;
    case ConstVar.FieldType_Float:
      float f1 = Util.bytes2float(value, 0);
      float f2 = Util.bytes2float(ifv1.value, 0);
      if (f1 == f2)
        return 0;
      return f1 > f2 ? 1 : -1;
    case ConstVar.FieldType_Double:
      double d1 = Util.bytes2double(value, 0);
      double d2 = Util.bytes2double(ifv1.value, 0);
      if (d1 == d2)
        return 0;
      return d1 > d2 ? 1 : -1;
    case ConstVar.FieldType_String:
      return new String(this.value).compareTo(new String(ifv1.value));
    }
    return 0;
  }

  public void setIdx(short idx) {
    fieldType.setIndex(idx);
  }

  public short idx() {
    return fieldType.getIndex();
  }

  public byte type() {
    return fieldType.getType();
  }

  public int len() {
    if (fieldType.getType() == ConstVar.FieldType_String)
      return fieldType.getLen() + 2;
    return fieldType.getLen();
  }

  public byte[] value() {
    return value;
  }

  public IFieldType fieldType() {
    return this.fieldType;
  }

  public String show() {
    return show(value, fieldType.getType());
  }

  private static String show(byte[] value, byte type) {
    if (type == ConstVar.FieldType_Byte) {
      return String.valueOf(value[0]);
    } else if (type == ConstVar.FieldType_Short) {
      return String.valueOf(Util.bytes2short(value, 0, ConstVar.Sizeof_Short));
    } else if (type == ConstVar.FieldType_Int) {
      return String.valueOf(Util.bytes2int(value, 0, ConstVar.Sizeof_Int));
    } else if (type == ConstVar.FieldType_Long) {
      return String.valueOf(Util.bytes2long(value, 0, ConstVar.Sizeof_Long));
    } else if (type == ConstVar.FieldType_Float) {
      return String.valueOf(Util.bytes2float(value, 0));
    } else if (type == ConstVar.FieldType_Double) {
      return String.valueOf(Util.bytes2double(value, 0));
    } else if (type == ConstVar.FieldType_String) {
      return new String(value);
    } else {
      return null;
    }
  }
}
