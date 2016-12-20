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

public class IFieldType implements Writable, IPersistable {
  private byte type = -1;
  private int len = 0;
  private short index = -1;

  public IFieldType() {
  }

  public IFieldType(byte type) {
    this.type = type;
    try {
      if (this.type != ConstVar.FieldType_String)
        this.len = Util.type2len(type);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public IFieldType(byte type, short index) {
    this.type = type;
    this.index = index;
    try {
      if (type != ConstVar.FieldType_String)
        this.len = Util.type2len(type);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public byte getType() {
    return type;
  }

  public void setType(byte type) {
    this.type = type;
    if (type != ConstVar.FieldType_String)
      try {
        this.len = Util.type2len(type);
      } catch (Exception e) {
        e.printStackTrace();
      }
  }

  public int getLen() {
    return len;
  }

  public short getIndex() {
    return index;
  }

  public void setIndex(short index) {
    this.index = index;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(type);
    out.writeInt(len);
    out.writeShort(index);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    type = in.readByte();
    len = in.readInt();
    index = in.readShort();
  }

  @Override
  public void persistent(DataOutput out) throws IOException {
    this.write(out);
  }

  @Override
  public void unpersistent(DataInput in) throws IOException {
    this.readFields(in);
  }

  public static class IFieldByteType extends IFieldType {
    public IFieldByteType() {
      setType(ConstVar.FieldType_Byte);
    }

    public IFieldByteType(short index) {
      setType(ConstVar.FieldType_Byte);
      setIndex(index);
    }
  }

  public static class IFieldCharType extends IFieldType {
    public IFieldCharType() {
      setType(ConstVar.FieldType_Char);
    }

    public IFieldCharType(short index) {
      setType(ConstVar.FieldType_Char);
      setIndex(index);
    }
  }

  public static class IFieldShortType extends IFieldType {
    public IFieldShortType() {
      setType(ConstVar.FieldType_Short);
    }

    public IFieldShortType(short index) {
      setType(ConstVar.FieldType_Short);
      setIndex(index);
    }
  }

  public static class IFieldIntType extends IFieldType {
    public IFieldIntType() {
      setType(ConstVar.FieldType_Int);
    }

    public IFieldIntType(short index) {
      setType(ConstVar.FieldType_Int);
      setIndex(index);
    }
  }

  public static class IFieldLongType extends IFieldType {
    public IFieldLongType() {
      setType(ConstVar.FieldType_Long);
    }

    public IFieldLongType(short index) {
      setType(ConstVar.FieldType_Long);
      setIndex(index);
    }
  }

  public static class IFieldFloatType extends IFieldType {
    public IFieldFloatType() {
      setType(ConstVar.FieldType_Float);
    }

    public IFieldFloatType(short index) {
      setType(ConstVar.FieldType_Float);
      setIndex(index);
    }
  }

  public static class IFieldDoubleType extends IFieldType {
    public IFieldDoubleType() {
      setType(ConstVar.FieldType_Double);
    }

    public IFieldDoubleType(short index) {
      setType(ConstVar.FieldType_Double);
      setIndex(index);
    }
  }

  public static class IFieldStringType extends IFieldType {
    public IFieldStringType() {
      setType(ConstVar.FieldType_String);
    }

    public IFieldStringType(int len) {
      setType(ConstVar.FieldType_String);
      super.len = len;
    }

    public IFieldStringType(int len, short index) {
      setType(ConstVar.FieldType_String);
      setIndex(index);
      super.len = len;
    }
  }

  public void setLen(int length) {
    this.len = length;
  }
}
