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
package FormatStorage;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import Comm.ConstVar;
import Comm.SEException;
import Comm.Util;
import FormatStorage.Unit.FixedBitSet;

public class FieldMap {
  public static final Log LOG = LogFactory.getLog("FieldMap");

  static public class Field {
    String name = null;
    byte type;
    short index;
    int len;

    public Field() {
      type = ConstVar.FieldType_Int;
      len = ConstVar.Sizeof_Int;

      index = -1;
    }

    public Field(byte type, int len, short index) {
      this.type = type;
      this.len = len;
      this.index = index;
    }

    public byte type() {
      return type;
    }

    public int len() {
      return len;
    }

    public short index() {
      return index;
    }

    public short persistentLen() {
      return (short) (ConstVar.Sizeof_Byte + ConstVar.Sizeof_Short + ConstVar.Sizeof_Int);
    }

    public void persistent(FSDataOutputStream out) throws IOException {
      out.writeByte(type);
      out.writeInt(len);
      out.writeShort(index);

    }

    public void unpersistent(FSDataInputStream in) throws IOException {
      this.type = in.readByte();
      this.len = in.readInt();
      this.index = in.readShort();

    }
  }

  Map<Short, Field> fields = new LinkedHashMap<Short, Field>(10);

  boolean var = false;
  int chunkLen = 0;

  public FieldMap() {

  }

  public void addField(Field field) {
    if (fields.size() >= ConstVar.MaxFieldNum) {
      return;
    }
    if (field.index == -1) {
      field.index = (short) fields.size();
    }

    chunkLen += field.len;

    if (Util.isVarType(field.type())) {
      var = true;
    }

    fields.put(field.index, field);

  }

  public short fieldNum() {
    return (short) fields.size();
  }

  public Set<Short> idxs() {
    return fields.keySet();
  }

  public int len() {
    int len = ConstVar.Sizeof_Short;

    Set<Short> keys = fields.keySet();
    Iterator<Short> iter = keys.iterator();
    while (iter.hasNext()) {
      Short key = iter.next();
      Field field = fields.get(key);
      if (field != null) {
        len += field.persistentLen();
      }
    }

    return len;
  }

  public boolean var() {
    return var;
  }

  public int getChunkLen() {
    return chunkLen + (fieldNum() / 8 + 1);
  }

  public byte getFieldType(short idx) throws Exception {
    Field field = fields.get(idx);
    if (field == null) {
      throw new SEException.InvalidParameterException(
          "no field found with index:" + idx);
    }

    return field.type;
  }

  public Field getField(short idx) throws Exception {
    Field field = fields.get(idx);
    if (field == null) {
      throw new SEException.NoEntityException("no field found with index:"
          + idx);
    }
    return field;
  }

  public void persistent(FSDataOutputStream out) throws IOException {

    out.writeShort(fieldNum());

    Set<Short> keys = fields.keySet();
    Iterator<Short> iter = keys.iterator();
    while (iter.hasNext()) {
      Short key = iter.next();
      fields.get(key).persistent(out);
    }

  }

  public void unpersistent(FSDataInputStream in) throws Exception {
    int fieldNum = in.readShort();

    for (short i = 0; i < fieldNum; i++) {
      Field field = new Field();
      field.unpersistent(in);
      addField(field);
    }
  }

  public void show() {
    int size = fields.size();
    LOG.info("size:" + size);

    Iterator iterator = fields.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry entry = (Entry) iterator.next();
      short index = (Short) entry.getKey();
      Field field = (Field) entry.getValue();

      LOG.info("Field " + index + ": type," + field.type + ",index,"
          + field.index + ",len " + field.len);
    }
  }
}
