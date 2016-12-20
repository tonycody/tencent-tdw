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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;

import com.hadoop.compression.lzo.LzoCodec;

import Comm.Util;
import Comm.ConstVar;
import Comm.SEException;
import FormatStorage.BlockIndex.IndexInfo;
import FormatStorage.FieldMap.Field;

public class Unit {
  public static final Log LOG = LogFactory.getLog("Unit");

  public static class FixedBitSet {
    private byte[] bytes = null;
    private int len = 0;
    private int need = 0;

    public FixedBitSet(int len) {
      need = len / 8 + 1;
      bytes = new byte[need];

      this.len = len;
    }

    public FixedBitSet(FixedBitSet bitSet) {
      this.bytes = bitSet.bytes;
      this.len = bitSet.len;
    }

    public void set(int i) throws Exception {
      if (i > length()) {
        throw new Exception("out of index:" + i);
      }

      bytes[need - i / 8 - 1] |= 1 << (i % 8);
    }

    public boolean get(int i) throws Exception {
      if (i > length()) {
        throw new Exception("out of index:" + i);
      }

      if ((bytes[need - i / 8 - 1] & (1 << (i % 8))) > 0) {
        return true;
      } else {
        return false;
      }
    }

    public byte[] bytes() {
      return bytes;
    }

    public void setBytes(byte[] bytes) {
      if (bytes != null) {
        this.bytes = bytes;
      }
    }

    public int length() {
      return size() * 8;
    }

    public int size() {
      return bytes.length;
    }

    public void clear() {
      for (int i = 0; i < bytes.length * 8; i++) {
        bytes[bytes.length - i / 8 - 1] &= 0;
      }
    }

    public int needByte(int len) {
      need = len / 8 + 1;
      if (bytes.length < need) {
        bytes = new byte[need];
      }

      return need;
    }

    public int need() {
      return need;
    }
  }

  static public class FieldValue {
    short idx = -1;
    int len = 0;
    byte[] value = null;
    byte type = ConstVar.FieldType_Unknown;

    public FieldValue() {
    }

    public FieldValue(byte type, int len, byte[] value, short idx)
        throws Exception {
      this.idx = idx;
      this.len = len;
      this.type = type;

      if (!Util.isValidType(type)) {
        throw new SEException.InvalidParameterException("invlid type:" + type);
      }

      if (!Util.isVarType(type) && Util.type2len(type) != len) {
        throw new SEException.InvalidParameterException("len:" + len
            + ",but type len:" + Util.type2len(type));
      }

      this.value = value;
      if (value != null) {
        if (len != value.length) {
          throw new SEException.InvalidParameterException(
              "value length must equal to len");
        }
        this.len = (short) value.length;
      }

    }

    public FieldValue(byte val, short index) {
      type = ConstVar.FieldType_Byte;
      len = ConstVar.Sizeof_Byte;

      value = new byte[len];
      value[0] = val;

      idx = index;
    }

    public FieldValue(short val, short index) {
      type = ConstVar.FieldType_Short;
      len = ConstVar.Sizeof_Short;

      value = new byte[len];
      Util.short2bytes(value, val);

      idx = index;
    }

    public FieldValue(int val, short index) {
      type = ConstVar.FieldType_Int;
      len = ConstVar.Sizeof_Int;

      value = new byte[len];
      Util.int2bytes(value, val);

      idx = index;
    }

    public FieldValue(long val, short index) {
      type = ConstVar.FieldType_Long;
      len = ConstVar.Sizeof_Long;

      value = new byte[len];
      Util.long2bytes(value, val);

      idx = index;
    }

    public FieldValue(float val, short index) {
      type = ConstVar.FieldType_Float;
      len = ConstVar.Sizeof_Float;

      value = new byte[len];
      Util.float2bytes(value, val);

      idx = index;
    }

    public FieldValue(double val, short index) {
      type = ConstVar.FieldType_Double;
      len = ConstVar.Sizeof_Double;

      value = new byte[len];
      Util.double2bytes(value, val);

      idx = index;
    }

    public FieldValue(String val, short index) {
      type = ConstVar.FieldType_String;

      if (val != null) {
        len = val.getBytes().length;
        if (len != 0) {
          value = val.getBytes();
        }
      }
      idx = index;
    }

    public void setValue(byte[] value) {
      this.value = value;
      if (value != null) {
        this.len = (short) value.length;
      }
    }

    public void setLen(int len) {
      this.len = len;
    }

    public void setType(byte type) {
      this.type = type;
    }

    public void setIdx(short idx) {
      this.idx = idx;
    }

    public Object toObject() throws Exception {
      if (type == ConstVar.FieldType_Byte) {
        if (len == 0 || value == null) {
          return null;
        }
        return new Byte(value[0]);
      } else if (type == ConstVar.FieldType_Short) {
        if (len == 0 || value == null) {
          return null;
        }
        return new Short(Util.bytes2short(value, 0, ConstVar.Sizeof_Short));
      } else if (type == ConstVar.FieldType_Int) {
        if (len == 0 || value == null) {
          return null;
        }
        return new Integer(Util.bytes2int(value, 0, ConstVar.Sizeof_Int));
      } else if (type == ConstVar.FieldType_Long) {
        if (len == 0 || value == null) {
          return null;
        }
        return new Long(Util.bytes2long(value, 0, ConstVar.Sizeof_Long));
      } else if (type == ConstVar.FieldType_Float) {
        if (len == 0 || value == null) {
          return null;
        }
        return new Float(Util.bytes2float(value, 0));
      } else if (type == ConstVar.FieldType_Double) {
        if (len == 0 || value == null) {
          return null;
        }
        return new Double(Util.bytes2double(value, 0));
      } else if (type == ConstVar.FieldType_String) {
        if (len == 0 || value == null) {
          return null;
        }

        String str = new String(value, 0, len, "utf-8");
        return str;
      } else {
        throw new SEException.InvalidParameterException("invalid type:" + type);
      }
    }

    public byte type() {
      return type;
    }

    public int len() {
      return len;
    }

    public byte[] value() {
      return value;
    }

    public short idx() {
      return idx;
    }

    @SuppressWarnings("deprecation")
    public String toString() {
      if (value == null || len == 0) {
        return "";
      }

      switch (type) {
      case ConstVar.FieldType_Byte:
        return "" + value[0];

      case ConstVar.FieldType_Short:
        return "" + Util.bytes2short(value, 0, len);

      case ConstVar.FieldType_Int:
        return "" + Util.bytes2int(value, 0, len);

      case ConstVar.FieldType_Long:
        return "" + Util.bytes2long(value, 0, len);

      case ConstVar.FieldType_Float:
        return "" + Util.bytes2float(value, 0);

      case ConstVar.FieldType_Double:
        return "" + Util.bytes2double(value, 0);

      case ConstVar.FieldType_String:
        return new String(value, len);
      }

      return "";
    }
  }

  public static class FieldValueComparator implements Comparator {
    @Override
    public int compare(Object field1, Object field2) {
      FieldValue f1 = (FieldValue) field1;
      FieldValue f2 = (FieldValue) field2;

      if (f1.idx > f2.idx) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  static public class RowRecord implements Writable {
    int len = 0;
    byte[] bytes = null;

    @Override
    public void readFields(DataInput in) throws IOException {
      len = in.readInt();
      if (len > 0) {
        bytes = new byte[len];
        in.readFully(bytes, 0, len);
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(len);
      out.write(bytes, 0, len);
    }

    public void assign(RowRecord record) {
      len = record.len;
      bytes = record.bytes;
    }
  }

  static public class Record implements Writable {
    short keyIndex = -1;

    short fieldNum = 0;

    boolean columnMode = false;
    private Map<Short, Short> idxs = null;
    private ArrayList<FieldValue> fieldValues = null;

    private static Map<Short, Short> fieldIndexMap = null;
    private static ArrayList<FieldValue> objectList = null;

    private boolean needTrim = false;

    private static FieldValueComparator comparator = null;

    public Record() {
      if (columnMode) {
        idxs = new HashMap<Short, Short>(10);
      }
      fieldValues = new ArrayList<FieldValue>(10);
    }

    public Record(int fieldNum) throws Exception {
      if (fieldNum > ConstVar.MaxFieldNum) {
        throw new SEException.InvalidParameterException(
            "fieldNum greater than MaxFieldNum:" + ConstVar.MaxFieldNum);
      }

      this.fieldNum = (short) fieldNum;

      if (columnMode) {
        idxs = new HashMap<Short, Short>(fieldNum);
      }
      fieldValues = new ArrayList<FieldValue>(fieldNum);
    }

    public void assign(Record record) {
      clear();

      this.columnMode = record.columnMode;
      this.fieldNum = record.fieldNum;
      this.keyIndex = record.keyIndex;

      int size = record.fieldValues.size();
      for (int i = 0; i < size; i++) {
        try {
          addValue(record.fieldValues.get(i));
        } catch (Exception e) {
          LOG.error("get error in assign:" + e.getMessage());
        }
      }

      needTrim = record.needTrim;
    }

    public boolean addValue(FieldValue fieldValue) throws Exception {
      boolean added = false;
      if (fieldValues.size() >= fieldNum) {
        throw new SEException.FieldValueFull(
            "field value full, fieldValues.size:" + fieldValues.size()
                + ", fieldNum:" + fieldNum);
      }

      if (fieldValue.idx == -1) {
        fieldValue.idx = (short) fieldValues.size();
      }

      if (!columnMode) {
        fieldValues.add(fieldValue);
        return true;
      }

      if (idxs == null)
        idxs = new HashMap<Short, Short>(10);
      if (idxs.put(fieldValue.idx, (short) fieldValues.size()) == null) {
        fieldValues.add(fieldValue);
        added = true;
      }
      return added;
    }

    public void setFieldNum(short fieldNum) {
      this.fieldNum = fieldNum;
    }

    public void setColumnMode(boolean columnMode) {
      this.columnMode = columnMode;
    }

    public short fieldNum() {
      return fieldNum;
    }

    public byte[] getValue(short idx) {
      FieldValue fieldValue = fieldValues.get(idx);

      return fieldValue.value;
    }

    public byte getType(short idx) {
      FieldValue fieldValue = fieldValues.get(idx);

      return fieldValue.type;
    }

    public short getIndex(short idx) {
      FieldValue fieldValue = fieldValues.get(idx);

      return fieldValue.idx;
    }

    public int getLen(short idx) {
      FieldValue fieldValue = fieldValues.get(idx);
      if (fieldValue.value != null) {
        return fieldValue.len;
      }

      return 0;
    }

    public ArrayList<FieldValue> fieldValues() {
      return fieldValues;
    }

    public short tabIndex2recIndex(short idx) {
      if (!columnMode) {
        return idx;
      }

      Short st = idxs.get(idx);
      if (st == null) {
        return -1;
      }
      return st;
    }

    public void show() throws Exception {
      System.out.println("in showRecord, fieldNum = " + fieldNum);
      java.io.PrintStream gbk_out = new java.io.PrintStream(System.out, true,
          "gbk");
      java.io.PrintStream utf8_out = new java.io.PrintStream(System.out, true,
          "utf-8");
      for (short i = 0; i < fieldNum; i++) {
        if (getType(i) == ConstVar.FieldType_String) {

          String value = new String(getValue(i), 0, getLen(i), "utf-8");

          System.out.println("field " + getIndex(i) + ", type "
              + Util.type2name(getType(i)) + ",len " + getLen(i) + ",value: "
              + value);
          gbk_out.println("in gbk.out, field " + getIndex(i) + ", type "
              + Util.type2name(getType(i)) + ",len " + getLen(i) + ",value "
              + value);
          utf8_out.println("in utf8.out, field " + getIndex(i) + ", type "
              + Util.type2name(getType(i)) + ",len " + getLen(i) + ",value "
              + value);
        } else {
          System.out.println("field "
              + getIndex(i)
              + ", type "
              + Util.type2name(getType(i))
              + ",len "
              + getLen(i)
              + ",value "
              + (fieldValues.get(i).value == null ? "null" : Util.getValue(
                  getType(i), getValue(i))));
          gbk_out.println("in gbk.out, field "
              + getIndex(i)
              + ", type "
              + Util.type2name(getType(i))
              + ",len "
              + getLen(i)
              + ",value "
              + (fieldValues.get(i).value == null ? "null" : Util.getValue(
                  getType(i), getValue(i))));
          utf8_out.println("in utf8.out, field "
              + getIndex(i)
              + ", type "
              + Util.type2name(getType(i))
              + ",len "
              + getLen(i)
              + ",value "
              + (fieldValues.get(i).value == null ? "null" : Util.getValue(
                  getType(i), getValue(i))));
        }
      }
    }

    public void showlog() throws Exception {
      LOG.info("in showRecord, fieldNum = " + fieldNum);
      for (short i = 0; i < fieldNum; i++) {
        if (getType(i) == ConstVar.FieldType_String) {
          LOG.info("field "
              + getIndex(i)
              + ", type "
              + Util.type2name(getType(i))
              + ",len "
              + getLen(i)
              + ",value "
              + (fieldValues.get(i).value == null ? "null" : new String(
                  getValue(i))));
        } else {
          LOG.info("field "
              + getIndex(i)
              + ", type "
              + Util.type2name(getType(i))
              + ",len "
              + getLen(i)
              + ",value "
              + (fieldValues.get(i).value == null ? "null" : Util.getValue(
                  getType(i), getValue(i))));
        }
      }
    }

    public void clear() {
      fieldNum = 0;
      keyIndex = -1;

      for (int i = 0; i < fieldValues.size(); i++) {
        fieldValues.get(i).value = null;
      }
      fieldValues.clear();
      if (columnMode && idxs != null) {
        idxs.clear();
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      clear();

      columnMode = in.readBoolean();
      keyIndex = in.readShort();
      fieldNum = in.readShort();
      for (int i = 0; i < fieldNum; i++) {
        short idx = in.readShort();
        int len = in.readInt();
        byte type = in.readByte();
        byte[] value = new byte[len];
        in.readFully(value, 0, len);

        try {
          FieldValue fieldValue = new FieldValue(type, len, value, idx);
          addValue(fieldValue);
        } catch (Exception e) {
          LOG.error("readFields fail:" + e.getMessage());
        }

      }

      needTrim = in.readBoolean();

    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeBoolean(columnMode);
      out.writeShort(keyIndex);
      out.writeShort(fieldNum);
      for (int i = 0; i < fieldNum; i++) {
        FieldValue fieldValue = fieldValues.get(i);

        out.writeShort(fieldValue.idx);
        out.writeInt(fieldValue.len);
        out.writeByte(fieldValue.type);
        out.write(fieldValue.value, 0, fieldValue.len);
      }

      out.writeBoolean(needTrim);
    }

    public ArrayList<FieldValue> toList() throws Exception {
      return fieldValues;

    }

    public void merge(Record record) throws Exception {
      if (record == null) {
        return;
      }

      fieldNum += record.fieldNum;
      for (int i = 0; i < record.fieldNum; i++) {
        if (addValue(record.fieldValues.get(i)) == false) {
          fieldNum--;
        }
      }

      needTrim = true;
    }

    public void trim(FieldMap fieldMap) {
      int size = fieldMap.fieldNum();

      if (!needTrim) {
        return;
      }

      if (columnMode) {
        if (comparator == null) {
          comparator = new FieldValueComparator();
        }

        Collections.sort(fieldValues, comparator);
      }
    }
  }

  static public class DataChunk {
    public short fieldNum;

    static FixedBitSet fixedBitSet = null;
    static byte[] NullBytes = new byte[ConstVar.Sizeof_Long];

    private static DataInputBuffer in = null;
    private static DataOutputBuffer out = null;

    byte[] values = null;

    public long len = 0;

    private void initBitSet(int fieldNum) {
      if (fixedBitSet == null) {
        fixedBitSet = new FixedBitSet(fieldNum);
      } else {
        if (fixedBitSet.length() < fieldNum) {
          fixedBitSet = null;
          fixedBitSet = new FixedBitSet(fieldNum);
        } else {
          fixedBitSet.needByte(fieldNum);
          fixedBitSet.clear();
        }
      }
    }

    public DataChunk(short fieldNum) {
      this.fieldNum = fieldNum;

      initBitSet(fieldNum);
    }

    public DataChunk(Record record) throws Exception {
      this.fieldNum = record.fieldNum;

      initBitSet(fieldNum);

      for (short i = 0; i < record.fieldNum; i++) {
        FieldValue fieldValue = record.fieldValues.get(i);
        if (fieldValue.value == null || fieldValue.len == 0) {
          continue;
        } else {
          fixedBitSet.set(i);
        }
      }

      if (out == null) {
        out = new DataOutputBuffer();
      }
      out.reset();

      int pos = 0;
      out.write(fixedBitSet.bytes(), 0, fixedBitSet.need());
      pos += fixedBitSet.need();

      for (int i = 0; i < fieldNum; i++) {
        boolean NULL = false;
        boolean VAR = false;
        FieldValue fieldValue = record.fieldValues.get(i);
        ;
        if (fieldValue.len == 0 || fieldValue.value == null) {
          NULL = true;
        }

        if (Util.isVarType(fieldValue.type)) {
          VAR = true;
        }

        if (NULL) {
          if (VAR) {
            continue;
          } else {
            int fieldlen = fieldValue.len;
            try {
              fieldlen = Util.type2len(fieldValue.type);
            } catch (Exception e) {
            }
            out.write(NullBytes, 0, fieldlen);
            pos += fieldValue.len;
          }
        } else {
          if (VAR) {
            out.writeShort(fieldValue.len);
            pos += 2;
          }

          out.write(fieldValue.value, 0, fieldValue.len);
          pos += fieldValue.len;
        }
      }

      values = out.getData();
      len = out.getLength();
    }

    public void unpersistent(long offset, long len, DataInputBuffer in)
        throws Exception {
      if (chunkBytes == null || chunkBytes.length < len) {
        chunkBytes = new byte[(int) len];
      }
      this.values = chunkBytes;
      this.len = len;

      in.read(values, 0, (int) len);
    }

    public byte[] values() {
      return values;
    }

    public Record toRecord(FieldMap fieldMap, boolean optimize,
        ArrayList<byte[]> fieldValueBytes) throws Exception {
      Record record = new Record(fieldNum);
      int pos = 0;

      initBitSet(fieldNum);

      if (in == null) {
        in = new DataInputBuffer();
      }
      in.reset(values, (int) len);

      int nbyte = fixedBitSet.needByte(fieldNum);

      if (nbyte != 0) {
        in.read(fixedBitSet.bytes(), 0, nbyte);
        pos += nbyte;
      }

      Set<Short> keys = fieldMap.fields.keySet();
      if (keys.size() != fieldNum) {
        throw new SEException.InnerException("load fieldNum fail:"
            + keys.size() + "-" + fieldNum);
      }

      if (fieldValueBytes.size() < fieldNum) {
        int less = fieldNum - fieldValueBytes.size();
        for (int i = 0; i < less; i++) {
          fieldValueBytes.add(new byte[100]);
        }
      }

      Iterator<Short> iter = keys.iterator();
      for (short k = 0; k < fieldNum && iter.hasNext(); k++) {
        FieldValue fieldValue = new FieldValue();

        short index = iter.next();
        Field field = fieldMap.getField(index);

        fieldValue.idx = index;
        fieldValue.type = field.type();
        fieldValue.len = field.len();

        record.addValue(fieldValue);

        boolean VAR = Util.isVarType(fieldValue.type);
        boolean NULL = !fixedBitSet.get(k);

        if (NULL && VAR) {
          continue;
        }

        if (VAR) {
          if (field.type() == ConstVar.FieldType_User) {
            readUserFieldValue();
          } else {
            short fieldLen = in.readShort();
            pos += 2;
            fieldValue.len = fieldLen;

            if (optimize) {
              byte[] valueBytes = fieldValueBytes.get(k);
              if (valueBytes == null || valueBytes.length < fieldLen) {
                valueBytes = new byte[fieldLen];
                fieldValueBytes.set(k, valueBytes);
              }
              fieldValue.value = valueBytes;
            } else {
              fieldValue.value = new byte[fieldLen];
            }

            in.read(fieldValue.value, 0, fieldLen);
            pos += fieldLen;
          }
        } else {
          if (field.type() == ConstVar.FieldType_Unknown) {
            throw new SEException.InvalidParameterException(
                "invalid field type when data load");
          }

          int fieldLen = Util.type2len(field.type());
          fieldValue.len = fieldLen;

          if (NULL) {
            fieldValue.value = null;

            in.read(NullBytes, 0, fieldLen);
            pos += fieldLen;
          } else {
            if (optimize) {
              byte[] valueBytes = fieldValueBytes.get(k);
              if (valueBytes == null || valueBytes.length < fieldLen) {
                valueBytes = new byte[fieldLen];
                fieldValueBytes.set(k, valueBytes);
              }
              fieldValue.value = valueBytes;
            } else {
              fieldValue.value = new byte[fieldLen];
            }

            in.read(fieldValue.value, 0, fieldLen);
            pos += fieldLen;
          }
        }
      }
      return record;
    }

    public Record toRecord(FieldMap fieldMap, Record record,
        ArrayList<byte[]> fieldValueBytes) throws Exception {
      int pos = 0;

      record.clear();
      record.fieldNum = fieldNum;

      initBitSet(fieldNum);

      if (in == null) {
        in = new DataInputBuffer();
      }

      in.reset(values, (int) len);

      int nbyte = fixedBitSet.needByte(fieldNum);

      if (nbyte != 0) {
        in.read(fixedBitSet.bytes(), 0, nbyte);
        pos += nbyte;
      }

      Set<Short> keys = fieldMap.fields.keySet();
      if (keys.size() != fieldNum) {
        throw new SEException.InnerException("load fieldNum fail:"
            + keys.size() + "-" + fieldNum);
      }

      boolean optimize = (FormatDataFile.counter > 1);
      optimize = true;

      if (fieldValueBytes.size() < fieldNum) {
        int less = fieldNum - fieldValueBytes.size();
        for (int i = 0; i < less; i++) {
          fieldValueBytes.add(new byte[100]);
        }
      }

      Iterator<Short> iter = keys.iterator();
      for (short k = 0; k < fieldNum && iter.hasNext(); k++) {
        FieldValue fieldValue = new FieldValue();

        short index = iter.next();
        Field field = fieldMap.getField(index);

        fieldValue.idx = index;
        fieldValue.type = field.type();
        fieldValue.len = field.len();

        record.addValue(fieldValue);

        boolean VAR = Util.isVarType(fieldValue.type);
        boolean NULL = !fixedBitSet.get(k);
        if (NULL && VAR) {
          continue;
        }

        if (VAR) {
          if (field.type() == ConstVar.FieldType_User) {
            readUserFieldValue();
          } else {
            short fieldLen = in.readShort();
            pos += 2;
            fieldValue.len = fieldLen;

            if (optimize) {
              byte[] valueBytes = fieldValueBytes.get(k);
              if (valueBytes == null || valueBytes.length < fieldLen) {
                valueBytes = new byte[fieldLen];
                fieldValueBytes.set(k, valueBytes);
              }
              fieldValue.value = valueBytes;
            } else {
              fieldValue.value = new byte[fieldLen];
            }

            in.read(fieldValue.value, 0, fieldLen);
            pos += fieldLen;
          }
        } else {
          if (field.type() == ConstVar.FieldType_Unknown) {
            throw new SEException.InvalidParameterException(
                "invalid field type when data load");
          }

          int fieldLen = Util.type2len(field.type());
          fieldValue.len = fieldLen;

          if (NULL) {
            fieldValue.value = null;
            in.read(NullBytes, 0, fieldLen);
            pos += fieldLen;
          } else {
            if (optimize) {
              byte[] valueBytes = fieldValueBytes.get(k);
              if (valueBytes == null || valueBytes.length < fieldLen) {
                valueBytes = new byte[fieldLen];
                fieldValueBytes.set(k, valueBytes);
              }
              fieldValue.value = valueBytes;
            } else {
              fieldValue.value = new byte[fieldLen];
            }
            in.read(fieldValue.value, 0, fieldLen);
            pos += fieldLen;
          }
        }
      }
      return record;
    }

    private void readUserFieldValue() throws Exception {
      throw new SEException.NotSupportException("user define type not support");
    }
  }

  private FieldMap fieldMap = null;

  private long[] offsetArray = null;

  int beginKey = 0;
  int endKey = 0;
  int beginLine = 0;
  int endLine = 0;

  int recordNum = 0;

  private long offset = -1;
  private long len = 0;
  private int index = 0;
  private long metaOffset = -1;
  private int chunksLen = 0;

  private Segment segment = null;

  OutputStream chunksBuffer = new DataOutputBuffer();
  OutputStream metasBuffer = new DataOutputBuffer();

  private static byte[] rowChunkBytes = null;
  private static byte[] chunkBytes = null;

  private byte[] unitBytes = null;
  private static DataInputBuffer dataInputBuffer = new DataInputBuffer();;

  private boolean full = false;

  private boolean compressed = false;
  private byte compressStyle = ConstVar.LZOCompress;

  private LzoCodec codec = null;
  private CompressionOutputStream compressedChunksOutput = null;
  private CompressionOutputStream compressedMetasOutput = null;

  private CompressionInputStream compressedChunksInput = null;
  private CompressionInputStream compressedMetasInput = null;

  private InputStream chunksInputStream = new DataInputBuffer();
  private InputStream metasInputStream = new DataInputBuffer();

  private static byte[] metaOffsetBytes = new byte[ConstVar.Sizeof_Long];

  public Unit(IndexInfo indexInfo, Segment segment) throws IOException {
    full = false;

    this.beginKey = indexInfo.beginKey;
    this.endKey = indexInfo.endKey;
    this.beginLine = indexInfo.beginLine;
    this.endLine = indexInfo.endLine;
    this.offset = indexInfo.offset;
    this.len = indexInfo.len;
    this.index = indexInfo.idx;

    this.segment = segment;

    this.fieldMap = segment.formatData.head.fieldMap;

    if (segment.formatData.workStatus() == ConstVar.WS_Write) {
      beginLine = segment.formatData.recordNum();
      endLine = beginLine;

      this.len = 0;
    }

    compressed = (segment.formatData.head().compress() == ConstVar.Compressed);
    if (compressed) {
      compressStyle = segment.formatData.head().compressStyle();

      codec = new LzoCodec();
      codec.setConf(segment.formatData.conf());

      if (segment.formatData.workStatus() == ConstVar.WS_Write) {
        compressedChunksOutput = codec.createOutputStream(chunksBuffer);
        compressedMetasOutput = codec.createOutputStream(metasBuffer);
      }

    }
  }

  void setLen(long len) {
    this.len = len;
  }

  public long len() throws Exception {
    if (!compressed) {
      if (full) {
        return len;
      }

      int metaLen = ConstVar.DataChunkMetaOffset;
      if (segment.formatData.isVar()) {
        metaLen += recordNum * ConstVar.Sizeof_Long;
      }

      return len + metaLen;
    } else {
      compressedChunksOutput.finish();
      compressedMetasOutput.finish();

      int metaLen = ConstVar.DataChunkMetaOffset;
      if (segment.formatData.isVar()) {
        metaLen += ((DataOutputBuffer) metasBuffer).getLength();
      }

      int chunkLen = ((DataOutputBuffer) chunksBuffer).getLength();

      return chunkLen + metaLen;
    }
  }

  public long offset() {
    return offset;
  }

  public int index() {
    return index;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public void setFull() {
    full = true;
  }

  public int beginLine() {
    return beginLine;
  }

  public int endLine() {
    return endLine;
  }

  public int beginKey() {
    return beginKey;
  }

  public int endKey() {
    return endKey;
  }

  public int recordNum() {
    return recordNum;
  }

  public long[] offsetArray() {
    return offsetArray;
  }

  int chunkLen() {
    return chunksLen;
  }

  public long metaOffset() {
    return metaOffset;
  }

  void setMetaOffset(long meta) {
    metaOffset = meta;
  }

  void releaseUnitBuffer() {
    offsetArray = null;
    unitBytes = null;

    chunksInputStream = null;
    chunksBuffer = null;

    metasInputStream = null;
    metasBuffer = null;

    compressedChunksInput = null;
    compressedChunksOutput = null;

    compressedMetasInput = null;
    compressedMetasOutput = null;
  }

  public void addRecord(Record record) throws Exception {
    DataChunk chunk = new DataChunk(record);

    if (compressed) {
      compressedChunksOutput.write(chunk.values, 0, (int) chunk.len);
    } else {
      chunksBuffer.write(chunk.values, 0, (int) chunk.len);
    }

    chunk.values = null;

    if (metaOffset < 0) {
      metaOffset = offset;
    }

    boolean VAR = segment.formatData.isVar();
    if (VAR) {
      if (compressed) {
        Util.long2bytes(metaOffsetBytes, metaOffset);
        compressedMetasOutput.write(metaOffsetBytes, 0, ConstVar.Sizeof_Long);
      } else {
        ((DataOutputBuffer) metasBuffer).writeLong(metaOffset);
      }

    }

    metaOffset += chunk.len;
    len += chunk.len;
    recordNum++;

    endLine++;
    if (segment.formatData.isPrimaryKeyFile()) {
      int keyValue = getKeyValue(record);
      if (beginKey == Integer.MIN_VALUE) {
        beginKey = keyValue;
        this.endKey = keyValue;
      } else {
        if (keyValue >= endKey) {
          this.endKey = keyValue;
        } else {
          throw new IOException("key index must be increase");
        }

      }
    }
    segment.formatData.incRecordNum();

    if (len > segment.formatData.confUnitSize()) {
      int metaLen = ConstVar.DataChunkMetaOffset;
      if (VAR) {
        metaLen += recordNum * ConstVar.Sizeof_Long;
      }
      len += metaLen;

      full = true;

      throw new SEException.UnitFullException("unit full, len:" + len);
    }
  }

  public void transfer(long newOffset) throws Exception {
    long adjust = newOffset - offset;

    boolean VAR = segment.formatData.isVar();
    if (VAR) {
      if (!compressed) {
        int tnum = ((DataOutputBuffer) metasBuffer).getLength()
            / ConstVar.Sizeof_Long;
        if (tnum != recordNum) {
          throw new SEException.InnerException("tnum != recordNum");
        }

        DataOutputBuffer tmpOuputBuffer = new DataOutputBuffer();
        DataInputBuffer tmpinput = new DataInputBuffer();
        tmpinput.reset(((DataOutputBuffer) metasBuffer).getData(), 0,
            ((DataOutputBuffer) metasBuffer).getLength());
        for (int i = 0; i < recordNum; i++) {
          long value = tmpinput.readLong() + adjust;
          tmpOuputBuffer.writeLong(value);
        }

        tmpinput.reset(tmpOuputBuffer.getData(), 0, tmpOuputBuffer.getLength());
        ((DataOutputBuffer) metasBuffer).reset();
        for (int i = 0; i < recordNum; i++) {
          ((DataOutputBuffer) metasBuffer).writeLong(tmpinput.readLong());
        }

        tmpOuputBuffer = null;
        tmpinput = null;
      } else {
        compressedMetasOutput.finish();

        InputStream tmpMetasInputStream = new DataInputBuffer();
        ((DataInputBuffer) tmpMetasInputStream).reset(
            ((DataOutputBuffer) metasBuffer).getData(), 0,
            ((DataOutputBuffer) metasBuffer).getLength());
        CompressionInputStream tmpCompressedMetasInput = codec
            .createInputStream(tmpMetasInputStream);

        DataOutputBuffer tmpOutputBuffer = new DataOutputBuffer();
        for (int i = 0; i < recordNum; i++) {
          int count = 0;
          try {
            count = tmpCompressedMetasInput.read(metaOffsetBytes, 0,
                ConstVar.Sizeof_Long);
            long meta = Util.bytes2long(metaOffsetBytes, 0,
                ConstVar.Sizeof_Long) + adjust;

            tmpOutputBuffer.writeLong(meta);

          } catch (Exception e) {
            e.printStackTrace();
            System.out.println("i:" + i + ",count:" + count);

            throw e;
          }
        }

        ((DataOutputBuffer) metasBuffer).reset();
        compressedMetasOutput.resetState();

        DataInputBuffer tmpInputBuffer = new DataInputBuffer();
        tmpInputBuffer.reset(tmpOutputBuffer.getData(), 0,
            tmpOutputBuffer.getLength());
        for (int i = 0; i < recordNum; i++) {
          long newMeta = tmpInputBuffer.readLong();
          Util.long2bytes(metaOffsetBytes, newMeta);
          compressedMetasOutput.write(metaOffsetBytes, 0, ConstVar.Sizeof_Long);
        }
      }
    }

    metaOffset += adjust;
    setOffset(newOffset);
  }

  public void persistent(FSDataOutputStream out) throws Exception {
    if (compressed) {
      compressedChunksOutput.finish();
      compressedMetasOutput.finish();
    }

    int chunkLen = ((DataOutputBuffer) chunksBuffer).getLength();
    out.write(((DataOutputBuffer) chunksBuffer).getData(), 0, chunkLen);

    ((DataOutputBuffer) chunksBuffer).reset();

    if (segment.formatData.isVar()) {
      out.write(((DataOutputBuffer) metasBuffer).getData(), 0,
          ((DataOutputBuffer) metasBuffer).getLength());
      ((DataOutputBuffer) metasBuffer).reset();
    }

    out.writeInt(recordNum);

    if (compressed) {
      out.writeLong(chunkLen + offset);
    } else {
      out.writeLong(metaOffset);
    }
    out.writeInt((int) (metaOffset - offset));

    if (!compressed && (chunkLen + offset) != metaOffset) {
      throw new SEException.InnerException("inner error, chunkLen:" + chunkLen
          + ",metaOffset:" + metaOffset);
    }

  }

  public Record[] getRecordByValue(FieldValue[] values, int width, byte op)
      throws Exception {

    int sidx = segment.index();
    byte[] buffer = segment.formatData.unitPool().getUnitBytes(sidx, index);

    if (buffer == null) {
      buffer = loadUnitBuffer(segment.formatData.in());

      segment.formatData.unitPool().poolUnitBytes(sidx, index, buffer, segment);
    }

    boolean VAR = segment.formatData.isVar();

    loadDataMeta(buffer, VAR);

    dataInputBuffer.reset(buffer, 0, buffer.length);

    ArrayList<Record> resultArrary = new ArrayList<Record>(100);
    for (int i = 0; i < recordNum; i++) {
      long dataChunkOffset = offsetArray[i];
      long dataChunkLen = 0;
      if (i < recordNum - 1) {
        dataChunkLen = offsetArray[i + 1] - dataChunkOffset;
      } else {
        dataChunkLen = metaOffset - dataChunkOffset;
      }

      short fieldNum = fieldMap.fieldNum();
      DataChunk chunk = new DataChunk(fieldNum);
      chunk.unpersistent(dataChunkOffset, dataChunkLen, dataInputBuffer);

      boolean optimize = false;
      Record record = chunk.toRecord(fieldMap, false,
          segment.formatData.fieldValueBytes);
      chunk = null;

      if (op == ConstVar.OP_GetSpecial) {
        boolean equal = true;
        for (int j = 0; j < values.length; j++) {
          short idx = record.tabIndex2recIndex(values[j].idx);
          if (idx == -1) {
            equal = false;
            break;
          }

          int cr = WritableComparator.compareBytes(values[j].value, 0,
              values[j].value.length, record.fieldValues.get(idx).value, 0,
              record.fieldValues.get(idx).len);

          if (cr != 0) {
            equal = false;
            break;
          }
        }

        if (equal) {
          resultArrary.add(record);
        }
      } else {
        resultArrary.add(record);
      }

      record = null;
    }

    Record[] records = null;
    int size = resultArrary.size();
    if (size == 0) {
      return records;
    }

    records = new Record[size];
    for (int i = 0; i < size; i++) {
      records[i] = resultArrary.get(i);
    }

    resultArrary.clear();

    return records;

  }

  public Record getRecordByLine(int line) throws Exception {
    if (line < 0 || line >= endLine) {
      return null;
    }

    int sidx = segment.index();
    byte[] buffer = segment.formatData.unitPool().getUnitBytes(sidx, index);

    if (buffer == null) {
      buffer = loadUnitBuffer(segment.formatData.in());

      segment.formatData.unitPool().poolUnitBytes(sidx, index, buffer, segment);
    }

    boolean VAR = segment.formatData.isVar();

    loadDataMeta(buffer, VAR);

    int i = line - beginLine;

    long dataChunkOffset = -1;
    long dataChunkLen = 0;

    if (VAR) {
      dataChunkOffset = offsetArray[i];
      if (line < endLine - 1) {
        dataChunkLen = offsetArray[i + 1] - dataChunkOffset;
      } else {
        if (compressed) {
          dataChunkLen = chunksLen - dataChunkOffset;
        } else {
          dataChunkLen = metaOffset - dataChunkOffset;
        }
      }
    } else {
      dataChunkLen = fieldMap.getChunkLen();
      dataChunkOffset = dataChunkLen * i;
    }

    short fieldNum = fieldMap.fieldNum();
    DataChunk chunk = new DataChunk(fieldNum);

    if (chunkBytes == null || chunkBytes.length < dataChunkLen) {
      chunkBytes = new byte[(int) dataChunkLen];
    }

    chunk.values = chunkBytes;
    chunk.len = dataChunkLen;

    if (!compressed) {
      dataInputBuffer.reset(buffer, 0, buffer.length);
      dataInputBuffer.skip(dataChunkOffset);
      dataInputBuffer.read(chunk.values, 0, (int) dataChunkLen);
    } else {
      ((DataInputBuffer) chunksInputStream).reset(buffer, 0, buffer.length);
      compressedChunksInput = codec.createInputStream(chunksInputStream);

      compressedChunksInput.skip(dataChunkOffset);
      int count = compressedChunksInput.read(chunk.values, 0,
          (int) dataChunkLen);
    }

    boolean optimize = segment.formatData.optimize();
    Record record = chunk.toRecord(fieldMap, optimize,
        segment.formatData.fieldValueBytes);

    chunk.values = null;
    chunk = null;

    return record;
  }

  public Record getRecordByLine(int line, Record record) throws Exception {
    if (line < 0 || line >= endLine) {
      return null;
    }

    int sidx = segment.index();
    byte[] buffer = segment.formatData.unitPool().getUnitBytes(sidx, index);

    if (buffer == null) {
      buffer = loadUnitBuffer(segment.formatData.in());

      segment.formatData.unitPool().poolUnitBytes(sidx, index, buffer, segment);
    }

    boolean VAR = segment.formatData.isVar();

    loadDataMeta(buffer, VAR);

    int i = line - beginLine;
    long dataChunkOffset = -1;
    long dataChunkLen = 0;

    if (VAR) {
      dataChunkOffset = offsetArray[i];
      if (line < endLine - 1) {
        dataChunkLen = offsetArray[i + 1] - dataChunkOffset;
      } else {
        if (compressed) {
          dataChunkLen = chunksLen - dataChunkOffset;
        } else {
          dataChunkLen = metaOffset - dataChunkOffset;
        }
      }
    } else {
      dataChunkLen = fieldMap.getChunkLen();
      dataChunkOffset = dataChunkLen * i;
    }

    short fieldNum = fieldMap.fieldNum();
    DataChunk chunk = new DataChunk(fieldNum);

    if (chunkBytes == null || chunkBytes.length < dataChunkLen) {
      chunkBytes = new byte[(int) dataChunkLen];
    }

    chunk.values = chunkBytes;
    chunk.len = dataChunkLen;

    if (!compressed) {
      dataInputBuffer.reset(buffer, 0, buffer.length);
      dataInputBuffer.skip(dataChunkOffset);
      dataInputBuffer.read(chunk.values, 0, (int) dataChunkLen);
    } else {
      ((DataInputBuffer) chunksInputStream).reset(buffer, 0, buffer.length);
      compressedChunksInput = codec.createInputStream(chunksInputStream);
      compressedChunksInput.skip(dataChunkOffset);
      compressedChunksInput.read(chunk.values, 0, (int) dataChunkLen);
    }

    chunk.toRecord(fieldMap, record, segment.formatData.fieldValueBytes);
    chunk.values = null;
    chunk = null;

    return record;
  }

  public boolean seek(int line) throws IOException {
    if (line < 0 || line >= endLine) {
      return false;
    }

    int sidx = segment.index();
    byte[] buffer = segment.formatData.unitPool().getUnitBytes(sidx, index);

    if (buffer == null) {
      buffer = loadUnitBuffer(segment.formatData.in());

      segment.formatData.unitPool().poolUnitBytes(sidx, index, buffer, segment);
    }

    boolean VAR = segment.formatData.isVar();

    loadDataMeta(buffer, VAR);

    int i = line - beginLine;
    long dataChunkOffset = -1;
    long dataChunkLen = 0;

    if (VAR) {
      dataChunkOffset = offsetArray[i];
      if (line < endLine - 1) {
        dataChunkLen = offsetArray[i + 1] - dataChunkOffset;
      } else {
        if (compressed) {
          dataChunkLen = chunksLen - dataChunkOffset;
        } else {
          dataChunkLen = metaOffset - dataChunkOffset;
        }
      }
    } else {
      dataChunkLen = fieldMap.getChunkLen();
      dataChunkOffset = dataChunkLen * i;
    }

    if (!compressed) {
      dataInputBuffer.reset(buffer, 0, buffer.length);
      dataInputBuffer.skip(dataChunkOffset);
    } else {
      ((DataInputBuffer) chunksInputStream).reset(buffer, 0, buffer.length);
      compressedChunksInput = codec.createInputStream(chunksInputStream);
      compressedChunksInput.skip(dataChunkOffset);
    }

    return true;

  }

  public Record getNextRecord() throws Exception {
    int sidx = segment.index();
    byte[] buffer = segment.formatData.unitPool().getUnitBytes(sidx, index);

    if (buffer == null) {
      buffer = loadUnitBuffer(segment.formatData.in());

      segment.formatData.unitPool().poolUnitBytes(sidx, index, buffer, segment);

      ((DataInputBuffer) chunksInputStream).reset(buffer, 0, buffer.length);
      compressedChunksInput = codec.createInputStream(chunksInputStream);
    }

    boolean VAR = segment.formatData.isVar();

    loadDataMeta(buffer, VAR);

    int line = segment.currentLine();
    int i = line - beginLine;
    long dataChunkOffset = -1;
    long dataChunkLen = 0;

    if (VAR) {
      dataChunkOffset = offsetArray[i];
      if (line < endLine - 1) {
        dataChunkLen = offsetArray[i + 1] - dataChunkOffset;
      } else {
        if (compressed) {
          dataChunkLen = chunksLen - dataChunkOffset;
        } else {
          dataChunkLen = metaOffset - dataChunkOffset;
        }
      }
    } else {
      dataChunkLen = fieldMap.getChunkLen();
      dataChunkOffset = dataChunkLen * i;
    }

    short fieldNum = fieldMap.fieldNum();
    DataChunk chunk = new DataChunk(fieldNum);

    if (chunkBytes == null || chunkBytes.length < dataChunkLen) {
      chunkBytes = new byte[(int) dataChunkLen];
    }

    chunk.values = chunkBytes;
    chunk.len = dataChunkLen;

    if (!compressed) {
      dataInputBuffer.reset(buffer, 0, buffer.length);
      dataInputBuffer.skip(dataChunkOffset);
      dataInputBuffer.read(chunk.values, 0, (int) dataChunkLen);
    } else {
      try {
        compressedChunksInput.read(chunk.values, 0, (int) dataChunkLen);
      } catch (Exception e) {
        System.out.println("dataChunkLen:" + dataChunkLen + ",line:" + line
            + ",beginLine:" + beginLine);
        throw e;
      }
    }

    boolean optimize = segment.formatData.optimize();
    Record record = chunk.toRecord(fieldMap, true,
        segment.formatData.fieldValueBytes);

    chunk.values = null;
    chunk = null;

    return record;
  }

  public Record getNextRecord(Record record) throws Exception {
    int sidx = segment.index();
    byte[] buffer = segment.formatData.unitPool().getUnitBytes(sidx, index);

    if (buffer == null) {
      buffer = loadUnitBuffer(segment.formatData.in());

      segment.formatData.unitPool().poolUnitBytes(sidx, index, buffer, segment);

      ((DataInputBuffer) chunksInputStream).reset(buffer, 0, buffer.length);
      if (compressed)
        compressedChunksInput = codec.createInputStream(chunksInputStream);
    }

    boolean VAR = segment.formatData.isVar();

    loadDataMeta(buffer, VAR);

    int line = segment.currentLine();
    int i = line - beginLine;
    long dataChunkOffset = -1;
    long dataChunkLen = 0;

    if (VAR) {
      dataChunkOffset = offsetArray[i];
      if (line < endLine - 1) {
        dataChunkLen = offsetArray[i + 1] - dataChunkOffset;
      } else {
        if (compressed) {
          dataChunkLen = chunksLen - dataChunkOffset;
        } else {
          dataChunkLen = metaOffset - dataChunkOffset;
        }
      }
    } else {
      dataChunkLen = fieldMap.getChunkLen();
      dataChunkOffset = dataChunkLen * i;
    }

    short fieldNum = fieldMap.fieldNum();
    DataChunk chunk = new DataChunk(fieldNum);

    if (chunkBytes == null || chunkBytes.length < dataChunkLen) {
      chunkBytes = new byte[(int) dataChunkLen];
    }

    chunk.values = chunkBytes;
    chunk.len = dataChunkLen;

    int count = 0;
    if (!compressed) {
      dataInputBuffer.reset(buffer, 0, buffer.length);
      dataInputBuffer.skip(dataChunkOffset);
      dataInputBuffer.read(chunk.values, 0, (int) dataChunkLen);
    } else {
      count = compressedChunksInput.read(chunk.values, 0, (int) dataChunkLen);
      if (count < dataChunkLen) {
        int resnum = (int) dataChunkLen - count;
        count += compressedChunksInput.read(chunk.values, count, resnum);
        if (count < resnum) {
          throw new IOException("compressedChunksInput read error");
        }
      }
    }
    chunk.toRecord(fieldMap, record, segment.formatData.fieldValueBytes);
    chunk.values = null;
    chunk = null;

    return record;
  }

  public RowRecord getRowByLine(int line) throws Exception {
    if (line < 0 || line >= endLine) {
      return null;
    }

    int sidx = segment.index();
    byte[] buffer = segment.formatData.unitPool().getUnitBytes(sidx, index);

    if (buffer == null) {
      buffer = loadUnitBuffer(segment.formatData.in());

      segment.formatData.unitPool().poolUnitBytes(sidx, index, buffer, segment);
    }

    boolean VAR = segment.formatData.isVar();

    loadDataMeta(buffer, VAR);

    int i = line - beginLine;

    long dataChunkOffset = -1;
    long dataChunkLen = 0;

    if (VAR) {
      dataChunkOffset = offsetArray[i];
      if (line < endLine - 1) {
        dataChunkLen = offsetArray[i + 1] - dataChunkOffset;
      } else {
        dataChunkLen = metaOffset - dataChunkOffset;
      }
    } else {
      dataChunkLen = fieldMap.getChunkLen();
      dataChunkOffset = dataChunkLen * i;
    }

    if (rowChunkBytes == null || rowChunkBytes.length < dataChunkLen) {
      rowChunkBytes = new byte[(int) dataChunkLen];
    }

    RowRecord rowRecord = new RowRecord();
    rowRecord.len = (int) dataChunkLen;
    rowRecord.bytes = rowChunkBytes;

    dataInputBuffer.reset(buffer, 0, buffer.length);
    dataInputBuffer.skip(dataChunkOffset);
    dataInputBuffer.read(rowChunkBytes, 0, (int) dataChunkLen);

    return rowRecord;
  }

  private int getKeyValue(Record record) throws IOException {
    short keyIndex = segment.formatData.primaryKeyIndex();

    for (int i = 0; i < record.fieldNum; i++) {
      FieldValue fieldValue = record.fieldValues.get(i);
      if (fieldValue.idx == keyIndex) {
        if (fieldValue.type != ConstVar.FieldType_Int) {
          throw new IOException("the index type must be int");
        }
        return Util.bytes2int(fieldValue.value, 0, ConstVar.Sizeof_Int);
      }
    }

    return 0;

  }

  byte[] loadUnitBuffer(FSDataInputStream in) throws IOException {
    if (unitBytes == null || unitBytes.length < len) {
      unitBytes = new byte[(int) len];
    }
    in.seek(offset);
    in.read(unitBytes, 0, (int) len);

    return unitBytes;
  }

  void loadDataMeta(FSDataInputStream in) throws IOException {
    if (metaOffset == -1) {
      long pos = offset + len - ConstVar.DataChunkMetaOffset;
      in.seek(pos);

      recordNum = in.readInt();
      metaOffset = in.readLong();
      chunksLen = in.readInt();
    }

    if (offsetArray == null) {
      offsetArray = new long[recordNum];
    }

    if (segment.formatData.var() == ConstVar.VarFlag) {
      in.seek(metaOffset);
      for (int i = 0; i < recordNum; i++) {
        offsetArray[i] = in.readLong();
      }
    }
  }

  void loadDataMeta(byte[] buffer, boolean VAR) throws IOException {
    if (metaOffset == -1) {
      int pos = (int) (len - ConstVar.DataChunkMetaOffset);

      dataInputBuffer.reset(buffer, 0, (int) len);

      dataInputBuffer.skip(pos);
      recordNum = dataInputBuffer.readInt();
      metaOffset = dataInputBuffer.readLong() - offset;
      chunksLen = dataInputBuffer.readInt();

    }

    if (VAR && offsetArray == null) {
      offsetArray = new long[recordNum];

      if (compressed) {
        ((DataInputBuffer) metasInputStream).reset(buffer, (int) metaOffset,
            (int) (len - metaOffset));
        compressedMetasInput = codec.createInputStream(metasInputStream);

        for (int i = 0; i < recordNum; i++) {
          compressedMetasInput.read(metaOffsetBytes, 0, ConstVar.Sizeof_Long);
          long meta = Util.bytes2long(metaOffsetBytes, 0, ConstVar.Sizeof_Long);
          offsetArray[i] = meta - offset;
        }
      } else {
        dataInputBuffer.reset(buffer, (int) metaOffset,
            (int) (len - metaOffset));
        for (int i = 0; i < recordNum; i++) {
          offsetArray[i] = dataInputBuffer.readLong() - offset;
        }
      }
    } else if (offsetArray == null) {
      offsetArray = new long[recordNum];
      int recordlen = fieldMap.getChunkLen();
      for (int i = 0; i < offsetArray.length; i++) {
        offsetArray[i] = i * recordlen;
      }
    }
  }
}
