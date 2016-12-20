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

package org.apache.hadoop.hive.serde2.thrift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.thrift.TException;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;
import java.util.*;
import java.io.*;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.io.Text;

import java.util.Properties;

public class TBinarySortableProtocol extends TProtocol implements
    ConfigurableTProtocol, WriteNullsProtocol, WriteTextProtocol {

  final static Log LOG = LogFactory.getLog(TBinarySortableProtocol.class
      .getName());

  static byte ORDERED_TYPE = (byte) -1;

  public static class Factory implements TProtocolFactory {

    public TProtocol getProtocol(TTransport trans) {
      return new TBinarySortableProtocol(trans);
    }
  }

  public TBinarySortableProtocol(TTransport trans) {
    super(trans);
    stackLevel = 0;
  }

  int stackLevel;

  int topLevelStructFieldID;

  String sortOrder;

  boolean ascending;

  public void initialize(Configuration conf, Properties tbl) throws TException {
    sortOrder = tbl.getProperty(Constants.SERIALIZATION_SORT_ORDER);
    if (sortOrder == null) {
      sortOrder = "";
    }
    for (int i = 0; i < sortOrder.length(); i++) {
      char c = sortOrder.charAt(i);
      if (c != '+' && c != '-') {
        throw new TException(Constants.SERIALIZATION_SORT_ORDER
            + " should be a string consists of only '+' and '-'!");
      }
    }
    LOG.info("Sort order is \"" + sortOrder + "\"");
  }

  public void writeMessageBegin(TMessage message) throws TException {
  }

  public void writeMessageEnd() throws TException {
  }

  public void writeStructBegin(TStruct struct) throws TException {
    stackLevel++;
    if (stackLevel == 1) {
      topLevelStructFieldID = 0;
      ascending = (topLevelStructFieldID >= sortOrder.length() || sortOrder
          .charAt(topLevelStructFieldID) != '-');
    } else {
      writeRawBytes(nonNullByte, 0, 1);
    }
  }

  public void writeStructEnd() throws TException {
    stackLevel--;
  }

  public void writeFieldBegin(TField field) throws TException {
  }

  public void writeFieldEnd() throws TException {
    if (stackLevel == 1) {
      topLevelStructFieldID++;
      ascending = (topLevelStructFieldID >= sortOrder.length() || sortOrder
          .charAt(topLevelStructFieldID) != '-');
    }
  }

  public void writeFieldStop() {
  }

  public void writeMapBegin(TMap map) throws TException {
    stackLevel++;
    if (map == null) {
      writeRawBytes(nonNullByte, 0, 1);
    } else {
      writeI32(map.size);
    }
  }

  public void writeMapEnd() throws TException {
    stackLevel--;
  }

  public void writeListBegin(TList list) throws TException {
    stackLevel++;
    if (list == null) {
      writeRawBytes(nonNullByte, 0, 1);
    } else {
      writeI32(list.size);
    }
  }

  public void writeListEnd() throws TException {
    stackLevel--;
  }

  public void writeSetBegin(TSet set) throws TException {
    stackLevel++;
    if (set == null) {
      writeRawBytes(nonNullByte, 0, 1);
    } else {
      writeI32(set.size);
    }
  }

  public void writeSetEnd() throws TException {
    stackLevel--;
  }

  byte[] rawBytesBuffer;

  final private void writeRawBytes(byte[] bytes, int begin, int length)
      throws TException {
    if (ascending) {
      trans_.write(bytes, begin, length);
    } else {
      if (rawBytesBuffer == null || rawBytesBuffer.length < bytes.length) {
        rawBytesBuffer = new byte[bytes.length];
      }
      for (int i = begin; i < begin + length; i++) {
        rawBytesBuffer[i] = (byte) (~bytes[i]);
      }
      trans_.write(rawBytesBuffer, begin, length);
    }
  }

  private byte[] bout = new byte[1];

  public void writeBool(boolean b) throws TException {
    bout[0] = (b ? (byte) 2 : (byte) 1);
    writeRawBytes(bout, 0, 1);
  }

  public void writeByte(byte b) throws TException {
    writeRawBytes(nonNullByte, 0, 1);
    bout[0] = (byte) (b ^ 0x80);
    writeRawBytes(bout, 0, 1);
  }

  private byte[] i16out = new byte[2];

  public void writeI16(short i16) throws TException {
    i16out[0] = (byte) (0xff & ((i16 >> 8) ^ 0x80));
    i16out[1] = (byte) (0xff & (i16));
    writeRawBytes(nonNullByte, 0, 1);
    writeRawBytes(i16out, 0, 2);
  }

  private byte[] i32out = new byte[4];

  public void writeI32(int i32) throws TException {
    i32out[0] = (byte) (0xff & ((i32 >> 24) ^ 0x80));
    i32out[1] = (byte) (0xff & (i32 >> 16));
    i32out[2] = (byte) (0xff & (i32 >> 8));
    i32out[3] = (byte) (0xff & (i32));
    writeRawBytes(nonNullByte, 0, 1);
    writeRawBytes(i32out, 0, 4);
  }

  private byte[] i64out = new byte[8];

  public void writeI64(long i64) throws TException {
    i64out[0] = (byte) (0xff & ((i64 >> 56) ^ 0x80));
    i64out[1] = (byte) (0xff & (i64 >> 48));
    i64out[2] = (byte) (0xff & (i64 >> 40));
    i64out[3] = (byte) (0xff & (i64 >> 32));
    i64out[4] = (byte) (0xff & (i64 >> 24));
    i64out[5] = (byte) (0xff & (i64 >> 16));
    i64out[6] = (byte) (0xff & (i64 >> 8));
    i64out[7] = (byte) (0xff & (i64));
    writeRawBytes(nonNullByte, 0, 1);
    writeRawBytes(i64out, 0, 8);
  }

  public void writeDouble(double dub) throws TException {
    long i64 = Double.doubleToLongBits(dub);
    if ((i64 & (1L << 63)) != 0) {
      i64out[0] = (byte) (0xff & ((i64 >> 56) ^ 0xff));
      i64out[1] = (byte) (0xff & ((i64 >> 48) ^ 0xff));
      i64out[2] = (byte) (0xff & ((i64 >> 40) ^ 0xff));
      i64out[3] = (byte) (0xff & ((i64 >> 32) ^ 0xff));
      i64out[4] = (byte) (0xff & ((i64 >> 24) ^ 0xff));
      i64out[5] = (byte) (0xff & ((i64 >> 16) ^ 0xff));
      i64out[6] = (byte) (0xff & ((i64 >> 8) ^ 0xff));
      i64out[7] = (byte) (0xff & ((i64) ^ 0xff));
    } else {
      i64out[0] = (byte) (0xff & ((i64 >> 56) ^ 0x80));
      i64out[1] = (byte) (0xff & (i64 >> 48));
      i64out[2] = (byte) (0xff & (i64 >> 40));
      i64out[3] = (byte) (0xff & (i64 >> 32));
      i64out[4] = (byte) (0xff & (i64 >> 24));
      i64out[5] = (byte) (0xff & (i64 >> 16));
      i64out[6] = (byte) (0xff & (i64 >> 8));
      i64out[7] = (byte) (0xff & (i64));
    }
    writeRawBytes(nonNullByte, 0, 1);
    writeRawBytes(i64out, 0, 8);
  }

  final protected byte[] nullByte = new byte[] { 0 };
  final protected byte[] nonNullByte = new byte[] { 1 };

  final protected byte[] escapedNull = new byte[] { 1, 1 };

  final protected byte[] escapedOne = new byte[] { 1, 2 };

  public void writeString(String str) throws TException {
    byte[] dat;
    try {
      dat = str.getBytes("UTF-8");
    } catch (UnsupportedEncodingException uex) {
      throw new TException("JVM DOES NOT SUPPORT UTF-8: " + uex.getMessage());
    }
    writeTextBytes(dat, 0, dat.length);
  }

  public void writeBinary(byte[] bin) throws TException {
    if (bin == null) {
      writeRawBytes(nullByte, 0, 1);
    } else {
      writeI32(bin.length);
      writeRawBytes(bin, 0, bin.length);
    }
  }

  public TMessage readMessageBegin() throws TException {
    return new TMessage();
  }

  public void readMessageEnd() throws TException {
  }

  TStruct tstruct = new TStruct();

  public TStruct readStructBegin() throws TException {
    stackLevel++;
    if (stackLevel == 1) {
      topLevelStructFieldID = 0;
      ascending = (topLevelStructFieldID >= sortOrder.length() || sortOrder
          .charAt(topLevelStructFieldID) != '-');
    } else {
      if (readIsNull())
        return null;
    }
    return tstruct;
  }

  public void readStructEnd() throws TException {
    stackLevel--;
  }

  TField f = null;

  public TField readFieldBegin() throws TException {
    f = new TField("", ORDERED_TYPE, (short) -1);
    return f;
  }

  public void readFieldEnd() throws TException {
    if (stackLevel == 1) {
      topLevelStructFieldID++;
      ascending = (topLevelStructFieldID >= sortOrder.length() || sortOrder
          .charAt(topLevelStructFieldID) != '-');
    }
  }

  private TMap tmap = null;

  public TMap readMapBegin() throws TException {
    stackLevel++;
    tmap = new TMap(ORDERED_TYPE, ORDERED_TYPE, readI32());
    if (tmap.size == 0 && lastPrimitiveWasNull()) {
      return null;
    }
    return tmap;
  }

  public void readMapEnd() throws TException {
    stackLevel--;
  }

  private TList tlist = null;

  public TList readListBegin() throws TException {
    stackLevel++;
    tlist = new TList(ORDERED_TYPE, readI32());
    if (tlist.size == 0 && lastPrimitiveWasNull()) {
      return null;
    }
    return tlist;
  }

  public void readListEnd() throws TException {
    stackLevel--;
  }

  private TSet set = null;

  public TSet readSetBegin() throws TException {
    stackLevel++;
    set = new TSet(ORDERED_TYPE, readI32());
    if (set.size == 0 && lastPrimitiveWasNull()) {
      return null;
    }
    return set;
  }

  public void readSetEnd() throws TException {
    stackLevel--;
  }

  final private int readRawAll(byte[] buf, int off, int len) throws TException {
    int bytes = trans_.readAll(buf, off, len);
    if (!ascending) {
      for (int i = off; i < off + bytes; i++) {
        buf[i] = (byte) ~buf[i];
      }
    }
    return bytes;
  }

  public boolean readBool() throws TException {
    readRawAll(bin, 0, 1);
    lastPrimitiveWasNull = (bin[0] == 0);
    return lastPrimitiveWasNull ? false : bin[0] == 2;
  }

  private byte[] wasNull = new byte[1];

  final public boolean readIsNull() throws TException {
    readRawAll(wasNull, 0, 1);
    lastPrimitiveWasNull = (wasNull[0] == 0);
    return lastPrimitiveWasNull;
  }

  private byte[] bin = new byte[1];

  public byte readByte() throws TException {
    if (readIsNull())
      return 0;
    readRawAll(bin, 0, 1);
    return (byte) (bin[0] ^ 0x80);
  }

  private byte[] i16rd = new byte[2];

  public short readI16() throws TException {
    if (readIsNull())
      return 0;
    readRawAll(i16rd, 0, 2);
    return (short) ((((i16rd[0] ^ 0x80) & 0xff) << 8) | ((i16rd[1] & 0xff)));
  }

  private byte[] i32rd = new byte[4];

  public int readI32() throws TException {
    if (readIsNull())
      return 0;
    readRawAll(i32rd, 0, 4);
    return (((i32rd[0] ^ 0x80) & 0xff) << 24) | ((i32rd[1] & 0xff) << 16)
        | ((i32rd[2] & 0xff) << 8) | ((i32rd[3] & 0xff));
  }

  private byte[] i64rd = new byte[8];

  public long readI64() throws TException {
    if (readIsNull())
      return 0;
    readRawAll(i64rd, 0, 8);
    return ((long) ((i64rd[0] ^ 0x80) & 0xff) << 56)
        | ((long) (i64rd[1] & 0xff) << 48) | ((long) (i64rd[2] & 0xff) << 40)
        | ((long) (i64rd[3] & 0xff) << 32) | ((long) (i64rd[4] & 0xff) << 24)
        | ((long) (i64rd[5] & 0xff) << 16) | ((long) (i64rd[6] & 0xff) << 8)
        | ((long) (i64rd[7] & 0xff));
  }

  public double readDouble() throws TException {
    if (readIsNull())
      return 0;
    readRawAll(i64rd, 0, 8);
    long v = 0;
    if ((i64rd[0] & 0x80) != 0) {
      v = ((long) ((i64rd[0] ^ 0x80) & 0xff) << 56)
          | ((long) (i64rd[1] & 0xff) << 48) | ((long) (i64rd[2] & 0xff) << 40)
          | ((long) (i64rd[3] & 0xff) << 32) | ((long) (i64rd[4] & 0xff) << 24)
          | ((long) (i64rd[5] & 0xff) << 16) | ((long) (i64rd[6] & 0xff) << 8)
          | ((long) (i64rd[7] & 0xff));
    } else {
      v = ((long) ((i64rd[0] ^ 0xff) & 0xff) << 56)
          | ((long) ((i64rd[1] ^ 0xff) & 0xff) << 48)
          | ((long) ((i64rd[2] ^ 0xff) & 0xff) << 40)
          | ((long) ((i64rd[3] ^ 0xff) & 0xff) << 32)
          | ((long) ((i64rd[4] ^ 0xff) & 0xff) << 24)
          | ((long) ((i64rd[5] ^ 0xff) & 0xff) << 16)
          | ((long) ((i64rd[6] ^ 0xff) & 0xff) << 8)
          | ((long) ((i64rd[7] ^ 0xff) & 0xff));
    }
    return Double.longBitsToDouble(v);
  }

  private byte[] stringBytes = new byte[1000];

  public String readString() throws TException {
    if (readIsNull()) {
      return null;
    }
    int i = 0;
    while (true) {
      readRawAll(bin, 0, 1);
      if (bin[0] == 0) {
        break;
      }
      if (bin[0] == 1) {
        readRawAll(bin, 0, 1);
        assert (bin[0] == 1 || bin[0] == 2);
        bin[0] = (byte) (bin[0] - 1);
      }
      if (i == stringBytes.length) {
        stringBytes = Arrays.copyOf(stringBytes, stringBytes.length * 2);
      }
      stringBytes[i] = bin[0];
      i++;
    }
    try {
      String r = new String(stringBytes, 0, i, "UTF-8");
      return r;
    } catch (UnsupportedEncodingException uex) {
      throw new TException("JVM DOES NOT SUPPORT UTF-8: " + uex.getMessage());
    }
  }

  boolean lastPrimitiveWasNull;

  public boolean lastPrimitiveWasNull() throws TException {
    return lastPrimitiveWasNull;
  }

  public void writeNull() throws TException {
    writeRawBytes(nullByte, 0, 1);
  }

  void writeTextBytes(byte[] bytes, int start, int length) throws TException {
    writeRawBytes(nonNullByte, 0, 1);
    int begin = 0;
    int i = start;
    for (; i < length; i++) {
      if (bytes[i] == 0 || bytes[i] == 1) {
        if (i > begin) {
          writeRawBytes(bytes, begin, i - begin);
        }
        if (bytes[i] == 0) {
          writeRawBytes(escapedNull, 0, escapedNull.length);
        } else {
          writeRawBytes(escapedOne, 0, escapedOne.length);
        }
        begin = i + 1;
      }
    }
    if (i > begin) {
      writeRawBytes(bytes, begin, i - begin);
    }
    writeRawBytes(nullByte, 0, 1);
  }

  public void writeText(Text text) throws TException {
    writeTextBytes(text.getBytes(), 0, text.getLength());
  }

  @Override
  public void writeBinary(ByteBuffer buf) throws TException {

  }

  @Override
  public ByteBuffer readBinary() throws TException {
    return null;
  }

}
