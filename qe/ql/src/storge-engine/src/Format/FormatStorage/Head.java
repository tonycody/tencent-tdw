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
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapred.JobConf;

import Comm.ConstVar;
import FormatStorage.FieldMap.Field;

public class Head {
  public static final Log LOG = LogFactory.getLog("Head");

  int magic;
  short ver;
  byte var;
  byte compress;
  byte compressStyle;
  short primaryIndex = -1;
  byte encode;
  byte encodeStyle;
  String key;
  FieldMap fieldMap;

  public Head() {
    magic = ConstVar.DataMagic;
    ver = ConstVar.Ver;

    primaryIndex = -1;
    encode = 0;
    compress = 0;
    var = 0;

    key = null;
  }

  public Head(Head head) {
    this.magic = head.magic;
    this.ver = head.ver;
    this.var = head.var;
    this.compress = head.compress;
    this.compressStyle = head.compressStyle;
    this.primaryIndex = head.primaryIndex;
    this.encode = head.encode;
    this.encodeStyle = head.encodeStyle;

    this.key = head.key;
    this.fieldMap = head.fieldMap;
  }

  public int magic() {
    return magic;
  }

  public void setMagic(int magic) {
    this.magic = magic;
  }

  public short ver() {
    return ver;
  }

  public void setVer(short ver) {
    this.ver = ver;
  }

  public byte var() {
    return var;
  }

  public void setVar(byte var) {
    this.var = var;
  }

  public byte compress() {
    return compress;
  }

  public void setCompress(byte compress) {
    this.compress = compress;
  }

  public byte compressStyle() {
    return compressStyle;
  }

  public void setCompressStyle(byte compressStyle) {
    this.compressStyle = compressStyle;
  }

  public short primaryIndex() {
    return primaryIndex;
  }

  public void setPrimaryIndex(short primaryIndex) throws Exception {
    if (primaryIndex < 0) {
      return;
    }

    if (this.fieldMap == null) {
      throw new IOException("fieldMap null");
    }
    if (this.fieldMap.getFieldType(primaryIndex) != ConstVar.FieldType_Int) {
      throw new IOException("primary index must be int type");
    }
    this.primaryIndex = primaryIndex;
  }

  public byte encode() {
    return encode;
  }

  public void setEncode(byte encode) {
    this.encode = encode;
  }

  public byte encodeStyle() {
    return encodeStyle;
  }

  public void setEncodeStyle(byte encodeStyle) {
    this.encodeStyle = encodeStyle;
  }

  public String key() {
    return key;
  }

  public void setKey(String key) {
    this.key = key;
  }

  public FieldMap fieldMap() {
    return fieldMap;
  }

  public void setFieldMap(FieldMap fieldMap) {
    this.fieldMap = fieldMap;
    if (fieldMap.var()) {
      this.var = ConstVar.VarFlag;
    } else {
      this.var = (byte) 0;
    }

  }

  private short baseLen() {
    if (ver == ConstVar.VER_01) {
      return ConstVar.BaseHeadLen_01;
    }

    return ConstVar.BaseHeadLen;
  }

  public int len() {
    int tlen = (short) (baseLen() + ConstVar.Sizeof_Short);

    if (key != null) {
      tlen += key.length();
    }

    if (fieldMap != null) {
      tlen += fieldMap.len();
    }

    return tlen;
  }

  public void persistent(FSDataOutputStream out) throws IOException {
    out.writeInt(magic);
    out.writeShort(ver);
    out.writeByte(var);
    out.writeByte(compress);
    out.writeByte(compressStyle);
    out.writeShort(primaryIndex);
    out.writeByte(encode);
    out.writeByte(encodeStyle);

    short keyLen = 0;
    if (key != null) {
      keyLen = (short) key.length();
      out.writeShort(key.length());

      if (key.length() != 0) {
        out.write(key.getBytes());
      }
    } else {
      out.writeShort(keyLen);
    }

    if (fieldMap == null) {
      fieldMap = new FieldMap();
    }

    fieldMap.persistent(out);
  }

  public void unpersistent(FSDataInputStream in) throws Exception {
    ver = in.readShort();
    var = in.readByte();
    compress = in.readByte();
    compressStyle = in.readByte();
    primaryIndex = in.readShort();
    encode = in.readByte();
    encodeStyle = in.readByte();

    short keyLen = in.readShort();
    if (keyLen != 0) {
      byte[] keyBytes = new byte[keyLen];
      in.read(keyBytes);

      key = new String(keyBytes);
    }

    if (fieldMap == null) {
      fieldMap = new FieldMap();
    }

    fieldMap.unpersistent(in);

  }

  public void fromJobConf(JobConf job) throws Exception {
    byte var = (byte) job.getInt(ConstVar.HD_var, 0);
    byte compress = (byte) job.getInt(ConstVar.HD_compress, 0);
    byte compressStyle = (byte) job.getInt(ConstVar.HD_compressStyle, 0);
    short primaryIndex = (short) job.getInt(ConstVar.HD_primaryIndex, -1);
    byte encode = (byte) job.getInt(ConstVar.HD_encode, 0);
    byte encodeStyle = (byte) job.getInt(ConstVar.HD_encodeStyle, 0);
    String keyString = job.get(ConstVar.HD_key);
    String[] fieldStrings = job.getStrings(ConstVar.HD_fieldMap);

    LOG.info("in fromJobConf, compressed:" + compress + ",compressStyle:"
        + compressStyle);

    setVar(var);
    setCompress(compress);
    setCompressStyle(compressStyle);

    setEncode(encode);
    setEncodeStyle(encodeStyle);
    if (keyString != null && keyString.length() != 0) {
      setKey(keyString);
    }

    short fieldNum = 0;
    if (fieldStrings != null) {
      fieldNum = (short) fieldStrings.length;
    }

    FieldMap fieldMap = new FieldMap();
    for (short i = 0; i < fieldNum; i++) {
      String[] def = fieldStrings[i].split(ConstVar.RecordSplit);
      byte type = Byte.valueOf(def[0]);
      int len = Integer.valueOf(def[1]);
      short index = Short.valueOf(def[2]);

      fieldMap.addField(new Field(type, len, index));
    }

    setFieldMap(fieldMap);
    setPrimaryIndex(primaryIndex);
  }

  public void toJobConf(Configuration conf) {
    conf.setInt(ConstVar.HD_var, var);
    conf.setInt(ConstVar.HD_compress, compress);
    conf.setInt(ConstVar.HD_compressStyle, compressStyle);
    conf.setInt(ConstVar.HD_primaryIndex, primaryIndex);
    conf.setInt(ConstVar.HD_encode, encode);
    conf.setInt(ConstVar.HD_encodeStyle, encodeStyle);
    if (key != null) {
      conf.set(ConstVar.HD_key, key);
    }

    if (fieldMap != null) {
      short fieldNum = fieldMap.fieldNum();
      String[] fieldStrings = new String[fieldNum];

      Set<Short> keySet = fieldMap.fields.keySet();
      Iterator<Short> iterator = keySet.iterator();

      int i = 0;
      while (iterator.hasNext()) {
        Field field = fieldMap.fields.get(iterator.next());

        fieldStrings[i++] = field.type + ConstVar.RecordSplit + field.len
            + ConstVar.RecordSplit + field.index;
      }
      conf.setStrings(ConstVar.HD_fieldMap, fieldStrings);
    }

  }

  public void show() {
    LOG.info("Magic: \n" + magic);
    LOG.info("headLen: \n" + len());
    LOG.info("ver: \n" + ver);
    LOG.info("varFlag: \n" + var);
    LOG.info("compressFlag: \n" + compress);

    LOG.info("compressStyle: \n" + compressStyle);
    LOG.info("primaryIndexFlag: \n" + primaryIndex);
    LOG.info("encodeFlag: \n" + encode);
    LOG.info("encodeStyle: \n" + encodeStyle);

    fieldMap.show();
  }
}
