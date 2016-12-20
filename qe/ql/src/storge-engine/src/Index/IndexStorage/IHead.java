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

import Comm.ConstVar;

public class IHead implements IPersistable {

  private int magic;
  private short ver;
  private byte var;
  private byte lineindex = 1;
  private short primaryIndex = -1;
  private byte compress;
  private byte compressStyle;
  private byte encode;
  private byte encodeStyle;
  private IFieldMap fieldMap;
  private IUserDefinedHeadInfo udi;

  public IHead() {
    magic = ConstVar.DataMagic;
    ver = ConstVar.Ver;
    var = 0;
    lineindex = 1;
    primaryIndex = -1;
    compress = 0;
    encode = 0;
    udi = new IUserDefinedHeadInfo();
  }

  @Override
  public void persistent(DataOutput out) throws IOException {
    out.writeInt(magic);
    out.writeShort(ver);
    out.writeByte(var);
    out.writeByte(lineindex);
    out.writeShort(primaryIndex);
    out.writeByte(compress);
    if (compress == ConstVar.Compressed)
      out.writeByte(compressStyle);
    out.writeByte(encode);
    if (encode == ConstVar.ENCODED)
      out.writeByte(encodeStyle);
    fieldMap.persistent(out);
    udi.persistent(out);
  }

  @Override
  public void unpersistent(DataInput in) throws IOException {
    magic = in.readInt();
    ver = in.readShort();
    var = in.readByte();
    lineindex = in.readByte();
    primaryIndex = in.readShort();
    compress = in.readByte();
    if (compress == ConstVar.Compressed)
      compressStyle = in.readByte();
    encode = in.readByte();
    if (encode == ConstVar.ENCODED)
      encodeStyle = in.readByte();
    fieldMap = new IFieldMap();
    fieldMap.unpersistent(in);
    udi = new IUserDefinedHeadInfo();
    udi.unpersistent(in);
  }

  public void setFieldMap(IFieldMap fieldMap) {
    this.fieldMap = fieldMap;
    for (IFieldType type : fieldMap.fieldtypes().values()) {
      if (type.getType() == ConstVar.FieldType_String) {
        this.var = 1;
        break;
      }
    }
  }

  public void setUserDefinedInfo(IUserDefinedHeadInfo udi) {
    this.udi = udi;
  }

  public int getMagic() {
    return magic;
  }

  public void setMagic(int magic) {
    this.magic = magic;
  }

  public short getVer() {
    return ver;
  }

  public void setVer(short ver) {
    this.ver = ver;
  }

  public byte getVar() {
    return var;
  }

  public void setVar(byte var) {
    this.var = var;
  }

  public byte getCompress() {
    return compress;
  }

  public void setCompress(byte compress) {
    this.compress = compress;
  }

  public byte getCompressStyle() {
    return compressStyle;
  }

  public void setCompressStyle(byte compressStyle) {
    this.compressStyle = compressStyle;
  }

  public short getPrimaryIndex() {
    return primaryIndex;
  }

  public void setPrimaryIndex(short primaryIndex) {
    this.primaryIndex = primaryIndex;
  }

  public byte getEncode() {
    return encode;
  }

  public void setEncode(byte encode) {
    this.encode = encode;
  }

  public byte getEncodeStyle() {
    return encodeStyle;
  }

  public void setEncodeStyle(byte encodeStyle) {
    this.encodeStyle = encodeStyle;
  }

  public IUserDefinedHeadInfo getUdi() {
    return udi;
  }

  public void setUdi(IUserDefinedHeadInfo udi) {
    this.udi = udi;
  }

  public IFieldMap fieldMap() {
    return fieldMap;
  }

  public void setLineindex(byte lineindex) {
    this.lineindex = lineindex;
  }

  public byte lineindex() {
    return lineindex;
  }
}
