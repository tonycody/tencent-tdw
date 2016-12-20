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
package FormatStorage1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import Comm.ConstVar;

public class IHead implements IPersistable {

  private int magic = ConstVar.NewFormatMagic;
  private short ver;
  private byte var;
  private byte lineindex = 1;
  private int primaryIndex = -1;
  private byte compress;
  private byte compressStyle;
  private byte encode;
  private byte encodeStyle;
  private IFieldMap fieldMap;
  private IUserDefinedHeadInfo udi;
  private int formatfiletype = ConstVar.NewFormatFile;

  private String key;

  public IHead() {
    this(ConstVar.NewFormatFile);
  }

  public IHead(int formatfiletype) {
    this.formatfiletype = formatfiletype;
    if (this.formatfiletype == ConstVar.OldFormatFile) {
      this.magic = ConstVar.DataMagic;
    }
    ver = ConstVar.Ver;
    var = 0;
    lineindex = 1;
    primaryIndex = -1;
    compress = 0;
    encode = 0;
    udi = new IUserDefinedHeadInfo();
  }

  public IHead(IHead head) {
    this.setMagic(head.magic);
    this.setCompress(head.compress);
    this.setCompressStyle(head.compressStyle);
    this.setEncode(head.encode);
    this.setEncodeStyle(head.encodeStyle);
    this.setFormatType(head.formatfiletype);
    this.setLineindex(head.lineindex);
    this.setPrimaryIndex(head.primaryIndex);
    this.setVar(head.var);
    this.setVer(head.ver);
    this.udi = head.udi;
  }

  @Override
  public void persistent(DataOutput out) throws IOException {
    if (this.formatfiletype == ConstVar.OldFormatFile) {
      magic = ConstVar.DataMagic;
      out.writeInt(magic);
      out.writeShort(ver);
      out.writeByte(var);
      out.writeByte(compress);
      out.writeByte(compressStyle);
      out.writeShort(primaryIndex);
      out.writeByte(encode);
      out.writeByte(encodeStyle);

      if (key == null) {
        out.writeShort(0);
      } else {
        out.writeShort(key.length());
        if (key.length() != 0) {
          out.write(key.getBytes());
        }
      }
      fieldMap.persistent(out);

    } else if (this.formatfiletype == ConstVar.NewFormatFile) {
      magic = ConstVar.NewFormatMagic;
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
  }

  @Override
  public void unpersistent(DataInput in) throws IOException {
    magic = in.readInt();
    if (magic == ConstVar.DataMagic) {
      this.formatfiletype = ConstVar.OldFormatFile;
    } else {
      this.formatfiletype = ConstVar.NewFormatFile;
    }

    if (this.formatfiletype == ConstVar.OldFormatFile) {
      if (magic != ConstVar.DataMagic) {
        throw new IOException("invalid file magic:" + magic);
      }
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
        in.readFully(keyBytes);
        key = new String(keyBytes);
      }

      fieldMap = new IFieldMap();
      fieldMap.unpersistent(in);

    } else {
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
  }

  public void setFieldMap(IFieldMap fieldMap) {
    this.fieldMap = fieldMap;
    for (IRecord.IFType type : fieldMap.fieldtypes().values()) {
      if (type.type() == ConstVar.FieldType_String) {
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

  public int getPrimaryIndex() {
    return primaryIndex;
  }

  public void setPrimaryIndex(int primaryIndex) {
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

  public void setFormatType(int formatfiletype) {
    this.formatfiletype = formatfiletype;
  }

  public int formatfiletype() {
    return formatfiletype;
  }

  public void fromJobConf(Configuration conf) {
    this.magic = conf.getInt(ConstVar.HD_magic, ConstVar.NewFormatMagic);
    this.var = (byte) conf.getInt(ConstVar.HD_var, 0);
    this.ver = (byte) conf.getInt(ConstVar.HD_ver, 0);
    this.lineindex = (byte) conf.getInt(ConstVar.HD_lineindex, 1);
    this.primaryIndex = (short) conf.getInt(ConstVar.HD_primaryIndex, -1);
    this.compress = (byte) conf.getInt(ConstVar.HD_compress, 0);
    this.compressStyle = (byte) conf.getInt(ConstVar.HD_compressStyle, 0);
    this.encode = (byte) conf.getInt(ConstVar.HD_encode, 0);
    this.encodeStyle = (byte) conf.getInt(ConstVar.HD_encodeStyle, 0);

    this.fieldMap = new IFieldMap();
    String[] fieldStrings = conf.getStrings(ConstVar.HD_fieldMap);
    if (fieldStrings != null)
      for (int i = 0; i < fieldStrings.length; i++) {
        String[] def = fieldStrings[i].split(ConstVar.RecordSplit);
        byte type = Byte.valueOf(def[0]);
        int index = Integer.valueOf(def[2]);
        fieldMap.addFieldType(new IRecord.IFType(type, index));
      }

    this.udi = new IUserDefinedHeadInfo();
    String[] udistrs = conf.getStrings(ConstVar.HD_udi);
    if (udistrs != null)
      for (int i = 0; i < udistrs.length; i++) {
        String[] def = udistrs[i].split(ConstVar.RecordSplit);
        udi.addInfo(Integer.valueOf(def[0]), def[1]);
      }
  }

  public void toJobConf(Configuration conf) {
    conf.setInt(ConstVar.HD_magic, magic);
    conf.setInt(ConstVar.HD_var, var);
    conf.setInt(ConstVar.HD_ver, ver);
    conf.setInt(ConstVar.HD_lineindex, lineindex);
    conf.setInt(ConstVar.HD_primaryIndex, primaryIndex);
    conf.setInt(ConstVar.HD_compress, compress);
    conf.setInt(ConstVar.HD_compressStyle, compressStyle);
    conf.setInt(ConstVar.HD_encode, encode);
    conf.setInt(ConstVar.HD_encodeStyle, encodeStyle);

    if (fieldMap != null) {
      int fieldNum = fieldMap.fieldtypes().size();
      String[] fieldStrings = new String[fieldNum];

      int i = 0;
      for (IRecord.IFType ft : this.fieldMap.fieldtypes().values()) {
        fieldStrings[i++] = ft.type() + ConstVar.RecordSplit + ft.len()
            + ConstVar.RecordSplit + ft.idx();
      }
      conf.setStrings(ConstVar.HD_fieldMap, fieldStrings);
    }

    if (udi != null && udi.infos.size() > 0) {
      String[] udistrs = new String[udi.infos.size()];
      int i = 0;
      for (Map.Entry<Integer, String> en : udi.infos.entrySet()) {
        udistrs[i++] = en.getKey() + ConstVar.RecordSplit + en.getValue();
      }
      conf.setStrings(ConstVar.HD_udi, udistrs);
    }
  }

  public String show() {
    StringBuffer sb = new StringBuffer();
    sb.append("magic:\t" + this.magic + "\r\n")
        .append("ver:\t" + this.ver + "\r\n")
        .append("var:\t" + this.var + "\r\n")
        .append("lineindex:\t" + this.lineindex + "\r\n")
        .append("primaryindex:\t" + this.primaryIndex + "\r\n")
        .append("compress:\t" + this.compress + "\r\n")
        .append("compresstype:\t" + this.compressStyle + "\r\n")
        .append("encode:\t" + this.encode + "\r\n")
        .append("encodetype:\t" + this.encodeStyle + "\r\n");
    sb.append("fieldMap\t" + this.fieldMap.fieldtypes().size() + "\r\n");
    for (IRecord.IFType ift : this.fieldMap.fieldtypes().values()) {
      sb.append("\t" + ift.type()).append("\t" + ift.idx())
          .append("\t" + ift.len() + "\r\n");
    }
    if (udi != null && udi.infos.size() > 0) {
      sb.append("udi:\t" + udi.infos.size());
      for (Integer i : udi.infos.keySet()) {
        sb.append("\t" + i + "\t" + udi.infos.get(i) + "\r\n");
      }
    }

    return sb.toString();
  }

  public String toStr() {
    StringBuffer sb = new StringBuffer();
    sb.append(this.magic + ":").append(this.ver + ":").append(this.var + ":")
        .append(this.lineindex + ":").append(this.primaryIndex + ":")
        .append(this.compress + ":").append(this.compressStyle + ":")
        .append(this.encode + ":").append(this.encodeStyle + ":");
    sb.append(this.fieldMap.fieldtypes().size() + ":");
    for (IRecord.IFType ift : this.fieldMap.fieldtypes().values()) {
      sb.append(ift.show()).append(";");
    }
    if (udi != null && udi.infos.size() > 0) {
      sb.append(":" + udi.infos.size() + ":");
      for (Integer i : udi.infos.keySet()) {
        sb.append(i + "," + udi.infos.get(i) + ";");
      }
    }
    return sb.toString();
  }

  public boolean fromStr(String str) {
    String[] strs = str.split(":");
    if (strs.length < 9)
      return false;
    this.magic = Integer.parseInt(strs[0]);
    this.ver = Short.parseShort(strs[1]);
    this.var = Byte.parseByte(strs[2]);
    this.lineindex = Byte.parseByte(strs[3]);
    this.primaryIndex = Integer.parseInt(strs[4]);
    this.compress = Byte.parseByte(strs[5]);
    this.compressStyle = Byte.parseByte(strs[6]);
    this.encode = Byte.parseByte(strs[7]);
    this.encodeStyle = Byte.parseByte(strs[8]);

    if (this.magic == ConstVar.DataMagic) {
      this.formatfiletype = ConstVar.OldFormatFile;
    }

    int size = Integer.parseInt(strs[9]);
    String[] strs1 = strs[10].split(";");

    this.fieldMap = new IFieldMap();
    for (int i = 0; i < size; i++) {
      String[] strs2 = strs1[i].split(",");
      byte type = Byte.parseByte(strs2[0]);
      int index = Integer.parseInt(strs2[1]);
      fieldMap.addFieldType(new IRecord.IFType(type, index));
    }
    if (strs.length > 11) {
      this.udi = new IUserDefinedHeadInfo();
      size = Integer.parseInt(strs[11]);
      strs1 = strs[12].split(";");
      for (int i = 0; i < size; i++) {
        String[] strs2 = strs1[i].split(",");
        udi.addInfo(Integer.valueOf(strs2[0]), strs2[1]);
      }
    }
    return true;
  }
}
