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
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import Comm.ConstVar;
import Comm.Util;

public class IRecord implements IPersistable, Writable {
  public static final Log LOG = LogFactory.getLog(IRecord.class);

  private int fieldnum = 0;
  private HashMap<Integer, IFType> fieldTypes = null;
  private HashMap<Integer, IFType> readfieldTypes = null;
  private HashMap<Integer, Integer> idx2bitsetpos;
  private int[] idxs;
  private boolean fixlenmode = true;
  private boolean oldformat = false;

  private boolean serilized = false;
  private DataOutputBuffer bytes = null;
  private FixedBitSet bitSet = null;
  private ArrayList<IFValue> fieldValues = null;

  public IRecord() {
  }

  public IRecord(HashMap<Integer, IFType> fieldTypes) {
    this(fieldTypes, fieldTypes);
  }

  public IRecord(HashMap<Integer, IFType> fieldtypes,
      HashMap<Integer, IFType> readfieldTypes) {
    this.setFieldTypes(fieldtypes);
    this.readfieldTypes = readfieldTypes;
  }

  private void setFieldTypes(HashMap<Integer, IFType> fieldtypes) {
    this.fieldTypes = fieldtypes;
    this.fieldnum = fieldtypes.size();

    this.bitSet = new FixedBitSet(fieldnum);
    this.fieldValues = new ArrayList<IFValue>(fieldnum);
    for (int i = 0; i < fieldnum; i++) {
      this.fieldValues.add(null);
    }
    this.bytes = new DataOutputBuffer();
    idx2bitsetpos = new HashMap<Integer, Integer>(fieldnum);
    idxs = new int[fieldnum];
    int i = 0, idx = 0;
    while (i < fieldnum) {
      if (fieldtypes.containsKey(idx++)) {
        idxs[i] = idx - 1;
        idx2bitsetpos.put(idx - 1, i++);
        if (fieldtypes.get(idx - 1).type == ConstVar.FieldType_String) {
          this.fixlenmode = false;
        }
      }
    }
  }

  public void setNull(int i) {
    this.fieldValues.set(i, null);
  }

  public boolean addFieldValue(IFValue fv) throws IOException {
    if (this.fieldTypes == null || !this.idx2bitsetpos.containsKey(fv.idx()))
      return false;

    int i = idx2bitsetpos.get(fv.idx());
    fieldValues.set(i, fv);
    bitSet.set(i);
    this.serilized = false;
    return true;
  }

  public boolean addFieldValue2(IFValue fv) throws IOException {
    fieldValues.set(fv.idx(), fv);
    return true;
  }

  private void serializeAll() throws IOException {
    this.bytes.reset();
    for (int i = 0; i < this.fieldValues.size(); i++) {
      IFValue fv = this.fieldValues.get(i);
      IFType ft = fieldTypes.get(idxs[i]);
      if (fv == null) {
        if (fixlenmode || (oldformat && ft.type != ConstVar.FieldType_String)) {
          new IFValue(ft, null).persistent(bytes);
        }
      } else {
        fv.persistent(bytes);
      }
    }
    serilized = true;
  }

  public boolean containsIdx(int idx) {
    return this.idx2bitsetpos.containsKey(idx)
        && this.fieldValues.get(this.idx2bitsetpos.get(idx)) != null;
  }

  public IFValue getByIdx(int idx) {
    if (!this.idx2bitsetpos.containsKey(idx)) {
      return null;
    }
    return this.fieldValues.get(this.idx2bitsetpos.get(idx));
  }

  public IFValue getByPos(int i) {
    return this.fieldValues.get(i);
  }

  public int fieldnum() {
    return this.fieldnum;
  }

  public int len() {
    int len = bitSet.needbytes();
    if (!serilized) {
      try {
        this.serializeAll();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    len += bytes.getLength();
    return len;
  }

  public void setoldformat(boolean oldformat) {
    this.oldformat = oldformat;
  }

  @Override
  public void persistent(DataOutput out) throws IOException {
    bitSet.persistent(out);
    if (!serilized) {
      this.serializeAll();
    }
    out.write(bytes.getData(), 0, bytes.getLength());
  }

  @Override
  public void unpersistent(DataInput in) throws IOException {
    bitSet.reset(fieldnum);
    bitSet.unpersistent(in);

    for (int i = 0; i < fieldnum; i++) {
      this.fieldValues.set(i, null);
    }

    int i = 0;
    IFType ft;
    this.bytes.reset();
    while (i++ < fieldnum) {
      ft = fieldTypes.get(idxs[i - 1]);
      IFValue fv = new IFValue(ft);
      boolean VAR = Util.isVarType(ft.type);
      boolean contains = bitSet.get(idx2bitsetpos.get(ft.idx));
      if ((oldformat ? !VAR : fixlenmode) || contains) {
        fv.unpersistent(in);
      }
      if (contains && this.readfieldTypes.containsKey(ft.idx)) {
        fieldValues.set(i - 1, fv);
      }
    }
    bytes.reset();
    serilized = false;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.oldformat = in.readBoolean();
    this.fieldnum = in.readInt();
    HashMap<Integer, IFType> fts = new HashMap<Integer, IFType>(fieldnum);
    for (int i = 0; i < fieldnum; i++) {
      IFType ift = new IFType();
      ift.readFields(in);
      fts.put(ift.idx, ift);
    }
    this.setFieldTypes(fts);
    this.readfieldTypes = this.fieldTypes;

    this.unpersistent(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeBoolean(this.oldformat);
    out.writeInt(this.fieldnum);

    int i = 0;
    while (i++ < fieldnum) {
      fieldTypes.get(idxs[i - 1]).write(out);
    }
    this.persistent(out);
  }

  public void clear() {
    for (int i = 0; i < this.fieldnum; i++) {
      this.fieldValues.set(i, null);
    }
    this.bitSet.clear();
    bytes.reset();
    serilized = false;
  }

  public String getFieldStr(int i) {
    IFValue fv = fieldValues.get(i);
    if (this.fieldTypes.containsKey(i) && fv != null) {
      return fv.show();
    }
    return "";
  }

  public String getFieldStr2(int i) {
    IFValue fv = fieldValues.get(i);
    if (fv != null && fv.show().length() == 0) {
      return "\\NN";
    }
    if (this.fieldTypes.containsKey(i) && fv != null) {
      return fv.show();
    }
    return "\\N";
  }

  public String showtype() {
    StringBuffer sb = new StringBuffer();
    StringBuffer sb2 = new StringBuffer();
    sb.append("idx:\t");
    sb2.append("type:\t");
    for (IFType ft : this.fieldTypes.values()) {
      sb.append(ft.idx + "\t");
      sb2.append(ft.type + "\t");
    }
    sb.append("\r\n").append(sb2.toString()).append("\r\n");
    return sb.toString();
  }

  public HashMap<Integer, IFType> getFieldTypes() {
    return this.fieldTypes;
  }

  public void show() {
    for (IFType ft : this.readfieldTypes.values()) {
      IFValue fv = fieldValues.get(idx2bitsetpos.get(ft.idx));
      if (fv != null) {
        System.out.print(fv.show());
      } else {
        System.out.print("NULL");
      }
      System.out.print("\t");
    }
    System.out.println();
  }

  public String showstr() {
    StringBuffer sb = new StringBuffer();
    for (IFType ft : this.readfieldTypes.values()) {
      IFValue fv = fieldValues.get(idx2bitsetpos.get(ft.idx));
      if (fv != null) {
        sb.append(fv.show());
      } else {
        sb.append("NULL");
      }
      sb.append("\t");
    }
    return sb.toString();
  }

  public void reset(HashMap<Integer, IFType> fieldtypes) {
    this.setFieldTypes(fieldtypes);
    this.readfieldTypes = fieldtypes;
  }

  private static class FixedBitSet implements IPersistable {
    private byte[] bytes = null;
    private int needbytes = 0;

    public FixedBitSet(int len) {
      needbytes = len / 8 + 1;
      bytes = new byte[needbytes];
    }

    public void clear() {
      Arrays.fill(bytes, (byte) 0);
    }

    public void reset(int len) {
      int needbytes1 = len / 8 + 1;
      if (needbytes1 != needbytes) {
        needbytes = needbytes1;
        bytes = new byte[needbytes];
      }
    }

    public int needbytes() {
      return needbytes;
    }

    public void set(int i) {
      bytes[needbytes - i / 8 - 1] |= 1 << (i % 8);
    }

    public boolean get(int i) {
      if ((bytes[needbytes - i / 8 - 1] & (1 << (i % 8))) > 0) {
        return true;
      } else {
        return false;
      }
    }

    @Override
    public void persistent(DataOutput out) throws IOException {
      out.write(bytes, 0, needbytes);
    }

    @Override
    public void unpersistent(DataInput in) throws IOException {
      in.readFully(bytes, 0, needbytes);
    }
  }

  public static class IFType implements Writable, IPersistable {
    private byte type = -1;
    private int idx = -1;
    private int len = -1;

    public byte type() {
      return type;
    }

    public int idx() {
      return idx;
    }

    public void setType(byte type) {
      this.type = type;
    }

    public void setIdx(int idx) {
      this.idx = idx;
    }

    public IFType() {
    }

    public IFType(byte type, int idx) {
      this.type = type;
      this.idx = idx;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.type = in.readByte();
      this.len = in.readInt();
      this.idx = in.readShort();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeByte(type);
      out.writeInt(len());
      out.writeShort(idx);
    }

    @Override
    public void persistent(DataOutput out) throws IOException {
      this.write(out);
    }

    @Override
    public void unpersistent(DataInput in) throws IOException {
      this.readFields(in);
    }

    public int len() {
      if (len == -1)
        switch (type) {
        case ConstVar.FieldType_Byte:
          len = 1;
          break;
        case ConstVar.FieldType_Short:
          len = 2;
          break;
        case ConstVar.FieldType_Int:
          len = 4;
          break;
        case ConstVar.FieldType_Long:
          len = 8;
          break;
        case ConstVar.FieldType_Float:
          len = 4;
          break;
        case ConstVar.FieldType_Double:
          len = 8;
          break;
        case ConstVar.FieldType_String:
          len = 0;
          break;
        }
      return len;
    }

    public String show() {
      return this.type + "," + this.idx + "," + this.len();
    }

    @Override
    protected Object clone() {
      return new IFType(type, idx);
    }
  }

  public static class IFValue implements Writable, IPersistable,
      Comparable<IFValue> {

    IFType type;
    Object data;

    public IFValue() {
    }

    public IFValue(IFType ft, Object data) {
      this.type = ft;
      this.data = data;
    }

    public IFValue(IFType ft) {
      this(ft, null);
    }

    public IFValue(byte val, int idx) {
      this.type = new IFType(ConstVar.FieldType_Byte, idx);
      this.data = val;
    }

    public IFValue(short val, int idx) {
      this.type = new IFType(ConstVar.FieldType_Short, idx);
      this.data = val;
    }

    public IFValue(int val, int idx) {
      this.type = new IFType(ConstVar.FieldType_Int, idx);
      this.data = val;
    }

    public IFValue(long val, int idx) {
      this.type = new IFType(ConstVar.FieldType_Long, idx);
      this.data = val;
    }

    public IFValue(float val, int idx) {
      this.type = new IFType(ConstVar.FieldType_Float, idx);
      this.data = val;
    }

    public IFValue(double val, int idx) {
      this.type = new IFType(ConstVar.FieldType_Double, idx);
      this.data = val;
    }

    public IFValue(String val, int idx) {
      this.type = new IFType(ConstVar.FieldType_String, idx);
      this.data = val;
    }

    public int idx() {
      return type.idx;
    }

    public IFType type() {
      return type;
    }

    public Object data() {
      return this.data;
    }

    public int len() {
      if (this.type.type != ConstVar.FieldType_String)
        return this.type.len();
      else
        return ((String) data).length();
    }

    public ByteBuffer tobytes() {
      byte[] bb = null;
      switch (type.type) {
      case ConstVar.FieldType_Byte:
        bb = new byte[1];
        bb[0] = (Byte) data;
        break;
      case ConstVar.FieldType_Short:
        bb = new byte[2];
        Util.short2bytes(bb, (Short) data);
        break;
      case ConstVar.FieldType_Int:
        bb = new byte[4];
        Util.int2bytes(bb, (Integer) data);
        break;
      case ConstVar.FieldType_Long:
        bb = new byte[8];
        Util.long2bytes(bb, (Long) data);
        break;
      case ConstVar.FieldType_Float:
        bb = new byte[4];
        Util.float2bytes(bb, (Float) data);
        break;
      case ConstVar.FieldType_Double:
        bb = new byte[8];
        Util.double2bytes(bb, (Double) data);
        break;
      case ConstVar.FieldType_String:
        try {
          return Text.encode((String) data);
        } catch (CharacterCodingException e) {
          e.printStackTrace();
        }
        break;
      }
      ByteBuffer bbf = ByteBuffer.wrap(bb, 0, bb.length);
      return bbf;
    }

    @Override
    public void unpersistent(DataInput in) throws IOException {
      switch (type.type) {
      case ConstVar.FieldType_Byte:
        data = in.readByte();
        break;
      case ConstVar.FieldType_Short:
        data = in.readShort();
        break;
      case ConstVar.FieldType_Int:
        data = in.readInt();
        break;
      case ConstVar.FieldType_Long:
        data = in.readLong();
        break;
      case ConstVar.FieldType_Float:
        data = in.readFloat();
        break;
      case ConstVar.FieldType_Double:
        data = in.readDouble();
        break;
      case ConstVar.FieldType_String:
        int len = in.readShort();
        byte[] bb = new byte[len];
        in.readFully(bb);
        data = Text.decode(bb).toString();
        break;
      }
    }

    @Override
    public void persistent(DataOutput bytes) throws IOException {
      if (data == null) {
        switch (type.type) {
        case ConstVar.FieldType_Byte:
          data = (byte) 0;
          break;
        case ConstVar.FieldType_Short:
          data = (short) 0;
          break;
        case ConstVar.FieldType_Int:
          data = (int) 0;
          break;
        case ConstVar.FieldType_Long:
          data = (long) 0;
          break;
        case ConstVar.FieldType_Float:
          data = (float) 0;
          break;
        case ConstVar.FieldType_Double:
          data = (double) 0;
          break;
        case ConstVar.FieldType_String:
          data = "";
          break;
        }
      }
      switch (type.type) {
      case ConstVar.FieldType_Byte:
        bytes.writeByte((Byte) data);
        break;
      case ConstVar.FieldType_Short:
        bytes.writeShort((Short) data);
        break;
      case ConstVar.FieldType_Int:
        bytes.writeInt((Integer) data);
        break;
      case ConstVar.FieldType_Long:
        bytes.writeLong((Long) data);
        break;
      case ConstVar.FieldType_Float:
        bytes.writeFloat((Float) data);
        break;
      case ConstVar.FieldType_Double:
        bytes.writeDouble((Double) data);
        break;
      case ConstVar.FieldType_String:
        ByteBuffer bb = Text.encode((String) data);
        int len = bb.limit();
        if (len > Short.MAX_VALUE) {
          System.out.println("String field length is : " + len
              + ", which is bigger than " + Short.MAX_VALUE
              + " so the value is set to blank ");
          System.out.println(((String) data).substring(0,
              Math.min(((String) data).length(), Short.MAX_VALUE)));
          LOG.info("a record bigger than " + Short.MAX_VALUE
              + " and has been set to blank");
          len = 0;
        }
        bytes.writeShort(len);
        bytes.write(bb.array(), 0, len);
        break;
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.type = new IFType();
      this.type.readFields(in);
      this.unpersistent(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      this.type.write(out);
      this.persistent(out);
    }

    @Override
    public int compareTo(IFValue ifv) {
      if (this.data == null && ifv.data == null)
        return 0;
      else if (this.data == null)
        return -1;
      switch (this.type.type) {
      case ConstVar.FieldType_Byte:
        return (Byte) this.data - (Byte) ifv.data;
      case ConstVar.FieldType_Short:
        return (Short) this.data - (Short) ifv.data;
      case ConstVar.FieldType_Int:
        return (Integer) this.data - (Integer) ifv.data;
      case ConstVar.FieldType_Long:
        return (int) ((Long) this.data - (Long) ifv.data);
      case ConstVar.FieldType_Float:
        float f1 = (Float) this.data;
        float f2 = (Float) ifv.data;
        if (f1 == f2)
          return 0;
        return f1 > f2 ? 1 : -1;
      case ConstVar.FieldType_Double:
        double d1 = (Double) this.data;
        double d2 = (Double) ifv.data;
        if (d1 == d2)
          return 0;
        return d1 > d2 ? 1 : -1;
      case ConstVar.FieldType_String:
        return ((String) this.data).compareTo((String) ifv.data);
      }
      return 0;
    }

    @Override
    public IRecord.IFValue clone() {
      Object cdata = null;
      switch (this.type.type) {
      case ConstVar.FieldType_Byte:
        cdata = new Byte((Byte) this.data);
        break;
      case ConstVar.FieldType_Short:
        cdata = new Short((Short) this.data);
        break;
      case ConstVar.FieldType_Int:
        cdata = new Integer((Integer) this.data);
        break;
      case ConstVar.FieldType_Long:
        cdata = new Long((Long) this.data);
        break;
      case ConstVar.FieldType_Float:
        cdata = new Float((Float) this.data);
        break;
      case ConstVar.FieldType_Double:
        cdata = new Double((Double) this.data);
        break;
      case ConstVar.FieldType_String:
        cdata = new String((String) this.data);
        break;
      }
      return new IFValue((IFType) this.type.clone(), cdata);
    }

    public String show() {
      return String.valueOf(this.data);
    }

    public Object fieldValue(byte type) {
      return TypeConvertUtil.convert(this.type.type, type, this.data);
    }

  }

  public static void main(String[] args) {
    int num = Integer.parseInt(args[0]);

    String[] tts = { "tinyint", "smallint", "int", "bigint", "float", "double",
        "string" };
    byte[] types = { ConstVar.FieldType_Byte, ConstVar.FieldType_Short,
        ConstVar.FieldType_Int, ConstVar.FieldType_Long,
        ConstVar.FieldType_Float, ConstVar.FieldType_Double,
        ConstVar.FieldType_String };

    long time = System.currentTimeMillis();
    for (int k = 0; k < tts.length; k++) {
      for (int j = 0; j < types.length; j++) {
        for (int i = 0; i < num; i++) {
        }
      }
    }
    System.out.println(System.currentTimeMillis() - time);

  }
}
