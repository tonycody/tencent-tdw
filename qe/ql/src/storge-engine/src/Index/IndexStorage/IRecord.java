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
import java.util.TreeMap;

public class IRecord implements IPersistable {

  private static final byte[] dummybytes = new byte[100];
  private TreeMap<Short, IFieldType> fieldTypes;
  private FixedBitSet bitSet;
  private TreeMap<Short, IFieldValue> fieldValues;
  private boolean fixlen = true;

  public IRecord() {
    this.fieldValues = new TreeMap<Short, IFieldValue>();
  }

  public IRecord(TreeMap<Short, IFieldType> fieldTypes) {
    this();
    this.setFieldTypes(fieldTypes);
  }

  public void setFieldTypes(TreeMap<Short, IFieldType> fieldtypes) {
    this.fieldTypes = fieldtypes;
  }

  public boolean addFieldValue(IFieldValue fv) {
    if (this.fieldTypes != null) {
      if (fv.idx() < 0 || !fieldTypes.containsKey(fv.idx())
          || fieldTypes.get(fv.idx()).getType() != fv.type())
        return false;
    }
    if (fv.idx() < 0)
      fv.setIdx((short) fieldValues.size());
    fieldValues.put(fv.idx(), fv);
    return true;
  }

  public TreeMap<Short, IFieldValue> fieldValues() {
    return fieldValues;
  }

  public void setFixLen(boolean fixlen) {
    this.fixlen = fixlen;
  }

  public short len() {
    updatebitset();
    int len = bitSet.needbytes();
    for (IFieldValue fv : fieldValues.values()) {
      len += fv.len();
    }
    return (short) len;
  }

  public void clear() {
    this.bitSet = null;
    this.fieldValues.clear();
  }

  public void show() {
    for (short i = 0; i < this.fieldTypes.size(); i++) {
      IFieldValue fv = fieldValues.get(i);
      if (fv != null) {
        System.out.print(fv.show());
      } else {
        System.out.print("null");
      }
      System.out.print("\t");
    }
    System.out.println();
  }

  private void updatebitset() {
    bitSet = new FixedBitSet(fieldTypes.size());
    for (IFieldValue fv : fieldValues.values()) {
      if (fieldTypes.containsKey(fv.idx()))
        bitSet.set(fv.idx());
    }
  }

  @Override
  public void persistent(DataOutput out) throws IOException {
    if (fieldTypes == null) {
      throw new IOException("IRecord-->fieldTypes is null");
    }
    updatebitset();
    bitSet.persistent(out);
    if (fixlen) {
      for (short i = 0; i < this.fieldTypes.size(); i++) {
        IFieldValue fv = fieldValues.get(i);
        if (fv == null) {
          fv = new IFieldValue(fieldTypes.get(i), dummybytes);
        }
        fv.persistent(out);
      }
    } else {
      for (IFieldValue fv : fieldValues.values()) {
        fv.persistent(out);
      }
    }
  }

  @Override
  public void unpersistent(DataInput in) throws IOException {
    if (fieldTypes == null) {
      throw new IOException("IRecord-->fieldTypes is null");
    }
    bitSet = new FixedBitSet(fieldTypes.size());
    bitSet.unpersistent(in);
    fieldValues = new TreeMap<Short, IFieldValue>();
    if (fixlen) {
      for (short i = 0; i < this.fieldTypes.size(); i++) {
        IFieldType ft = fieldTypes.get(i);
        IFieldValue fv = new IFieldValue(ft);
        fv.unpersistent(in);
        if (bitSet.get(i)) {
          fieldValues.put((short) i, fv);
        }
      }
    } else {
      for (short i = 0; i < this.fieldTypes.size(); i++) {
        IFieldType ft = fieldTypes.get(i);
        IFieldValue fv = new IFieldValue(ft);
        fv.unpersistent(in);
        fieldValues.put((short) i, fv);
      }
    }
  }

  private static class FixedBitSet implements IPersistable {
    private byte[] bytes = null;
    private int needbytes = 0;

    public FixedBitSet(int len) {
      needbytes = len / 8 + 1;
      bytes = new byte[needbytes];
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
      this.bytes = new byte[needbytes];
      in.readFully(bytes, 0, needbytes);
    }
  }

  public String showtype() {
    StringBuffer sb = new StringBuffer();
    StringBuffer sb2 = new StringBuffer();
    sb.append("idx:\t");
    sb2.append("type:\t");
    for (IFieldValue fv : this.fieldValues.values()) {
      sb.append(fv.idx() + "\t");
      sb2.append(fv.type() + "\t");
    }
    sb.append("\r\n").append(sb2.toString()).append("\r\n");
    return sb.toString();
  }

}
