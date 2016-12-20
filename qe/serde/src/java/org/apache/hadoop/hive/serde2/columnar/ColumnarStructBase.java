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

package org.apache.hadoop.hive.serde2.columnar;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.SerDeStatsStruct;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazy.LazyObjectBase;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public abstract class ColumnarStructBase implements SerDeStatsStruct {

  class FieldInfo {
    LazyObjectBase field;

    ByteArrayRef cachedByteArrayRef;
    BytesRefWritable rawBytesField;
    boolean inited;
    boolean fieldSkipped;
    ObjectInspector objectInspector;

    public FieldInfo(LazyObjectBase lazyObject, boolean fieldSkipped,
        ObjectInspector oi) {
      field = lazyObject;
      cachedByteArrayRef = new ByteArrayRef();
      objectInspector = oi;
      if (fieldSkipped) {
        this.fieldSkipped = true;
        inited = true;
      } else {
        inited = false;
      }
    }

    public void init(BytesRefWritable col) {
      if (col != null) {
        rawBytesField = col;
        inited = false;
        fieldSkipped = false;
      } else {
        fieldSkipped = true;
      }
    }

    public long getSerializedSize() {
      if (rawBytesField == null) {
        return 0;
      }
      return rawBytesField.getLength();
    }

    protected Object uncheckedGetField() {
      if (fieldSkipped) {
        return null;
      }
      if (!inited) {
        try {
          cachedByteArrayRef.setData(rawBytesField.getData());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        inited = true;
        int byteLength = getLength(objectInspector, cachedByteArrayRef,
            rawBytesField.getStart(), rawBytesField.getLength());
        if (byteLength == -1) {
          return null;
        }

        field.init(cachedByteArrayRef, rawBytesField.getStart(), byteLength);
        return field.getObject();
      } else {
        if (getLength(objectInspector, cachedByteArrayRef,
            rawBytesField.getStart(), rawBytesField.getLength()) == -1) {
          return null;
        }
        return field.getObject();
      }
    }
  }

  protected int[] prjColIDs = null;
  private FieldInfo[] fieldInfoList = null;
  private ArrayList<Object> cachedList;

  public ColumnarStructBase(ObjectInspector oi,
      ArrayList<Integer> notSkippedColumnIDs) {
    List<? extends StructField> fieldRefs = ((StructObjectInspector) oi)
        .getAllStructFieldRefs();
    int num = fieldRefs.size();

    fieldInfoList = new FieldInfo[num];

    if (notSkippedColumnIDs == null || notSkippedColumnIDs.size() == 0) {
      for (int i = 0; i < num; i++) {
        notSkippedColumnIDs.add(i);
      }
    }

    for (int i = 0; i < num; i++) {
      ObjectInspector foi = fieldRefs.get(i).getFieldObjectInspector();
      fieldInfoList[i] = new FieldInfo(createLazyObjectBase(foi),
          !notSkippedColumnIDs.contains(i), foi);
    }

    int min = notSkippedColumnIDs.size() > num ? num : notSkippedColumnIDs
        .size();
    prjColIDs = new int[min];
    for (int i = 0, index = 0; i < notSkippedColumnIDs.size(); ++i) {
      int readCol = notSkippedColumnIDs.get(i).intValue();
      if (readCol < num) {
        prjColIDs[index] = readCol;
        index++;
      }
    }
  }

  public Object getField(int fieldID) {
    return fieldInfoList[fieldID].uncheckedGetField();
  }

  protected abstract int getLength(ObjectInspector objectInspector,
      ByteArrayRef cachedByteArrayRef, int start, int length);

  protected abstract LazyObjectBase createLazyObjectBase(
      ObjectInspector objectInspector);

  public void init(BytesRefArrayWritable cols) {
    for (int i = 0; i < prjColIDs.length; ++i) {
      int fieldIndex = prjColIDs[i];
      if (fieldIndex < cols.size()) {
        fieldInfoList[fieldIndex].init(cols.unCheckedGet(fieldIndex));
      } else {
        fieldInfoList[fieldIndex].init(null);
      }
    }
  }

  public ArrayList<Object> getFieldsAsList() {
    if (cachedList == null) {
      cachedList = new ArrayList<Object>();
    } else {
      cachedList.clear();
    }
    for (int i = 0; i < fieldInfoList.length; i++) {
      cachedList.add(fieldInfoList[i].uncheckedGetField());
    }
    return cachedList;
  }

  public long getRawDataSerializedSize() {
    long serializedSize = 0;
    for (int i = 0; i < fieldInfoList.length; ++i) {
      serializedSize += fieldInfoList[i].getSerializedSize();
    }
    return serializedSize;
  }

}
