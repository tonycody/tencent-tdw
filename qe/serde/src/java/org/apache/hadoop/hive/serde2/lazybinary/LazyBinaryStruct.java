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
package org.apache.hadoop.hive.serde2.lazybinary;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.RecordInfo;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class LazyBinaryStruct extends
    LazyBinaryNonPrimitive<LazyBinaryStructObjectInspector> {

  private static Log LOG = LogFactory.getLog(LazyBinaryStruct.class.getName());

  boolean parsed;

  LazyBinaryObject[] fields;

  boolean[] fieldInited;

  boolean[] fieldIsNull;

  int[] fieldStart;
  int[] fieldLength;

  protected LazyBinaryStruct(LazyBinaryStructObjectInspector oi) {
    super(oi);
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    super.init(bytes, start, length);
    parsed = false;
  }

  RecordInfo recordInfo = new LazyBinaryUtils.RecordInfo();
  boolean missingFieldWarned = false;
  boolean extraFieldWarned = false;

  private void parse() {

    List<? extends StructField> fieldRefs = ((StructObjectInspector) oi)
        .getAllStructFieldRefs();

    if (fields == null) {
      fields = new LazyBinaryObject[fieldRefs.size()];
      for (int i = 0; i < fields.length; i++) {
        ObjectInspector insp = fieldRefs.get(i).getFieldObjectInspector();
        fields[i] = insp == null ? null : LazyBinaryFactory
            .createLazyBinaryObject(insp);
      }
      fieldInited = new boolean[fields.length];
      fieldIsNull = new boolean[fields.length];
      fieldStart = new int[fields.length];
      fieldLength = new int[fields.length];
    }

    int fieldId = 0;
    int structByteEnd = start + length;
    byte[] bytes = this.bytes.getData();

    byte nullByte = bytes[start];
    int lastFieldByteEnd = start + 1;
    for (int i = 0; i < fields.length; i++) {
      fieldIsNull[i] = true;
      if ((nullByte & (1 << (i % 8))) != 0) {
        fieldIsNull[i] = false;
        LazyBinaryUtils.checkObjectByteInfo(fieldRefs.get(i)
            .getFieldObjectInspector(), bytes, lastFieldByteEnd, recordInfo);
        fieldStart[i] = lastFieldByteEnd + recordInfo.elementOffset;
        fieldLength[i] = recordInfo.elementSize;
        lastFieldByteEnd = fieldStart[i] + fieldLength[i];
      }

      if (lastFieldByteEnd <= structByteEnd)
        fieldId++;
      if (7 == (i % 8)) {
        if (lastFieldByteEnd < structByteEnd) {
          nullByte = bytes[lastFieldByteEnd];
          lastFieldByteEnd++;
        } else {
          nullByte = 0;
          lastFieldByteEnd++;
        }
      }
    }

    if (!extraFieldWarned && lastFieldByteEnd < structByteEnd) {
      extraFieldWarned = true;
      LOG.warn("Extra bytes detected at the end of the row! Ignoring similar "
          + "problems.");
    }

    if (!missingFieldWarned && lastFieldByteEnd > structByteEnd) {
      missingFieldWarned = true;
      LOG.warn("Missing fields! Expected " + fields.length + " fields but "
          + "only got " + fieldId + "! Ignoring similar problems.");
    }

    Arrays.fill(fieldInited, false);
    parsed = true;
  }

  public Object getField(int fieldID) {
    if (!parsed) {
      parse();
    }
    return uncheckedGetField(fieldID);
  }

  private Object uncheckedGetField(int fieldID) {
    if (fieldIsNull[fieldID]) {
      return null;
    }
    if (!fieldInited[fieldID]) {
      fieldInited[fieldID] = true;
      fields[fieldID].init(bytes, fieldStart[fieldID], fieldLength[fieldID]);
    }
    return fields[fieldID].getObject();
  }

  ArrayList<Object> cachedList;

  public ArrayList<Object> getFieldsAsList() {
    if (!parsed) {
      parse();
    }
    if (cachedList == null) {
      cachedList = new ArrayList<Object>();
    } else {
      cachedList.clear();
    }
    for (int i = 0; i < fields.length; i++) {
      cachedList.add(uncheckedGetField(i));
    }
    return cachedList;
  }

  @Override
  public Object getObject() {
    return this;
  }
}
