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
package org.apache.hadoop.hive.serde2.lazy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.Text;

public class LazyStruct extends
    LazyNonPrimitive<LazySimpleStructObjectInspector> {

  private static Log LOG = LogFactory.getLog(LazyStruct.class.getName());

  boolean parsed;

  int[] startPosition;

  LazyObject[] fields;

  boolean[] fieldInited;

  boolean haserror1 = false;

  public LazyStruct(LazySimpleStructObjectInspector oi) {
    super(oi);
  }

  public void init(ByteArrayRef bytes, int start, int length) {
    super.init(bytes, start, length);
    parsed = false;
  }

  public void init1(ByteArrayRef byteArrayRef, int i, int length) {
    this.haserror1 = false;
    cachedListGot = false;
    this.init(byteArrayRef, i, length);
    this.parse();
    this.getFieldsAsList();
  }

  boolean missingFieldWarned = false;
  boolean extraFieldWarned = false;

  private void parse() {

    byte separator = oi.getSeparator();
    boolean lastColumnTakesRest = oi.getLastColumnTakesRest();
    boolean isEscaped = oi.isEscaped();
    byte escapeChar = oi.getEscapeChar();

    if (fields == null) {
      List<? extends StructField> fieldRefs = ((StructObjectInspector) oi)
          .getAllStructFieldRefs();
      fields = new LazyObject[fieldRefs.size()];
      for (int i = 0; i < fields.length; i++) {
        fields[i] = LazyFactory.createLazyObject(fieldRefs.get(i)
            .getFieldObjectInspector());
      }
      fieldInited = new boolean[fields.length];
      startPosition = new int[fields.length + 1];
    }

    int structByteEnd = start + length;
    int fieldId = 0;
    int fieldByteBegin = start;
    int fieldByteEnd = start;
    byte[] bytes = this.bytes.getData();

    while (fieldByteEnd <= structByteEnd) {
      if (fieldByteEnd == structByteEnd || bytes[fieldByteEnd] == separator) {
        if (lastColumnTakesRest && fieldId == fields.length - 1) {
          fieldByteEnd = structByteEnd;
        }
        startPosition[fieldId] = fieldByteBegin;
        fieldId++;
        if (fieldId == fields.length || fieldByteEnd == structByteEnd) {

          for (int i = fieldId; i <= fields.length; i++) {
            startPosition[i] = fieldByteEnd + 1;
          }

          break;
        }
        fieldByteBegin = fieldByteEnd + 1;
        fieldByteEnd++;
      } else {
        if (isEscaped
            && bytes[fieldByteEnd] == escapeChar
            && fieldByteEnd + 1 < structByteEnd
            && !(bytes[fieldByteEnd + 1] == separator && bytes[fieldByteEnd] == (byte) 1)) {
          fieldByteEnd += 2;
        } else {
          fieldByteEnd++;
        }
      }
    }

    if (!extraFieldWarned && fieldByteEnd < structByteEnd) {
      extraFieldWarned = true;
      LOG.warn("Extra bytes detected at the end of the row! Ignoring similar "
          + "problems.");
    }

    if (!missingFieldWarned && fieldId < fields.length) {
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
    Text nullSequence = oi.getNullSequence();
    int fieldByteBegin = startPosition[fieldID];
    int fieldLength = startPosition[fieldID + 1] - startPosition[fieldID] - 1;
    if ((fieldLength < 0)
        || (fieldLength == nullSequence.getLength() && LazyUtils.compare(
            bytes.getData(), fieldByteBegin, fieldLength,
            nullSequence.getBytes(), 0, nullSequence.getLength()) == 0)) {
      return null;
    }
    if (!fieldInited[fieldID]) {
      fieldInited[fieldID] = true;
      fields[fieldID].init(bytes, fieldByteBegin, fieldLength);
      if (fieldLength > 0 && fields[fieldID] instanceof LazyPrimitive
          && ((LazyPrimitive) fields[fieldID]).isNull) {
        haserror1 = true;
      }
    }
    return fields[fieldID].getObject();
  }

  ArrayList<Object> cachedList;
  boolean cachedListGot = false;

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
    cachedListGot = true;
    return cachedList;
  }

  @Override
  public Object getObject() {
    return this;
  }

}
