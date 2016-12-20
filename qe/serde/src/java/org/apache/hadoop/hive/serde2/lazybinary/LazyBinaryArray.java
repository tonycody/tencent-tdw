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

import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.RecordInfo;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.VInt;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class LazyBinaryArray extends
    LazyBinaryNonPrimitive<LazyBinaryListObjectInspector> {

  boolean parsed = false;

  int arraySize = 0;

  int[] elementStart;
  int[] elementLength;

  boolean[] elementInited;

  boolean[] elementIsNull;

  LazyBinaryObject[] arrayElements;

  protected LazyBinaryArray(LazyBinaryListObjectInspector oi) {
    super(oi);
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    super.init(bytes, start, length);
    parsed = false;
  }

  private void adjustArraySize(int newSize) {
    if (elementStart == null || elementStart.length < newSize) {
      elementStart = new int[newSize];
      elementLength = new int[newSize];
      elementInited = new boolean[newSize];
      elementIsNull = new boolean[newSize];
      arrayElements = new LazyBinaryObject[newSize];
    }
  }

  VInt vInt = new LazyBinaryUtils.VInt();
  RecordInfo recordInfo = new LazyBinaryUtils.RecordInfo();

  private void parse() {

    byte[] bytes = this.bytes.getData();

    LazyBinaryUtils.readVInt(bytes, start, vInt);
    arraySize = vInt.value;
    if (0 == arraySize) {
      parsed = true;
      return;
    }

    adjustArraySize(arraySize);
    int arryByteStart = start + vInt.length;
    int nullByteCur = arryByteStart;
    int nullByteEnd = arryByteStart + (arraySize + 7) / 8;
    int lastElementByteEnd = nullByteEnd;
    ObjectInspector listEleObjectInspector = ((ListObjectInspector) oi)
        .getListElementObjectInspector();
    for (int i = 0; i < arraySize; i++) {
      elementIsNull[i] = true;
      if ((bytes[nullByteCur] & (1 << (i % 8))) != 0) {
        elementIsNull[i] = false;
        LazyBinaryUtils.checkObjectByteInfo(listEleObjectInspector, bytes,
            lastElementByteEnd, recordInfo);
        elementStart[i] = lastElementByteEnd + recordInfo.elementOffset;
        elementLength[i] = recordInfo.elementSize;
        lastElementByteEnd = elementStart[i] + elementLength[i];
      }
      if (7 == (i % 8)) {
        nullByteCur++;
      }
    }

    Arrays.fill(elementInited, 0, arraySize, false);
    parsed = true;
  }

  public Object getListElementObject(int index) {
    if (!parsed) {
      parse();
    }
    if (index < 0 || index >= arraySize) {
      return null;
    }
    return uncheckedGetElement(index);
  }

  private Object uncheckedGetElement(int index) {

    if (elementIsNull[index]) {
      return null;
    } else {
      if (!elementInited[index]) {
        elementInited[index] = true;
        if (arrayElements[index] == null) {
          arrayElements[index] = LazyBinaryFactory
              .createLazyBinaryObject(((LazyBinaryListObjectInspector) oi)
                  .getListElementObjectInspector());
        }
        arrayElements[index].init(bytes, elementStart[index],
            elementLength[index]);
      }
    }
    return arrayElements[index].getObject();
  }

  public int getListLength() {
    if (!parsed) {
      parse();
    }
    return arraySize;
  }

  ArrayList<Object> cachedList;

  public List<Object> getList() {
    if (!parsed) {
      parse();
    }
    if (cachedList == null) {
      cachedList = new ArrayList<Object>(arraySize);
    } else {
      cachedList.clear();
    }
    for (int index = 0; index < arraySize; index++) {
      cachedList.add(uncheckedGetElement(index));
    }
    return cachedList;
  }
}
