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
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.io.Text;

public class LazyArray extends LazyNonPrimitive<LazyListObjectInspector> {
  
  public static final Log LOG = LogFactory.getLog(LazyArray.class
      .getName());

  boolean parsed = false;

  int arrayLength = 0;

  int[] startPosition;

  boolean[] elementInited;

  LazyObject[] arrayElements;

  protected LazyArray(LazyListObjectInspector oi) {
    super(oi);
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    super.init(bytes, start, length);
    parsed = false;
  }

  private void enlargeArrays() {
    if (startPosition == null) {
      int initialSize = 2;
      startPosition = new int[initialSize];
      arrayElements = new LazyObject[initialSize];
      elementInited = new boolean[initialSize];
    } else {
      startPosition = Arrays.copyOf(startPosition, startPosition.length * 2);
      arrayElements = Arrays.copyOf(arrayElements, arrayElements.length * 2);
      elementInited = Arrays.copyOf(elementInited, elementInited.length * 2);
    }
  }

  private void parse() {
    parsed = true;

    byte separator = oi.getSeparator();
    boolean isEscaped = oi.isEscaped();
    byte escapeChar = oi.getEscapeChar();

    if (length == 0) {
      arrayLength = 0;
      return;
    }

    byte[] bytes = this.bytes.getData();

    arrayLength = 0;
    int arrayByteEnd = start + length;
    int elementByteBegin = start;
    int elementByteEnd = start;

    while (elementByteEnd <= arrayByteEnd) {
      if (elementByteEnd == arrayByteEnd || bytes[elementByteEnd] == separator) {
        if (startPosition == null || arrayLength + 1 == startPosition.length) {
          enlargeArrays();
        }
        startPosition[arrayLength] = elementByteBegin;
        arrayLength++;
        elementByteBegin = elementByteEnd + 1;
        elementByteEnd++;
      } else {
        if (isEscaped && bytes[elementByteEnd] == escapeChar
            && elementByteEnd + 1 < arrayByteEnd) {
          elementByteEnd += 2;
        } else {
          elementByteEnd++;
        }
      }
    }
    startPosition[arrayLength] = arrayByteEnd + 1;

    if (arrayLength > 0) {
      Arrays.fill(elementInited, 0, arrayLength, false);
    }

  }

  public Object getListElementObject(int index) {
    if (!parsed) {
      parse();
    }
    if (index < 0 || index >= arrayLength) {
      return null;
    }
    return uncheckedGetElement(index);
  }

  private Object uncheckedGetElement(int index) {
    Text nullSequence = oi.getNullSequence();

    int elementLength = startPosition[index + 1] - startPosition[index] - 1;
    if (elementLength == nullSequence.getLength()
        && 0 == LazyUtils
            .compare(bytes.getData(), startPosition[index], elementLength,
                nullSequence.getBytes(), 0, nullSequence.getLength())) {
      return null;
    } else {
      if (!elementInited[index]) {
        elementInited[index] = true;
        if (arrayElements[index] == null) {
          arrayElements[index] = LazyFactory
              .createLazyObject(((ListObjectInspector) oi)
                  .getListElementObjectInspector());
        }
        arrayElements[index].init(bytes, startPosition[index], elementLength);
      }
    }
    return arrayElements[index].getObject();
  }

  public int getListLength() {
    if (!parsed) {
      parse();
    }
    return arrayLength;
  }

  ArrayList<Object> cachedList;

  public List<Object> getList() {
    if (!parsed) {
      parse();
    }
    if (arrayLength == -1) {
      return null;
    }
    if (cachedList == null) {
      cachedList = new ArrayList<Object>(arrayLength);
    } else {
      cachedList.clear();
    }
    for (int index = 0; index < arrayLength; index++) {
      cachedList.add(uncheckedGetElement(index));
    }
    return cachedList;
  }
}
