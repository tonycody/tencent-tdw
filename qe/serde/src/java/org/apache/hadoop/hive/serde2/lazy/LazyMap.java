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

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.Text;

public class LazyMap extends LazyNonPrimitive<LazyMapObjectInspector> {

  boolean parsed = false;

  int mapSize = 0;

  int[] keyStart;

  int[] keyEnd;

  LazyPrimitive<?, ?>[] keyObjects;

  boolean[] keyInited;

  LazyObject[] valueObjects;

  boolean[] valueInited;

  protected LazyMap(LazyMapObjectInspector oi) {
    super(oi);
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    super.init(bytes, start, length);
    parsed = false;
  }

  protected void enlargeArrays() {
    if (keyStart == null) {
      int initialSize = 2;
      keyStart = new int[initialSize];
      keyEnd = new int[initialSize];
      keyObjects = new LazyPrimitive<?, ?>[initialSize];
      valueObjects = new LazyObject[initialSize];
      keyInited = new boolean[initialSize];
      valueInited = new boolean[initialSize];
    } else {
      keyStart = Arrays.copyOf(keyStart, keyStart.length * 2);
      keyEnd = Arrays.copyOf(keyEnd, keyEnd.length * 2);
      keyObjects = Arrays.copyOf(keyObjects, keyObjects.length * 2);
      valueObjects = Arrays.copyOf(valueObjects, valueObjects.length * 2);
      keyInited = Arrays.copyOf(keyInited, keyInited.length * 2);
      valueInited = Arrays.copyOf(valueInited, valueInited.length * 2);
    }
  }

  private void parse() {
    parsed = true;

    byte itemSeparator = oi.getItemSeparator();
    byte keyValueSeparator = oi.getKeyValueSeparator();
    boolean isEscaped = oi.isEscaped();
    byte escapeChar = oi.getEscapeChar();

    if (length == 0) {
      mapSize = 0;
      return;
    }

    mapSize = 0;
    int arrayByteEnd = start + length;
    int elementByteBegin = start;
    int keyValueSeparatorPosition = -1;
    int elementByteEnd = start;
    byte[] bytes = this.bytes.getData();

    while (elementByteEnd <= arrayByteEnd) {
      if (elementByteEnd == arrayByteEnd
          || bytes[elementByteEnd] == itemSeparator) {
        if (keyStart == null || mapSize + 1 == keyStart.length) {
          enlargeArrays();
        }
        keyStart[mapSize] = elementByteBegin;
        keyEnd[mapSize] = (keyValueSeparatorPosition == -1 ? elementByteEnd
            : keyValueSeparatorPosition);
        keyValueSeparatorPosition = -1;
        mapSize++;
        elementByteBegin = elementByteEnd + 1;
        elementByteEnd++;
      } else {
        if (keyValueSeparatorPosition == -1
            && bytes[elementByteEnd] == keyValueSeparator) {
          keyValueSeparatorPosition = elementByteEnd;
        }
        if (isEscaped && bytes[elementByteEnd] == escapeChar
            && elementByteEnd + 1 < arrayByteEnd) {
          elementByteEnd += 2;
        } else {
          elementByteEnd++;
        }
      }
    }

    keyStart[mapSize] = arrayByteEnd + 1;

    if (mapSize > 0) {
      Arrays.fill(keyInited, 0, mapSize, false);
      Arrays.fill(valueInited, 0, mapSize, false);
    }
  }

  public Object getMapValueElement(Object key) {
    if (!parsed) {
      parse();
    }
    for (int i = 0; i < mapSize; i++) {
      LazyPrimitive<?, ?> lazyKeyI = uncheckedGetKey(i);
      if (lazyKeyI == null)
        continue;
      Object keyI = lazyKeyI.getWritableObject();
      if (keyI == null)
        continue;
      if (keyI.equals(key)) {
        LazyObject v = uncheckedGetValue(i);
        return v == null ? v : v.getObject();
      }
    }

    return null;
  }

  private LazyObject uncheckedGetValue(int index) {
    Text nullSequence = oi.getNullSequence();
    int valueIBegin = keyEnd[index] + 1;
    int valueILength = keyStart[index + 1] - 1 - valueIBegin;
    if (valueILength < 0
        || ((valueILength == nullSequence.getLength()) && 0 == LazyUtils
            .compare(bytes.getData(), valueIBegin, valueILength,
                nullSequence.getBytes(), 0, nullSequence.getLength()))) {
      return null;
    }
    if (!valueInited[index]) {
      valueInited[index] = true;
      if (valueObjects[index] == null) {
        valueObjects[index] = LazyFactory
            .createLazyObject(((MapObjectInspector) oi)
                .getMapValueObjectInspector());
      }
      valueObjects[index].init(bytes, valueIBegin, valueILength);
    }
    return valueObjects[index];
  }

  private LazyPrimitive<?, ?> uncheckedGetKey(int index) {
    Text nullSequence = oi.getNullSequence();
    int keyIBegin = keyStart[index];
    int keyILength = keyEnd[index] - keyStart[index];
    if (keyILength < 0
        || ((keyILength == nullSequence.getLength()) && 0 == LazyUtils.compare(
            bytes.getData(), keyIBegin, keyILength, nullSequence.getBytes(), 0,
            nullSequence.getLength()))) {
      return null;
    }
    if (!keyInited[index]) {
      keyInited[index] = true;
      if (keyObjects[index] == null) {
        keyObjects[index] = LazyFactory
            .createLazyPrimitiveClass((PrimitiveObjectInspector) ((MapObjectInspector) oi)
                .getMapKeyObjectInspector());
      }
      keyObjects[index].init(bytes, keyIBegin, keyILength);
    }
    return keyObjects[index];
  }

  LinkedHashMap<Object, Object> cachedMap;

  public Map<Object, Object> getMap() {
    if (!parsed) {
      parse();
    }
    if (cachedMap == null) {
      cachedMap = new LinkedHashMap<Object, Object>();
    } else {
      cachedMap.clear();
    }

    for (int i = 0; i < mapSize; i++) {
      LazyPrimitive<?, ?> lazyKey = uncheckedGetKey(i);
      if (lazyKey == null)
        continue;
      Object key = lazyKey.getObject();
      if (key != null && !cachedMap.containsKey(key)) {
        LazyObject lazyValue = uncheckedGetValue(i);
        Object value = (lazyValue == null ? null : lazyValue.getObject());
        cachedMap.put(key, value);
      }
    }
    return cachedMap;
  }

  public int getMapSize() {
    if (!parsed) {
      parse();
    }
    return mapSize;
  }

}
