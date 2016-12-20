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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazy.LazyObject;
import org.apache.hadoop.hive.serde2.lazy.LazyPrimitive;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.RecordInfo;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.VInt;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

public class LazyBinaryMap extends
    LazyBinaryNonPrimitive<LazyBinaryMapObjectInspector> {

  private static Log LOG = LogFactory.getLog(LazyBinaryMap.class.getName());

  boolean parsed;

  int mapSize = 0;

  int[] keyStart;
  int[] keyLength;
  int[] valueStart;
  int[] valueLength;

  boolean[] keyInited;
  boolean[] valueInited;

  boolean[] keyIsNull;
  boolean[] valueIsNull;

  LazyBinaryPrimitive<?, ?>[] keyObjects;

  LazyBinaryObject[] valueObjects;

  protected LazyBinaryMap(LazyBinaryMapObjectInspector oi) {
    super(oi);
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    super.init(bytes, start, length);
    parsed = false;
  }

  protected void adjustArraySize(int newSize) {
    if (keyStart == null || keyStart.length < newSize) {
      keyStart = new int[newSize];
      keyLength = new int[newSize];
      valueStart = new int[newSize];
      valueLength = new int[newSize];
      keyInited = new boolean[newSize];
      keyIsNull = new boolean[newSize];
      valueInited = new boolean[newSize];
      valueIsNull = new boolean[newSize];
      keyObjects = new LazyBinaryPrimitive<?, ?>[newSize];
      valueObjects = new LazyBinaryObject[newSize];
    }
  }

  boolean nullMapKey = false;
  VInt vInt = new LazyBinaryUtils.VInt();
  RecordInfo recordInfo = new LazyBinaryUtils.RecordInfo();

  private void parse() {

    byte[] bytes = this.bytes.getData();

    LazyBinaryUtils.readVInt(bytes, start, vInt);
    mapSize = vInt.value;
    if (0 == mapSize) {
      parsed = true;
      return;
    }

    adjustArraySize(mapSize);

    int mapByteStart = start + vInt.length;
    int nullByteCur = mapByteStart;
    int nullByteEnd = mapByteStart + (mapSize * 2 + 7) / 8;
    int lastElementByteEnd = nullByteEnd;

    for (int i = 0; i < mapSize; i++) {
      keyIsNull[i] = true;
      if ((bytes[nullByteCur] & (1 << ((i * 2) % 8))) != 0) {
        keyIsNull[i] = false;
        LazyBinaryUtils.checkObjectByteInfo(
            ((MapObjectInspector) oi).getMapKeyObjectInspector(), bytes,
            lastElementByteEnd, recordInfo);
        keyStart[i] = lastElementByteEnd + recordInfo.elementOffset;
        keyLength[i] = recordInfo.elementSize;
        lastElementByteEnd = keyStart[i] + keyLength[i];
      } else if (!nullMapKey) {
        nullMapKey = true;
        LOG.warn("Null map key encountered! Ignoring similar problems.");
      }

      valueIsNull[i] = true;
      if ((bytes[nullByteCur] & (1 << ((i * 2 + 1) % 8))) != 0) {
        valueIsNull[i] = false;
        LazyBinaryUtils.checkObjectByteInfo(
            ((MapObjectInspector) oi).getMapValueObjectInspector(), bytes,
            lastElementByteEnd, recordInfo);
        valueStart[i] = lastElementByteEnd + recordInfo.elementOffset;
        valueLength[i] = recordInfo.elementSize;
        lastElementByteEnd = valueStart[i] + valueLength[i];
      }

      if (3 == (i % 4)) {
        nullByteCur++;
      }
    }

    Arrays.fill(keyInited, 0, mapSize, false);
    Arrays.fill(valueInited, 0, mapSize, false);
    parsed = true;
  }

  private LazyBinaryObject uncheckedGetValue(int index) {
    if (valueIsNull[index]) {
      return null;
    }
    if (!valueInited[index]) {
      valueInited[index] = true;
      if (valueObjects[index] == null) {
        valueObjects[index] = LazyBinaryFactory
            .createLazyBinaryObject(((MapObjectInspector) oi)
                .getMapValueObjectInspector());
      }
      valueObjects[index].init(bytes, valueStart[index], valueLength[index]);
    }
    return valueObjects[index];
  }

  public Object getMapValueElement(Object key) {
    if (!parsed) {
      parse();
    }
    for (int i = 0; i < mapSize; i++) {
      LazyBinaryPrimitive<?, ?> lazyKeyI = uncheckedGetKey(i);
      if (lazyKeyI == null)
        continue;
      Object keyI = lazyKeyI.getWritableObject();
      if (keyI == null)
        continue;
      if (keyI.equals(key)) {
        LazyBinaryObject v = uncheckedGetValue(i);
        return v == null ? v : v.getObject();
      }
    }
    return null;
  }

  private LazyBinaryPrimitive<?, ?> uncheckedGetKey(int index) {
    if (keyIsNull[index]) {
      return null;
    }
    if (!keyInited[index]) {
      keyInited[index] = true;
      if (keyObjects[index] == null) {
        keyObjects[index] = LazyBinaryFactory
            .createLazyBinaryPrimitiveClass((PrimitiveObjectInspector) ((MapObjectInspector) oi)
                .getMapKeyObjectInspector());
      }
      keyObjects[index].init(bytes, keyStart[index], keyLength[index]);
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
      LazyBinaryPrimitive<?, ?> lazyKey = uncheckedGetKey(i);
      if (lazyKey == null)
        continue;
      Object key = lazyKey.getObject();
      if (key != null && !cachedMap.containsKey(key)) {
        LazyBinaryObject lazyValue = uncheckedGetValue(i);
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
