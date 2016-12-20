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
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.lazybinary.objectinspector.LazyBinaryObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.WritableUtils;

public class LazyBinaryUtils {

  public static int byteArrayToInt(byte[] b, int offset) {
    int value = 0;
    for (int i = 0; i < 4; i++) {
      int shift = (4 - 1 - i) * 8;
      value += (b[i + offset] & 0x000000FF) << shift;
    }
    return value;
  }

  public static long byteArrayToLong(byte[] b, int offset) {
    long value = 0;
    for (int i = 0; i < 8; i++) {
      int shift = (8 - 1 - i) * 8;
      value += ((long) (b[i + offset] & 0x00000000000000FF)) << shift;
    }
    return value;
  }

  public static short byteArrayToShort(byte[] b, int offset) {
    short value = 0;
    value += (b[offset] & 0x000000FF) << 8;
    value += (b[offset + 1] & 0x000000FF);
    return value;
  }

  public static class RecordInfo {
    public RecordInfo() {
      elementOffset = 0;
      elementSize = 0;
    }

    public byte elementOffset;
    public int elementSize;
  }

  static VInt vInt = new LazyBinaryUtils.VInt();

  public static void checkObjectByteInfo(ObjectInspector objectInspector,
      byte[] bytes, int offset, RecordInfo recordInfo) {
    Category category = objectInspector.getCategory();
    switch (category) {
    case PRIMITIVE:
      PrimitiveCategory primitiveCategory = ((PrimitiveObjectInspector) objectInspector)
          .getPrimitiveCategory();
      switch (primitiveCategory) {
      case VOID:
        recordInfo.elementOffset = 0;
        recordInfo.elementSize = 0;
        break;
      case BOOLEAN:
      case BYTE:
        recordInfo.elementOffset = 0;
        recordInfo.elementSize = 1;
        break;
      case SHORT:
        recordInfo.elementOffset = 0;
        recordInfo.elementSize = 2;
        break;
      case FLOAT:
        recordInfo.elementOffset = 0;
        recordInfo.elementSize = 4;
        break;
      case DOUBLE:
        recordInfo.elementOffset = 0;
        recordInfo.elementSize = 8;
        break;
      case INT:
        recordInfo.elementOffset = 0;
        recordInfo.elementSize = WritableUtils.decodeVIntSize(bytes[offset]);
        break;
      case LONG:
        recordInfo.elementOffset = 0;
        recordInfo.elementSize = WritableUtils.decodeVIntSize(bytes[offset]);
        break;
      case STRING:
        LazyBinaryUtils.readVInt(bytes, offset, vInt);
        recordInfo.elementOffset = vInt.length;
        recordInfo.elementSize = vInt.value;
        break;
      case TIMESTAMP:
        recordInfo.elementOffset = 0;
        recordInfo.elementSize = 4;
        if (TimestampWritable.hasDecimal(bytes[offset])) {
          recordInfo.elementSize += (byte) WritableUtils
              .decodeVIntSize(bytes[offset + 4]);
        }
        break;
      default: {
        throw new RuntimeException("Unrecognized primitive type: "
            + primitiveCategory);
      }
      }
      break;
    case LIST:
    case MAP:
    case STRUCT:
      recordInfo.elementOffset = 4;
      recordInfo.elementSize = LazyBinaryUtils.byteArrayToInt(bytes, offset);
      break;
    default: {
      throw new RuntimeException("Unrecognized non-primitive type: " + category);
    }
    }
  }

  public static class VLong {
    public VLong() {
      value = 0;
      length = 0;
    }

    public long value;
    public byte length;
  };

  public static void readVLong(byte[] bytes, int offset, VLong vlong) {
    byte firstByte = bytes[offset];
    vlong.length = (byte) WritableUtils.decodeVIntSize(firstByte);
    if (vlong.length == 1) {
      vlong.value = firstByte;
      return;
    }
    long i = 0;
    for (int idx = 0; idx < vlong.length - 1; idx++) {
      byte b = bytes[offset + 1 + idx];
      i = i << 8;
      i = i | (b & 0xFF);
    }
    vlong.value = (WritableUtils.isNegativeVInt(firstByte) ? (i ^ -1L) : i);
  }

  public static class VInt {
    public VInt() {
      value = 0;
      length = 0;
    }

    public int value;
    public byte length;
  };

  public static void readVInt(byte[] bytes, int offset, VInt vInt) {
    byte firstByte = bytes[offset];
    vInt.length = (byte) WritableUtils.decodeVIntSize(firstByte);
    if (vInt.length == 1) {
      vInt.value = firstByte;
      return;
    }
    int i = 0;
    for (int idx = 0; idx < vInt.length - 1; idx++) {
      byte b = bytes[offset + 1 + idx];
      i = i << 8;
      i = i | (b & 0xFF);
    }
    vInt.value = (WritableUtils.isNegativeVInt(firstByte) ? (i ^ -1) : i);
  }

  public static void writeVInt(Output byteStream, int i) {
    writeVLong(byteStream, i);
  }

  public static int writeVLongToByteArray(byte[] bytes, long l) {
    return LazyBinaryUtils.writeVLongToByteArray(bytes, 0, l);
  }

  public static int writeVLongToByteArray(byte[] bytes, int offset, long l) {
    if (l >= -112 && l <= 127) {
      bytes[offset] = (byte) l;
      return 1;
    }

    int len = -112;
    if (l < 0) {
      l ^= -1L;
      len = -120;
    }

    long tmp = l;
    while (tmp != 0) {
      tmp = tmp >> 8;
      len--;
    }

    bytes[offset] = (byte) len;

    len = (len < -120) ? -(len + 120) : -(len + 112);

    for (int idx = len; idx != 0; idx--) {
      int shiftbits = (idx - 1) * 8;
      long mask = 0xFFL << shiftbits;
      bytes[offset + 1 - (idx - len)] = (byte) ((l & mask) >> shiftbits);
    }
    return 1 + len;
  }

  private static byte[] vLongBytes = new byte[9];

  public static void writeVLong(Output byteStream, long l) {
    int len = LazyBinaryUtils.writeVLongToByteArray(vLongBytes, l);
    byteStream.write(vLongBytes, 0, len);
  }

  static ConcurrentHashMap<TypeInfo, ObjectInspector> cachedLazyBinaryObjectInspector = new ConcurrentHashMap<TypeInfo, ObjectInspector>();

  public static ObjectInspector getLazyBinaryObjectInspectorFromTypeInfo(
      TypeInfo typeInfo) {
    ObjectInspector result = cachedLazyBinaryObjectInspector.get(typeInfo);
    if (result == null) {
      switch (typeInfo.getCategory()) {
      case PRIMITIVE: {
        result = PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector(((PrimitiveTypeInfo) typeInfo)
                .getPrimitiveCategory());
        break;
      }
      case LIST: {
        ObjectInspector elementObjectInspector = getLazyBinaryObjectInspectorFromTypeInfo(((ListTypeInfo) typeInfo)
            .getListElementTypeInfo());
        result = LazyBinaryObjectInspectorFactory
            .getLazyBinaryListObjectInspector(elementObjectInspector);
        break;
      }
      case MAP: {
        MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
        ObjectInspector keyObjectInspector = getLazyBinaryObjectInspectorFromTypeInfo(mapTypeInfo
            .getMapKeyTypeInfo());
        ObjectInspector valueObjectInspector = getLazyBinaryObjectInspectorFromTypeInfo(mapTypeInfo
            .getMapValueTypeInfo());
        result = LazyBinaryObjectInspectorFactory
            .getLazyBinaryMapObjectInspector(keyObjectInspector,
                valueObjectInspector);
        break;
      }
      case STRUCT: {
        StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
        List<String> fieldNames = structTypeInfo.getAllStructFieldNames();
        List<TypeInfo> fieldTypeInfos = structTypeInfo
            .getAllStructFieldTypeInfos();
        List<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>(
            fieldTypeInfos.size());
        for (int i = 0; i < fieldTypeInfos.size(); i++) {
          fieldObjectInspectors
              .add(getLazyBinaryObjectInspectorFromTypeInfo(fieldTypeInfos
                  .get(i)));
        }
        result = LazyBinaryObjectInspectorFactory
            .getLazyBinaryStructObjectInspector(fieldNames,
                fieldObjectInspectors);
        break;
      }
      default: {
        result = null;
      }
      }
      cachedLazyBinaryObjectInspector.put(typeInfo, result);
    }
    return result;
  }
}
