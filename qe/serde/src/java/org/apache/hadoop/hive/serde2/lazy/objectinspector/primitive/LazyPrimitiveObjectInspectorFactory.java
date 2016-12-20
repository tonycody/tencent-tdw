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

package org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveJavaObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableVoidObjectInspector;
import org.apache.hadoop.io.Text;

public class LazyPrimitiveObjectInspectorFactory {

  public final static LazyBooleanObjectInspector lazyBooleanObjectInspector = new LazyBooleanObjectInspector();
  public final static LazyByteObjectInspector lazyByteObjectInspector = new LazyByteObjectInspector();
  public final static LazyShortObjectInspector lazyShortObjectInspector = new LazyShortObjectInspector();
  public final static LazyIntObjectInspector lazyIntObjectInspector = new LazyIntObjectInspector();
  public final static LazyLongObjectInspector lazyLongObjectInspector = new LazyLongObjectInspector();
  public final static LazyFloatObjectInspector lazyFloatObjectInspector = new LazyFloatObjectInspector();
  public final static LazyDoubleObjectInspector lazyDoubleObjectInspector = new LazyDoubleObjectInspector();
  public final static LazyVoidObjectInspector lazyVoidObjectInspector = new LazyVoidObjectInspector();
  public final static LazyTimestampObjectInspector LAZY_TIMESTAMP_OBJECT_INSPECTOR = new LazyTimestampObjectInspector();

  static ConcurrentHashMap<ArrayList<Object>, LazyStringObjectInspector> cachedLazyStringObjectInspector = new ConcurrentHashMap<ArrayList<Object>, LazyStringObjectInspector>();

  public static LazyStringObjectInspector getLazyStringObjectInspector(
      boolean escaped, byte escapeChar, boolean gbkcoding) {
    ArrayList<Object> signature = new ArrayList<Object>();
    signature.add(Boolean.valueOf(escaped));
    signature.add(Byte.valueOf(escapeChar));
    signature.add(Boolean.valueOf(gbkcoding));
    LazyStringObjectInspector result = cachedLazyStringObjectInspector
        .get(signature);
    if (result == null) {
      result = new LazyStringObjectInspector(escaped, escapeChar, gbkcoding);
      cachedLazyStringObjectInspector.put(signature, result);
    }
    return result;
  }

  public static AbstractPrimitiveLazyObjectInspector<?> getLazyObjectInspector(
      PrimitiveCategory primitiveCategory, boolean escaped, byte escapeChar,
      boolean gbkcoding) {

    switch (primitiveCategory) {
    case BOOLEAN:
      return lazyBooleanObjectInspector;
    case BYTE:
      return lazyByteObjectInspector;
    case SHORT:
      return lazyShortObjectInspector;
    case INT:
      return lazyIntObjectInspector;
    case LONG:
      return lazyLongObjectInspector;
    case FLOAT:
      return lazyFloatObjectInspector;
    case DOUBLE:
      return lazyDoubleObjectInspector;
    case STRING:
      return getLazyStringObjectInspector(escaped, escapeChar, gbkcoding);
    case VOID:
      return lazyVoidObjectInspector;
    case TIMESTAMP:
      return LAZY_TIMESTAMP_OBJECT_INSPECTOR;
    default:
      throw new RuntimeException("Internal error: Cannot find ObjectInspector "
          + " for " + primitiveCategory);
    }
  }

}
