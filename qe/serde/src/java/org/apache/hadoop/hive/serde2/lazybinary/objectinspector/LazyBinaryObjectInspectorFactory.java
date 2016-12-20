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
package org.apache.hadoop.hive.serde2.lazybinary.objectinspector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class LazyBinaryObjectInspectorFactory {

  static ConcurrentHashMap<ArrayList<Object>, LazyBinaryStructObjectInspector> cachedLazyBinaryStructObjectInspector = new ConcurrentHashMap<ArrayList<Object>, LazyBinaryStructObjectInspector>();

  public static LazyBinaryStructObjectInspector getLazyBinaryStructObjectInspector(
      List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors) {
    ArrayList<Object> signature = new ArrayList<Object>();
    signature.add(structFieldNames);
    signature.add(structFieldObjectInspectors);
    LazyBinaryStructObjectInspector result = cachedLazyBinaryStructObjectInspector
        .get(signature);
    if (result == null) {
      result = new LazyBinaryStructObjectInspector(structFieldNames,
          structFieldObjectInspectors);
      cachedLazyBinaryStructObjectInspector.put(signature, result);
    }
    return result;
  }

  static ConcurrentHashMap<ArrayList<Object>, LazyBinaryListObjectInspector> cachedLazyBinaryListObjectInspector = new ConcurrentHashMap<ArrayList<Object>, LazyBinaryListObjectInspector>();

  public static LazyBinaryListObjectInspector getLazyBinaryListObjectInspector(
      ObjectInspector listElementObjectInspector) {
    ArrayList<Object> signature = new ArrayList<Object>();
    signature.add(listElementObjectInspector);
    LazyBinaryListObjectInspector result = cachedLazyBinaryListObjectInspector
        .get(signature);
    if (result == null) {
      result = new LazyBinaryListObjectInspector(listElementObjectInspector);
      cachedLazyBinaryListObjectInspector.put(signature, result);
    }
    return result;
  }

  static ConcurrentHashMap<ArrayList<Object>, LazyBinaryMapObjectInspector> cachedLazyBinaryMapObjectInspector = new ConcurrentHashMap<ArrayList<Object>, LazyBinaryMapObjectInspector>();

  public static LazyBinaryMapObjectInspector getLazyBinaryMapObjectInspector(
      ObjectInspector mapKeyObjectInspector,
      ObjectInspector mapValueObjectInspector) {
    ArrayList<Object> signature = new ArrayList<Object>();
    signature.add(mapKeyObjectInspector);
    signature.add(mapValueObjectInspector);
    LazyBinaryMapObjectInspector result = cachedLazyBinaryMapObjectInspector
        .get(signature);
    if (result == null) {
      result = new LazyBinaryMapObjectInspector(mapKeyObjectInspector,
          mapValueObjectInspector);
      cachedLazyBinaryMapObjectInspector.put(signature, result);
    }
    return result;
  }
}
