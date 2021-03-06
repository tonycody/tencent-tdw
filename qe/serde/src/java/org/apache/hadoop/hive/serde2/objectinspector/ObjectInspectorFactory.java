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

package org.apache.hadoop.hive.serde2.objectinspector;

import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.swing.event.ListSelectionEvent;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.Text;

public class ObjectInspectorFactory {

  public enum ObjectInspectorOptions {
    JAVA, THRIFT
  };

  private static ConcurrentHashMap<Type, ObjectInspector> objectInspectorCache = new ConcurrentHashMap<Type, ObjectInspector>();

  public static ObjectInspector getReflectionObjectInspector(Type t,
      ObjectInspectorOptions options) {
    ObjectInspector oi = objectInspectorCache.get(t);
    if (oi == null) {
      oi = getReflectionObjectInspectorNoCache(t, options);
      objectInspectorCache.put(t, oi);
    }
    if ((options.equals(ObjectInspectorOptions.JAVA) && oi.getClass().equals(
        ThriftStructObjectInspector.class))
        || (options.equals(ObjectInspectorOptions.THRIFT) && oi.getClass()
            .equals(ReflectionStructObjectInspector.class))) {
      throw new RuntimeException(
          "Cannot call getObjectInspectorByReflection with both JAVA and THRIFT !");
    }
    return oi;
  }

  private static ObjectInspector getReflectionObjectInspectorNoCache(Type t,
      ObjectInspectorOptions options) {
    if (t instanceof GenericArrayType) {
      GenericArrayType at = (GenericArrayType) t;
      return getStandardListObjectInspector(getReflectionObjectInspector(
          at.getGenericComponentType(), options));
    }

    if (t instanceof ParameterizedType) {
      ParameterizedType pt = (ParameterizedType) t;
      if (List.class.isAssignableFrom((Class<?>) pt.getRawType())) {
        return getStandardListObjectInspector(getReflectionObjectInspector(
            pt.getActualTypeArguments()[0], options));
      }
      if (Map.class.isAssignableFrom((Class<?>) pt.getRawType())) {
        return getStandardMapObjectInspector(
            getReflectionObjectInspector(pt.getActualTypeArguments()[0],
                options),
            getReflectionObjectInspector(pt.getActualTypeArguments()[1],
                options));
      }
      t = pt.getRawType();
    }

    if (!(t instanceof Class)) {
      throw new RuntimeException(ObjectInspectorFactory.class.getName()
          + " internal error:" + t);
    }
    Class<?> c = (Class<?>) t;

    if (PrimitiveObjectInspectorUtils.isPrimitiveJavaType(c)) {
      return PrimitiveObjectInspectorFactory
          .getPrimitiveJavaObjectInspector(PrimitiveObjectInspectorUtils
              .getTypeEntryFromPrimitiveJavaType(c).primitiveCategory);
    }

    if (PrimitiveObjectInspectorUtils.isPrimitiveJavaClass(c)) {
      return PrimitiveObjectInspectorFactory
          .getPrimitiveJavaObjectInspector(PrimitiveObjectInspectorUtils
              .getTypeEntryFromPrimitiveJavaClass(c).primitiveCategory);
    }

    if (PrimitiveObjectInspectorUtils.isPrimitiveWritableClass(c)) {
      return PrimitiveObjectInspectorFactory
          .getPrimitiveWritableObjectInspector(PrimitiveObjectInspectorUtils
              .getTypeEntryFromPrimitiveWritableClass(c).primitiveCategory);
    }

    assert (!List.class.isAssignableFrom(c));
    assert (!Map.class.isAssignableFrom(c));

    ReflectionStructObjectInspector oi;
    switch (options) {
    case JAVA:
      oi = new ReflectionStructObjectInspector();
      break;
    case THRIFT:
      oi = new ThriftStructObjectInspector();
      break;
    default:
      throw new RuntimeException(ObjectInspectorFactory.class.getName()
          + ": internal error.");
    }
    objectInspectorCache.put(t, oi);
    Field[] fields = ObjectInspectorUtils.getDeclaredNonStaticFields(c);
    ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>(
        fields.length);
    for (int i = 0; i < fields.length; i++) {
      if (!oi.shouldIgnoreField(fields[i].getName())) {
        structFieldObjectInspectors.add(getReflectionObjectInspector(
            fields[i].getGenericType(), options));
      }
    }
    oi.init(c, structFieldObjectInspectors);
    return oi;
  }

  public static StandardListObjectInspector getStandardListObjectInspector(
      ObjectInspector listElementObjectInspector) {
    StandardListObjectInspector result = new StandardListObjectInspector(
        listElementObjectInspector);
    return result;
  }
  
  public static StandardByteArrayObjectInspector getStandardByteArrayObjectInspector() {
    StandardByteArrayObjectInspector result = new StandardByteArrayObjectInspector();
    return result;    
  }

  static ConcurrentHashMap<List<ObjectInspector>, StandardMapObjectInspector> cachedStandardMapObjectInspector = new ConcurrentHashMap<List<ObjectInspector>, StandardMapObjectInspector>();

  public static StandardMapObjectInspector getStandardMapObjectInspector(
      ObjectInspector mapKeyObjectInspector,
      ObjectInspector mapValueObjectInspector) {
    ArrayList<ObjectInspector> signature = new ArrayList<ObjectInspector>(2);
    signature.add(mapKeyObjectInspector);
    signature.add(mapValueObjectInspector);
    StandardMapObjectInspector result = cachedStandardMapObjectInspector
        .get(signature);
    if (result == null) {
      result = new StandardMapObjectInspector(mapKeyObjectInspector,
          mapValueObjectInspector);
      cachedStandardMapObjectInspector.put(signature, result);
    }
    return result;
  }

  static ConcurrentHashMap<ArrayList<List<?>>, StandardStructObjectInspector> cachedStandardStructObjectInspector = new ConcurrentHashMap<ArrayList<List<?>>, StandardStructObjectInspector>();

  public static StandardStructObjectInspector getStandardStructObjectInspector(
      List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors) {
    ArrayList<List<?>> signature = new ArrayList<List<?>>();
    signature.add(structFieldNames);
    signature.add(structFieldObjectInspectors);
    StandardStructObjectInspector result = cachedStandardStructObjectInspector
        .get(signature);
    if (result == null) {
      result = new StandardStructObjectInspector(structFieldNames,
          structFieldObjectInspectors);
      cachedStandardStructObjectInspector.put(signature, result);
    }
    return result;
  }

  static ConcurrentHashMap<List<StructObjectInspector>, UnionStructObjectInspector> cachedUnionStructObjectInspector = new ConcurrentHashMap<List<StructObjectInspector>, UnionStructObjectInspector>();

  public static UnionStructObjectInspector getUnionStructObjectInspector(
      List<StructObjectInspector> structObjectInspectors) {
    UnionStructObjectInspector result = cachedUnionStructObjectInspector
        .get(structObjectInspectors);
    if (result == null) {
      result = new UnionStructObjectInspector(structObjectInspectors);
      cachedUnionStructObjectInspector.put(structObjectInspectors, result);
    }
    return result;
  }

  static ConcurrentHashMap<ArrayList<Object>, ColumnarStructObjectInspector> cachedColumnarStructObjectInspector = new ConcurrentHashMap<ArrayList<Object>, ColumnarStructObjectInspector>();

  public static ColumnarStructObjectInspector getColumnarStructObjectInspector(
      List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors, Text nullSequence) {
    ArrayList<Object> signature = new ArrayList<Object>();
    signature.add(structFieldNames);
    signature.add(structFieldObjectInspectors);
    signature.add(nullSequence.toString());
    ColumnarStructObjectInspector result = cachedColumnarStructObjectInspector
        .get(signature);
    if (result == null) {
      result = new ColumnarStructObjectInspector(structFieldNames,
          structFieldObjectInspectors, nullSequence);
      cachedColumnarStructObjectInspector.put(signature, result);
    }
    return result;
  }

  static ConcurrentHashMap<List<ObjectInspector>, StandardUnionObjectInspector> cachedStandardUnionObjectInspector = new ConcurrentHashMap<List<ObjectInspector>, StandardUnionObjectInspector>();

  public static StandardUnionObjectInspector getStandardUnionObjectInspector(
      List<ObjectInspector> unionObjectInspectors) {
    StandardUnionObjectInspector result = cachedStandardUnionObjectInspector
        .get(unionObjectInspectors);
    if (result == null) {
      result = new StandardUnionObjectInspector(unionObjectInspectors);
      cachedStandardUnionObjectInspector.put(unionObjectInspectors, result);
    }
    return result;
  }

}
