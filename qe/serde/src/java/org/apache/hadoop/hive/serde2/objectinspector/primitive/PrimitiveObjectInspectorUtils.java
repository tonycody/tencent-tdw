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

package org.apache.hadoop.hive.serde2.objectinspector.primitive;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.sql.Timestamp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;

public class PrimitiveObjectInspectorUtils {

  private static Log LOG = LogFactory
      .getLog(PrimitiveObjectInspectorUtils.class.getName());

  public static class PrimitiveTypeEntry {

    public PrimitiveObjectInspector.PrimitiveCategory primitiveCategory;

    public Class<?> primitiveJavaType;

    public Class<?> primitiveJavaClass;

    public Class<?> primitiveWritableClass;

    public String typeName;

    PrimitiveTypeEntry(
        PrimitiveObjectInspector.PrimitiveCategory primitiveCategory,
        String typeName, Class<?> primitiveType, Class<?> javaClass,
        Class<?> hiveClass) {
      this.primitiveCategory = primitiveCategory;
      this.primitiveJavaType = primitiveType;
      this.primitiveJavaClass = javaClass;
      this.primitiveWritableClass = hiveClass;
      this.typeName = typeName;
    }
  }

  static final Map<PrimitiveCategory, PrimitiveTypeEntry> primitiveCategoryToTypeEntry = new HashMap<PrimitiveCategory, PrimitiveTypeEntry>();
  static final Map<Class<?>, PrimitiveTypeEntry> primitiveJavaTypeToTypeEntry = new HashMap<Class<?>, PrimitiveTypeEntry>();
  static final Map<Class<?>, PrimitiveTypeEntry> primitiveJavaClassToTypeEntry = new HashMap<Class<?>, PrimitiveTypeEntry>();
  static final Map<Class<?>, PrimitiveTypeEntry> primitiveWritableClassToTypeEntry = new HashMap<Class<?>, PrimitiveTypeEntry>();
  static final Map<String, PrimitiveTypeEntry> typeNameToTypeEntry = new HashMap<String, PrimitiveTypeEntry>();

  static void registerType(PrimitiveTypeEntry t) {
    if (t.primitiveCategory != PrimitiveCategory.UNKNOWN) {
      primitiveCategoryToTypeEntry.put(t.primitiveCategory, t);
    }
    if (t.primitiveJavaType != null) {
      primitiveJavaTypeToTypeEntry.put(t.primitiveJavaType, t);
    }
    if (t.primitiveJavaClass != null) {
      primitiveJavaClassToTypeEntry.put(t.primitiveJavaClass, t);
    }
    if (t.primitiveWritableClass != null) {
      primitiveWritableClassToTypeEntry.put(t.primitiveWritableClass, t);
    }
    if (t.typeName != null) {
      typeNameToTypeEntry.put(t.typeName, t);
    }
  }

  public static final PrimitiveTypeEntry stringTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.STRING, Constants.STRING_TYPE_NAME, null, String.class,
      Text.class);
  public static final PrimitiveTypeEntry booleanTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.BOOLEAN, Constants.BOOLEAN_TYPE_NAME, Boolean.TYPE,
      Boolean.class, BooleanWritable.class);
  public static final PrimitiveTypeEntry intTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.INT, Constants.INT_TYPE_NAME, Integer.TYPE,
      Integer.class, IntWritable.class);
  public static final PrimitiveTypeEntry longTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.LONG, Constants.BIGINT_TYPE_NAME, Long.TYPE,
      Long.class, LongWritable.class);
  public static final PrimitiveTypeEntry floatTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.FLOAT, Constants.FLOAT_TYPE_NAME, Float.TYPE,
      Float.class, FloatWritable.class);
  public static final PrimitiveTypeEntry voidTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.VOID, Constants.VOID_TYPE_NAME, Void.TYPE, Void.class,
      NullWritable.class);

  public static final PrimitiveTypeEntry doubleTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.DOUBLE, Constants.DOUBLE_TYPE_NAME, Double.TYPE,
      Double.class, DoubleWritable.class);
  public static final PrimitiveTypeEntry byteTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.BYTE, Constants.TINYINT_TYPE_NAME, Byte.TYPE,
      Byte.class, ByteWritable.class);
  public static final PrimitiveTypeEntry shortTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.SHORT, Constants.SMALLINT_TYPE_NAME, Short.TYPE,
      Short.class, ShortWritable.class);

  public static final PrimitiveTypeEntry unknownTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.UNKNOWN, "unknown", null, Object.class, null);

  public static final PrimitiveTypeEntry timestampTypeEntry = new PrimitiveTypeEntry(
      PrimitiveCategory.TIMESTAMP, Constants.TIMESTAMP_TYPE_NAME, null,
      Object.class, TimestampWritable.class);

  static {
    registerType(stringTypeEntry);
    registerType(booleanTypeEntry);
    registerType(intTypeEntry);
    registerType(longTypeEntry);
    registerType(floatTypeEntry);
    registerType(voidTypeEntry);
    registerType(doubleTypeEntry);
    registerType(byteTypeEntry);
    registerType(shortTypeEntry);
    registerType(unknownTypeEntry);
    registerType(timestampTypeEntry);
  }

  public static Class<?> primitiveJavaTypeToClass(Class<?> clazz) {
    PrimitiveTypeEntry t = primitiveJavaTypeToTypeEntry.get(clazz);
    return t == null ? clazz : t.primitiveJavaClass;
  }

  public static boolean isPrimitiveJava(Class<?> clazz) {
    return primitiveJavaTypeToTypeEntry.get(clazz) != null
        || primitiveJavaClassToTypeEntry.get(clazz) != null;
  }

  public static boolean isPrimitiveJavaType(Class<?> clazz) {
    return primitiveJavaTypeToTypeEntry.get(clazz) != null;
  }

  public static boolean isPrimitiveJavaClass(Class<?> clazz) {
    return primitiveJavaClassToTypeEntry.get(clazz) != null;
  }

  public static boolean isPrimitiveWritableClass(Class<?> clazz) {
    return primitiveWritableClassToTypeEntry.get(clazz) != null;
  }

  public static String getTypeNameFromPrimitiveJava(Class<?> clazz) {
    PrimitiveTypeEntry t = primitiveJavaTypeToTypeEntry.get(clazz);
    if (t == null) {
      t = primitiveJavaClassToTypeEntry.get(clazz);
    }
    return t == null ? null : t.typeName;
  }

  public static String getTypeNameFromPrimitiveWritable(Class<?> clazz) {
    PrimitiveTypeEntry t = primitiveWritableClassToTypeEntry.get(clazz);
    return t == null ? null : t.typeName;
  }

  public static PrimitiveTypeEntry getTypeEntryFromPrimitiveCategory(
      PrimitiveCategory category) {
    return primitiveCategoryToTypeEntry.get(category);
  }

  public static PrimitiveTypeEntry getTypeEntryFromPrimitiveJava(Class<?> clazz) {
    PrimitiveTypeEntry t = primitiveJavaTypeToTypeEntry.get(clazz);
    if (t == null) {
      t = primitiveJavaClassToTypeEntry.get(clazz);
    }
    return t;
  }

  public static PrimitiveTypeEntry getTypeEntryFromPrimitiveJavaType(
      Class<?> clazz) {
    return primitiveJavaTypeToTypeEntry.get(clazz);
  }

  public static PrimitiveTypeEntry getTypeEntryFromPrimitiveJavaClass(
      Class<?> clazz) {
    return primitiveJavaClassToTypeEntry.get(clazz);
  }

  public static PrimitiveTypeEntry getTypeEntryFromPrimitiveWritableClass(
      Class<?> clazz) {
    return primitiveWritableClassToTypeEntry.get(clazz);
  }

  public static PrimitiveTypeEntry getTypeEntryFromTypeName(String typeName) {
    return typeNameToTypeEntry.get(typeName);
  }

  public static boolean comparePrimitiveObjects(Object o1,
      PrimitiveObjectInspector oi1, Object o2, PrimitiveObjectInspector oi2) {
    if (o1 == null || o2 == null)
      return false;

    if (oi1.getPrimitiveCategory() != oi2.getPrimitiveCategory()) {
      return false;
    }
    switch (oi1.getPrimitiveCategory()) {
    case BOOLEAN: {
      return ((BooleanObjectInspector) oi1).get(o1) == ((BooleanObjectInspector) oi2)
          .get(o2);
    }
    case BYTE: {
      return ((ByteObjectInspector) oi1).get(o1) == ((ByteObjectInspector) oi2)
          .get(o2);
    }
    case SHORT: {
      return ((ShortObjectInspector) oi1).get(o1) == ((ShortObjectInspector) oi2)
          .get(o2);
    }
    case INT: {
      return ((IntObjectInspector) oi1).get(o1) == ((IntObjectInspector) oi2)
          .get(o2);
    }
    case LONG: {
      return ((LongObjectInspector) oi1).get(o1) == ((LongObjectInspector) oi2)
          .get(o2);
    }
    case FLOAT: {
      return ((FloatObjectInspector) oi1).get(o1) == ((FloatObjectInspector) oi2)
          .get(o2);
    }
    case DOUBLE: {
      return ((DoubleObjectInspector) oi1).get(o1) == ((DoubleObjectInspector) oi2)
          .get(o2);
    }
    case STRING: {
      Writable t1 = ((StringObjectInspector) oi1)
          .getPrimitiveWritableObject(o1);
      Writable t2 = ((StringObjectInspector) oi2)
          .getPrimitiveWritableObject(o2);
      return t1.equals(t2);
    }
    case TIMESTAMP: {
      return ((TimestampObjectInspector) oi1).getPrimitiveWritableObject(o1)
          .equals(
              ((TimestampObjectInspector) oi2).getPrimitiveWritableObject(o2));
    }
    default:
      return false;
    }
  }

  public static double convertPrimitiveToDouble(Object o,
      PrimitiveObjectInspector oi) throws NumberFormatException {
    switch (oi.getPrimitiveCategory()) {
    case BOOLEAN: {
      return ((BooleanObjectInspector) oi).get(o) ? 1 : 0;
    }
    case BYTE: {
      return ((ByteObjectInspector) oi).get(o);
    }
    case SHORT: {
      return ((ShortObjectInspector) oi).get(o);
    }
    case INT: {
      return ((IntObjectInspector) oi).get(o);
    }
    case LONG: {
      return ((LongObjectInspector) oi).get(o);
    }
    case FLOAT: {
      return ((FloatObjectInspector) oi).get(o);
    }
    case DOUBLE: {
      return ((DoubleObjectInspector) oi).get(o);
    }
    case STRING: {
      return Double.valueOf(((StringObjectInspector) oi)
          .getPrimitiveJavaObject(o));
    }
    case TIMESTAMP:
      return ((TimestampObjectInspector) oi).getPrimitiveWritableObject(o)
          .getDouble();
    default:
      throw new NumberFormatException();
    }
  }

  public static boolean comparePrimitiveObjectsWithConversion(Object o1,
      PrimitiveObjectInspector oi1, Object o2, PrimitiveObjectInspector oi2) {
    if (o1 == null || o2 == null)
      return false;

    if (oi1.getPrimitiveCategory() == oi2.getPrimitiveCategory()) {
      return comparePrimitiveObjects(o1, oi1, o2, oi2);
    }

    try {
      return convertPrimitiveToDouble(o1, oi1) == convertPrimitiveToDouble(o2,
          oi2);
    } catch (NumberFormatException e) {
      return false;
    }
  }

  public static boolean getBoolean(Object o, PrimitiveObjectInspector oi)
      throws NumberFormatException {
    boolean result = false;
    switch (oi.getPrimitiveCategory()) {
    case VOID: {
      result = false;
      break;
    }
    case BOOLEAN: {
      result = ((BooleanObjectInspector) oi).get(o);
      break;
    }
    case BYTE: {
      result = ((ByteObjectInspector) oi).get(o) != 0;
      break;
    }
    case SHORT: {
      result = ((ShortObjectInspector) oi).get(o) != 0;
      break;
    }
    case INT: {
      result = ((IntObjectInspector) oi).get(o) != 0;
      break;
    }
    case LONG: {
      result = (int) ((LongObjectInspector) oi).get(o) != 0;
      break;
    }
    case FLOAT: {
      result = (int) ((FloatObjectInspector) oi).get(o) != 0;
      break;
    }
    case DOUBLE: {
      result = (int) ((DoubleObjectInspector) oi).get(o) != 0;
      break;
    }
    case STRING: {
      StringObjectInspector soi = (StringObjectInspector) oi;
      if (soi.preferWritable()) {
        Text t = soi.getPrimitiveWritableObject(o);
        result = t.getLength() != 0;
      } else {
        String s = soi.getPrimitiveJavaObject(o);
        result = s.length() != 0;
      }
      break;
    }
    case TIMESTAMP:
      result = (((TimestampObjectInspector) oi).getPrimitiveWritableObject(o)
          .getSeconds() != 0);
      break;
    default: {
      throw new RuntimeException("Hive 2 Internal error: unknown type: "
          + oi.getTypeName());
    }
    }
    return result;
  }

  public static byte getByte(Object o, PrimitiveObjectInspector oi)
      throws NumberFormatException {
    return (byte) getInt(o, oi);
  }

  public static short getShort(Object o, PrimitiveObjectInspector oi)
      throws NumberFormatException {
    return (short) getInt(o, oi);
  }

  public static int getInt(Object o, PrimitiveObjectInspector oi)
      throws NumberFormatException {
    int result = 0;
    switch (oi.getPrimitiveCategory()) {
    case VOID: {
      result = 0;
      break;
    }
    case BOOLEAN: {
      result = (((BooleanObjectInspector) oi).get(o) ? 1 : 0);
      break;
    }
    case BYTE: {
      result = ((ByteObjectInspector) oi).get(o);
      break;
    }
    case SHORT: {
      result = ((ShortObjectInspector) oi).get(o);
      break;
    }
    case INT: {
      result = ((IntObjectInspector) oi).get(o);
      break;
    }
    case LONG: {
      result = (int) ((LongObjectInspector) oi).get(o);
      break;
    }
    case FLOAT: {
      result = (int) ((FloatObjectInspector) oi).get(o);
      break;
    }
    case DOUBLE: {
      result = (int) ((DoubleObjectInspector) oi).get(o);
      break;
    }
    case STRING: {
      StringObjectInspector soi = (StringObjectInspector) oi;
      if (soi.preferWritable()) {
        Text t = soi.getPrimitiveWritableObject(o);
        result = LazyInteger.parseInt(t.getBytes(), 0, t.getLength());
      } else {
        String s = soi.getPrimitiveJavaObject(o);
        result = Integer.parseInt(s);
      }
      break;
    }
    case TIMESTAMP:
      result = (int) (((TimestampObjectInspector) oi)
          .getPrimitiveWritableObject(o).getSeconds());
      break;
    default: {
      throw new RuntimeException("Hive 2 Internal error: unknown type: "
          + oi.getTypeName());
    }
    }
    return result;
  }

  public static long getLong(Object o, PrimitiveObjectInspector oi)
      throws NumberFormatException {
    long result = 0;
    switch (oi.getPrimitiveCategory()) {
    case VOID: {
      result = 0;
      break;
    }
    case BOOLEAN: {
      result = (((BooleanObjectInspector) oi).get(o) ? 1 : 0);
      break;
    }
    case BYTE: {
      result = ((ByteObjectInspector) oi).get(o);
      break;
    }
    case SHORT: {
      result = ((ShortObjectInspector) oi).get(o);
      break;
    }
    case INT: {
      result = ((IntObjectInspector) oi).get(o);
      break;
    }
    case LONG: {
      result = ((LongObjectInspector) oi).get(o);
      break;
    }
    case FLOAT: {
      result = (long) ((FloatObjectInspector) oi).get(o);
      break;
    }
    case DOUBLE: {
      result = (long) ((DoubleObjectInspector) oi).get(o);
      break;
    }
    case STRING: {
      StringObjectInspector soi = (StringObjectInspector) oi;
      if (soi.preferWritable()) {
        Text t = soi.getPrimitiveWritableObject(o);
        result = LazyLong.parseLong(t.getBytes(), 0, t.getLength());
      } else {
        String s = soi.getPrimitiveJavaObject(o);
        result = Long.parseLong(s);
      }
      break;
    }
    case TIMESTAMP:
      result = ((TimestampObjectInspector) oi).getPrimitiveWritableObject(o)
          .getSeconds();
      break;
    default: {
      throw new RuntimeException("Hive 2 Internal error: unknown type: "
          + oi.getTypeName());
    }
    }
    return result;
  }

  public static double getDouble(Object o, PrimitiveObjectInspector oi)
      throws NumberFormatException {
    double result = 0;
    switch (oi.getPrimitiveCategory()) {
    case VOID: {
      result = 0;
      break;
    }
    case BOOLEAN: {
      result = (((BooleanObjectInspector) oi).get(o) ? 1 : 0);
      break;
    }
    case BYTE: {
      result = ((ByteObjectInspector) oi).get(o);
      break;
    }
    case SHORT: {
      result = ((ShortObjectInspector) oi).get(o);
      break;
    }
    case INT: {
      result = ((IntObjectInspector) oi).get(o);
      break;
    }
    case LONG: {
      result = ((LongObjectInspector) oi).get(o);
      break;
    }
    case FLOAT: {
      result = ((FloatObjectInspector) oi).get(o);
      break;
    }
    case DOUBLE: {
      result = ((DoubleObjectInspector) oi).get(o);
      break;
    }
    case STRING: {
      StringObjectInspector soi = (StringObjectInspector) oi;
      String s = soi.getPrimitiveJavaObject(o);
      result = Double.parseDouble(s);
      break;
    }
    case TIMESTAMP:
      result = ((TimestampObjectInspector) oi).getPrimitiveWritableObject(o)
          .getDouble();
      break;
    default: {
      throw new RuntimeException("Hive 2 Internal error: unknown type: "
          + oi.getTypeName());
    }
    }
    return result;
  }

  public static float getFloat(Object o, PrimitiveObjectInspector oi)
      throws NumberFormatException {
    return (float) getDouble(o, oi);
  }

  public static String getString(Object o, PrimitiveObjectInspector oi) {

    if (o == null) {
      return null;
    }

    String result = null;
    switch (oi.getPrimitiveCategory()) {
    case VOID:
      result = null;
      break;
    case BOOLEAN:
      result = String.valueOf((((BooleanObjectInspector) oi).get(o)));
      break;
    case BYTE:
      result = String.valueOf((((ByteObjectInspector) oi).get(o)));
      break;
    case SHORT:
      result = String.valueOf((((ShortObjectInspector) oi).get(o)));
      break;
    case INT:
      result = String.valueOf((((IntObjectInspector) oi).get(o)));
      break;
    case LONG:
      result = String.valueOf((((LongObjectInspector) oi).get(o)));
      break;
    case FLOAT:
      result = String.valueOf((((FloatObjectInspector) oi).get(o)));
      break;
    case DOUBLE:
      result = String.valueOf((((DoubleObjectInspector) oi).get(o)));
      break;
    case STRING:
      StringObjectInspector soi = (StringObjectInspector) oi;
      result = soi.getPrimitiveJavaObject(o);
      break;
    case TIMESTAMP:
      result = ((TimestampObjectInspector) oi).getPrimitiveWritableObject(o)
          .toString();
      break;
    default:
      throw new RuntimeException("Hive 2 Internal error: unknown type: "
          + oi.getTypeName());
    }
    return result;
  }

  public static Timestamp getTimestamp(Object o, PrimitiveObjectInspector oi) {
    if (o == null) {
      return null;
    }

    Timestamp result = null;
    switch (oi.getPrimitiveCategory()) {
    case VOID:
      result = null;
      break;
    case BOOLEAN:
      result = new Timestamp(((BooleanObjectInspector) oi).get(o) ? 1 : 0);
      break;
    case BYTE:
      result = new Timestamp(((ByteObjectInspector) oi).get(o));
      break;
    case SHORT:
      result = new Timestamp(((ShortObjectInspector) oi).get(o));
      break;
    case INT:
      result = new Timestamp(((IntObjectInspector) oi).get(o));
      break;
    case LONG:
      result = new Timestamp(((LongObjectInspector) oi).get(o));
      break;
    case FLOAT:
      result = TimestampWritable.floatToTimestamp(((FloatObjectInspector) oi)
          .get(o));
      break;
    case DOUBLE:
      result = TimestampWritable.doubleToTimestamp(((DoubleObjectInspector) oi)
          .get(o));
      break;
    case STRING:
      StringObjectInspector soi = (StringObjectInspector) oi;
      String s = soi.getPrimitiveJavaObject(o).trim();

      int periodIdx = s.indexOf(".");
      if (periodIdx != -1) {
        if (s.length() - periodIdx > 9) {
          s = s.substring(0, periodIdx + 10);
        }
      }
      try {
        result = Timestamp.valueOf(s);
      } catch (IllegalArgumentException e) {
        result = null;
      }
      break;
    case TIMESTAMP:
      result = ((TimestampObjectInspector) oi).getPrimitiveWritableObject(o)
          .getTimestamp();
      break;
    default:
      throw new RuntimeException("Hive 2 Internal error: unknown type: "
          + oi.getTypeName());
    }
    return result;
  }
}
