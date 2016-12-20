/**
* Tencent is pleased to support the open source community by making TDW available.
* Copyright (C) 2014 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use 
* this file except in compliance with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed 
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS 
* OF ANY KIND, either express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;

@description(name = "least", value = "_FUNC_(col1,col2,col3,constant,...) - Returns the min number of the arguments list", extended = "String type can only compare with string type\n"
    + "Example: \n "
    + "  > SELECT _FUNC_('a', 'b', 'c') FROM src LIMIT 1;\n"
    + "    'a' will be return. \n"
    + "  > SELECT _FUNC_(4, 5, 6) FROM src LIMIT 1;\n"
    + "     4 will be return.\n")
public class GenericUDFLeast extends GenericUDF {

  private static Log LOG = LogFactory
      .getLog(GenericUDFGreatest.class.getName());

  ObjectInspector[] argumentOIs;
  GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;

  public static enum TypeList {
    VOID, BYTE, SHORT, INT, LONG, FLOAT, DOUBLE
  };

  TypeList typeFlag = TypeList.VOID;

  boolean onlyOneType = true;

  boolean hasStringType = false;
  boolean hasBooleanType = false;
  boolean hasFloatType = false;
  boolean hasDoubleType = false;
  boolean hasLongType = false;

  DoubleWritable doubleObject;
  FloatWritable floatObject;
  LongWritable longObject;
  IntWritable intObject;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentTypeException, UDFArgumentException {

    this.argumentOIs = arguments;
    returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver();

    if (arguments.length < 1) {
      throw new UDFArgumentException("Greatest requires at least one argument.");
    }

    for (int i = 0; i < arguments.length; i++) {
      if (TypeInfoUtils.getTypeInfoFromObjectInspector(arguments[i]).equals(
          TypeInfoFactory.voidTypeInfo)) {
        LOG.info("first argument is null");
        return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
      }
    }

    for (int i = 0; i < arguments.length; i++) {
      ObjectInspector.Category category = arguments[i].getCategory();
      if (category != ObjectInspector.Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(i, "The "
            + GenericUDFUtils.getOrdinal(i + 1)
            + " argument of Greatest is expected to a "
            + ObjectInspector.Category.PRIMITIVE.toString().toLowerCase()
            + " type, but " + category.toString().toLowerCase() + " is found");
      }
    }

    for (int i = 0; i < arguments.length; i++) {
      PrimitiveObjectInspector poi = ((PrimitiveObjectInspector) arguments[i]);

      if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP) {
        throw new UDFArgumentTypeException(i,
            "Timestamp type is not support in Greatest Function");
      }
    }

    PrimitiveObjectInspector po0 = ((PrimitiveObjectInspector) arguments[0]);
    for (int i = 1; i < arguments.length; i++) {
      PrimitiveObjectInspector poi = ((PrimitiveObjectInspector) arguments[i]);
      if (po0.getPrimitiveCategory() != poi.getPrimitiveCategory()) {
        onlyOneType = false;
        break;
      }
    }

    if (onlyOneType) {
      if (po0.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.DOUBLE) {
        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
      }

      if (po0.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.FLOAT) {
        return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
      }

      if (po0.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.LONG) {
        return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
      }

      if (po0.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.INT) {
        return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
      }

      if (po0.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
      }
      returnOIResolver.update(arguments[0]);
      return returnOIResolver.get();
    }

    for (int i = 0; i < arguments.length; i++) {
      PrimitiveObjectInspector poi = ((PrimitiveObjectInspector) arguments[i]);

      if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
        throw new UDFArgumentException("String type cannot be "
            + "compared with another data type in Greatest Function");
      }

      if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN) {
        throw new UDFArgumentException("boolean type cannot be "
            + "compared with another data type in Greatest Function");
      }

      if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.DOUBLE) {
        hasDoubleType = true;
        typeFlag = TypeList.DOUBLE;
      }

      if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.FLOAT) {
        hasFloatType = true;
        if (TypeList.FLOAT.compareTo(typeFlag) > 0) {
          typeFlag = TypeList.FLOAT;
        }
      }

      if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.LONG) {
        hasLongType = true;
        if (TypeList.LONG.compareTo(typeFlag) > 0) {
          typeFlag = TypeList.LONG;
        }
      }

      if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.INT) {
        if (TypeList.INT.compareTo(typeFlag) > 0) {
          typeFlag = TypeList.INT;
        }
      }

      if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.SHORT) {
        if (TypeList.SHORT.compareTo(typeFlag) > 0) {
          typeFlag = TypeList.SHORT;
        }
      }

    }

    switch (typeFlag) {
    case DOUBLE:
      return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    case FLOAT:
      return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
    case LONG:
    case INT:
    case SHORT:
    case BYTE:
      return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    default:
      returnOIResolver.update(arguments[0]);
      return returnOIResolver.get();
    }
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    for (int i = 0; i < arguments.length; i++) {
      if (argumentOIs[i] == null || arguments[i] == null
          || arguments[i].get() == null) {
        return null;
      }

      if (TypeInfoUtils.getTypeInfoFromObjectInspector(argumentOIs[i]).equals(
          TypeInfoFactory.voidTypeInfo)) {
        return null;
      }
    }

    int minIndex = 0;

    ObjectInspector oiMin = argumentOIs[0];
    Object oMin = arguments[0].get();

    ObjectInspector oi1 = oiMin;
    Object o1 = oMin;

    if (onlyOneType) {
      PrimitiveObjectInspector poi = ((PrimitiveObjectInspector) oiMin);
      switch (poi.getPrimitiveCategory()) {
      case BOOLEAN:
        for (int i = 0; i < arguments.length; i++) {
          oi1 = argumentOIs[i];
          o1 = arguments[i].get();

          BooleanObjectInspector boi = (BooleanObjectInspector) oi1;

          boolean b = boi.get(o1);
          if (b == false) {
            minIndex = i;
            return arguments[minIndex].get();
          }
        }
        return arguments[minIndex].get();
      case STRING:
        StringObjectInspector soi0 = (StringObjectInspector) oiMin;
        String minString = soi0.getPrimitiveJavaObject(oMin);
        for (int i = 1; i < arguments.length; i++) {
          oi1 = argumentOIs[i];
          o1 = arguments[i].get();

          StringObjectInspector soi1 = (StringObjectInspector) oi1;

          String s1 = soi1.getPrimitiveJavaObject(o1);
          if (minString.compareTo(s1) > 0) {
            minIndex = i;
            oiMin = argumentOIs[minIndex];
            oMin = arguments[minIndex].get();
            minString = s1;
          }
        }
        return new Text(minString);
      case DOUBLE:
        DoubleObjectInspector doi0 = (DoubleObjectInspector) oiMin;
        double minDoubleData = doi0.get(oMin);
        for (int i = 1; i < arguments.length; i++) {
          oi1 = argumentOIs[i];
          o1 = arguments[i].get();

          DoubleObjectInspector doi1 = (DoubleObjectInspector) oi1;

          if (doi0.get(oMin) > doi1.get(o1)) {
            minIndex = i;
            oiMin = argumentOIs[minIndex];
            oMin = arguments[minIndex].get();
            minDoubleData = doi1.get(o1);
          }
        }
        doubleObject = new DoubleWritable((double) minDoubleData);
        return doubleObject;
      case FLOAT:
        FloatObjectInspector foi0 = (FloatObjectInspector) oiMin;
        float minFloatData = foi0.get(oMin);
        for (int i = 1; i < arguments.length; i++) {
          oi1 = argumentOIs[i];
          o1 = arguments[i].get();

          FloatObjectInspector foi1 = (FloatObjectInspector) oi1;
          if (foi0.get(oMin) > foi1.get(o1)) {
            minIndex = i;
            oiMin = argumentOIs[minIndex];
            oMin = arguments[minIndex].get();
            minFloatData = foi1.get(o1);
          }
        }
        floatObject = new FloatWritable((float) minFloatData);
        return floatObject;
      case LONG:
        LongObjectInspector loi0 = (LongObjectInspector) oiMin;
        long minLongData = loi0.get(oMin);
        for (int i = 1; i < arguments.length; i++) {
          oi1 = argumentOIs[i];
          o1 = arguments[i].get();

          LongObjectInspector loi1 = (LongObjectInspector) oi1;
          if (loi0.get(oMin) > loi1.get(o1)) {
            minIndex = i;
            oiMin = argumentOIs[minIndex];
            oMin = arguments[minIndex].get();
            minLongData = loi1.get(o1);
          }
        }
        longObject = new LongWritable((long) minLongData);
        return longObject;
      case INT:
        IntObjectInspector ioi0 = (IntObjectInspector) oiMin;
        int minIntData = ioi0.get(oMin);
        for (int i = 1; i < arguments.length; i++) {
          oi1 = argumentOIs[i];
          o1 = arguments[i].get();

          IntObjectInspector ioi1 = (IntObjectInspector) oi1;

          if (ioi0.get(oMin) > ioi1.get(o1)) {
            minIndex = i;
            oiMin = argumentOIs[minIndex];
            oMin = arguments[minIndex].get();
            minIntData = ioi1.get(o1);
          }
        }
        intObject = new IntWritable((int) minIntData);
        return intObject;
      case SHORT:
        for (int i = 1; i < arguments.length; i++) {
          oi1 = argumentOIs[i];
          o1 = arguments[i].get();

          ShortObjectInspector shoi0 = (ShortObjectInspector) oiMin;
          ShortObjectInspector shoi1 = (ShortObjectInspector) oi1;
          if (shoi0.get(oMin) > shoi1.get(o1)) {
            minIndex = i;
            oiMin = argumentOIs[minIndex];
            oMin = arguments[minIndex].get();
          }
        }
        return arguments[minIndex].get();
      case BYTE:
        for (int i = 1; i < arguments.length; i++) {
          oi1 = argumentOIs[i];
          o1 = arguments[i].get();

          ByteObjectInspector byoi0 = (ByteObjectInspector) oiMin;
          ByteObjectInspector byoi1 = (ByteObjectInspector) oi1;
          if (byoi0.get(oMin) > byoi1.get(o1)) {
            minIndex = i;
            oiMin = argumentOIs[minIndex];
            oMin = arguments[minIndex].get();
          }
        }
        return arguments[minIndex].get();
      default:
        return null;
      }
    }

    if (hasDoubleType) {
      double minDoubleData = convertToDoubleData(oiMin, oMin);
      for (int i = 1; i < arguments.length; i++) {
        oi1 = argumentOIs[i];
        o1 = arguments[i].get();

        double tmp = convertToDoubleData(oi1, o1);
        if (minDoubleData > tmp) {
          minDoubleData = tmp;
          minIndex = i;
          oiMin = argumentOIs[minIndex];
          oMin = arguments[minIndex].get();
        }
      }
      doubleObject = new DoubleWritable((double) minDoubleData);
    }

    if (hasFloatType) {
      float minFloatData = convertToFloatData(oiMin, oMin);
      for (int i = 1; i < arguments.length; i++) {
        oi1 = argumentOIs[i];
        o1 = arguments[i].get();

        float tmp = convertToFloatData(oi1, o1);
        if (minFloatData > tmp) {
          minFloatData = tmp;
          minIndex = i;
          oiMin = argumentOIs[minIndex];
          oMin = arguments[minIndex].get();
        }
      }
      floatObject = new FloatWritable((float) minFloatData);
    }

    if ((!hasDoubleType) && (!hasFloatType)) {
      long minLongData = convertToLongData(oiMin, oMin);
      for (int i = 1; i < arguments.length; i++) {
        oi1 = argumentOIs[i];
        o1 = arguments[i].get();
        long tmp = convertToLongData(oi1, o1);

        if (minLongData > tmp) {
          minLongData = tmp;
          minIndex = i;
          oiMin = argumentOIs[minIndex];
          oMin = arguments[minIndex].get();
        }
      }

      longObject = new LongWritable((long) minLongData);
    }

    switch (typeFlag) {
    case DOUBLE:
      return doubleObject;
    case FLOAT:
      return floatObject;
    case LONG:
    case INT:
    case SHORT:
    case BYTE:
      return longObject;
    default:
      return null;
    }
  }

  private long convertToLongData(ObjectInspector oi, Object o)
      throws UDFArgumentException {
    long ret = 0;

    if (oi == null || o == null) {
      throw new UDFArgumentException("NULL ObjectInspector or Object");
    }

    PrimitiveObjectInspector poi = ((PrimitiveObjectInspector) oi);
    switch (poi.getPrimitiveCategory()) {
    case BYTE:
      ByteObjectInspector byoi = (ByteObjectInspector) oi;
      byte by = byoi.get(o);
      ret = (long) by;
      break;
    case SHORT:
      ShortObjectInspector soi = (ShortObjectInspector) oi;
      short s = soi.get(o);
      ret = (long) s;
      break;
    case INT:
      IntObjectInspector ioi = (IntObjectInspector) oi;
      int i = ioi.get(o);
      ret = (long) i;
      break;
    case LONG:
      LongObjectInspector loi = (LongObjectInspector) oi;
      ret = loi.get(o);
      break;
    default:
      return ret;
    }

    return ret;
  }

  private double convertToDoubleData(ObjectInspector oi, Object o)
      throws UDFArgumentException {
    double ret = 0.0;

    if (oi == null || o == null) {
      throw new UDFArgumentException("NULL ObjectInspector or Object");
    }

    PrimitiveObjectInspector poi = ((PrimitiveObjectInspector) oi);
    switch (poi.getPrimitiveCategory()) {
    case BYTE:
      ByteObjectInspector byoi = (ByteObjectInspector) oi;
      byte by = byoi.get(o);
      ret = (double) by;
      break;
    case SHORT:
      ShortObjectInspector soi = (ShortObjectInspector) oi;
      short s = soi.get(o);
      ret = (double) s;
      break;
    case INT:
      IntObjectInspector ioi = (IntObjectInspector) oi;
      int i = ioi.get(o);
      ret = (double) i;
      break;
    case LONG:
      LongObjectInspector loi = (LongObjectInspector) oi;
      long l = loi.get(o);
      ret = (double) l;
      break;
    case FLOAT:
      FloatObjectInspector foi = (FloatObjectInspector) oi;
      float f = foi.get(o);
      ret = (double) f;
      break;
    case DOUBLE:
      DoubleObjectInspector doi = (DoubleObjectInspector) oi;
      ret = doi.get(o);
      break;
    default:
      return ret;
    }

    return ret;
  }

  private float convertToFloatData(ObjectInspector oi, Object o)
      throws UDFArgumentException {
    float ret = 0;

    if (oi == null || o == null) {
      throw new UDFArgumentException("NULL ObjectInspector or Object");
    }

    PrimitiveObjectInspector poi = ((PrimitiveObjectInspector) oi);
    switch (poi.getPrimitiveCategory()) {
    case BYTE:
      ByteObjectInspector byoi = (ByteObjectInspector) oi;
      byte by = byoi.get(o);
      ret = (float) by;
      break;
    case SHORT:
      ShortObjectInspector soi = (ShortObjectInspector) oi;
      short s = soi.get(o);
      ret = (float) s;
      break;
    case INT:
      IntObjectInspector ioi = (IntObjectInspector) oi;
      int i = ioi.get(o);
      ret = (float) i;
      break;
    case LONG:
      LongObjectInspector loi = (LongObjectInspector) oi;
      long l = loi.get(o);
      ret = (float) l;
      break;
    case FLOAT:
      FloatObjectInspector foi = (FloatObjectInspector) oi;
      ret = foi.get(o);
      break;
    default:
      return ret;
    }

    return ret;
  }

  @Override
  public String getDisplayString(String[] children) {
    return null;
  }

}
