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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableFloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableIntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableTimestampObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;

public final class ObjectInspectorConverters {

  public static interface Converter {
    Object convert(Object input);
  }

  public static class IdentityConverter implements Converter {
    public Object convert(Object input) {
      return input;
    }
  }

  public static Converter getConverter(ObjectInspector inputOI,
      ObjectInspector outputOI) {
    if (inputOI == outputOI) {
      return new IdentityConverter();
    }
    switch (outputOI.getCategory()) {
    case PRIMITIVE:
      switch (((PrimitiveObjectInspector) outputOI).getPrimitiveCategory()) {
      case BOOLEAN:
        return new PrimitiveObjectInspectorConverter.BooleanConverter(
            (PrimitiveObjectInspector) inputOI,
            (SettableBooleanObjectInspector) outputOI);
      case BYTE:
        return new PrimitiveObjectInspectorConverter.ByteConverter(
            (PrimitiveObjectInspector) inputOI,
            (SettableByteObjectInspector) outputOI);
      case SHORT:
        return new PrimitiveObjectInspectorConverter.ShortConverter(
            (PrimitiveObjectInspector) inputOI,
            (SettableShortObjectInspector) outputOI);
      case INT:
        return new PrimitiveObjectInspectorConverter.IntConverter(
            (PrimitiveObjectInspector) inputOI,
            (SettableIntObjectInspector) outputOI);
      case LONG:
        return new PrimitiveObjectInspectorConverter.LongConverter(
            (PrimitiveObjectInspector) inputOI,
            (SettableLongObjectInspector) outputOI);
      case FLOAT:
        return new PrimitiveObjectInspectorConverter.FloatConverter(
            (PrimitiveObjectInspector) inputOI,
            (SettableFloatObjectInspector) outputOI);
      case DOUBLE:
        return new PrimitiveObjectInspectorConverter.DoubleConverter(
            (PrimitiveObjectInspector) inputOI,
            (SettableDoubleObjectInspector) outputOI);
      case STRING:
        if (outputOI instanceof WritableStringObjectInspector) {
          return new PrimitiveObjectInspectorConverter.TextConverter(
              (PrimitiveObjectInspector) inputOI);
        } else if (outputOI instanceof JavaStringObjectInspector) {
          return new PrimitiveObjectInspectorConverter.StringConverter(
              (PrimitiveObjectInspector) inputOI);
        }
      case TIMESTAMP:
        return new PrimitiveObjectInspectorConverter.TimestampConverter(
            (PrimitiveObjectInspector) inputOI,
            (SettableTimestampObjectInspector) outputOI);
      default:
        throw new RuntimeException("Hive internal error: conversion of "
            + inputOI.getTypeName() + " to " + outputOI.getTypeName()
            + " not supported yet.");
      }
    case STRUCT:
      return new StructConverter((StructObjectInspector) inputOI,
          (SettableStructObjectInspector) outputOI);
    case LIST:
      return new ListConverter((ListObjectInspector) inputOI,
          (SettableListObjectInspector) outputOI);
    case MAP:
      return new MapConverter((MapObjectInspector) inputOI,
          (SettableMapObjectInspector) outputOI);
    default:
      throw new RuntimeException("Hive internal error: conversion of "
          + inputOI.getTypeName() + " to " + outputOI.getTypeName()
          + " not supported yet.");
    }
  }

  public static class ListConverter implements Converter {

    ListObjectInspector inputOI;
    SettableListObjectInspector outputOI;

    ObjectInspector inputElementOI;
    ObjectInspector outputElementOI;

    ArrayList<Converter> elementConverters;

    Object output;

    public ListConverter(ListObjectInspector inputOI,
        SettableListObjectInspector outputOI) {
      this.inputOI = inputOI;
      this.outputOI = outputOI;
      inputElementOI = inputOI.getListElementObjectInspector();
      outputElementOI = outputOI.getListElementObjectInspector();
      output = outputOI.create(0);
      elementConverters = new ArrayList<Converter>();
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }
      int size = inputOI.getListLength(input);
      while (elementConverters.size() < size) {
        elementConverters.add(getConverter(inputElementOI, outputElementOI));
      }

      outputOI.resize(output, size);
      for (int index = 0; index < size; index++) {
        Object inputElement = inputOI.getListElement(input, index);
        Object outputElement = elementConverters.get(index).convert(
            inputElement);
        outputOI.set(output, index, outputElement);
      }
      return output;
    }

  }

  public static class StructConverter implements Converter {

    StructObjectInspector inputOI;
    SettableStructObjectInspector outputOI;

    List<? extends StructField> inputFields;
    List<? extends StructField> outputFields;

    ArrayList<Converter> fieldConverters;

    Object output;

    public StructConverter(StructObjectInspector inputOI,
        SettableStructObjectInspector outputOI) {

      this.inputOI = inputOI;
      this.outputOI = outputOI;
      inputFields = inputOI.getAllStructFieldRefs();
      outputFields = outputOI.getAllStructFieldRefs();
      assert (inputFields.size() == outputFields.size());

      fieldConverters = new ArrayList<Converter>(inputFields.size());
      for (int f = 0; f < inputFields.size(); f++) {
        fieldConverters.add(getConverter(inputFields.get(f)
            .getFieldObjectInspector(), outputFields.get(f)
            .getFieldObjectInspector()));
      }
      output = outputOI.create();
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }

      for (int f = 0; f < inputFields.size(); f++) {
        Object inputFieldValue = inputOI.getStructFieldData(input,
            inputFields.get(f));
        Object outputFieldValue = fieldConverters.get(f).convert(
            inputFieldValue);
        outputOI.setStructFieldData(output, outputFields.get(f),
            outputFieldValue);
      }
      return output;
    }
  }

  public static class MapConverter implements Converter {

    MapObjectInspector inputOI;
    SettableMapObjectInspector outputOI;

    ObjectInspector inputKeyOI;
    ObjectInspector outputKeyOI;

    ObjectInspector inputValueOI;
    ObjectInspector outputValueOI;

    ArrayList<Converter> keyConverters;
    ArrayList<Converter> valueConverters;

    Object output;

    public MapConverter(MapObjectInspector inputOI,
        SettableMapObjectInspector outputOI) {
      this.inputOI = inputOI;
      this.outputOI = outputOI;
      inputKeyOI = inputOI.getMapKeyObjectInspector();
      outputKeyOI = outputOI.getMapKeyObjectInspector();
      inputValueOI = inputOI.getMapValueObjectInspector();
      outputValueOI = outputOI.getMapValueObjectInspector();
      keyConverters = new ArrayList<Converter>();
      valueConverters = new ArrayList<Converter>();
      output = outputOI.create();
    }

    @Override
    public Object convert(Object input) {
      if (input == null) {
        return null;
      }

      Map<?, ?> map = inputOI.getMap(input);
      int size = map.size();

      while (keyConverters.size() < size) {
        keyConverters.add(getConverter(inputKeyOI, outputKeyOI));
        valueConverters.add(getConverter(inputValueOI, outputValueOI));
      }

      outputOI.clear(output);

      int entryID = 0;
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        Object inputKey = entry.getKey();
        Object inputValue = entry.getValue();
        Object outputKey = keyConverters.get(entryID).convert(inputKey);
        Object outputValue = valueConverters.get(entryID).convert(inputValue);
        entryID++;
        outputOI.put(output, outputKey, outputValue);
      }
      return output;
    }

  }

  private ObjectInspectorConverters() {
  }

}
