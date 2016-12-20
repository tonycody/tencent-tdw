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

package org.apache.hadoop.hive.ql.udf.generic;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.IdentityConverter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;

public class GenericUDFUtils {

  private static Log LOG = LogFactory.getLog(GenericUDFUtils.class.getName());

  public static boolean isUtfStartByte(byte b) {
    return (b & 0xC0) != 0x80;
  }

  public static class ReturnObjectInspectorResolver {

    boolean allowTypeConversion;
    ObjectInspector returnObjectInspector;

    HashMap<ObjectInspector, Converter> converters;

    public ReturnObjectInspectorResolver() {
      this(false);
    }

    public ReturnObjectInspectorResolver(boolean allowTypeConversion) {
      this.allowTypeConversion = allowTypeConversion;
    }

    public boolean update(ObjectInspector oi) throws UDFArgumentTypeException {
      if (oi instanceof VoidObjectInspector) {
        return true;
      }

      if (returnObjectInspector == null) {
        returnObjectInspector = oi;
        return true;
      }

      if (returnObjectInspector == oi) {
        return true;
      }

      TypeInfo oiTypeInfo = TypeInfoUtils.getTypeInfoFromObjectInspector(oi);
      TypeInfo rTypeInfo = TypeInfoUtils
          .getTypeInfoFromObjectInspector(returnObjectInspector);
      if (oiTypeInfo == rTypeInfo) {
        returnObjectInspector = ObjectInspectorUtils
            .getStandardObjectInspector(returnObjectInspector,
                ObjectInspectorCopyOption.WRITABLE);
        return true;
      }

      if (!allowTypeConversion) {
        return false;
      }

      TypeInfo commonTypeInfo = FunctionRegistry.getCommonClass(oiTypeInfo,
          rTypeInfo);
      if (commonTypeInfo == null) {
        return false;
      }

      returnObjectInspector = TypeInfoUtils
          .getStandardWritableObjectInspectorFromTypeInfo(commonTypeInfo);

      return true;
    }

    public ObjectInspector get() {
      return returnObjectInspector;
    }

    public Object convertIfNecessary(Object o, ObjectInspector oi) {
      Object converted = null;
      if (oi == returnObjectInspector) {
        converted = o;
      } else {

        if (o == null) {
          return null;
        }

        if (converters == null) {
          converters = new HashMap<ObjectInspector, Converter>();
        }

        Converter converter = converters.get(oi);
        if (converter == null) {
          converter = ObjectInspectorConverters.getConverter(oi,
              returnObjectInspector);
          converters.put(oi, converter);
        }
        converted = converter.convert(o);
      }
      return converted;
    }

  }

  public static class ConversionHelper {

    private Method m;
    private ObjectInspector[] givenParameterOIs;
    Type[] methodParameterTypes;
    private boolean isVariableLengthArgument;
    Type lastParaElementType;

    boolean conversionNeeded;
    Converter[] converters;
    Object[] convertedParameters;
    Object[] convertedParametersInArray;

    private static Class<?> getClassFromType(Type t) {
      if (t instanceof Class<?>) {
        return (Class<?>) t;
      } else if (t instanceof ParameterizedType) {
        ParameterizedType pt = (ParameterizedType) t;
        return (Class<?>) pt.getRawType();
      }
      return null;
    }

    public ConversionHelper(Method m, ObjectInspector[] parameterOIs) {
      this.m = m;
      this.givenParameterOIs = parameterOIs;

      methodParameterTypes = m.getGenericParameterTypes();

      lastParaElementType = TypeInfoUtils
          .getArrayElementType(methodParameterTypes.length == 0 ? null
              : methodParameterTypes[methodParameterTypes.length - 1]);
      isVariableLengthArgument = (lastParaElementType != null);

      ObjectInspector[] methodParameterOIs = new ObjectInspector[parameterOIs.length];

      if (isVariableLengthArgument) {

        for (int i = 0; i < methodParameterTypes.length - 1; i++) {
          if (methodParameterTypes[i] == Object.class) {
            methodParameterOIs[i] = ObjectInspectorUtils
                .getStandardObjectInspector(parameterOIs[i],
                    ObjectInspectorCopyOption.JAVA);
          } else {
            methodParameterOIs[i] = ObjectInspectorFactory
                .getReflectionObjectInspector(methodParameterTypes[i],
                    ObjectInspectorOptions.JAVA);
          }
        }

        if (lastParaElementType == Object.class) {
          for (int i = methodParameterTypes.length - 1; i < parameterOIs.length; i++) {
            methodParameterOIs[i] = ObjectInspectorUtils
                .getStandardObjectInspector(parameterOIs[i],
                    ObjectInspectorCopyOption.JAVA);
          }
        } else {
          ObjectInspector oi = ObjectInspectorFactory
              .getReflectionObjectInspector(lastParaElementType,
                  ObjectInspectorOptions.JAVA);
          for (int i = methodParameterTypes.length - 1; i < parameterOIs.length; i++) {
            methodParameterOIs[i] = oi;
          }
        }

      } else {

        assert methodParameterTypes.length == parameterOIs.length;
        for (int i = 0; i < methodParameterTypes.length; i++) {
          if (methodParameterTypes[i] == Object.class) {
            methodParameterOIs[i] = ObjectInspectorUtils
                .getStandardObjectInspector(parameterOIs[i],
                    ObjectInspectorCopyOption.JAVA);
          } else {
            methodParameterOIs[i] = ObjectInspectorFactory
                .getReflectionObjectInspector(methodParameterTypes[i],
                    ObjectInspectorOptions.JAVA);
          }
        }
      }

      conversionNeeded = false;
      converters = new Converter[parameterOIs.length];
      for (int i = 0; i < parameterOIs.length; i++) {
        Converter pc = ObjectInspectorConverters.getConverter(parameterOIs[i],
            methodParameterOIs[i]);
        converters[i] = pc;
        conversionNeeded = conversionNeeded
            || (!(pc instanceof IdentityConverter));
      }

      if (isVariableLengthArgument) {
        convertedParameters = new Object[methodParameterTypes.length];
        convertedParametersInArray = (Object[]) Array.newInstance(
            getClassFromType(lastParaElementType), parameterOIs.length
                - methodParameterTypes.length + 1);
        convertedParameters[convertedParameters.length - 1] = convertedParametersInArray;

        if (m.getName().equalsIgnoreCase("concat")) {
          conversionNeeded = true;
        }

      } else {
        convertedParameters = new Object[parameterOIs.length];
      }
    }

    public Object[] convertIfNecessary(Object... parameters) {

      assert (parameters.length == givenParameterOIs.length);

      if (!conversionNeeded) {
        if (!isVariableLengthArgument) {
          return parameters;
        } else if (methodParameterTypes.length == 1) {
          convertedParameters[0] = parameters;
          return convertedParameters;
        }
      }

      if (isVariableLengthArgument) {
        for (int i = 0; i < methodParameterTypes.length - 1; i++) {
          convertedParameters[i] = converters[i].convert(parameters[i]);
        }
        for (int i = methodParameterTypes.length - 1; i < parameters.length; i++) {
          convertedParametersInArray[i + 1 - methodParameterTypes.length] = converters[i]
              .convert(parameters[i]);
        }
      } else {

        for (int i = 0; i < methodParameterTypes.length; i++) {
          convertedParameters[i] = converters[i].convert(parameters[i]);
        }
      }
      return convertedParameters;
    }
  };

  public static String getOrdinal(int i) {
    int unit = i % 10;
    return (i <= 0) ? "" : (i != 11 && unit == 1) ? i + "st"
        : (i != 12 && unit == 2) ? i + "nd" : (i != 13 && unit == 3) ? i + "rd"
            : i + "th";
  }

  public static int findText(Text text, Text subtext, int start) {
    if (start < 0)
      return -1;

    ByteBuffer src = ByteBuffer.wrap(text.getBytes(), 0, text.getLength());
    ByteBuffer tgt = ByteBuffer
        .wrap(subtext.getBytes(), 0, subtext.getLength());
    byte b = tgt.get();
    src.position(start);

    while (src.hasRemaining()) {
      if (b == src.get()) {
        src.mark();
        tgt.mark();
        boolean found = true;
        int pos = src.position() - 1;
        while (tgt.hasRemaining()) {
          if (!src.hasRemaining()) {
            tgt.reset();
            src.reset();
            found = false;
            break;
          }
          if (!(tgt.get() == src.get())) {
            tgt.reset();
            src.reset();
            found = false;
            break;
          }
        }
        if (found)
          return pos;
      }
    }
    return -1;
  }

}
