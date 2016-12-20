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
package org.apache.hadoop.hive.serde2.typeinfo;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Type;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveTypeEntry;

public class TypeInfoUtils {
  private static Log LOG = LogFactory.getLog(TypeInfoUtils.class.getName());

  private static TypeInfo getExtendedTypeInfoFromJavaType(Type t, Method m) {

    if (t == Object.class) {
      return TypeInfoFactory.unknownTypeInfo;
    }

    if (t instanceof ParameterizedType) {
      ParameterizedType pt = (ParameterizedType) t;
      if (List.class == (Class<?>) pt.getRawType()
          || ArrayList.class == (Class<?>) pt.getRawType()) {
        return TypeInfoFactory.getListTypeInfo(getExtendedTypeInfoFromJavaType(
            pt.getActualTypeArguments()[0], m));
      }
      if (Map.class == (Class<?>) pt.getRawType()
          || HashMap.class == (Class<?>) pt.getRawType()) {
        return TypeInfoFactory.getMapTypeInfo(
            getExtendedTypeInfoFromJavaType(pt.getActualTypeArguments()[0], m),
            getExtendedTypeInfoFromJavaType(pt.getActualTypeArguments()[1], m));
      }
      t = pt.getRawType();
    }

    if (!(t instanceof Class)) {
      throw new RuntimeException("Hive does not understand type " + t
          + " from " + m);
    }
    Class<?> c = (Class<?>) t;

    if (PrimitiveObjectInspectorUtils.isPrimitiveJavaType(c)) {
      return TypeInfoUtils
          .getTypeInfoFromObjectInspector(PrimitiveObjectInspectorFactory
              .getPrimitiveJavaObjectInspector(PrimitiveObjectInspectorUtils
                  .getTypeEntryFromPrimitiveJavaType(c).primitiveCategory));
    }

    if (PrimitiveObjectInspectorUtils.isPrimitiveJavaClass(c)) {
      return TypeInfoUtils
          .getTypeInfoFromObjectInspector(PrimitiveObjectInspectorFactory
              .getPrimitiveJavaObjectInspector(PrimitiveObjectInspectorUtils
                  .getTypeEntryFromPrimitiveJavaClass(c).primitiveCategory));
    }

    if (PrimitiveObjectInspectorUtils.isPrimitiveWritableClass(c)) {
      return TypeInfoUtils
          .getTypeInfoFromObjectInspector(PrimitiveObjectInspectorFactory
              .getPrimitiveWritableObjectInspector(PrimitiveObjectInspectorUtils
                  .getTypeEntryFromPrimitiveWritableClass(c).primitiveCategory));
    }

    Field[] fields = ObjectInspectorUtils.getDeclaredNonStaticFields(c);
    ArrayList<String> fieldNames = new ArrayList<String>(fields.length);
    ArrayList<TypeInfo> fieldTypeInfos = new ArrayList<TypeInfo>(fields.length);
    for (int i = 0; i < fields.length; i++) {
      fieldNames.add(fields[i].getName());
      fieldTypeInfos.add(getExtendedTypeInfoFromJavaType(
          fields[i].getGenericType(), m));
    }
    return TypeInfoFactory.getStructTypeInfo(fieldNames, fieldTypeInfos);
  }

  public static Type getArrayElementType(Type t) {
    if (t instanceof Class && ((Class<?>) t).isArray()) {
      Class<?> arrayClass = (Class<?>) t;
      return arrayClass.getComponentType();
    } else if (t instanceof GenericArrayType) {
      GenericArrayType arrayType = (GenericArrayType) t;
      return arrayType.getGenericComponentType();
    }
    return null;
  }

  public static List<TypeInfo> getParameterTypeInfos(Method m, int size) {
    Type[] methodParameterTypes = m.getGenericParameterTypes();

    Type lastParaElementType = TypeInfoUtils
        .getArrayElementType(methodParameterTypes.length == 0 ? null
            : methodParameterTypes[methodParameterTypes.length - 1]);
    boolean isVariableLengthArgument = (lastParaElementType != null);

    List<TypeInfo> typeInfos = null;
    if (!isVariableLengthArgument) {
      if (size != methodParameterTypes.length) {
        return null;
      }
      typeInfos = new ArrayList<TypeInfo>(methodParameterTypes.length);
      for (int i = 0; i < methodParameterTypes.length; i++) {
        typeInfos.add(getExtendedTypeInfoFromJavaType(methodParameterTypes[i],
            m));
      }
    } else {
      if (size < methodParameterTypes.length - 1) {
        return null;
      }
      typeInfos = new ArrayList<TypeInfo>(size);
      for (int i = 0; i < methodParameterTypes.length - 1; i++) {
        typeInfos.add(getExtendedTypeInfoFromJavaType(methodParameterTypes[i],
            m));
      }
      for (int i = methodParameterTypes.length - 1; i < size; i++) {
        typeInfos.add(getExtendedTypeInfoFromJavaType(lastParaElementType, m));
      }
    }
    return typeInfos;
  }

  private static class TypeInfoParser {

    private static class Token {
      public int position;
      public String text;
      public boolean isType;

      public String toString() {
        return "" + position + ":" + text;
      }
    };

    private static boolean isTypeChar(char c) {
      return Character.isLetterOrDigit(c) || c == '_' || c == '.';
    }

    private static ArrayList<Token> tokenize(String typeInfoString) {
      ArrayList<Token> tokens = new ArrayList<Token>(0);
      int begin = 0;
      int end = 1;
      while (end <= typeInfoString.length()) {
        if (end == typeInfoString.length()
            || !isTypeChar(typeInfoString.charAt(end - 1))
            || !isTypeChar(typeInfoString.charAt(end))) {
          Token t = new Token();
          t.position = begin;
          t.text = typeInfoString.substring(begin, end);
          t.isType = isTypeChar(typeInfoString.charAt(begin));
          tokens.add(t);
          begin = end;
        }
        end++;
      }
      return tokens;
    }

    public TypeInfoParser(String typeInfoString) {
      this.typeInfoString = typeInfoString;
      this.typeInfoTokens = tokenize(typeInfoString);
    }

    private String typeInfoString;
    private ArrayList<Token> typeInfoTokens;
    private ArrayList<TypeInfo> typeInfos;
    private int iToken;

    public ArrayList<TypeInfo> parseTypeInfos() throws IllegalArgumentException {
      typeInfos = new ArrayList<TypeInfo>();
      iToken = 0;
      while (iToken < typeInfoTokens.size()) {
        typeInfos.add(parseType());
        if (iToken < typeInfoTokens.size()) {
          Token separator = typeInfoTokens.get(iToken);
          if (",".equals(separator.text) || ";".equals(separator.text)
              || ":".equals(separator.text)) {
            iToken++;
          } else {
            throw new IllegalArgumentException(
                "Error: ',', ':', or ';' expected at position "
                    + separator.position + " from '" + typeInfoString + "' "
                    + typeInfoTokens);
          }
        }
      }
      return typeInfos;
    }

    private Token expect(String item) {
      return expect(item, null);
    }

    private Token expect(String item, String alternative) {
      if (iToken >= typeInfoTokens.size()) {
        throw new IllegalArgumentException("Error: " + item
            + " expected at the end of '" + typeInfoString + "'");
      }
      Token t = typeInfoTokens.get(iToken);
      if (item.equals("type")) {
        if (!Constants.LIST_TYPE_NAME.equals(t.text)
            && !Constants.MAP_TYPE_NAME.equals(t.text)
            && !Constants.STRUCT_TYPE_NAME.equals(t.text)
            && !Constants.UNION_TYPE_NAME.equals(t.text)
            && !Constants.BYTEARRAY_NAME.equals(t.text)
            && null == PrimitiveObjectInspectorUtils
                .getTypeEntryFromTypeName(t.text)
            && !t.text.equals(alternative)
            ) {
          throw new IllegalArgumentException("Error: " + item
              + " expected at the position " + t.position + " of '"
              + typeInfoString + "' but '" + t.text + "' is found.");
        }
      } else if (item.equals("name")) {
        if (!t.isType && !t.text.equals(alternative)) {
          throw new IllegalArgumentException("Error: " + item
              + " expected at the position " + t.position + " of '"
              + typeInfoString + "' but '" + t.text + "' is found.");
        }
      } else {
        if (!item.equals(t.text) && !t.text.equals(alternative)) {
          throw new IllegalArgumentException("Error: " + item
              + " expected at the position " + t.position + " of '"
              + typeInfoString + "' but '" + t.text + "' is found.");
        }
      }
      iToken++;
      return t;
    }

    private TypeInfo parseType() {

      Token t = expect("type");

      PrimitiveTypeEntry primitiveType = PrimitiveObjectInspectorUtils
          .getTypeEntryFromTypeName(t.text);
      if (primitiveType != null
          && !primitiveType.primitiveCategory.equals(PrimitiveCategory.UNKNOWN)) {
        return TypeInfoFactory.getPrimitiveTypeInfo(primitiveType.typeName);
      }

      if(Constants.BYTEARRAY_NAME.equals(t.text)){
        return TypeInfoFactory.getByteArrayTypeInfo();
      }
      
      
      if (Constants.LIST_TYPE_NAME.equals(t.text)) {
        expect("<");
        TypeInfo listElementType = parseType();
        expect(">");
        return TypeInfoFactory.getListTypeInfo(listElementType);
      }

      if (Constants.MAP_TYPE_NAME.equals(t.text)) {
        expect("<");
        TypeInfo mapKeyType = parseType();
        expect(",");
        TypeInfo mapValueType = parseType();
        expect(">");
        return TypeInfoFactory.getMapTypeInfo(mapKeyType, mapValueType);
      }

      if (Constants.STRUCT_TYPE_NAME.equals(t.text)) {
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<TypeInfo> fieldTypeInfos = new ArrayList<TypeInfo>();
        boolean first = true;
        do {
          if (first) {
            expect("<");
            first = false;
          } else {
            Token separator = expect(">", ",");
            if (separator.text.equals(">")) {
              break;
            }
          }
          Token name = expect("name");
          fieldNames.add(name.text);
          expect(":");
          fieldTypeInfos.add(parseType());
        } while (true);

        return TypeInfoFactory.getStructTypeInfo(fieldNames, fieldTypeInfos);
      }

      if (Constants.UNION_TYPE_NAME.equals(t.text)) {
        List<TypeInfo> objectTypeInfos = new ArrayList<TypeInfo>();
        boolean first = true;
        do {
          if (first) {
            expect("<");
            first = false;
          } else {
            Token separator = expect(">", ",");
            if (separator.text.equals(">")) {
              break;
            }
          }
          objectTypeInfos.add(parseType());
        } while (true);

        return TypeInfoFactory.getUnionTypeInfo(objectTypeInfos);
      }

      throw new RuntimeException("Internal error parsing position "
          + t.position + " of '" + typeInfoString + "'");
    }

  }

  static HashMap<TypeInfo, ObjectInspector> cachedStandardObjectInspector = new HashMap<TypeInfo, ObjectInspector>();

  public static ObjectInspector getStandardWritableObjectInspectorFromTypeInfo(
      TypeInfo typeInfo) {
    ObjectInspector result = cachedStandardObjectInspector.get(typeInfo);
    if (result == null) {
      switch (typeInfo.getCategory()) {
      case PRIMITIVE: {
        result = PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector(((PrimitiveTypeInfo) typeInfo)
                .getPrimitiveCategory());
        break;
      }
      case LIST: {
        ObjectInspector elementObjectInspector = getStandardWritableObjectInspectorFromTypeInfo(((ListTypeInfo) typeInfo)
            .getListElementTypeInfo());
        result = ObjectInspectorFactory
            .getStandardListObjectInspector(elementObjectInspector);
        break;
      }
      
      case BYTEARRAY: {
        result = ObjectInspectorFactory.getStandardByteArrayObjectInspector();
        break;
      }
      
      
      case MAP: {
        MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
        ObjectInspector keyObjectInspector = getStandardWritableObjectInspectorFromTypeInfo(mapTypeInfo
            .getMapKeyTypeInfo());
        ObjectInspector valueObjectInspector = getStandardWritableObjectInspectorFromTypeInfo(mapTypeInfo
            .getMapValueTypeInfo());
        result = ObjectInspectorFactory.getStandardMapObjectInspector(
            keyObjectInspector, valueObjectInspector);
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
              .add(getStandardWritableObjectInspectorFromTypeInfo(fieldTypeInfos
                  .get(i)));
        }
        result = ObjectInspectorFactory.getStandardStructObjectInspector(
            fieldNames, fieldObjectInspectors);
        break;
      }

      case UNION: {
        UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
        List<TypeInfo> objectTypeInfos = unionTypeInfo
            .getAllUnionObjectTypeInfos();
        List<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>(
            objectTypeInfos.size());
        for (int i = 0; i < objectTypeInfos.size(); i++) {
          fieldObjectInspectors
              .add(getStandardWritableObjectInspectorFromTypeInfo(objectTypeInfos
                  .get(i)));
        }
        result = ObjectInspectorFactory
            .getStandardUnionObjectInspector(fieldObjectInspectors);
        break;
      }

      default: {
        result = null;
      }
      }
      cachedStandardObjectInspector.put(typeInfo, result);
    }
    return result;
  }

  static HashMap<TypeInfo, ObjectInspector> cachedStandardJavaObjectInspector = new HashMap<TypeInfo, ObjectInspector>();

  public static ObjectInspector getStandardJavaObjectInspectorFromTypeInfo(
      TypeInfo typeInfo) {
    ObjectInspector result = cachedStandardJavaObjectInspector.get(typeInfo);
    if (result == null) {
      switch (typeInfo.getCategory()) {
      case PRIMITIVE: {
        result = PrimitiveObjectInspectorFactory
            .getPrimitiveJavaObjectInspector(PrimitiveObjectInspectorUtils
                .getTypeEntryFromTypeName(typeInfo.getTypeName()).primitiveCategory);
        break;
      }
      case LIST: {
        ObjectInspector elementObjectInspector = getStandardJavaObjectInspectorFromTypeInfo(((ListTypeInfo) typeInfo)
            .getListElementTypeInfo());
        result = ObjectInspectorFactory
            .getStandardListObjectInspector(elementObjectInspector);
        break;
      }
      case MAP: {
        MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
        ObjectInspector keyObjectInspector = getStandardJavaObjectInspectorFromTypeInfo(mapTypeInfo
            .getMapKeyTypeInfo());
        ObjectInspector valueObjectInspector = getStandardJavaObjectInspectorFromTypeInfo(mapTypeInfo
            .getMapValueTypeInfo());
        result = ObjectInspectorFactory.getStandardMapObjectInspector(
            keyObjectInspector, valueObjectInspector);
        break;
      }
      case STRUCT: {
        StructTypeInfo strucTypeInfo = (StructTypeInfo) typeInfo;
        List<String> fieldNames = strucTypeInfo.getAllStructFieldNames();
        List<TypeInfo> fieldTypeInfos = strucTypeInfo
            .getAllStructFieldTypeInfos();
        List<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>(
            fieldTypeInfos.size());
        for (int i = 0; i < fieldTypeInfos.size(); i++) {
          fieldObjectInspectors
              .add(getStandardJavaObjectInspectorFromTypeInfo(fieldTypeInfos
                  .get(i)));
        }
        result = ObjectInspectorFactory.getStandardStructObjectInspector(
            fieldNames, fieldObjectInspectors);
        break;
      }

      case UNION: {
        UnionTypeInfo unionTypeInfo = (UnionTypeInfo) typeInfo;
        List<TypeInfo> objectTypeInfos = unionTypeInfo
            .getAllUnionObjectTypeInfos();
        List<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>(
            objectTypeInfos.size());
        for (int i = 0; i < objectTypeInfos.size(); i++) {
          fieldObjectInspectors
              .add(getStandardJavaObjectInspectorFromTypeInfo(objectTypeInfos
                  .get(i)));
        }
        result = ObjectInspectorFactory
            .getStandardUnionObjectInspector(fieldObjectInspectors);
        break;
      }

      default: {
        result = null;
      }
      }
      cachedStandardJavaObjectInspector.put(typeInfo, result);
    }
    return result;
  }

  public static TypeInfo getTypeInfoFromObjectInspector(ObjectInspector oi) {

    TypeInfo result = null;
    switch (oi.getCategory()) {
    case PRIMITIVE: {
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      result = TypeInfoFactory.getPrimitiveTypeInfo(poi.getTypeName());
      break;
    }
    case LIST: {
      ListObjectInspector loi = (ListObjectInspector) oi;
      result = TypeInfoFactory
          .getListTypeInfo(getTypeInfoFromObjectInspector(loi
              .getListElementObjectInspector()));
      break;
    }
    
    case BYTEARRAY:{
      result = TypeInfoFactory.getByteArrayTypeInfo();
      break;
    }
    
    
    case MAP: {
      MapObjectInspector moi = (MapObjectInspector) oi;
      result = TypeInfoFactory.getMapTypeInfo(
          getTypeInfoFromObjectInspector(moi.getMapKeyObjectInspector()),
          getTypeInfoFromObjectInspector(moi.getMapValueObjectInspector()));
      break;
    }
    case STRUCT: {
      StructObjectInspector soi = (StructObjectInspector) oi;
      List<? extends StructField> fields = soi.getAllStructFieldRefs();
      List<String> fieldNames = new ArrayList<String>(fields.size());
      List<TypeInfo> fieldTypeInfos = new ArrayList<TypeInfo>(fields.size());
      for (StructField f : fields) {
        fieldNames.add(f.getFieldName());
        fieldTypeInfos.add(getTypeInfoFromObjectInspector(f
            .getFieldObjectInspector()));
      }
      result = TypeInfoFactory.getStructTypeInfo(fieldNames, fieldTypeInfos);
      break;
    }

    case UNION: {
      UnionObjectInspector uoi = (UnionObjectInspector) oi;
      List<TypeInfo> objectTypeInfos = new ArrayList<TypeInfo>();
      for (ObjectInspector eoi : uoi.getObjectInspectors()) {
        objectTypeInfos.add(getTypeInfoFromObjectInspector(eoi));
      }
      result = TypeInfoFactory.getUnionTypeInfo(objectTypeInfos);
      break;
    }

    default: {
      throw new RuntimeException("Unknown ObjectInspector category!");
    }
    }
    return result;
  }

  public static ArrayList<TypeInfo> getTypeInfosFromTypeString(String typeString) {
    TypeInfoParser parser = new TypeInfoParser(typeString);
    return parser.parseTypeInfos();
  }

  public static TypeInfo getTypeInfoFromTypeString(String typeString) {
    TypeInfoParser parser = new TypeInfoParser(typeString);
    return parser.parseTypeInfos().get(0);
  }
}
