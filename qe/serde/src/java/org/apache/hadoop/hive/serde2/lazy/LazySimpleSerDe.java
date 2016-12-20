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

package org.apache.hadoop.hive.serde2.lazy;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ByteArrayObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class LazySimpleSerDe implements SerDe {

  public static final Log LOG = LogFactory.getLog(LazySimpleSerDe.class
      .getName());

  final public static byte[] DefaultSeparators = { (byte) 1, (byte) 2, (byte) 3 };

  private ObjectInspector cachedObjectInspector;
  private boolean useJSONSerialize;

  private String charset;

  Configuration job;
  String TOLERATE_DATAERROR_READEXT = "delete";

  public String toString() {
    return getClass().toString()
        + "["
        + Arrays.asList(serdeParams.separators)
        + ":"
        + ((StructTypeInfo) serdeParams.rowTypeInfo).getAllStructFieldNames()
        + ":"
        + ((StructTypeInfo) serdeParams.rowTypeInfo)
            .getAllStructFieldTypeInfos() + "]";
  }

  public LazySimpleSerDe() throws SerDeException {
  }

  public static byte getByte(String altValue, byte defaultVal) {
    if (altValue != null && altValue.length() > 0) {
      try {
        return Byte.valueOf(altValue).byteValue();
      } catch (NumberFormatException e) {
        return (byte) altValue.charAt(0);
      }
    }
    return defaultVal;
  }

  public static class SerDeParameters {
    byte[] separators = DefaultSeparators;
    String nullString;
    Text nullSequence;
    TypeInfo rowTypeInfo;
    boolean lastColumnTakesRest;
    List<String> columnNames;
    List<TypeInfo> columnTypes;

    boolean escaped;
    byte escapeChar;
    boolean[] needsEscape;
    boolean jsonSerialize;
    boolean gbkcoding = false;

    public List<TypeInfo> getColumnTypes() {
      return columnTypes;
    }

    public List<String> getColumnNames() {
      return columnNames;
    }

    public byte[] getSeparators() {
      return separators;
    }

    public String getNullString() {
      return nullString;
    }

    public Text getNullSequence() {
      return nullSequence;
    }

    public TypeInfo getRowTypeInfo() {
      return rowTypeInfo;
    }

    public boolean isLastColumnTakesRest() {
      return lastColumnTakesRest;
    }

    public boolean isEscaped() {
      return escaped;
    }

    public byte getEscapeChar() {
      return escapeChar;
    }

    public boolean[] getNeedsEscape() {
      return needsEscape;
    }

    public boolean isJsonSerialize() {
      return jsonSerialize;
    }

    public void setJsonSerialize(boolean jsonSerialize) {
      this.jsonSerialize = jsonSerialize;
    }

    public boolean isGbkcoding() {
      return gbkcoding;
    }

  }

  SerDeParameters serdeParams = null;

  boolean exttable = false;

  public void initialize(Configuration job, Properties tbl)
      throws SerDeException {
    charset = tbl.getProperty("charset", "");
    if (charset == null || charset.isEmpty()) {
      charset = "utf-8";
    }

    if (job != null) {
      this.job = job;
      if (tbl.getProperty("EXTERNAL") != null
          && tbl.getProperty("EXTERNAL").equalsIgnoreCase("true")) {
        exttable = true;
        TOLERATE_DATAERROR_READEXT = HiveConf.getVar(job,
            HiveConf.ConfVars.TOLERATE_DATAERROR_READEXT);
      } else {
        TOLERATE_DATAERROR_READEXT = "tolerate";
      }

    }

    serdeParams = LazySimpleSerDe.initSerdeParams(job, tbl, getClass()
        .getName());

    cachedObjectInspector = LazyFactory.createLazyStructInspector(
        serdeParams.getColumnNames(), serdeParams.getColumnTypes(),
        serdeParams.getSeparators(), serdeParams.getNullSequence(),
        serdeParams.isLastColumnTakesRest(), serdeParams.isEscaped(),
        serdeParams.getEscapeChar(), serdeParams.isGbkcoding());

    if (serdeParams.isJsonSerialize())
      setUseJSONSerialize(true);

    cachedLazyStruct = (LazyStruct) LazyFactory
        .createLazyObject(cachedObjectInspector);

    LOG.debug("LazySimpleSerDe initialized with: columnNames="
        + serdeParams.columnNames + " columnTypes=" + serdeParams.columnTypes
        + " separator=" + Arrays.asList(serdeParams.separators)
        + " nullstring=" + serdeParams.nullString + " lastColumnTakesRest="
        + serdeParams.lastColumnTakesRest);
  }

  public static SerDeParameters initSerdeParams(Configuration job,
      Properties tbl, String serdeName) throws SerDeException {
    SerDeParameters serdeParams = new SerDeParameters();
    serdeParams.separators = new byte[8];
    serdeParams.separators[0] = getByte(
        tbl.getProperty(Constants.FIELD_DELIM,
            tbl.getProperty(Constants.SERIALIZATION_FORMAT)),
        DefaultSeparators[0]);
    serdeParams.separators[1] = getByte(
        tbl.getProperty(Constants.COLLECTION_DELIM), DefaultSeparators[1]);
    serdeParams.separators[2] = getByte(
        tbl.getProperty(Constants.MAPKEY_DELIM), DefaultSeparators[2]);
    for (int i = 3; i < serdeParams.separators.length; i++) {
      serdeParams.separators[i] = (byte) (i + 1);
    }

    serdeParams.nullString = tbl.getProperty(
        Constants.SERIALIZATION_NULL_FORMAT, "\\N");
    serdeParams.nullSequence = new Text(serdeParams.nullString);

    String lastColumnTakesRestString = tbl
        .getProperty(Constants.SERIALIZATION_LAST_COLUMN_TAKES_REST);
    serdeParams.lastColumnTakesRest = (lastColumnTakesRestString != null && lastColumnTakesRestString
        .equalsIgnoreCase("true"));

    String useJsonSerialize = tbl
        .getProperty(Constants.SERIALIZATION_USE_JSON_OBJECTS);
    serdeParams.jsonSerialize = (useJsonSerialize != null && useJsonSerialize
        .equalsIgnoreCase("true"));

    String columnNameProperty = tbl.getProperty(Constants.LIST_COLUMNS);
    String columnTypeProperty = tbl.getProperty(Constants.LIST_COLUMN_TYPES);

    if (columnNameProperty != null && columnNameProperty.length() > 0) {
      serdeParams.columnNames = Arrays.asList(columnNameProperty.split(","));
    } else {
      serdeParams.columnNames = new ArrayList<String>();
    }
    if (columnTypeProperty == null) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < serdeParams.columnNames.size(); i++) {
        if (i > 0)
          sb.append(":");
        sb.append(Constants.STRING_TYPE_NAME);
      }
      columnTypeProperty = sb.toString();
    }

    serdeParams.columnTypes = TypeInfoUtils
        .getTypeInfosFromTypeString(columnTypeProperty);

    if (serdeParams.columnNames.size() != serdeParams.columnTypes.size()) {
      throw new SerDeException(serdeName + ": columns has "
          + serdeParams.columnNames.size()
          + " elements while columns.types has "
          + serdeParams.columnTypes.size() + " elements!");
    }

    serdeParams.rowTypeInfo = TypeInfoFactory.getStructTypeInfo(
        serdeParams.columnNames, serdeParams.columnTypes);

    String escapeProperty = tbl.getProperty(Constants.ESCAPE_CHAR);
    serdeParams.escaped = (escapeProperty != null);
    if (serdeParams.escaped) {
      serdeParams.escapeChar = getByte(escapeProperty, (byte) '\\');
    }
    if (serdeParams.escaped) {
      serdeParams.needsEscape = new boolean[128];
      for (int i = 0; i < 128; i++) {
        serdeParams.needsEscape[i] = false;
      }
      serdeParams.needsEscape[serdeParams.escapeChar] = true;
      for (int i = 0; i < serdeParams.separators.length; i++) {
        serdeParams.needsEscape[serdeParams.separators[i]] = true;
      }
    }
    serdeParams.gbkcoding = "gbk".equalsIgnoreCase(tbl.getProperty("charset"));

    return serdeParams;
  }

  LazyStruct cachedLazyStruct;

  ByteArrayRef byteArrayRef;

  public Object deserialize(Writable field) throws SerDeException {
    if (byteArrayRef == null) {
      byteArrayRef = new ByteArrayRef();
    }
    if (field instanceof BytesWritable) {
      BytesWritable b = (BytesWritable) field;
      byteArrayRef.setData(b.get());
      cachedLazyStruct.init(byteArrayRef, 0, b.getSize());
    } else if (field instanceof Text) {
      Text t = (Text) field;

      byteArrayRef.setData(t.getBytes());
      if (exttable
          && (this.TOLERATE_DATAERROR_READEXT.equals("delete") || this.TOLERATE_DATAERROR_READEXT
              .equals("never"))) {
        cachedLazyStruct.init1(byteArrayRef, 0, t.getLength());
        if (cachedLazyStruct.haserror1
            && this.TOLERATE_DATAERROR_READEXT.equals("never")) {
          throw new SerDeException("some dataerror while read data!!!");
        } else if ((cachedLazyStruct.haserror1 && this.TOLERATE_DATAERROR_READEXT
            .equals("delete"))) {
          return null;
        }

      } else {
        cachedLazyStruct.init(byteArrayRef, 0, t.getLength());
      }
    } else {
      throw new SerDeException(getClass().toString()
          + ": expects either BytesWritable or Text object!");
    }
    return cachedLazyStruct;
  }

  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }

  Text serializeCache = new Text();
  ByteStream.Output serializeStream = new ByteStream.Output();

  public Writable serialize(Object obj, ObjectInspector objInspector)
      throws SerDeException {

    if (objInspector.getCategory() != Category.STRUCT) {
      throw new SerDeException(getClass().toString()
          + " can only serialize struct types, but we got: "
          + objInspector.getTypeName());
    }

    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    List<Object> list = soi.getStructFieldsDataAsList(obj);
    List<? extends StructField> declaredFields = (serdeParams.rowTypeInfo != null && ((StructTypeInfo) serdeParams.rowTypeInfo)
        .getAllStructFieldNames().size() > 0) ? ((StructObjectInspector) getObjectInspector())
        .getAllStructFieldRefs() : null;

    serializeStream.reset();

    try {
      for (int i = 0; i < fields.size(); i++) {
        if (i > 0) {
          serializeStream.write(serdeParams.separators[0]);
        }
        ObjectInspector foi = fields.get(i).getFieldObjectInspector();
        Object f = (list == null ? null : list.get(i));

        if (declaredFields != null && i >= declaredFields.size()) {
          throw new SerDeException("Error: expecting " + declaredFields.size()
              + " but asking for field " + i + "\n" + "data=" + obj + "\n"
              + "tableType=" + serdeParams.rowTypeInfo.toString() + "\n"
              + "dataType="
              + TypeInfoUtils.getTypeInfoFromObjectInspector(objInspector));
        }

        if (!foi.getCategory().equals(Category.PRIMITIVE)
            && (declaredFields == null
                || declaredFields.get(i).getFieldObjectInspector()
                    .getCategory().equals(Category.PRIMITIVE) || useJSONSerialize)) {
          serialize(serializeStream, SerDeUtils.getJSONString(f, foi),
              PrimitiveObjectInspectorFactory.javaStringObjectInspector,
              serdeParams.separators, 1, serdeParams.nullSequence,
              serdeParams.escaped, serdeParams.escapeChar,
              serdeParams.needsEscape);
        } else {
          serialize(serializeStream, f, foi, serdeParams.separators, 1,
              serdeParams.nullSequence, serdeParams.escaped,
              serdeParams.escapeChar, serdeParams.needsEscape);
        }
      }
    } catch (IOException e) {
      throw new SerDeException(e);
    }
    serializeCache
        .set(serializeStream.getData(), 0, serializeStream.getCount());
    return serializeCache;
  }

  public static void serialize(ByteStream.Output out, Object obj,
      ObjectInspector objInspector, byte[] separators, int level,
      Text nullSequence, boolean escaped, byte escapeChar, boolean[] needsEscape)
      throws IOException {

    if (obj == null) {
      out.write(nullSequence.getBytes(), 0, nullSequence.getLength());
      return;
    }

    switch (objInspector.getCategory()) {
    case PRIMITIVE: {
      LazyUtils.writePrimitiveUTF8(out, obj,
          (PrimitiveObjectInspector) objInspector, escaped, escapeChar,
          needsEscape);
      return;
    }
    case LIST: {
      char separator = (char) separators[level];
      ListObjectInspector loi = (ListObjectInspector) objInspector;
      List<?> list = loi.getList(obj);
      ObjectInspector eoi = loi.getListElementObjectInspector();
      if (list == null) {
        out.write(nullSequence.getBytes(), 0, nullSequence.getLength());
      } else {
        for (int i = 0; i < list.size(); i++) {
          if (i > 0) {
            out.write(separator);
          }
          serialize(out, list.get(i), eoi, separators, level + 1, nullSequence,
              escaped, escapeChar, needsEscape);
        }
      }
      return;
    }
    
    
    case BYTEARRAY:{
      //char separator = (char)separators[level];
      ByteArrayObjectInspector baoi = (ByteArrayObjectInspector)objInspector; 
      byte [] data = baoi.getBytesForStrBuff(obj);
      if(data == null){
        out.write(nullSequence.getBytes(), 0, nullSequence.getLength());
      }
      else{
        out.write(data);
      }
      
      return;
    }
    
    
    case MAP: {
      char separator = (char) separators[level];
      char keyValueSeparator = (char) separators[level + 1];
      MapObjectInspector moi = (MapObjectInspector) objInspector;
      ObjectInspector koi = moi.getMapKeyObjectInspector();
      ObjectInspector voi = moi.getMapValueObjectInspector();

      Map<?, ?> map = moi.getMap(obj);
      if (map == null) {
        out.write(nullSequence.getBytes(), 0, nullSequence.getLength());
      } else {
        boolean first = true;
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          if (first) {
            first = false;
          } else {
            out.write(separator);
          }
          serialize(out, entry.getKey(), koi, separators, level + 2,
              nullSequence, escaped, escapeChar, needsEscape);
          out.write(keyValueSeparator);
          serialize(out, entry.getValue(), voi, separators, level + 2,
              nullSequence, escaped, escapeChar, needsEscape);
        }
      }
      return;
    }
    case STRUCT: {
      char separator = (char) separators[level];
      StructObjectInspector soi = (StructObjectInspector) objInspector;
      List<? extends StructField> fields = soi.getAllStructFieldRefs();
      List<Object> list = soi.getStructFieldsDataAsList(obj);
      if (list == null) {
        out.write(nullSequence.getBytes(), 0, nullSequence.getLength());
      } else {
        for (int i = 0; i < list.size(); i++) {
          if (i > 0) {
            out.write(separator);
          }
          serialize(out, list.get(i), fields.get(i).getFieldObjectInspector(),
              separators, level + 1, nullSequence, escaped, escapeChar,
              needsEscape);
        }
      }
      return;
    }

    case UNION:
      char separator = (char) separators[level];
      UnionObjectInspector uoi = (UnionObjectInspector) objInspector;
      List<? extends ObjectInspector> ois = uoi.getObjectInspectors();
      if (ois == null) {
        out.write(nullSequence.getBytes(), 0, nullSequence.getLength());
      } else {
        LazyUtils.writePrimitiveUTF8(out, new Byte(uoi.getTag(obj)),
            PrimitiveObjectInspectorFactory.javaByteObjectInspector, escaped,
            escapeChar, needsEscape);
        out.write(separator);
        serialize(out, uoi.getField(obj), ois.get(uoi.getTag(obj)), separators,
            level + 1, nullSequence, escaped, escapeChar, needsEscape);
      }
      return;

    }

    throw new RuntimeException("Unknown category type: "
        + objInspector.getCategory());
  }

  public boolean isUseJSONSerialize() {
    return useJSONSerialize;
  }

  public void setUseJSONSerialize(boolean useJSONSerialize) {
    this.useJSONSerialize = useJSONSerialize;
  }
}
