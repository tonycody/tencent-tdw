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
package StorageEngineClient;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import org.apache.hadoop.io.Writable;

import Comm.ConstVar;
import Comm.SEException;
import FormatStorage1.IFieldMap;
import FormatStorage1.IHead;
import FormatStorage1.IRecord;
import FormatStorage1.IRecord.IFType;

public class FormatStorageSerDe implements SerDe {
  public static final Log LOG = LogFactory.getLog(FormatStorageSerDe.class);

  TypeInfo rowTypeInfo;
  IFieldMap fieldMap = null;
  TreeMap<Integer, IRecord.IFType> sortedFieldTypes = null;
  ArrayList<Integer> idxs = null;
  static String[] typesname = new String[10];
  static {
    typesname[ConstVar.FieldType_Byte] = Constants.TINYINT_TYPE_NAME;
    typesname[ConstVar.FieldType_Short] = Constants.SMALLINT_TYPE_NAME;
    typesname[ConstVar.FieldType_Int] = Constants.INT_TYPE_NAME;
    typesname[ConstVar.FieldType_Long] = Constants.BIGINT_TYPE_NAME;
    typesname[ConstVar.FieldType_Float] = Constants.FLOAT_TYPE_NAME;
    typesname[ConstVar.FieldType_Double] = Constants.DOUBLE_TYPE_NAME;
    typesname[ConstVar.FieldType_String] = Constants.STRING_TYPE_NAME;
  }

  final public static byte[] DefaultSeparators = { (byte) 1, (byte) 2, (byte) 3 };

  SerDeParameters serdeParams = null;
  ObjectInspector objectInspector = null;

  public static class SerDeParameters {
    byte[] separators = DefaultSeparators;
    String nullString;
    TypeInfo rowTypeInfo;
    boolean lastColumnTakesRest;
    List<String> columnNames;
    List<TypeInfo> columnTypes;
    List<Byte> columnTypesFormat;

    boolean escaped;
    byte escapeChar;
    boolean[] needsEscape;
    boolean jsonSerialize;

    public List<TypeInfo> getColumnTypes() {
      return columnTypes;
    }

    public int getColumnNum() {
      return columnNames.size();
    }

    public List<String> getColumnNames() {
      return columnNames;
    }

  }

  public FormatStorageSerDe() throws SerDeException {
  }

  @Override
  public void initialize(Configuration job, Properties tbl)
      throws SerDeException {
    serdeParams = initSerdeParams(job, tbl, getClass().getName());

    try {
      List<ObjectInspector> objectInspectors = getObjectInspectors(serdeParams
          .getColumnTypes());
      objectInspector = ObjectInspectorFactory
          .getStandardStructObjectInspector(serdeParams.getColumnNames(),
              objectInspectors);

      initHead(job, tbl);
    } catch (Exception e) {
      e.printStackTrace();
      throw new SerDeException(e.getMessage());
    }

  }

  @Override
  public Writable serialize(Object object, ObjectInspector objInspector)
      throws SerDeException {
    if (objInspector.getCategory() != Category.STRUCT) {
      throw new SerDeException(getClass().toString()
          + " can only serialize struct types, but we got: "
          + objInspector.getTypeName());
    }

    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    List<Object> list = soi.getStructFieldsDataAsList(object);
    List<? extends StructField> declaredFields = null;
    if (serdeParams.rowTypeInfo != null
        && ((StructTypeInfo) serdeParams.rowTypeInfo).getAllStructFieldNames()
            .size() > 0) {
      declaredFields = ((StandardStructObjectInspector) getObjectInspector())
          .getAllStructFieldRefs();
    }

    try {
      IRecord record = new IRecord(this.fieldMap.fieldtypes());

      for (int i = 0; i < fields.size(); i++) {
        ObjectInspector foi = fields.get(i).getFieldObjectInspector();
        Object f = (list == null ? null : list.get(i));
        if (declaredFields != null && i >= declaredFields.size()) {
          throw new SerDeException("Error: expecting " + declaredFields.size()
              + " but asking for field " + i + "\n" + "data=" + object + "\n"
              + "tableType=" + serdeParams.rowTypeInfo.toString() + "\n"
              + "dataType="
              + TypeInfoUtils.getTypeInfoFromObjectInspector(objInspector));
        }

        IRecord.IFValue fieldValue = serialize(f, foi, i);

        if (fieldValue != null)
          record.addFieldValue(fieldValue);
      }

      return record;
    } catch (Exception e) {
      e.printStackTrace();
      throw new SerDeException(e);
    }
  }

  @Override
  public Object deserialize(Writable field) throws SerDeException {
    IRecord record = (IRecord) field;

    ArrayList<Object> objectList = new ArrayList<Object>();

    int i = 0;
    for (Integer idx : this.idxs) {
      IRecord.IFValue val = record.getByIdx(idx);
      Object ooo = (val != null ? val
          .fieldValue(this.serdeParams.columnTypesFormat.get(i)) : null);
      if (serdeParams.columnTypes.get(i).getTypeName()
          .equalsIgnoreCase(Constants.TIMESTAMP_TYPE_NAME)) {
        if (ooo != null) {
          Timestamp ttt = null;
          try {
            ttt = Timestamp.valueOf((String) ooo);
          } catch (Exception e) {
            LOG.error("LazyTimestamp init failed: " + (String) ooo);
          }
          objectList.add(ttt);
        } else {
          objectList.add(ooo);
        }
      } else {
        objectList.add(ooo);
      }

      i++;

    }
    return objectList;
  }

  public static SerDeParameters initSerdeParams(Configuration job,
      Properties tbl, String serdeName) throws SerDeException {
    SerDeParameters serdeParams = new SerDeParameters();

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
        if (i > 0) {
          sb.append(":");
        }

        sb.append(Constants.STRING_TYPE_NAME);
      }
      columnTypeProperty = sb.toString();
    }

    serdeParams.columnTypes = TypeInfoUtils
        .getTypeInfosFromTypeString(columnTypeProperty);
    serdeParams.columnTypesFormat = new ArrayList<Byte>(
        serdeParams.columnTypes.size());
    for (TypeInfo ti : serdeParams.columnTypes) {
      if (ti.getTypeName().equals(Constants.TINYINT_TYPE_NAME)) {
        serdeParams.columnTypesFormat.add(ConstVar.FieldType_Byte);
      } else if (ti.getTypeName().equals(Constants.SMALLINT_TYPE_NAME)) {
        serdeParams.columnTypesFormat.add(ConstVar.FieldType_Short);
      } else if (ti.getTypeName().equals(Constants.INT_TYPE_NAME)) {
        serdeParams.columnTypesFormat.add(ConstVar.FieldType_Int);
      } else if (ti.getTypeName().equals(Constants.BIGINT_TYPE_NAME)) {
        serdeParams.columnTypesFormat.add(ConstVar.FieldType_Long);
      } else if (ti.getTypeName().equals(Constants.FLOAT_TYPE_NAME)) {
        serdeParams.columnTypesFormat.add(ConstVar.FieldType_Float);
      } else if (ti.getTypeName().equals(Constants.DOUBLE_TYPE_NAME)) {
        serdeParams.columnTypesFormat.add(ConstVar.FieldType_Double);
      } else if (ti.getTypeName().equals(Constants.STRING_TYPE_NAME)) {
        serdeParams.columnTypesFormat.add(ConstVar.FieldType_String);
      } else if (ti.getTypeName().equals(Constants.TIMESTAMP_TYPE_NAME)) {
        serdeParams.columnTypesFormat.add(ConstVar.FieldType_String);
      } else {
        serdeParams.columnTypesFormat.add(ConstVar.FieldType_Unknown);
      }
    }

    if (serdeParams.columnNames.size() != serdeParams.columnTypes.size()) {
      throw new SerDeException(serdeName + ": columns has "
          + serdeParams.columnNames.size()
          + " elements while columns.types has "
          + serdeParams.columnTypes.size() + " elements!");
    }

    serdeParams.rowTypeInfo = TypeInfoFactory.getStructTypeInfo(
        serdeParams.columnNames, serdeParams.columnTypes);

    return serdeParams;
  }

  private List<ObjectInspector> getObjectInspectors(List<TypeInfo> typeInfos)
      throws IOException {
    List<ObjectInspector> ois = new ArrayList<ObjectInspector>(10);
    ObjectInspector oi = null;

    int size = typeInfos.size();
    for (int i = 0; i < size; i++) {
      TypeInfo typeInfo = typeInfos.get(i);
      oi = PrimitiveObjectInspectorFactory
          .getPrimitiveJavaObjectInspector(((PrimitiveTypeInfo) typeInfo)
              .getPrimitiveCategory());

      ois.add(oi);
    }

    return ois;
  }

  private void initHead(Configuration conf, Properties tbl) throws Exception {
    fieldMap = new IFieldMap();
    sortedFieldTypes = new TreeMap<Integer, IFType>();

    List<TypeInfo> typeInfos = serdeParams.getColumnTypes();
    int size = serdeParams.getColumnNum();
    for (int i = 0; i < size; i++) {
      TypeInfo typeInfo = typeInfos.get(i);
      String typeName = typeInfo.getTypeName();

      byte fieldType = ConstVar.FieldType_Unknown;
      if (typeName == Constants.TINYINT_TYPE_NAME) {
        fieldType = ConstVar.FieldType_Byte;
        fieldMap.addFieldType(new IRecord.IFType(fieldType, i));
      } else if (typeName == Constants.SMALLINT_TYPE_NAME) {
        fieldType = ConstVar.FieldType_Short;
        fieldMap.addFieldType(new IRecord.IFType(fieldType, i));
      } else if (typeName == Constants.INT_TYPE_NAME) {
        fieldType = ConstVar.FieldType_Int;
        fieldMap.addFieldType(new IRecord.IFType(fieldType, i));
      } else if (typeName == Constants.BIGINT_TYPE_NAME) {
        fieldType = ConstVar.FieldType_Long;
        fieldMap.addFieldType(new IRecord.IFType(fieldType, i));
      } else if (typeName == Constants.FLOAT_TYPE_NAME) {
        fieldType = ConstVar.FieldType_Float;
        fieldMap.addFieldType(new IRecord.IFType(fieldType, i));
      } else if (typeName == Constants.DOUBLE_TYPE_NAME) {
        fieldType = ConstVar.FieldType_Double;
        fieldMap.addFieldType(new IRecord.IFType(fieldType, i));
      } else if (typeName == Constants.STRING_TYPE_NAME) {
        fieldType = ConstVar.FieldType_String;
        fieldMap.addFieldType(new IRecord.IFType(fieldType, i));
      } else if (typeName == Constants.TIMESTAMP_TYPE_NAME) {
        fieldType = ConstVar.FieldType_String;
        fieldMap.addFieldType(new IRecord.IFType(fieldType, i));
      } else {
        throw new SEException.NotSupportException("not support typeName:"
            + typeName);
      }
    }
    for (IFType type : fieldMap.fieldtypes().values()) {
      sortedFieldTypes.put(type.idx(), type);
    }
    idxs = new ArrayList<Integer>();
    idxs.addAll(sortedFieldTypes.keySet());

    IHead head = new IHead(ConstVar.OldFormatFile);

    String compress = tbl.getProperty(ConstVar.Compress);
    String tableType = tbl.getProperty(ConstVar.TableType);
    String projection = tbl.getProperty(ConstVar.Projection);

    int indexNum = 0;
    String indexNumString = tbl.getProperty("indexNum");
    if (indexNumString != null && indexNumString.length() != 0) {
      indexNum = Integer.valueOf(indexNumString);
    }

    for (int i = 0; i < indexNum; i++) {
      String indexInfoString = tbl.getProperty("index" + i);
      if (indexInfoString != null) {
        String[] indexInfo = indexInfoString.split(";");

        if (indexInfo != null && indexInfo[2].equalsIgnoreCase("0")) {
          conf.set(ConstVar.HD_primaryIndex, indexInfo[1]);
          break;
        }
      }
    }

    if (tableType != null)
      conf.set(ConstVar.TableType, tableType);

    if (projection != null)
      conf.set(ConstVar.Projection, projection);

    if (compress != null && compress.equalsIgnoreCase("true")) {
      head.setCompress((byte) 1);
    }

    head.setFieldMap(fieldMap);
    head.toJobConf(conf);
    conf.set("ifdf.head.info", head.toStr());
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return objectInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return IRecord.class;
  }

  private IRecord.IFValue serialize(Object f, ObjectInspector foi, int i) {
    switch (foi.getCategory()) {
    case PRIMITIVE:
      return serializePrimitive(f, (PrimitiveObjectInspector) foi, i);
    default:
    }
    return null;
  }

  private IRecord.IFValue serializePrimitive(Object obj,
      PrimitiveObjectInspector ssoi, int i) {

    if (obj == null)
      return null;
    switch (ssoi.getPrimitiveCategory()) {
    case BOOLEAN: {
      return null;
    }
    case BYTE: {
      byte value = ((ByteObjectInspector) ssoi).get(obj);
      return new IRecord.IFValue(value, i);
    }
    case SHORT: {
      short value = ((ShortObjectInspector) ssoi).get(obj);
      return new IRecord.IFValue(value, i);
    }
    case INT: {

      int value = ((IntObjectInspector) ssoi).get(obj);
      return new IRecord.IFValue(value, i);

    }
    case LONG: {
      long value = ((LongObjectInspector) ssoi).get(obj);
      return new IRecord.IFValue(value, i);
    }
    case FLOAT: {
      float value = ((FloatObjectInspector) ssoi).get(obj);
      return new IRecord.IFValue(value, i);
    }
    case DOUBLE: {
      double value = ((DoubleObjectInspector) ssoi).get(obj);
      return new IRecord.IFValue(value, i);
    }
    case STRING: {

      String value = ((StringObjectInspector) ssoi).getPrimitiveJavaObject(obj);
      return new IRecord.IFValue(value, i);
    }
    case TIMESTAMP: {
      Timestamp value = ((TimestampObjectInspector) ssoi)
          .getPrimitiveJavaObject(obj);
      return new IRecord.IFValue(value.toString(), i);
    }
    }
    return null;

  }

}
