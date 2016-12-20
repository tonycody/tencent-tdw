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
package org.apache.hadoop.hive.serde2.protobuf;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.protobuf.objectinspector.ProtobufObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.protobuf.objectinspector.ProtobufStructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.ByteStream;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;

public class ProtobufSerDe implements SerDe {

  public static final Log LOG = LogFactory
      .getLog(ProtobufSerDe.class.getName());

  ProtobufStructObjectInspector rowObjectInspector;
  Method parseFromMethod = null;

  @Override
  public void initialize(Configuration conf, Properties tbl)
      throws SerDeException {
    String columnNameProperty = tbl.getProperty(Constants.LIST_COLUMNS);
    String columnTypeProperty = tbl.getProperty(Constants.LIST_COLUMN_TYPES);
    String msgName = tbl.getProperty(Constants.PB_MSG_NAME);
    String outerName = tbl.getProperty(Constants.PB_OUTER_CLASS_NAME);

    List<String> columnNames = Arrays.asList(columnNameProperty.split(","));
    List<TypeInfo> columnTypes = TypeInfoUtils
        .getTypeInfosFromTypeString(columnTypeProperty);
    assert columnNames.size() == columnTypes.size();

    Class<?> tableMsgClass = ProtobufUtils
        .loadTableMsgClass(outerName, msgName);
    try {
      parseFromMethod = tableMsgClass.getMethod("parseFrom",
          CodedInputStream.class);
    } catch (java.lang.NoSuchMethodException e) {
      throw new SerDeException(e.getMessage());
    }

    Descriptor tableMsgDescriptor = ProtobufUtils
        .getMsgDescriptor(tableMsgClass);
    List<FieldDescriptor> fieldDescriptors = tableMsgDescriptor.getFields();

    ArrayList<ObjectInspector> columnObjectInspectors = new ArrayList<ObjectInspector>(
        columnTypes.size());
    for (int i = 0; i < columnTypes.size(); ++i) {
      TypeInfo ti = columnTypes.get(i);
      ObjectInspector oi = ProtobufObjectInspectorFactory
          .getFieldObjectInspectorFromTypeInfo(columnTypes.get(i),
              fieldDescriptors.get(i));
      columnObjectInspectors.add(oi);
    }
    rowObjectInspector = ProtobufObjectInspectorFactory
        .getProtobufStructObjectInspector(columnNames, columnObjectInspectors,
            tableMsgDescriptor);
  }

  @Override
  public String toString() {
    return rowObjectInspector.getTypeName();
  }

  @Override
  public StructObjectInspector getObjectInspector() throws SerDeException {
    return rowObjectInspector;
  }

  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    BytesWritable bw = (BytesWritable) blob;
    if (bw.getSize() <= 0) {
      return null;
    }
    CodedInputStream cis = CodedInputStream.newInstance(bw.getBytes(), 0,
        bw.getSize());
    Object message = null;
    try {
      message = parseFromMethod.invoke(null, cis);
    } catch (java.lang.IllegalAccessException e) {
      throw new SerDeException(e.getMessage());
    } catch (java.lang.reflect.InvocationTargetException e) {
      String errmsg = e.getMessage();
      Throwable cause = e.getCause();
      if (cause instanceof InvalidProtocolBufferException) {
        errmsg = "__InvalidProtocolBufferException:" + errmsg;
      }
      throw new SerDeException(errmsg);
    }
    return message;
  }

  BytesWritable serializeBytesWritable = new BytesWritable();
  ByteStream.Output bos = new ByteStream.Output();
  CodedOutputStream cos = CodedOutputStream.newInstance(bos);

  static final int WIRETYPE_VARINT = 0;
  static final int WIRETYPE_FIXED64 = 1;
  static final int WIRETYPE_LENGTH_DELIMITED = 2;
  static final int WIRETYPE_START_GROUP = 3;
  static final int WIRETYPE_END_GROUP = 4;
  static final int WIRETYPE_FIXED32 = 5;

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return BytesWritable.class;
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector)
      throws SerDeException {
    try {
      bos.reset();
      StructObjectInspector soi = (StructObjectInspector) objInspector;
      Descriptor descriptor = rowObjectInspector.getDescriptor();

      serializeMessage(cos, obj, soi, descriptor);
      cos.flush();

      serializeBytesWritable.set(bos.getData(), 0, bos.getLength());
      return serializeBytesWritable;
    } catch (java.io.IOException e) {
      throw new SerDeException(e.getMessage());
    }
  }

  static void serializeMessage(CodedOutputStream cos, Object obj,
      StructObjectInspector soi, Descriptor descriptor)
      throws java.io.IOException {
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    List<FieldDescriptor> fieldDescriptors = descriptor.getFields();
    assert (fields.size() == fieldDescriptors.size());

    for (int i = 0; i < fields.size(); i++) {
      serializeField(cos, soi.getStructFieldData(obj, fields.get(i)), fields
          .get(i).getFieldObjectInspector(), fieldDescriptors.get(i));
    }
  }

  static void serializeField(CodedOutputStream cos, Object o,
      ObjectInspector oi, FieldDescriptor fd) throws java.io.IOException {
    if (o == null) {

      if (fd.isRequired()) {
        if (fd.getJavaType() != FieldDescriptor.JavaType.MESSAGE) {
          Object defaultValue = fd.getDefaultValue();
          PrimitiveObjectInspector poiOld = (PrimitiveObjectInspector) oi;
          PrimitiveObjectInspector poiNew = PrimitiveObjectInspectorFactory
              .getPrimitiveJavaObjectInspector(poiOld.getPrimitiveCategory());
          serializeField(cos, defaultValue, poiNew, fd, false);
        } else {
          StructObjectInspector soi = (StructObjectInspector) oi;
          if (fd.getType() == FieldDescriptor.Type.MESSAGE) {
            serializeFieldGroup(cos, o, soi, fd, false);
          } else {
            serializeFieldMessage(cos, o, soi, fd, false);
          }
        }
      }
      return;
    }
    if (fd.isPacked()) {
      ListObjectInspector loi = (ListObjectInspector) oi;
      serializePackedField(cos, o, loi, fd);
    } else if (fd.isRepeated()) {
      ListObjectInspector loi = (ListObjectInspector) oi;
      serializeRepeatedField(cos, o, loi, fd);
    } else {
      serializeField(cos, o, oi, fd, false);
    }
  }

  static void serializeRepeatedField(CodedOutputStream cos, Object o,
      ListObjectInspector loi, FieldDescriptor fd) throws java.io.IOException {
    if (loi.getListLength(o) <= 0) {
      return;
    }

    ObjectInspector oi = loi.getListElementObjectInspector();
    for (Object element : loi.getList(o)) {
      serializeField(cos, element, oi, fd, false);
    }
  }

  static void serializePackedField(CodedOutputStream cos, Object o,
      ListObjectInspector loi, FieldDescriptor fd) throws java.io.IOException {
    if (loi.getListLength(o) <= 0) {
      return;
    }

    ByteStream.Output bos2 = new ByteStream.Output();
    CodedOutputStream cos2 = CodedOutputStream.newInstance(bos2);

    ObjectInspector oi2 = loi.getListElementObjectInspector();
    for (Object element : loi.getList(o)) {
      serializeField(cos2, element, oi2, fd, true);
    }
    cos2.flush();

    cos.writeTag(fd.getNumber(), WIRETYPE_LENGTH_DELIMITED);
    cos.writeRawVarint32(bos2.getLength());
    cos.writeRawBytes(bos2.getData(), 0, bos2.getLength());
  }

  static void serializeField(CodedOutputStream cos, Object o,
      ObjectInspector oi, FieldDescriptor fd, boolean noTag)
      throws java.io.IOException {
    if (o == null) {
      return;
    }
    switch (fd.getType()) {
    case DOUBLE: {
      DoubleObjectInspector doi = (DoubleObjectInspector) oi;
      serializeFieldDouble(cos, o, doi, fd, noTag);
      break;
    }
    case FLOAT: {
      FloatObjectInspector foi = (FloatObjectInspector) oi;
      serializeFieldFloat(cos, o, foi, fd, noTag);
      break;
    }
    case INT64: {
      LongObjectInspector loi = (LongObjectInspector) oi;
      serializeFieldInt64(cos, o, loi, fd, noTag);
      break;
    }
    case UINT64: {
      LongObjectInspector loi = (LongObjectInspector) oi;
      serializeFieldUInt64(cos, o, loi, fd, noTag);
      break;
    }
    case INT32: {
      IntObjectInspector ioi = (IntObjectInspector) oi;
      serializeFieldInt32(cos, o, ioi, fd, noTag);
      break;
    }
    case FIXED64: {
      LongObjectInspector loi = (LongObjectInspector) oi;
      serializeFieldFixed64(cos, o, loi, fd, noTag);
      break;
    }
    case FIXED32: {
      IntObjectInspector ioi = (IntObjectInspector) oi;
      serializeFieldFixed32(cos, o, ioi, fd, noTag);
      break;
    }
    case BOOL: {
      BooleanObjectInspector boi = (BooleanObjectInspector) oi;
      serializeFieldBool(cos, o, boi, fd, noTag);
      break;
    }
    case STRING: {
      StringObjectInspector soi = (StringObjectInspector) oi;
      serializeFieldString(cos, o, soi, fd, noTag);
      break;
    }
    case GROUP: {
      StructObjectInspector soi = (StructObjectInspector) oi;
      serializeFieldGroup(cos, o, soi, fd, noTag);
      break;
    }
    case MESSAGE: {
      StructObjectInspector soi = (StructObjectInspector) oi;
      serializeFieldMessage(cos, o, soi, fd, noTag);
      break;
    }
    case BYTES:
      break;
    case UINT32: {
      IntObjectInspector ioi = (IntObjectInspector) oi;
      serializeFieldUInt32(cos, o, ioi, fd, noTag);
      break;
    }
    case ENUM:
      break;
    case SFIXED32: {
      IntObjectInspector ioi = (IntObjectInspector) oi;
      serializeFieldSFixed32(cos, o, ioi, fd, noTag);
      break;
    }
    case SFIXED64: {
      LongObjectInspector loi = (LongObjectInspector) oi;
      serializeFieldSFixed64(cos, o, loi, fd, noTag);
      break;
    }
    case SINT32: {
      IntObjectInspector ioi = (IntObjectInspector) oi;
      serializeFieldSInt32(cos, o, ioi, fd, noTag);
      break;
    }
    case SINT64: {
      LongObjectInspector loi = (LongObjectInspector) oi;
      serializeFieldSInt64(cos, o, loi, fd, noTag);
      break;
    }
    default:
      assert (false);
    }
  }

  static void serializeFieldDouble(CodedOutputStream cos, Object o,
      DoubleObjectInspector doi, FieldDescriptor fd, boolean noTag)
      throws java.io.IOException {
    double value = doi.get(o);
    if (fd.isOptional() && fd.hasDefaultValue()) {
      if (((Double) fd.getDefaultValue()).equals(value)) {
        return;
      }
    }
    if (!noTag) {
      cos.writeDouble(fd.getNumber(), value);
    } else {
      cos.writeDoubleNoTag(value);
    }
  }

  static void serializeFieldFloat(CodedOutputStream cos, Object o,
      FloatObjectInspector foi, FieldDescriptor fd, boolean noTag)
      throws java.io.IOException {
    float value = foi.get(o);
    if (fd.isOptional() && fd.hasDefaultValue()) {
      if (((Float) fd.getDefaultValue()).equals(value)) {
        return;
      }
    }
    if (!noTag) {
      cos.writeFloat(fd.getNumber(), value);
    } else {
      cos.writeFloatNoTag(value);
    }
  }

  static void serializeFieldInt64(CodedOutputStream cos, Object o,
      LongObjectInspector loi, FieldDescriptor fd, boolean noTag)
      throws java.io.IOException {
    long value = loi.get(o);
    if (fd.isOptional() && fd.hasDefaultValue()) {
      if (((Long) fd.getDefaultValue()).equals(value)) {
        return;
      }
    }
    if (!noTag) {
      cos.writeInt64(fd.getNumber(), value);
    } else {
      cos.writeInt64NoTag(value);
    }
  }

  static void serializeFieldUInt64(CodedOutputStream cos, Object o,
      LongObjectInspector loi, FieldDescriptor fd, boolean noTag)
      throws java.io.IOException {
    long value = loi.get(o);
    if (fd.isOptional() && fd.hasDefaultValue()) {
      if (((Long) fd.getDefaultValue()).equals(value)) {
        return;
      }
    }
    if (!noTag) {
      cos.writeUInt64(fd.getNumber(), value);
    } else {
      cos.writeUInt64NoTag(value);
    }
  }

  static void serializeFieldInt32(CodedOutputStream cos, Object o,
      IntObjectInspector ioi, FieldDescriptor fd, boolean noTag)
      throws java.io.IOException {
    int value = ioi.get(o);
    if (fd.isOptional() && fd.hasDefaultValue()) {
      if (((Integer) fd.getDefaultValue()).equals(value)) {
        return;
      }
    }
    if (!noTag) {
      cos.writeInt32(fd.getNumber(), value);
    } else {
      cos.writeInt32NoTag(value);
    }
  }

  static void serializeFieldUInt32(CodedOutputStream cos, Object o,
      IntObjectInspector ioi, FieldDescriptor fd, boolean noTag)
      throws java.io.IOException {
    int value = ioi.get(o);
    if (fd.isOptional() && fd.hasDefaultValue()) {
      if (((Integer) fd.getDefaultValue()).equals(value)) {
        return;
      }
    }
    if (!noTag) {
      cos.writeUInt32(fd.getNumber(), value);
    } else {
      cos.writeUInt32NoTag(value);
    }
  }

  static void serializeFieldFixed64(CodedOutputStream cos, Object o,
      LongObjectInspector loi, FieldDescriptor fd, boolean noTag)
      throws java.io.IOException {
    long value = loi.get(o);
    if (fd.isOptional() && fd.hasDefaultValue()) {
      if (((Long) fd.getDefaultValue()).equals(value)) {
        return;
      }
    }
    if (!noTag) {
      cos.writeFixed64(fd.getNumber(), value);
    } else {
      cos.writeFixed64NoTag(value);
    }
  }

  static void serializeFieldFixed32(CodedOutputStream cos, Object o,
      IntObjectInspector ioi, FieldDescriptor fd, boolean noTag)
      throws java.io.IOException {
    int value = ioi.get(o);
    if (fd.isOptional() && fd.hasDefaultValue()) {
      if (((Integer) fd.getDefaultValue()).equals(value)) {
        return;
      }
    }
    if (!noTag) {
      cos.writeFixed32(fd.getNumber(), value);
    } else {
      cos.writeFixed32NoTag(value);
    }
  }

  static void serializeFieldBool(CodedOutputStream cos, Object o,
      BooleanObjectInspector boi, FieldDescriptor fd, boolean noTag)
      throws java.io.IOException {
    boolean value = boi.get(o);
    if (fd.isOptional() && fd.hasDefaultValue()) {
      if (value == ((Boolean) fd.getDefaultValue()).booleanValue()) {
        return;
      }
    }
    if (!noTag) {
      cos.writeBool(fd.getNumber(), value);
    } else {
      cos.writeBoolNoTag(value);
    }
  }

  static void serializeFieldString(CodedOutputStream cos, Object o,
      StringObjectInspector soi, FieldDescriptor fd, boolean noTag /* ommited */)
      throws java.io.IOException {
    String value = soi.getPrimitiveJavaObject(o);
    cos.writeString(fd.getNumber(), value);
  }

  static void serializeFieldGroup(CodedOutputStream cos, Object o,
      StructObjectInspector soi, FieldDescriptor fd, boolean noTag /* ommited */)
      throws java.io.IOException {
    cos.writeTag(fd.getNumber(), WIRETYPE_START_GROUP);
    serializeMessage(cos, o, soi, fd.getMessageType());
    cos.writeTag(fd.getNumber(), WIRETYPE_END_GROUP);
  }

  static void serializeFieldMessage(CodedOutputStream cos, Object o,
      StructObjectInspector soi, FieldDescriptor fd, boolean noTag /* ommited */)
      throws java.io.IOException {
    ByteStream.Output bos2 = new ByteStream.Output();
    CodedOutputStream cos2 = CodedOutputStream.newInstance(bos2);
    serializeMessage(cos2, o, soi, fd.getMessageType());
    cos2.flush();

    cos.writeTag(fd.getNumber(), WIRETYPE_LENGTH_DELIMITED);
    cos.writeRawVarint32(bos2.getLength());
    cos.writeRawBytes(bos2.getData(), 0, bos2.getLength());
  }

  static void serializeFieldSFixed32(CodedOutputStream cos, Object o,
      IntObjectInspector ioi, FieldDescriptor fd, boolean noTag)
      throws java.io.IOException {
    int value = ioi.get(o);
    if (fd.isOptional() && fd.hasDefaultValue()) {
      if (((Integer) fd.getDefaultValue()).equals(value)) {
        return;
      }
    }
    if (!noTag) {
      cos.writeSFixed32(fd.getNumber(), value);
    } else {
      cos.writeSFixed32NoTag(value);
    }
  }

  static void serializeFieldSFixed64(CodedOutputStream cos, Object o,
      LongObjectInspector loi, FieldDescriptor fd, boolean noTag)
      throws java.io.IOException {
    long value = loi.get(o);
    if (fd.isOptional() && fd.hasDefaultValue()) {
      if (((Long) fd.getDefaultValue()).equals(value)) {
        return;
      }
    }
    if (!noTag) {
      cos.writeSFixed64(fd.getNumber(), value);
    } else {
      cos.writeSFixed64NoTag(value);
    }
  }

  static void serializeFieldSInt32(CodedOutputStream cos, Object o,
      IntObjectInspector ioi, FieldDescriptor fd, boolean noTag)
      throws java.io.IOException {
    int value = ioi.get(o);
    if (fd.isOptional() && fd.hasDefaultValue()) {
      if (((Integer) fd.getDefaultValue()).equals(value)) {
        return;
      }
    }
    if (!noTag) {
      cos.writeSInt32(fd.getNumber(), value);
    } else {
      cos.writeSInt32NoTag(value);
    }
  }

  static void serializeFieldSInt64(CodedOutputStream cos, Object o,
      LongObjectInspector loi, FieldDescriptor fd, boolean noTag)
      throws java.io.IOException {
    long value = loi.get(o);
    if (fd.isOptional() && fd.hasDefaultValue()) {
      if (((Long) fd.getDefaultValue()).equals(value)) {
        return;
      }
    }
    if (!noTag) {
      cos.writeSInt64(fd.getNumber(), value);
    } else {
      cos.writeSInt64NoTag(value);
    }
  }

}
