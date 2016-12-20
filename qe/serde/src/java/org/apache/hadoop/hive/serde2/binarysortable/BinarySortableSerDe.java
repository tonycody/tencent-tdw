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

package org.apache.hadoop.hive.serde2.binarysortable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ByteArrayObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardUnionObjectInspector.StandardUnion;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class BinarySortableSerDe implements SerDe {

  public static final Log LOG = LogFactory.getLog(BinarySortableSerDe.class
      .getName());

  List<String> columnNames;
  List<TypeInfo> columnTypes;

  TypeInfo rowTypeInfo;
  StructObjectInspector rowObjectInspector;

  boolean[] columnSortOrderIsDesc;

  @Override
  public void initialize(Configuration conf, Properties tbl)
      throws SerDeException {

    String columnNameProperty = tbl.getProperty(Constants.LIST_COLUMNS);
    String columnTypeProperty = tbl.getProperty(Constants.LIST_COLUMN_TYPES);
    if (columnNameProperty.length() == 0) {
      columnNames = new ArrayList<String>();
    } else {
      columnNames = Arrays.asList(columnNameProperty.split(","));
    }
    if (columnTypeProperty.length() == 0) {
      columnTypes = new ArrayList<TypeInfo>();
    } else {
      columnTypes = TypeInfoUtils
          .getTypeInfosFromTypeString(columnTypeProperty);
    }
    assert (columnNames.size() == columnTypes.size());

    rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
    rowObjectInspector = (StructObjectInspector) TypeInfoUtils
        .getStandardWritableObjectInspectorFromTypeInfo(rowTypeInfo);
    row = new ArrayList<Object>(columnNames.size());
    for (int i = 0; i < columnNames.size(); i++) {
      row.add(null);
    }

    String columnSortOrder = tbl
        .getProperty(Constants.SERIALIZATION_SORT_ORDER);
    columnSortOrderIsDesc = new boolean[columnNames.size()];
    for (int i = 0; i < columnSortOrderIsDesc.length; i++) {
      columnSortOrderIsDesc[i] = (columnSortOrder != null && columnSortOrder
          .charAt(i) == '-');
    }
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return BytesWritable.class;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return rowObjectInspector;
  }

  ArrayList<Object> row;
  InputByteBuffer inputByteBuffer = new InputByteBuffer();

  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    BytesWritable data = (BytesWritable) blob;
    inputByteBuffer.reset(data.get(), 0, data.getSize());

    try {
      for (int i = 0; i < columnNames.size(); i++) {
        row.set(
            i,
            deserialize(inputByteBuffer, columnTypes.get(i),
                columnSortOrderIsDesc[i], row.get(i)));
      }
    } catch (IOException e) {
      throw new SerDeException(e);
    }

    return row;
  }

  static Object deserialize(InputByteBuffer buffer, TypeInfo type,
      boolean invert, Object reuse) throws IOException {

    byte isNull = buffer.read(invert);
    if (isNull == 0) {
      return null;
    }
    assert (isNull == 1);

    switch (type.getCategory()) {
    case PRIMITIVE: {
      PrimitiveTypeInfo ptype = (PrimitiveTypeInfo) type;
      switch (ptype.getPrimitiveCategory()) {
      case VOID: {
        return null;
      }
      case BOOLEAN: {
        BooleanWritable r = reuse == null ? new BooleanWritable()
            : (BooleanWritable) reuse;
        byte b = buffer.read(invert);
        assert (b == 1 || b == 2);
        r.set(b == 2);
        return r;
      }
      case BYTE: {
        ByteWritable r = reuse == null ? new ByteWritable()
            : (ByteWritable) reuse;
        r.set((byte) (buffer.read(invert) ^ 0x80));
        return r;
      }
      case SHORT: {
        ShortWritable r = reuse == null ? new ShortWritable()
            : (ShortWritable) reuse;
        int v = buffer.read(invert) ^ 0x80;
        v = (v << 8) + (buffer.read(invert) & 0xff);
        r.set((short) v);
        return r;
      }
      case INT: {
        IntWritable r = reuse == null ? new IntWritable() : (IntWritable) reuse;
        int v = buffer.read(invert) ^ 0x80;
        for (int i = 0; i < 3; i++) {
          v = (v << 8) + (buffer.read(invert) & 0xff);
        }
        r.set(v);
        return r;
      }
      case LONG: {
        LongWritable r = reuse == null ? new LongWritable()
            : (LongWritable) reuse;
        long v = buffer.read(invert) ^ 0x80;
        for (int i = 0; i < 7; i++) {
          v = (v << 8) + (buffer.read(invert) & 0xff);
        }
        r.set(v);
        return r;
      }
      case FLOAT: {
        FloatWritable r = reuse == null ? new FloatWritable()
            : (FloatWritable) reuse;
        int v = 0;
        for (int i = 0; i < 4; i++) {
          v = (v << 8) + (buffer.read(invert) & 0xff);
        }
        if ((v & (1 << 31)) == 0) {
          v = ~v;
        } else {
          v = v ^ (1 << 31);
        }
        r.set(Float.intBitsToFloat(v));
        return r;
      }
      case DOUBLE: {
        DoubleWritable r = reuse == null ? new DoubleWritable()
            : (DoubleWritable) reuse;
        long v = 0;
        for (int i = 0; i < 8; i++) {
          v = (v << 8) + (buffer.read(invert) & 0xff);
        }
        if ((v & (1L << 63)) == 0) {
          v = ~v;
        } else {
          v = v ^ (1L << 63);
        }
        r.set(Double.longBitsToDouble(v));
        return r;
      }
      case STRING: {
        Text r = reuse == null ? new Text() : (Text) reuse;
        int start = buffer.tell();
        int length = 0;
        do {
          byte b = buffer.read(invert);
          if (b == 0) {
            break;
          }
          if (b == 1) {
            buffer.read(invert);
          }
          length++;
        } while (true);

        if (length == buffer.tell() - start) {
          r.set(buffer.getData(), start, length);
        } else {
          r.set(buffer.getData(), start, length);
          buffer.seek(start);
          byte[] rdata = r.getBytes();
          for (int i = 0; i < length; i++) {
            byte b = buffer.read(invert);
            if (b == 1) {
              b = (byte) (buffer.read(invert) - 1);
            }
            rdata[i] = b;
          }
          byte b = buffer.read(invert);
          assert (b == 0);
        }
        return r;
      }
      case TIMESTAMP: {
        TimestampWritable t = (reuse == null ? new TimestampWritable()
            : (TimestampWritable) reuse);
        byte[] bytes = new byte[8];

        for (int i = 0; i < bytes.length; i++) {
          bytes[i] = buffer.read(invert);
        }
        t.setBinarySortable(bytes, 0);
        return t;
      }
      default: {
        throw new RuntimeException("Unrecognized type: "
            + ptype.getPrimitiveCategory());
      }
      }
    }
    
    
    case BYTEARRAY:
    {
      byte b1 = buffer.read(invert);
      byte b2 = buffer.read(invert);
      
      int size = ((int)(b1 & 0x7f) << 8)|(int)b2;
      
      byte [] r = new byte[size];
      int index = 0;

      while(index < size)
      {
        r[index] = buffer.read(invert);
        index++;
      }
      
      return r;
    }
    
    
    case LIST: {
      ListTypeInfo ltype = (ListTypeInfo) type;
      TypeInfo etype = ltype.getListElementTypeInfo();

      ArrayList<Object> r = reuse == null ? new ArrayList<Object>()
          : (ArrayList<Object>) reuse;

      int size = 0;
      while (true) {
        int more = buffer.read(invert);
        if (more == 0) {
          break;
        }
        assert (more == 1);
        if (size == r.size()) {
          r.add(null);
        }
        r.set(size, deserialize(buffer, etype, invert, r.get(size)));
        size++;
      }
      while (r.size() > size) {
        r.remove(r.size() - 1);
      }
      return r;
    }
    case MAP: {
      MapTypeInfo mtype = (MapTypeInfo) type;
      TypeInfo ktype = mtype.getMapKeyTypeInfo();
      TypeInfo vtype = mtype.getMapValueTypeInfo();

      Map<Object, Object> r;
      if (reuse == null) {
        r = new HashMap<Object, Object>();
      } else {
        r = (HashMap<Object, Object>) reuse;
        r.clear();
      }

      int size = 0;
      while (true) {
        int more = buffer.read(invert);
        if (more == 0) {
          break;
        }
        assert (more == 1);
        Object k = deserialize(buffer, ktype, invert, null);
        Object v = deserialize(buffer, vtype, invert, null);
        r.put(k, v);
      }
      return r;
    }
    case STRUCT: {
      StructTypeInfo stype = (StructTypeInfo) type;
      List<TypeInfo> fieldTypes = stype.getAllStructFieldTypeInfos();
      int size = fieldTypes.size();
      ArrayList<Object> r = reuse == null ? new ArrayList<Object>(size)
          : (ArrayList<Object>) reuse;
      assert (r.size() <= size);
      while (r.size() < size) {
        r.add(null);
      }
      for (int eid = 0; eid < size; eid++) {
        r.set(eid, deserialize(buffer, fieldTypes.get(eid), invert, r.get(eid)));
      }
      return r;
    }

    case UNION: {
      UnionTypeInfo utype = (UnionTypeInfo) type;
      StandardUnion r = reuse == null ? new StandardUnion()
          : (StandardUnion) reuse;
      byte tag = buffer.read(invert);
      r.setTag(tag);
      r.setObject(deserialize(buffer,
          utype.getAllUnionObjectTypeInfos().get(tag), invert, null));
      return r;
    }

    default: {
      throw new RuntimeException("Unrecognized type: " + type.getCategory());
    }
    }
  }

  BytesWritable serializeBytesWritable = new BytesWritable();
  OutputByteBuffer outputByteBuffer = new OutputByteBuffer();

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector)
      throws SerDeException {
    outputByteBuffer.reset();
    StructObjectInspector soi = (StructObjectInspector) objInspector;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();

    for (int i = 0; i < columnNames.size(); i++) {
      serialize(outputByteBuffer, soi.getStructFieldData(obj, fields.get(i)),
          fields.get(i).getFieldObjectInspector(), columnSortOrderIsDesc[i]);
    }

    serializeBytesWritable.set(outputByteBuffer.getData(), 0,
        outputByteBuffer.getLength());
    return serializeBytesWritable;
  }

  static void serialize(OutputByteBuffer buffer, Object o, ObjectInspector oi,
      boolean invert) {
    if (o == null) {
      buffer.write((byte) 0, invert);
      return;
    }
    buffer.write((byte) 1, invert);

    switch (oi.getCategory()) {
    case PRIMITIVE: {
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      switch (poi.getPrimitiveCategory()) {
      case VOID: {
        return;
      }
      case BOOLEAN: {
        BooleanObjectInspector boi = (BooleanObjectInspector) poi;
        boolean v = ((BooleanObjectInspector) poi).get(o);
        buffer.write((byte) (v ? 2 : 1), invert);
        return;
      }
      case BYTE: {
        ByteObjectInspector boi = (ByteObjectInspector) poi;
        byte v = boi.get(o);
        buffer.write((byte) (v ^ 0x80), invert);
        return;
      }
      case SHORT: {
        ShortObjectInspector spoi = (ShortObjectInspector) poi;
        short v = spoi.get(o);
        buffer.write((byte) ((v >> 8) ^ 0x80), invert);
        buffer.write((byte) v, invert);
        return;
      }
      case INT: {
        IntObjectInspector ioi = (IntObjectInspector) poi;
        int v = ioi.get(o);
        buffer.write((byte) ((v >> 24) ^ 0x80), invert);
        buffer.write((byte) (v >> 16), invert);
        buffer.write((byte) (v >> 8), invert);
        buffer.write((byte) v, invert);
        return;
      }
      case LONG: {
        LongObjectInspector loi = (LongObjectInspector) poi;
        long v = loi.get(o);
        buffer.write((byte) ((v >> 56) ^ 0x80), invert);
        buffer.write((byte) (v >> 48), invert);
        buffer.write((byte) (v >> 40), invert);
        buffer.write((byte) (v >> 32), invert);
        buffer.write((byte) (v >> 24), invert);
        buffer.write((byte) (v >> 16), invert);
        buffer.write((byte) (v >> 8), invert);
        buffer.write((byte) v, invert);
        return;
      }
      case FLOAT: {
        FloatObjectInspector foi = (FloatObjectInspector) poi;
        int v = Float.floatToIntBits(foi.get(o));
        if ((v & (1 << 31)) != 0) {
          v = ~v;
        } else {
          v = v ^ (1 << 31);
        }
        buffer.write((byte) (v >> 24), invert);
        buffer.write((byte) (v >> 16), invert);
        buffer.write((byte) (v >> 8), invert);
        buffer.write((byte) v, invert);
        return;
      }
      case DOUBLE: {
        DoubleObjectInspector doi = (DoubleObjectInspector) poi;
        long v = Double.doubleToLongBits(doi.get(o));
        if ((v & (1L << 63)) != 0) {
          v = ~v;
        } else {
          v = v ^ (1L << 63);
        }
        buffer.write((byte) (v >> 56), invert);
        buffer.write((byte) (v >> 48), invert);
        buffer.write((byte) (v >> 40), invert);
        buffer.write((byte) (v >> 32), invert);
        buffer.write((byte) (v >> 24), invert);
        buffer.write((byte) (v >> 16), invert);
        buffer.write((byte) (v >> 8), invert);
        buffer.write((byte) v, invert);
        return;
      }
      case STRING: {
        StringObjectInspector soi = (StringObjectInspector) poi;
        Text t = soi.getPrimitiveWritableObject(o);
        byte[] data = t.getBytes();
        int length = t.getLength();
        for (int i = 0; i < length; i++) {
          if (data[i] == 0 || data[i] == 1) {
            buffer.write((byte) 1, invert);
            buffer.write((byte) (data[i] + 1), invert);
          } else {
            buffer.write(data[i], invert);
          }
        }
        buffer.write((byte) 0, invert);
        return;
      }
      case TIMESTAMP: {
        TimestampObjectInspector toi = (TimestampObjectInspector) poi;
        TimestampWritable t = toi.getPrimitiveWritableObject(o);
        byte[] data = t.getBinarySortable();
        for (int i = 0; i < data.length; i++) {
          buffer.write(data[i], invert);
        }
        return;
      }
      default: {
        throw new RuntimeException("Unrecognized type: "
            + poi.getPrimitiveCategory());
      }
      }
    }
    
    
    
    case BYTEARRAY: {
      ByteArrayObjectInspector baoi = (ByteArrayObjectInspector)oi; 
      //byte [] data = baoi.getBytes(o);
      int size = baoi.getListLength(o);
      buffer.write((byte)((size >> 8) & 0xff ^ 0x80), invert);
      buffer.write((byte)(size & 0xff ), invert);
      
      for(int i = 0; i < size; i++){
        //LOG.info("****data[" + i + "]=" + data[i]);
        buffer.write(baoi.getListElement(o, i), invert);
      }
      break;
    }    
    
    case LIST: {
      ListObjectInspector loi = (ListObjectInspector) oi;
      ObjectInspector eoi = loi.getListElementObjectInspector();

      int size = loi.getListLength(o);
      for (int eid = 0; eid < size; eid++) {
        buffer.write((byte) 1, invert);
        serialize(buffer, loi.getListElement(o, eid), eoi, invert);
      }
      buffer.write((byte) 0, invert);
      return;
    }
    case MAP: {
      MapObjectInspector moi = (MapObjectInspector) oi;
      ObjectInspector koi = moi.getMapKeyObjectInspector();
      ObjectInspector voi = moi.getMapValueObjectInspector();

      Map<?, ?> map = moi.getMap(o);
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        buffer.write((byte) 1, invert);
        serialize(buffer, entry.getKey(), koi, invert);
        serialize(buffer, entry.getValue(), voi, invert);
      }
      buffer.write((byte) 0, invert);
      return;
    }
    case STRUCT: {
      StructObjectInspector soi = (StructObjectInspector) oi;
      List<? extends StructField> fields = soi.getAllStructFieldRefs();

      for (int i = 0; i < fields.size(); i++) {
        serialize(buffer, soi.getStructFieldData(o, fields.get(i)),
            fields.get(i).getFieldObjectInspector(), invert);
      }
      return;
    }

    case UNION: {
      UnionObjectInspector uoi = (UnionObjectInspector) oi;
      byte tag = uoi.getTag(o);
      buffer.write(tag, invert);
      serialize(buffer, uoi.getField(o), uoi.getObjectInspectors().get(tag),
          invert);
      return;
    }

    default: {
      throw new RuntimeException("Unrecognized type: " + oi.getCategory());
    }
    }

  }
}
