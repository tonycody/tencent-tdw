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

package org.apache.hadoop.hive.serde2.lazybinary;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hive.serde2.ByteStream.Output;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;

public class LazyBinarySerDe implements SerDe {

  public static final Log LOG = LogFactory.getLog(LazyBinarySerDe.class
      .getName());

  public LazyBinarySerDe() throws SerDeException {
  }

  List<String> columnNames;
  List<TypeInfo> columnTypes;

  TypeInfo rowTypeInfo;
  ObjectInspector cachedObjectInspector;

  LazyBinaryStruct cachedLazyBinaryStruct;

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
    cachedObjectInspector = LazyBinaryUtils
        .getLazyBinaryObjectInspectorFromTypeInfo(rowTypeInfo);
    cachedLazyBinaryStruct = (LazyBinaryStruct) LazyBinaryFactory
        .createLazyBinaryObject(cachedObjectInspector);
    LOG.debug("LazyBinarySerDe initialized with: columnNames=" + columnNames
        + " columnTypes=" + columnTypes);
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return cachedObjectInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return BytesWritable.class;
  }

  ByteArrayRef byteArrayRef;

  @Override
  public Object deserialize(Writable field) throws SerDeException {
    if (byteArrayRef == null) {
      byteArrayRef = new ByteArrayRef();
    }
    if (field instanceof BytesWritable) {
      BytesWritable b = (BytesWritable) field;
      if (b.getSize() == 0)
        return null;
      byteArrayRef.setData(b.get());
      cachedLazyBinaryStruct.init(byteArrayRef, 0, b.getSize());
    } else if (field instanceof Text) {
      Text t = (Text) field;
      if (t.getLength() == 0)
        return null;
      byteArrayRef.setData(t.getBytes());
      cachedLazyBinaryStruct.init(byteArrayRef, 0, t.getLength());
    } else {
      throw new SerDeException(getClass().toString()
          + ": expects either BytesWritable or Text object!");
    }
    return cachedLazyBinaryStruct;
  }

  BytesWritable serializeBytesWritable = new BytesWritable();
  ByteStream.Output serializeByteStream = new ByteStream.Output();

  @Override
  public Writable serialize(Object obj, ObjectInspector objInspector)
      throws SerDeException {
    if (objInspector.getCategory() != Category.STRUCT) {
      throw new SerDeException(getClass().toString()
          + " can only serialize struct types, but we got: "
          + objInspector.getTypeName());
    }

    serializeByteStream.reset();
    serializeStruct(serializeByteStream, obj,
        (StructObjectInspector) objInspector);
    serializeBytesWritable.set(serializeByteStream.getData(), 0,
        serializeByteStream.getCount());
    return serializeBytesWritable;
  }

  boolean nullMapKey = false;

  private void serializeStruct(Output byteStream, Object obj,
      StructObjectInspector soi) {
    if (null == obj)
      return;

    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    int size = fields.size();
    int lasti = 0;
    byte nullByte = 0;
    for (int i = 0; i < size; i++) {
      if (null != soi.getStructFieldData(obj, fields.get(i))) {
        nullByte |= 1 << (i % 8);
      }
      if (7 == i % 8 || i == size - 1) {
        serializeByteStream.write(nullByte);
        for (int j = lasti; j <= i; j++) {
          serialize(serializeByteStream, soi.getStructFieldData(obj,
              fields.get(j)), fields.get(j).getFieldObjectInspector());
        }
        lasti = i + 1;
        nullByte = 0;
      }
    }
  }

  private void serialize(Output byteStream, Object obj,
      ObjectInspector objInspector) {

    if (null == obj)
      return;

    switch (objInspector.getCategory()) {
    case PRIMITIVE: {
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) objInspector;
      switch (poi.getPrimitiveCategory()) {
      case VOID: {
        return;
      }
      case BOOLEAN: {
        BooleanObjectInspector boi = (BooleanObjectInspector) poi;
        boolean v = ((BooleanObjectInspector) poi).get(obj);
        byteStream.write((byte) (v ? 1 : 0));
        return;
      }
      case BYTE: {
        ByteObjectInspector boi = (ByteObjectInspector) poi;
        byte v = boi.get(obj);
        byteStream.write(v);
        return;
      }
      case SHORT: {
        ShortObjectInspector spoi = (ShortObjectInspector) poi;
        short v = spoi.get(obj);
        byteStream.write((byte) (v >> 8));
        byteStream.write((byte) (v));
        return;
      }
      case INT: {
        IntObjectInspector ioi = (IntObjectInspector) poi;
        int v = ioi.get(obj);
        LazyBinaryUtils.writeVInt(byteStream, v);
        return;
      }
      case LONG: {
        LongObjectInspector loi = (LongObjectInspector) poi;
        long v = loi.get(obj);
        LazyBinaryUtils.writeVLong(byteStream, v);
        return;
      }
      case FLOAT: {
        FloatObjectInspector foi = (FloatObjectInspector) poi;
        int v = Float.floatToIntBits(foi.get(obj));
        byteStream.write((byte) (v >> 24));
        byteStream.write((byte) (v >> 16));
        byteStream.write((byte) (v >> 8));
        byteStream.write((byte) (v));
        return;
      }
      case DOUBLE: {
        DoubleObjectInspector doi = (DoubleObjectInspector) poi;
        long v = Double.doubleToLongBits(doi.get(obj));
        byteStream.write((byte) (v >> 56));
        byteStream.write((byte) (v >> 48));
        byteStream.write((byte) (v >> 40));
        byteStream.write((byte) (v >> 32));
        byteStream.write((byte) (v >> 24));
        byteStream.write((byte) (v >> 16));
        byteStream.write((byte) (v >> 8));
        byteStream.write((byte) (v));
        return;
      }
      case STRING: {
        StringObjectInspector soi = (StringObjectInspector) poi;
        Text t = soi.getPrimitiveWritableObject(obj);
        /* write byte size of the string which is a vint */
        int length = t.getLength();
        LazyBinaryUtils.writeVInt(byteStream, length);
        /* write string itself */
        byte[] data = t.getBytes();
        byteStream.write(data, 0, length);
        return;
      }
      case TIMESTAMP: {
        TimestampObjectInspector toi = (TimestampObjectInspector) poi;
        TimestampWritable t = toi.getPrimitiveWritableObject(obj);
        t.writeToByteStream(byteStream);
        return;
      }
      default: {
        throw new RuntimeException("Unrecognized type: "
            + poi.getPrimitiveCategory());
      }
      }
    }
    case LIST: {
      ListObjectInspector loi = (ListObjectInspector) objInspector;
      ObjectInspector eoi = loi.getListElementObjectInspector();

      int byteSizeStart = byteStream.getCount();
      byteStream.write((byte) 0);
      byteStream.write((byte) 0);
      byteStream.write((byte) 0);
      byteStream.write((byte) 0);
      int listStart = byteStream.getCount();

      int size = loi.getListLength(obj);
      LazyBinaryUtils.writeVInt(byteStream, size);

      byte nullByte = 0;
      for (int eid = 0; eid < size; eid++) {
        if (null != loi.getListElement(obj, eid)) {
          nullByte |= 1 << (eid % 8);
        }
        if (7 == eid % 8 || eid == size - 1) {
          byteStream.write(nullByte);
          nullByte = 0;
        }
      }

      for (int eid = 0; eid < size; eid++) {
        serialize(byteStream, loi.getListElement(obj, eid), eoi);
      }

      int listEnd = byteStream.getCount();
      int listSize = listEnd - listStart;
      byte[] bytes = byteStream.getData();
      bytes[byteSizeStart] = (byte) (listSize >> 24);
      bytes[byteSizeStart + 1] = (byte) (listSize >> 16);
      bytes[byteSizeStart + 2] = (byte) (listSize >> 8);
      bytes[byteSizeStart + 3] = (byte) (listSize);

      return;
    }
    case MAP: {
      MapObjectInspector moi = (MapObjectInspector) objInspector;
      ObjectInspector koi = moi.getMapKeyObjectInspector();
      ObjectInspector voi = moi.getMapValueObjectInspector();
      Map<?, ?> map = moi.getMap(obj);

      int byteSizeStart = byteStream.getCount();
      byteStream.write((byte) 0);
      byteStream.write((byte) 0);
      byteStream.write((byte) 0);
      byteStream.write((byte) 0);
      int mapStart = byteStream.getCount();

      int size = map.size();
      LazyBinaryUtils.writeVInt(byteStream, size);

      int b = 0;
      byte nullByte = 0;
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        if (null != entry.getKey()) {
          nullByte |= 1 << (b % 8);
        } else if (!nullMapKey) {
          nullMapKey = true;
          LOG.warn("Null map key encountered! Ignoring similar problems.");
        }
        b++;
        if (null != entry.getValue()) {
          nullByte |= 1 << (b % 8);
        }
        b++;
        if (0 == b % 8 || b == size * 2) {
          byteStream.write(nullByte);
          nullByte = 0;
        }
      }

      for (Map.Entry<?, ?> entry : map.entrySet()) {
        serialize(byteStream, entry.getKey(), koi);
        serialize(byteStream, entry.getValue(), voi);
      }

      int mapEnd = byteStream.getCount();
      int mapSize = mapEnd - mapStart;
      byte[] bytes = byteStream.getData();
      bytes[byteSizeStart] = (byte) (mapSize >> 24);
      bytes[byteSizeStart + 1] = (byte) (mapSize >> 16);
      bytes[byteSizeStart + 2] = (byte) (mapSize >> 8);
      bytes[byteSizeStart + 3] = (byte) (mapSize);

      return;
    }
    case STRUCT: {
      int byteSizeStart = byteStream.getCount();
      byteStream.write((byte) 0);
      byteStream.write((byte) 0);
      byteStream.write((byte) 0);
      byteStream.write((byte) 0);
      int structStart = byteStream.getCount();

      serializeStruct(byteStream, obj, (StructObjectInspector) objInspector);

      int structEnd = byteStream.getCount();
      int structSize = structEnd - structStart;
      byte[] bytes = byteStream.getData();
      bytes[byteSizeStart] = (byte) (structSize >> 24);
      bytes[byteSizeStart + 1] = (byte) (structSize >> 16);
      bytes[byteSizeStart + 2] = (byte) (structSize >> 8);
      bytes[byteSizeStart + 3] = (byte) (structSize);

      return;
    }
    default: {
      throw new RuntimeException("Unrecognized type: "
          + objInspector.getCategory());
    }
    }
  }
}
