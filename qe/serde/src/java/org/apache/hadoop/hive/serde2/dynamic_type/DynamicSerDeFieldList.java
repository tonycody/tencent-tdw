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

package org.apache.hadoop.hive.serde2.dynamic_type;

import org.apache.thrift.TException;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.protocol.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import java.util.*;
import java.io.*;
import org.apache.hadoop.hive.serde2.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.lang.reflect.*;
import org.apache.thrift.protocol.TType.*;

public class DynamicSerDeFieldList extends DynamicSerDeSimpleNode implements
    Serializable {

  private Map<Integer, DynamicSerDeTypeBase> types_by_id = null;
  private Map<String, DynamicSerDeTypeBase> types_by_column_name = null;
  private DynamicSerDeTypeBase ordered_types[] = null;

  private Map<String, Integer> ordered_column_id_by_name = null;

  public DynamicSerDeFieldList(int i) {
    super(i);
  }

  public DynamicSerDeFieldList(thrift_grammar p, int i) {
    super(p, i);
  }

  private DynamicSerDeField getField(int i) {
    return (DynamicSerDeField) this.jjtGetChild(i);
  }

  final public DynamicSerDeField[] getChildren() {
    int size = this.jjtGetNumChildren();
    DynamicSerDeField result[] = new DynamicSerDeField[size];
    for (int i = 0; i < size; i++) {
      result[i] = (DynamicSerDeField) this.jjtGetChild(i);
    }
    return result;
  }

  private int getNumFields() {
    return this.jjtGetNumChildren();
  }

  public void initialize() {
    if (types_by_id == null) {
      types_by_id = new HashMap<Integer, DynamicSerDeTypeBase>();
      types_by_column_name = new HashMap<String, DynamicSerDeTypeBase>();
      ordered_types = new DynamicSerDeTypeBase[this.jjtGetNumChildren()];
      ordered_column_id_by_name = new HashMap<String, Integer>();

      for (int i = 0; i < this.jjtGetNumChildren(); i++) {
        DynamicSerDeField mt = this.getField(i);
        DynamicSerDeTypeBase type = mt.getFieldType().getMyType();
        type.initialize();
        type.fieldid = mt.fieldid;
        type.name = mt.name;

        types_by_id.put(Integer.valueOf(mt.fieldid), type);
        types_by_column_name.put(mt.name, type);
        ordered_types[i] = type;
        ordered_column_id_by_name.put(mt.name, i);
      }
    }
  }

  private DynamicSerDeTypeBase getFieldByFieldId(int i) {
    return types_by_id.get(i);
  }

  protected DynamicSerDeTypeBase getFieldByName(String fieldname) {
    return types_by_column_name.get(fieldname);
  }

  protected boolean isRealThrift = false;

  protected boolean[] fieldsPresent;

  public Object deserialize(Object reuse, TProtocol iprot)
      throws SerDeException, TException, IllegalAccessException {
    ArrayList<Object> struct = null;

    if (reuse == null) {
      struct = new ArrayList<Object>(this.getNumFields());
      for (int i = 0; i < ordered_types.length; i++) {
        struct.add(null);
      }
    } else {
      struct = (ArrayList<Object>) reuse;
      assert (struct.size() == ordered_types.length);
    }

    boolean fastSkips = iprot instanceof org.apache.hadoop.hive.serde2.thrift.SkippableTProtocol;

    boolean stopSeen = false;

    if (fieldsPresent == null) {
      fieldsPresent = new boolean[ordered_types.length];
    }
    Arrays.fill(fieldsPresent, false);

    for (int i = 0; i < this.getNumFields(); i++) {
      DynamicSerDeTypeBase mt = null;
      TField field = null;

      if (!isRealThrift && this.getField(i).isSkippable()) {
        mt = this.ordered_types[i];
        if (fastSkips) {
          ((org.apache.hadoop.hive.serde2.thrift.SkippableTProtocol) iprot)
              .skip(mt.getType());
        } else {
          TProtocolUtil.skip(iprot, mt.getType());
        }
        struct.set(i, null);
        continue;
      }
      if (thrift_mode) {
        field = iprot.readFieldBegin();

        if (field.type >= 0) {
          if (field.type == TType.STOP) {
            stopSeen = true;
            break;
          }
          mt = this.getFieldByFieldId(field.id);
          if (mt == null) {
            System.err.println("ERROR for fieldid: " + field.id
                + " system has no knowledge of this field which is of type : "
                + field.type);
            TProtocolUtil.skip(iprot, field.type);
            continue;
          }
        }
      }

      int orderedId = -1;
      if (!thrift_mode || field.type < 0) {
        mt = this.ordered_types[i];
        orderedId = i;
      } else {
        orderedId = ordered_column_id_by_name.get(mt.name);
      }
      struct.set(orderedId, mt.deserialize(struct.get(orderedId), iprot));
      if (thrift_mode) {
        iprot.readFieldEnd();
      }
      fieldsPresent[orderedId] = true;
    }

    for (int i = 0; i < ordered_types.length; i++) {
      if (!fieldsPresent[i]) {
        struct.set(i, null);
      }
    }

    if (thrift_mode && !stopSeen) {
      iprot.readFieldBegin();
    }
    return struct;
  }

  TField field = new TField();

  public void serialize(Object o, ObjectInspector oi, TProtocol oprot)
      throws TException, SerDeException, NoSuchFieldException,
      IllegalAccessException {
    assert (oi instanceof StructObjectInspector);
    StructObjectInspector soi = (StructObjectInspector) oi;

    boolean writeNulls = oprot instanceof org.apache.hadoop.hive.serde2.thrift.WriteNullsProtocol;

    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    if (fields.size() != ordered_types.length) {
      throw new SerDeException("Trying to serialize " + fields.size()
          + " fields into a struct with " + ordered_types.length + " object="
          + o + " objectinspector=" + oi.getTypeName());
    }
    for (int i = 0; i < fields.size(); i++) {
      Object f = soi.getStructFieldData(o, fields.get(i));
      DynamicSerDeTypeBase mt = ordered_types[i];

      if (f == null && !writeNulls) {
        continue;
      }

      if (thrift_mode) {
        field = new TField(mt.name, mt.getType(), (short) mt.fieldid);
        oprot.writeFieldBegin(field);
      }

      if (f == null) {
        ((org.apache.hadoop.hive.serde2.thrift.WriteNullsProtocol) oprot)
            .writeNull();
      } else {
        mt.serialize(f, fields.get(i).getFieldObjectInspector(), oprot);
      }
      if (thrift_mode) {
        oprot.writeFieldEnd();
      }
    }
    if (thrift_mode) {
      oprot.writeFieldStop();
    }
  }

  public String toString() {
    StringBuffer result = new StringBuffer();
    String prefix = "";
    for (DynamicSerDeField t : this.getChildren()) {
      result.append(prefix + t.fieldid + ":"
          + t.getFieldType().getMyType().toString() + " " + t.name);
      prefix = ",";
    }
    return result.toString();
  }
}
