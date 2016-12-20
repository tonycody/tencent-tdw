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
package org.apache.hadoop.hive.serde2.protobuf.objectinspector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.GeneratedMessage;

public class ProtobufStructObjectInspector extends StructObjectInspector {
  public static final Log LOG = LogFactory
      .getLog(ProtobufStructObjectInspector.class.getName());

  protected static class ProtobufField implements StructField {
    private String name;
    private ObjectInspector oi;

    protected ProtobufField(String name, ObjectInspector oi) {
      this.name = name;
      this.oi = oi;
    }

    @Override
    public String getFieldName() {
      return name;
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return oi;
    }

    @Override
    public String toString() {
      return (name + ": " + oi.toString());
    }
  }

  private List<ProtobufField> fields;
  private Descriptor descriptor = null;

  private Map<String, FieldDescriptor> fieldDescriptorMap;
  private List<FieldDescriptor> fieldDescriptorList;

  public ProtobufStructObjectInspector(List<String> structFieldNames,
      List<ObjectInspector> structFieldObjectInspectors, Descriptor descriptor) {
    fields = new ArrayList<ProtobufField>(structFieldNames.size());
    for (int i = 0; i < structFieldNames.size(); ++i) {
      fields.add(new ProtobufField(structFieldNames.get(i),
          structFieldObjectInspectors.get(i)));
    }
    this.descriptor = descriptor;
    this.fieldDescriptorList = descriptor.getFields();
    this.fieldDescriptorMap = new HashMap<String, FieldDescriptor>();
    for (FieldDescriptor fd : this.fieldDescriptorList) {
      this.fieldDescriptorMap.put(fd.getName(), fd);
    }
  }

  public Descriptor getDescriptor() {
    return descriptor;
  }

  @Override
  public List<? extends StructField> getAllStructFieldRefs() {
    return fields;
  }

  @Override
  public Object getStructFieldData(Object data, StructField fieldRef) {
    if (data == null) {
      return null;
    }

    GeneratedMessage message = (GeneratedMessage) data;
    FieldDescriptor fd = fieldDescriptorMap.get(fieldRef.getFieldName());
    return getField(message, fd);
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object data) {
    if (data == null) {
      return null;
    }
    GeneratedMessage message = (GeneratedMessage) data;
    ArrayList<Object> fieldDataList = new ArrayList<Object>(fields.size());
    for (int i = 0; i < fieldDescriptorList.size(); ++i) {
      fieldDataList.add(getField(message, fieldDescriptorList.get(i)));
    }
    return fieldDataList;
  }

  private Object getField(GeneratedMessage message, FieldDescriptor fd) {

    if (fd.isOptional()) {
      if (!message.hasField(fd) && !fd.hasDefaultValue()) {
        return null;
      }
    }
    return message.getField(fd);
  }

  @Override
  public StructField getStructFieldRef(String fieldName) {
    return ObjectInspectorUtils.getStandardStructFieldRef(fieldName, fields);
  }

  @Override
  public String toString() {
    String rt = null;
    for (int i = 0; i < fields.size(); ++i) {
      rt += fields.get(i).toString();
    }
    return rt;
  }

  @Override
  public Category getCategory() {
    return Category.STRUCT;
  }

  @Override
  public String getTypeName() {
    return ObjectInspectorUtils.getStandardStructTypeName(this);
  }
}
