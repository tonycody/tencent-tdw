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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.util.ReflectionUtils;

public class ReflectionStructObjectInspector extends
    SettableStructObjectInspector {

  public static class MyField implements StructField {
    protected Field field;
    protected ObjectInspector fieldObjectInspector;

    public MyField(Field field, ObjectInspector fieldObjectInspector) {
      this.field = field;
      this.fieldObjectInspector = fieldObjectInspector;
    }

    public String getFieldName() {
      return field.getName().toLowerCase();
    }

    public ObjectInspector getFieldObjectInspector() {
      return fieldObjectInspector;
    }

    public String toString() {
      return field.toString();
    }
  }

  Class<?> objectClass;
  List<MyField> fields;

  public Category getCategory() {
    return Category.STRUCT;
  }

  public String getTypeName() {
    return objectClass.getName();
  }

  ReflectionStructObjectInspector() {
  }

  void init(Class<?> objectClass,
      List<ObjectInspector> structFieldObjectInspectors) {
    assert (!List.class.isAssignableFrom(objectClass));
    assert (!Map.class.isAssignableFrom(objectClass));

    this.objectClass = objectClass;
    Field[] reflectionFields = ObjectInspectorUtils
        .getDeclaredNonStaticFields(objectClass);
    fields = new ArrayList<MyField>(structFieldObjectInspectors.size());
    int used = 0;
    for (int i = 0; i < reflectionFields.length; i++) {
      if (!shouldIgnoreField(reflectionFields[i].getName())) {
        reflectionFields[i].setAccessible(true);
        fields.add(new MyField(reflectionFields[i], structFieldObjectInspectors
            .get(used++)));
      }
    }
    assert (fields.size() == structFieldObjectInspectors.size());
  }

  public boolean shouldIgnoreField(String name) {
    return false;
  }

  public StructField getStructFieldRef(String fieldName) {
    return ObjectInspectorUtils.getStandardStructFieldRef(fieldName, fields);
  }

  public List<? extends StructField> getAllStructFieldRefs() {
    return fields;
  }

  public Object getStructFieldData(Object data, StructField fieldRef) {
    if (data == null) {
      return null;
    }
    if (!(fieldRef instanceof MyField)) {
      throw new RuntimeException("fieldRef has to be of MyField");
    }
    MyField f = (MyField) fieldRef;
    try {
      Object r = f.field.get(data);
      return r;
    } catch (Exception e) {
      throw new RuntimeException("cannot get field " + f.field + " from "
          + data.getClass() + " " + data, e);
    }
  }

  public List<Object> getStructFieldsDataAsList(Object data) {
    if (data == null) {
      return null;
    }
    try {
      ArrayList<Object> result = new ArrayList<Object>(fields.size());
      for (int i = 0; i < fields.size(); i++) {
        result.add(fields.get(i).field.get(data));
      }
      return result;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Object create() {
    return ReflectionUtils.newInstance(objectClass, null);
  }

  public Object setStructFieldData(Object struct, StructField field,
      Object fieldValue) {
    MyField myField = (MyField) field;
    try {
      myField.field.set(struct, fieldValue);
    } catch (Exception e) {
      throw new RuntimeException("cannot set field " + myField.field + " of "
          + struct.getClass() + " " + struct, e);
    }
    return struct;
  }
}
