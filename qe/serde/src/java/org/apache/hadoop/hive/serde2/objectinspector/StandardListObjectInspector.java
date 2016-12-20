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

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class StandardListObjectInspector implements SettableListObjectInspector {

  ObjectInspector listElementObjectInspector;

  protected StandardListObjectInspector(
      ObjectInspector listElementObjectInspector) {
    this.listElementObjectInspector = listElementObjectInspector;
  }

  public final Category getCategory() {
    return Category.LIST;
  }

  public ObjectInspector getListElementObjectInspector() {
    return listElementObjectInspector;
  }

  public Object getListElement(Object data, int index) {
    if (data == null) {
      return null;
    }
    boolean isArray = data.getClass().isArray();
    if (isArray) {
      Object[] list = (Object[]) data;
      if (index < 0 || index >= list.length) {
        return null;
      }
      return list[index];
    } else {
      List<?> list = (List<?>) data;
      if (index < 0 || index >= list.size()) {
        return null;
      }
      return list.get(index);
    }
  }

  public int getListLength(Object data) {
    if (data == null) {
      return -1;
    }
    boolean isArray = data.getClass().isArray();
    if (isArray) {
      Object[] list = (Object[]) data;
      return list.length;
    } else {
      List<?> list = (List<?>) data;
      return list.size();
    }
  }

  public List<?> getList(Object data) {
    if (data == null)
      return null;
    if (data.getClass().isArray()) {
      data = java.util.Arrays.asList((Object[]) data);
    }
    List<?> list = (List<?>) data;
    return list;
  }

  public String getTypeName() {
    return org.apache.hadoop.hive.serde.Constants.LIST_TYPE_NAME + "<"
        + listElementObjectInspector.getTypeName() + ">";
  }

  public Object create(int size) {
    List<Object> a = new ArrayList<Object>(size);
    for (int i = 0; i < size; i++) {
      a.add(null);
    }
    return a;
  }

  public Object resize(Object list, int newSize) {
    List<Object> a = (List<Object>) list;
    while (a.size() < newSize) {
      a.add(null);
    }
    while (a.size() > newSize) {
      a.remove(a.size() - 1);
    }
    return a;
  }

  public Object set(Object list, int index, Object element) {
    List<Object> a = (List<Object>) list;
    a.set(index, element);
    return a;
  }

}
