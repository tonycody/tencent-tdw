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

import java.util.HashMap;
import java.util.Map;

public class StandardMapObjectInspector implements SettableMapObjectInspector {

  ObjectInspector mapKeyObjectInspector;
  ObjectInspector mapValueObjectInspector;

  protected StandardMapObjectInspector(ObjectInspector mapKeyObjectInspector,
      ObjectInspector mapValueObjectInspector) {
    this.mapKeyObjectInspector = mapKeyObjectInspector;
    this.mapValueObjectInspector = mapValueObjectInspector;
  }

  public ObjectInspector getMapKeyObjectInspector() {
    return mapKeyObjectInspector;
  }

  public ObjectInspector getMapValueObjectInspector() {
    return mapValueObjectInspector;
  }

  public Object getMapValueElement(Object data, Object key) {
    if (data == null || key == null)
      return null;
    Map<?, ?> map = (Map<?, ?>) data;
    return map.get(key);
  }

  public int getMapSize(Object data) {
    if (data == null)
      return -1;
    Map<?, ?> map = (Map<?, ?>) data;
    return map.size();
  }

  public Map<?, ?> getMap(Object data) {
    if (data == null)
      return null;
    Map<?, ?> map = (Map<?, ?>) data;
    return map;
  }

  public final Category getCategory() {
    return Category.MAP;
  }

  public String getTypeName() {
    return org.apache.hadoop.hive.serde.Constants.MAP_TYPE_NAME + "<"
        + mapKeyObjectInspector.getTypeName() + ","
        + mapValueObjectInspector.getTypeName() + ">";
  }

  public Object create() {
    Map<Object, Object> m = new HashMap<Object, Object>();
    return m;
  }

  public Object clear(Object map) {
    Map<Object, Object> m = (HashMap<Object, Object>) map;
    m.clear();
    return m;
  }

  public Object put(Object map, Object key, Object value) {
    Map<Object, Object> m = (HashMap<Object, Object>) map;
    m.put(key, value);
    return m;
  }

  public Object remove(Object map, Object key) {
    Map<Object, Object> m = (HashMap<Object, Object>) map;
    m.remove(key);
    return m;
  }
}
