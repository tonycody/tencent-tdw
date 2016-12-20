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

import java.util.List;

public class StandardUnionObjectInspector implements UnionObjectInspector {
  List<ObjectInspector> ois;

  public StandardUnionObjectInspector(List<ObjectInspector> ois) {
    this.ois = ois;
  }

  public List<ObjectInspector> getObjectInspectors() {
    return ois;
  }

  public static class StandardUnion implements UnionObject {
    protected byte tag;
    protected Object object;

    public StandardUnion() {
    }

    public StandardUnion(byte tag, Object object) {
      this.tag = tag;
      this.object = object;
    }

    public void setObject(Object o) {
      this.object = o;
    }

    public void setTag(byte tag) {
      this.tag = tag;
    }

    @Override
    public Object getObject() {
      return object;
    }

    @Override
    public byte getTag() {
      return tag;
    }

    @Override
    public String toString() {
      return tag + ":" + object;
    }

    @Override
    public int hashCode() {
      return tag * 31 + (object == null ? 0 : this.object.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof StandardUnion) {
        return this.toString().equals(obj.toString());
      }
      return super.equals(obj);
    }
  }

  public byte getTag(Object o) {
    if (o == null) {
      return -1;
    }
    return ((UnionObject) o).getTag();
  }

  public Object getField(Object o) {
    if (o == null) {
      return null;
    }
    return ((UnionObject) o).getObject();
  }

  public Category getCategory() {
    return Category.UNION;
  }

  public String getTypeName() {
    return ObjectInspectorUtils.getStandardUnionTypeName(this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getClass().getName());
    sb.append(getTypeName());
    return sb.toString();
  }

}
