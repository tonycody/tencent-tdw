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

package org.apache.hadoop.hive.serde2.typeinfo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;

public class StructTypeInfo extends TypeInfo implements Serializable {

  private static final long serialVersionUID = 1L;

  ArrayList<String> allStructFieldNames;
  ArrayList<TypeInfo> allStructFieldTypeInfos;

  public StructTypeInfo() {
  }

  public String getTypeName() {
    StringBuilder sb = new StringBuilder();
    sb.append(Constants.STRUCT_TYPE_NAME + "<");
    for (int i = 0; i < allStructFieldNames.size(); i++) {
      if (i > 0)
        sb.append(",");
      sb.append(allStructFieldNames.get(i));
      sb.append(":");
      sb.append(allStructFieldTypeInfos.get(i).getTypeName());
    }
    sb.append(">");
    return sb.toString();
  }

  public void setAllStructFieldNames(ArrayList<String> allStructFieldNames) {
    this.allStructFieldNames = allStructFieldNames;
  }

  public void setAllStructFieldTypeInfos(
      ArrayList<TypeInfo> allStructFieldTypeInfos) {
    this.allStructFieldTypeInfos = allStructFieldTypeInfos;
  }

  StructTypeInfo(List<String> names, List<TypeInfo> typeInfos) {
    allStructFieldNames = new ArrayList<String>();
    allStructFieldNames.addAll(names);
    allStructFieldTypeInfos = new ArrayList<TypeInfo>();
    allStructFieldTypeInfos.addAll(typeInfos);
  }

  public Category getCategory() {
    return Category.STRUCT;
  }

  public ArrayList<String> getAllStructFieldNames() {
    return allStructFieldNames;
  }

  public ArrayList<TypeInfo> getAllStructFieldTypeInfos() {
    return allStructFieldTypeInfos;
  }

  public TypeInfo getStructFieldTypeInfo(String field) {
    String fieldLowerCase = field.toLowerCase();
    for (int i = 0; i < allStructFieldNames.size(); i++) {
      if (fieldLowerCase.equals(allStructFieldNames.get(i))) {
        return allStructFieldTypeInfos.get(i);
      }
    }
    throw new RuntimeException("cannot find field " + field
        + "(lowercase form: " + fieldLowerCase + ") in " + allStructFieldNames);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other)
      return true;
    if (!(other instanceof StructTypeInfo)) {
      return false;
    }
    StructTypeInfo o = (StructTypeInfo) other;
    return o.getCategory().equals(getCategory())
        && o.getAllStructFieldNames().equals(getAllStructFieldNames())
        && o.getAllStructFieldTypeInfos().equals(getAllStructFieldTypeInfos());
  }

  public int hashCode() {
    return allStructFieldNames.hashCode() ^ allStructFieldTypeInfos.hashCode();
  }

}
