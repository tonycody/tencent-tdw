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

package org.apache.hadoop.hive.ql.exec;

import java.lang.Class;
import java.io.*;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class ColumnInfo implements Serializable {

  private static final long serialVersionUID = 1L;

  private String internalName;

  /* add by roachxiang for ctas begin */
  private String alias = null;

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  /* add by roachxiang for ctas end */

  private String tabAlias;

  private boolean isPartitionCol;

  transient private TypeInfo type;

  public ColumnInfo() {
  }

  public ColumnInfo(String internalName, TypeInfo type, String tabAlias,
      boolean isPartitionCol) {
    this.internalName = internalName;
    this.type = type;
    this.tabAlias = tabAlias;
    this.isPartitionCol = isPartitionCol;
  }

  public ColumnInfo(String internalName, TypeInfo type, String tabAlias,
      boolean isPartitionCol, String alia) {
    this.internalName = internalName;
    this.type = type;
    this.tabAlias = tabAlias;
    this.isPartitionCol = isPartitionCol;
    this.alias = alia;
  }

  public ColumnInfo(String internalName, Class type, String tabAlias,
      boolean isPartitionCol) {
    this.internalName = internalName;
    this.type = TypeInfoFactory.getPrimitiveTypeInfoFromPrimitiveWritable(type);
    this.tabAlias = tabAlias;
    this.isPartitionCol = isPartitionCol;
  }

  public TypeInfo getType() {
    return type;
  }

  public String getInternalName() {
    return internalName;
  }

  public void setType(TypeInfo type) {
    this.type = type;
  }

  public void setInternalName(String internalName) {
    this.internalName = internalName;
  }

  public String getTabAlias() {
    return this.tabAlias;
  }

  public boolean getIsPartitionCol() {
    return this.isPartitionCol;
  }

  public String toString() {
    return internalName + ": " + type;
  }
}
