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
package org.apache.hadoop.hive.serde2.typeinfo;

import java.io.Serializable;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;

public class ByteArrayTypeInfo extends TypeInfo implements Serializable
{
  private static final long serialVersionUID = 1L;

  @Override
  public Category getCategory() {
    // TODO Auto-generated method stub
    return Category.BYTEARRAY;
  }

  @Override
  public String getTypeName() {
    // TODO Auto-generated method stub
    return org.apache.hadoop.hive.serde.Constants.BYTEARRAY_NAME;
  }

  public boolean equals(Object other) {
    return this == other;
  }
  
  /**
   * Generate the hashCode for this TypeInfo.
   */
  public int hashCode() {
    return "array<byte>".hashCode();
  }
  
}
