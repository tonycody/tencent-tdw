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
package org.apache.hadoop.hive.serde2.objectinspector;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;

public class StandardByteArrayObjectInspector implements ByteArrayObjectInspector
{
  public StandardByteArrayObjectInspector() {
    
  }

  public final Category getCategory() {
    return Category.BYTEARRAY;
  }
  
  public byte getListElement(Object data, int index) {
    byte[] list = (byte[]) data;
    return list[index];
  }
  
  public byte[] getBytesForStrBuff(Object data) {
    if(data == null){
      return null;
    }
    byte[] list = (byte[]) data;
    for(int i = 0 ; i < list.length ; i++){
      list[i] = (byte) (list[i] ^ 0x80);
    }
    return list;
  }
  
  public int getListLength(Object data) {
    if(data == null){
      return 0;
    }
    byte[] list = (byte[]) data;
    return list.length;
  }

  public String getTypeName() {
    return org.apache.hadoop.hive.serde.Constants.BYTEARRAY_NAME;
  }
  
  public Object set(Object array, int index, Object element) {
    return null;
  }

  @Override
  public byte[] getBytes(Object data) {
    // TODO Auto-generated method stub   
    return (byte[]) data;
  }

  @Override
  public byte[] getBytesCompressed(Object data) {
    // TODO Auto-generated method stub
    return null;
  }
}
