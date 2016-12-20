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
package org.apache.hadoop.hive.serde2.lazy.objectinspector;

import java.util.List;

import org.apache.hadoop.hive.serde2.lazy.LazyByteArray;
import org.apache.hadoop.hive.serde2.objectinspector.ByteArrayObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;

public class LazyByteArrayObjectInspector implements ByteArrayObjectInspector
{
  public LazyByteArrayObjectInspector(){
    
  }

  public final Category getCategory() {
    return Category.BYTEARRAY;
  }
  
  // with data
  public byte getListElement(Object data, int index) {
    LazyByteArray lba = (LazyByteArray) data;
    return lba.getListElementObject(index);
  }
  
  public int getListLength(Object data){
    if(data == null){
      return 0;
    }
    LazyByteArray lba = (LazyByteArray) data;
    return lba.getListLength();
  }

  public String getTypeName(){
    return org.apache.hadoop.hive.serde.Constants.BYTEARRAY_NAME;
  }
  
  public Object set(Object list, int index, Object element) {
    //List<Object> a = (List<Object>)list;
    //a.set(index, element);
    //return a;
    return null;
  }

  //@Override
  public byte[] getBytesForWrite(Object data){
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[] getBytes(Object data) {
    // TODO Auto-generated method stub
    if(data == null){
      return null; 
    }
    LazyByteArray lba = (LazyByteArray) data;
    byte [] byteArray = lba.getData();

    for(int i = 0; i < byteArray.length; i++){
      byteArray[i] = (byte) (byteArray[i] & 0x7f);
    }
    return byteArray;
  }

  @Override
  public byte[] getBytesCompressed(Object data) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public byte[] getBytesForStrBuff(Object data) {
    // TODO Auto-generated method stub
    if(data == null){
      return null;
    }
    
    LazyByteArray lba = (LazyByteArray) data;
    byte[] list = (byte[]) lba.getData();
    for(int i = 0 ; i < list.length ; i++){
      list[i] = (byte) (list[i] ^ 0x80);
    }
    return list;
  }
}
