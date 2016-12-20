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
package org.apache.hadoop.hive.serde2.lazy;

import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyByteArrayObjectInspector;

public class LazyByteArray extends LazyObject<LazyByteArrayObjectInspector>
{

  protected ByteArrayRef bytes;
  protected int start;
  protected int length;
  
  protected LazyByteArray(LazyByteArrayObjectInspector oi) {
    super(oi);
    // TODO Auto-generated constructor stub
  }
  
  public int getListLength(){
    if (bytes == null) {
      throw new RuntimeException("bytes cannot be null!");
    }
    return length;
  }
  
  public byte getListElementObject(int index) {
    return (byte) (bytes.data[start + index] & 0x7f);
  }
  

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    // TODO Auto-generated method stub
    if (bytes == null) {
      throw new RuntimeException("bytes cannot be null!");
    }
    this.bytes = bytes;
    this.start = start;
    this.length = length;
    assert start >= 0;
    assert start + length <= bytes.getData().length;
  }

  @Override
  public Object getObject() {
    // TODO Auto-generated method stub
    return this;
  }
  
  public byte[] getData()
  {
    return bytes.getData();
  }
  
}
