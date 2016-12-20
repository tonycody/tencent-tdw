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
package recordio.utils;

public class LittleEndian {
  public static short getShort(byte[] buffer, int offset) {
    assert (buffer.length <= (offset + Constants.SHORT_SIZE));
    short value = 0;
    value |= ((short) buffer[offset++]) & 0x00ff;
    value |= ((short) buffer[offset]) << 8;
    return value;
  }

  public static int getInt(byte[] buffer, int offset) {
    assert (buffer.length <= (offset + Constants.INT_SIZE));
    int value = 0;
    value |= ((int) buffer[offset++]) & 0x000000ff;
    value |= (((int) buffer[offset++]) & 0x000000ff) << 8;
    value |= (((int) buffer[offset++]) & 0x000000ff) << 16;
    value |= (((int) buffer[offset++]) & 0x000000ff) << 24;
    return value;
  }

  public static void putShort(short value, byte[] buffer, int offset) {
    assert (buffer.length <= (offset + Constants.SHORT_SIZE));
    buffer[offset++] = (byte) value;
    buffer[offset++] = (byte) (value >>> 8);
  }

  public static void putInt(int value, byte[] buffer, int offset) {
    assert (buffer.length <= (offset + Constants.INT_SIZE));
    buffer[offset++] = (byte) value;
    buffer[offset++] = (byte) (value >>> 8);
    buffer[offset++] = (byte) (value >>> 16);
    buffer[offset++] = (byte) (value >>> 24);
  }
}
