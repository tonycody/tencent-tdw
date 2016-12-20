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

public class Varint {
  public static int decode(byte[] data, int off) {
    int pos = off;
    int firstByte = (int) data[pos++];

    if ((firstByte & 0x80) == 0) {
      return firstByte;
    }

    int result = firstByte & 0x7f;
    int offset = 7;
    for (; offset < 32; offset += 7) {
      byte b = data[pos++];
      result |= (b & 0x7f) << offset;
      if ((b & 0x80) == 0) {
        return result;
      }
    }
    for (; offset < 64; offset += 7) {
      byte b = data[pos++];
      if ((b & 0x80) == 0) {
        return result;
      }
    }
    return result;
  }

  public static int size(int value) {
    if ((value & (0xffffffff << 7)) == 0)
      return 1;
    if ((value & (0xffffffff << 14)) == 0)
      return 2;
    if ((value & (0xffffffff << 21)) == 0)
      return 3;
    if ((value & (0xffffffff << 28)) == 0)
      return 4;
    return 5;
  }

  public static int encode(int value, byte[] data, int offset) {
    int pos = offset;
    while (true) {
      if ((value & ~0x7F) == 0) {
        data[pos++] = (byte) value;
        break;
      } else {
        data[pos++] = (byte) ((value & 0x7F) | 0x80);
        value >>>= 7;
      }
    }
    return pos - offset;
  }

}
