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

public enum BlockType {
  INVALID((byte) -1), FIXED((byte) 0), VARIABLE((byte) 1), SINGLE((byte) 2);

  public static BlockType getBlockType(byte value) {
    switch (value) {
    case 0:
      return FIXED;
    case 1:
      return VARIABLE;
    case 2:
      return SINGLE;
    }
    return INVALID;
  }

  private BlockType(byte value) {
    this.value = value;
  }

  public byte getValue() {
    return value;
  }

  private byte value;

}
