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

package org.apache.hadoop.hive.ql.util.jdbm.recman;

class RecordHeader {
  private static final short O_CURRENTSIZE = 0;
  private static final short O_AVAILABLESIZE = Magic.SZ_INT;
  static final int SIZE = O_AVAILABLESIZE + Magic.SZ_INT;

  private BlockIo block;
  private short pos;

  RecordHeader(BlockIo block, short pos) {
    this.block = block;
    this.pos = pos;
    if (pos > (RecordFile.BLOCK_SIZE - SIZE))
      throw new Error("Offset too large for record header ("
          + block.getBlockId() + ":" + pos + ")");
  }

  int getCurrentSize() {
    return block.readInt(pos + O_CURRENTSIZE);
  }

  void setCurrentSize(int value) {
    block.writeInt(pos + O_CURRENTSIZE, value);
  }

  int getAvailableSize() {
    return block.readInt(pos + O_AVAILABLESIZE);
  }

  void setAvailableSize(int value) {
    block.writeInt(pos + O_AVAILABLESIZE, value);
  }

  public String toString() {
    return "RH(" + block.getBlockId() + ":" + pos + ", avl="
        + getAvailableSize() + ", cur=" + getCurrentSize() + ")";
  }
}
