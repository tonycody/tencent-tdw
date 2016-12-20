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

class PhysicalRowId {
  private static final short O_BLOCK = 0;
  private static final short O_OFFSET = Magic.SZ_LONG;
  static final int SIZE = O_OFFSET + Magic.SZ_SHORT;

  BlockIo block;
  short pos;

  PhysicalRowId(BlockIo block, short pos) {
    this.block = block;
    this.pos = pos;
  }

  long getBlock() {
    return block.readLong(pos + O_BLOCK);
  }

  void setBlock(long value) {
    block.writeLong(pos + O_BLOCK, value);
  }

  short getOffset() {
    return block.readShort(pos + O_OFFSET);
  }

  void setOffset(short value) {
    block.writeShort(pos + O_OFFSET, value);
  }
}
