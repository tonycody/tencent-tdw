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

class FreeLogicalRowIdPage extends PageHeader {
  private static final short O_COUNT = PageHeader.SIZE;
  static final short O_FREE = (short) (O_COUNT + Magic.SZ_SHORT);
  static final short ELEMS_PER_PAGE = (short) ((RecordFile.BLOCK_SIZE - O_FREE) / PhysicalRowId.SIZE);

  final PhysicalRowId[] slots = new PhysicalRowId[ELEMS_PER_PAGE];

  FreeLogicalRowIdPage(BlockIo block) {
    super(block);
  }

  static FreeLogicalRowIdPage getFreeLogicalRowIdPageView(BlockIo block) {

    BlockView view = block.getView();
    if (view != null && view instanceof FreeLogicalRowIdPage)
      return (FreeLogicalRowIdPage) view;
    else
      return new FreeLogicalRowIdPage(block);
  }

  short getCount() {
    return block.readShort(O_COUNT);
  }

  private void setCount(short i) {
    block.writeShort(O_COUNT, i);
  }

  void free(int slot) {
    get(slot).setBlock(0);
    setCount((short) (getCount() - 1));
  }

  PhysicalRowId alloc(int slot) {
    setCount((short) (getCount() + 1));
    get(slot).setBlock(-1);
    return get(slot);
  }

  boolean isAllocated(int slot) {
    return get(slot).getBlock() > 0;
  }

  boolean isFree(int slot) {
    return !isAllocated(slot);
  }

  PhysicalRowId get(int slot) {
    if (slots[slot] == null)
      slots[slot] = new PhysicalRowId(block, slotToOffset(slot));
    ;
    return slots[slot];
  }

  private short slotToOffset(int slot) {
    return (short) (O_FREE + (slot * PhysicalRowId.SIZE));
  }

  int getFirstFree() {
    for (int i = 0; i < ELEMS_PER_PAGE; i++) {
      if (isFree(i))
        return i;
    }
    return -1;
  }

  int getFirstAllocated() {
    for (int i = 0; i < ELEMS_PER_PAGE; i++) {
      if (isAllocated(i))
        return i;
    }
    return -1;
  }
}
