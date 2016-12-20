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

final class FreePhysicalRowIdPage extends PageHeader {
  private static final short O_COUNT = PageHeader.SIZE;
  static final short O_FREE = O_COUNT + Magic.SZ_SHORT;
  static final short ELEMS_PER_PAGE = (RecordFile.BLOCK_SIZE - O_FREE)
      / FreePhysicalRowId.SIZE;

  FreePhysicalRowId[] slots = new FreePhysicalRowId[ELEMS_PER_PAGE];

  FreePhysicalRowIdPage(BlockIo block) {
    super(block);
  }

  static FreePhysicalRowIdPage getFreePhysicalRowIdPageView(BlockIo block) {
    BlockView view = block.getView();
    if (view != null && view instanceof FreePhysicalRowIdPage)
      return (FreePhysicalRowIdPage) view;
    else
      return new FreePhysicalRowIdPage(block);
  }

  short getCount() {
    return block.readShort(O_COUNT);
  }

  private void setCount(short i) {
    block.writeShort(O_COUNT, i);
  }

  void free(int slot) {
    get(slot).setSize(0);
    setCount((short) (getCount() - 1));
  }

  FreePhysicalRowId alloc(int slot) {
    setCount((short) (getCount() + 1));
    return get(slot);
  }

  boolean isAllocated(int slot) {
    return get(slot).getSize() != 0;
  }

  boolean isFree(int slot) {
    return !isAllocated(slot);
  }

  FreePhysicalRowId get(int slot) {
    if (slots[slot] == null)
      slots[slot] = new FreePhysicalRowId(block, slotToOffset(slot));
    ;
    return slots[slot];
  }

  short slotToOffset(int slot) {
    return (short) (O_FREE + (slot * FreePhysicalRowId.SIZE));
  }

  int getFirstFree() {
    for (int i = 0; i < ELEMS_PER_PAGE; i++) {
      if (isFree(i))
        return i;
    }
    return -1;
  }

  int getFirstLargerThan(int size) {
    for (int i = 0; i < ELEMS_PER_PAGE; i++) {
      if (isAllocated(i) && get(i).getSize() >= size)
        return i;
    }
    return -1;
  }
}
