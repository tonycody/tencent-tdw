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

public class PageHeader implements BlockView {
  private static final short O_MAGIC = 0;
  private static final short O_NEXT = Magic.SZ_SHORT;
  private static final short O_PREV = O_NEXT + Magic.SZ_LONG;
  protected static final short SIZE = O_PREV + Magic.SZ_LONG;

  protected BlockIo block;

  protected PageHeader(BlockIo block) {
    initialize(block);
    if (!magicOk())
      throw new Error("CRITICAL: page header magic for block "
          + block.getBlockId() + " not OK " + getMagic());
  }

  PageHeader(BlockIo block, short type) {
    initialize(block);
    setType(type);
  }

  static PageHeader getView(BlockIo block) {
    BlockView view = block.getView();
    if (view != null && view instanceof PageHeader)
      return (PageHeader) view;
    else
      return new PageHeader(block);
  }

  private void initialize(BlockIo block) {
    this.block = block;
    block.setView(this);
  }

  private boolean magicOk() {
    int magic = getMagic();
    return magic >= Magic.BLOCK
        && magic <= (Magic.BLOCK + Magic.FREEPHYSIDS_PAGE);
  }

  protected void paranoiaMagicOk() {
    if (!magicOk())
      throw new Error("CRITICAL: page header magic not OK " + getMagic());
  }

  short getMagic() {
    return block.readShort(O_MAGIC);
  }

  long getNext() {
    paranoiaMagicOk();
    return block.readLong(O_NEXT);
  }

  void setNext(long next) {
    paranoiaMagicOk();
    block.writeLong(O_NEXT, next);
  }

  long getPrev() {
    paranoiaMagicOk();
    return block.readLong(O_PREV);
  }

  void setPrev(long prev) {
    paranoiaMagicOk();
    block.writeLong(O_PREV, prev);
  }

  void setType(short type) {
    block.writeShort(O_MAGIC, (short) (Magic.BLOCK + type));
  }
}
