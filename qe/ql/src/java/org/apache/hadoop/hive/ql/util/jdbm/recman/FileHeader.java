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

class FileHeader implements BlockView {
  private static final short O_MAGIC = 0;
  private static final short O_LISTS = Magic.SZ_SHORT;
  private static final int O_ROOTS = O_LISTS
      + (Magic.NLISTS * 2 * Magic.SZ_LONG);

  private BlockIo block;

  static final int NROOTS = (RecordFile.BLOCK_SIZE - O_ROOTS) / Magic.SZ_LONG;

  FileHeader(BlockIo block, boolean isNew) {
    this.block = block;
    if (isNew)
      block.writeShort(O_MAGIC, Magic.FILE_HEADER);
    else if (!magicOk())
      throw new Error("CRITICAL: file header magic not OK "
          + block.readShort(O_MAGIC));
  }

  private boolean magicOk() {
    return block.readShort(O_MAGIC) == Magic.FILE_HEADER;
  }

  private short offsetOfFirst(int list) {
    return (short) (O_LISTS + (2 * Magic.SZ_LONG * list));
  }

  private short offsetOfLast(int list) {
    return (short) (offsetOfFirst(list) + Magic.SZ_LONG);
  }

  private short offsetOfRoot(int root) {
    return (short) (O_ROOTS + (root * Magic.SZ_LONG));
  }

  long getFirstOf(int list) {
    return block.readLong(offsetOfFirst(list));
  }

  void setFirstOf(int list, long value) {
    block.writeLong(offsetOfFirst(list), value);
  }

  long getLastOf(int list) {
    return block.readLong(offsetOfLast(list));
  }

  void setLastOf(int list, long value) {
    block.writeLong(offsetOfLast(list), value);
  }

  long getRoot(int root) {
    return block.readLong(offsetOfRoot(root));
  }

  void setRoot(int root, long rowid) {
    block.writeLong(offsetOfRoot(root), rowid);
  }
}
