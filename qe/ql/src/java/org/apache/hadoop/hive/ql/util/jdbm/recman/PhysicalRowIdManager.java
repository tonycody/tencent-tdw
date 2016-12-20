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

import java.io.IOException;

final class PhysicalRowIdManager {

  private RecordFile file;
  private PageManager pageman;
  private FreePhysicalRowIdPageManager freeman;

  PhysicalRowIdManager(RecordFile file, PageManager pageManager)
      throws IOException {
    this.file = file;
    this.pageman = pageManager;
    this.freeman = new FreePhysicalRowIdPageManager(file, pageman);
  }

  Location insert(byte[] data, int start, int length) throws IOException {
    Location retval = alloc(length);
    write(retval, data, start, length);
    return retval;
  }

  Location update(Location rowid, byte[] data, int start, int length)
      throws IOException {
    BlockIo block = file.get(rowid.getBlock());
    RecordHeader head = new RecordHeader(block, rowid.getOffset());
    if (length > head.getAvailableSize()) {
      file.release(block);
      free(rowid);
      rowid = alloc(length);
    } else {
      file.release(block);
    }

    write(rowid, data, start, length);
    return rowid;
  }

  void delete(Location rowid) throws IOException {
    free(rowid);
  }

  byte[] fetch(Location rowid) throws IOException {
    PageCursor curs = new PageCursor(pageman, rowid.getBlock());
    BlockIo block = file.get(curs.getCurrent());
    RecordHeader head = new RecordHeader(block, rowid.getOffset());

    byte[] retval = new byte[head.getCurrentSize()];
    if (retval.length == 0) {
      file.release(curs.getCurrent(), false);
      return retval;
    }

    int offsetInBuffer = 0;
    int leftToRead = retval.length;
    short dataOffset = (short) (rowid.getOffset() + RecordHeader.SIZE);
    while (leftToRead > 0) {
      int toCopy = RecordFile.BLOCK_SIZE - dataOffset;
      if (leftToRead < toCopy) {
        toCopy = leftToRead;
      }
      System.arraycopy(block.getData(), dataOffset, retval, offsetInBuffer,
          toCopy);

      leftToRead -= toCopy;
      offsetInBuffer += toCopy;

      file.release(block);

      if (leftToRead > 0) {
        block = file.get(curs.next());
        dataOffset = DataPage.O_DATA;
      }

    }

    return retval;
  }

  private Location alloc(int size) throws IOException {
    Location retval = freeman.get(size);
    if (retval == null) {
      retval = allocNew(size, pageman.getLast(Magic.USED_PAGE));
    }
    return retval;
  }

  private Location allocNew(int size, long start) throws IOException {
    BlockIo curBlock;
    DataPage curPage;
    if (start == 0) {
      start = pageman.allocate(Magic.USED_PAGE);
      curBlock = file.get(start);
      curPage = DataPage.getDataPageView(curBlock);
      curPage.setFirst(DataPage.O_DATA);
      RecordHeader hdr = new RecordHeader(curBlock, DataPage.O_DATA);
      hdr.setAvailableSize(0);
      hdr.setCurrentSize(0);
    } else {
      curBlock = file.get(start);
      curPage = DataPage.getDataPageView(curBlock);
    }

    short pos = curPage.getFirst();
    if (pos == 0) {
      file.release(curBlock);
      return allocNew(size, 0);
    }

    RecordHeader hdr = new RecordHeader(curBlock, pos);
    while (hdr.getAvailableSize() != 0 && pos < RecordFile.BLOCK_SIZE) {
      pos += hdr.getAvailableSize() + RecordHeader.SIZE;
      if (pos == RecordFile.BLOCK_SIZE) {
        file.release(curBlock);
        return allocNew(size, 0);
      }

      hdr = new RecordHeader(curBlock, pos);
    }

    if (pos == RecordHeader.SIZE) {
      file.release(curBlock);
    }

    Location retval = new Location(start, pos);
    int freeHere = RecordFile.BLOCK_SIZE - pos - RecordHeader.SIZE;
    if (freeHere < size) {
      int lastSize = (size - freeHere) % DataPage.DATA_PER_PAGE;
      if ((DataPage.DATA_PER_PAGE - lastSize) < (RecordHeader.SIZE + 16)) {
        size += (DataPage.DATA_PER_PAGE - lastSize);
      }

      hdr.setAvailableSize(size);
      file.release(start, true);

      int neededLeft = size - freeHere;
      while (neededLeft >= DataPage.DATA_PER_PAGE) {
        start = pageman.allocate(Magic.USED_PAGE);
        curBlock = file.get(start);
        curPage = DataPage.getDataPageView(curBlock);
        curPage.setFirst((short) 0);
        file.release(start, true);
        neededLeft -= DataPage.DATA_PER_PAGE;
      }
      if (neededLeft > 0) {
        start = pageman.allocate(Magic.USED_PAGE);
        curBlock = file.get(start);
        curPage = DataPage.getDataPageView(curBlock);
        curPage.setFirst((short) (DataPage.O_DATA + neededLeft));
        file.release(start, true);
      }
    } else {
      if (freeHere - size <= (16 + RecordHeader.SIZE)) {
        size = freeHere;
      }
      hdr.setAvailableSize(size);
      file.release(start, true);
    }
    return retval;

  }

  private void free(Location id) throws IOException {
    BlockIo curBlock = file.get(id.getBlock());
    DataPage curPage = DataPage.getDataPageView(curBlock);
    RecordHeader hdr = new RecordHeader(curBlock, id.getOffset());
    hdr.setCurrentSize(0);
    file.release(id.getBlock(), true);

    freeman.put(id, hdr.getAvailableSize());
  }

  private void write(Location rowid, byte[] data, int start, int length)
      throws IOException {
    PageCursor curs = new PageCursor(pageman, rowid.getBlock());
    BlockIo block = file.get(curs.getCurrent());
    RecordHeader hdr = new RecordHeader(block, rowid.getOffset());
    hdr.setCurrentSize(length);
    if (length == 0) {
      file.release(curs.getCurrent(), true);
      return;
    }

    int offsetInBuffer = start;
    int leftToWrite = length;
    short dataOffset = (short) (rowid.getOffset() + RecordHeader.SIZE);
    while (leftToWrite > 0) {
      int toCopy = RecordFile.BLOCK_SIZE - dataOffset;

      if (leftToWrite < toCopy) {
        toCopy = leftToWrite;
      }
      System.arraycopy(data, offsetInBuffer, block.getData(), dataOffset,
          toCopy);

      leftToWrite -= toCopy;
      offsetInBuffer += toCopy;

      file.release(curs.getCurrent(), true);

      if (leftToWrite > 0) {
        block = file.get(curs.next());
        dataOffset = DataPage.O_DATA;
      }
    }
  }
}
