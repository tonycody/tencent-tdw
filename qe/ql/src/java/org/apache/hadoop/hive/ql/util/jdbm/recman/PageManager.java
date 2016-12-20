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

import java.io.*;

final class PageManager {
  private RecordFile file;
  private FileHeader header;
  private BlockIo headerBuf;

  PageManager(RecordFile file) throws IOException {
    this.file = file;

    headerBuf = file.get(0);
    if (headerBuf.readShort(0) == 0)
      header = new FileHeader(headerBuf, true);
    else
      header = new FileHeader(headerBuf, false);
  }

  long allocate(short type) throws IOException {

    if (type == Magic.FREE_PAGE)
      throw new Error("allocate of free page?");

    long retval = header.getFirstOf(Magic.FREE_PAGE);
    boolean isNew = false;
    if (retval != 0) {
      header.setFirstOf(Magic.FREE_PAGE, getNext(retval));
    } else {
      retval = header.getLastOf(Magic.FREE_PAGE);
      if (retval == 0)
        retval = 1;
      header.setLastOf(Magic.FREE_PAGE, retval + 1);
      isNew = true;
    }

    BlockIo buf = file.get(retval);
    PageHeader pageHdr = isNew ? new PageHeader(buf, type) : PageHeader
        .getView(buf);
    long oldLast = header.getLastOf(type);

    System.arraycopy(RecordFile.cleanData, 0, buf.getData(), 0,
        RecordFile.BLOCK_SIZE);
    pageHdr.setType(type);
    pageHdr.setPrev(oldLast);
    pageHdr.setNext(0);

    if (oldLast == 0)
      header.setFirstOf(type, retval);
    header.setLastOf(type, retval);
    file.release(retval, true);

    if (oldLast != 0) {
      buf = file.get(oldLast);
      pageHdr = PageHeader.getView(buf);
      pageHdr.setNext(retval);
      file.release(oldLast, true);
    }

    buf.setView(null);

    return retval;
  }

  void free(short type, long recid) throws IOException {
    if (type == Magic.FREE_PAGE)
      throw new Error("free free page?");
    if (recid == 0)
      throw new Error("free header page?");

    BlockIo buf = file.get(recid);
    PageHeader pageHdr = PageHeader.getView(buf);
    long prev = pageHdr.getPrev();
    long next = pageHdr.getNext();

    pageHdr.setType(Magic.FREE_PAGE);
    pageHdr.setNext(header.getFirstOf(Magic.FREE_PAGE));
    pageHdr.setPrev(0);

    header.setFirstOf(Magic.FREE_PAGE, recid);
    file.release(recid, true);

    if (prev != 0) {
      buf = file.get(prev);
      pageHdr = PageHeader.getView(buf);
      pageHdr.setNext(next);
      file.release(prev, true);
    } else {
      header.setFirstOf(type, next);
    }
    if (next != 0) {
      buf = file.get(next);
      pageHdr = PageHeader.getView(buf);
      pageHdr.setPrev(prev);
      file.release(next, true);
    } else {
      header.setLastOf(type, prev);
    }

  }

  long getNext(long block) throws IOException {
    try {
      return PageHeader.getView(file.get(block)).getNext();
    } finally {
      file.release(block, false);
    }
  }

  long getPrev(long block) throws IOException {
    try {
      return PageHeader.getView(file.get(block)).getPrev();
    } finally {
      file.release(block, false);
    }
  }

  long getFirst(short type) throws IOException {
    return header.getFirstOf(type);
  }

  long getLast(short type) throws IOException {
    return header.getLastOf(type);
  }

  void commit() throws IOException {
    file.release(headerBuf);
    file.commit();

    headerBuf = file.get(0);
    header = new FileHeader(headerBuf, false);
  }

  void rollback() throws IOException {
    file.discard(headerBuf);
    file.rollback();
    headerBuf = file.get(0);
    if (headerBuf.readShort(0) == 0)
      header = new FileHeader(headerBuf, true);
    else
      header = new FileHeader(headerBuf, false);
  }

  void close() throws IOException {
    file.release(headerBuf);
    file.commit();
    headerBuf = null;
    header = null;
    file = null;
  }

  FileHeader getFileHeader() {
    return header;
  }

}
