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
import java.util.*;

public final class RecordFile {
  final TransactionManager txnMgr;

  private final LinkedList free = new LinkedList();
  private final HashMap inUse = new HashMap();
  private final HashMap dirty = new HashMap();
  private final HashMap inTxn = new HashMap();

  private boolean transactionsDisabled = false;

  public final static int BLOCK_SIZE = 8192;

  final static String extension = ".db";

  final static byte[] cleanData = new byte[BLOCK_SIZE];

  private RandomAccessFile file;
  private final String fileName;

  RecordFile(String fileName) throws IOException {
    this.fileName = fileName;
    file = new RandomAccessFile(fileName + extension, "rw");
    txnMgr = new TransactionManager(this);
  }

  RecordFile(File file) throws IOException {
    this.fileName = file.getName();
    this.file = new RandomAccessFile(file, "rw");
    txnMgr = new TransactionManager(this);
  }

  String getFileName() {
    return fileName;
  }

  void disableTransactions() {
    transactionsDisabled = true;
  }

  BlockIo get(long blockid) throws IOException {
    Long key = new Long(blockid);

    BlockIo node = (BlockIo) inTxn.get(key);
    if (node != null) {
      inTxn.remove(key);
      inUse.put(key, node);
      return node;
    }
    node = (BlockIo) dirty.get(key);
    if (node != null) {
      dirty.remove(key);
      inUse.put(key, node);
      return node;
    }
    for (Iterator i = free.iterator(); i.hasNext();) {
      BlockIo cur = (BlockIo) i.next();
      if (cur.getBlockId() == blockid) {
        node = cur;
        i.remove();
        inUse.put(key, node);
        return node;
      }
    }

    if (inUse.get(key) != null) {
      throw new Error("double get for block " + blockid);
    }

    node = getNewNode(blockid);
    long offset = blockid * BLOCK_SIZE;
    if (file.length() > 0 && offset <= file.length()) {
      read(file, offset, node.getData(), BLOCK_SIZE);
    } else {
      System.arraycopy(cleanData, 0, node.getData(), 0, BLOCK_SIZE);
    }
    inUse.put(key, node);
    node.setClean();
    return node;
  }

  void release(long blockid, boolean isDirty) throws IOException {
    BlockIo node = (BlockIo) inUse.get(new Long(blockid));
    if (node == null)
      throw new IOException("bad blockid " + blockid + " on release");
    if (!node.isDirty() && isDirty)
      node.setDirty();
    release(node);
  }

  void release(BlockIo block) {
    Long key = new Long(block.getBlockId());
    inUse.remove(key);
    if (block.isDirty()) {
      dirty.put(key, block);
    } else {
      if (!transactionsDisabled && block.isInTransaction()) {
        inTxn.put(key, block);
      } else {
        free.add(block);
      }
    }
  }

  void discard(BlockIo block) {
    Long key = new Long(block.getBlockId());
    inUse.remove(key);

  }

  void commit() throws IOException {
    if (!inUse.isEmpty() && inUse.size() > 1) {
      showList(inUse.values().iterator());
      throw new Error("in use list not empty at commit time (" + inUse.size()
          + ")");
    }

    if (dirty.size() == 0) {
      return;
    }

    if (!transactionsDisabled) {
      txnMgr.start();
    }

    for (Iterator i = dirty.values().iterator(); i.hasNext();) {
      BlockIo node = (BlockIo) i.next();
      i.remove();
      if (transactionsDisabled) {
        long offset = node.getBlockId() * BLOCK_SIZE;
        file.seek(offset);
        file.write(node.getData());
        node.setClean();
        free.add(node);
      } else {
        txnMgr.add(node);
        inTxn.put(new Long(node.getBlockId()), node);
      }
    }
    if (!transactionsDisabled) {
      txnMgr.commit();
    }
  }

  void rollback() throws IOException {
    if (!inUse.isEmpty()) {
      showList(inUse.values().iterator());
      throw new Error("in use list not empty at rollback time (" + inUse.size()
          + ")");
    }
    dirty.clear();

    txnMgr.synchronizeLogFromDisk();

    if (!inTxn.isEmpty()) {
      showList(inTxn.values().iterator());
      throw new Error("in txn list not empty at rollback time (" + inTxn.size()
          + ")");
    }
    ;
  }

  void close() throws IOException {
    if (!dirty.isEmpty()) {
      commit();
    }
    txnMgr.shutdown();

    if (transactionsDisabled) {
      txnMgr.removeLogFile();
    }

    if (!inTxn.isEmpty()) {
      showList(inTxn.values().iterator());
      throw new Error("In transaction not empty");
    }

    if (!dirty.isEmpty()) {
      System.out.println("ERROR: dirty blocks at close time");
      showList(dirty.values().iterator());
      throw new Error("Dirty blocks at close time");
    }
    if (!inUse.isEmpty()) {
      System.out.println("ERROR: inUse blocks at close time");
      showList(inUse.values().iterator());
      throw new Error("inUse blocks at close time");
    }

    file.close();
    file = null;
  }

  void forceClose() throws IOException {
    txnMgr.forceClose();
    file.close();
  }

  private void showList(Iterator i) {
    int cnt = 0;
    while (i.hasNext()) {
      System.out.println("elem " + cnt + ": " + i.next());
      cnt++;
    }
  }

  private BlockIo getNewNode(long blockid) throws IOException {

    BlockIo retval = null;
    if (!free.isEmpty()) {
      retval = (BlockIo) free.removeFirst();
    }
    if (retval == null)
      retval = new BlockIo(0, new byte[BLOCK_SIZE]);

    retval.setBlockId(blockid);
    retval.setView(null);
    return retval;
  }

  void synch(BlockIo node) throws IOException {
    byte[] data = node.getData();
    if (data != null) {
      long offset = node.getBlockId() * BLOCK_SIZE;
      file.seek(offset);
      file.write(data);
    }
  }

  void releaseFromTransaction(BlockIo node, boolean recycle) throws IOException {
    Long key = new Long(node.getBlockId());
    if ((inTxn.remove(key) != null) && recycle) {
      free.add(node);
    }
  }

  void sync() throws IOException {
    file.getFD().sync();
  }

  private static void read(RandomAccessFile file, long offset, byte[] buffer,
      int nBytes) throws IOException {
    file.seek(offset);
    int remaining = nBytes;
    int pos = 0;
    while (remaining > 0) {
      int read = file.read(buffer, pos, remaining);
      if (read == -1) {
        System.arraycopy(cleanData, 0, buffer, pos, remaining);
        break;
      }
      remaining -= read;
      pos += read;
    }
  }

}
