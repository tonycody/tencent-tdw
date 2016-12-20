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

public final class TransactionManager {
  private RecordFile owner;

  private FileOutputStream fos;
  private ObjectOutputStream oos;

  static final int DEFAULT_TXNS_IN_LOG = 10;

  private int _maxTxns = DEFAULT_TXNS_IN_LOG;

  private ArrayList[] txns = new ArrayList[DEFAULT_TXNS_IN_LOG];
  private int curTxn = -1;

  static final String extension = ".lg";

  private String logFileName;

  TransactionManager(RecordFile owner) throws IOException {
    this.owner = owner;
    logFileName = null;
    recover();
    open();
  }

  public void synchronizeLog() throws IOException {
    synchronizeLogFromMemory();
  }

  public void setMaximumTransactionsInLog(int maxTxns) throws IOException {
    if (maxTxns <= 0) {
      throw new IllegalArgumentException(
          "Argument 'maxTxns' must be greater than 0.");
    }
    if (curTxn != -1) {
      throw new IllegalStateException(
          "Cannot change setting while transactions are pending in the log");
    }
    _maxTxns = maxTxns;
    txns = new ArrayList[maxTxns];
  }

  private String makeLogName() {
    return owner.getFileName() + extension;
  }

  private void synchronizeLogFromMemory() throws IOException {
    close();

    TreeSet blockList = new TreeSet(new BlockIoComparator());

    int numBlocks = 0;
    int writtenBlocks = 0;
    for (int i = 0; i < _maxTxns; i++) {
      if (txns[i] == null)
        continue;
      for (Iterator k = txns[i].iterator(); k.hasNext();) {
        BlockIo block = (BlockIo) k.next();
        if (blockList.contains(block)) {
          block.decrementTransactionCount();
        } else {
          writtenBlocks++;
          boolean result = blockList.add(block);
        }
        numBlocks++;
      }

      txns[i] = null;
    }
    synchronizeBlocks(blockList.iterator(), true);

    owner.sync();
    open();
  }

  private void open() throws IOException {
    logFileName = makeLogName();
    fos = new FileOutputStream(logFileName);
    oos = new ObjectOutputStream(fos);
    oos.writeShort(Magic.LOGFILE_HEADER);
    oos.flush();
    curTxn = -1;
  }

  private void recover() throws IOException {
    String logName = makeLogName();
    File logFile = new File(logName);
    if (!logFile.exists())
      return;
    if (logFile.length() == 0) {
      logFile.delete();
      return;
    }

    FileInputStream fis = new FileInputStream(logFile);
    ObjectInputStream ois = new ObjectInputStream(fis);

    try {
      if (ois.readShort() != Magic.LOGFILE_HEADER)
        throw new Error("Bad magic on log file");
    } catch (IOException e) {
      logFile.delete();
      return;
    }

    while (true) {
      ArrayList blocks = null;
      try {
        blocks = (ArrayList) ois.readObject();
      } catch (ClassNotFoundException e) {
        throw new Error("Unexcepted exception: " + e);
      } catch (IOException e) {
        break;
      }
      synchronizeBlocks(blocks.iterator(), false);

      try {
        ois = new ObjectInputStream(fis);
      } catch (IOException e) {
        break;
      }
    }
    owner.sync();
    logFile.delete();
  }

  private void synchronizeBlocks(Iterator blockIterator, boolean fromCore)
      throws IOException {
    while (blockIterator.hasNext()) {
      BlockIo cur = (BlockIo) blockIterator.next();
      owner.synch(cur);
      if (fromCore) {
        cur.decrementTransactionCount();
        if (!cur.isInTransaction()) {
          owner.releaseFromTransaction(cur, true);
        }
      }
    }
  }

  private void setClean(ArrayList blocks) throws IOException {
    for (Iterator k = blocks.iterator(); k.hasNext();) {
      BlockIo cur = (BlockIo) k.next();
      cur.setClean();
    }
  }

  private void discardBlocks(ArrayList blocks) throws IOException {
    for (Iterator k = blocks.iterator(); k.hasNext();) {
      BlockIo cur = (BlockIo) k.next();
      cur.decrementTransactionCount();
      if (!cur.isInTransaction()) {
        owner.releaseFromTransaction(cur, false);
      }
    }
  }

  void start() throws IOException {
    curTxn++;
    if (curTxn == _maxTxns) {
      synchronizeLogFromMemory();
      curTxn = 0;
    }
    txns[curTxn] = new ArrayList();
  }

  void add(BlockIo block) throws IOException {
    block.incrementTransactionCount();
    txns[curTxn].add(block);
  }

  void commit() throws IOException {
    oos.writeObject(txns[curTxn]);
    sync();

    setClean(txns[curTxn]);

    oos = new ObjectOutputStream(fos);
  }

  private void sync() throws IOException {
    oos.flush();
    fos.flush();
    fos.getFD().sync();
  }

  void shutdown() throws IOException {
    synchronizeLogFromMemory();
    close();
  }

  private void close() throws IOException {
    sync();
    oos.close();
    fos.close();
    oos = null;
    fos = null;
  }

  public void removeLogFile() {
    if (oos != null)
      return;
    if (logFileName != null) {
      File file = new File(logFileName);
      file.delete();
      logFileName = null;
    }
  }

  void forceClose() throws IOException {
    oos.close();
    fos.close();
    oos = null;
    fos = null;
  }

  void synchronizeLogFromDisk() throws IOException {
    close();

    for (int i = 0; i < _maxTxns; i++) {
      if (txns[i] == null)
        continue;
      discardBlocks(txns[i]);
      txns[i] = null;
    }

    recover();
    open();
  }

  public static class BlockIoComparator implements Comparator {

    public int compare(Object o1, Object o2) {
      BlockIo block1 = (BlockIo) o1;
      BlockIo block2 = (BlockIo) o2;
      int result = 0;
      if (block1.getBlockId() == block2.getBlockId()) {
        result = 0;
      } else if (block1.getBlockId() < block2.getBlockId()) {
        result = -1;
      } else {
        result = 1;
      }
      return result;
    }

    public boolean equals(Object obj) {
      return super.equals(obj);
    }
  }

}
