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

public final class BlockIo implements java.io.Externalizable {

  public final static long serialVersionUID = 2L;

  private long blockId;

  private transient byte[] data;
  private transient BlockView view = null;
  private transient boolean dirty = false;
  private transient int transactionCount = 0;

  public BlockIo() {
  }

  BlockIo(long blockId, byte[] data) {
    if (blockId > 10000000000L)
      throw new Error("bogus block id " + blockId);
    this.blockId = blockId;
    this.data = data;
  }

  byte[] getData() {
    return data;
  }

  void setBlockId(long id) {
    if (isInTransaction())
      throw new Error("BlockId assigned for transaction block");
    if (id > 10000000000L)
      throw new Error("bogus block id " + id);
    blockId = id;
  }

  long getBlockId() {
    return blockId;
  }

  public BlockView getView() {
    return view;
  }

  public void setView(BlockView view) {
    this.view = view;
  }

  void setDirty() {
    dirty = true;
  }

  void setClean() {
    dirty = false;
  }

  boolean isDirty() {
    return dirty;
  }

  boolean isInTransaction() {
    return transactionCount != 0;
  }

  synchronized void incrementTransactionCount() {
    transactionCount++;
    setClean();
  }

  synchronized void decrementTransactionCount() {
    transactionCount--;
    if (transactionCount < 0)
      throw new Error("transaction count on block " + getBlockId()
          + " below zero!");

  }

  public byte readByte(int pos) {
    return data[pos];
  }

  public void writeByte(int pos, byte value) {
    data[pos] = value;
    setDirty();
  }

  public short readShort(int pos) {
    return (short) (((short) (data[pos + 0] & 0xff) << 8) | ((short) (data[pos + 1] & 0xff) << 0));
  }

  public void writeShort(int pos, short value) {
    data[pos + 0] = (byte) (0xff & (value >> 8));
    data[pos + 1] = (byte) (0xff & (value >> 0));
    setDirty();
  }

  public int readInt(int pos) {
    return (((int) (data[pos + 0] & 0xff) << 24)
        | ((int) (data[pos + 1] & 0xff) << 16)
        | ((int) (data[pos + 2] & 0xff) << 8) | ((int) (data[pos + 3] & 0xff) << 0));
  }

  public void writeInt(int pos, int value) {
    data[pos + 0] = (byte) (0xff & (value >> 24));
    data[pos + 1] = (byte) (0xff & (value >> 16));
    data[pos + 2] = (byte) (0xff & (value >> 8));
    data[pos + 3] = (byte) (0xff & (value >> 0));
    setDirty();
  }

  public long readLong(int pos) {
    return ((long) (((data[pos + 0] & 0xff) << 24)
        | ((data[pos + 1] & 0xff) << 16) | ((data[pos + 2] & 0xff) << 8) | ((data[pos + 3] & 0xff))) << 32)
        | ((long) (((data[pos + 4] & 0xff) << 24)
            | ((data[pos + 5] & 0xff) << 16) | ((data[pos + 6] & 0xff) << 8) | ((data[pos + 7] & 0xff))) & 0xffffffff);

  }

  public void writeLong(int pos, long value) {
    data[pos + 0] = (byte) (0xff & (value >> 56));
    data[pos + 1] = (byte) (0xff & (value >> 48));
    data[pos + 2] = (byte) (0xff & (value >> 40));
    data[pos + 3] = (byte) (0xff & (value >> 32));
    data[pos + 4] = (byte) (0xff & (value >> 24));
    data[pos + 5] = (byte) (0xff & (value >> 16));
    data[pos + 6] = (byte) (0xff & (value >> 8));
    data[pos + 7] = (byte) (0xff & (value >> 0));
    setDirty();
  }

  public String toString() {
    return "BlockIO(" + blockId + "," + dirty + "," + view + ")";
  }

  public void readExternal(ObjectInput in) throws IOException,
      ClassNotFoundException {
    blockId = in.readLong();
    int length = in.readInt();
    data = new byte[length];
    in.readFully(data);
  }

  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeLong(blockId);
    out.writeInt(data.length);
    out.write(data);
  }

}
