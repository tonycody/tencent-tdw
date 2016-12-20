/**
* Tencent is pleased to support the open source community by making TDW available.
* Copyright (C) 2014 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use 
* this file except in compliance with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed 
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS 
* OF ANY KIND, either express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/

package org.apache.hadoop.hive.ql.exec.persistence;

import java.io.File;
import java.io.RandomAccessFile;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.Writable;

public class RowContainer1<Row> {

  protected Log LOG = LogFactory.getLog(this.getClass().getName());

  private static final int BLOCKSIZE = 25000;
  private static final int BLKMETA_LEN = 1;

  private Row[] lastBlock;
  private Row[] currBlock;

  private Row[] tmpBlock;
  private int pTmpBlock = -1;

  private int blockSize;
  private int numBlocks;
  private int size;
  private File tmpFile;
  private RandomAccessFile rFile;
  private long[] off_len;
  private int itrCursor;
  private int addCursor;
  private int pBlock;
  private SerDe serde;
  private ObjectInspector standardOI;
  private ArrayList dummyRow;
  private tableDesc tblDesc;

  int firstrowid = 0;
  int currBlockStartPos = 0;

  public RowContainer1() {
    this(BLOCKSIZE);
  }

  public RowContainer1(int blockSize) {
    this.blockSize = blockSize == 0 ? BLOCKSIZE : blockSize;
    this.size = 0;
    this.itrCursor = 0;
    this.addCursor = 0;
    this.numBlocks = 0;
    this.pBlock = 0;
    this.tmpFile = null;
    this.lastBlock = (Row[]) new ArrayList[blockSize];
    this.currBlock = this.lastBlock;
    this.serde = null;
    this.standardOI = null;
    this.dummyRow = new ArrayList(0);
  }

  public RowContainer1(int blockSize, SerDe sd, ObjectInspector oi) {
    this(blockSize);
    setSerDe(sd, oi);
  }

  public void setSerDe(SerDe sd, ObjectInspector oi) {
    assert serde != null : "serde is null";
    assert oi != null : "oi is null";
    this.serde = sd;
    this.standardOI = oi;
  }

  public void add(Row t) throws HiveException {
    if (currBlockStartPos == 0 && addCursor >= blockSize) {
      spillBlock(lastBlock);
      addCursor = 0;
      if (numBlocks == 1)
        lastBlock = (Row[]) new ArrayList[blockSize];
    } else if (addCursor >= blockSize) {
      addCursor = 0;
    }
    lastBlock[addCursor++] = t;
    ++size;
  }

  public void seek(int rowid) {
    if (numBlocks == 0) {
      currBlock = this.lastBlock;
      itrCursor = (rowid - this.firstrowid + currBlockStartPos) % blockSize;
    } else {
      int cb = rowid / blockSize;
      int cursor = rowid % blockSize;
      if (cb != pBlock) {
        pBlock = cb;
        currBlock = getBlock(pBlock);
      }
      itrCursor = cursor;
    }
  }

  public Row get(int rowid) {
    if (rowid >= size) {
      return null;
    }
    int cb = rowid / blockSize;
    int cursor = rowid % blockSize;
    if (cb > numBlocks) {
      return null;
    }
    if (cb == numBlocks) {
      return lastBlock[cursor];
    }
    if (cb == pBlock) {
      return currBlock[cursor];
    }
    if (cb != pTmpBlock) {
      pTmpBlock = cb;
      long offset = off_len[pTmpBlock * 2];
      long len = off_len[pTmpBlock * 2 + 1];
      byte[] buf = new byte[(int) len];
      try {
        rFile.seek(offset);
        rFile.readFully(buf);
        tmpBlock = deserialize(buf);
      } catch (Exception e) {
        LOG.error(e.toString());
      }
    }
    return tmpBlock[cursor];
  }

  public Row first() {
    if (size == 0)
      return null;
    if (numBlocks == 0) {
      currBlock = this.lastBlock;
      itrCursor = this.currBlockStartPos;
      return currBlock[this.itrCursor++];
    } else {
      if (pBlock > 0) {
        pBlock = 0;
        currBlock = getBlock(0);
        assert currBlock != null : "currBlock == null";
      }
      if (currBlock == null && lastBlock != null) {
        currBlock = lastBlock;
      }
      assert pBlock == 0 : "pBlock != 0 ";
      itrCursor = 1;
      return currBlock[0];
    }
  }

  public Row next() {
    assert pBlock <= numBlocks : "pBlock " + pBlock + " > numBlocks"
        + numBlocks;
    if (numBlocks == 0) {
      if (this.itrCursor >= this.currBlockStartPos
          || itrCursor < size % blockSize) {
        Row res = currBlock[itrCursor];
        if (itrCursor == blockSize)
          itrCursor = 0;
        return res;
      } else
        return null;

    } else {
      if (pBlock < numBlocks) {
        if (itrCursor < blockSize) {
          return currBlock[itrCursor++];
        } else if (++pBlock < numBlocks) {
          currBlock = getBlock(pBlock);
          assert currBlock != null : "currBlock == null";
          itrCursor = 1;
          return currBlock[0];
        } else {
          itrCursor = 0;
          currBlock = lastBlock;
        }
      }
      if (itrCursor < addCursor)
        return currBlock[itrCursor++];
      else
        return null;
    }
  }

  private void spillBlock(Row[] block) throws HiveException {
    try {
      if (tmpFile == null) {
        tmpFile = File.createTempFile("RowContainer", ".tmp", new File("/tmp"));
        LOG.info("RowContainer created temp file " + tmpFile.getAbsolutePath());
        tmpFile.deleteOnExit();
        rFile = new RandomAccessFile(tmpFile, "rw");
        off_len = new long[BLKMETA_LEN * 2];
      }
      byte[] buf = serialize(block);
      long offset = rFile.length();
      long len = buf.length;
      rFile.seek(offset);
      rFile.write(buf);

      addBlockMetadata(offset, len);
    } catch (Exception e) {
      LOG.debug(e.toString());
      throw new HiveException(e);
    }
  }

  private void addBlockMetadata(long offset, long len) {
    if ((numBlocks + 1) * 2 >= off_len.length) {
      off_len = Arrays.copyOf(off_len, off_len.length * 2);
    }
    off_len[numBlocks * 2] = offset;
    off_len[numBlocks * 2 + 1] = len;
    ++numBlocks;
  }

  private byte[] serialize(Row[] obj) throws HiveException {
    assert (serde != null && standardOI != null);

    ByteArrayOutputStream baos;
    DataOutputStream oos;

    try {
      baos = new ByteArrayOutputStream();
      oos = new DataOutputStream(baos);

      oos.writeInt(obj.length);

      if (serde != null && standardOI != null) {
        for (int i = 0; i < obj.length; ++i) {
          Writable outVal = serde.serialize(obj[i], standardOI);
          outVal.write(oos);
        }
      }
      oos.close();
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
    return baos.toByteArray();
  }

  private Row[] deserialize(byte[] buf) throws HiveException {
    ByteArrayInputStream bais;
    DataInputStream ois;

    try {
      bais = new ByteArrayInputStream(buf);
      ois = new DataInputStream(bais);
      int sz = ois.readInt();
      assert sz == blockSize : "deserialized size " + sz
          + " is not the same as block size " + blockSize;
      Row[] ret = (Row[]) new ArrayList[sz];

      for (int i = 0; i < sz; ++i) {
        if (serde != null && standardOI != null) {
          Writable val = serde.getSerializedClass().newInstance();
          val.readFields(ois);

          ret[i] = (Row) ObjectInspectorUtils.copyToStandardObject(
              serde.deserialize(val), serde.getObjectInspector(),
              ObjectInspectorCopyOption.WRITABLE);
        } else {
          ret[i] = (Row) dummyRow;
        }
      }
      return ret;
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  public int size() {
    return size;
  }

  public void clear() throws HiveException {
    itrCursor = 0;
    addCursor = 0;
    numBlocks = 0;
    pBlock = 0;
    size = 0;

    pTmpBlock = -1;
    this.firstrowid = 0;
    currBlockStartPos = 0;

    try {
      if (rFile != null)
        rFile.close();
      if (tmpFile != null)
        tmpFile.delete();
    } catch (Exception e) {
      LOG.error(e.toString());
      throw new HiveException(e);
    }
    tmpFile = null;
  }

  private Row[] getBlock(int block) {
    long offset = off_len[block * 2];
    long len = off_len[block * 2 + 1];
    byte[] buf = new byte[(int) len];
    try {
      rFile.seek(offset);
      rFile.readFully(buf);
      currBlock = deserialize(buf);
    } catch (Exception e) {
      LOG.error(e.toString());
      return null;
    }
    return currBlock;
  }

  public void setTableDesc(tableDesc tblDesc) {
    this.tblDesc = tblDesc;
  }

  public int firstId() {
    return firstrowid;
  }

  public void removeFirst() {
    if (numBlocks == 0)
      currBlockStartPos = (currBlockStartPos++) % blockSize;
    firstrowid++;
  }
}
