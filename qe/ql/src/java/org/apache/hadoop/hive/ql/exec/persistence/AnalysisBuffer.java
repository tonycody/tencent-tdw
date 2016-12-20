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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.Writable;

public class AnalysisBuffer<Row> {

  transient protected Log LOG = LogFactory.getLog(this.getClass().getName());

  static class Buffer<REC> {
    REC[] buffer;
    int buffersize = 0;

    boolean full = false;
    int firstrowid = 0;
    int firstpos = 0;
    int currentpos = 0;

    @SuppressWarnings("unchecked")
    public Buffer(int buffersize) {
      this.buffersize = buffersize;
      this.buffer = (REC[]) new Object[this.buffersize];
    }

    public boolean add(REC rec) {
      if (full)
        return false;
      buffer[currentpos] = rec;
      currentpos = (currentpos + 1) % buffersize;
      if (currentpos == firstpos) {
        full = true;
      }
      return true;
    }

    public REC removeFirst() {
      if (!full && (firstpos == currentpos))
        return null;
      REC res = buffer[firstpos];
      buffer[firstpos] = null;
      firstpos = (firstpos + 1) % buffersize;
      firstrowid++;
      full = false;
      return res;
    }

    public REC getByRowid(int rowid) {
      int offset = rowid - firstrowid;
      return getRelative(offset);
    }

    public REC getRelative(int offset) {
      if (offset < 0 || offset > currsize())
        return null;
      int pos = (offset + firstpos) % buffersize;
      return buffer[pos];
    }

    public void setfirstrowid(int rowid) {
      this.firstrowid = rowid;
    }

    public int currsize() {
      if (full)
        return buffersize;
      return (buffersize + currentpos - firstpos) % buffersize;
    }

    public void reset() {
      this.full = false;
      this.firstpos = 0;
      this.currentpos = 0;
      this.firstrowid = 0;
    }
  }

  static class IndexedFile {
    transient protected Log LOG1 = LogFactory.getLog(this.getClass().getName());

    File tmpfile;
    FileChannel channel;
    int buffersize = 1024 * 1024;

    ByteBuffer buffer;

    long buffoff = 0;

    int buffrecid = 0;

    ArrayList<Long> index;
    int indexstep = 30;

    int firstrecid = 0;

    int currreadrecid = 0;

    int currentrecnum = 0;

    MappedByteBuffer buffer2 = null;
    int recnum_buf2 = 50;
    int firstrowid_buf2 = 0;
    int endrowid_buf2 = -1;
    long firstrow_pos_buf2 = 0;

    ByteBuffer result;

    public IndexedFile(int buffersize) {
      this.buffersize = buffersize;
      buffer = ByteBuffer.allocate(buffersize);
      index = new ArrayList<Long>();

      try {
        tmpfile = gentmpfile();
        channel = new RandomAccessFile(tmpfile, "rw").getChannel();
        channel.truncate(0);
        channel.force(true);
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
      result = ByteBuffer.allocate(1024 * 1024);
    }

    public void add(byte[] rec) {
      this.add(rec, 0, rec.length);
    }

    public void add(byte[] rec, int s, int l) {
      if (currentrecnum % indexstep == 0) {
        index.add(buffer.position() + buffoff);
      }
      if (buffer.remaining() < (l + 4)) {
        flushbuffer();
      }
      buffer.put(encode(l));
      buffer.put(rec, s, l);
      currentrecnum++;
    }

    public void flush() {
      flushbuffer();
    }

    public ByteBuffer get(int recid) {
      int rowid = recid - firstrecid;
      int indexid = rowid / indexstep;
      int indexoff = rowid % indexstep;
      if (rowid < firstrowid_buf2 || rowid > endrowid_buf2) {
        try {
          if (!resetbuffer2(indexid))
            return null;
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      int pos = (int) (index.get(indexid) - firstrow_pos_buf2);
      buffer2.position(pos);

      while (indexoff-- > 0) {
        skiprow(buffer2);
      }
      currreadrecid = recid;
      readrow(buffer2, result);
      return result;
    }

    public boolean seek(int recid) {
      int rowid = recid - firstrecid;
      int indexid = rowid / indexstep;
      int indexoff = rowid % indexstep;
      if (rowid < firstrowid_buf2 || rowid > endrowid_buf2) {
        try {
          if (!resetbuffer2(indexid))
            return false;
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      int pos = (int) (index.get(indexid) - firstrow_pos_buf2);
      buffer2.position(pos);

      while (indexoff-- > 0) {
        skiprow(buffer2);
      }
      currreadrecid = recid;
      return true;
    }

    public ByteBuffer next() {
      int rowid = currreadrecid - firstrecid;
      if (rowid > endrowid_buf2 || rowid < firstrowid_buf2) {
        try {
          int indexid = rowid / indexstep;
          if (!resetbuffer2(indexid))
            return null;
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      currreadrecid++;
      readrow(buffer2, result);
      return result;
    }

    public void setfirstrecid(int firstrowid2) {
      this.firstrecid = firstrowid2;
    }

    public void reset() {
      buffer.clear();
      index.clear();
      try {
        channel.truncate(0);
      } catch (IOException e) {
        e.printStackTrace();
      }
      buffoff = 0;
      buffrecid = 0;
      firstrecid = 0;
      currentrecnum = 0;
      currreadrecid = 0;

      buffer2 = null;
      firstrowid_buf2 = 0;
      endrowid_buf2 = -1;
      recnum_buf2 = 0;

      result.clear();
    }

    public void close() {
      this.reset();
      try {
        this.channel.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
      this.tmpfile.delete();
    }

    private File gentmpfile() throws IOException {
      File tmpd = new File(tmpfiledir);
      if (!tmpd.exists()) {
        tmpd.mkdirs();
      }
      Random r = new Random();
      String tmp = null;
      File tmpf = null;
      while (true) {
        tmp = String.valueOf(r.nextInt(100000));
        tmpf = new File(tmpd, tmp);
        if (!tmpf.exists()) {
          tmpf.createNewFile();
          break;
        }
      }
      tmpf.deleteOnExit();
      return tmpf;

    }

    private void readrow(ByteBuffer buf, ByteBuffer res) {
      int oripos = buf.position();
      byte[] data = new byte[4];
      data[0] = buf.get();
      int len = ((data[0] >> 6) & 0x03) + 1;
      for (int i = 1; i < len; i++) {
        data[i] = buf.get();
      }
      int l = decode(data, 0);
      if (l < 0x40) {
        buf.position(oripos + 1);
      } else if (l < 0x4000) {
        buf.position(oripos + 2);
      } else if (l < 0x400000) {
        buf.position(oripos + 3);
      } else if (l < 0x40000000) {
        buf.position(oripos + 4);
      }
      res.clear();
      buf.get(res.array(), 0, (int) l);
      res.position((int) l);
      res.flip();
    }

    private void skiprow(ByteBuffer buf) {
      int oripos = buf.position();
      byte[] data = new byte[4];
      data[0] = buf.get();
      int len = ((data[0] >> 6) & 0x03) + 1;
      for (int i = 1; i < len; i++) {
        data[i] = buf.get();
      }
      long l = decode(data, 0);
      if (l < 0x40) {
        buf.position(oripos + 1 + (int) l);
      } else if (l < 0x4000) {
        buf.position(oripos + 2 + (int) l);
      } else if (l < 0x400000) {
        buf.position(oripos + 3 + (int) l);
      } else if (l < 0x40000000) {
        buf.position(oripos + 4 + (int) l);
      }
    }

    private void flushbuffer() {
      buffer.flip();
      try {
        channel.write(buffer);
        channel.force(false);
        buffoff = channel.position();
        buffrecid = currentrecnum;
        buffer.clear();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    private boolean resetbuffer2(int indexid) throws IOException {
      firstrowid_buf2 = 0;
      firstrow_pos_buf2 = 0;
      endrowid_buf2 = currentrecnum - 1;
      long buffer2size = channel.size();
      buffer2 = channel.map(MapMode.READ_WRITE, firstrow_pos_buf2, buffer2size);
      return true;

    }

    private byte[] encode(int l) {
      byte[] res;
      if (l < 0x40) {
        res = new byte[1];
        res[0] = (byte) l;
        return res;
      }
      if (l < 0x4000) {
        res = new byte[2];
        res[1] = (byte) (l & 0xff);
        res[0] = (byte) (0x40 | (l >> 8) & 0xff);
        return res;
      }
      if (l < 0x400000) {
        res = new byte[3];
        res[2] = (byte) (l & 0xff);
        res[1] = (byte) (l >> 8 & 0xff);
        res[0] = (byte) (0x80 | (l >> 16) & 0xff);
        return res;
      }
      if (l < 0x40000000) {
        res = new byte[4];
        res[3] = (byte) (l & 0xff);
        res[2] = (byte) (l >> 8 & 0xff);
        res[1] = (byte) (l >> 16 & 0xff);
        res[0] = (byte) (0xc0 | (l >> 24) & 0xff);
        return res;
      }
      return null;
    }

    private int decode(byte[] data, int offset) {
      if ((0xc0 & data[offset + 0]) == 0) {
        return data[offset + 0];
      }
      if ((0xc0 & data[offset + 0]) == 0x40) {
        return ((0x3f & data[offset + 0]) << 8) + (data[offset + 1] & 0xff);
      }
      if ((0xc0 & data[offset + 0]) == 0x80) {
        return ((0x3f & data[offset + 0]) << 16)
            + ((data[offset + 1] & 0xff) << 8) + (data[offset + 2] & 0xff);
      }
      if ((0xc0 & data[offset + 0]) == 0xc0) {
        return ((0x3f & data[offset + 0]) << 24)
            + ((data[offset + 1] & 0xff) << 16)
            + ((data[offset + 2] & 0xff) << 8) + (data[offset + 3] & 0xff);
      }
      return -1;
    }

  }

  int rowsize = 100000;
  int filebuffersize = 4 * 1024 * 1024;

  Buffer<Row> membuffer;
  IndexedFile indexedFile = null;
  private SerDe serde;
  private ObjectInspector standardOI;

  int firstrowid = 0;

  int lastrowid = -1;
  boolean fileused = false;
  Configuration conf;
  static String tmpfiledir = null;

  public AnalysisBuffer(SerDe sd, ObjectInspector oi, Configuration conf) {
    this.conf = conf;
    this.serde = sd;
    this.standardOI = oi;
    tmpfiledir = HiveConf.getVar(conf, ConfVars.ANAFUNCTMPDIR);
    rowsize = conf.getInt("ana.membuffer.rowsize", rowsize);
    membuffer = new Buffer<Row>(rowsize);
    indexedFile = null;
  }

  public void add(Row row) {
    if (!membuffer.add(row)) {
      spillhalf();
      membuffer.add(row);
    }
    lastrowid++;
  }

  public Row removeFirst(boolean res) {
    Row row = null;
    if (fileused) {
      if (res) {
        ByteBuffer buf = indexedFile.get(firstrowid);
        row = deserialize(buf.array(), 0, buf.limit());
      }
      firstrowid++;
      if (firstrowid == membuffer.firstrowid) {
        fileused = false;
      }
    } else {
      row = membuffer.removeFirst();
      firstrowid++;
    }
    return row;
  }

  public Row getByRowid(int rowid) {
    if (rowid > lastrowid || rowid < firstrowid)
      return null;
    if (!fileused || rowid >= membuffer.firstrowid) {
      return membuffer.getByRowid(rowid);
    } else {
      ByteBuffer buf = indexedFile.get(rowid);
      return deserialize(buf.array(), 0, buf.limit());
    }
  }

  public Row getRelative(int offset) {
    return getByRowid(offset + firstrowid);
  }

  public int lastrowid() {
    return lastrowid;
  }

  int currentseekrowid = 0;

  public boolean seek(int rowid) {
    if (rowid > lastrowid || rowid < firstrowid)
      return false;
    currentseekrowid = rowid;
    if (fileused && rowid < membuffer.firstrowid) {
      return indexedFile.seek(rowid);
    }
    return true;
  }

  public Row next() {
    if (currentseekrowid > lastrowid || currentseekrowid < firstrowid)
      return null;
    if (!fileused || currentseekrowid >= membuffer.firstrowid) {
      return membuffer.getByRowid(currentseekrowid++);
    } else {
      currentseekrowid++;
      ByteBuffer buf = indexedFile.next();
      byte[] bytes = Arrays.copyOf(buf.array(), buf.limit());
      return deserialize(bytes, 0, bytes.length);
    }
  }

  public void reset() {
    firstrowid = 0;
    lastrowid = -1;
    fileused = false;
    membuffer.reset();
    if (indexedFile != null)
      indexedFile.close();
    indexedFile = null;
  }

  public void close() {
    firstrowid = 0;
    lastrowid = -1;
    fileused = false;
    membuffer.reset();
    if (indexedFile != null)
      indexedFile.close();
    indexedFile = null;
  }

  public Iterator<Row> iterator() {
    return new Iterator<Row>() {

      int currentrowid = firstrowid;

      @Override
      public boolean hasNext() {
        if (currentrowid <= lastrowid)
          return true;
        return false;
      }

      @Override
      public Row next() {
        return getByRowid(currentrowid++);
      }

      @Override
      public void remove() {

      }
    };
  }

  private void spillhalf() {
    if (!fileused) {
      if (indexedFile == null) {
        indexedFile = new IndexedFile(filebuffersize);
      }
      indexedFile.reset();
      indexedFile.setfirstrecid(firstrowid);
      fileused = true;
    }
    try {
      for (int i = 0; i < rowsize / 2; i++) {
        Row row = membuffer.removeFirst();
        if (row == null)
          break;
        indexedFile.add(serilize(row));
      }
    } catch (SerDeException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    indexedFile.flush();
  }

  private ByteArrayOutputStream baos = new ByteArrayOutputStream();
  private DataOutputStream oos = new DataOutputStream(baos);

  private byte[] serilize(Row row) throws SerDeException, IOException {
    baos.reset();
    if (serde != null && standardOI != null) {
      serde.serialize(row, standardOI).write(oos);
    } else {
    }
    oos.flush();
    return baos.toByteArray();
  }

  @SuppressWarnings("unchecked")
  private Row deserialize(byte[] rec, int off, int length) {
    Row ret = null;
    try {
      DataInputStream ois = new DataInputStream(new ByteArrayInputStream(rec));
      if (serde != null && standardOI != null) {
        Writable val = serde.getSerializedClass().newInstance();
        val.readFields(ois);
        ret = (Row) ObjectInspectorUtils.copyToStandardObject(
            serde.deserialize(val), serde.getObjectInspector(),
            ObjectInspectorCopyOption.WRITABLE);
      } else {
        ret = (Row) new Object();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return ret;
  }

  public static void main(String[] args) throws IOException {

    IndexedFile iif = new IndexedFile(1024);
    iif.setfirstrecid(0);
    for (int i = 0; i < 10000; i++) {

      String val = String.valueOf(i) + ",";
      iif.add(val.getBytes());
    }
    iif.flush();
    int x = 2000;
    System.out.println((char) iif.get(0).array()[0]);
    System.out.println((char) iif.get(x).array()[1]);
    System.out.println((char) iif.get(x).array()[2]);
    Random r = new Random();
    for (int i = 0; i < 20000; i++) {
      int n = r.nextInt(10000);
      System.out.println(i + "\t" + n);
      System.out.println((char) iif.get(n).array()[0]);
      System.out.println((char) iif.get(n).array()[1]);
      System.out.println((char) iif.get(n).array()[2]);
    }

  }
}
