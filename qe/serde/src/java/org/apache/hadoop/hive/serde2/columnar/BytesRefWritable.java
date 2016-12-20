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

package org.apache.hadoop.hive.serde2.columnar;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

public class BytesRefWritable implements Writable, Comparable<BytesRefWritable> {

  private static final byte[] EMPTY_BYTES = new byte[0];
  public static BytesRefWritable ZeroBytesRefWritable = new BytesRefWritable();

  int start = 0;
  int length = 0;
  byte[] bytes = null;

  LazyDecompressionCallback lazyDecompressObj;

  public BytesRefWritable() {
    this(EMPTY_BYTES);
  }

  public BytesRefWritable(int length) {
    assert length > 0;
    this.length = length;
    bytes = new byte[this.length];
    start = 0;
  }

  public BytesRefWritable(byte[] bytes) {
    this.bytes = bytes;
    length = bytes.length;
    start = 0;
  }

  public BytesRefWritable(byte[] data, int offset, int len) {
    bytes = data;
    start = offset;
    length = len;
  }

  public BytesRefWritable(LazyDecompressionCallback lazyDecompressData,
      int offset, int len) {
    lazyDecompressObj = lazyDecompressData;
    start = offset;
    length = len;
  }

  private void lazyDecompress() throws IOException {
    if (bytes == null && lazyDecompressObj != null) {
      bytes = lazyDecompressObj.decompress();
    }
  }

  public byte[] getBytesCopy() throws IOException {
    lazyDecompress();
    byte[] bb = new byte[length];
    System.arraycopy(bytes, start, bb, 0, length);
    return bb;
  }

  public byte[] getData() throws IOException {
    lazyDecompress();
    return bytes;
  }

  public void set(byte[] newData, int offset, int len) {
    bytes = newData;
    start = offset;
    length = len;
    lazyDecompressObj = null;
  }

  public void set(LazyDecompressionCallback newData, int offset, int len) {
    bytes = null;
    start = offset;
    length = len;
    lazyDecompressObj = newData;
  }

  public void writeDataTo(DataOutput out) throws IOException {
    lazyDecompress();
    out.write(bytes, start, length);
  }

  public void readFields(DataInput in) throws IOException {
    int len = in.readInt();
    if (len > bytes.length) {
      bytes = new byte[len];
    }
    start = 0;
    length = len;
    in.readFully(bytes, start, length);
  }

  public void write(DataOutput out) throws IOException {
    lazyDecompress();
    out.writeInt(length);
    out.write(bytes, start, length);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(3 * length);
    for (int idx = start; idx < length; idx++) {
      if (idx != 0) {
        sb.append(' ');
      }
      String num = Integer.toHexString(0xff & bytes[idx]);
      if (num.length() < 2) {
        sb.append('0');
      }
      sb.append(num);
    }
    return sb.toString();
  }

  @Override
  public int compareTo(BytesRefWritable other) {
    if (other == null) {
      throw new IllegalArgumentException("Argument can not be null.");
    }
    if (this == other) {
      return 0;
    }
    try {
      return WritableComparator.compareBytes(getData(), start, getLength(),
          other.getData(), other.start, other.getLength());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean equals(Object right_obj) {
    if (right_obj == null || !(right_obj instanceof BytesRefWritable)) {
      return false;
    }
    return compareTo((BytesRefWritable) right_obj) == 0;
  }

  static {
    WritableFactories.setFactory(BytesRefWritable.class, new WritableFactory() {

      @Override
      public Writable newInstance() {
        return new BytesRefWritable();
      }

    });
  }

  public int getLength() {
    return length;
  }

  public int getStart() {
    return start;
  }
}
