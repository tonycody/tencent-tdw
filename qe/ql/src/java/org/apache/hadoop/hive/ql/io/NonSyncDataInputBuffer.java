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

package org.apache.hadoop.hive.ql.io;

import java.io.DataInput;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.PushbackInputStream;
import java.io.UTFDataFormatException;

import org.apache.hadoop.hive.common.io.NonSyncByteArrayInputStream;

public class NonSyncDataInputBuffer extends FilterInputStream implements
    DataInput {

  private NonSyncByteArrayInputStream buffer;

  byte[] buff;

  public NonSyncDataInputBuffer() {
    this(new NonSyncByteArrayInputStream());
  }

  private NonSyncDataInputBuffer(NonSyncByteArrayInputStream buffer) {
    super(buffer);
    this.buffer = buffer;
  }

  public void reset(byte[] input, int length) {
    buffer.reset(input, 0, length);
  }

  public void reset(byte[] input, int start, int length) {
    buffer.reset(input, start, length);
  }

  public int getPosition() {
    return buffer.getPosition();
  }

  public int getLength() {
    return buffer.getLength();
  }

  @Override
  public final int read(byte[] buffer) throws IOException {
    return in.read(buffer, 0, buffer.length);
  }

  @Override
  public final int read(byte[] buffer, int offset, int length)
      throws IOException {
    return in.read(buffer, offset, length);
  }

  public final boolean readBoolean() throws IOException {
    int temp = in.read();
    if (temp < 0) {
      throw new EOFException();
    }
    return temp != 0;
  }

  public final byte readByte() throws IOException {
    int temp = in.read();
    if (temp < 0) {
      throw new EOFException();
    }
    return (byte) temp;
  }

  private int readToBuff(int count) throws IOException {
    int offset = 0;

    while (offset < count) {
      int bytesRead = in.read(buff, offset, count - offset);
      if (bytesRead == -1)
        return bytesRead;
      offset += bytesRead;
    }
    return offset;
  }

  public final char readChar() throws IOException {
    if (readToBuff(2) < 0) {
      throw new EOFException();
    }
    return (char) (((buff[0] & 0xff) << 8) | (buff[1] & 0xff));

  }

  public final double readDouble() throws IOException {
    return Double.longBitsToDouble(readLong());
  }

  public final float readFloat() throws IOException {
    return Float.intBitsToFloat(readInt());
  }

  public final void readFully(byte[] buffer) throws IOException {
    readFully(buffer, 0, buffer.length);
  }

  public final void readFully(byte[] buffer, int offset, int length)
      throws IOException {
    if (length < 0) {
      throw new IndexOutOfBoundsException();
    }
    if (length == 0) {
      return;
    }
    if (in == null || buffer == null) {
      throw new NullPointerException("Null Pointer to underlying input stream");
    }

    if (offset < 0 || offset > buffer.length - length) {
      throw new IndexOutOfBoundsException();
    }
    while (length > 0) {
      int result = in.read(buffer, offset, length);
      if (result < 0) {
        throw new EOFException();
      }
      offset += result;
      length -= result;
    }
  }

  public final int readInt() throws IOException {
    if (readToBuff(4) < 0) {
      throw new EOFException();
    }
    return ((buff[0] & 0xff) << 24) | ((buff[1] & 0xff) << 16)
        | ((buff[2] & 0xff) << 8) | (buff[3] & 0xff);
  }

  @Deprecated
  public final String readLine() throws IOException {
    StringBuffer line = new StringBuffer(80);
    boolean foundTerminator = false;
    while (true) {
      int nextByte = in.read();
      switch (nextByte) {
      case -1:
        if (line.length() == 0 && !foundTerminator) {
          return null;
        }
        return line.toString();
      case (byte) '\r':
        if (foundTerminator) {
          ((PushbackInputStream) in).unread(nextByte);
          return line.toString();
        }
        foundTerminator = true;
        /* Have to be able to peek ahead one byte */
        if (!(in.getClass() == PushbackInputStream.class)) {
          in = new PushbackInputStream(in);
        }
        break;
      case (byte) '\n':
        return line.toString();
      default:
        if (foundTerminator) {
          ((PushbackInputStream) in).unread(nextByte);
          return line.toString();
        }
        line.append((char) nextByte);
      }
    }
  }

  public final long readLong() throws IOException {
    if (readToBuff(8) < 0) {
      throw new EOFException();
    }
    int i1 = ((buff[0] & 0xff) << 24) | ((buff[1] & 0xff) << 16)
        | ((buff[2] & 0xff) << 8) | (buff[3] & 0xff);
    int i2 = ((buff[4] & 0xff) << 24) | ((buff[5] & 0xff) << 16)
        | ((buff[6] & 0xff) << 8) | (buff[7] & 0xff);

    return ((i1 & 0xffffffffL) << 32) | (i2 & 0xffffffffL);
  }

  public final short readShort() throws IOException {
    if (readToBuff(2) < 0) {
      throw new EOFException();
    }
    return (short) (((buff[0] & 0xff) << 8) | (buff[1] & 0xff));
  }

  public final int readUnsignedByte() throws IOException {
    int temp = in.read();
    if (temp < 0) {
      throw new EOFException();
    }
    return temp;
  }

  public final int readUnsignedShort() throws IOException {
    if (readToBuff(2) < 0) {
      throw new EOFException();
    }
    return (char) (((buff[0] & 0xff) << 8) | (buff[1] & 0xff));
  }

  public final String readUTF() throws IOException {
    return decodeUTF(readUnsignedShort());
  }

  String decodeUTF(int utfSize) throws IOException {
    return decodeUTF(utfSize, this);
  }

  private static String decodeUTF(int utfSize, DataInput in) throws IOException {
    byte[] buf = new byte[utfSize];
    char[] out = new char[utfSize];
    in.readFully(buf, 0, utfSize);

    return convertUTF8WithBuf(buf, out, 0, utfSize);
  }

  public static final String readUTF(DataInput in) throws IOException {
    return decodeUTF(in.readUnsignedShort(), in);
  }

  public final int skipBytes(int count) throws IOException {
    int skipped = 0;
    long skip;
    while (skipped < count && (skip = in.skip(count - skipped)) != 0) {
      skipped += skip;
    }
    if (skipped < 0) {
      throw new EOFException();
    }
    return skipped;
  }

  public static String convertUTF8WithBuf(byte[] buf, char[] out, int offset,
      int utfSize) throws UTFDataFormatException {
    int count = 0, s = 0, a;
    while (count < utfSize) {
      if ((out[s] = (char) buf[offset + count++]) < '\u0080')
        s++;
      else if (((a = out[s]) & 0xe0) == 0xc0) {
        if (count >= utfSize)
          throw new UTFDataFormatException();
        int b = buf[count++];
        if ((b & 0xC0) != 0x80)
          throw new UTFDataFormatException();
        out[s++] = (char) (((a & 0x1F) << 6) | (b & 0x3F));
      } else if ((a & 0xf0) == 0xe0) {
        if (count + 1 >= utfSize)
          throw new UTFDataFormatException();
        int b = buf[count++];
        int c = buf[count++];
        if (((b & 0xC0) != 0x80) || ((c & 0xC0) != 0x80))
          throw new UTFDataFormatException();
        out[s++] = (char) (((a & 0x0F) << 12) | ((b & 0x3F) << 6) | (c & 0x3F));
      } else {
        throw new UTFDataFormatException();
      }
    }
    return new String(out, 0, s);
  }

}
