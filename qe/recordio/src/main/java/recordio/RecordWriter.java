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
package recordio;

import java.io.OutputStream;
import java.io.IOException;

import recordio.utils.Constants;
import recordio.utils.LittleEndian;
import recordio.utils.BlockType;
import recordio.utils.CRC32;
import recordio.utils.Varint;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.CodedOutputStream;

public class RecordWriter {

  protected static final int RECORD_SIZE_INIT = -1;
  protected static final int RECORD_SIZE_DIFFERENT = -2;

  protected OutputStream output = null;
  protected int recordSize = RECORD_SIZE_INIT;
  protected int recordCount = 0;

  protected byte[] recordSizeBuffer = null;
  protected byte[] recordBuffer = null;

  protected int recordSizeBufferPos = 0;
  protected int recordBufferPos = 0;

  protected int checksum = Constants.CRC32_INIT_VALUE;

  protected byte[] tmpBuffer = new byte[Constants.MAX_VARINT_SIZE_OF_RECORD_SIZE];
  protected byte[] msgBuffer = null;

  public RecordWriter(OutputStream output) {
    this.output = output;
  }

  public boolean write(byte[] buffer, int offset, int length)
      throws IOException {
    if (length > Constants.MAX_RECORD_SIZE) {
      return false;
    }
    if (recordSizeBuffer == null) {
      recordSizeBuffer = new byte[Constants.RECORD_SIZE_BUFFER_SIZE];
    }
    if (recordBuffer == null) {
      recordBuffer = new byte[Constants.RECORD_BUFFER_SIZE];
    }

    if (recordCount > 0) {
      if ((recordBufferPos + length) >= Constants.RECORD_BUFFER_SIZE
          || recordSizeBufferPos + Varint.size(length) >= Constants.RECORD_SIZE_BUFFER_SIZE) {
        flush();
      } else {
        if ((recordSize >= 0) && (recordSize != length)) {
          if (recordSizeBufferPos > (2 * (Constants.MAX_BLOCK_HEADER_SIZE + Constants.BLOCK_FOOTER_SIZE))) {
            flush();
          }
        }
      }
    }

    recordSizeBufferPos += Varint.encode(length, recordSizeBuffer,
        recordSizeBufferPos);
    ++recordCount;
    if ((recordBufferPos + length) >= Constants.RECORD_BUFFER_SIZE) {
      flushFromExternalBuffer(buffer, offset, length);
    } else {
      System.arraycopy(buffer, offset, recordBuffer, recordBufferPos, length);
      recordBufferPos += length;
      if (recordSize == RECORD_SIZE_INIT) {
        recordSize = length;
      } else if (recordSize != length) {
        recordSize = RECORD_SIZE_DIFFERENT;
      }
    }
    return true;
  }

  public boolean write(GeneratedMessage message) throws IOException {
    int msgSize = message.getSerializedSize();
    if (msgSize > Constants.MAX_RECORD_SIZE) {
      return false;
    }
    if (msgBuffer == null || msgBuffer.length < msgSize) {
      msgBuffer = new byte[msgSize];
    }
    CodedOutputStream cos = CodedOutputStream
        .newInstance(msgBuffer, 0, msgSize);
    message.writeTo(cos);
    cos.flush();
    return write(msgBuffer, 0, msgSize);
  }

  public void flush() throws IOException {
    if (recordCount > 0) {
      flushFromExternalBuffer(recordBuffer, 0, recordBufferPos);
    }
  }

  protected void flushFromExternalBuffer(byte[] buffer, int offset, int length)
      throws IOException {
    assert (recordCount > 0);
    assert (recordSizeBufferPos > 0);

    BlockType blockType = BlockType.INVALID;
    int blockBodySize = 0;
    if (recordCount == 1) {
      blockType = BlockType.SINGLE;
      blockBodySize = length;
    } else if (recordSize >= 0) {
      blockType = BlockType.FIXED;
      blockBodySize = Varint.size(recordSize) + length;
      if (recordSize == 0) {
        blockBodySize += Varint.size(recordCount);
      }
    } else {
      blockType = BlockType.VARIABLE;
      blockBodySize = Varint.size(recordSizeBufferPos) + recordSizeBufferPos
          + length;
    }

    checksum = Constants.CRC32_INIT_VALUE;
    writeBytes(Constants.BLOCK_HEADER_MAGIC, 0,
        Constants.BLOCK_HEADER_MAGIC.length, true);
    writeByte(blockType.getValue(), true);
    writeVarint(blockBodySize, true);

    short headerChecksum = CRC32.toCRC16(checksum);
    writeShort(headerChecksum, false);

    switch (blockType) {
    case FIXED:
      writeVarint(recordSize, true);
      if (recordSize == 0) {
        writeVarint(recordCount, true);
      }
      break;
    case VARIABLE:
      writeVarint(recordSizeBufferPos, true);
      writeBytes(recordSizeBuffer, 0, recordSizeBufferPos, true);
      break;
    case SINGLE:
      break;
    default:
      break;
    }
    writeBytes(buffer, offset, length, true);

    writeInt(checksum, false);
    output.flush();

    recordSize = RECORD_SIZE_INIT;
    recordCount = 0;
    recordSizeBufferPos = 0;
    recordBufferPos = 0;
  }

  protected void writeToStream(byte[] buffer, int offset, int length,
      boolean updateChecksum) throws IOException {
    if (length == 0) {
      return;
    }
    if (updateChecksum) {
      checksum = CRC32.update(buffer, offset, length, checksum);
    }
    output.write(buffer, offset, length);
  }

  protected void writeBytes(byte[] buffer, int offset, int length,
      boolean updateChecksum) throws IOException {
    writeToStream(buffer, offset, length, updateChecksum);
  }

  protected void writeVarint(int value, boolean updateChecksum)
      throws IOException {
    int length = Varint.encode(value, tmpBuffer, 0);
    writeToStream(tmpBuffer, 0, length, updateChecksum);
  }

  protected void writeInt(int value, boolean updateChecksum) throws IOException {
    LittleEndian.putInt(value, tmpBuffer, 0);
    writeToStream(tmpBuffer, 0, Constants.INT_SIZE, updateChecksum);
  }

  protected void writeShort(short value, boolean updateChecksum)
      throws IOException {
    LittleEndian.putShort(value, tmpBuffer, 0);
    writeToStream(tmpBuffer, 0, Constants.SHORT_SIZE, updateChecksum);
  }

  protected void writeByte(byte value, boolean updateChecksum)
      throws IOException {
    tmpBuffer[0] = value;
    writeToStream(tmpBuffer, 0, Constants.BYTE_SIZE, updateChecksum);
  }

  protected void finalize() {
    try {
      flush();
    } catch (IOException e) {
    }
  }
}
