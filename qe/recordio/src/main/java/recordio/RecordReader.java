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

import java.io.InputStream;
import java.io.IOException;

import recordio.utils.Constants;
import recordio.utils.LittleEndian;
import recordio.utils.BlockType;
import recordio.utils.CRC32;
import recordio.utils.Varint;

import com.google.protobuf.Message;
import com.google.protobuf.InvalidProtocolBufferException;

public class RecordReader {

  public static class Buffer {
    public byte[] buffer = null;
    public int offset = 0;
    public int length = 0;

    public Buffer() {
    }

    public Buffer(byte[] buffer, int offset, int length) {
      this.buffer = buffer;
      this.offset = offset;
      this.length = length;
    }

    public Buffer(Buffer rhs) {
      this(rhs.buffer, rhs.offset, rhs.length);
    }
  }

  public enum Options {
    DEFAULT(0x0000), RESUME_LAST_INCOMPLETE_BLOCK(0x0002);

    private Options(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }

    private int value;
  }

  protected enum State {
    START, VALID_HEADER, VALID_BLOCK;
  }

  protected InputStream input = null;
  protected int options = Options.DEFAULT.getValue();
  protected State state = State.START;

  protected BlockType blockType = BlockType.INVALID;
  protected int blockHeaderSize = 0;
  protected int blockBodySize = 0;
  protected int blockBodyEnd = 0;

  protected int fixedRecordSize = 0;
  protected int fixedRecordCount = 0;

  protected int varRecordSizePos = 0;
  protected int varRecordSizeEnd = 0;

  protected int checksum = Constants.CRC32_INIT_VALUE;

  protected byte[] buffer = null;
  protected int bufferEnd = 0;
  protected int bufferPos = 0;

  protected long bufferEndInStream = 0;
  protected long blockPosInStream = 0;

  protected long skippedBytes = 0;
  protected long accumulatedSkippedBytes = 0;

  protected long skippedRecords = 0;
  protected long accumulatedSkippedRecords = 0;

  public RecordReader(InputStream input) {
    this.input = input;
  }

  public RecordReader(InputStream input, int options) {
    this.input = input;
    this.options = options;
  }

  public Buffer read() throws IOException {
    skippedBytes = 0;
    long tmpAccumulatedSkippedBytes = accumulatedSkippedBytes;

    if (!isCurrentBlockAvailable()) {
      boolean available = tryReadNextBlock();
      skippedBytes = accumulatedSkippedBytes - tmpAccumulatedSkippedBytes;
      if (!available) {
        return null;
      }
    }

    int offset, length;
    switch (blockType) {
    case FIXED:
      offset = bufferPos;
      length = fixedRecordSize;
      if (fixedRecordSize == 0) {
        --fixedRecordCount;
      }
      break;
    case VARIABLE:
      int varintSize = 0;
      offset = bufferPos;
      length = Varint.decode(buffer, varRecordSizePos);
      varintSize = Varint.size(length);
      varRecordSizePos += varintSize;
      break;
    case SINGLE:
      offset = bufferPos;
      length = blockBodySize;
      break;
    default:
      return null;
    }

    bufferPos += length;
    return new Buffer(buffer, offset, length);
  }

  public boolean read(Message.Builder messageBuilder) throws IOException {
    skippedRecords = 0;
    Buffer b = null;
    while ((b = read()) != null) {
      try {
        messageBuilder.clear();
        messageBuilder.mergeFrom(b.buffer, b.offset, b.length);
        return true;
      } catch (InvalidProtocolBufferException e) {
        ++skippedRecords;
        ++accumulatedSkippedRecords;
      }
    }
    return false;
  }

  public long getSkippedBytes() {
    return skippedBytes;
  }

  public long getAccumulatedSkippedBytes() {
    return accumulatedSkippedBytes;
  }

  public long getSkippedRecords() {
    return skippedRecords;
  }

  public long getAccumulatedSkippedRecords() {
    return accumulatedSkippedRecords;
  }

  public long getUnconsumedBytes() {
    return bufferEnd - bufferPos;
  }

  public long getConsumedBytes() {
    return bufferPosInStream();
  }

  public boolean isBlockConsumed(long streamOffset) {
    if (bufferPosInStream() < streamOffset) {
      return false;
    }
    if (!isCurrentBlockAvailable()) {
      return true;
    }
    long blockEndPosInStream = blockPosInStream + blockHeaderSize
        + blockBodySize;
    if (blockEndPosInStream < streamOffset) {
      return true;
    } else {
      return false;
    }
  }

  protected boolean isCurrentBlockAvailable() {
    if (state != State.VALID_BLOCK) {
      return false;
    }

    boolean available = false;
    switch (blockType) {
    case FIXED:
      if (fixedRecordSize == 0) {
        available = fixedRecordCount > 0;
      } else {
        available = bufferPos < blockBodyEnd;
      }
      break;
    case VARIABLE:
      available = varRecordSizePos < varRecordSizeEnd;
      break;
    case SINGLE:
      available = bufferPos < blockBodyEnd;
      break;
    default:
      return false;
    }
    if (!available) {
      state = State.START;
      bufferPos += Constants.BLOCK_FOOTER_SIZE;
      blockBodyEnd = bufferPos;
    }
    return available;
  }

  protected boolean preloadBufferAtLeast(int bytesAtLeast) throws IOException {
    int bufferAvailable = bufferEnd - bufferPos;
    if (bufferAvailable < bytesAtLeast) {
      if (buffer == null || buffer.length < bytesAtLeast) {
        int newCapacity = computeBufferSize(bytesAtLeast);
        byte[] newBuffer = new byte[newCapacity];
        if (bufferAvailable > 0) {
          System.arraycopy(buffer, bufferPos, newBuffer, 0, bufferAvailable);
        }
        buffer = newBuffer;
        bufferPos = 0;
        bufferEnd = bufferPos + bufferAvailable;
      } else {
        int remainingCapacity = buffer.length - bufferEnd;
        if (bufferAvailable == 0 || remainingCapacity < bytesAtLeast) {
          if (bufferAvailable > 0) {
            System.arraycopy(buffer, bufferPos, buffer, 0, bufferAvailable);
          }
          bufferPos = 0;
          bufferEnd = bufferAvailable;
        }
      }

      int len = input.read(buffer, bufferEnd, buffer.length - bufferEnd);
      if (len == -1) {
        return false;
      }

      bufferEnd += len;
      bufferEndInStream += len;

      while ((bufferEnd - bufferPos) < bytesAtLeast) {
        len = input.read(buffer, bufferEnd, buffer.length - bufferEnd);
        if (len == -1) {
          return false;
        }

        bufferEnd += len;
        bufferEndInStream += len;
      }

      return (bufferEnd - bufferPos) >= bytesAtLeast;
    }
    return true;
  }

  protected boolean tryReadNextBlockHeader() throws IOException {
    while (true) {
      blockHeaderSize = 0;
      if (!preloadBufferAtLeast(Constants.MIN_BLOCK_SIZE)) {
        return false;
      }
      boolean foundHeaderMagic = false;
      while ((bufferPos + Constants.BLOCK_HEADER_MAGIC.length) <= bufferEnd) {
        if (checkHeaderMagick()) {
          if (!preloadBufferAtLeast(Constants.MIN_BLOCK_SIZE)) {
            return false;
          }
          blockPosInStream = bufferPosInStream();
          bufferPos += Constants.BLOCK_HEADER_MAGIC.length;
          foundHeaderMagic = true;
          blockHeaderSize += Constants.BLOCK_HEADER_MAGIC.length;
          break;
        }
        ++bufferPos;
        ++accumulatedSkippedBytes;
      }
      if (!foundHeaderMagic) {
        continue;
      }

      checksum = CRC32.update(Constants.BLOCK_HEADER_MAGIC, 0,
          Constants.BLOCK_HEADER_MAGIC.length, Constants.CRC32_INIT_VALUE);

      int tempBufferPos = bufferPos;
      blockType = BlockType.getBlockType(buffer[tempBufferPos++]);
      if (blockType == BlockType.INVALID) {
        accumulatedSkippedBytes += Constants.BLOCK_HEADER_MAGIC.length;
        continue;
      }
      blockHeaderSize += Constants.BLOCK_HEADER_TYPE_SIZE;

      int varintSize = 0;
      blockBodySize = Varint.decode(buffer, tempBufferPos);
      varintSize = Varint.size(blockBodySize);
      if (varintSize <= 0 || blockBodySize > Constants.MAX_RECORD_SIZE) {
        accumulatedSkippedBytes += Constants.BLOCK_HEADER_MAGIC.length;
        continue;
      }
      tempBufferPos += varintSize;
      blockHeaderSize += varintSize;

      checksum = CRC32.update(buffer, bufferPos, tempBufferPos - bufferPos,
          checksum);
      short headerChecksum = LittleEndian.getShort(buffer, tempBufferPos);
      tempBufferPos += Constants.BLOCK_HEADER_CHECKSUM_SIZE;
      if (headerChecksum != CRC32.toCRC16(checksum)) {
        accumulatedSkippedBytes += Constants.BLOCK_HEADER_MAGIC.length;
        continue;
      }
      blockHeaderSize += Constants.BLOCK_HEADER_CHECKSUM_SIZE;

      bufferPos = tempBufferPos;
      state = State.VALID_HEADER;
      return true;
    }
  }

  protected boolean tryReadNextBlock() throws IOException {
    while (true) {
      if (state != State.VALID_HEADER) {
        if (!tryReadNextBlockHeader()) {
          if ((options & Options.RESUME_LAST_INCOMPLETE_BLOCK.getValue()) == 0) {
            accumulatedSkippedBytes += bufferEnd - bufferPos;
            bufferPos = bufferEnd;
          }
          return false;
        }
      }

      if (!preloadBufferAtLeast(blockBodySize + Constants.BLOCK_FOOTER_SIZE)) {
        if ((options & Options.RESUME_LAST_INCOMPLETE_BLOCK.getValue()) != 0) {
          return false;
        } else {
          accumulatedSkippedBytes += bufferPosInStream() - blockPosInStream;
          state = State.START;
          continue;
        }
      }

      checksum = CRC32.update(buffer, bufferPos, blockBodySize, checksum);
      blockBodyEnd = bufferPos + blockBodySize;

      int blockChecksum = LittleEndian.getInt(buffer, blockBodyEnd);
      if (checksum != blockChecksum) {
        accumulatedSkippedBytes += bufferEndInStream - blockPosInStream;
        state = State.START;
        continue;
      }

      switch (blockType) {
      case FIXED: {
        int varintSize = 0;
        fixedRecordSize = Varint.decode(buffer, bufferPos);
        varintSize = Varint.size(fixedRecordSize);
        bufferPos += varintSize;
        if (fixedRecordSize == 0) {
          fixedRecordCount = Varint.decode(buffer, bufferPos);
          varintSize = Varint.size(fixedRecordCount);
          bufferPos += varintSize;
        }
        break;
      }
      case VARIABLE: {
        int allRecordBytes = Varint.decode(buffer, bufferPos);
        int varintSize = Varint.size(allRecordBytes);
        bufferPos += varintSize;
        varRecordSizePos = bufferPos;
        bufferPos += allRecordBytes;
        varRecordSizeEnd = bufferPos;
        break;
      }
      case SINGLE:
      default:
        break;
      }
      state = State.VALID_BLOCK;
      return true;
    }
  }

  protected boolean checkHeaderMagick() {
    int offset = 0;
    for (int i = 0; i < Constants.BLOCK_HEADER_MAGIC.length; i++, offset++) {
      if (buffer[bufferPos + offset] != Constants.BLOCK_HEADER_MAGIC[i]) {
        return false;
      }
    }
    return true;
  }

  protected int computeBufferSize(int bytesAtLeast) {
    int bufferCapacity = (bytesAtLeast + Constants.BUFFER_BLOCK_SIZE - 1)
        / Constants.BUFFER_BLOCK_SIZE * Constants.BUFFER_BLOCK_SIZE;
    if (bufferCapacity < (Constants.RECORD_SIZE_BUFFER_SIZE + Constants.RECORD_BUFFER_SIZE)) {
      bufferCapacity = Constants.RECORD_SIZE_BUFFER_SIZE
          + Constants.RECORD_BUFFER_SIZE;
    }
    return bufferCapacity;
  }

  protected long bufferPosInStream() {
    return bufferEndInStream - (bufferEnd - bufferPos);
  }
}
