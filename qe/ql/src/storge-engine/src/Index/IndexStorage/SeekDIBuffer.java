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
package IndexStorage;

import java.io.*;

public class SeekDIBuffer extends DataInputStream {
  private static class Buffer extends ByteArrayInputStream {
    public Buffer() {
      super(new byte[] {});
    }

    public void reset(byte[] input, int start, int length) {
      this.buf = input;
      this.count = start + length;
      this.mark = start;
      this.pos = start;
    }

    public byte[] getData() {
      return buf;
    }

    public int getPosition() {
      return pos;
    }

    public int getLength() {
      return count;
    }

    public boolean seek(int pos) {
      if (pos < 0 || pos >= count)
        return false;
      this.pos = pos;
      return true;
    }
  }

  private Buffer buffer;

  public SeekDIBuffer() {
    this(new Buffer());
  }

  private SeekDIBuffer(Buffer buffer) {
    super(buffer);
    this.buffer = buffer;
  }

  public void reset(byte[] input, int length) {
    buffer.reset(input, 0, length);
  }

  public void reset(byte[] input, int start, int length) {
    buffer.reset(input, start, length);
  }

  public byte[] getData() {
    return buffer.getData();
  }

  public int getPosition() {
    return buffer.getPosition();
  }

  public int getLength() {
    return buffer.getLength();
  }

  public boolean seek(int pos) {
    return buffer.seek(pos);
  }
}
