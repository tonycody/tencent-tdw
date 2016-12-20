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
package StorageEngineClient;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

public class MyLineReader {
  private static final int DEFAULT_BUFFER_SIZE = 64 * 1024;
  private int bufferSize = DEFAULT_BUFFER_SIZE;
  private InputStream in;
  private byte[] buffer;
  private int bufferLength = 0;
  private int bufferPosn = 0;

  private static final byte CR = '\r';
  private static final byte LF = '\n';

  private int lineendmode = 1;

  public MyLineReader(InputStream in) {
    this(in, DEFAULT_BUFFER_SIZE);
  }

  public MyLineReader(InputStream in, int bufferSize) {
    this.in = in;
    this.bufferSize = bufferSize;
    this.buffer = new byte[this.bufferSize];
  }

  public MyLineReader(InputStream in, Configuration conf) throws IOException {
    this(in, conf.getInt("io.file.buffer.size", DEFAULT_BUFFER_SIZE));
    lineendmode = conf.getInt("hive.textline.seperator.mode", 1);
  }

  public void close() throws IOException {
    in.close();
  }

  public int readLine(Text str, int maxLineLength, int maxBytesToConsume)
      throws IOException {

    str.clear();
    int txtLength = 0;
    int newlineLength = 0;
    boolean prevCharCR = false;
    long bytesConsumed = 0;
    do {
      int startPosn = bufferPosn;
      if (bufferPosn >= bufferLength) {
        startPosn = bufferPosn = 0;
        if (prevCharCR)
          ++bytesConsumed;
        bufferLength = in.read(buffer);
        if (bufferLength <= 0)
          break;
      }
      for (; bufferPosn < bufferLength; ++bufferPosn) {
        if (buffer[bufferPosn] == LF) {
          newlineLength = (prevCharCR && lineendmode != 2) ? 2 : 1;
          ++bufferPosn;
          break;
        }
        if (prevCharCR) {
          if (lineendmode == 0) {
            newlineLength = 1;
            break;
          }
        }
        prevCharCR = (buffer[bufferPosn] == CR);
      }
      int readLength = bufferPosn - startPosn;
      if (prevCharCR && newlineLength == 0)
        --readLength;
      bytesConsumed += readLength;
      int appendLength = readLength - newlineLength;
      if (appendLength > maxLineLength - txtLength) {
        appendLength = maxLineLength - txtLength;
      }
      if (appendLength > 0) {
        str.append(buffer, startPosn, appendLength);
        txtLength += appendLength;
      }
    } while (newlineLength == 0 && bytesConsumed < maxBytesToConsume);

    if (bytesConsumed > (long) Integer.MAX_VALUE)
      throw new IOException("Too many bytes before newline: " + bytesConsumed);
    return (int) bytesConsumed;
  }

  public int readLine(Text str, int maxLineLength) throws IOException {
    return readLine(str, maxLineLength, Integer.MAX_VALUE);
  }

  public int readLine(Text str) throws IOException {
    return readLine(str, Integer.MAX_VALUE, Integer.MAX_VALUE);
  }

}
