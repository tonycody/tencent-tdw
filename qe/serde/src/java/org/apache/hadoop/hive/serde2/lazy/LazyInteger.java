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
package org.apache.hadoop.hive.serde2.lazy;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyIntObjectInspector;
import org.apache.hadoop.io.IntWritable;

public class LazyInteger extends
    LazyPrimitive<LazyIntObjectInspector, IntWritable> {

  public LazyInteger(LazyIntObjectInspector oi) {
    super(oi);
    data = new IntWritable();
  }

  public LazyInteger(LazyInteger copy) {
    super(copy);
    data = new IntWritable(copy.data.get());
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    try {
      data.set(parseInt(bytes.getData(), start, length, 10));
      isNull = false;
    } catch (NumberFormatException e) {
      isNull = true;
    }
  }

  public static int parseInt(byte[] bytes, int start, int length)
      throws NumberFormatException {
    return parseInt(bytes, start, length, 10);
  }

  public static int parseInt(byte[] bytes, int start, int length, int radix)
      throws NumberFormatException {
    if (bytes == null) {
      throw new NumberFormatException("String is null");
    }
    if (radix < Character.MIN_RADIX || radix > Character.MAX_RADIX) {
      throw new NumberFormatException("Invalid radix: " + radix);
    }
    if (length == 0) {
      throw new NumberFormatException("Empty string!");
    }
    int offset = start;
    boolean negative = bytes[start] == '-';
    if (negative || bytes[start] == '+') {
      offset++;
      if (length == 1) {
        throw new NumberFormatException(LazyUtils.convertToString(bytes, start,
            length));
      }
    }

    return parse(bytes, start, length, offset, radix, negative);
  }

  private static int parse(byte[] bytes, int start, int length, int offset,
      int radix, boolean negative) throws NumberFormatException {
    int max = Integer.MIN_VALUE / radix;
    int result = 0, end = start + length;
    while (offset < end) {
      int digit = LazyUtils.digit(bytes[offset++], radix);
      if (digit == -1) {
        throw new NumberFormatException(LazyUtils.convertToString(bytes, start,
            length));
      }
      if (max > result) {
        throw new NumberFormatException(LazyUtils.convertToString(bytes, start,
            length));
      }
      int next = result * radix - digit;
      if (next > result) {
        throw new NumberFormatException(LazyUtils.convertToString(bytes, start,
            length));
      }
      result = next;
    }
    if (!negative) {
      result = -result;
      if (result < 0) {
        throw new NumberFormatException(LazyUtils.convertToString(bytes, start,
            length));
      }
    }
    return result;
  }

  public static void writeUTF8(OutputStream out, int i) throws IOException {
    if (i == 0) {
      out.write('0');
      return;
    }

    boolean negative = i < 0;
    if (negative) {
      out.write('-');
    } else {
      i = -i;
    }

    int start = 1000000000;
    while (i / start == 0) {
      start /= 10;
    }

    while (start > 0) {
      out.write('0' - (i / start % 10));
      start /= 10;
    }
  }

  public static void writeUTF8NoException(OutputStream out, int i) {
    try {
      writeUTF8(out, i);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
