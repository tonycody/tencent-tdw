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
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.Text;

public class LazyUtils {

  public static int digit(int b, int radix) {
    int r = -1;
    if (b >= '0' && b <= '9') {
      r = b - '0';
    } else if (b >= 'A' && b <= 'Z') {
      r = b - 'A' + 10;
    } else if (b >= 'a' && b <= 'z') {
      r = b - 'a' + 10;
    }
    if (r >= radix)
      r = -1;
    return r;
  }

  public static int compare(byte[] b1, int start1, int length1, byte[] b2,
      int start2, int length2) {

    int min = Math.min(length1, length2);

    for (int i = 0; i < min; i++) {
      if (b1[start1 + i] == b2[start2 + i]) {
        continue;
      }
      if (b1[start1 + i] < b2[start2 + i]) {
        return -1;
      } else {
        return 1;
      }
    }

    if (length1 < length2)
      return -1;
    if (length1 > length2)
      return 1;
    return 0;
  }

  public static String convertToString(byte[] bytes, int start, int length) {
    try {
      return Text.decode(bytes, start, length);
    } catch (CharacterCodingException e) {
      return null;
    }
  }

  static byte[] trueBytes = { (byte) 't', 'r', 'u', 'e' };
  static byte[] falseBytes = { (byte) 'f', 'a', 'l', 's', 'e' };

  private static void writeEscaped(OutputStream out, byte[] bytes, int start,
      int len, boolean escaped, byte escapeChar, boolean[] needsEscape)
      throws IOException {
    if (escaped) {
      int end = start + len;
      for (int i = start; i <= end; i++) {
        if (i == end || (bytes[i] >= 0 && needsEscape[bytes[i]])) {
          if (i > start) {
            out.write(bytes, start, i - start);
          }
          start = i;
          if (i < len) {
            out.write(escapeChar);
          }
        }
      }
    } else {
      out.write(bytes, 0, len);
    }
  }

  public static void writePrimitiveUTF8(OutputStream out, Object o,
      PrimitiveObjectInspector oi, boolean escaped, byte escapeChar,
      boolean[] needsEscape) throws IOException {

    switch (oi.getPrimitiveCategory()) {
    case BOOLEAN: {
      boolean b = ((BooleanObjectInspector) oi).get(o);
      if (b) {
        out.write(trueBytes, 0, trueBytes.length);
      } else {
        out.write(falseBytes, 0, falseBytes.length);
      }
      break;
    }
    case BYTE: {
      LazyInteger.writeUTF8(out, ((ByteObjectInspector) oi).get(o));
      break;
    }
    case SHORT: {
      LazyInteger.writeUTF8(out, ((ShortObjectInspector) oi).get(o));
      break;
    }
    case INT: {
      LazyInteger.writeUTF8(out, ((IntObjectInspector) oi).get(o));
      break;
    }
    case LONG: {
      LazyLong.writeUTF8(out, ((LongObjectInspector) oi).get(o));
      break;
    }
    case FLOAT: {
      float f = ((FloatObjectInspector) oi).get(o);
      ByteBuffer b = Text.encode(String.valueOf(f));
      out.write(b.array(), 0, b.limit());
      break;
    }
    case DOUBLE: {
      double d = ((DoubleObjectInspector) oi).get(o);
      ByteBuffer b = Text.encode(String.valueOf(d));
      out.write(b.array(), 0, b.limit());
      break;
    }
    case STRING: {
      Text t = ((StringObjectInspector) oi).getPrimitiveWritableObject(o);
      writeEscaped(out, t.getBytes(), 0, t.getLength(), escaped, escapeChar,
          needsEscape);
      break;
    }
    case TIMESTAMP: {
      LazyTimestamp.writeUTF8(out,
          ((TimestampObjectInspector) oi).getPrimitiveWritableObject(o));
      break;
    }
    default: {
      throw new RuntimeException("Hive internal error.\t"
          + oi.getPrimitiveCategory());
    }
    }
  }

}
