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

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyStringObjectInspector;
import org.apache.hadoop.io.Text;

public class LazyString extends LazyPrimitive<LazyStringObjectInspector, Text> {

  public LazyString(LazyStringObjectInspector oi) {
    super(oi);
    data = new Text();
  }

  public LazyString(LazyString copy) {
    super(copy);
    data = new Text(copy.data);
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    if (oi.isGbkcoding()) {
      byte[] bytess;
      try {
        bytess = new String(bytes.getData(), start, length, "gbk")
            .getBytes("utf8");
        bytes = new ByteArrayRef();
        bytes.setData(bytess);
        start = 0;
        length = bytess.length;
      } catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      }
    }
    if (oi.isEscaped()) {
      byte escapeChar = oi.getEscapeChar();
      byte[] inputBytes = bytes.getData();

      int outputLength = 0;
      for (int i = 0; i < length; i++) {
        if (escapeChar == 1 && inputBytes[start + i] == 1)
          continue;
        if (inputBytes[start + i] != escapeChar) {
          outputLength++;
        } else {
          outputLength++;
          i++;
        }
      }

      data.set(bytes.getData(), start, outputLength);

      if (outputLength < length) {
        int k = 0;
        byte[] outputBytes = data.getBytes();
        for (int i = 0; i < length; i++) {
          byte b = inputBytes[start + i];
          if (escapeChar == 1 && b == 1)
            continue;
          if (b != escapeChar || i == length - 1) {
            outputBytes[k++] = b;
          } else {
            i++;
            outputBytes[k++] = inputBytes[start + i];
          }
        }
        assert (k == outputLength);
      }
    } else {
      data.set(bytes.getData(), start, length);
    }
  }

}
