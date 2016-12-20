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
package org.apache.hadoop.hive.ql.udf;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@description(name = "conv", value = "_FUNC_(num, from_base, to_base) - convert num from from_base to"
    + " to_base", extended = "If to_base is negative, treat num as a signed integer,"
    + "otherwise, treat it as an unsigned integer.\n"
    + "Example:\n"
    + "  > SELECT _FUNC_('100', 2, 10) FROM src LIMIT 1;\n"
    + "  '4'\n"
    + "  > SELECT _FUNC_(-10, 16, -10) FROM src LIMIT 1;\n" + "  '16'")
public class UDFConv extends UDF {
  private Text result = new Text();
  private byte[] value = new byte[64];

  private static Log LOG = LogFactory.getLog(UDFConv.class.getName());

  private long unsignedLongDiv(long x, int m) {
    if (x >= 0) {
      return x / m;
    }

    return x / m + 2 * (Long.MAX_VALUE / m) + 2 / m
        + (x % m + 2 * (Long.MAX_VALUE % m) + 2 % m) / m;
  }

  private void decode(long val, int radix) {
    Arrays.fill(value, (byte) 0);
    for (int i = value.length - 1; val != 0; i--) {
      long q = unsignedLongDiv(val, radix);
      value[i] = (byte) (val - q * radix);
      val = q;
    }
  }

  private long encode(int radix) {
    long val = 0;
    long bound = unsignedLongDiv(-1 - radix, radix);
    for (int i = 0; i < value.length && value[i] >= 0; i++) {
      if (val >= bound) {
        if (unsignedLongDiv(-1 - value[i], radix) < val) {
          return -1;
        }
      }
      val = val * radix + value[i];
    }
    return val;
  }

  private void byte2char(int radix, int fromPos) {
    for (int i = fromPos; i < value.length; i++) {
      value[i] = (byte) Character.toUpperCase(Character.forDigit(value[i],
          radix));
    }
  }

  private void char2byte(int radix, int fromPos) {
    for (int i = fromPos; i < value.length; i++) {
      value[i] = (byte) Character.digit(value[i], radix);
    }
  }

  public Text evaluate(Text n, IntWritable fromBase, IntWritable toBase) {
    if (n == null || n.toString().equalsIgnoreCase("") || fromBase == null
        || toBase == null) {
      return null;
    }

    int fromBs = fromBase.get();
    int toBs = toBase.get();
    if (fromBs < Character.MIN_RADIX || fromBs > Character.MAX_RADIX
        || Math.abs(toBs) < Character.MIN_RADIX
        || Math.abs(toBs) > Character.MAX_RADIX) {
      return null;
    }

    byte[] num = n.getBytes();
    boolean negative = (num[0] == '-');
    int first = 0;
    if (negative) {
      first = 1;
    }
    Arrays.fill(value, (byte) 0);

    for (int i = 1; i <= n.getLength() - first; i++) {
      value[value.length - i] = num[n.getLength() - i];
    }
    char2byte(fromBs, value.length - n.getLength() + first);

    long val = encode(fromBs);
    if (negative && toBs > 0) {
      if (val < 0) {
        val = -1;
      } else {
        val = -val;
      }
    }
    if (toBs < 0 && val < 0) {
      val = -val;
      negative = true;
    }
    decode(val, Math.abs(toBs));

    for (first = 0; first < value.length - 1 && value[first] == 0; first++)
      ;

    byte2char(Math.abs(toBs), first);

    if (negative && toBs < 0) {
      value[--first] = '-';
    }

    result.set(value, first, value.length - first);
    return result;
  }
}
