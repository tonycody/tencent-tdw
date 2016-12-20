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
package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

public class UDFUinChange extends UDF {

  public String evaluate(String str) {
    if (str == null)
      return null;
    try {
      return "" + (changeuin(java.lang.Long.valueOf(str)));
    } catch (NumberFormatException e) {
      return null;
    }
  }

  public String evaluate(LongWritable wUin) {
    if (wUin == null)
      return null;
    long uin = wUin.get();
    return "" + (changeuin(uin));
  }

  public String evaluate(IntWritable wUin) {
    if (wUin == null)
      return null;
    long uin = wUin.get();
    return "" + (changeuin(uin));
  }

  private static long setbit(long a, long bitnum, long value) {
    long tmpl = (a >> (bitnum + 1)) << (bitnum + 1);
    long tmpm = value << bitnum;
    long tmpr = ((1 << bitnum) - 1) & a;

    return tmpl + tmpm + tmpr;
  }

  private static long getbit(long a, long bitnum) {
    return (a >> bitnum) & 0x1;
  }

  private static long reversebit(long a, long bitnum) {
    if (getbit(a, bitnum) == 1) {
      return setbit(a, bitnum, 0);
    } else {
      return setbit(a, bitnum, 1);
    }
  }

  private static long changeuin(long uin) {
    long b30 = getbit(uin, 30);
    long b1 = getbit(uin, 1);
    uin = setbit(uin, 30, b1);
    uin = setbit(uin, 1, b30);
    uin = reversebit(uin, 0);
    uin = reversebit(uin, 3);
    uin = reversebit(uin, 11);
    uin = reversebit(uin, 19);
    uin = reversebit(uin, 27);
    return uin;
  }

}
