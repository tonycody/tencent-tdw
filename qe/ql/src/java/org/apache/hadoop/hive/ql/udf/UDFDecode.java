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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.hive.ql.exec.description;

@description(name = "decode", value = "_FUNC_(cond,expr1,ret1...exprn,retn) - Returns ret1 if cond equal expr1, etc.", extended = "Example:\n"
    + "  > SELECT _FUNC_(1, 1,3, 2,4) FROM src LIMIT 1;\n")
public class UDFDecode extends UDF {

  private static Log LOG = LogFactory.getLog(UDFDecode.class.getName());

  private class BooleanPair {
    Boolean expr;
    Boolean ret;
  }

  private class BytePair {
    Byte expr;
    Byte ret;
  }

  private class ShortPair {
    Short expr;
    Short ret;
  }

  public Integer evaluate(Integer cond, Integer... params) {
    if (params.length == 0) {
      return null;
    }
    if (params.length % 2 != 0) {
      return null;
    }

    int pair = params.length / 2;
    for (int i = 0; i < pair; i++) {
      if (cond == params[2 * i]) {
        return params[2 * i + 1];
      }
    }

    return null;
  }

}
