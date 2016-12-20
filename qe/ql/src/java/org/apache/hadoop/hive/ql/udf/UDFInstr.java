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

@description(name = "instr", value = "_FUNC_(string,str,start, appear) - return pos str in string.  ", extended = "Example:\n"
    + "  > SELECT _FUNC_('kt2', 'kt', 1, 1) FROM src LIMIT 1;\n")
public class UDFInstr extends UDF {

  private static Log LOG = LogFactory.getLog(UDFInstr.class.getName());

  public Integer evaluate(String string, String substr) {
    if (string == null || substr == null) {
      return 0;
    }

    return string.indexOf(substr) + 1;
  }

  public Integer evaluate(String string, String substr, int start) {
    if (string == null || substr == null || start < 1) {
      return 0;
    }

    if (string.length() < start - 1) {
      return 0;
    }

    return string.indexOf(substr, start - 1) + 1;
  }

  public Integer evaluate(String string, String substr, int start, int appear) {
    if (string == null || substr == null || start < 1 || appear < 1) {
      return 0;
    }

    if (string.length() < start - 1) {
      return 0;
    }

    int from = start - 1;
    int tmpStart = string.indexOf(substr, from);

    if (tmpStart < 0) {
      return 0;
    } else {
      from = tmpStart + 1;
      for (int i = 1; i < appear; ++i) {
        tmpStart = string.indexOf(substr, from);
        if (tmpStart < 0) {
          break;
        }

        from = tmpStart + 1;
      }

    }

    return tmpStart + 1;
  }
}
