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

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Pattern;

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

@description(name = "to_char", value = "_FUNC_(string) - return string to int ", extended = "Example:\n"
    + "  > SELECT _FUNC_('2010') FROM src LIMIT 1;\n" + "2010")
public class UDFToNumber extends UDF {

  private static Log LOG = LogFactory.getLog(UDFToNumber.class.getName());

  public Long evaluate(String num) {
    if (num == null) {
      return null;
    } else if (num.equalsIgnoreCase("")) {
      return null;
    }

    Pattern pattern = Pattern.compile("-?[0-9]*");
    if (!pattern.matcher(num).matches()) {
      return null;
    }

    return new Long(num);
  }
}
