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

@description(name = "sysdate", value = "_FUNC_() - return current date, default format yyyy-mm-dd", extended = "Example:\n"
    + "  > SELECT _FUNC_() FROM src LIMIT 1;\n" + "2010-12-24")
public class UDFSysdate extends UDF {

  private static Log LOG = LogFactory.getLog(UDFSysdate.class.getName());

  public String evaluate() {
    Calendar cl = Calendar.getInstance();
    cl.setTimeInMillis(System.currentTimeMillis());

    int year = cl.get(Calendar.YEAR);
    int month = cl.get(Calendar.MONTH) + 1;
    int day = cl.get(Calendar.DAY_OF_MONTH);

    return String.format("%04d-%02d-%02d", year, month, day);

  }
}
