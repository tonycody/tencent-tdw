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
import java.util.regex.Matcher;
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

@description(name = "next_day", value = "_FUNC_(date,day) - return date of next week day", extended = "Example:\n"
    + "  > SELECT _FUNC_(sysdate, 3) FROM src LIMIT 1;\n")
public class UDFNextDay extends UDF {

  private static Log LOG = LogFactory.getLog(UDFNextDay.class.getName());

  public String evaluate(String date, Integer day) {
    if (date == null || day == null) {
      return null;
    }

    if (day < 1 || day > 7) {
      return null;
    }

    Pattern pattern = Pattern
        .compile("([0-9][0-9][0-9][0-9])-([0-9][0-9])-([0-9][0-9])[\\s\\S]*(\\..*)?$");

    Matcher matcher = pattern.matcher(date);

    if (!matcher.matches()) {
      return null;
    }

    int year = Integer.valueOf(matcher.group(1));
    int month = Integer.valueOf(matcher.group(2));
    int dd = Integer.valueOf(matcher.group(3));

    Date ret = new Date();
    ret.setYear(year - 1900);
    ret.setMonth(month - 1);
    ret.setDate(dd);

    ret.setHours(0);
    ret.setMinutes(0);
    ret.setSeconds(0);

    Calendar curr = Calendar.getInstance();
    curr.setTime(ret);

    int curr_week = curr.get(Calendar.DAY_OF_WEEK);
    int offset = 7 + (day - curr_week);

    curr.add(Calendar.DAY_OF_WEEK, offset);

    Date newDate = curr.getTime();
    System.out.println("newDate:" + newDate.toString());

    year = curr.get(Calendar.YEAR);
    month = curr.get(Calendar.MONTH) + 1;
    dd = curr.get(Calendar.DAY_OF_MONTH);

    System.out.println("curr.get(Calendar.MONTH):" + curr.get(Calendar.MONTH));

    return String.format("%04d-%02d-%02d", year, month, dd);
  }
}
