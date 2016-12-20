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

@description(name = "to_char", value = "_FUNC_(date,fmt) - return date to string with fmt", extended = "Example:\n"
    + "  > SELECT _FUNC_(sysdate, 'yyyymmdd') FROM src LIMIT 1;\n" + "20101224")
public class UDFToChar extends UDF {
  private static Log LOG = LogFactory.getLog(UDFToChar.class.getName());

  /* ��ǰ֧��7�ָ�ʽ */
  private String format1 = "yyyymmdd";
  private String format2 = "yyyymm";
  private String format3 = "yyyy";
  private String format4 = "mm";
  private String format5 = "dd";
  private String format6 = "yyyy-mm-dd";
  private String format7 = "yyyy-mm";

  private String format8 = "yyyymmddhh24miss";
  private String format9 = "yyyy-mm-dd hh24:mi:ss";
  private String format10 = "hh24miss";
  private String format11 = "yyyymmddhh24missff3";

  public String evaluate(String date, String format) {
    if (date == null) {
      return null;
    }

    if (format == null || format.length() == 0) {
      format = format1;
    }

    int year, month, day, hour, min, second, ff;
    if (format.equalsIgnoreCase(format1)) {
      Pattern pattern = Pattern
          .compile("([0-9][0-9][0-9][0-9])-([0-9][0-9])-([0-9][0-9])[\\s\\S]*(\\..*)?$");

      Matcher matcher = pattern.matcher(date);

      if (!matcher.matches()) {
        return null;
      }

      year = Integer.valueOf(matcher.group(1));
      month = Integer.valueOf(matcher.group(2));
      day = Integer.valueOf(matcher.group(3));

      return String.format("%04d%02d%02d", year, month, day);
    } else if (format.equalsIgnoreCase(format2)) {
      Pattern pattern = Pattern
          .compile("([0-9][0-9][0-9][0-9])-([0-9][0-9])[\\s\\S]*(\\..*)?$");

      Matcher matcher = pattern.matcher(date);

      if (!matcher.matches()) {
        return null;
      }

      year = Integer.valueOf(matcher.group(1));
      month = Integer.valueOf(matcher.group(2));

      return String.format("%04d%02d", year, month);
    } else if (format.equalsIgnoreCase(format3)) {
      Pattern pattern = Pattern
          .compile("([0-9][0-9][0-9][0-9])[\\s\\S]*(\\..*)?$");

      Matcher matcher = pattern.matcher(date);

      if (!matcher.matches()) {
        return null;
      }

      year = Integer.valueOf(matcher.group(1));

      return String.format("%04d", year);
    } else if (format.equalsIgnoreCase(format4)) {
      Pattern pattern = Pattern
          .compile("[0-9][0-9][0-9][0-9]-([0-9][0-9])-[0-9][0-9][\\s\\S]*(\\..*)?$");

      Matcher matcher = pattern.matcher(date);

      if (!matcher.matches()) {
        return null;
      }

      month = Integer.valueOf(matcher.group(1));

      return String.format("%02d", month);
    } else if (format.equalsIgnoreCase(format5)) {
      Pattern pattern = Pattern
          .compile("[0-9][0-9][0-9][0-9]-[0-9][0-9]-([0-9][0-9])[\\s\\S]*(\\..*)?$");

      Matcher matcher = pattern.matcher(date);

      if (!matcher.matches()) {
        return null;
      }

      day = Integer.valueOf(matcher.group(1));

      return String.format("%02d", day);
    } else if (format.equalsIgnoreCase(format6)) {
      Pattern pattern = Pattern
          .compile("([0-9][0-9][0-9][0-9])-([0-9][0-9])-([0-9][0-9])[\\s\\S]*(\\..*)?$");

      Matcher matcher = pattern.matcher(date);

      if (!matcher.matches()) {
        return null;
      }

      year = Integer.valueOf(matcher.group(1));
      month = Integer.valueOf(matcher.group(2));
      day = Integer.valueOf(matcher.group(3));

      return String.format("%04d-%02d-%02d", year, month, day);
    } else if (format.equalsIgnoreCase(format7)) {
      Pattern pattern = Pattern
          .compile("([0-9][0-9][0-9][0-9])-([0-9][0-9])[\\s\\S]*(\\..*)?$");

      Matcher matcher = pattern.matcher(date);

      if (!matcher.matches()) {
        return null;
      }

      year = Integer.valueOf(matcher.group(1));
      month = Integer.valueOf(matcher.group(2));

      return String.format("%04d-%02d", year, month);
    } else if (format.equalsIgnoreCase(format8)) {
      Pattern pattern = Pattern
          .compile("([0-9][0-9][0-9][0-9])-([0-9][0-9])-([0-9][0-9]) ([0-9][0-9]):([0-9][0-9]):([0-9][0-9])[\\s\\S]*(\\..*)?$");

      Matcher matcher = pattern.matcher(date);

      if (!matcher.matches()) {
        return null;
      }

      year = Integer.valueOf(matcher.group(1));
      month = Integer.valueOf(matcher.group(2));
      day = Integer.valueOf(matcher.group(3));
      hour = Integer.valueOf(matcher.group(4));
      min = Integer.valueOf(matcher.group(5));
      second = Integer.valueOf(matcher.group(6));

      return String.format("%04d%02d%02d%02d%02d%02d", year, month, day, hour,
          min, second);
    } else if (format.equalsIgnoreCase(format9)) {
      Pattern pattern = Pattern
          .compile("([0-9][0-9][0-9][0-9])-([0-9][0-9])-([0-9][0-9]) ([0-9][0-9]):([0-9][0-9]):([0-9][0-9])[\\s\\S]*(\\..*)?$");

      Matcher matcher = pattern.matcher(date);

      if (!matcher.matches()) {
        return null;
      }

      year = Integer.valueOf(matcher.group(1));
      month = Integer.valueOf(matcher.group(2));
      day = Integer.valueOf(matcher.group(3));
      hour = Integer.valueOf(matcher.group(4));
      min = Integer.valueOf(matcher.group(5));
      second = Integer.valueOf(matcher.group(6));

      return String.format("%04d-%02d-%02d %02d:%02d:%02d", year, month, day,
          hour, min, second);
    } else if (format.equalsIgnoreCase(format10)) {
      Pattern pattern = Pattern
          .compile("[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] ([0-9][0-9]):([0-9][0-9]):([0-9][0-9])[\\s\\S]*(\\..*)?$");

      Matcher matcher = pattern.matcher(date);

      if (!matcher.matches()) {
        return null;
      }

      hour = Integer.valueOf(matcher.group(1));
      min = Integer.valueOf(matcher.group(2));
      second = Integer.valueOf(matcher.group(3));

      return String.format("%02d%02d%02d", hour, min, second);
    } else if (format.equalsIgnoreCase(format11)) {
      Pattern pattern = Pattern
          .compile("([0-9][0-9][0-9][0-9])-([0-9][0-9])-([0-9][0-9]) ([0-9][0-9]):([0-9][0-9]):([0-9][0-9]):([0-9][0-9][0-9])[\\s\\S]*(\\..*)?$");

      Matcher matcher = pattern.matcher(date);

      if (!matcher.matches()) {
        return null;
      }

      year = Integer.valueOf(matcher.group(1));
      month = Integer.valueOf(matcher.group(2));
      day = Integer.valueOf(matcher.group(3));
      hour = Integer.valueOf(matcher.group(4));
      min = Integer.valueOf(matcher.group(5));
      second = Integer.valueOf(matcher.group(6));
      ff = Integer.valueOf(matcher.group(7));

      return String.format("%04d%02d%02d%02d%02d%02d%03d", year, month, day,
          hour, min, second, ff);
    } else {
      return null;
    }
  }
}
