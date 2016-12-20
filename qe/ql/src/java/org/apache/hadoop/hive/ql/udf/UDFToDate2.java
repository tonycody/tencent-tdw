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
import org.apache.hadoop.io.Text;

import org.apache.hadoop.hive.ql.exec.description;

@description(name = "to_date", value = "_FUNC_(str,fmt) - return string to date with fmt", extended = "Example:\n"
    + "  > SELECT _FUNC_('20100101', 'yyyymmdd') FROM src LIMIT 1;\n"
    + "20100101")
public class UDFToDate2 extends UDF {

  private static Log LOG = LogFactory.getLog(UDFToDate2.class.getName());

  /* ��ǰ֧��10�ָ�ʽ */
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

  public String evaluate(String date, String format) throws Exception {
    if (date == null || date.isEmpty()) {
      return null;
    }

    if (format == null || format.length() == 0) {
      throw new Exception("format can not be null!");
    }

    if (format.equalsIgnoreCase(format1)) {
      Pattern pattern = Pattern
          .compile("([0-9][0-9][0-9][0-9])([0-9][0-9])([0-9][0-9])[\\s\\S]*(\\..*)?$");

      Matcher matcher = pattern.matcher(date);

      if (!matcher.matches()) {
        return null;
      }

      int year = Integer.valueOf(matcher.group(1));
      int month = Integer.valueOf(matcher.group(2));
      int day = Integer.valueOf(matcher.group(3));

      return String.format("%04d-%02d-%02d 00:00:00:000", year, month, day);
    } else if (format.equalsIgnoreCase(format2)) {
      Pattern pattern = Pattern
          .compile("([0-9][0-9][0-9][0-9])([0-9][0-9])[\\s\\S]*(\\..*)?$");

      Matcher matcher = pattern.matcher(date);

      if (!matcher.matches()) {
        return null;
      }

      int year = Integer.valueOf(matcher.group(1));
      int month = Integer.valueOf(matcher.group(2));

      return String.format("%04d-%02d-01 00:00:00:000", year, month);
    } else if (format.equalsIgnoreCase(format3)) {
      Pattern pattern = Pattern
          .compile("([0-9][0-9][0-9][0-9])[\\s\\S]*(\\..*)?$");

      Matcher matcher = pattern.matcher(date);

      if (!matcher.matches()) {
        return null;
      }

      int year = Integer.valueOf(matcher.group(1));

      return String.format("%04d-01-01 00:00:00:000", year);
    } else if (format.equalsIgnoreCase(format4)) {
      Pattern pattern = Pattern.compile("([0-9][0-9])[\\s\\S]*(\\..*)?$");

      Matcher matcher = pattern.matcher(date);

      if (!matcher.matches()) {
        return null;
      }

      int month = Integer.valueOf(matcher.group(1));

      return String.format("1970-%02d-01 00:00:00:000", month);
    } else if (format.equalsIgnoreCase(format5)) {
      Pattern pattern = Pattern.compile("([0-9][0-9])[\\s\\S]*(\\..*)?$");

      Matcher matcher = pattern.matcher(date);

      if (!matcher.matches()) {
        return null;
      }

      int day = Integer.valueOf(matcher.group(1));

      return String.format("1970-01-%02d 00:00:00:000", day);
    } else if (format.equalsIgnoreCase(format6)) {
      Pattern pattern = Pattern
          .compile("([0-9][0-9][0-9][0-9])-([0-9][0-9])-([0-9][0-9])[\\s\\S]*(\\..*)?$");

      Matcher matcher = pattern.matcher(date);

      if (!matcher.matches()) {
        return null;
      }

      int year = Integer.valueOf(matcher.group(1));
      int month = Integer.valueOf(matcher.group(2));
      int day = Integer.valueOf(matcher.group(3));

      return String.format("%04d-%02d-%02d 00:00:00:000", year, month, day);
    } else if (format.equalsIgnoreCase(format7)) {
      Pattern pattern = Pattern
          .compile("([0-9][0-9][0-9][0-9])-([0-9][0-9])[\\s\\S]*(\\..*)?$");

      Matcher matcher = pattern.matcher(date);

      if (!matcher.matches()) {
        return null;
      }

      int year = Integer.valueOf(matcher.group(1));
      int month = Integer.valueOf(matcher.group(2));

      return String.format("%04d-%02d-01 00:00:00:000", year, month);
    } else if (format.equalsIgnoreCase(format8)) {
      Pattern pattern = Pattern
          .compile("([0-9][0-9][0-9][0-9])([0-9][0-9])([0-9][0-9])([0-9][0-9])([0-9][0-9])([0-9][0-9])[\\s\\S]*(\\..*)?$");

      Matcher matcher = pattern.matcher(date);

      if (!matcher.matches()) {
        return null;
      }

      int year = Integer.valueOf(matcher.group(1));
      int month = Integer.valueOf(matcher.group(2));
      int day = Integer.valueOf(matcher.group(3));
      int hour = Integer.valueOf(matcher.group(4));
      int min = Integer.valueOf(matcher.group(5));
      int second = Integer.valueOf(matcher.group(6));

      return String.format("%04d-%02d-%02d %02d:%02d:%02d:000", year, month,
          day, hour, min, second);
    } else if (format.equalsIgnoreCase(format9)) {
      Pattern pattern = Pattern
          .compile("([0-9][0-9][0-9][0-9])-([0-9][0-9])-([0-9][0-9]) ([0-9][0-9]):([0-9][0-9]):([0-9][0-9])[\\s\\S]*(\\..*)?$");

      Matcher matcher = pattern.matcher(date);

      if (!matcher.matches()) {
        return null;
      }

      int year = Integer.valueOf(matcher.group(1));
      int month = Integer.valueOf(matcher.group(2));
      int day = Integer.valueOf(matcher.group(3));
      int hour = Integer.valueOf(matcher.group(4));
      int min = Integer.valueOf(matcher.group(5));
      int second = Integer.valueOf(matcher.group(6));

      return String.format("%04d-%02d-%02d %02d:%02d:%02d:000", year, month,
          day, hour, min, second);
    } else if (format.equalsIgnoreCase(format10)) {
      Pattern pattern = Pattern
          .compile("([0-9][0-9])([0-9][0-9])([0-9][0-9])[\\s\\S]*(\\..*)?$");

      Matcher matcher = pattern.matcher(date);

      if (!matcher.matches()) {
        return null;
      }

      int hour = Integer.valueOf(matcher.group(1));
      int min = Integer.valueOf(matcher.group(2));
      int second = Integer.valueOf(matcher.group(3));

      return String.format("1970-01-01 %02d:%02d:%02d:000", hour, min, second);
    } else if (format.equalsIgnoreCase(format11)) {
      Pattern pattern = Pattern
          .compile("([0-9][0-9][0-9][0-9])([0-9][0-9])([0-9][0-9])([0-9][0-9])([0-9][0-9])([0-9][0-9])([0-9][0-9][0-9])[\\s\\S]*(\\..*)?$");

      Matcher matcher = pattern.matcher(date);

      if (!matcher.matches()) {
        LOG.error("no match");
        return null;
      }

      int year = Integer.valueOf(matcher.group(1));
      int month = Integer.valueOf(matcher.group(2));
      int day = Integer.valueOf(matcher.group(3));
      int hour = Integer.valueOf(matcher.group(4));
      int min = Integer.valueOf(matcher.group(5));
      int second = Integer.valueOf(matcher.group(6));
      int microSecond = Integer.valueOf(matcher.group(7));

      LOG.error(String.format("%04d-%02d-%02d %02d:%02d:%02d:%03d", year,
          month, day, hour, min, second, microSecond));
      return String.format("%04d-%02d-%02d %02d:%02d:%02d:%03d", year, month,
          day, hour, min, second, microSecond);
    } else {
      return null;
    }
  }
  
  public String evaluate(String date) throws Exception {
    if (date == null || date.isEmpty()) {
      return null;
    }

    Pattern pattern = Pattern
        .compile("([0-9][0-9][0-9][0-9])-([0-9][0-9])-([0-9][0-9]) ([0-9][0-9]):([0-9][0-9]):([0-9][0-9])[\\s\\S]*(\\..*)?$");

    Matcher matcher = pattern.matcher(date);

    if (!matcher.matches()) {
      return null;
    }

    int year = Integer.valueOf(matcher.group(1));
    int month = Integer.valueOf(matcher.group(2));
    int day = Integer.valueOf(matcher.group(3));
    int hour = Integer.valueOf(matcher.group(4));
    int min = Integer.valueOf(matcher.group(5));
    int second = Integer.valueOf(matcher.group(6));

    return String.format("%04d-%02d-%02d %02d:%02d:%02d:000", year, month,
        day, hour, min, second);
  }
  
}
