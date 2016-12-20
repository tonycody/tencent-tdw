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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.text.DecimalFormat;
import java.math.BigDecimal;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

@description(name = "trunc", value = "_FUNC_(val, [s]) a number truncated to a certain number of decimal places"
    + "or returns a date truncated to a specific unit of measure", extended = "Example:\n "
    + "  > SELECT _FUNC_(3.1415,2) FROM src LIMIT 1;\n" + "  3.14")
public class UDFTRUNC extends UDF {

  private static Log LOG = LogFactory.getLog(UDFTRUNC.class.getName());
  private SimpleDateFormat formatter = new SimpleDateFormat(
      "yyyy-MM-dd HH:mm:ss");
  private Calendar calendar = Calendar.getInstance();
  Text result = new Text();

  public UDFTRUNC() {
  }

  public Text evaluate(Text date, Text format) {
    if (date == null || format == null)
      return null;
    try {
      Date theDate = formatter.parse(date.toString());
      calendar.setTime(theDate);

      if (calendar.get(Calendar.ERA) == GregorianCalendar.BC)
        return null;
      int xxh = calendar.get(Calendar.YEAR);
      if ((xxh < 1900) || (xxh > 9999))
        return null;

      String fff = format.toString();
      if (fff.equals("YEAR") || fff.equals("YYYY") || fff.equals("YYY")
          || fff.equals("YY") || fff.equals("Y") || fff.equals("SYEAR")
          || fff.equals("SYYYY")) {
        calendar.set(calendar.get(Calendar.YEAR), 0, 1, 0, 0, 0);
        result.set(formatter.format(calendar.getTime()));
        return result;
      } else if (fff.equals("MONTH") || fff.equals("MON") || fff.equals("MM")
          || fff.equals("RM")) {
        calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH),
            1, 0, 0, 0);
        result.set(formatter.format(calendar.getTime()));
        return result;
      } else if (fff.equals("WW")) {
        calendar.set(Calendar.HOUR, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        if (calendar.get(Calendar.DAY_OF_YEAR) % 7 == 0)
          calendar.set(Calendar.DAY_OF_YEAR,
              ((calendar.get(Calendar.DAY_OF_YEAR) / 7 - 1) * 7 + 1));
        else
          calendar.set(Calendar.DAY_OF_YEAR,
              ((calendar.get(Calendar.DAY_OF_YEAR) / 7) * 7 + 1));
        result.set(formatter.format(calendar.getTime()));
        return result;
      } else if (fff.equals("W")) {
        calendar.set(Calendar.HOUR, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        if (calendar.get(Calendar.DAY_OF_MONTH) % 7 == 0)
          calendar.set(Calendar.DAY_OF_MONTH,
              ((calendar.get(Calendar.DAY_OF_MONTH) / 7 - 1) * 7 + 1));
        else
          calendar.set(Calendar.DAY_OF_MONTH,
              ((calendar.get(Calendar.DAY_OF_MONTH) / 7) * 7 + 1));
        result.set(formatter.format(calendar.getTime()));
        return result;
      } else if (fff.equals("DAY") || fff.equals("D") || fff.equals("DY")) {
        calendar.set(Calendar.DAY_OF_MONTH, calendar.get(Calendar.DAY_OF_MONTH)
            + 1 - calendar.get(Calendar.DAY_OF_WEEK));
        calendar.set(Calendar.HOUR, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        result.set(formatter.format(calendar.getTime()));
        return result;
      } else if (fff.equals("DDD") || fff.equals("DD") || fff.equals("J")) {
        calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH),
            calendar.get(Calendar.DAY_OF_MONTH), 0, 0, 0);
        result.set(formatter.format(calendar.getTime()));
        return result;
      } else if (fff.equals("HH") || fff.equals("HH12") || fff.equals("HH24")) {
        calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH),
            calendar.get(Calendar.DAY_OF_MONTH),
            calendar.get(Calendar.HOUR_OF_DAY), 0, 0);
        result.set(formatter.format(calendar.getTime()));
        return result;
      } else if (fff.equals("MI")) {
        calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH),
            calendar.get(Calendar.DAY_OF_MONTH),
            calendar.get(Calendar.HOUR_OF_DAY), calendar.get(Calendar.MINUTE),
            0);
        result.set(formatter.format(calendar.getTime()));
        return result;
      } else {
        return null;
      }
    } catch (ParseException e) {
      return null;
    }
  }

  public Text evaluate(Text date) {
    if (date == null)
      return null;
    try {
      Date theDate = formatter.parse(date.toString());
      calendar.setTime(theDate);

      if (calendar.get(Calendar.ERA) == GregorianCalendar.BC)
        return null;
      int xxh = calendar.get(Calendar.YEAR);
      if ((xxh < 1900) || (xxh > 9999))
        return null;

      calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH),
          calendar.get(Calendar.DAY_OF_MONTH), 0, 0, 0);
      result.set(formatter.format(calendar.getTime()));
      return result;

    } catch (ParseException e) {
      return null;
    }
  }

  public DoubleWritable evaluate(DoubleWritable val, IntWritable s) {
    if (val == null || s == null)
      return null;
    try {
      DoubleWritable re = new DoubleWritable();
      double d = val.get();
      String tmp = Double.toString(d);
      BigDecimal big = new BigDecimal(tmp);
      big = big.setScale(s.get(), 1);
      re.set(big.doubleValue());
      return re;
    } catch (Exception e) {
      return null;
    }
  }

  public DoubleWritable evaluate(DoubleWritable val) {
    if (val == null)
      return null;
    try {
      DoubleWritable re = new DoubleWritable();
      double d = val.get();
      BigDecimal big = new BigDecimal(d);
      big = big.setScale(0, 1);
      re.set(big.doubleValue());
      return re;
    } catch (Exception e) {
      return null;
    }
  }

  public IntWritable evaluate(IntWritable val, IntWritable s) {
    if (val == null || s == null)
      return null;
    try {
      IntWritable re = new IntWritable();
      int d = val.get();
      BigDecimal big = new BigDecimal(d);
      big = big.setScale(s.get(), 1);
      re.set((int) (big.doubleValue()));
      return re;
    } catch (Exception e) {
      return null;
    }
  }

  public LongWritable evaluate(LongWritable val, IntWritable s) {
    if (val == null || s == null)
      return null;
    try {
      LongWritable re = new LongWritable();
      long d = val.get();
      BigDecimal big = new BigDecimal(d);
      big = big.setScale(s.get(), 1);
      re.set((long) (big.doubleValue()));
      return re;
    } catch (Exception e) {
      return null;
    }
  }

  public IntWritable evaluate(IntWritable val) {
    if (val == null)
      return null;
    try {
      return val;
    } catch (Exception e) {
      return null;
    }
  }

  public LongWritable evaluate(LongWritable val) {
    if (val == null)
      return null;
    try {
      return val;
    } catch (Exception e) {
      return null;
    }
  }

}
