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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

@description(name = "week", value = "_FUNC_(date) - Returns the week of date", extended = "date is a string in the format of 'yyyy-MM-dd HH:mm:ss' or "
    + "'yyyy-MM-dd','yyyyMMdd'.\n"
    + "Example:\n "
    + "  > SELECT _FUNC_('2012-05-28 12:58:59') FROM src LIMIT 1;\n"
    + "  2\n"
    + "  > SELECT _FUNC_('2012-05-28') FROM src LIMIT 1;\n"
    + "  2"
    + "  > SELECT _FUNC_(20120528) FROM src LIMIT 1;\n" + "  2")
public class UDFWeek extends UDF {
  private static Log LOG = LogFactory.getLog(UDFWeek.class.getName());

  private SimpleDateFormat formatter1 = new SimpleDateFormat(
      "yyyy-MM-dd HH:mm:ss");
  private SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd");
  private Calendar calendar = Calendar.getInstance();

  IntWritable result = new IntWritable();

  public IntWritable evaluate(Text dateString) {
    if (dateString == null) {
      return null;
    }

    try {
      Date date = null;
      try {
        date = formatter1.parse(dateString.toString());
      } catch (ParseException e1) {
        try {
          date = formatter2.parse(dateString.toString());
        } catch (ParseException e2) {
          String time = dateString.toString().trim();
          if (time.length() != 8)
            return null;

          calendar.set(Calendar.YEAR, Integer.valueOf(time.substring(0, 4)));
          calendar.set(Calendar.MONTH,
              Integer.valueOf(time.substring(4, 6)) - 1);
          calendar.set(Calendar.DAY_OF_MONTH,
              Integer.valueOf(time.substring(6, 8)));

          result.set(calendar.get(Calendar.DAY_OF_WEEK));
          return result;
        }
      }
      calendar.setTime(date);
      result.set(calendar.get(Calendar.DAY_OF_WEEK));
      return result;
    } catch (Exception e3) {
      return null;
    }
  }

  public IntWritable evaluate(TimestampWritable t) {
    if (t == null) {
      return null;
    }
    try {
      calendar.setTime(t.getTimestamp());
      result.set(calendar.get(Calendar.DAY_OF_WEEK));
      return result;
    } catch (Exception x) {
      return null;
    }
  }
}
