/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.udf;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

@description(name = "datediff", value = "_FUNC_(date1, date2) - Returns the number of days between date1 "
    + "and date2", extended = "date1 and date2 are strings in the format "
    + "'yyyy-MM-dd HH:mm:ss' or 'yyyy-MM-dd'. The time parts are ignored."
    + "If date1 is earlier than date2, the result is negative.\n"
    + "Example:\n "
    + "  > SELECT _FUNC_('2009-10-07', '2009-11-07') FROM src LIMIT 1;\n"
    + "  -31" + "  > SELECT _FUNC_('20091007', '20091107') FROM src LIMIT 1;\n"
    + "  -31")
public class UDFDateDiff extends UDF {

  private static Log LOG = LogFactory.getLog(UDFDateDiff.class.getName());

  private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

  private SimpleDateFormat formatterNoLine = new SimpleDateFormat("yyyyMMdd");

  private final IntWritable result = new IntWritable();

  public UDFDateDiff() {
    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    formatterNoLine.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  public IntWritable evaluate(Text dateString1, Text dateString2) {
    return evaluate(toDate(dateString1), toDate(dateString2));
  }

  public IntWritable evaluate(TimestampWritable t1, TimestampWritable t2) {
    return evaluate(toDate(t1), toDate(t2));
  }

  public IntWritable evaluate(TimestampWritable t, Text dateString) {
    return evaluate(toDate(t), toDate(dateString));
  }

  public IntWritable evaluate(Text dateString, TimestampWritable t) {
    return evaluate(toDate(dateString), toDate(t));
  }

  private IntWritable evaluate(Date date, Date date2) {
    if (date == null || date2 == null) {
      return null;
    }

    long diffInMilliSeconds = date.getTime() - date2.getTime();
    result.set((int) (diffInMilliSeconds / (86400 * 1000)));
    return result;
  }

  private Date format(String dateString) {
    try {

      boolean hasLine = false;
      for (int i = 0; i < dateString.toString().length(); i++) {
        if (dateString.toString().charAt(i) == '-') {
          hasLine = true;
          break;
        }
      }

      if (!hasLine) {
        if (dateString.toString().trim().length() > 8) {
          return null;
        }
        return formatterNoLine.parse(dateString.trim());
      } else {
        return formatter.parse(dateString);
      }
    } catch (ParseException e) {
      return null;
    }
  }

  private Date toDate(Text dateString) {
    if (dateString == null) {
      return null;
    }
    return format(dateString.toString());
  }

  private Date toDate(TimestampWritable t) {
    if (t == null) {
      return null;
    }
    return t.getTimestamp();
  }

}
