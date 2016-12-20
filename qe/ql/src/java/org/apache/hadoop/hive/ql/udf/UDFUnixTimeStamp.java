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

import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.TimeZone;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

@description(name = "unix_timestamp", value = "_FUNC_([date[, pattern]]) - Returns the UNIX timestamp", extended = "Converts the current or specified time to number of seconds "
    + "since 1970-01-01.")
public class UDFUnixTimeStamp extends UDF {

  private static Log LOG = LogFactory.getLog(UDFUnixTimeStamp.class.getName());

  private SimpleDateFormat formatter = new SimpleDateFormat(
      "yyyy-MM-dd HH:mm:ss");

  LongWritable result = new LongWritable();

  public UDFUnixTimeStamp() {
  }

  public LongWritable evaluate() {
    Date date = new Date();
    result.set(date.getTime() / 1000);
    return result;
  }

  public LongWritable evaluate(Text dateText) {
    if (dateText == null) {
      return null;
    }

    try {
      Date date = (Date) formatter.parse(dateText.toString());
      result.set(date.getTime() / 1000);
      return result;
    } catch (ParseException e) {
      return null;
    }
  }

  Text lastPatternText = new Text();

  public LongWritable evaluate(Text dateText, Text patternText) {
    if (dateText == null || patternText == null) {
      return null;
    }
    try {
      if (!patternText.equals(lastPatternText)) {
        formatter.applyPattern(patternText.toString());
        lastPatternText.set(patternText);
      }
    } catch (Exception e) {
      return null;
    }

    return evaluate(dateText);
  }

  public LongWritable evaluate(TimestampWritable i) {
    if (i == null) {
      return null;
    } else {
      result.set(i.getSeconds());
      return result;
    }
  }
}
