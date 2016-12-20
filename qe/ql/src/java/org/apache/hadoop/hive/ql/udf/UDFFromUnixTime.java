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
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

@description(name = "from_unixtime", value = "_FUNC_(unix_time, format) - returns unix_time in the specified "
    + "format", extended = "Example:\n"
    + "  > SELECT _FUNC_(0, 'yyyy-MM-dd HH:mm:ss') FROM src LIMIT 1;\n"
    + "  '1970-01-01 00:00:00'")
public class UDFFromUnixTime extends UDF {

  private static Log LOG = LogFactory.getLog(UDFFromUnixTime.class.getName());

  private SimpleDateFormat formatter;

  Text result = new Text();
  Text lastFormat = new Text();

  private long mymin = -62135798400L;
  private long mymax = 253402271999L;

  public UDFFromUnixTime() {
  }

  Text defaultFormat = new Text("yyyy-MM-dd HH:mm:ss");

  public Text evaluate(IntWritable unixtime) {
    return evaluate(unixtime, defaultFormat);
  }

  public Text evaluate(LongWritable unixtime) {
    return evaluate(unixtime, defaultFormat);
  }

  public Text evaluate(IntWritable unixtime, Text format) {
    if (unixtime == null || format == null) {
      return null;
    }

    if (!format.equals(lastFormat)) {
      formatter = new SimpleDateFormat(format.toString());
      lastFormat.set(format);
    }

    Date date = new Date(unixtime.get() * 1000L);
    result.set(formatter.format(date));
    return result;
  }

  public Text evaluate(LongWritable unixtime, Text format) {
    if (unixtime == null || format == null) {
      return null;
    }

    if (!format.equals(lastFormat)) {
      formatter = new SimpleDateFormat(format.toString());
      lastFormat.set(format);
    }

    long tmpl = 0;
    long ori = unixtime.get();
    if (ori > mymax) {
      tmpl = mymax * 1000;
    } else if (ori < mymin) {
      tmpl = mymin * 1000;
    } else {
      tmpl = ori * 1000L;
    }

    Date date = new Date(tmpl);
    result.set(formatter.format(date));
    return result;
  }

}
