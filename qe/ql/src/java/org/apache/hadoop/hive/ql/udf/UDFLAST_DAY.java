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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

@description(name = "last_day", value = "_FUNC_(date) - Returns the last day of the month based on a date value", extended = "date is a string in the format of 'yyyy-MM-dd'.\n"
    + "Example:\n "
    + "  > SELECT _FUNC_('2003/03/15') FROM src LIMIT 1;\n"
    + "  31")
public class UDFLAST_DAY extends UDF {

  private static Log LOG = LogFactory.getLog(UDFDayOfMonth.class.getName());

  private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
  private Calendar calendar = Calendar.getInstance();

  IntWritable result = new IntWritable();

  public UDFLAST_DAY() {
  }

  public IntWritable evaluate(Text dateString) {

    if (dateString == null) {
      return null;
    }

    try {
      Date date = formatter.parse(dateString.toString());
      calendar.setTime(date);

      if (calendar.get(Calendar.ERA) == GregorianCalendar.BC)
        return null;
      int xxh = calendar.get(Calendar.YEAR);
      if ((xxh < 1900) || (xxh > 9999))
        return null;

      int maxDay = calendar.getActualMaximum(Calendar.DAY_OF_MONTH);
      result.set(maxDay);
      return result;
    } catch (ParseException e) {
      return null;
    }
  }

}
