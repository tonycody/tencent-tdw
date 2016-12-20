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
import java.util.TimeZone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

@description(name = "add_months", value = "_FUNC_(date1, n) - returns a date plus n months", extended = "Example:\n "
    + "  > SELECT _FUNC_('2001-08-03', 3) FROM src LIMIT 1;\n"
    + "  '2001-11-03'"
    + "  > SELECT _FUNC_('20010803', 3) FROM src LIMIT 1;\n"
    + "  '20011103'")
public class UDFADD_MONTHS extends UDF {

  private static Log LOG = LogFactory.getLog(UDFADD_MONTHS.class.getName());

  private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

  private SimpleDateFormat formatterNoLine = new SimpleDateFormat("yyyyMMdd");

  private Calendar calendar = Calendar.getInstance();

  Text result = new Text();

  public UDFADD_MONTHS() {
  }

  public Text evaluate(Text dateString1, IntWritable months) {

    if (dateString1 == null || months == null) {
      return null;
    }

    try {

      Date inDate;
      boolean hasLine = false;
      for (int i = 0; i < dateString1.toString().length(); i++) {
        if (dateString1.toString().charAt(i) == '-') {
          hasLine = true;
          break;
        }
      }

      if (!hasLine) {
        if (dateString1.toString().trim().length() > 8) {
          return null;
        }
        inDate = formatterNoLine.parse(dateString1.toString().trim());
      } else {
        inDate = formatter.parse(dateString1.toString());
      }

      calendar.setTime(inDate);
      int fff = calendar.get(Calendar.ERA);
      if (fff == GregorianCalendar.BC)
        return null;
      int yyy = calendar.get(Calendar.YEAR);
      if ((yyy < 1900) || (yyy > 9999))
        return null;
      int mmm = calendar.get(Calendar.MONTH);
      int rrr = yyy + (months.get() + mmm) / 12;
      if ((rrr < 1900) || (rrr > 9999))
        return null;
      calendar.add(Calendar.DAY_OF_MONTH, 1);
      if (calendar.get(Calendar.DAY_OF_MONTH) == 1) {
        calendar.add(Calendar.MONTH, months.get());
        calendar.add(Calendar.DAY_OF_MONTH, -1);
      } else {
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        calendar.add(Calendar.MONTH, months.get());
      }
      yyy = calendar.get(Calendar.YEAR);
      if ((yyy < 1900) || (yyy > 9999))
        return null;
      Date newDate = calendar.getTime();

      if (!hasLine) {
        result.set(formatterNoLine.format(newDate));
      } else {
        result.set(formatter.format(newDate));
      }
      return result;
    } catch (ParseException e) {
      return null;
    }
  }

}
