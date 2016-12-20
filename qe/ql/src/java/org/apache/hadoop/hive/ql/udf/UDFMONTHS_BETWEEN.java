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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

@description(name = "months_between", value = "_FUNC_(date1, date2) - Returns the number of months between date1 and date2", extended = "date is a string in the format of 'yyyy-MM-dd'"
    + "Example:\n "
    + "  > SELECT _FUNC_(to_date ('2003/07/01', 'yyyy/mm/dd'), to_date ('2003/03/14', 'yyyy/mm/dd') ) FROM src LIMIT 1;\n"
    + "  3.58064516129032"
    + "  > SELECT _FUNC_('20030701, '20030314') FROM src LIMIT 1;\n"
    + "  3.58064516129032")
public class UDFMONTHS_BETWEEN extends UDF {

  private static Log LOG = LogFactory.getLog(UDFMONTHS_BETWEEN.class.getName());

  private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

  private SimpleDateFormat formatterNoLine = new SimpleDateFormat("yyyyMMdd");

  private Calendar calendar = Calendar.getInstance();

  DoubleWritable result = new DoubleWritable();

  public UDFMONTHS_BETWEEN() {
  }

  public DoubleWritable evaluate(Text date1, Text date2) {

    if (date1 == null || date2 == null) {
      return null;
    }

    try {

      Date date_1, date_2;
      boolean hasLine = false;

      for (int i = 0; i < date1.toString().length(); i++) {
        if (date1.toString().charAt(i) == '-') {
          hasLine = true;
          break;
        }
      }
      if (!hasLine) {
        if (date1.toString().trim().length() > 8) {
          return null;
        }
        date_1 = formatterNoLine.parse(date1.toString().trim());
      } else {
        date_1 = formatter.parse(date1.toString());
      }

      hasLine = false;
      for (int i = 0; i < date2.toString().length(); i++) {
        if (date2.toString().charAt(i) == '-') {
          hasLine = true;
          break;
        }
      }
      if (!hasLine) {
        if (date2.toString().trim().length() > 8) {
          return null;
        }
        date_2 = formatterNoLine.parse(date2.toString().trim());
      } else {
        date_2 = formatter.parse(date2.toString());
      }

      calendar.setTime(date_1);
      if (calendar.get(Calendar.ERA) == GregorianCalendar.BC)
        return null;
      int yyy = calendar.get(Calendar.YEAR);
      if ((yyy < 1900) || (yyy > 9999))
        return null;
      int d_1 = calendar.get(Calendar.DAY_OF_MONTH);
      int m_1 = calendar.get(Calendar.MONTH);
      int y_1 = calendar.get(Calendar.YEAR);
      calendar.setTime(date_2);
      if (calendar.get(Calendar.ERA) == GregorianCalendar.BC)
        return null;
      yyy = calendar.get(Calendar.YEAR);
      if ((yyy < 1900) || (yyy > 9999))
        return null;
      int d_2 = calendar.get(Calendar.DAY_OF_MONTH);
      int m_2 = calendar.get(Calendar.MONTH);
      int y_2 = calendar.get(Calendar.YEAR);
      double tmpR = (double) ((y_1 - y_2) * 12) + (double) (m_1 - m_2)
          + (double) (d_1 - d_2) / 31.0;
      result.set(tmpR);
      return result;
    } catch (ParseException e) {
      return null;
    }
  }

}
