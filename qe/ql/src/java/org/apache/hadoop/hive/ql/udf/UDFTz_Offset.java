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
import java.util.TimeZone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

@description(name = "tz_offset", value = "_FUNC_(timezone ) - returns the time zone offset of a value", extended = "Example:\n "
    + "  > SELECT _FUNC_('US/Michigan') FROM src LIMIT 1;\n" + "-05:00")
public class UDFTz_Offset extends UDF {

  private static Log LOG = LogFactory.getLog(UDFTz_Offset.class.getName());

  private SimpleDateFormat formatter = new SimpleDateFormat("h:mm");
  Text result = new Text();

  public UDFTz_Offset() {
  }

  public Text evaluate(Text timezone) {

    if (timezone == null) {
      return null;
    }
    try {
      String ttt = timezone.toString();
      if (ttt.endsWith("00") && (!ttt.startsWith("GMT"))) {
        ttt = "GMT" + ttt;
      }
      TimeZone tz = TimeZone.getTimeZone(ttt);
      Calendar calendar = Calendar.getInstance(tz);
      int off = calendar.get(Calendar.ZONE_OFFSET);
      off = off / 3600000;
      String ooo = String.format("%1$+03d", off);
      ooo = ooo + ":00";
      result.set(ooo);
      return result;
    } catch (Exception e) {
      return null;
    }
  }

}
