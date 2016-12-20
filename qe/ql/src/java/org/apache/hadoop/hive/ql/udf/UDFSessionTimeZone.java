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

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.io.Text;

@description(name = "sessiontimezone", value = "_FUNC_() - returns the current session's time zone", extended = "Example:\n "
    + "  > SELECT _FUNC_() FROM src LIMIT 1;\n" + "Asia/Shanghai")
public class UDFSessionTimeZone extends UDF {

  private static Log LOG = LogFactory
      .getLog(UDFSessionTimeZone.class.getName());

  Text result = new Text();

  public UDFSessionTimeZone() {
  }

  public Text evaluate() {

    try {
      TimeZone tz = TimeZone.getDefault();
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
