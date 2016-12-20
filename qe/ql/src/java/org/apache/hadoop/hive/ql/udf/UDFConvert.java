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
import java.util.TimeZone;
import java.lang.String;
import java.nio.charset.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

@description(name = "convert", value = "_FUNC_(str,dset,sset) - Converts a string from one character set to another ", extended = "Example:\n "
    + "  > SELECT _FUNC_('', '', '') FROM src LIMIT 1;\n")
public class UDFConvert extends UDF {

  private static Log LOG = LogFactory.getLog(UDFConvert.class.getName());

  Text result = new Text();
  private Charset ori_1 = Charset.forName("GBK");
  private Charset ori_2 = Charset.forName("utf-8");

  public UDFConvert() {
  }

  public Text evaluate(Text str, Text dset, Text sset) {

    if (str == null || dset == null || sset == null) {
      return null;
    }
    Charset in_dset = Charset.forName(dset.toString());
    Charset in_sset = Charset.forName(sset.toString());

    if (in_dset.equals(in_sset)) {
      result.set(str.toString());
      return result;
    }

    if (ori_1.equals(in_dset) && ori_2.equals(in_sset)) {
      try {
        result.set(new String(str.toString().getBytes("gbk"), "gbk"));
        return result;
      } catch (Exception e) {
        return null;
      }
    } else if (ori_1.equals(in_sset) && ori_2.equals(in_dset)) {
      try {
        result.set(str.toString().getBytes("utf-8"));
        return result;
      } catch (Exception e) {
        return null;
      }
    } else {
      return null;
    }

  }

}
