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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;

@description(name = "chr", value = "_FUNC_(asc) - returns the character based on the NUMBER code", extended = "Returns null if asc out of range\n"
    + "Example:\n"
    + "  > SELECT _FUNC_(116) FROM src LIMIT 1;"
    + "  t\n"
    + "  > SELECT _FUNC_(84) FROM src LIMIT 1;\n" + "  T")
public class UDFCHR extends UDF {

  private static Log LOG = LogFactory.getLog(UDFCHR.class.getName());
  private Text result = new Text();

  public UDFCHR() {
  }

  public Text evaluate(IntWritable asc) {
    if (asc == null) {
      return null;
    }
    int asci = asc.get();
    if (asci > 127 || asci < 0) {
      return null;
    } else {
      String ch = String.valueOf((char) asci);
      result.set(ch);
      return result;
    }
  }
}
