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

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

@description(name = "inet_aton", value = "_FUNC_(x) - returns the integer value of ip address", extended = "Example:\n"
    + "  > SELECT _FUNC_(10.10.10.10) FROM src LIMIT 1;\n" + "  168430090")
public class UDFInet_aton extends UDF {

  private LongWritable result = new LongWritable();

  public LongWritable evaluate(Text ip) {
    if (ip == null) {
      return null;
    }
    long ipres = -1;
    String ipstr = ip.toString();
    String[] strs = ipstr.split("\\.");
    if (strs.length != 4) {
      return null;
    }

    try {
      long x1 = Integer.parseInt(strs[0]);
      int x2 = Integer.parseInt(strs[1]);
      int x3 = Integer.parseInt(strs[2]);
      int x4 = Integer.parseInt(strs[3]);
      if (x1 < 0 || x1 > 255 || x2 < 0 || x2 > 255 || x3 < 0 || x3 > 255
          || x4 < 0 || x4 > 255) {
        return null;
      }
      ipres = 0;
      ipres = (x1 & 0x0ff) << 24 | (x2 & 0x0ff) << 16 | (x3 & 0x0ff) << 8
          | (x4 & 0x0ff);
      result.set(ipres);
      return result;

    } catch (NumberFormatException e) {
      return null;
    }
  }

  public static void main(String[] args) {
    UDFInet_aton udf = new UDFInet_aton();
    System.out.println(udf.evaluate(new Text("10.10.10.10")));
  }
}
