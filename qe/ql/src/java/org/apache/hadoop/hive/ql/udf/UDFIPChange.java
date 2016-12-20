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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

public class UDFIPChange extends UDF {

  public LongWritable evaluate(String str) {
    if (str == null) {
      return null;
    }
    try {
      return IP2Long.ipToLong(str);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  public String evaluate(LongWritable wIpint) {
    if (wIpint == null) {
      return null;
    }
    long ipint = wIpint.get();
    return IP2Long.longToIP(ipint);
  }

  public String evaluate(IntWritable wIpint) {
    if (wIpint == null) {
      return null;
    }
    long ipint = wIpint.get();
    return IP2Long.longToIP(ipint);
  }
}

class IP2Long {
  public static LongWritable ipToLong(String strIp) {
    LongWritable result = new LongWritable();
    long[] ip = new long[4];
    int position1 = strIp.indexOf(".");
    if (position1 == -1)
      return null;
    int position2 = strIp.indexOf(".", position1 + 1);
    if (position2 == -1)
      return null;
    int position3 = strIp.indexOf(".", position2 + 1);
    if (position3 == -1)
      return null;
    ip[0] = Long.parseLong(strIp.substring(0, position1));
    ip[1] = Long.parseLong(strIp.substring(position1 + 1, position2));
    ip[2] = Long.parseLong(strIp.substring(position2 + 1, position3));
    ip[3] = Long.parseLong(strIp.substring(position3 + 1));
    result.set((ip[0] << 24) + (ip[1] << 16) + (ip[2] << 8) + ip[3]);
    return result;
  }

  public static String longToIP(long longIp) {
    StringBuffer sb = new StringBuffer("");
    sb.append(String.valueOf((longIp >>> 24)));
    sb.append(".");
    sb.append(String.valueOf((longIp & 0x00FFFFFF) >>> 16));
    sb.append(".");
    sb.append(String.valueOf((longIp & 0x0000FFFF) >>> 8));
    sb.append(".");
    sb.append(String.valueOf((longIp & 0x000000FF)));
    return sb.toString();
  }
}
