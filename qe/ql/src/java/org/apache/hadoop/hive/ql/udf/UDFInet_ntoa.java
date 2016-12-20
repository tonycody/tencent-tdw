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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

@description(name = "inet_ntoa", value = "_FUNC_(x) - returns the network address value of x", extended = "Example:\n"
    + "  > SELECT _FUNC_(168430090) FROM src LIMIT 1;\n" + "  10.10.10.10")
public class UDFInet_ntoa extends UDF {

  private Text result = new Text();

  public Text evaluate(LongWritable ip) {
    result.clear();
    if (ip == null) {
      return null;
    }
    long ipint = ip.get();
    if (ipint < 0 || ipint > 4294967295l)
      return null;

    StringBuffer sb = new StringBuffer();
    int x = (int) ((ipint >> 24) & 0x0ff);
    sb.append(x).append(".");
    x = (int) ((ipint >> 16) & 0x0ff);
    sb.append(x).append(".");
    x = (int) ((ipint >> 8) & 0x0ff);
    sb.append(x).append(".");
    x = (int) (ipint & 0x0ff);
    sb.append(x);
    result.set(sb.toString());

    return result;
  }

  public Text evaluate(IntWritable ip) {
    if (ip == null) {
      return null;
    }
    long ipint = ip.get();
    if (ipint < 0)
      return null;

    StringBuffer sb = new StringBuffer();
    int x = (int) ((ipint >> 24) & 0x0ff);
    sb.append(x).append(".");
    x = (int) ((ipint >> 16) & 0x0ff);
    sb.append(x).append(".");
    x = (int) ((ipint >> 8) & 0x0ff);
    sb.append(x).append(".");
    x = (int) (ipint & 0x0ff);
    sb.append(x);
    result.set(sb.toString());

    return result;
  }
}
