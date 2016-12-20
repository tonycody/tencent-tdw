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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.io.Text;

@description(name = "CommonStr", value = "_FUNC_(col1,col2,separator) - Returns the common elements of col1 and col2 which separated by separator", extended = "col1 and col2 must be String type, they will be casted to Text,\n"
    + "col1 and col2 is separated by separator"
    + "Example: \n "
    + "  > SELECT _FUNC_('a,b,c,d','d,c',',') FROM src LIMIT 1;\n"
    + "  d,c\n"
    + "  > SELECT _FUNC_(' ab,bc,ac','ac,bc',',') FROM src LIMIT 1;\n"
    + "  ac,bc")
public class UDFCommonStr extends UDF {
  public static Log LOG = LogFactory.getLog(UDFCommonStr.class.getName());

  public Text evaluate(Text col1, Text col2, Text separator) {
    if (col1 == null || col2 == null)
      return null;
    String se = separator.toString();
    StringTokenizer st1 = new StringTokenizer(col1.toString(), se);
    HashSet<String> set1 = new HashSet<String>();
    while (st1.hasMoreTokens()) {
      set1.add(st1.nextToken());
    }
    st1 = new StringTokenizer(col2.toString(), se);
    StringBuilder sb = new StringBuilder();
    while (st1.hasMoreTokens()) {
      String str = st1.nextToken();
      if (set1.contains(str)) {
        sb.append(str);
        break;
      }
    }
    while (st1.hasMoreTokens()) {
      String str = st1.nextToken();
      if (set1.contains(str)) {
        sb.append(se + str);
      }
    }
    return new Text(sb.toString());
  }

  public static void main(String[] args) {
    UDFCommonStr commonstr = new UDFCommonStr();
    String str = commonstr.evaluate(new Text("迷路\0bc\0ac\0中国"),
        new Text("ab\0迷路\0bc\0中国"), new Text("\0")).toString();
    System.out.println(str);

  }

}
