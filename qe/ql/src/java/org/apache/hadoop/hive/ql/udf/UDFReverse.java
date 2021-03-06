/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.io.Text;

@description(name = "reverse", value = "_FUNC_(str) - reverse str", extended = "Example:\n"
    + "  > SELECT _FUNC_('Facebook') FROM src LIMIT 1;\n" + "  'koobecaF'")
public class UDFReverse extends UDF {
  private Text result = new Text();

  private void reverse(byte[] arr, int first, int last) {
    for (int i = 0; i < (last - first + 1) / 2; i++) {
      byte temp = arr[last - i];
      arr[last - i] = arr[first + i];
      arr[first + i] = temp;
    }
  }

  public Text evaluate(Text s) {
    if (s == null) {
      return null;
    }

    result.set(s);

    byte[] data = result.getBytes();
    int prev = 0;
    for (int i = 1; i < result.getLength(); i++) {
      if (GenericUDFUtils.isUtfStartByte(data[i])) {
        reverse(data, prev, i - 1);
        prev = i;
      }
    }
    reverse(data, prev, result.getLength() - 1);
    reverse(data, 0, result.getLength() - 1);

    return result;
  }
}
