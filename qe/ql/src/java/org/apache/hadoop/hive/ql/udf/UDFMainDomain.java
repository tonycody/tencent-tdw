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
import org.apache.hadoop.io.Text;
import org.apache.commons.lang.StringUtils;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

import com.google.common.base.Joiner;
import com.google.common.net.InternetDomainName;

@description(name = "get_main_domain", value = "_FUNC_(strUrl) - Gets the main domain of the specified "
    + "strUrl ", extended = "Example:\n"
    + "  > SELECT _FUNC_('   www.tencent.com  ') FROM src LIMIT 1;\n" + "  'tencent.com'")
public class UDFMainDomain extends UDF {

  Text result = new Text();

  public UDFMainDomain() {
  }

  public Text evaluate(Text s) {
    if (s == null) {
      return result;
    }
    String strMainDomain = s.toString();
    try {
      InternetDomainName domainname = InternetDomainName.from(strMainDomain).topPrivateDomain();
      strMainDomain = Joiner.on(".").skipNulls().join(domainname.parts());
    } catch (Exception e) {
      strMainDomain = s.toString();
    }
    result.set(strMainDomain);
    return result;
  }

}
