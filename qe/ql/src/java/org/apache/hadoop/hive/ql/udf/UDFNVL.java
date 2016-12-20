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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.hive.ql.exec.description;

@description(name = "nvl", value = "_FUNC_(expr1,expr2) - Returns expr2 if expr1 is null, otherwise return expr1", extended = "Example:\n"
    + "  > SELECT _FUNC_(name, 'kt') FROM src LIMIT 1;\n")
public class UDFNVL extends UDF {

  private static Log LOG = LogFactory.getLog(UDFNVL.class.getName());

  public Boolean evaluate(Boolean v1, Boolean v2) {
    if (v1 == null)
      return v2;
    else
      return v1;
  }

  public Byte evaluate(Byte v1, Byte v2) {
    if (v1 == null)
      return v2;
    else
      return v1;
  }

  public Short evaluate(Short v1, Short v2) {
    if (v1 == null)
      return v2;
    else
      return v1;
  }

  public Integer evaluate(Integer v1, Integer v2) {
    if (v1 == null)
      return v2;
    else
      return v1;
  }

  public Long evaluate(Long v1, Long v2) {
    if (v1 == null)
      return v2;
    else
      return v1;
  }

  public Float evaluate(Float v1, Float v2) {
    if (v1 == null)
      return v2;
    else
      return v1;
  }

  public Double evaluate(Double v1, Double v2) {
    if (v1 == null)
      return v2;
    else
      return v1;
  }

  public String evaluate(String v1, String v2) {
    if (v1 == null)
      return v2;
    else
      return v1;
  }
}
