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

import org.apache.hadoop.hive.ql.exec.ComparisonOpMethodResolver;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

@description(name = "comparison", value = "a _FUNC_ b - Returns 1 if a > b , 0 if a = b, -1 if a < b.")
public class UDFComparison extends UDF {

  IntWritable resultCache;

  public UDFComparison() {

    resultCache = new IntWritable();
  }

  public IntWritable evaluate(Text a, Text b) {
    IntWritable i = this.resultCache;
    if (a == null || (b == null)) {
      i = null;
    } else {
      i.set(a.compareTo(b));
    }
    return i;
  }

  public IntWritable evaluate(ByteWritable a, ByteWritable b) {
    IntWritable i = this.resultCache;
    if (a == null || (b == null)) {
      i = null;
    } else {
      i.set(a.compareTo(b));
    }
    return i;
  }

  public IntWritable evaluate(ShortWritable a, ShortWritable b) {
    IntWritable i = this.resultCache;
    if (a == null || (b == null)) {
      i = null;
    } else {
      i.set(a.compareTo(b));
    }
    return i;
  }

  public IntWritable evaluate(IntWritable a, IntWritable b) {
    IntWritable i = this.resultCache;
    if (a == null || (b == null)) {
      i = null;
    } else {
      i.set(a.compareTo(b));
    }
    return i;
  }

  public IntWritable evaluate(LongWritable a, LongWritable b) {
    IntWritable i = this.resultCache;
    if (a == null || (b == null)) {
      i = null;
    } else {
      i.set(a.compareTo(b));
    }
    return i;
  }

  public IntWritable evaluate(FloatWritable a, FloatWritable b) {
    IntWritable i = this.resultCache;
    if (a == null || (b == null)) {
      i = null;
    } else {
      i.set(a.compareTo(b));
    }
    return i;
  }

  public IntWritable evaluate(DoubleWritable a, DoubleWritable b) {
    IntWritable i = this.resultCache;
    if (a == null || (b == null)) {
      i = null;
    } else {
      i.set(a.compareTo(b));
    }
    return i;
  }

}
