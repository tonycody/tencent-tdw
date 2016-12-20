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
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class UDFBetween extends UDFMyCompare {

  private static Log LOG = LogFactory.getLog(UDFBetween.class.getName());

  public UDFBetween() {
  }

  public BooleanWritable evaluate(Text a, Text b, Text c) {
    if (a == null || b == null || c == null) {
      return new BooleanWritable(false);
    }

    boolean d = ShimLoader.getHadoopShims().compareText(a, c) <= 0;
    boolean e = ShimLoader.getHadoopShims().compareText(a, b) >= 0;

    if (d && e) {
      return new BooleanWritable(true);
    } else {
      return new BooleanWritable(false);
    }
  }

  public BooleanWritable evaluate(ByteWritable a, ByteWritable b, ByteWritable c) {
    if (a == null || b == null || c == null) {
      return new BooleanWritable(false);
    }

    Byte aa = a.get();
    Byte bb = b.get();
    Byte cc = c.get();

    if (aa >= bb && aa <= cc) {
      return new BooleanWritable(true);
    } else {
      return new BooleanWritable(false);
    }
  }

  public BooleanWritable evaluate(ShortWritable a, ShortWritable b,
      ShortWritable c) {
    if (a == null || b == null || c == null) {
      return new BooleanWritable(false);
    }

    short aa = a.get();
    short bb = b.get();
    short cc = c.get();

    if (aa >= bb && aa <= cc) {
      return new BooleanWritable(true);
    } else {
      return new BooleanWritable(false);
    }
  }

  public BooleanWritable evaluate(IntWritable a, IntWritable b, IntWritable c) {
    if (a == null || b == null || c == null) {
      return new BooleanWritable(false);
    }

    int aa = a.get();
    int bb = b.get();
    int cc = c.get();

    if (aa >= bb && aa <= cc) {
      return new BooleanWritable(true);
    } else {
      return new BooleanWritable(false);
    }
  }

  public BooleanWritable evaluate(LongWritable a, LongWritable b, LongWritable c) {
    if (a == null || b == null || c == null) {
      return new BooleanWritable(false);
    }

    long aa = a.get();
    long bb = b.get();
    long cc = c.get();

    if (aa >= bb && aa <= cc) {
      return new BooleanWritable(true);
    } else {
      return new BooleanWritable(false);
    }
  }

  public BooleanWritable evaluate(FloatWritable a, FloatWritable b,
      FloatWritable c) {
    if (a == null || b == null || c == null) {
      return new BooleanWritable(false);
    }

    float aa = a.get();
    float bb = b.get();
    float cc = c.get();
    int tmp_a = Float.compare(aa, bb);
    int tmp_b = Float.compare(aa, cc);

    if (tmp_a >= 0 && tmp_b <= 0) {
      return new BooleanWritable(true);
    } else {
      return new BooleanWritable(false);
    }
  }

  public BooleanWritable evaluate(DoubleWritable a, DoubleWritable b,
      DoubleWritable c) {
    if (a == null || b == null || c == null) {
      return new BooleanWritable(false);
    }

    double aa = a.get();
    double bb = b.get();
    double cc = c.get();
    int tmp_a = Double.compare(aa, bb);
    int tmp_b = Double.compare(aa, cc);

    if (tmp_a >= 0 && tmp_b <= 0) {
      return new BooleanWritable(true);
    } else {
      return new BooleanWritable(false);
    }
  }
}
