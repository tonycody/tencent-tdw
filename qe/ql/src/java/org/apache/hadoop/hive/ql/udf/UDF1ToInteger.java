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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class UDF1ToInteger extends UDF {

  private static Log LOG = LogFactory.getLog(UDF1ToInteger.class.getName());

  IntWritable intWritable = new IntWritable();

  public UDF1ToInteger() {
  }

  public IntWritable evaluate(NullWritable i) {
    return null;
  }

  public IntWritable evaluate(BooleanWritable i) {
    if (i == null) {
      return null;
    } else {
      intWritable.set(i.get() ? 1 : 0);
      return intWritable;
    }
  }

  public IntWritable evaluate(ByteWritable i) {
    if (i == null) {
      return null;
    } else {
      intWritable.set((int) i.get());
      return intWritable;
    }
  }

  public IntWritable evaluate(ShortWritable i) {
    if (i == null) {
      return null;
    } else {
      intWritable.set((int) i.get());
      return intWritable;
    }
  }

  public IntWritable evaluate(LongWritable i) throws HiveException {
    if (i == null) {
      return null;
    } else {
      intWritable.set((int) i.get());
      if (intWritable.get() != i.get())
        throw new HiveException("dataerror");
      return intWritable;
    }
  }

  public IntWritable evaluate(FloatWritable i) throws HiveException {
    if (i == null) {
      return null;
    } else {
      intWritable.set((int) i.get());
      if (intWritable.get() != i.get())
        throw new HiveException("dataerror");
      return intWritable;
    }
  }

  public IntWritable evaluate(DoubleWritable i) throws HiveException {
    if (i == null) {
      return null;
    } else {
      intWritable.set((int) i.get());
      if (intWritable.get() != i.get())
        throw new HiveException("dataerror");
      return intWritable;
    }
  }

  public IntWritable evaluate(Text i) throws HiveException {
    if (i == null) {
      return null;
    } else {
      try {
        intWritable
            .set(LazyInteger.parseInt(i.getBytes(), 0, i.getLength(), 10));
        if (!String.valueOf(intWritable.get()).equals(i.toString()))
          throw new HiveException("dataerror");
        return intWritable;
      } catch (NumberFormatException e) {
        throw new HiveException("dataerror");
      }
    }
  }

}
