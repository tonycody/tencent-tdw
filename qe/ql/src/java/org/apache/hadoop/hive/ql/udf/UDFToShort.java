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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyByte;
import org.apache.hadoop.hive.serde2.lazy.LazyShort;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class UDFToShort extends UDF {

  private static Log LOG = LogFactory.getLog(UDFToByte.class.getName());

  ShortWritable shortWritable = new ShortWritable();

  public UDFToShort() {
  }

  public ShortWritable evaluate(NullWritable i) {
    return null;
  }

  public ShortWritable evaluate(BooleanWritable i) {
    if (i == null) {
      return null;
    } else {
      shortWritable.set(i.get() ? (short) 1 : (short) 0);
      return shortWritable;
    }
  }

  public ShortWritable evaluate(ByteWritable i) {
    if (i == null) {
      return null;
    } else {
      shortWritable.set((short) i.get());
      return shortWritable;
    }
  }

  public ShortWritable evaluate(IntWritable i) {
    if (i == null) {
      return null;
    } else {
      shortWritable.set((short) i.get());
      return shortWritable;
    }
  }

  public ShortWritable evaluate(LongWritable i) {
    if (i == null) {
      return null;
    } else {
      shortWritable.set((short) i.get());
      return shortWritable;
    }
  }

  public ShortWritable evaluate(FloatWritable i) {
    if (i == null) {
      return null;
    } else {
      shortWritable.set((short) i.get());
      return shortWritable;
    }
  }

  public ShortWritable evaluate(DoubleWritable i) {
    if (i == null) {
      return null;
    } else {
      shortWritable.set((short) i.get());
      return shortWritable;
    }
  }

  public ShortWritable evaluate(Text i) {
    if (i == null) {
      return null;
    } else {
      try {
        shortWritable.set(LazyShort.parseShort(i.getBytes(), 0, i.getLength(),
            10));
        return shortWritable;
      } catch (NumberFormatException e) {
        return null;
      }
    }
  }

  public ShortWritable evaluate(TimestampWritable i) {
    if (i == null) {
      return null;
    } else {
      shortWritable.set((short) i.getSeconds());
      return shortWritable;
    }
  }
}
