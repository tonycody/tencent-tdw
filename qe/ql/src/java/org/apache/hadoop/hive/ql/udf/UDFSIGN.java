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
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

@description(name = "sign", value = "_FUNC_(expr) - returns a value indicating the sign of a number", extended = "Example:\n "
    + "  > SELECT _FUNC_sign(-23) FROM src LIMIT 1;\n" + "  -1")
public class UDFSIGN extends UDF {

  private static Log LOG = LogFactory.getLog(UDFSIGN.class.getName());
  private ByteWritable byteWritable = new ByteWritable();

  public UDFSIGN() {
  }

  public ByteWritable evaluate(ByteWritable a) {
    if (a == null) {
      return null;
    }
    if (a.get() > (byte) 0) {
      byteWritable.set((byte) 1);
      return byteWritable;
    } else if (a.get() < (byte) 0) {
      byteWritable.set((byte) -1);
      return byteWritable;
    } else {
      byteWritable.set((byte) 0);
      return byteWritable;
    }
  }

  public ByteWritable evaluate(ShortWritable a) {
    if (a == null) {
      return null;
    }
    if (a.get() > (short) 0) {
      byteWritable.set((byte) 1);
      return byteWritable;
    } else if (a.get() < (short) 0) {
      byteWritable.set((byte) -1);
      return byteWritable;
    } else {
      byteWritable.set((byte) 0);
      return byteWritable;
    }
  }

  public ByteWritable evaluate(IntWritable a) {
    if (a == null) {
      return null;
    }
    if (a.get() > 0) {
      byteWritable.set((byte) 1);
      return byteWritable;
    } else if (a.get() < 0) {
      byteWritable.set((byte) -1);
      return byteWritable;
    } else {
      byteWritable.set((byte) 0);
      return byteWritable;
    }
  }

  public ByteWritable evaluate(LongWritable a) {
    if (a == null) {
      return null;
    }
    if (a.get() > (long) 0) {
      byteWritable.set((byte) 1);
      return byteWritable;
    } else if (a.get() < (long) 0) {
      byteWritable.set((byte) -1);
      return byteWritable;
    } else {
      byteWritable.set((byte) 0);
      return byteWritable;
    }
  }

  public ByteWritable evaluate(FloatWritable a) {
    if (a == null) {
      return null;
    }
    int tmp_a = Float.compare(a.get(), (float) 0.0);
    int tmp_b = Float.compare(a.get(), (float) -0.0);
    if (tmp_a > 0) {
      byteWritable.set((byte) 1);
      return byteWritable;
    } else if (tmp_b < 0) {
      byteWritable.set((byte) -1);
      return byteWritable;
    } else {
      byteWritable.set((byte) 0);
      return byteWritable;
    }
  }

  public ByteWritable evaluate(DoubleWritable a) {
    if (a == null) {
      return null;
    }
    int tmp_a = Double.compare(a.get(), (double) 0.0);
    int tmp_b = Double.compare(a.get(), (double) -0.0);
    if (tmp_a > 0) {
      byteWritable.set((byte) 1);
      return byteWritable;
    } else if (tmp_b < 0) {
      byteWritable.set((byte) -1);
      return byteWritable;
    } else {
      byteWritable.set((byte) 0);
      return byteWritable;
    }
  }

}
