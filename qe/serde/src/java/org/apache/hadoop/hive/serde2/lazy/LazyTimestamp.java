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
package org.apache.hadoop.hive.serde2.lazy;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyTimestampObjectInspector;

public class LazyTimestamp extends
    LazyPrimitive<LazyTimestampObjectInspector, TimestampWritable> {
  static final private Log LOG = LogFactory.getLog(LazyTimestamp.class);

  public LazyTimestamp(LazyTimestampObjectInspector oi) {
    super(oi);
    data = new TimestampWritable();
  }

  public LazyTimestamp(LazyTimestamp copy) {
    super(copy);
    data = new TimestampWritable(copy.data);
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {

    String s = null;
    try {
      s = new String(bytes.getData(), start, length, "US-ASCII");
    } catch (UnsupportedEncodingException e) {
      LOG.error(e);
      s = "";
    }
    Timestamp t;
    if (s.compareTo("NULL") == 0) {
      t = null;
    } else {
      try {
        t = Timestamp.valueOf(s);
      } catch (Exception e) {
        LOG.error("LazyTimestamp init failed: " + s);
        t = null;
      }
    }
    data.set(t);
  }

  private static final String nullTimestamp = "NULL";

  public static void writeUTF8(OutputStream out, TimestampWritable i)
      throws IOException {
    if (i == null) {
      out.write(TimestampWritable.nullBytes);
    } else {
      out.write(i.toString().getBytes("US-ASCII"));
    }
  }

  @Override
  public TimestampWritable getWritableObject() {
    return data;
  }
}
