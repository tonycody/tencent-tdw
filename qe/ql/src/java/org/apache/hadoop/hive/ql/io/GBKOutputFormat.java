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
package org.apache.hadoop.hive.ql.io;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.*;

import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

@Deprecated
public class GBKOutputFormat<K, V> extends FileOutputFormat<K, V> {

  protected static class LineRecordWriter<K, V> implements RecordWriter<K, V> {
    private static final String gbk = "gbk";
    private static final byte[] newline;
    static {
      try {
        newline = "\n".getBytes(gbk);
      } catch (UnsupportedEncodingException uee) {
        throw new IllegalArgumentException("can't find " + gbk + " encoding");
      }
    }

    protected DataOutputStream out;
    private final byte[] keyValueSeparator;

    public LineRecordWriter(DataOutputStream out, String keyValueSeparator) {
      this.out = out;
      try {
        this.keyValueSeparator = keyValueSeparator.getBytes(gbk);
      } catch (UnsupportedEncodingException uee) {
        throw new IllegalArgumentException("can't find " + gbk + " encoding");
      }
    }

    public LineRecordWriter(DataOutputStream out) {
      this(out, "\t");
    }

    private void writeObject(Object o) throws IOException {
      if (o instanceof Text) {

        out.write(o.toString().getBytes(gbk));
      }
    }

    public synchronized void write(K key, V value) throws IOException {

      boolean nullKey = key == null || key instanceof NullWritable;
      boolean nullValue = value == null || value instanceof NullWritable;
      if (nullKey && nullValue) {
        return;
      }
      if (!nullKey) {
        writeObject(key);
      }
      if (!(nullKey || nullValue)) {
        out.write(keyValueSeparator);
      }
      if (!nullValue) {
        writeObject(value);
      }
      out.write(newline);
    }

    public synchronized void close(Reporter reporter) throws IOException {
      out.close();
    }
  }

  public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job,
      String name, Progressable progress) throws IOException {
    boolean isCompressed = getCompressOutput(job);
    String keyValueSeparator = job.get("mapred.textoutputformat.separator",
        "\t");
    if (!isCompressed) {
      Path file = FileOutputFormat.getTaskOutputPath(job, name);
      FileSystem fs = file.getFileSystem(job);
      FSDataOutputStream fileOut = fs.create(file, progress);
      return new LineRecordWriter<K, V>(fileOut, keyValueSeparator);
    } else {
      Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
          job, GzipCodec.class);
      CompressionCodec codec = ReflectionUtils.newInstance(codecClass, job);
      Path file = FileOutputFormat.getTaskOutputPath(job,
          name + codec.getDefaultExtension());
      FileSystem fs = file.getFileSystem(job);
      FSDataOutputStream fileOut = fs.create(file, progress);
      return new LineRecordWriter<K, V>(new DataOutputStream(
          codec.createOutputStream(fileOut)), keyValueSeparator);
    }
  }
}
