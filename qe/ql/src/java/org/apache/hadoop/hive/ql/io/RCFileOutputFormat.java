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

package org.apache.hadoop.hive.ql.io;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import com.hadoop.compression.lzo.LzoCodec;

import Comm.ConstVar;

public class RCFileOutputFormat extends
    FileOutputFormat<WritableComparable, BytesRefArrayWritable> implements
    HiveOutputFormat<WritableComparable, BytesRefArrayWritable> {

  private static final Log LOG = LogFactory.getLog(RCFileOutputFormat.class);

  public static void setColumnNumber(Configuration conf, int columnNum) {
    assert columnNum > 0;
    conf.setInt(RCFile.COLUMN_NUMBER_CONF_STR, columnNum);
  }

  public static int getColumnNumber(Configuration conf) {
    return conf.getInt(RCFile.COLUMN_NUMBER_CONF_STR, 0);
  }

  @Override
  public RecordWriter<WritableComparable, BytesRefArrayWritable> getRecordWriter(
      FileSystem ignored, JobConf job, String name, Progressable progress)
      throws IOException {

    Path outputPath = getWorkOutputPath(job);
    FileSystem fs = outputPath.getFileSystem(job);
    Path file = new Path(outputPath, name);
    CompressionCodec codec = null;
    String rcfCompressionOn = job.get("rcfile.head.compression", "true");
    if (rcfCompressionOn != null && rcfCompressionOn.equalsIgnoreCase("true")) {
      String compressName = job.get("rcfile.head.compressionMethod");
      if (compressName != null) {
        Class<?> codecClass;
        try {
          codecClass = job.getClassByName(compressName).asSubclass(
              CompressionCodec.class);
        } catch (ClassNotFoundException e) {
          throw new IllegalArgumentException("Compression codec "
              + compressName + " was not found.", e);
        }
        codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, job);
        LOG.info("rcfile output codec:" + compressName);
      } else {
        codec = new LzoCodec();
        ((LzoCodec) codec).setConf(job);
        LOG.info("rcfile output codec: LzoCodec");
      }
      String compressionLevel = job.get("rcfile.head.zlibCompressionLevel");
      if (compressionLevel != null) {
        if (compressionLevel.equalsIgnoreCase("bestSpeed")) {
          job.set("zlib.compress.level",
              ZlibCompressor.CompressionLevel.BEST_SPEED.toString());
        } else if (compressionLevel.equalsIgnoreCase("bestCompression")) {
          job.set("zlib.compress.level",
              ZlibCompressor.CompressionLevel.BEST_COMPRESSION.toString());
        }
      }
    }
    final RCFile.Writer out = new RCFile.Writer(fs, job, file, progress, codec);

    return new RecordWriter<WritableComparable, BytesRefArrayWritable>() {

      @Override
      public void close(Reporter reporter) throws IOException {
        out.close();
      }

      @Override
      public void write(WritableComparable key, BytesRefArrayWritable value)
          throws IOException {
        out.append(value);
      }
    };
  }

  @Override
  public org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter getHiveRecordWriter(
      JobConf jc, Path finalOutPath, Class<? extends Writable> valueClass,
      boolean isCompressed, Properties tableProperties, Progressable progress)
      throws IOException {

    String[] cols = null;
    String columns = tableProperties.getProperty("columns");
    if (columns == null || columns.trim().equals("")) {
      cols = new String[0];
    } else {
      cols = StringUtils.split(columns, ",");
    }
    RCFileOutputFormat.setColumnNumber(jc, cols.length);

    CompressionCodec codec = null;
    String compress = tableProperties.getProperty("compress");
    String rcfCompressionOn = jc.get("rcfile.head.compression", "true");
    if ((rcfCompressionOn != null && rcfCompressionOn.equalsIgnoreCase("true"))
        || (compress != null && compress.equalsIgnoreCase("true"))) {
      String compressName = jc.get("rcfile.head.compressionMethod");
      if (compressName != null) {
        Class<?> codecClass;
        try {
          codecClass = jc.getClassByName(compressName).asSubclass(
              CompressionCodec.class);
        } catch (ClassNotFoundException e) {
          throw new IllegalArgumentException("Compression codec "
              + compressName + " was not found.", e);
        }
        codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, jc);
        LOG.info("rcfile output codec:" + compressName);
      } else {
        codec = new LzoCodec();
        ((LzoCodec) codec).setConf(jc);
        LOG.info("rcfile output codec: LzoCodec");
      }
      String compressionLevel = jc.get("rcfile.head.zlibCompressionLevel");
      if (compressionLevel != null) {
        if (compressionLevel.equalsIgnoreCase("bestSpeed")) {
          jc.set("zlib.compress.level",
              ZlibCompressor.CompressionLevel.BEST_SPEED.toString());
        } else if (compressionLevel.equalsIgnoreCase("bestCompression")) {
          jc.set("zlib.compress.level",
              ZlibCompressor.CompressionLevel.BEST_COMPRESSION.toString());
        }
      }
    }

    final RCFile.Writer outWriter = new RCFile.Writer(
        finalOutPath.getFileSystem(jc), jc, finalOutPath, progress, codec);

    return new org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter() {
      public void write(Writable r) throws IOException {
        outWriter.append(r);
      }

      public void close(boolean abort) throws IOException {
        outWriter.close();
      }
    };
  }
}
