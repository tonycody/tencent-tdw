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
import java.io.EOFException;
import java.io.InputStream;
import java.io.DataInputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RecordReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configurable;

import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Deserializer;

import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;

public class FlatFileInputFormat<T> extends
    FileInputFormat<Void, FlatFileInputFormat.RowContainer<T>> {

  static public class RowContainer<T> {
    T row;
  }

  static public interface SerializationContext<S> extends Configurable {

    public Serialization<S> getSerialization() throws IOException;

    public Class<? extends S> getRealClass() throws IOException;
  }

  static public final String SerializationImplKey = "mapred.input.serialization.implKey";

  static public class SerializationContextFromConf<S> implements
      FlatFileInputFormat.SerializationContext<S> {

    static public final String SerializationSubclassKey = "mapred.input.serialization.subclassKey";

    private Configuration conf;

    public void setConf(Configuration conf) {
      this.conf = conf;
    }

    public Configuration getConf() {
      return conf;
    }

    public Class<S> getRealClass() throws IOException {
      return (Class<S>) conf.getClass(SerializationSubclassKey, null,
          Object.class);
    }

    public Serialization<S> getSerialization() throws IOException {
      Class<Serialization<S>> tClass = (Class<Serialization<S>>) conf.getClass(
          SerializationImplKey, null, Serialization.class);
      return tClass == null ? null : (Serialization<S>) ReflectionUtils
          .newInstance(tClass, conf);
    }
  }

  public class FlatFileRecordReader<R> implements
      RecordReader<Void, FlatFileInputFormat.RowContainer<R>> {

    private final DataInputStream in;

    private final InputStream dcin;

    private final FSDataInputStream fsin;

    private final long end;

    private final Deserializer<R> deserializer;

    private boolean isEOF;

    private Configuration conf;

    private Class<R> realRowClass;

    public FlatFileRecordReader(Configuration conf, FileSplit split)
        throws IOException {
      final Path path = split.getPath();
      FileSystem fileSys = path.getFileSystem(conf);
      CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(
          conf);
      final CompressionCodec codec = compressionCodecs.getCodec(path);
      this.conf = conf;

      fsin = fileSys.open(path);
      if (codec != null) {
        dcin = codec.createInputStream(fsin);
        in = new DataInputStream(dcin);
      } else {
        dcin = null;
        in = fsin;
      }

      isEOF = false;
      end = split.getLength();

      SerializationContext<R> sinfo;
      Class<SerializationContext<R>> sinfoClass = (Class<SerializationContext<R>>) conf
          .getClass(SerializationContextImplKey,
              SerializationContextFromConf.class);

      sinfo = (SerializationContext<R>) ReflectionUtils.newInstance(sinfoClass,
          conf);

      Serialization<R> serialization = sinfo.getSerialization();
      realRowClass = (Class<R>) sinfo.getRealClass();

      deserializer = (Deserializer<R>) serialization
          .getDeserializer((Class<R>) realRowClass);
      deserializer.open(in);
    }

    private Class<R> realRowclass;

    static public final String SerializationContextImplKey = "mapred.input.serialization.context_impl";

    public Void createKey() {
      return null;
    }

    public RowContainer<R> createValue() {
      RowContainer<R> r = new RowContainer<R>();
      r.row = (R) ReflectionUtils.newInstance(realRowClass, conf);
      return r;
    }

    public synchronized boolean next(Void key, RowContainer<R> value)
        throws IOException {
      if (isEOF || in.available() == 0) {
        isEOF = true;
        return false;
      }

      try {
        value.row = deserializer.deserialize(value.row);
        if (value.row == null) {
          isEOF = true;
          return false;
        }
        return true;
      } catch (EOFException e) {
        isEOF = true;
        return false;
      }
    }

    public synchronized float getProgress() throws IOException {
      if (end == 0) {
        return 0.0f;
      } else {
        return Math.min(1.0f, fsin.getPos() / (float) (end));
      }
    }

    public synchronized long getPos() throws IOException {
      return fsin.getPos();
    }

    public synchronized void close() throws IOException {
      deserializer.close();
    }
  }

  protected boolean isSplittable(FileSystem fs, Path filename) {
    return false;
  }

  public RecordReader<Void, RowContainer<T>> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {

    reporter.setStatus(split.toString());

    return new FlatFileRecordReader<T>(job, (FileSplit) split);
  }
}
