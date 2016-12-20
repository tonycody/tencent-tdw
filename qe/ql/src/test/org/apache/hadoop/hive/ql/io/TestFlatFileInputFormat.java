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

import java.io.*;
import java.util.*;
import junit.framework.TestCase;

import org.apache.commons.logging.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.record.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.serializer.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.thrift.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

public class TestFlatFileInputFormat extends TestCase {

  public void testFlatFileInputJava() throws Exception {
    Configuration conf;
    JobConf job;
    FileSystem fs;
    Path dir;
    Path file;
    Reporter reporter;
    FSDataOutputStream ds;

    try {
      conf = new Configuration();
      job = new JobConf(conf);
      fs = FileSystem.getLocal(conf);
      dir = new Path(System.getProperty("test.data.dir", ".") + "/mapred");
      file = new Path(dir, "test.txt");
      reporter = Reporter.NULL;
      fs.delete(dir, true);

      job.setClass(FlatFileInputFormat.SerializationImplKey,
          org.apache.hadoop.io.serializer.JavaSerialization.class,
          org.apache.hadoop.io.serializer.Serialization.class);

      job.setClass(
          FlatFileInputFormat.SerializationContextFromConf.SerializationSubclassKey,
          JavaTestObjFlatFileInputFormat.class, java.io.Serializable.class);

      FileInputFormat.setInputPaths(job, dir);
      ds = fs.create(file);
      Serializer serializer = new JavaSerialization().getSerializer(null);

      serializer.open(ds);
      for (int i = 0; i < 10; i++) {
        serializer.serialize(new JavaTestObjFlatFileInputFormat("Hello World! "
            + String.valueOf(i), i));
      }
      serializer.close();

      FileInputFormat<Void, FlatFileInputFormat.RowContainer<Serializable>> format = new FlatFileInputFormat<Serializable>();
      InputSplit[] splits = format.getSplits(job, 1);

      RecordReader<Void, FlatFileInputFormat.RowContainer<Serializable>> reader = format
          .getRecordReader(splits[0], job, reporter);

      Void key = reader.createKey();
      FlatFileInputFormat.RowContainer<Serializable> value = reader
          .createValue();

      int count = 0;
      while (reader.next(key, value)) {
        assertTrue(key == null);
        assertTrue(((JavaTestObjFlatFileInputFormat) value.row).s
            .equals("Hello World! " + String.valueOf(count)));
        assertTrue(((JavaTestObjFlatFileInputFormat) value.row).num == count);
        count++;
      }
      reader.close();

    } catch (Exception e) {
      System.err.println("caught: " + e);
      e.printStackTrace();
    } finally {
    }

  }

  public void testFlatFileInputRecord() throws Exception {
    Configuration conf;
    JobConf job;
    FileSystem fs;
    Path dir;
    Path file;
    Reporter reporter;
    FSDataOutputStream ds;

    try {
      conf = new Configuration();
      job = new JobConf(conf);
      fs = FileSystem.getLocal(conf);
      dir = new Path(System.getProperty("test.data.dir", ".") + "/mapred");
      file = new Path(dir, "test.txt");
      reporter = Reporter.NULL;
      fs.delete(dir, true);

      job.setClass(FlatFileInputFormat.SerializationImplKey,
          org.apache.hadoop.io.serializer.WritableSerialization.class,
          org.apache.hadoop.io.serializer.Serialization.class);

      job.setClass(
          FlatFileInputFormat.SerializationContextFromConf.SerializationSubclassKey,
          RecordTestObj.class, Writable.class);

      FileInputFormat.setInputPaths(job, dir);
      ds = fs.create(file);
      Serializer serializer = new WritableSerialization()
          .getSerializer(Writable.class);

      serializer.open(ds);
      for (int i = 0; i < 10; i++) {
        serializer.serialize(new RecordTestObj("Hello World! "
            + String.valueOf(i), i));
      }
      serializer.close();

      FileInputFormat<Void, FlatFileInputFormat.RowContainer<Writable>> format = new FlatFileInputFormat<Writable>();
      InputSplit[] splits = format.getSplits(job, 1);

      RecordReader<Void, FlatFileInputFormat.RowContainer<Writable>> reader = format
          .getRecordReader(splits[0], job, reporter);

      Void key = reader.createKey();
      FlatFileInputFormat.RowContainer<Writable> value = reader.createValue();

      int count = 0;
      while (reader.next(key, value)) {
        assertTrue(key == null);
        assertTrue(((RecordTestObj) value.row).getS().equals(
            "Hello World! " + String.valueOf(count)));
        assertTrue(((RecordTestObj) value.row).getNum() == count);
        count++;
      }
      reader.close();

    } catch (Exception e) {
      System.err.println("caught: " + e);
      e.printStackTrace();
    } finally {
    }

  }

  public static void main(String[] args) throws Exception {
    new TestFlatFileInputFormat().testFlatFileInputJava();
    new TestFlatFileInputFormat().testFlatFileInputRecord();
  }
}
