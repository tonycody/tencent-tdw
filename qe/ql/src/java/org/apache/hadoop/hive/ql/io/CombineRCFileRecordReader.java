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

import java.io.*;
import java.lang.reflect.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.Configuration;

import StorageEngineClient.CombineFileSplit;

public class CombineRCFileRecordReader<K, V> implements RecordReader<K, V> {
  public static final Log LOG = LogFactory
      .getLog(CombineRCFileRecordReader.class);

  @SuppressWarnings("deprecation")
  public CombineRCFileRecordReader(JobConf job, CombineFileSplit split,
      Reporter reporter) throws IOException {
    this.split = split;
    this.jc = job;
    this.rrClass = RCFileRecordReader.class;
    this.reporter = reporter;
    this.idx = 0;
    this.curReader = null;
    this.progress = 0;

    try {
      rrConstructor = rrClass.getDeclaredConstructor(constructorSignature);
      rrConstructor.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException(rrClass.getName()
          + " does not have valid constructor", e);
    }
    initNextRecordReader();
  }

  @SuppressWarnings("unchecked")
  static final Class[] constructorSignature = new Class[] {
      Configuration.class, FileSplit.class };

  protected CombineFileSplit split;
  @SuppressWarnings("deprecation")
  protected JobConf jc;
  protected Reporter reporter;
  protected Class<RCFileRecordReader> rrClass;
  protected Constructor<RCFileRecordReader> rrConstructor;
  protected FileSystem fs;

  protected int idx;
  protected long progress;
  protected RecordReader<K, V> curReader;

  public boolean next(K key, V value) throws IOException {
    try {
      while ((curReader == null) || !curReader.next(key, value)) {
        if (!initNextRecordReader(value)) {
          return false;
        }
      }
    } catch (IOException ioe) {
      String BrokenLog = "the file : " + jc.get("map.input.file")
          + " is broken, please check it";
      throw new IOException(BrokenLog);
    }
    return true;
  }

  public K createKey() {
    return curReader.createKey();
  }

  public V createValue() {
    return curReader.createValue();
  }

  public long getPos() throws IOException {
    return progress;
  }

  public void close() throws IOException {
    if (curReader != null) {
      curReader.close();
      curReader = null;
    }
  }

  public float getProgress() throws IOException {
    return Math.min(1.0f, progress / (float) (split.getLength()));
  }

  private boolean initNextRecordReader(V value) throws IOException {
    if (!this.initNextRecordReader())
      return false;
    return true;
  }

  protected boolean initNextRecordReader() throws IOException {
    if (curReader != null) {
      curReader.close();
      curReader = null;
      if (idx > 0) {
        progress += split.getLength(idx - 1);
      }
    }

    if (idx == split.getNumPaths()) {
      return false;
    }

    try {
      FileSplit fileSplit = new FileSplit(split.getPath(idx),
          split.getOffset(idx), split.getLength(idx), (String[]) null);
      curReader = rrConstructor.newInstance(new Object[] { jc, fileSplit });

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    idx++;
    return true;
  }
}
