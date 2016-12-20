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

package StorageEngineClient;

import java.io.*;
import java.lang.reflect.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.Configuration;

import FormatStorage1.IRecord;

public class CombineFileRecordReader<K, V> implements RecordReader<K, V> {
  public static final Log LOG = LogFactory
      .getLog(CombineFileRecordReader.class);

  @SuppressWarnings("unchecked")
  static final Class[] constructorSignature = new Class[] {
      CombineFileSplit.class, Configuration.class, Reporter.class,
      Integer.class };

  protected CombineFileSplit split;
  @SuppressWarnings("deprecation")
  protected JobConf jc;
  protected Reporter reporter;
  protected Class<? extends RecordReader<K, V>> rrClass;
  protected Constructor<? extends RecordReader<K, V>> rrConstructor;
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

  @SuppressWarnings("deprecation")
  public CombineFileRecordReader(JobConf job, CombineFileSplit split,
      Reporter reporter, Class<? extends RecordReader<K, V>> rrClass)
      throws IOException {
    this.split = split;
    this.jc = job;
    this.rrClass = rrClass;
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

  private boolean initNextRecordReader(V value) throws IOException {
    if (!this.initNextRecordReader())
      return false;
    if (curReader instanceof FormatStorageIRecordReader) {
      ((FormatStorageIRecordReader) curReader).reset((IRecord) value);
    }
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
      curReader = rrConstructor.newInstance(new Object[] { split, jc, reporter,
          Integer.valueOf(idx) });

      jc.set("map.input.file", split.getPath(idx).toString());
      jc.setLong("map.input.start", split.getOffset(idx));
      jc.setLong("map.input.length", split.getLength(idx));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    idx++;
    return true;
  }
}
