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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.Progressable;

import FormatStorage1.IColumnDataFile;
import FormatStorage1.IRecord;

@SuppressWarnings("deprecation")
public class MultiColumnStorageRecordReader<K, V> implements
    RecordReader<LongWritable, IRecord> {
  public static final Log LOG = LogFactory
      .getLog(MultiColumnStorageRecordReader.class);

  Configuration conf;
  Progressable progress;

  IColumnDataFile icdf = null;

  int currentFile = 0;
  int currentrecnum = 0;
  Path[] paths;

  public MultiColumnStorageRecordReader(Configuration conf, InputSplit split)
      throws IOException {
    this.conf = conf;

    if (((MultiFormatStorageSplit) split).getLength() == 0) {
      LOG.info("split.len = 0");
      return;
    }
    paths = ((MultiFormatStorageSplit) split).getAllPath();
    icdf = new IColumnDataFile(conf);
    icdf.open(paths[0].toString());
  }

  @Override
  public boolean next(LongWritable key, IRecord value) throws IOException {
    if (paths == null || paths.length == 0 || icdf == null)
      return false;

    key.set(currentrecnum++);
    if (icdf.next(value))
      return true;
    currentFile++;
    while (currentFile < paths.length) {
      icdf.close();
      icdf = new IColumnDataFile(conf);
      icdf.open(paths[currentFile].toString());
      if (icdf.next(value))
        return true;
      currentFile++;
    }
    return false;

  }

  @Override
  public void close() throws IOException {
    if (icdf != null) {
      try {
        icdf.close();
      } catch (Exception e) {
        LOG.error("close fail:" + e.getMessage());
      }
    }
  }

  @Override
  public LongWritable createKey() {
    return new LongWritable();
  }

  @Override
  public IRecord createValue() {
    IRecord record = icdf.getIRecordObj();
    return record;
  }

  @Override
  public long getPos() throws IOException {
    return currentrecnum;
  }

  @Override
  public float getProgress() throws IOException {
    return 0;
  }
}
