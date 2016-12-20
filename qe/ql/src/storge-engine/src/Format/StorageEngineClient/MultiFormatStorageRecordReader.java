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

import FormatStorage1.IFormatDataFile;
import FormatStorage1.IRecord;

@SuppressWarnings("deprecation")
public class MultiFormatStorageRecordReader<K, V> implements
    RecordReader<LongWritable, IRecord> {
  public static final Log LOG = LogFactory
      .getLog(MultiFormatStorageRecordReader.class);

  Configuration conf;
  Progressable progress;

  IFormatDataFile ifdf = null;

  int currentFile = 0;
  int currentrecnum = 0;
  Path[] paths;

  public MultiFormatStorageRecordReader(Configuration conf, InputSplit split)
      throws IOException {
    this.conf = conf;

    if (((MultiFormatStorageSplit) split).getLength() == 0) {
      LOG.info("split.len = 0");
      return;
    }
    paths = ((MultiFormatStorageSplit) split).getAllPath();
    ifdf = new IFormatDataFile(conf);
    ifdf.open(paths[0].toString());
    ifdf.seek(0);
  }

  @Override
  public boolean next(LongWritable key, IRecord value) throws IOException {
    if (paths == null || paths.length == 0 || ifdf == null)
      return false;
    key.set(currentrecnum++);
    if (ifdf.next(value))
      return true;
    while (++currentFile < paths.length) {
      ifdf.close();
      ifdf = new IFormatDataFile(conf);
      ifdf.open(paths[currentFile].toString());
      ifdf.seek(0);
      if (ifdf.next(value))
        return true;
    }
    return false;
  }

  @Override
  public void close() throws IOException {
    if (ifdf != null) {
      try {
        ifdf.close();
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
    IRecord record = ifdf.getIRecordObj();
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
