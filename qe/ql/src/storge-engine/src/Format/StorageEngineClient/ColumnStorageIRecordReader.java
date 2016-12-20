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
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import FormatStorage1.IColumnDataFile;
import FormatStorage1.IFormatDataFile;
import FormatStorage1.IRecord;

public class ColumnStorageIRecordReader implements
    RecordReader<LongWritable, IRecord> {
  public static final Log LOG = LogFactory
      .getLog(ColumnStorageIRecordReader.class);
  Configuration conf;

  private IColumnDataFile icdf;
  int recnum = 0;
  int currentrec = 0;

  @SuppressWarnings("deprecation")
  public ColumnStorageIRecordReader(CombineFileSplit split, Configuration conf,
      Reporter report, Integer idx) throws IOException {
    int id = idx.intValue();
    this.conf = conf;
    Path p = split.getPath(id);
    String file = p.toString();
    if (!file.contains("_idx")) {
      return;
    }

    IFormatDataFile ifdf = new IFormatDataFile(conf);
    ifdf.open(file);
    int segid = (int) (split.getOffset(id) / p.getFileSystem(conf)
        .getBlockSize(p));
    int beginline = 0;
    if (ifdf.recnum() > 0) {
      beginline = ifdf.segIndex().getILineIndex(segid).beginline();
      this.recnum = ifdf.segIndex().getILineIndex(segid).recnum();
    } else {
      this.recnum = 0;
    }
    ifdf.close();

    String cfile = p.toString().substring(0, p.toString().lastIndexOf("_idx"));

    icdf = new IColumnDataFile(conf);
    icdf.open(cfile);
    icdf.seek(beginline);
  }

  public ColumnStorageIRecordReader(Configuration conf,
      FormatStorageInputSplit split) throws IOException {
    this.conf = conf;
    String file = split.getPath().toString();
    icdf = new IColumnDataFile(conf);
    icdf.open(file);
    if (split.wholefileASasplit) {
      this.recnum = icdf.recnum();
      icdf.seek(0);
    } else {
      this.recnum = split.recnum;
      icdf.seek(split.beginline);
    }
  }

  @Override
  public boolean next(LongWritable key, IRecord value) throws IOException {
    if (currentrec >= recnum || icdf == null)
      return false;
    key.set(currentrec);
    if (!icdf.next(value))
      return false;
    currentrec++;
    return true;
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
    if (icdf == null)
      return new IRecord();
    return icdf.getIRecordObj();
  }

  @Override
  public long getPos() throws IOException {
    return currentrec;
  }

  @Override
  public float getProgress() throws IOException {
    return (float) currentrec / recnum;
  }
}
