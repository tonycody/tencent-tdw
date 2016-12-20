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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class CombineTextFileInputFormat extends
    CombineFileInputFormat<LongWritable, Text> {
  public static final Log LOG = LogFactory
      .getLog(CombineTextFileInputFormat.class);

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    InputSplit[] res = super.getSplits(job, numSplits);
    if (res.length == 0) {
      InputSplit[] res1 = super.getSplits1(job, numSplits);
      res = new InputSplit[res1.length];
      for (int i = 0; i < res1.length; i++) {
        res[i] = new CombineFileSplit(job,
            new Path[] { ((FileSplit) res1[i]).getPath() },
            new long[] { ((FileSplit) res1[i]).getStart() },
            new long[] { res1[i].getLength() },
            ((FileSplit) res1[i]).getLocations());
      }
    }
    LOG.info("splits #:\t" + res.length);
    return res;
  }

  @Override
  public RecordReader<LongWritable, Text> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {
    return new CombineFileRecordReader<LongWritable, Text>(job,
        (CombineFileSplit) split, reporter, TextLineRecordReader.class);
  }

  public static class TextLineRecordReader extends MyLineRecordReader {

    public TextLineRecordReader(Configuration job, FileSplit split)
        throws IOException {
      super(job, split);
    }

    public TextLineRecordReader(CombineFileSplit split, Configuration conf,
        Reporter report, Integer idx) throws IOException {
      super(conf, new FileSplit(split.getPath(idx.intValue()),
          split.getOffset(idx.intValue()), split.getLength(idx.intValue()),
          (JobConf) conf));
      LOG.info("open text file:\t" + split.getPath(idx.intValue()).toString());
    }
  }
}
