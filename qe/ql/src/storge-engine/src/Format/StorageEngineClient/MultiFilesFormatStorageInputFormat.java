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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MultiFileInputFormat;
import org.apache.hadoop.mapred.MultiFileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import FormatStorage1.IFormatDataFile;
import FormatStorage1.IRecord;

@SuppressWarnings({ "unchecked", "deprecation" })
public class MultiFilesFormatStorageInputFormat extends MultiFileInputFormat {

  @Override
  public RecordReader getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {
    return null;
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {

    FileStatus[] fileStatuss = listStatus(job);
    List<MultiFileSplit> splits = new ArrayList<MultiFileSplit>(Math.min(
        numSplits, fileStatuss.length));
    if (fileStatuss.length != 0) {
      int splitSize = 0;
      ArrayList<Path> paths = new ArrayList<Path>();
      ArrayList<Long> pathlengths = new ArrayList<Long>();
      for (int i = 0; i < fileStatuss.length; i++) {
        long blocksize = fileStatuss[i].getBlockSize();
        if (fileStatuss[i].getLen() > blocksize) {

        } else if (splitSize + fileStatuss[i].getLen() < blocksize * 0.8) {
          splitSize += fileStatuss[i].getLen();
          paths.add(fileStatuss[i].getPath());
          pathlengths.add(fileStatuss[i].getLen());
        } else {
          Path[] splitPaths = paths.toArray(new Path[paths.size()]);
          long[] splitLengths = new long[pathlengths.size()];
          for (int j = 0; j < splitLengths.length; j++) {
            splitLengths[j] = pathlengths.get(j);
          }
          splits.add(new MultiFileSplit(job, splitPaths, splitLengths));
          pathlengths.clear();
          paths.clear();
          splitSize += fileStatuss[i].getLen();
          paths.add(fileStatuss[i].getPath());
          pathlengths.add(fileStatuss[i].getLen());
        }
      }
      Path[] splitPaths = paths.toArray(new Path[paths.size()]);
      long[] splitLengths = new long[pathlengths.size()];
      for (int j = 0; j < splitLengths.length; j++) {
        splitLengths[j] = pathlengths.get(j);
      }
      splits.add(new MultiFileSplit(job, splitPaths, splitLengths));
    }
    return splits.toArray(new MultiFileSplit[splits.size()]);
  }

  public static class MultiFileFormatIRecordReader implements
      RecordReader<LongWritable, IRecord> {

    private MultiFileSplit split;
    private long offset;
    private long totLength;
    private int count = 0;
    private Path[] paths;

    private IFormatDataFile ifdf;

    public MultiFileFormatIRecordReader(Configuration conf, MultiFileSplit split)
        throws IOException {

      this.split = split;
      this.paths = split.getPaths();
      this.totLength = split.getLength();
      this.offset = 0;

      Path file = paths[count];
      ifdf = new IFormatDataFile(conf);
      ifdf.open(file.toString());
    }

    public void close() throws IOException {
      ifdf.close();
    }

    public long getPos() throws IOException {
      long currentOffset = ifdf == null ? 0 : ifdf.fileInfo().in().getPos();
      return offset + currentOffset;
    }

    public float getProgress() throws IOException {
      return ((float) getPos()) / totLength;
    }

    public boolean next(LongWritable key, IRecord value) throws IOException {
      if (count >= split.getNumPaths())
        return false;

      return true;
    }

    public LongWritable createKey() {
      return new LongWritable();
    }

    public IRecord createValue() {
      return null;
    }
  }

}
