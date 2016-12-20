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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MultiFileInputFormat;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import FormatStorage1.IRecord;

@SuppressWarnings("deprecation")
public class HashMultiFileFormatStorageInputFormat<K, V> extends
    MultiFileInputFormat<LongWritable, IRecord> {
  public static final Log LOG = LogFactory
      .getLog(HashMultiFileFormatStorageInputFormat.class);

  public HashMultiFileFormatStorageInputFormat() {
  }

  public RecordReader<LongWritable, IRecord> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {
    reporter.setStatus(split.toString());

    return new MultiFormatStorageRecordReader<K, V>(job,
        (MultiFormatStorageSplit) split);
  }

  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {

    List<InputSplit> splits = new ArrayList<InputSplit>(numSplits);

    List<Path> paths = new ArrayList<Path>(10);
    int count = 0;
    BlockLocation[] blkLocations = null;
    for (FileStatus file : listStatus(job)) {
      Path path = file.getPath();

      FileSystem fs = path.getFileSystem(job);
      long length = file.getLen();

      if (count == 0) {
        blkLocations = fs.getFileBlockLocations(file, 0, length);
        count++;
      }

      paths.add(path);
    }

    if (paths.size() == 0 || blkLocations == null) {
      splits.add(new MultiFormatStorageSplit(new Path[0], new String[0]));
      return splits.toArray(new MultiFormatStorageSplit[splits.size()]);
    }

    int blkIndex = getBlockIndex(blkLocations, 0);
    MultiFormatStorageSplit split = new MultiFormatStorageSplit(
        paths.toArray(new Path[paths.size()]),
        blkLocations[blkIndex].getHosts());
    splits.add(split);

    LOG.info("Total # of splits: " + splits.size());
    return splits.toArray(new MultiFormatStorageSplit[splits.size()]);
  }
}
