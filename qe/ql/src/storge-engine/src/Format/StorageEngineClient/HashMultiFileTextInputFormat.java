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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MultiFileInputFormat;
import org.apache.hadoop.mapred.MultiFileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings("deprecation")
public class HashMultiFileTextInputFormat extends
    MultiFileInputFormat<WordOffset, Text> {
  public static final Log LOG = LogFactory
      .getLog(HashMultiFileTextInputFormat.class);

  public RecordReader<WordOffset, Text> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {
    return new MultiFileLineRecordReader(job, (MultiFileSplit) split);
  }

  public static class MultiFileLineRecordReader implements
      RecordReader<WordOffset, Text> {

    private MultiFileSplit split;
    private long offset;
    private long totLength;
    private FileSystem fs;
    private int count = 0;
    private Path[] paths;

    private FSDataInputStream currentStream;
    private BufferedReader currentReader;

    public MultiFileLineRecordReader(Configuration conf, MultiFileSplit split)
        throws IOException {

      this.split = split;
      fs = FileSystem.get(conf);
      this.paths = split.getPaths();
      this.totLength = split.getLength();
      this.offset = 0;

      Path file = paths[count];
      currentStream = fs.open(file);
      currentReader = new BufferedReader(new InputStreamReader(currentStream));
    }

    public void close() throws IOException {
    }

    public long getPos() throws IOException {
      long currentOffset = currentStream == null ? 0 : currentStream.getPos();
      return offset + currentOffset;
    }

    public float getProgress() throws IOException {
      return ((float) getPos()) / totLength;
    }

    public boolean next(WordOffset key, Text value) throws IOException {
      if (count >= split.getNumPaths())
        return false;

      String line;
      do {
        line = currentReader.readLine();
        if (line == null) {
          currentReader.close();
          offset += split.getLength(count);

          if (++count >= split.getNumPaths())
            return false;

          Path file = paths[count];
          currentStream = fs.open(file);
          currentReader = new BufferedReader(new InputStreamReader(
              currentStream));
          key.fileName = file.getName();
        }
      } while (line == null);
      key.offset = currentStream.getPos();
      value.set(line);

      return true;
    }

    public WordOffset createKey() {
      WordOffset wo = new WordOffset();
      wo.fileName = paths[0].toString();
      return wo;
    }

    public Text createValue() {
      return new Text();
    }
  }

}
