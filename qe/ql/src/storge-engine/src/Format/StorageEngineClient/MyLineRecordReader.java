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
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

@Deprecated
public class MyLineRecordReader implements RecordReader<LongWritable, Text> {
  private static final Log LOG = LogFactory.getLog(MyLineRecordReader.class
      .getName());

  private CompressionCodecFactory compressionCodecs = null;
  private long start;
  private long pos;
  private long end;
  private LineReader in;
  int maxLineLength;

  private Configuration tempConf = null;
  private FileSplit tempSplit = null;
  public LogHelper console = new LogHelper(LOG);

  @Deprecated
  public static class LineReader extends MyLineReader {
    LineReader(InputStream in) {
      super(in);
    }

    LineReader(InputStream in, int bufferSize) {
      super(in, bufferSize);
    }

    public LineReader(InputStream in, Configuration conf) throws IOException {
      super(in, conf);
    }
  }

  public MyLineRecordReader(Configuration job, FileSplit split)
      throws IOException {
    this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
        Integer.MAX_VALUE);
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();
    compressionCodecs = new CompressionCodecFactory(job);
    final CompressionCodec codec = compressionCodecs.getCodec(file);

    this.tempConf = job;
    this.tempSplit = split;

    FileSystem fs = file.getFileSystem(job);
    FSDataInputStream fileIn = fs.open(split.getPath());
    boolean skipFirstLine = false;
    if (codec != null) {
      in = new LineReader(codec.createInputStream(fileIn), job);
      end = Long.MAX_VALUE;
    } else {
      if (start != 0) {
        skipFirstLine = true;
        --start;
        fileIn.seek(start);
      }
      in = new LineReader(fileIn, job);
    }
    if (skipFirstLine) {
      start += in.readLine(new Text(), 0,
          (int) Math.min((long) Integer.MAX_VALUE, end - start));
    }
    this.pos = start;
  }

  public MyLineRecordReader(InputStream in, long offset, long endOffset,
      int maxLineLength) {
    this.maxLineLength = maxLineLength;
    this.in = new LineReader(in);
    this.start = offset;
    this.pos = offset;
    this.end = endOffset;
  }

  public MyLineRecordReader(InputStream in, long offset, long endOffset,
      Configuration job) throws IOException {
    this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
        Integer.MAX_VALUE);
    this.in = new LineReader(in, job);
    this.start = offset;
    this.pos = offset;
    this.end = endOffset;

    this.tempConf = job;
  }

  public LongWritable createKey() {
    return new LongWritable();
  }

  public Text createValue() {
    return new Text();
  }

  public synchronized boolean next(LongWritable key, Text value)
      throws IOException {

    while (pos < end) {
      key.set(pos);

      int newSize;
      try {
        newSize = in.readLine(value, maxLineLength, Math.max(
            (int) Math.min(Integer.MAX_VALUE, end - pos), maxLineLength));
        if (newSize == 0) {
          return false;
        }
        pos += newSize;
        if (newSize < maxLineLength) {
          return true;
        }

        LOG.info("Skipped line of size " + newSize + " at pos "
            + (pos - newSize));
      } catch (IOException e) {
        if (tempSplit != null) {
          LOG.info("");
          if (tempSplit.getPath() != null) {
            String filePath = tempSplit.getPath().toUri().toString();
            String ioeMsg = "The file " + filePath
                + " maybe is broken, please check it!";

            throw new IOException(ioeMsg);
          } else {
            if (tempConf != null && tempConf instanceof JobConf) {
              if (tempConf.get("map.input.file") != null
                  && tempConf.get("map.input.file") != "") {
                String filePath = tempConf.get("map.input.file");
                String ioeMsg = "The file " + filePath
                    + " maybe is broken, please check it!";

                throw new IOException(ioeMsg);
              }
            }
          }
        }
        throw e;
      }
    }

    return false;
  }

  public float getProgress() {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float) (end - start));
    }
  }

  public synchronized long getPos() throws IOException {
    return pos;
  }

  public synchronized void close() throws IOException {
    if (in != null) {
      in.close();
    }
  }
}
