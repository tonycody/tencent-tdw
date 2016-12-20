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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.plan.partitionDesc;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;

import FormatStorage1.IFormatDataFile;
import FormatStorage1.IRecord;

@SuppressWarnings("deprecation")
public class FormatStorageInputFormat_SplitByLineNum extends
    FileInputFormat<LongWritable, IRecord> implements JobConfigurable {
  public static final Log LOG = LogFactory
      .getLog(FormatStorageInputFormat_SplitByLineNum.class);

  public static class FileSplit implements Writable {
    public String path;
    int start;
    int recnum;

    public FileSplit() {
    }

    public FileSplit(String path, int start, int recnum) {
      this.path = path;
      this.start = start;
      this.recnum = recnum;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(path);
      out.writeInt(start);
      out.writeInt(recnum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.path = in.readUTF();
      this.start = in.readInt();
      this.recnum = in.readInt();
    }

    @Override
    public String toString() {
      return path + ":" + start + ":" + recnum;
    }

  }

  public static class FormatStorageInputSplit_WithLineNum implements InputSplit {
    public FileSplit[] filesplits;
    private String[] hosts;
    private int recnum = -1;

    public FormatStorageInputSplit_WithLineNum() {
    }

    public FormatStorageInputSplit_WithLineNum(FileSplit[] filesplits,
        String[] hosts) {
      this.filesplits = filesplits;
      this.hosts = hosts;
      recnum = 0;
      for (int i = 0; i < filesplits.length; i++) {
        recnum += filesplits[i].recnum;
      }
    }

    public int recnum() {
      if (this.recnum == -1) {
        recnum = 0;
        for (int i = 0; i < filesplits.length; i++) {
          recnum += filesplits[i].recnum;
        }
      }
      return recnum;
    }

    @Override
    public long getLength() {
      return recnum();
    }

    @Override
    public String[] getLocations() throws IOException {
      if (this.hosts == null) {
        return new String[] {};
      } else {
        return this.hosts;
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(filesplits.length);
      for (int i = 0; i < filesplits.length; i++) {
        filesplits[i].write(out);
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      int len = in.readInt();
      this.filesplits = new FileSplit[len];
      for (int i = 0; i < len; i++) {
        filesplits[i] = new FileSplit();
        filesplits[i].readFields(in);
      }
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      for (int i = 0; i < this.filesplits.length; i++) {
        sb.append(this.filesplits[i].toString()).append(";\t");
      }
      return sb.toString();
    }
  }

  @Override
  public void configure(JobConf job) {

  }

  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {

    List<FormatStorageInputSplit_WithLineNum> splits = new ArrayList<FormatStorageInputSplit_WithLineNum>();

    int lenNum = job.getInt("hive.inputfiles.line_num_per_split", 1000000);
    if (lenNum < 10000) {
      LOG.info("lenNum been set to " + lenNum
          + " is too small, so set it to 1000000");
      lenNum = 1000000;
    }
    FileStatus[] fss = listStatus(job);

    FileStatus[] orignalFss = fss;
    List<FileStatus> fssList = new ArrayList<FileStatus>();
    for (int i = 0; i < fss.length; i++) {
      if (fss[i].getLen() > 0) {
        fssList.add(fss[i]);
      }
    }

    fss = (FileStatus[]) fssList.toArray(new FileStatus[0]);
    int listSize = fss.length;

    if (listSize == 0) {

      mapredWork mrWork = Utilities.getMapRedWork(job);
      Path inputPath = orignalFss[0].getPath();
      Path inputParentPath = inputPath.getParent();
      String inputPathStr = inputPath.toUri().toString();
      String inputPathParentStr = inputParentPath.toString();

      FileSystem fs = inputPath.getFileSystem(job);
      fs.delete(inputPath, true);

      LinkedHashMap<String, partitionDesc> partDescMap = mrWork
          .getPathToPartitionInfo();
      partitionDesc partDesc = partDescMap.get(inputPathParentStr);

      job.setBoolean("NeedPostfix", false);
      RecordWriter recWriter = new FormatStorageHiveOutputFormat()
          .getHiveRecordWriter(job, inputPath, Text.class, false, partDesc
              .getTableDesc().getProperties(), null);
      recWriter.close(false);
      job.setBoolean("NeedPostfix", true);

      fss = listStatus(job);
    }

    Random r = new Random(123456);
    for (int i = 0; i < fss.length; i++) {
      int x = r.nextInt(fss.length);
      FileStatus tmp = fss[i];
      fss[i] = fss[x];
      fss[x] = tmp;
    }
    int[] fslengths = new int[fss.length];
    for (int i = 0; i < fss.length; i++) {
      IFormatDataFile ifdf = new IFormatDataFile(job);
      ifdf.open(fss[i].getPath().toString());
      fslengths[i] = ifdf.recnum();
      ifdf.close();
    }

    int id = 0;
    int offset = 0;
    int currlen = 0;
    ArrayList<FileSplit> currFileSplits = new ArrayList<FormatStorageInputFormat_SplitByLineNum.FileSplit>();
    while (true) {
      int need = lenNum - currlen;
      int remain = fslengths[id] - offset;

      if (need <= remain) {
        currFileSplits.add(new FileSplit(fss[id].getPath().toString(), offset,
            need));
        splits
            .add(new FormatStorageInputSplit_WithLineNum(currFileSplits
                .toArray(new FileSplit[currFileSplits.size()]), fss[id]
                .getPath().getFileSystem(job)
                .getFileBlockLocations(fss[id], 0, fss[id].getLen())[0]
                .getHosts()));
        currFileSplits.clear();

        currlen = 0;

        offset += need;
      } else {
        if (remain != 0) {
          currFileSplits.add(new FileSplit(fss[id].getPath().toString(),
              offset, remain));
        }
        id++;
        offset = 0;
        currlen += remain;
      }

      if (id == fss.length) {
        if (currFileSplits.size() != 0) {
          splits.add(new FormatStorageInputSplit_WithLineNum(currFileSplits
              .toArray(new FileSplit[currFileSplits.size()]), fss[id - 1]
              .getPath().getFileSystem(job)
              .getFileBlockLocations(fss[id - 1], 0, fss[id - 1].getLen())[0]
              .getHosts()));
        }
        break;
      }
    }

    if (splits.size() == 0) {
      ArrayList<FileSplit> emptyFileSplits = new ArrayList<FormatStorageInputFormat_SplitByLineNum.FileSplit>();
      emptyFileSplits.add(new FileSplit(fss[0].getPath().toString(), 0, 0));

      splits
          .add(new FormatStorageInputSplit_WithLineNum(emptyFileSplits
              .toArray(new FileSplit[emptyFileSplits.size()]), fss[0].getPath()
              .getFileSystem(job)
              .getFileBlockLocations(fss[0], 0, fss[0].getLen())[0].getHosts()));
    }

    for (int i = 0; i < splits.size(); i++) {
      LOG.info(splits.get(i).toString());
    }

    LOG.info("Total # of splits: " + splits.size());
    return splits
        .toArray(new FormatStorageInputSplit_WithLineNum[splits.size()]);

  }

  static class FormatStorageIRecordReader_WithLineNum implements
      RecordReader<LongWritable, IRecord> {

    FormatStorageInputSplit_WithLineNum split;
    JobConf job;
    private IFormatDataFile ifdf;

    public FormatStorageIRecordReader_WithLineNum(
        FormatStorageInputSplit_WithLineNum split, JobConf job)
        throws IOException {
      this.split = split;
      this.job = job;
      this.wholerecnum = split.recnum();
      if (split.filesplits.length > 0) {
        String fileName = split.filesplits[0].path;
        this.ifdf = new IFormatDataFile(job);
        ifdf.open(fileName);
        ifdf.seek(split.filesplits[0].start);
        currfilerecnum = split.filesplits[0].recnum;
      }
    }

    int wholerecnum;
    int wholerec = 0;

    int currfileid = 0;
    int currfilerecnum;
    int currfilerec = 0;

    @Override
    public boolean next(LongWritable key, IRecord value) throws IOException {
      if (wholerec >= wholerecnum)
        return false;

      if (currfilerec < currfilerecnum) {
        if (!ifdf.next(value)) {
          String err = "FSIR error read:\t"
              + this.split.filesplits[currfileid].path + ":\tcurrentrec\t"
              + wholerec + "\trecnum\t" + wholerecnum + "\r\nvalue"
              + value.showstr();
          throw new IOException(err);
        }
        currfilerec++;
      } else {
        if (currfileid < split.filesplits.length - 1) {
          currfileid++;
          String fileName = split.filesplits[currfileid].path;
          this.ifdf = new IFormatDataFile(job);
          ifdf.open(fileName);
          ifdf.seek(split.filesplits[currfileid].start);
          currfilerecnum = split.filesplits[currfileid].recnum;
          currfilerec = 0;

          if (currfilerec < currfilerecnum) {
            if (!ifdf.next(value)) {
              String err = "FSIR error read:\t"
                  + this.split.filesplits[currfileid].path + ":\tcurrentrec\t"
                  + wholerec + "\trecnum\t" + wholerecnum + "\r\nvalue"
                  + value.showstr();
              throw new IOException(err);
            }
            currfilerec++;
          }

        } else {
          return false;
        }
      }

      wholerec++;
      return true;
    }

    @Override
    public LongWritable createKey() {
      return new LongWritable(0);
    }

    @Override
    public IRecord createValue() {
      return ifdf == null ? null : ifdf.getIRecordObj();
    }

    @Override
    public long getPos() throws IOException {
      return this.wholerec;
    }

    @Override
    public void close() throws IOException {
      if (ifdf != null) {
        ifdf.close();
      }
    }

    @Override
    public float getProgress() throws IOException {
      return this.wholerec / (float) wholerecnum;
    }
  }

  @Override
  public RecordReader<LongWritable, IRecord> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {
    return new FormatStorageIRecordReader_WithLineNum(
        (FormatStorageInputSplit_WithLineNum) split, job);
  }
}
