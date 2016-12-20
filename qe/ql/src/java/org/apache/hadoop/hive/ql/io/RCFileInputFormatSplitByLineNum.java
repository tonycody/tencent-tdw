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
package org.apache.hadoop.hive.ql.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.plan.partitionDesc;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.net.NetworkTopology;

@SuppressWarnings("deprecation")
public class RCFileInputFormatSplitByLineNum<K extends LongWritable, V extends BytesRefArrayWritable>
    extends FileInputFormat<K, V> implements JobConfigurable {
  public static final Log LOG = LogFactory
      .getLog(RCFileInputFormatSplitByLineNum.class);

  public static class RCFileFileSplit_WithLineNum implements Writable {
    private Path file;
    private long start;
    private long length;
    private String[] hosts;

    public RCFileFileSplit_WithLineNum() {
    }

    public RCFileFileSplit_WithLineNum(Path file, long start, long length, String[] hosts) {
      this.file = file;
      this.start = start;
      this.length = length;
      this.hosts = hosts;
    }

    /** The file containing this split's data. */
    public Path getPath() {
      return file;
    }

    /** The position of the first byte in the file to process. */
    public long getStart() {
      return start;
    }

    /** The number of bytes in the file to process. */
    public long getLength() {
      return length;
    }

    public String toString() {
      return file + ":" + start + "+" + length;
    }

    // //////////////////////////////////////////
    // Writable methods
    // //////////////////////////////////////////

    public void write(DataOutput out) throws IOException {
      UTF8.writeString(out, file.toString());
      out.writeLong(start);
      out.writeLong(length);
    }

    public void readFields(DataInput in) throws IOException {
      file = new Path(UTF8.readString(in));
      start = in.readLong();
      length = in.readLong();
      hosts = null;
    }

    public String[] getLocations() throws IOException {
      if (this.hosts == null) {
        return new String[] {};
      } else {
        return this.hosts;
      }
    }
  }
  
  public static class CombineRCFileFileSplit_WithLineNum implements InputSplit {
    public RCFileFileSplit_WithLineNum[] filesplits;
    private String[] hosts;
    private long length = 0;

    public CombineRCFileFileSplit_WithLineNum() {
    }

    public CombineRCFileFileSplit_WithLineNum(RCFileFileSplit_WithLineNum[] filesplits,
        String[] hosts) {
      this.filesplits = filesplits;
      this.hosts = hosts;
      for (int i = 0; i < filesplits.length; i++) {
        length += filesplits[i].getLength();
      }
    }

    @Override
    public long getLength() {
      return length;
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
      this.filesplits = new RCFileFileSplit_WithLineNum[len];
      for (int i = 0; i < len; i++) {
        filesplits[i] = new RCFileFileSplit_WithLineNum();
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
  
  public class CombineRCFileRecordReader_WithLineNum<K, V> implements RecordReader<K, V> {

    @SuppressWarnings("deprecation")
    public CombineRCFileRecordReader_WithLineNum(JobConf job, CombineRCFileFileSplit_WithLineNum split,
        Reporter reporter) throws IOException {
      this.split = split;
      this.jc = job;
      this.rrClass = RCFileRecordReader.class;
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

    @SuppressWarnings("unchecked")
    Class[] constructorSignature = new Class[] {
        Configuration.class, FileSplit.class };

    protected CombineRCFileFileSplit_WithLineNum split;
    @SuppressWarnings("deprecation")
    protected JobConf jc;
    protected Reporter reporter;
    protected Class<RCFileRecordReader> rrClass;
    protected Constructor<RCFileRecordReader> rrConstructor;
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

    private boolean initNextRecordReader(V value) throws IOException {
      if (!this.initNextRecordReader())
        return false;
      return true;
    }

    protected boolean initNextRecordReader() throws IOException {
      if (curReader != null) {
        curReader.close();
        curReader = null;
        if (idx > 0) {
          progress += split.filesplits[idx - 1].getLength();
        }
      }

      if (idx == split.filesplits.length) {
        return false;
      }

      try {
        FileSplit fileSplit = new FileSplit(split.filesplits[idx].getPath(),
            split.filesplits[idx].getStart(), split.filesplits[idx].getLength(), (String[]) null);
        curReader = rrConstructor.newInstance(new Object[] { jc, fileSplit });

      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      idx++;
      return true;
    }
  }
  
  @Override
  public void configure(JobConf arg0) {

  }

  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    ArrayList<CombineRCFileFileSplit_WithLineNum> splits = new ArrayList<CombineRCFileFileSplit_WithLineNum>(numSplits);

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
      RecordWriter recWriter = new RCFileOutputFormat().getHiveRecordWriter(
          job, inputPath, Text.class, false, partDesc.getTableDesc()
              .getProperties(), null);
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

    long fileRecordNum=0;
    long fileLen=0;
    for (int i = 0; i < fss.length; i++) {
      String fileName = fss[i].getPath().toString();
      if (fileName.endsWith(".rcf")) {
        int index = fileName.lastIndexOf("_");
        String sub = fileName.substring(index + 1, fileName.length() - 4);
        fileRecordNum += Long.valueOf(sub);
        fileLen += fss[i].getLen();
      } else {
        // throw new IOException("file:"+fileName+" is not rcfile.");
      }
    }
    long minBlockSize=job.getInt("hive.io.rcfile.record.buffer.size",
        4 * 1024 * 1024) * 2;
    long splitLen=0;
    if(fileRecordNum>0){
      splitLen = (fileLen / fileRecordNum) * lenNum;
    }
    
    long splitSize = Math
        .max(splitLen, minBlockSize);
    LOG.info("fileRecordNum=" + fileRecordNum+",fileLen="+fileLen+",splitSize="+splitSize);
    
    int id = 0;
    int preId=0;
    long offset = 0;
    long currlen = 0;
    ArrayList<RCFileFileSplit_WithLineNum> currFileSplits = new ArrayList<RCFileFileSplit_WithLineNum>();
    while (true) {
      long need = splitSize - currlen;
      long remain = fss[id].getLen() - offset;
      
      if (need <= remain) {
        if(preId != id && need < minBlockSize){//当是新文件时，剩余空间超过8MB才会加到上个文件处理的MAP中
          
        }else{
          currFileSplits.add(new RCFileFileSplit_WithLineNum(fss[id].getPath(), offset,
              need,(String[]) null));
          offset += need;
        }
        splits.add(new CombineRCFileFileSplit_WithLineNum(currFileSplits
            .toArray(new RCFileFileSplit_WithLineNum[currFileSplits.size()]), fss[id]
            .getPath().getFileSystem(job)
            .getFileBlockLocations(fss[id], 0, fss[id].getLen())[0]
            .getHosts()));
        currFileSplits.clear();
        currlen = 0;    
      } else {
        if (remain != 0) {
          currFileSplits.add(new RCFileFileSplit_WithLineNum(fss[id].getPath(), offset,
              remain,(String[]) null));
        }
        preId=id;
        id++;
        offset = 0;
        currlen += remain;
      }

      if (id == fss.length) {
        if (currFileSplits.size() != 0) {
          splits.add(new CombineRCFileFileSplit_WithLineNum(currFileSplits
              .toArray(new RCFileFileSplit_WithLineNum[currFileSplits.size()]), fss[id - 1]
              .getPath().getFileSystem(job)
              .getFileBlockLocations(fss[id - 1], 0, fss[id - 1].getLen())[0]
              .getHosts()));
        }
        break;
      }
    }

    // add by payniexiao in 20130115 for resolve split by line bug
    if (splits.size() == 0) {
      ArrayList<RCFileFileSplit_WithLineNum> emptyFileSplits = new ArrayList<RCFileFileSplit_WithLineNum>();
      emptyFileSplits.add(new RCFileFileSplit_WithLineNum(fss[0].getPath(), 0, 0,(String[]) null));

      splits
          .add(new CombineRCFileFileSplit_WithLineNum(emptyFileSplits
              .toArray(new RCFileFileSplit_WithLineNum[emptyFileSplits.size()]), fss[0].getPath()
              .getFileSystem(job)
              .getFileBlockLocations(fss[0], 0, fss[0].getLen())[0].getHosts()));
    }
    // add end

    for (int i = 0; i < splits.size(); i++) {
      LOG.info(splits.get(i).toString());
    }

    LOG.info("Total # of splits: " + splits.size());
    return splits.toArray(new CombineRCFileFileSplit_WithLineNum[splits.size()]);
  }

  @Override
  public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {
    LOG.info("enter RCFileInputFormatSplitByLineNum RecordReader");
    return new CombineRCFileRecordReader_WithLineNum(job, (CombineRCFileFileSplit_WithLineNum) split,reporter);
  }

}
