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
package FormatStorage1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Progressable;

import StorageEngineClient.CombineFormatStorageFileInputFormat;

@SuppressWarnings("deprecation")
public class MergeFileUtil {
  public static final Log LOG = LogFactory.getLog(MergeFileUtil.class);

  public static class MergeMap extends MapReduceBase implements
      Mapper<LongWritable, IRecord, LongWritable, IRecord> {

    public MergeMap() {
    }

    @Override
    public void map(LongWritable key, IRecord value,
        OutputCollector<LongWritable, IRecord> output, Reporter reporter)
        throws IOException {
      output.collect(key, value);
    }

  }

  public static class MergeReduce extends MapReduceBase implements
      Reducer<LongWritable, IRecord, LongWritable, IRecord> {
    @Override
    public void reduce(LongWritable key, Iterator<IRecord> values,
        OutputCollector<LongWritable, IRecord> output, Reporter reporter)
        throws IOException {
      while (values.hasNext()) {
        output.collect(key, values.next());
      }
    }
  }

  public static class MergeIFormatInputFormat<K, V> extends
      FileInputFormat<LongWritable, IRecord> {

    @Override
    public RecordReader<LongWritable, IRecord> getRecordReader(
        InputSplit split, JobConf job, Reporter reporter) throws IOException {
      MergeFileSplit sp = (MergeFileSplit) split;

      final IFormatDataFile ifdf = new IFormatDataFile(job);
      ifdf.open(sp.file);
      ifdf.seek(0);

      return new RecordReader<LongWritable, IRecord>() {

        int num = 0;

        @Override
        public boolean next(LongWritable key, IRecord value) throws IOException {
          num++;
          key.set(num);
          boolean res = ifdf.next(value);
          return res;
        }

        @Override
        public float getProgress() throws IOException {
          return (float) num / ifdf.recnum();
        }

        @Override
        public long getPos() throws IOException {
          return num;
        }

        @Override
        public IRecord createValue() {
          return ifdf.getIRecordObj();
        }

        @Override
        public LongWritable createKey() {
          return new LongWritable(0);
        }

        @Override
        public void close() throws IOException {
          ifdf.close();
        }
      };
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits)
        throws IOException {
      String inputdir = job.get("mapred.input.dir");
      FileSystem fs = FileSystem.get(job);
      FileStatus[] fss = fs.listStatus(new Path(inputdir));
      MergeFileSplit[] splits = new MergeFileSplit[fss.length];
      int i = 0;
      for (FileStatus status : fss) {
        BlockLocation[] blkLocations = fs.getFileBlockLocations(status, 0,
            status.getLen());

        MergeFileSplit split = new MergeFileSplit(status.getPath().toString(),
            status.getLen(), blkLocations[0].getHosts());
        splits[i++] = split;
      }
      return splits;
    }
  }

  public static class MergeFileSplit implements InputSplit {
    String file;
    long length;
    private String[] hosts;

    public MergeFileSplit() {
    }

    public MergeFileSplit(String file, long length, String[] hosts) {
      this.file = file;
      this.length = length;
      this.hosts = hosts;
    }

    @Override
    public long getLength() throws IOException {
      return this.length;
    }

    @Override
    public String[] getLocations() throws IOException {
      return this.hosts;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.file = in.readUTF();
      this.length = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(this.file);
      out.writeLong(this.length);
    }
  }

  public static class MergeIFormatOutputFormat<K, V> extends
      FileOutputFormat<LongWritable, IRecord> {

    public MergeIFormatOutputFormat() {
    }

    @Override
    public RecordWriter<LongWritable, IRecord> getRecordWriter(
        FileSystem ignored, JobConf job, String name, Progressable progress)
        throws IOException {
      Path path = FileOutputFormat.getTaskOutputPath(job, name);
      String fileName = path.toString();
      final IFormatDataFile ifdf = new IFormatDataFile(job);
      IHead head = new IHead();
      head.fromStr(job.get("ifdf.head.info"));
      ifdf.create(fileName, head);

      return new RecordWriter<LongWritable, IRecord>() {
        @Override
        public void write(LongWritable key, IRecord value) throws IOException {
          ifdf.addRecord(value);
        }

        @Override
        public void close(Reporter reporter) throws IOException {
          ifdf.close();
        }
      };
    }
  }

  public static void run(String inputdir, String outputdir, Configuration conf)
      throws IOException {
    JobConf job = new JobConf(conf);
    job.setJobName("MergeFileUtil");
    job.setJarByClass(MergeFileUtil.class);
    FileSystem fs = null;
    fs = FileSystem.get(job);
    if (fs.exists(new Path(outputdir))) {
      throw new IOException("outputdir: " + outputdir + " exist!!!");
    }

    FileStatus[] fss = fs.listStatus(new Path(inputdir));

    if (fss == null || fss.length <= 0) {
      throw new IOException("no input files");
    }

    IFormatDataFile ifdf = new IFormatDataFile(job);
    ifdf.open(fss[0].getPath().toString());
    job.set("ifdf.head.info", ifdf.fileInfo().head().toStr());
    ifdf.close();

    long wholesize = 0;
    for (FileStatus status : fss) {
      wholesize += status.getLen();
    }

    job.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(job, inputdir);
    FileOutputFormat.setOutputPath(job, new Path(outputdir));

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(IRecord.class);

    job.setMapperClass(MergeMap.class);

    job.setInputFormat(CombineFormatStorageFileInputFormat.class);
    job.setOutputFormat(MergeIFormatOutputFormat.class);

    JobClient jc = new JobClient(job);
    RunningJob rjob = jc.submitJob(job);
    try {

      String lastReport = "";
      SimpleDateFormat dateFormat = new SimpleDateFormat(
          "yyyy-MM-dd hh:mm:ss,SSS");
      long reportTime = System.currentTimeMillis();
      long maxReportInterval = 3 * 1000;

      while (!rjob.isComplete()) {
        Thread.sleep(1000);

        int mapProgress = Math.round(rjob.mapProgress() * 100);
        int reduceProgress = Math.round(rjob.reduceProgress() * 100);

        String report = " map = " + mapProgress + "%,  reduce = "
            + reduceProgress + "%";

        if (!report.equals(lastReport)
            || System.currentTimeMillis() >= reportTime + maxReportInterval) {

          String output = dateFormat.format(Calendar.getInstance().getTime())
              + report;
          System.err.println(output);
          lastReport = report;
          reportTime = System.currentTimeMillis();
        }
      }
      LOG.info(rjob.getJobState());

    } catch (IOException e1) {
      e1.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static void runold(String inputdir, String outputdir,
      Configuration conf) throws IOException {
    JobConf job = new JobConf(conf);
    job.setJobName("MergeFileUtil");
    job.setJarByClass(MergeFileUtil.class);
    FileSystem fs = null;
    fs = FileSystem.get(job);
    if (fs.exists(new Path(outputdir))) {
      throw new IOException("outputdir: " + outputdir + " exist!!!");
    }

    FileStatus[] fss = fs.listStatus(new Path(inputdir));

    if (fss == null || fss.length <= 0) {
      throw new IOException("no input files");
    }

    for (FileStatus status : fss) {
      if (status.isDir()) {
        throw new IOException("!!!input dir contains directory:\t"
            + status.getPath().toString());
      }
    }

    IFormatDataFile ifdf = new IFormatDataFile(job);
    ifdf.open(fss[0].getPath().toString());
    job.set("ifdf.head.info", ifdf.fileInfo().head().toStr());
    ifdf.close();

    long wholesize = 0;
    for (FileStatus status : fss) {
      wholesize += status.getLen();
    }

    long fl = 512 * 1024 * 1024;
    int reduces = (int) (wholesize / fl + 1);
    job.setNumReduceTasks(reduces);

    FileInputFormat.setInputPaths(job, inputdir);
    FileOutputFormat.setOutputPath(job, new Path(outputdir));

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(IRecord.class);

    job.setMapperClass(MergeMap.class);
    job.setReducerClass(MergeReduce.class);

    job.setInputFormat(MergeIFormatInputFormat.class);
    job.setOutputFormat(MergeIFormatOutputFormat.class);

    JobClient jc = new JobClient(job);
    RunningJob rjob = jc.submitJob(job);
    try {

      String lastReport = "";
      SimpleDateFormat dateFormat = new SimpleDateFormat(
          "yyyy-MM-dd hh:mm:ss,SSS");
      long reportTime = System.currentTimeMillis();
      long maxReportInterval = 3 * 1000;

      while (!rjob.isComplete()) {
        Thread.sleep(1000);

        int mapProgress = Math.round(rjob.mapProgress() * 100);
        int reduceProgress = Math.round(rjob.reduceProgress() * 100);

        String report = " map = " + mapProgress + "%,  reduce = "
            + reduceProgress + "%";

        if (!report.equals(lastReport)
            || System.currentTimeMillis() >= reportTime + maxReportInterval) {

          String output = dateFormat.format(Calendar.getInstance().getTime())
              + report;
          System.err.println(output);
          lastReport = report;
          reportTime = System.currentTimeMillis();
        }
      }
      LOG.info(rjob.getJobState());

    } catch (IOException e1) {
      e1.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.out
          .println("please input the inputdir and outputdir in such format \r\n\t"
              + "$HADOOP_HOME/hadoop jar $jarname FormatStorage1.MergeFileUtil srchdfsdir desthdfsdir");
      System.exit(0);
    }
    Configuration conf = new Configuration();
    if (args.length >= 3 && args[2].equals("old")) {
      runold(args[0], args[1], conf);
    } else {
      run(args[0], args[1], conf);
    }

    Path op = new Path(args[1] + "/_logs");
    op.getFileSystem(conf).delete(op, true);

    if (args[args.length - 1].equals("check")) {
      check(args, conf);
    }
  }

  private static void check(String[] args, Configuration conf)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    long orirecnum = 0;
    FileStatus[] fss = fs.listStatus(new Path(args[0]));
    for (FileStatus status : fss) {
      IFormatDataFile ifdf = new IFormatDataFile(conf);
      ifdf.open(status.getPath().toString());
      orirecnum += ifdf.segIndex().recnum();
      ifdf.close();
    }
    long newrecnum = 0;
    fss = fs.listStatus(new Path(args[1]));
    for (FileStatus status : fss) {
      IFormatDataFile ifdf = new IFormatDataFile(conf);
      ifdf.open(status.getPath().toString());
      newrecnum += ifdf.segIndex().recnum();
      ifdf.close();
    }
    System.out.println(args[0] + ":\tmerge result:\torirecnum:\t" + orirecnum
        + "\tnewrecnum:\t" + newrecnum + "\tmerge "
        + ((orirecnum == newrecnum) ? "success" : "fail"));
  }
}
