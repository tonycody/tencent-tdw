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
package IndexService;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;

import Comm.ConstVar;
import FormatStorage1.IFormatDataFile;
import FormatStorage1.IRecord;

@SuppressWarnings("deprecation")
public class IndexMergeMR {

  public static class MergeIndexMap extends MapReduceBase implements
      Mapper<IndexKey, IndexValue, IndexKey, IndexValue> {

    @Override
    public void map(IndexKey key, IndexValue value,
        OutputCollector<IndexKey, IndexValue> output, Reporter reporter)
        throws IOException {
      output.collect(key, value);
    }

  }

  public static class MergeIndexReduce extends MapReduceBase implements
      Reducer<IndexKey, IndexValue, IndexKey, IndexValue> {
    @Override
    public void reduce(IndexKey key, Iterator<IndexValue> values,
        OutputCollector<IndexKey, IndexValue> output, Reporter reporter)
        throws IOException {

      while (values.hasNext()) {
        output.collect(key, values.next());
      }

    }
  }

  public static RunningJob run(String inputfiles, String outputdir,
      Configuration conf) {
    if (inputfiles == null || outputdir == null)
      return null;

    JobConf job = new JobConf(conf);
    job.setJobName("MergeIndexMR");
    job.setJarByClass(IndexMergeMR.class);
    job.setNumReduceTasks(1);
    FileSystem fs = null;
    try {
      fs = FileSystem.get(job);
      fs.delete(new Path(outputdir), true);

      String[] ifs = inputfiles.split(",");
      TreeSet<String> files = new TreeSet<String>();
      for (int i = 0; i < ifs.length; i++) {
        IFormatDataFile ifdf = new IFormatDataFile(job);
        ifdf.open(ifs[i]);
        Collection<String> strs = ifdf.fileInfo().head().getUdi().infos()
            .values();
        for (String str : strs) {
          files.add(str);
        }
        ifdf.close();
      }
      StringBuffer sb = new StringBuffer();
      for (String str : files) {
        sb.append(str + ",");
      }
      job.set(ConstVar.HD_index_filemap, sb.substring(0, sb.length() - 1));

      IFormatDataFile ifdf = new IFormatDataFile(job);
      ifdf.open(ifs[0]);

      HashMap<Integer, IRecord.IFType> map = ifdf.fileInfo().head().fieldMap()
          .fieldtypes();
      ArrayList<String> fieldStrings = new ArrayList<String>();

      for (int i = 0; i < map.size(); i++) {
        IRecord.IFType type = map.get(i);
        fieldStrings.add(type.type() + ConstVar.RecordSplit + type.idx());
      }

      job.setStrings(ConstVar.HD_fieldMap,
          fieldStrings.toArray(new String[fieldStrings.size()]));
      job.set("datafiletype",
          ifdf.fileInfo().head().getUdi().infos().get(123456));
      ifdf.close();
    } catch (Exception e2) {
      e2.printStackTrace();
    }

    FileInputFormat.setInputPaths(job, inputfiles);
    FileOutputFormat.setOutputPath(job, new Path(outputdir));

    job.setOutputKeyClass(IndexKey.class);
    job.setOutputValueClass(IndexValue.class);

    job.setPartitionerClass(IndexMergePartitioner.class);

    job.setMapperClass(MergeIndexMap.class);
    job.setCombinerClass(MergeIndexReduce.class);
    job.setReducerClass(MergeIndexReduce.class);

    job.setInputFormat(IndexMergeIFormatInputFormat.class);
    job.setOutputFormat(IndexMergeIFormatOutputFormat.class);

    try {
      JobClient jc = new JobClient(job);
      return jc.submitJob(job);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  public static void running(String inputfiles, String outputdir,
      Configuration conf) {
    RunningJob job = run(inputfiles, outputdir, conf);
    try {

      String lastReport = "";
      SimpleDateFormat dateFormat = new SimpleDateFormat(
          "yyyy-MM-dd hh:mm:ss,SSS");
      long reportTime = System.currentTimeMillis();
      long maxReportInterval = 3 * 1000;

      while (!job.isComplete()) {
        Thread.sleep(1000);

        int mapProgress = Math.round(job.mapProgress() * 100);
        int reduceProgress = Math.round(job.reduceProgress() * 100);

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

    } catch (IOException e1) {
      e1.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }

}
