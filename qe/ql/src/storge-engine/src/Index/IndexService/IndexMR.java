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
import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
import FormatStorage1.IColumnDataFile;
import FormatStorage1.IFormatDataFile;

@SuppressWarnings("deprecation")
public class IndexMR {

  public static class IndexMap extends MapReduceBase implements
      Mapper<IndexKey, IndexValue, IndexKey, IndexValue> {
    public static final Log LOG = LogFactory.getLog("IndexMR");

    @Override
    public void map(IndexKey key, IndexValue value,
        OutputCollector<IndexKey, IndexValue> output, Reporter reporter)
        throws IOException {
      if (!key.show().trim().equals("-1")) {
        output.collect(key, value);
      }
    }
  }

  public static class IndexReduce extends MapReduceBase implements
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

  public static RunningJob run(Configuration conf2, String inputfiles,
      boolean column, String ids, String outputdir) {
    if (inputfiles == null || outputdir == null)
      return null;

    JobConf conf = new JobConf(conf2);
    conf.setJobName("IndexMR:\t" + ids);
    conf.setJarByClass(IndexMR.class);
    FileSystem fs = null;
    try {
      fs = FileSystem.get(conf);
      fs.delete(new Path(outputdir), true);
    } catch (IOException e3) {
      e3.printStackTrace();
    }

    conf.set("index.ids", ids);
    if (column) {
      conf.set("datafiletype", "column");
    } else {
      conf.set("datafiletype", "format");
    }

    String[] ifs = inputfiles.split(",");
    long wholerecnum = 0;

    String[] idxs = ids.split(",");
    String[] fieldStrings = new String[idxs.length + 2];

    if (!column) {
      IFormatDataFile ifdf;
      try {
        ifdf = new IFormatDataFile(conf);
        ifdf.open(ifs[0]);
        for (int i = 0; i < idxs.length; i++) {
          int id = Integer.parseInt(idxs[i]);
          byte type = ifdf.fileInfo().head().fieldMap().fieldtypes().get(id)
              .type();
          fieldStrings[i] = type + ConstVar.RecordSplit + i;
        }
        ifdf.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    } else {
      try {
        IColumnDataFile icdf = new IColumnDataFile(conf);
        icdf.open(ifs[0]);
        for (int i = 0; i < idxs.length; i++) {
          int id = Integer.parseInt(idxs[i]);
          byte type = icdf.fieldtypes().get(id).type();
          fieldStrings[i] = type + ConstVar.RecordSplit + i;
        }
        icdf.close();
      } catch (IOException e) {
        e.printStackTrace();
      }

    }

    fieldStrings[fieldStrings.length - 2] = ConstVar.FieldType_Short
        + ConstVar.RecordSplit + (fieldStrings.length - 2);
    fieldStrings[fieldStrings.length - 1] = ConstVar.FieldType_Int
        + ConstVar.RecordSplit + (fieldStrings.length - 1);

    conf.setStrings(ConstVar.HD_fieldMap, fieldStrings);

    if (!column) {
      conf.set(ConstVar.HD_index_filemap, inputfiles);
      for (String file : ifs) {
        IFormatDataFile fff;
        try {
          fff = new IFormatDataFile(conf);
          fff.open(file);
          wholerecnum += fff.segIndex().recnum();
          fff.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    } else {
      HashSet<String> files = new HashSet<String>();
      for (String file : ifs) {
        files.add(file);
      }
      StringBuffer sb = new StringBuffer();
      for (String str : files) {
        sb.append(str).append(",");
      }
      conf.set(ConstVar.HD_index_filemap, sb.substring(0, sb.length() - 1));

      for (String file : files) {
        Path parent = new Path(file).getParent();
        try {
          FileStatus[] fss = fs.listStatus(parent);
          String openfile = "";
          for (FileStatus status : fss) {
            if (status.getPath().toString().contains(file)) {
              openfile = status.getPath().toString();
              break;
            }
          }
          IFormatDataFile fff = new IFormatDataFile(conf);
          fff.open(openfile);
          wholerecnum += fff.segIndex().recnum();
          fff.close();

        } catch (IOException e) {
          e.printStackTrace();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }

    conf.setNumReduceTasks((int) ((wholerecnum - 1) / (100000000) + 1));

    FileInputFormat.setInputPaths(conf, inputfiles);
    Path outputPath = new Path(outputdir);
    FileOutputFormat.setOutputPath(conf, outputPath);

    conf.setOutputKeyClass(IndexKey.class);
    conf.setOutputValueClass(IndexValue.class);

    conf.setPartitionerClass(IndexPartitioner.class);

    conf.setMapperClass(IndexMap.class);
    conf.setCombinerClass(IndexReduce.class);
    conf.setReducerClass(IndexReduce.class);

    if (column) {
      conf.setInputFormat(IColumnInputFormat.class);
    } else {
      conf.setInputFormat(IFormatInputFormat.class);
    }
    conf.setOutputFormat(IndexIFormatOutputFormat.class);

    try {
      JobClient jc = new JobClient(conf);
      return jc.submitJob(conf);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  public static void running(Configuration conf2, String inputfiles,
      boolean column, String ids, String outputdir) {
    RunningJob job = run(conf2, inputfiles, column, ids, outputdir);
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

  public static void main(String[] args) {
    Configuration conf = new Configuration();
    String inputfiles = "/user/tdw/warehouse/default_db/kv_f/attempt_201109070949_0594_m_000000_0.1316590944464";
    boolean column = false;
    String ids = "1";
    String outputdir = "/se/idxx";

    IndexMR.running(conf, inputfiles, column, ids, outputdir);
  }

}
