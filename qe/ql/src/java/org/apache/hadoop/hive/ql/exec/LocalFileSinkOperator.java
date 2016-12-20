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
package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.LocalFileSinkDesc;

import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

import StorageEngineClient.FormatStorageHiveOutputFormat;

public class LocalFileSinkOperator extends Operator<LocalFileSinkDesc>
    implements Serializable {

  private static final long serialVersionUID = 1L;

  transient protected RecordWriter outWriter;

  transient protected FileSystem fs;

  transient protected Path finalPath;

  transient protected Serializer serializer;

  transient protected BytesWritable commonKey = new BytesWritable();

  transient private LongWritable row_count;

  transient private LongWritable filesink_success_count = new LongWritable();

  private long rowcount = 0;

  private JobConf jc = null;

  public static enum FileSinkCount {
    FILESINK_SUCCESS_COUNT
  }

  transient protected boolean autoDelete = false;

  protected void initializeOp(Configuration hconf) throws HiveException {
    try {
      if (hconf instanceof JobConf) {
        jc = (JobConf) hconf;
      } else {
        jc = new JobConf(hconf, ExecDriver.class);
      }

      serializer = (Serializer) conf.getTableInfo().getDeserializerClass()
          .newInstance();
      serializer.initialize(hconf, conf.getTableInfo().getProperties());

      statsMap
          .put(FileSinkCount.FILESINK_SUCCESS_COUNT, filesink_success_count);
      String specPath = conf.getDirName();

      Path tmpPath = new Path(specPath);
      String taskId = Utilities.getTaskId(hconf);

      fs = (new Path(specPath)).getFileSystem(hconf);
      finalPath = new Path(tmpPath, taskId);

      LOG.info("OutputfileFormatClass : "
          + ((conf.getTableInfo().getOutputFileFormatClass() == null) ? "null"
              : conf.getTableInfo().getOutputFileFormatClass()
                  .getCanonicalName()));
      LOG.info("OutputfileFormatClass : "
          + ((conf.getTableInfo().getOutputFileFormatClass() == null) ? "null"
              : conf.getTableInfo().getOutputFileFormatClass().getName()));
      LOG.info("InputfileFormatClass : "
          + ((conf.getTableInfo().getInputFileFormatClassName() == null) ? "null"
              : conf.getTableInfo().getInputFileFormatClass().getName()));

      HiveOutputFormat<?, ?> hiveOutputFormat = conf.getTableInfo()
          .getOutputFileFormatClass().newInstance();
      boolean isCompressed = conf.getCompressed();

      final Class<? extends Writable> outputClass = serializer
          .getSerializedClass();
      outWriter = HiveFileFormatUtils.getHiveRecordWriter(jc,
          conf.getTableInfo(), outputClass, conf, finalPath);

      autoDelete = ShimLoader.getHadoopShims().fileSystemDeleteOnExit(fs,
          finalPath);

      initializeChildren(hconf);
    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  Writable recordValue;

  public void process(Object row, int tag) throws HiveException {
    try {
      if (reporter != null)
        reporter.progress();

      recordValue = serializer.serialize(row, inputObjInspectors[tag]);
      if (row_count != null) {
        row_count.set(row_count.get() + 1);
      }

      filesink_success_count.set(filesink_success_count.get() + 1);
      rowcount++;
      outWriter.write(recordValue);
    } catch (IOException e) {
      throw new HiveException(e);
    } catch (SerDeException e) {
      throw new HiveException(e);
    }
  }

  public void closeOp(boolean abort) throws HiveException {
    if (!abort) {
      if (outWriter != null) {
        try {
          outWriter.close(abort);
        } catch (IOException e) {
          throw new HiveException(e);
        }
      }
    } else {
      try {
        if (outWriter != null) {
          try {
            outWriter.close(abort);
          } catch (IOException e) {

          }
        }
        if (!autoDelete) {
          fs.delete(finalPath, true);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public String getName() {
    return new String("LOCALFS");
  }

  @Override
  public void jobClose(Configuration hconf, boolean success)
      throws HiveException {
    super.jobClose(hconf, success);
  }

}
