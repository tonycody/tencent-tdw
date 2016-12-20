/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec;

import org.apache.commons.logging.Log;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.fileSinkDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;

import StorageEngineClient.FormatStorageHiveOutputFormat;

public class FileSinkOperator extends TerminalOperator<fileSinkDesc> implements
    Serializable {

  public static interface RecordWriter {
    public void write(Writable w) throws IOException;

    public void close(boolean abort) throws IOException;
  }

  private static final long serialVersionUID = 1L;
  transient protected RecordWriter outWriter;
  transient protected FileSystem fs;
  transient protected Path outPath;
  transient protected Path finalPath;
  transient protected Serializer serializer;
  transient protected BytesWritable commonKey = new BytesWritable();
  transient protected TableIdEnum tabIdEnum = null;
  transient private LongWritable row_count;
  transient private LongWritable filesink_success_count = new LongWritable();

  private long rowcount = 0;
  private JobConf jc = null;
  private boolean overFormatStorageLimit = false;
  private boolean formatStorageLimitFix = false;
  private long formatStorageLimitNum = 100000000;
  private HashMap<Path, Path> outPathList = new HashMap<Path, Path>();
  private int filenum = 0;
  private boolean isRCF = false;

  public static enum FileSinkCount {
    FILESINK_SUCCESS_COUNT
  }

  public static enum TableIdEnum {

    TABLE_ID_1_ROWCOUNT, TABLE_ID_2_ROWCOUNT, TABLE_ID_3_ROWCOUNT, TABLE_ID_4_ROWCOUNT, TABLE_ID_5_ROWCOUNT, TABLE_ID_6_ROWCOUNT, TABLE_ID_7_ROWCOUNT, TABLE_ID_8_ROWCOUNT, TABLE_ID_9_ROWCOUNT, TABLE_ID_10_ROWCOUNT, TABLE_ID_11_ROWCOUNT, TABLE_ID_12_ROWCOUNT, TABLE_ID_13_ROWCOUNT, TABLE_ID_14_ROWCOUNT, TABLE_ID_15_ROWCOUNT;

  }

  transient protected boolean autoDelete = false;

  private String addRCFileRecordCount(String fileName) {
    if (this.isRCF) {
      return fileName + "_" + this.rowcount + ".rcf";
    } else {
      return fileName;
    }
  }

  private void commit() throws IOException {
    if (overFormatStorageLimit) {
      for (Path outPathtmp : outPathList.keySet()) {
        Path finalPathtmp = outPathList.get(outPathtmp);
        if (fs.exists(outPathtmp)) {
          finalPathtmp = new Path(this.addRCFileRecordCount(finalPathtmp
              .toString()));
          if (!fs.rename(outPathtmp, finalPathtmp)) {
            throw new IOException("Unable to rename output to: " + finalPathtmp);
          }
          LOG.info("Committed to output file: " + finalPathtmp);
          continue;
        }

        Path outparPath = outPathtmp.getParent();
        FileStatus[] fss = fs.listStatus(outparPath);
        for (FileStatus f : fss) {
          String onefile = f.getPath().toString();
          if (onefile.contains(outPathtmp.getName())) {
            int timelength = new String(".1275129140958").length();
            LOG.info("finalPathtmp.getName().substring(0,finalPath.getName().length()-timelength)="
                + finalPathtmp.getName().substring(0,
                    finalPathtmp.getName().length() - timelength));
            LOG.info("onefile=" + onefile);
            LOG.info("onefile.indexOf(finalPathtmp.getName().substring(0,finalPathtmp.getName().length()-timelength))="
                + onefile.indexOf(finalPathtmp.getName().substring(0,
                    finalPathtmp.getName().length() - timelength)));
            if (SessionState.get() != null) {
              SessionState.get().ssLog(
                  "finalPathtmp.getName().substring(0,finalPath.getName().length()-timelength)="
                      + finalPathtmp.getName().substring(0,
                          finalPathtmp.getName().length() - timelength));
              SessionState.get().ssLog("onefile=" + onefile);
              SessionState
                  .get()
                  .ssLog(
                      "onefile.indexOf(finalPathtmp().substring(0,finalPath.getName().length()-timelength))="
                          + onefile.indexOf(finalPathtmp.getName().substring(0,
                              finalPathtmp.getName().length() - timelength)));
            }
            String end = onefile.substring(onefile.indexOf(finalPathtmp
                .getName().substring(0,
                    finalPathtmp.getName().length() - timelength))
                + finalPathtmp.getName().length());
            Path endPath = new Path(finalPathtmp.toString() + end);
            endPath = new Path(this.addRCFileRecordCount(endPath.toString()));
            LOG.info("src=" + f.getPath().toString() + " endPath="
                + endPath.toString());
            if (SessionState.get() != null)
              SessionState.get().ssLog(
                  "src=" + f.getPath().toString() + " endPath="
                      + endPath.toString());
            if (!fs.rename(f.getPath(), endPath)) {
              throw new IOException("Unable to rename output to: " + endPath);
            }
            LOG.info("Committed to output file: " + endPath);
            if (SessionState.get() != null)
              SessionState.get().ssLog("Committed to output file: " + endPath);
          }
        }
      }
    } else {
      if (fs.exists(outPath)) {
        finalPath = new Path(this.addRCFileRecordCount(finalPath.toString()));
        if (!fs.rename(outPath, finalPath)) {
          throw new IOException("Unable to rename output to: " + finalPath);
        }
        LOG.info("Committed to output file: " + finalPath);
        return;
      }

      Path outparPath = outPath.getParent();
      FileStatus[] fss = fs.listStatus(outparPath);
      for (FileStatus f : fss) {
        String onefile = f.getPath().toString();
        if (onefile.contains(outPath.getName())) {
          int timelength = new String(".1275129140958").length();
          LOG.info("finalPath.getName().substring(0,finalPath.getName().length()-timelength)="
              + finalPath.getName().substring(0,
                  finalPath.getName().length() - timelength));
          LOG.info("onefile=" + onefile);
          LOG.info("onefile.indexOf(finalPath.getName().substring(0,finalPath.getName().length()-timelength))="
              + onefile.indexOf(finalPath.getName().substring(0,
                  finalPath.getName().length() - timelength)));
          if (SessionState.get() != null) {
            SessionState.get().ssLog(
                "finalPath.getName().substring(0,finalPath.getName().length()-timelength)="
                    + finalPath.getName().substring(0,
                        finalPath.getName().length() - timelength));
            SessionState.get().ssLog("onefile=" + onefile);
            SessionState
                .get()
                .ssLog(
                    "onefile.indexOf(finalPath.getName().substring(0,finalPath.getName().length()-timelength))="
                        + onefile.indexOf(finalPath.getName().substring(0,
                            finalPath.getName().length() - timelength)));
          }
          String end = onefile.substring(onefile.indexOf(finalPath.getName()
              .substring(0, finalPath.getName().length() - timelength))
              + finalPath.getName().length());
          Path endPath = new Path(finalPath.toString() + end);
          endPath = new Path(this.addRCFileRecordCount(endPath.toString()));
          LOG.info("src=" + f.getPath().toString() + " endPath="
              + endPath.toString());
          if (SessionState.get() != null)
            SessionState.get().ssLog(
                "src=" + f.getPath().toString() + " endPath="
                    + endPath.toString());
          if (!fs.rename(f.getPath(), endPath)) {
            throw new IOException("Unable to rename output to: " + endPath);
          }
          LOG.info("Committed to output file: " + endPath);
          if (SessionState.get() != null)
            SessionState.get().ssLog("Committed to output file: " + endPath);
        }
      }
    }
  }

  private RecordWriter updateoutWriter() throws HiveException {
    try {
      RecordWriter newOutWriter = null;
      String specPath = conf.getDirName();
      Path tmpPath = Utilities.toTempPath(specPath);
      String taskId = Utilities.getTaskId(jc);

      finalPath = new Path(tmpPath, taskId);
      filenum++;
      finalPath = Utilities.toFilePath(finalPath, filenum);
      outPath = new Path(tmpPath, Utilities.toTempPath(taskId));
      LOG.info("Update outwriter to:" + outPath.toString());

      outPathList.put(outPath, finalPath);

      final Class<? extends Writable> outputClass = serializer
          .getSerializedClass();
      newOutWriter = HiveFileFormatUtils.getHiveRecordWriter(jc,
          conf.getTableInfo(), outputClass, conf, outPath);

      autoDelete = ShimLoader.getHadoopShims().fileSystemDeleteOnExit(fs,
          outPath);
      return newOutWriter;
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  protected void initializeOp(Configuration hconf) throws HiveException {
    try {
      serializer = (Serializer) conf.getTableInfo().getDeserializerClass()
          .newInstance();
      serializer.initialize(hconf, conf.getTableInfo().getProperties());

      if (hconf instanceof JobConf) {
        jc = (JobConf) hconf;
      } else {
        jc = new JobConf(hconf, ExecDriver.class);
      }

      int id = conf.getDestTableId();
      if ((id > 0) && (id <= TableIdEnum.values().length)) {
        String enumName = "TABLE_ID_" + String.valueOf(id) + "_ROWCOUNT";
        tabIdEnum = TableIdEnum.valueOf(enumName);
        row_count = new LongWritable();
        statsMap.put(tabIdEnum, row_count);

      }
      statsMap
          .put(FileSinkCount.FILESINK_SUCCESS_COUNT, filesink_success_count);
      String specPath = conf.getDirName();
      Path tmpPath = Utilities.toTempPath(specPath);
      String taskId = Utilities.getTaskId(hconf);
      fs = (new Path(specPath)).getFileSystem(hconf);
      finalPath = new Path(tmpPath, taskId);
      outPath = new Path(tmpPath, Utilities.toTempPath(taskId));

      LOG.info("Writing to temp file: FS " + outPath);

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

      if (conf.getTableInfo().getOutputFileFormatClass().getName()
          .equalsIgnoreCase("org.apache.hadoop.hive.ql.io.RCFileOutputFormat")) {
        this.isRCF = true;
      }

      Path parent = Utilities.toTempPath(specPath);
      finalPath = HiveFileFormatUtils.getOutputFormatFinalPath(parent, jc,
          hiveOutputFormat, isCompressed, finalPath);

      final Class<? extends Writable> outputClass = serializer
          .getSerializedClass();
      outWriter = HiveFileFormatUtils.getHiveRecordWriter(jc,
          conf.getTableInfo(), outputClass, conf, outPath);

      autoDelete = ShimLoader.getHadoopShims().fileSystemDeleteOnExit(fs,
          outPath);

      if (HiveConf.getBoolVar(hconf, HiveConf.ConfVars.FORMATSTORAGELIMITFIX)) {
        if (conf.getTableInfo().getOutputFileFormatClass().getName()
            .equals(FormatStorageHiveOutputFormat.class.getName())) {
          int defaultLimitMin = HiveConf.getIntVar(hconf,
              HiveConf.ConfVars.FORMATSTORAGELIMITMIN);
          int defaultLimitMax = HiveConf.getIntVar(hconf,
              HiveConf.ConfVars.FORMATSTORAGELIMITMAX);
          formatStorageLimitFix = true;
          rowcount = 0;
          filenum = 0;
          formatStorageLimitNum = HiveConf.getLongVar(hconf,
              HiveConf.ConfVars.FORMATSTORAGELIMITNUM);

          if (formatStorageLimitNum > defaultLimitMax) {
            formatStorageLimitNum = defaultLimitMax;
          } else if (formatStorageLimitNum < defaultLimitMin) {
            formatStorageLimitNum = defaultLimitMin;
          } else
            ;
        }
      }

      LOG.info("Set Format Storage limit Fix:" + formatStorageLimitFix);
      if (formatStorageLimitFix) {
        LOG.info("Set Format Storage limit Num:" + formatStorageLimitNum);
      }

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

      if (formatStorageLimitFix && !this.isRCF) {
        if (rowcount > formatStorageLimitNum) {
          overFormatStorageLimit = true;
          finalPath = Utilities.toFilePath(finalPath, filenum);
          outPathList.put(outPath, finalPath);
          outWriter.close(false);
          rowcount = 0;
          outWriter = updateoutWriter();
        }
      }

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
          commit();
        } catch (IOException e) {
          throw new HiveException(e);
        }
      }
    } else {
      try {
        outWriter.close(abort);
        if (!autoDelete) {
          fs.delete(outPath, true);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public String getName() {
    return new String("FS");
  }

  @Override
  public void jobClose(Configuration hconf, boolean success)
      throws HiveException {
    try {
      if (conf != null) {
        String specPath = conf.getDirName();
        FileSinkOperator.mvFileToFinalPath(specPath, hconf, success, LOG);
      }
    } catch (IOException e) {
      throw new HiveException(e);
    }
    super.jobClose(hconf, success);
  }

  public static void mvFileToFinalPath(String specPath, Configuration hconf,
      boolean success, Log LOG) throws IOException, HiveException {
    FileSystem fs = (new Path(specPath)).getFileSystem(hconf);
    Path tmpPath = Utilities.toTempPath(specPath);
    Path intermediatePath = new Path(tmpPath.getParent(), tmpPath.getName()
        + ".intermediate");
    Path finalPath = new Path(specPath);
    if (success) {
      if (fs.exists(tmpPath)) {
        LOG.info("Moving tmp dir: " + tmpPath + " to intermediate: " + intermediatePath);
        Utilities.rename(fs, tmpPath, intermediatePath);
        Utilities.removeTempOrDuplicateFiles(fs, intermediatePath);
        LOG.info("Moving intermediate dir: " + intermediatePath + " to final: " + finalPath);
        Utilities.renameOrMoveFiles(fs, intermediatePath, finalPath);
      }
    } else {
      fs.delete(tmpPath, true);
    }
  }

}
