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

package org.apache.hadoop.hive.ql.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.LocalFileSinkDesc;
import org.apache.hadoop.hive.ql.plan.fileSinkDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.StringUtils;

public class HiveFileFormatUtils {

  static {
    outputFormatSubstituteMap = new HashMap<Class<? extends OutputFormat>, Class<? extends HiveOutputFormat>>();
    HiveFileFormatUtils.registerOutputFormatSubstitute(
        IgnoreKeyTextOutputFormat.class, HiveIgnoreKeyTextOutputFormat.class);
    HiveFileFormatUtils.registerOutputFormatSubstitute(
        SequenceFileOutputFormat.class, HiveSequenceFileOutputFormat.class);
    HiveFileFormatUtils.registerOutputFormatSubstitute(GBKOutputFormat.class,
        GBKIgnoreKeyOutputFormat.class);
  }

  @SuppressWarnings("unchecked")
  private static Map<Class<? extends OutputFormat>, Class<? extends HiveOutputFormat>> outputFormatSubstituteMap;

  @SuppressWarnings("unchecked")
  public synchronized static void registerOutputFormatSubstitute(
      Class<? extends OutputFormat> origin,
      Class<? extends HiveOutputFormat> substitute) {
    outputFormatSubstituteMap.put(origin, substitute);
  }

  @SuppressWarnings("unchecked")
  public synchronized static Class<? extends HiveOutputFormat> getOutputFormatSubstitute(
      Class<?> origin) {
    if (HiveOutputFormat.class.isAssignableFrom(origin))
      return (Class<? extends HiveOutputFormat>) origin;
    Class<? extends HiveOutputFormat> result = outputFormatSubstituteMap
        .get(origin);
    return result;
  }

  public static Path getOutputFormatFinalPath(Path parent, JobConf jc,
      HiveOutputFormat<?, ?> hiveOutputFormat, boolean isCompressed,
      Path defaultFinalPath) throws IOException {
    if (hiveOutputFormat instanceof HiveIgnoreKeyTextOutputFormat) {
      return new Path(parent, Utilities.getTaskId(jc)
          + Utilities.getFileExtension(jc, isCompressed));
    }
    if (hiveOutputFormat instanceof GBKIgnoreKeyOutputFormat) {
      return new Path(parent, Utilities.getTaskId(jc)
          + Utilities.getFileExtension(jc, isCompressed));
    }
    return defaultFinalPath;
  }

  static {
    inputFormatCheckerMap = new HashMap<Class<? extends InputFormat>, Class<? extends InputFormatChecker>>();
    HiveFileFormatUtils.registerInputFormatChecker(
        SequenceFileInputFormat.class, SequenceFileInputFormatChecker.class);
    HiveFileFormatUtils.registerInputFormatChecker(RCFileInputFormat.class,
        RCFileInputFormat.class);
    inputFormatCheckerInstanceCache = new HashMap<Class<? extends InputFormatChecker>, InputFormatChecker>();
  }

  @SuppressWarnings("unchecked")
  private static Map<Class<? extends InputFormat>, Class<? extends InputFormatChecker>> inputFormatCheckerMap;

  private static Map<Class<? extends InputFormatChecker>, InputFormatChecker> inputFormatCheckerInstanceCache;

  @SuppressWarnings("unchecked")
  public synchronized static void registerInputFormatChecker(
      Class<? extends InputFormat> format,
      Class<? extends InputFormatChecker> checker) {
    inputFormatCheckerMap.put(format, checker);
  }

  public synchronized static Class<? extends InputFormatChecker> getInputFormatChecker(
      Class<?> inputFormat) {
    Class<? extends InputFormatChecker> result = inputFormatCheckerMap
        .get(inputFormat);
    return result;
  }

  @SuppressWarnings("unchecked")
  public static boolean checkInputFormat(FileSystem fs, HiveConf conf,
      Class<? extends org.apache.hadoop.mapred.InputFormat> inputFormatCls,
      ArrayList<FileStatus> files) throws HiveException {
    if (files.size() > 0) {
      Class<? extends InputFormatChecker> checkerCls = getInputFormatChecker(inputFormatCls);
      if (checkerCls == null
          && inputFormatCls.isAssignableFrom(TextInputFormat.class)) {
        return checkTextInputFormat(fs, conf, files);
      }

      if (checkerCls != null) {
        InputFormatChecker checkerInstance = inputFormatCheckerInstanceCache
            .get(checkerCls);
        try {
          if (checkerInstance == null) {
            checkerInstance = checkerCls.newInstance();
            inputFormatCheckerInstanceCache.put(checkerCls, checkerInstance);
          }
          return checkerInstance.validateInput(fs, conf, files);
        } catch (Exception e) {
          throw new HiveException(e);
        }
      }
      return true;
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  private static boolean checkTextInputFormat(FileSystem fs, HiveConf conf,
      ArrayList<FileStatus> files) throws HiveException {
    Set<Class<? extends InputFormat>> inputFormatter = inputFormatCheckerMap
        .keySet();
    for (Class<? extends InputFormat> reg : inputFormatter) {
      boolean result = checkInputFormat(fs, conf, reg, files);
      if (result)
        return false;
    }
    return true;
  }

  public static String READ_COLUMN_IDS_CONF_STR = "hive.io.file.readcolumn.ids";

  public static void setReadColumnIDs(Configuration conf, ArrayList<Integer> ids) {
    String id = null;
    if (ids != null) {
      for (int i = 0; i < ids.size(); i++) {
        if (i == 0) {
          id = "" + ids.get(i);
        } else {
          id = id + StringUtils.COMMA_STR + ids.get(i);
        }
      }
    }

    if (id == null || id.length() <= 0) {
      conf.set(READ_COLUMN_IDS_CONF_STR, "");
      return;
    }

    conf.set(READ_COLUMN_IDS_CONF_STR, id);
  }

  private static String toReadColumnIDString(ArrayList<Integer> ids) {
    String id = null;
    if (ids != null) {
      for (int i = 0; i < ids.size(); i++) {
        if (i == 0) {
          id = "" + ids.get(i);
        } else {
          id = id + StringUtils.COMMA_STR + ids.get(i);
        }
      }
    }
    return id;
  }

  private static void setReadColumnIDConf(Configuration conf, String id) {
    if (id == null || id.length() <= 0) {
      conf.set(READ_COLUMN_IDS_CONF_STR, "");
      return;
    }

    conf.set(READ_COLUMN_IDS_CONF_STR, id);
  }

  public static void appendReadColumnIDs(Configuration conf,
      ArrayList<Integer> ids) {
    String id = toReadColumnIDString(ids);
    if (id != null) {
      String old = conf.get(READ_COLUMN_IDS_CONF_STR, null);
      String newConfStr = id;
      if (old != null) {
        newConfStr = newConfStr + StringUtils.COMMA_STR + old;
      }

      setReadColumnIDConf(conf, newConfStr);
    }
  }

  public static ArrayList<Integer> getReadColumnIDs(Configuration conf) {
    String skips = conf.get(READ_COLUMN_IDS_CONF_STR, "");
    String[] list = StringUtils.split(skips);
    ArrayList<Integer> result = new ArrayList<Integer>(list.length);
    for (int i = 0; i < list.length; i++) {
      result.add(Integer.parseInt(list[i]));
    }
    return result;
  }

  public static void setFullyReadColumns(Configuration conf) {
    conf.set(READ_COLUMN_IDS_CONF_STR, "");
  }

  public static RecordWriter getHiveRecordWriter(JobConf jc,
      tableDesc tableInfo, Class<? extends Writable> outputClass,
      fileSinkDesc conf, Path outPath) throws HiveException {
    try {
      HiveOutputFormat<?, ?> hiveOutputFormat = tableInfo
          .getOutputFileFormatClass().newInstance();
      boolean isCompressed = conf.getCompressed();
      JobConf jc_output = jc;
      if (isCompressed) {
        jc_output = new JobConf(jc);
        String codecStr = conf.getCompressCodec();
        if (codecStr != null && !codecStr.trim().equals("")) {
          Class<? extends CompressionCodec> codec = (Class<? extends CompressionCodec>) Class
              .forName(codecStr);
          FileOutputFormat.setOutputCompressorClass(jc_output, codec);
        }
        String type = conf.getCompressType();
        if (type != null && !type.trim().equals("")) {
          CompressionType style = CompressionType.valueOf(type);
          SequenceFileOutputFormat.setOutputCompressionType(jc, style);
        }
      }
      return getRecordWriter(jc_output, hiveOutputFormat, outputClass,
          isCompressed, tableInfo.getProperties(), outPath);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public static RecordWriter getHiveRecordWriter(JobConf jc,
      tableDesc tableInfo, Class<? extends Writable> outputClass,
      LocalFileSinkDesc conf, Path outPath) throws HiveException {
    try {
      HiveOutputFormat<?, ?> hiveOutputFormat = tableInfo
          .getOutputFileFormatClass().newInstance();
      boolean isCompressed = conf.getCompressed();
      JobConf jc_output = jc;

      if (isCompressed) {
        jc_output = new JobConf(jc);
        String codecStr = conf.getCompressCodec();

        if (codecStr != null && !codecStr.trim().equals("")) {
          Class<? extends CompressionCodec> codec = (Class<? extends CompressionCodec>) Class
              .forName(codecStr);
          FileOutputFormat.setOutputCompressorClass(jc_output, codec);
        }

        String type = conf.getCompressType();
        if (type != null && !type.trim().equals("")) {
          CompressionType style = CompressionType.valueOf(type);
          SequenceFileOutputFormat.setOutputCompressionType(jc, style);
        }
      }

      return getRecordWriter(jc_output, hiveOutputFormat, outputClass,
          isCompressed, tableInfo.getProperties(), outPath);

    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public static RecordWriter getRecordWriter(JobConf jc,
      HiveOutputFormat<?, ?> hiveOutputFormat,
      final Class<? extends Writable> valueClass, boolean isCompressed,
      Properties tableProp, Path outPath) throws IOException, HiveException {
    if (hiveOutputFormat != null) {
      return hiveOutputFormat.getHiveRecordWriter(jc, outPath, valueClass,
          isCompressed, tableProp, null);
    }
    return null;
  }
}
