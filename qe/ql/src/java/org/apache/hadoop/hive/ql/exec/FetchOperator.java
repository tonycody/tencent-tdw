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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.fetchWork;
import org.apache.hadoop.hive.ql.plan.partitionDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class FetchOperator {

  transient protected Log LOG;
  transient protected LogHelper console;

  public FetchOperator(fetchWork work, JobConf job) {
    LOG = LogFactory.getLog(this.getClass().getName());
    console = new LogHelper(LOG);

    this.work = work;
    this.job = job;

    currRecReader = null;
    currPath = null;
    currTbl = null;
    iterPath = null;
  }

  private fetchWork work;
  private int splitNum;
  private RecordReader<WritableComparable, Writable> currRecReader;
  private InputSplit[] inputSplits;
  private InputFormat inputFormat;
  private JobConf job;
  private WritableComparable key;
  private Writable value;
  private Deserializer serde;
  private Iterator<Path> iterPath;
  private Path currPath;
  private tableDesc currTbl;
  private StructObjectInspector rowObjectInspector;

  private static Map<Class, InputFormat<WritableComparable, Writable>> inputFormats = new HashMap<Class, InputFormat<WritableComparable, Writable>>();

  static InputFormat<WritableComparable, Writable> getInputFormatFromCache(
      Class inputFormatClass, Configuration conf) throws IOException {
    if (!inputFormats.containsKey(inputFormatClass)) {
      try {
        InputFormat<WritableComparable, Writable> newInstance = (InputFormat<WritableComparable, Writable>) ReflectionUtils
            .newInstance(inputFormatClass, conf);
        inputFormats.put(inputFormatClass, newInstance);
      } catch (Exception e) {
        throw new IOException("Cannot create an instance of InputFormat class "
            + inputFormatClass.getName() + " as specified in mapredWork!");
      }
    }
    return inputFormats.get(inputFormatClass);
  }

  private void getNextPath() throws Exception {

    if (iterPath == null) {

      currTbl = work.getTblDesc();
      iterPath = fetchWork.convertStringToPathArray(work.getFetchDir())
          .iterator();
    }

    while (iterPath.hasNext()) {
      Path nxt = iterPath.next();
      FileSystem fs = nxt.getFileSystem(job);
      if (fs.exists(nxt)) {
        FileStatus[] fStats = fs.listStatus(nxt);
        for (FileStatus fStat : fStats) {
          if (fStat.getLen() > 0) {
            currPath = nxt;
            return;
          }
        }
      }
    }
  }

  private RecordReader<WritableComparable, Writable> getRecordReader()
      throws Exception {
    try {
      if (currPath == null) {
        getNextPath();

        if (currPath == null)
          return null;

        LOG.info("set mapred.input.dir = " + currPath.toString());
        if (SessionState.get() != null)
          SessionState.get().ssLog(
              "set mapred.input.dir = " + currPath.toString());

        job.set("mapred.input.dir", org.apache.hadoop.util.StringUtils
            .escapeString(currPath.toString()));

        tableDesc tmp = currTbl;

        inputFormat = getInputFormatFromCache(tmp.getInputFileFormatClass(),
            job);
        inputSplits = inputFormat.getSplits(job, 1);
        splitNum = 0;
        serde = tmp.getDeserializerClass().newInstance();
        serde.initialize(job, tmp.getProperties());
        LOG.debug("Creating fetchTask with deserializer typeinfo: "
            + serde.getObjectInspector().getTypeName());
        LOG.debug("deserializer properties: " + tmp.getProperties());
      }

      if (splitNum >= inputSplits.length) {
        if (currRecReader != null) {
          currRecReader.close();
          currRecReader = null;
        }
        currPath = null;
        return getRecordReader();
      }

      currRecReader = inputFormat.getRecordReader(inputSplits[splitNum++], job,
          Reporter.NULL);
      key = currRecReader.createKey();
      value = currRecReader.createValue();
      return currRecReader;
    } catch (Exception e) {
      LOG.info(org.apache.hadoop.util.StringUtils.stringifyException(e));
      throw e;
    }
  }

  public InspectableObject getNextRow() throws IOException {
    try {
      if (currRecReader == null) {
        currRecReader = getRecordReader();
        if (currRecReader == null)
          return null;
      }

      boolean ret = currRecReader.next(key, value);
      if (ret) {
        Object obj = serde.deserialize(value);
        if (obj == null && serde instanceof LazySimpleSerDe)
          return getNextRow();
        return new InspectableObject(obj, serde.getObjectInspector());
      } else {
        currRecReader.close();
        currRecReader = null;
        currRecReader = getRecordReader();
        if (currRecReader == null)
          return null;
        else
          return getNextRow();
      }
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public void clearFetchContext() throws HiveException {
    try {
      if (currRecReader != null) {
        currRecReader.close();
        currRecReader = null;
      }
    } catch (Exception e) {
      throw new HiveException("Failed with exception " + e.getMessage()
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
  }

  public ObjectInspector getOutputObjectInspector() throws HiveException {
    try {
      ObjectInspector outInspector;
      if (work.getTblDesc() != null) {
        tableDesc tbl = work.getTblDesc();
        Deserializer serde = tbl.getDeserializerClass().newInstance();
        serde.initialize(job, tbl.getProperties());
        return serde.getObjectInspector();

      } else
        throw new HiveException("error : fetch op without tblDesc!");
    } catch (Exception e) {
      throw new HiveException("Failed with exception " + e.getMessage()
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
  }

  public void setupContext(Iterator<Path> iterPath) {
    this.iterPath = iterPath;
    this.currTbl = work.getTblDesc();
  }
}
