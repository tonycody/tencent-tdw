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
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.plan.fetchWork;
import org.apache.hadoop.hive.ql.plan.mapredLocalWork;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.plan.mapredLocalWork.HashMapJoinContext;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCardinalityEstimation;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFExplode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFPosExplode;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ExecMapper extends MapReduceBase implements Mapper {

  private MapOperator mo;
  private Map<String, FetchOperator> fetchOperators;
  private OutputCollector oc;
  private JobConf jc;
  private boolean abort = false;
  private Reporter rp;
  public static final Log l4j = LogFactory.getLog("ExecMapper");
  private static boolean done;

  private MemoryMXBean memoryMXBean;
  private long numRows = 0;
  private long nextCntr = 1;

  private String lastInputFile = null;
  private mapredLocalWork localWork = null;
  private LinkedHashMap<String, LinkedHashMap<String, ArrayList<String>>> aliasHashPathMapping;

  public void configure(JobConf job) {
    memoryMXBean = ManagementFactory.getMemoryMXBean();
    l4j.info("maximum memory = " + memoryMXBean.getHeapMemoryUsage().getMax());

    try {
      l4j.info("conf classpath = "
          + Arrays.asList(((URLClassLoader) job.getClassLoader()).getURLs()));
      l4j.info("thread classpath = "
          + Arrays.asList(((URLClassLoader) Thread.currentThread()
              .getContextClassLoader()).getURLs()));
    } catch (Exception e) {
      l4j.info("cannot get classpath: " + e.getMessage());
    }
    try {
      jc = job;
      int estCountBucketSize = jc.getInt("hive.exec.estdistinct.bucketsize.log", 15);
      int estCountBufferSize = jc.getInt("hive.exec.estdistinct.buffsize.log", 8);
      GenericUDAFCardinalityEstimation.initParams(estCountBucketSize, estCountBufferSize);
      
      boolean isChangSizeZero2Null = jc.getBoolean(HiveConf.ConfVars.HIVE_UDTF_EXPLODE_CHANGE_ZERO_SIZE_2_NULL.varname,
              HiveConf.ConfVars.HIVE_UDTF_EXPLODE_CHANGE_ZERO_SIZE_2_NULL.defaultBoolVal);
      boolean isChangeNull2Null = jc.getBoolean(HiveConf.ConfVars.HIVE_UDTF_EXPLODE_CHANGE_NULL_2_NULL.varname,
          HiveConf.ConfVars.HIVE_UDTF_EXPLODE_CHANGE_NULL_2_NULL.defaultBoolVal);
      GenericUDTFExplode.isChangSizeZero2Null = isChangSizeZero2Null;
      GenericUDTFExplode.isChangNull2Null = isChangeNull2Null;
      GenericUDTFPosExplode.isChangSizeZero2Null = isChangSizeZero2Null;
      GenericUDTFPosExplode.isChangNull2Null = isChangeNull2Null;
      
      mapredWork mrwork = Utilities.getMapRedWork(job);
      mo = new MapOperator();
      mo.setConf(mrwork);
      mo.setChildren(job);
      l4j.info(mo.dump(0));
      mo.initialize(jc, null);

      localWork = mrwork.getMapLocalWork();
      if (localWork == null) {
        return;
      }
      fetchOperators = new HashMap<String, FetchOperator>();
      for (Map.Entry<String, fetchWork> entry : localWork.getAliasToFetchWork()
          .entrySet()) {
        fetchOperators.put(entry.getKey(), new FetchOperator(entry.getValue(),
            job));
        l4j.info("fetchoperator for " + entry.getKey() + " created");
      }
      for (Map.Entry<String, FetchOperator> entry : fetchOperators.entrySet()) {
        Operator<? extends Serializable> forwardOp = localWork.getAliasToWork()
            .get(entry.getKey());
        forwardOp.initialize(jc, new ObjectInspector[] { entry.getValue()
            .getOutputObjectInspector() });
        l4j.info("fetchoperator for " + entry.getKey() + " initialized");
      }
    } catch (Throwable e) {
      abort = true;
      if (e instanceof OutOfMemoryError) {
        throw (OutOfMemoryError) e;
      } else {
        throw new RuntimeException("Map operator initialization failed", e);
      }
    }

  }

  public void map(Object key, Object value, OutputCollector output,
      Reporter reporter) throws IOException {
    if (oc == null) {
      oc = output;
      rp = reporter;
      mo.setOutputCollector(oc);
      mo.setReporter(rp);

      if (localWork != null && (this.lastInputFile == null)) {
        if (this.localWork.getHashMapjoinContext() == null) {
          processOldMapLocalWork();
        } else {
          this.lastInputFile = HiveConf.getVar(jc,
              HiveConf.ConfVars.HADOOPMAPFILENAME);
          processMapLocalWork();
        }

      }
    }

    try {
      if (mo.getDone())
        done = true;
      else {
        mo.process((Writable) value);
        if (l4j.isInfoEnabled()) {
          numRows++;
          if (numRows == nextCntr) {
            long used_memory = memoryMXBean.getHeapMemoryUsage().getUsed();
            l4j.info("ExecMapper: processing " + numRows
                + " rows: used memory = " + used_memory);
            nextCntr = getNextCntr(numRows);
          }
        }
      }
    } catch (Throwable e) {
      abort = true;
      e.printStackTrace();
      if (e instanceof OutOfMemoryError) {
        throw (OutOfMemoryError) e;
      } else {
        throw new RuntimeException(e.getMessage(), e);
      }
    }
  }

  private boolean inputFileChanged() {
    String currentInputFile = HiveConf.getVar(jc,
        HiveConf.ConfVars.HADOOPMAPFILENAME);
    if (this.lastInputFile == null
        || !this.lastInputFile.equals(currentInputFile)) {
      return true;
    }
    return false;
  }

  private void processMapLocalWork() {
    l4j.info("Begin to process map side computing!");
    if (fetchOperators != null) {
      try {
        int fetchOpNum = 0;
        for (Map.Entry<String, FetchOperator> entry : fetchOperators.entrySet()) {
          int fetchOpRows = 0;
          String alias = entry.getKey();
          FetchOperator fetchOp = entry.getValue();
          l4j.info("The table processed in the fetch operator: " + alias);

          fetchOp.clearFetchContext();
          setUpFetchOpContext(fetchOp, alias);

          Operator<? extends Serializable> forwardOp = localWork
              .getAliasToWork().get(alias);

          while (true) {
            InspectableObject row = fetchOp.getNextRow();
            if (row == null) {
              forwardOp.close(false);
              break;
            }
            fetchOpRows++;
            forwardOp.process(row.o, 0);
            if (forwardOp.getDone()) {
              done = true;
              break;
            }
          }

          if (l4j.isInfoEnabled()) {
            l4j.info("fetch " + fetchOpNum++ + " processed " + fetchOpRows
                + " used mem: " + memoryMXBean.getHeapMemoryUsage().getUsed());
          }
        }
      } catch (Throwable e) {
        abort = true;
        if (e instanceof OutOfMemoryError) {
          throw (OutOfMemoryError) e;
        } else {
          throw new RuntimeException("Map local work failed", e);
        }
      }
    }
  }

  private void processOldMapLocalWork() {
    if (fetchOperators != null) {
      try {
        mapredLocalWork localWork = mo.getConf().getMapLocalWork();
        int fetchOpNum = 0;
        for (Map.Entry<String, FetchOperator> entry : fetchOperators.entrySet()) {
          int fetchOpRows = 0;
          String alias = entry.getKey();
          FetchOperator fetchOp = entry.getValue();
          Operator<? extends Serializable> forwardOp = localWork
              .getAliasToWork().get(alias);

          while (true) {
            InspectableObject row = fetchOp.getNextRow();
            if (row == null) {
              break;
            }
            fetchOpRows++;
            forwardOp.process(row.o, 0);
          }

          if (l4j.isInfoEnabled()) {
            l4j.info("fetch " + fetchOpNum++ + " processed " + fetchOpRows
                + " used mem: " + memoryMXBean.getHeapMemoryUsage().getUsed());
          }
        }
      } catch (Throwable e) {
        abort = true;
        if (e instanceof OutOfMemoryError) {
          throw (OutOfMemoryError) e;
        } else {
          throw new RuntimeException("Map local work failed", e);
        }
      }
    }
  }

  private String tellHashParFromFileName(String fileName) {
    String hashPar = null;
    if (!fileName.contains("Hash_"))
      return null;
    int begin = fileName.indexOf("Hash_");
    hashPar = fileName.substring(begin, begin + 9);
    return hashPar;
  }

  List<Path> getAliasHashFiles(String hashPar, String refTableAlias,
      String alias) {
    List<String> pathStr = aliasHashPathMapping.get(alias).get(hashPar);
    List<Path> paths = new ArrayList<Path>();
    if (pathStr != null) {
      for (String p : pathStr) {
        l4j.info("Loading file " + p + " for " + alias + ". (" + hashPar + ")");
        paths.add(new Path(p));
      }
    }
    return paths;
  }

  private void setUpFetchOpContext(FetchOperator fetchOp, String alias)
      throws Exception {
    HashMapJoinContext hashMapJoinCxt = this.localWork.getHashMapjoinContext();
    if (hashMapJoinCxt != null) {
      l4j.info("The HashMapJoinContext is not null!");
    } else {
      throw new Exception("localWork.getHashMapjoinContext() is null");
    }
    this.aliasHashPathMapping = hashMapJoinCxt.getAliasHashParMapping();
    if (this.aliasHashPathMapping == null) {
      l4j.info("aliasHashPathMapping is null!");
    }
    String par = tellHashParFromFileName(this.lastInputFile);
    l4j.info("The last input file: " + this.lastInputFile);
    l4j.info("The par being processed: " + par);
    if (par == null)
      throw new Exception("The input file " + this.lastInputFile
          + " is not in a hash partition!");
    List<Path> aliasFiles = getAliasHashFiles(par,
        hashMapJoinCxt.getMapJoinBigTableAlias(), alias);

    l4j.info("The file processed in the fetch operator: "
        + aliasFiles.toArray().toString());
    Iterator<Path> iter = aliasFiles.iterator();
    fetchOp.setupContext(iter);
  }

  private long getNextCntr(long cntr) {
    if (cntr >= 1000000)
      return cntr + 1000000;

    return 10 * cntr;
  }

  public void close() {
    if (oc == null) {
      l4j.trace("Close called. no row processed by map.");
    }

    try {
      mo.close(abort);
      if (fetchOperators != null) {
        mapredLocalWork localWork = mo.getConf().getMapLocalWork();
        for (Map.Entry<String, FetchOperator> entry : fetchOperators.entrySet()) {
          Operator<? extends Serializable> forwardOp = localWork
              .getAliasToWork().get(entry.getKey());
          forwardOp.close(abort);
        }
      }

      if (l4j.isInfoEnabled()) {
        long used_memory = memoryMXBean.getHeapMemoryUsage().getUsed();
        l4j.info("ExecMapper: processed " + numRows + " rows: used memory = "
            + used_memory);
      }

      reportStats rps = new reportStats(rp);
      mo.preorderMap(rps);
      oc = null;
      mo.setOutputCollector(oc);
      return;
    } catch (Exception e) {
      if (!abort) {
        l4j.error("Hit error while closing operators - failing tree");
        throw new RuntimeException("Error while closing operators", e);
      }
    }
  }

  public static boolean getDone() {
    return done;
  }

  public static class reportStats implements Operator.OperatorFunc {
    Reporter rp;

    public reportStats(Reporter rp) {
      this.rp = rp;
    }

    public void func(Operator op) {
      Map<Enum, Long> opStats = op.getStats();
      for (Map.Entry<Enum, Long> e : opStats.entrySet()) {
        if (this.rp != null) {
          rp.incrCounter(e.getKey(), e.getValue());
        }
      }
    }
  }
}
