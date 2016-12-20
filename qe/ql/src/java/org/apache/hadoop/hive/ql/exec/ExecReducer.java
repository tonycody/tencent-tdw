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

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.net.URLClassLoader;
import java.util.*;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCardinalityEstimation;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFExplode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFPosExplode;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.exec.ExecMapper.reportStats;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ExecReducer extends MapReduceBase implements Reducer {

  ComputationBalancerReducer cbr;
  private JobConf jc;
  private OutputCollector<?, ?> oc;
  private Operator<?> reducer;
  private Reporter rp;
  private boolean abort = false;
  private boolean isTagged = false;
  private long cntr = 0;
  private long nextCntr = 1;
  private Log LOG = LogFactory.getLog(this.getClass().getName());

  private static String[] fieldNames;
  public static final Log l4j = LogFactory.getLog("ExecReducer");

  private MemoryMXBean memoryMXBean;

  private Deserializer inputKeyDeserializer;
  private SerDe[] inputValueDeserializer = new SerDe[Byte.MAX_VALUE];
  static {
    ArrayList<String> fieldNameArray = new ArrayList<String>();
    for (Utilities.ReduceField r : Utilities.ReduceField.values()) {
      fieldNameArray.add(r.toString());
    }
    fieldNames = fieldNameArray.toArray(new String[0]);
  }

  public void configure(JobConf job) {
    ObjectInspector[] rowObjectInspector = new ObjectInspector[Byte.MAX_VALUE];
    ObjectInspector[] valueObjectInspector = new ObjectInspector[Byte.MAX_VALUE];
    ObjectInspector keyObjectInspector;

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
    
    mapredWork gWork = Utilities.getMapRedWork(job);
    reducer = gWork.getReducer();
    reducer.setParentOperators(null);
    isTagged = gWork.getNeedsTagging();
    try {
      tableDesc keyTableDesc = gWork.getKeyDesc();
      inputKeyDeserializer = (SerDe) ReflectionUtils.newInstance(
          keyTableDesc.getDeserializerClass(), null);
      inputKeyDeserializer.initialize(null, keyTableDesc.getProperties());
      keyObjectInspector = inputKeyDeserializer.getObjectInspector();
      for (int tag = 0; tag < gWork.getTagToValueDesc().size(); tag++) {
        tableDesc valueTableDesc = gWork.getTagToValueDesc().get(tag);
        inputValueDeserializer[tag] = (SerDe) ReflectionUtils.newInstance(
            valueTableDesc.getDeserializerClass(), null);
        inputValueDeserializer[tag].initialize(null,
            valueTableDesc.getProperties());
        valueObjectInspector[tag] = inputValueDeserializer[tag]
            .getObjectInspector();

        ArrayList<ObjectInspector> ois = new ArrayList<ObjectInspector>();
        ois.add(keyObjectInspector);
        ois.add(valueObjectInspector[tag]);
        ois.add(PrimitiveObjectInspectorFactory.writableByteObjectInspector);
        rowObjectInspector[tag] = ObjectInspectorFactory
            .getStandardStructObjectInspector(Arrays.asList(fieldNames), ois);
      }

      cbr = new ComputationBalancerReducer();
      cbr.setJobConf(jc);
      String tableNameString = jc
          .get(ComputationBalancerReducer.CBR_TABLENAME_ATTR);
      cbr.setTableName(tableNameString);
      String tableDataLocationString = jc
          .get(ComputationBalancerReducer.CBR_TABLEDATALOCATION_ATTR);
      cbr.setTableDataLocation(tableDataLocationString);
      cbr.parseTableStruct();
      String _s = job.get(ComputationBalancerReducer.CBR_FLUSHFILEURI_ATTR);
      cbr.setDestURI("/tmp//" + _s + "//");
      // LOG.info("/tmp//hive-michealxu//" + _s + "//");
      cbr.setNumberOfBins(10);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    try {
      l4j.info(reducer.dump(0));
      reducer.initialize(jc, rowObjectInspector);
    } catch (Throwable e) {
      abort = true;
      if (e instanceof OutOfMemoryError) {
        throw (OutOfMemoryError) e;
      } else {
        throw new RuntimeException("Reduce operator initialization failed", e);
      }
    }
  }

  private Object keyObject;
  private Object[] valueObject = new Object[Byte.MAX_VALUE];

  private BytesWritable groupKey;

  ArrayList<Object> row = new ArrayList<Object>(3);
  ByteWritable tag = new ByteWritable();

  public void reduce(Object key, Iterator values, OutputCollector output,
      Reporter reporter) throws IOException {

    if (oc == null) {
      oc = output;
      rp = reporter;
      reducer.setOutputCollector(oc);
      reducer.setReporter(rp);
    }

    try {

      BytesWritable keyWritable = (BytesWritable) key;

      byte _byte_ = 0;
      if (new Boolean(jc.get("Cb.Switch")) == true) {
        if (keyWritable.getSize() > 0) {
          _byte_ = keyWritable.get()[keyWritable.getSize() - 1];
        }
      }

      tag.set((byte) 0);
      int size = keyWritable.getSize();
      if (isTagged) {
        tag.set(keyWritable.get()[--size]);
        keyWritable.setSize(size);
      }

      LOG.debug("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
      LOG.debug("_byte_:  " + _byte_);
      LOG.debug("Cb.Switch:  " + jc.get("Cb.Switch"));
      LOG.debug("isTagged:  " + isTagged);
      LOG.debug("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

      if (_byte_ == StatsCollectionOperator.streamTag
          || _byte_ == SampleOperator.streamTag
          || _byte_ == HistogramOperator.streamTag) {
        while (values.hasNext()) {
          try {
            Text valueText = (Text) values.next();
            cbr.reduce(keyWritable, valueText);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        return;
      }

      if (!keyWritable.equals(groupKey)) {
        if (groupKey == null) {
          groupKey = new BytesWritable();
        } else {
          l4j.trace("End Group");
          reducer.endGroup();
        }

        try {
          keyObject = inputKeyDeserializer.deserialize(keyWritable);
        } catch (Exception e) {
          throw new HiveException(e);
        }

        groupKey.set(keyWritable.get(), 0, keyWritable.getSize());
        l4j.trace("Start Group");
        reducer.startGroup();
        reducer.setGroupKeyObject(keyObject);
      }
      while (values.hasNext()) {
        Writable valueWritable = (Writable) values.next();
        try {
          valueObject[tag.get()] = inputValueDeserializer[tag.get()]
              .deserialize(valueWritable);
        } catch (SerDeException e) {
          throw new HiveException(e);
        }
        row.clear();
        row.add(keyObject);
        row.add(valueObject[tag.get()]);
        row.add(tag);
        if (l4j.isInfoEnabled()) {
          cntr++;
          if (cntr == nextCntr) {
            long used_memory = memoryMXBean.getHeapMemoryUsage().getUsed();
            l4j.info("ExecReducer: processing " + cntr
                + " rows: used memory = " + used_memory);
            nextCntr = getNextCntr(cntr);
          }
        }
        reducer.process(row, tag.get());
      }

    } catch (Throwable e) {
      abort = true;
      if (e instanceof OutOfMemoryError) {
        throw (OutOfMemoryError) e;
      } else {
        throw new IOException(e);
      }
    }
  }

  private long getNextCntr(long cntr) {
    if (cntr >= 1000000)
      return cntr + 1000000;

    return 10 * cntr;
  }

  public void close() {

    if (oc == null) {
      l4j.trace("Close called no row");
    }

    try {
      if (groupKey != null) {
        l4j.trace("End Group");
        reducer.endGroup();
      }
      if (l4j.isInfoEnabled()) {
        l4j.info("ExecReducer: processed " + cntr + " rows: used memory = "
            + memoryMXBean.getHeapMemoryUsage().getUsed());
      }

      reducer.close(abort);
      reportStats rps = new reportStats(rp);
      reducer.preorderMap(rps);

      assert (cbr != null);
      cbr.close();

      return;
    } catch (Exception e) {
      if (!abort) {
        l4j.error("Hit error while closing operators - failing tree");
        throw new RuntimeException("Error while closing operators: "
            + e.getMessage(), e);
      }
    }
  }
}
