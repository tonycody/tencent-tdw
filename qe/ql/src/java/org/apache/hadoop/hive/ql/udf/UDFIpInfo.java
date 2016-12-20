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
package org.apache.hadoop.hive.ql.udf;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

public class UDFIpInfo extends UDF {

  private static Log LOG = LogFactory.getLog(UDFIpInfo.class.getName());

  public class IpRange {
    private long startip;
    private long endip;
    private Object[] row;

    public IpRange() {
    }

    public IpRange(long start, long end, Object[] row) {
      this.startip = start;
      this.endip = end;
      this.row = row;
    }

    public long getStartip() {
      return startip;
    }

    public long getEndip() {
      return endip;
    }

    public Object[] getRow() {
      return row;
    }

    public void setStartip(long startip) {
      this.startip = startip;
    }

    public void setEndip(long endip) {
      this.endip = endip;
    }

    public void setRow(Object[] row) {
      this.row = row;
    }

    public String toString() {
      return new String(this.startip + " " + this.endip + " "
          + this.row.toString());
    }

  }

  private TreeMap<Long, IpRange> ipTables;

  private static HashMap<String, TreeMap<Long, IpRange>> tabToIpTables = new HashMap<String, TreeMap<Long, IpRange>>();

  public UDFIpInfo() {
  }

  private static Object convertLazyToJava(Object o, ObjectInspector oi) {
    Object obj = ObjectInspectorUtils.copyToStandardObject(o, oi,
        ObjectInspectorCopyOption.JAVA);
    if (obj != null && oi.getCategory() != ObjectInspector.Category.PRIMITIVE) {
      obj = obj.toString();
    }
    return obj;
  }

  private IpRange subSetRange(IpRange rang1, IpRange rang2) {
    if (rang1.getStartip() < rang2.getStartip()) {
      if (rang1.getEndip() < rang2.getStartip()) {
        this.ipTables.put(rang1.getEndip(), rang1);
        return rang2;
      } else if (rang1.getEndip() == rang2.getStartip()) {
        rang1.setEndip(rang2.getStartip() - 1);
        this.ipTables.put(rang1.getEndip(), rang1);
        return rang2;
      } else if (rang1.getEndip() < rang2.getEndip()) {
        rang1.setEndip(rang2.getStartip() - 1);
        this.ipTables.put(rang1.getEndip(), rang1);
        return rang2;
      } else if (rang1.getEndip() == rang2.getEndip()) {
        rang1.setEndip(rang2.getStartip() - 1);
        this.ipTables.put(rang1.getEndip(), rang1);
        return rang2;
      } else {
        this.ipTables.put(rang2.getEndip(), rang2);
        IpRange rang = new IpRange(rang1.getStartip(), rang2.getStartip() - 1,
            rang1.getRow());
        this.ipTables.put(rang.getEndip(), rang);
        rang1.setStartip(rang2.getEndip() + 1);
        this.ipTables.put(rang1.getEndip(), rang1);
        return null;
      }
    } else if (rang1.getStartip() == rang2.getStartip()) {
      if (rang1.getEndip() <= rang2.getEndip()) {
        return rang2;
      } else {
        this.ipTables.put(rang2.getEndip(), rang2);
        rang1.setStartip(rang2.getEndip() + 1);
        this.ipTables.put(rang1.getEndip(), rang1);
        return null;
      }
    } else if (rang1.getStartip() > rang2.getStartip()) {
      if (rang1.getEndip() <= rang2.getEndip()) {
        return rang2;
      } else {
        this.ipTables.put(rang2.getEndip(), rang2);
        rang1.setStartip(rang2.getEndip() + 1);
        this.ipTables.put(rang1.getEndip(), rang1);
        return null;
      }
    }
    return null;
  }

  private void putToIpTables(IpRange range) {
    Long range_end = range.getEndip();
    SortedMap<Long, IpRange> subList = ipTables.tailMap(range.getStartip());
    List<IpRange> rangeList = new ArrayList<IpRange>();
    for (Iterator<Long> it = subList.keySet().iterator(); it.hasNext();) {
      Long start = it.next();
      IpRange rang1 = subList.get(start);
      if (rang1.getStartip() <= range_end) {
        rangeList.add(rang1);
      }
    }

    for (int i = 0; i < rangeList.size(); i++) {
      IpRange rang1 = rangeList.get(i);
      this.ipTables.remove(rang1.getEndip());
      range = this.subSetRange(rang1, range);
      if (range == null) {
        break;
      }
    }
    if (range != null) {
      this.ipTables.put(range.getEndip(), range);
    }
  }

  private void init(String tableName) throws HiveException {
    SerDe serde;
    try {
      serde = new LazySimpleSerDe();
    } catch (SerDeException e) {
      LOG.info("SerDeException::" + e.getMessage());
      throw new HiveException(e.getMessage());
    }
    Properties props = new Properties();
    int index = tableName.indexOf(",");
    LOG.info("init::tableName::" + tableName);
    String tablePath = tableName.substring(0, index);
    String types = tableName.substring(index + 1);
    String[] colTypes = types.split(",");
    String names = "";
    for (int i = 0; i < colTypes.length; i++) {
      if (i != colTypes.length - 1) {
        names = names + "col_" + i + ",";
      } else {
        names = names + "col_" + i;
      }
    }
    if (names.length() > 0) {
      LOG.info("init::names::" + names);
      props.setProperty(Constants.LIST_COLUMNS, names);
    }
    if (types.length() > 0) {
      LOG.info("init::types::" + types);
      props.setProperty(Constants.LIST_COLUMN_TYPES, types);
    }
    JobConf job = new JobConf();
    job.set("mapred.input.dir", tablePath);
    List<? extends StructField> fieldRefs;
    StructObjectInspector soi;
    try {
      serde.initialize(job, props);
      soi = (StructObjectInspector) serde.getObjectInspector();
      fieldRefs = soi.getAllStructFieldRefs();
    } catch (SerDeException e) {
      LOG.info("SerDeException::" + e.getMessage());
      throw new HiveException(e.getMessage());
    }
    TextInputFormat inputFormat = new TextInputFormat();
    ArrayList<FileSplit> splits = new ArrayList<FileSplit>();
    Path p = new Path(tablePath);
    FileSystem fs;
    int splitNum = 0;
    InputSplit[] inputSplits;
    RecordReader<LongWritable, Text> currRecReader = null;
    try {
      fs = p.getFileSystem(job);
      FileStatus[] files = fs.listStatus(p);
      for (FileStatus file : files) {
        Path path = file.getPath();
        long length = file.getLen();
        FileSplit split = new FileSplit(path, 0, length, job);
        splits.add(split);
      }
      inputSplits = splits.toArray(new FileSplit[splits.size()]);
      LOG.info("inputSplits::" + inputSplits.length);
      if (inputSplits.length < 1) {
        LOG.info("ip table files is null.");
        return;
      }
      currRecReader = inputFormat.getRecordReader(inputSplits[splitNum++], job,
          Reporter.NULL);
      while (true) {
        if (currRecReader == null) {
          break;
        }
        LongWritable key = currRecReader.createKey();
        Text value = currRecReader.createValue();
        boolean ret = currRecReader.next(key, value);
        if (ret) {
          Object data = null;
          try {
            data = serde.deserialize(value);
          } catch (SerDeException e) {
            continue;
          }
          Object[] row = new Object[colTypes.length - 2];
          long startIp = 0;
          long endIp = 0;
          boolean isRight = true;
          int start = 0;
          for (int i = 0; i < fieldRefs.size(); i++) {
            StructField fieldRef = fieldRefs.get(i);
            ObjectInspector oi = fieldRef.getFieldObjectInspector();
            Object obj = soi.getStructFieldData(data, fieldRef);
            obj = convertLazyToJava(obj, oi);
            if (i == 0) {
              if (obj instanceof Long && obj != null) {
                startIp = (Long) obj;
              } else {
                isRight = false;
              }
            } else if (i == 1) {
              if (obj instanceof Long && obj != null) {
                endIp = (Long) obj;
              } else {
                isRight = false;
              }
            } else {
              row[start++] = obj;
            }
          }
          if (isRight) {
            if (startIp <= endIp) {
              this.putToIpTables(new IpRange(startIp, endIp, row));
            }
          }
        } else {
          if (splitNum == inputSplits.length) {
            break;
          } else {
            currRecReader = inputFormat.getRecordReader(
                inputSplits[splitNum++], job, Reporter.NULL);
          }
        }
      }
      LOG.info("init::" + tablePath + " fininsh.");
    } catch (IOException e) {
      LOG.info("SerDeException::" + e.getMessage());
      throw new HiveException(e.getMessage());
    }
  }

  public Text evaluate(Text table, Text ipString, IntWritable idx)
      throws HiveException {
    if (ipString == null) {
      return null;
    }
    UDFInet_aton ipToLong = new UDFInet_aton();
    LongWritable ipLong = ipToLong.evaluate(ipString);
    if (ipLong == null) {
      return null;
    }
    return this.evaluate(table, ipLong, idx);
  }

  public Text evaluate(Text table, LongWritable ip, IntWritable idx)
      throws HiveException {
    if (ip == null || idx == null) {
      return null;
    }
    String tableName = table.toString();
    int index = tableName.indexOf(",");
    String tablePath = tableName.substring(0, index);
    if (tabToIpTables.containsKey(tablePath)) {
      ipTables = tabToIpTables.get(tablePath);
    } else {
      ipTables = new TreeMap<Long, IpRange>();
      tabToIpTables.put(tablePath, ipTables);
      this.init(tableName);
    }

    Map.Entry<Long, IpRange> entry = ipTables.ceilingEntry(ip.get());
    if (entry != null) {
      IpRange range = entry.getValue();
      long startIp = range.getStartip();
      if (ip.get() >= startIp) {
        if (idx.get() > 0) {
          if (idx.get() == 1) {
            return new Text(Long.toString(range.getStartip()));
          } else if (idx.get() == 2) {
            return new Text(Long.toString(range.getEndip()));
          } else {
            Object[] row = range.getRow();
            if (row.length > idx.get() - 3) {
              if (row[idx.get() - 3] == null) {
                return null;
              } else {
                return new Text(row[idx.get() - 3].toString());
              }
            }
          }
        }
      }
    }
    return null;
  }

  public Text evaluate(Text table, IntWritable ipNum, IntWritable idx)
      throws HiveException {
    if (ipNum == null) {
      return null;
    }
    return this.evaluate(table, new LongWritable(ipNum.get()), idx);
  }

}
