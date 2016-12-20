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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.plan.partitionDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class MapOperator extends Operator<mapredWork> implements Serializable {

  private static final long serialVersionUID = 1L;

  public static enum Counter {
    DESERIALIZE_ERRORS, READ_SUCCESS_COUNT, READ_ERROR_COUNT, SELECT_SUCCESS_COUNT, SELECT_ERROR_COUNT
  }

  transient private LongWritable deserialize_error_count = new LongWritable();
  transient private LongWritable read_success_count = new LongWritable();
  transient private LongWritable read_error_count = new LongWritable();
  transient private Deserializer deserializer;

  transient private Object[] rowWithPart;
  transient private StructObjectInspector rowObjectInspector;
  transient private boolean isPartitioned;
  private Map<MapInputPath, MapOpCtx> opCtxMap;

  private ArrayList<Operator<? extends Serializable>> extraChildrenToClose = null;

  int maxnum_rowdeleted_printlog = 0;

  private static class MapInputPath {
    String path;
    String alias;
    Operator<? extends Serializable> op;

    public MapInputPath(String path, String alias,
        Operator<? extends Serializable> op) {
      this.path = path;
      this.alias = alias;
      this.op = op;
    }

    public boolean equals(Object o) {
      if (o instanceof MapInputPath) {
        MapInputPath mObj = (MapInputPath) o;
        if (mObj == null)
          return false;
        return path.equals(mObj.path) && alias.equals(mObj.alias)
            && op.equals(mObj.op);
      }

      return false;
    }

    public int hashCode() {
      return (op == null) ? 0 : op.hashCode();
    }
  }

  private static class MapOpCtx {
    boolean isPartitioned;
    StructObjectInspector rowObjectInspector;
    Object[] rowWithPart;
    Deserializer deserializer;
    public String tableName;
    public String partName;

    public MapOpCtx(boolean isPartitioned,
        StructObjectInspector rowObjectInspector, Object[] rowWithPart,
        Deserializer deserializer) {
      this.isPartitioned = isPartitioned;
      this.rowObjectInspector = rowObjectInspector;
      this.rowWithPart = rowWithPart;
      this.deserializer = deserializer;
    }

    public boolean isPartitioned() {
      return isPartitioned;
    }

    public StructObjectInspector getRowObjectInspector() {
      return rowObjectInspector;
    }

    public Object[] getRowWithPart() {
      return rowWithPart;
    }

    public Deserializer getDeserializer() {
      return deserializer;
    }
  }

  public void initializeAsRoot(Configuration hconf, mapredWork mrwork)
      throws HiveException {
    setConf(mrwork);
    setChildren(hconf);
    initialize(hconf, null);
  }

  private static MapOpCtx initObjectInspector(mapredWork conf,
      Configuration hconf, String onefile) throws HiveException,
      ClassNotFoundException, InstantiationException, IllegalAccessException,
      SerDeException {
    partitionDesc pd = conf.getPathToPartitionInfo().get(onefile);
    LinkedHashMap<String, String> partSpec = pd.getPartSpec();
    tableDesc td = pd.getTableDesc();
    Properties tblProps = td.getProperties();

    Class sdclass = td.getDeserializerClass();
    if (sdclass == null) {
      String className = td.getSerdeClassName();
      if ((className == null) || className.isEmpty()) {
        throw new HiveException(
            "SerDe class or the SerDe class name is not set for table: "
                + td.getProperties().getProperty("name"));
      }
      sdclass = hconf.getClassByName(className);
    }

    String tableName = String.valueOf(tblProps.getProperty("name"));
    String partName = String.valueOf(partSpec);
    Deserializer deserializer = (Deserializer) sdclass.newInstance();
    deserializer.initialize(hconf, tblProps);
    StructObjectInspector rowObjectInspector = (StructObjectInspector) deserializer
        .getObjectInspector();

    MapOpCtx opCtx = null;
    String pcols = tblProps
        .getProperty(org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_PARTITION_COLUMNS);
    if (pcols != null && pcols.length() > 0) {
      String[] partKeys = pcols.trim().split("/");
      List<String> partNames = new ArrayList<String>(partKeys.length);
      Object[] partValues = new Object[partKeys.length];
      List<ObjectInspector> partObjectInspectors = new ArrayList<ObjectInspector>(
          partKeys.length);
      for (int i = 0; i < partKeys.length; i++) {
        String key = partKeys[i];
        partNames.add(key);
        partValues[i] = new Text(partSpec.get(key));
        partObjectInspectors
            .add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      }
      StructObjectInspector partObjectInspector = ObjectInspectorFactory
          .getStandardStructObjectInspector(partNames, partObjectInspectors);

      Object[] rowWithPart = new Object[2];
      rowWithPart[1] = partValues;
      rowObjectInspector = ObjectInspectorFactory
          .getUnionStructObjectInspector(Arrays
              .asList(new StructObjectInspector[] { rowObjectInspector,
                  partObjectInspector }));
      opCtx = new MapOpCtx(true, rowObjectInspector, rowWithPart, deserializer);
    } else {
      opCtx = new MapOpCtx(false, rowObjectInspector, null, deserializer);
    }
    opCtx.tableName = tableName;
    opCtx.partName = partName;
    return opCtx;
  }

  public void setChildren(Configuration hconf) throws HiveException {

    Path fpath = new Path((new Path(HiveConf.getVar(hconf,
        HiveConf.ConfVars.HADOOPMAPFILENAME))).toUri().getPath());
    ArrayList<Operator<? extends Serializable>> children = new ArrayList<Operator<? extends Serializable>>();
    opCtxMap = new HashMap<MapInputPath, MapOpCtx>();
    statsMap.put(Counter.DESERIALIZE_ERRORS, deserialize_error_count);
    statsMap.put(Counter.READ_SUCCESS_COUNT, read_success_count);
    statsMap.put(Counter.READ_ERROR_COUNT, read_error_count);

    try {
      boolean done = false;
      for (String onefile : conf.getPathToAliases().keySet()) {
        MapOpCtx opCtx = initObjectInspector(conf, hconf, onefile);
        Path onepath = new Path(new Path(onefile).toUri().getPath());
        List<String> aliases = conf.getPathToAliases().get(onefile);
        for (String onealias : aliases) {
          Operator<? extends Serializable> op = conf.getAliasToWork().get(
              onealias);
          LOG.info("Adding alias " + onealias + " to work list for file "
              + fpath.toUri().getPath());
          MapInputPath inp = new MapInputPath(onefile, onealias, op);
          opCtxMap.put(inp, opCtx);
          op.setParentOperators(new ArrayList<Operator<? extends Serializable>>());
          op.getParentOperators().add(this);
          if (!onepath.toUri().relativize(fpath.toUri()).equals(fpath.toUri())) {
            children.add(op);
            LOG.info("dump " + op.getName() + " "
                + opCtxMap.get(inp).getRowObjectInspector().getTypeName());
            if (!done) {
              deserializer = opCtxMap.get(inp).getDeserializer();
              isPartitioned = opCtxMap.get(inp).isPartitioned();
              rowWithPart = opCtxMap.get(inp).getRowWithPart();
              rowObjectInspector = opCtxMap.get(inp).getRowObjectInspector();
              done = true;
            }
          }
        }
      }
      if (children.size() == 0) {
        LOG.error("Configuration does not have any alias for path: "
            + fpath.toUri().getPath());
        throw new HiveException("Configuration and input path are inconsistent");
      }

      setChildOperators(children);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void initializeOp(Configuration hconf) throws HiveException {
    state = State.INIT;
    List<Operator<? extends Serializable>> children = getChildOperators();

    maxnum_rowdeleted_printlog = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.MAXNUM_ROWDELETED_PRINTLOG_PERTASK);
    Path fpath = new Path((new Path(HiveConf.getVar(hconf,
        HiveConf.ConfVars.HADOOPMAPFILENAME))).toUri().getPath());
    for (Entry<MapInputPath, MapOpCtx> entry : opCtxMap.entrySet()) {
      HiveConf.setVar(hconf, HiveConf.ConfVars.HIVETABLENAME,
          entry.getValue().tableName);
      HiveConf.setVar(hconf, HiveConf.ConfVars.HIVEPARTITIONNAME,
          entry.getValue().partName);
      Operator<? extends Serializable> op = entry.getKey().op;

      if (children.indexOf(op) == -1) {
        if (extraChildrenToClose == null) {
          extraChildrenToClose = new ArrayList<Operator<? extends Serializable>>();
        }
        extraChildrenToClose.add(op);
      }

      Path onepath = new Path(new Path(entry.getKey().path).toUri().getPath());
      if ((op instanceof UnionOperator)
          && (onepath.toUri().relativize(fpath.toUri()).equals(fpath.toUri()))) {
        continue;
      }
      op.initialize(hconf, new ObjectInspector[] { entry.getValue()
          .getRowObjectInspector() });

    }
  }

  public void closeOp(boolean abort) throws HiveException {
    if (extraChildrenToClose != null) {
      for (Operator<? extends Serializable> op : extraChildrenToClose) {
        op.close(abort);
      }
    }
  }

  public void process(Writable value) throws HiveException {
    try {

      Object row = deserializer.deserialize(value);
      if (row == null && deserializer instanceof LazySimpleSerDe) {
        read_error_count.set(read_error_count.get() + 1);
        if (maxnum_rowdeleted_printlog != -1
            && read_error_count.get() <= maxnum_rowdeleted_printlog)
          LOG.info("delete a row:\t" + value.toString());
        return;
      }

      if (!isPartitioned) {
        forward(row, rowObjectInspector);
      } else {
        rowWithPart[0] = row;
        forward(rowWithPart, rowObjectInspector);
      }
      read_success_count.set(read_success_count.get() + 1);
    } catch (SerDeException e) {
      deserialize_error_count.set(deserialize_error_count.get() + 1);
      String errmsg = e.getMessage();
      LOG.info(errmsg);
      if (errmsg.startsWith("__InvalidProtocolBufferException")) {
        read_error_count.set(read_error_count.get() + 1);
      } else {
        throw new HiveException(e);
      }
    }
  }

  public void process(Object row, int tag) throws HiveException {
    throw new HiveException("Hive 2 Internal error: should not be called!");
  }

  public String getName() {
    return "MAP";
  }
}
