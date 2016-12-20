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

package org.apache.hadoop.hive.ql.optimizer;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.io.Serializable;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MultiHdfsInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.tableDesc;

public class GenMRProcContext implements NodeProcessorCtx {

  public static class GenMapRedCtx {
    Task<? extends Serializable> currTask;
    Operator<? extends Serializable> currTopOp;
    String currAliasId;

    public GenMapRedCtx() {
    }

    public GenMapRedCtx(Task<? extends Serializable> currTask,
        Operator<? extends Serializable> currTopOp, String currAliasId) {
      this.currTask = currTask;
      this.currTopOp = currTopOp;
      this.currAliasId = currAliasId;
    }

    public Task<? extends Serializable> getCurrTask() {
      return currTask;
    }

    public Operator<? extends Serializable> getCurrTopOp() {
      return currTopOp;
    }

    public String getCurrAliasId() {
      return currAliasId;
    }
  }

  public static class GenMRUnionCtx {
    Task<? extends Serializable> uTask;
    List<String> taskTmpDir;
    List<tableDesc> tt_desc;

    public GenMRUnionCtx() {
      uTask = null;
      taskTmpDir = new ArrayList<String>();
      tt_desc = new ArrayList<tableDesc>();
    }

    public Task<? extends Serializable> getUTask() {
      return uTask;
    }

    public void setUTask(Task<? extends Serializable> uTask) {
      this.uTask = uTask;
    }

    public void addTaskTmpDir(String taskTmpDir) {
      this.taskTmpDir.add(taskTmpDir);
    }

    public List<String> getTaskTmpDir() {
      return taskTmpDir;
    }

    public void addTTDesc(tableDesc tt_desc) {
      this.tt_desc.add(tt_desc);
    }

    public List<tableDesc> getTTDesc() {
      return tt_desc;
    }
  }

  public static class GenMRMapJoinCtx {
    String taskTmpDir;
    tableDesc tt_desc;
    Operator<? extends Serializable> rootMapJoinOp;
    MapJoinOperator oldMapJoin;

    public GenMRMapJoinCtx() {
      taskTmpDir = null;
      tt_desc = null;
      rootMapJoinOp = null;
      oldMapJoin = null;
    }

    public GenMRMapJoinCtx(String taskTmpDir, tableDesc tt_desc,
        Operator<? extends Serializable> rootMapJoinOp,
        MapJoinOperator oldMapJoin) {
      this.taskTmpDir = taskTmpDir;
      this.tt_desc = tt_desc;
      this.rootMapJoinOp = rootMapJoinOp;
      this.oldMapJoin = oldMapJoin;
    }

    public void setTaskTmpDir(String taskTmpDir) {
      this.taskTmpDir = taskTmpDir;
    }

    public String getTaskTmpDir() {
      return taskTmpDir;
    }

    public void setTTDesc(tableDesc tt_desc) {
      this.tt_desc = tt_desc;
    }

    public tableDesc getTTDesc() {
      return tt_desc;
    }

    public Operator<? extends Serializable> getRootMapJoinOp() {
      return rootMapJoinOp;
    }

    public void setRootMapJoinOp(Operator<? extends Serializable> rootMapJoinOp) {
      this.rootMapJoinOp = rootMapJoinOp;
    }

    public MapJoinOperator getOldMapJoin() {
      return oldMapJoin;
    }

    public void setOldMapJoin(MapJoinOperator oldMapJoin) {
      this.oldMapJoin = oldMapJoin;
    }
  }

  private HiveConf conf;
  private HashMap<Operator<? extends Serializable>, Task<? extends Serializable>> opTaskMap;
  private HashMap<UnionOperator, GenMRUnionCtx> unionTaskMap;
  private HashMap<MapJoinOperator, GenMRMapJoinCtx> mapJoinTaskMap;
  private List<Operator<? extends Serializable>> seenOps;
  private List<Operator> seenFileSinkOps;

  private ParseContext parseCtx;
  private List<Task<? extends Serializable>> mvTask;
  private List<Task<? extends Serializable>> rootTasks;

  private LinkedHashMap<Operator<? extends Serializable>, GenMapRedCtx> mapCurrCtx;
  private Task<? extends Serializable> currTask;
  private Operator<? extends Serializable> currTopOp;
  private UnionOperator currUnionOp;
  private UnionOperator currMapUnionOp;
  private MapJoinOperator currMapJoinOp;
  private String currAliasId;
  private List<Operator<? extends Serializable>> rootOps;

  private MultiHdfsInfo multiHdfsInfo = null;

  public MultiHdfsInfo getMultiHdfsInfo() {
    return multiHdfsInfo;
  }

  public void setMultiHdfsInfo(MultiHdfsInfo multiHdfsInfo) {
    this.multiHdfsInfo = multiHdfsInfo;
  }

  private Set<ReadEntity> inputs;

  private Set<WriteEntity> outputs;

  public GenMRProcContext() {
  }

  public GenMRProcContext(
      HiveConf conf,
      HashMap<Operator<? extends Serializable>, Task<? extends Serializable>> opTaskMap,
      List<Operator<? extends Serializable>> seenOps, ParseContext parseCtx,
      List<Task<? extends Serializable>> mvTask,
      List<Task<? extends Serializable>> rootTasks,
      LinkedHashMap<Operator<? extends Serializable>, GenMapRedCtx> mapCurrCtx,
      Set<ReadEntity> inputs, Set<WriteEntity> outputs) {
    this.conf = conf;
    this.opTaskMap = opTaskMap;
    this.seenOps = seenOps;
    this.mvTask = mvTask;
    this.parseCtx = parseCtx;
    this.rootTasks = rootTasks;
    this.mapCurrCtx = mapCurrCtx;
    this.inputs = inputs;
    this.outputs = outputs;
    currTask = null;
    currTopOp = null;
    currUnionOp = null;
    currMapUnionOp = null;
    currMapJoinOp = null;
    currAliasId = null;
    rootOps = new ArrayList<Operator<? extends Serializable>>();
    rootOps.addAll(parseCtx.getTopOps().values());
    unionTaskMap = new HashMap<UnionOperator, GenMRUnionCtx>();
    mapJoinTaskMap = new HashMap<MapJoinOperator, GenMRMapJoinCtx>();
  }

  public UnionOperator getCurrMapUnionOp() {
    return currMapUnionOp;
  }

  public void setCurrMapUnionOp(UnionOperator currMapUnionOp) {
    this.currMapUnionOp = currMapUnionOp;
  }

  public HashMap<Operator<? extends Serializable>, Task<? extends Serializable>> getOpTaskMap() {
    return opTaskMap;
  }

  public void setOpTaskMap(
      HashMap<Operator<? extends Serializable>, Task<? extends Serializable>> opTaskMap) {
    this.opTaskMap = opTaskMap;
  }

  public List<Operator<? extends Serializable>> getSeenOps() {
    return seenOps;
  }

  public List<Operator> getSeenFileSinkOps() {
    return seenFileSinkOps;
  }

  public void setSeenOps(List<Operator<? extends Serializable>> seenOps) {
    this.seenOps = seenOps;
  }

  public void setSeenFileSinkOps(List<Operator> seenFileSinkOps) {
    this.seenFileSinkOps = seenFileSinkOps;
  }

  public List<Operator<? extends Serializable>> getRootOps() {
    return rootOps;
  }

  public void setRootOps(List<Operator<? extends Serializable>> rootOps) {
    this.rootOps = rootOps;
  }

  public ParseContext getParseCtx() {
    return parseCtx;
  }

  public void setParseCtx(ParseContext parseCtx) {
    this.parseCtx = parseCtx;
  }

  public List<Task<? extends Serializable>> getMvTask() {
    return mvTask;
  }

  public void setMvTask(List<Task<? extends Serializable>> mvTask) {
    this.mvTask = mvTask;
  }

  public List<Task<? extends Serializable>> getRootTasks() {
    return rootTasks;
  }

  public void setRootTasks(List<Task<? extends Serializable>> rootTasks) {
    this.rootTasks = rootTasks;
  }

  public LinkedHashMap<Operator<? extends Serializable>, GenMapRedCtx> getMapCurrCtx() {
    return mapCurrCtx;
  }

  public void setMapCurrCtx(
      LinkedHashMap<Operator<? extends Serializable>, GenMapRedCtx> mapCurrCtx) {
    this.mapCurrCtx = mapCurrCtx;
  }

  public Task<? extends Serializable> getCurrTask() {
    return currTask;
  }

  public void setCurrTask(Task<? extends Serializable> currTask) {
    this.currTask = currTask;
  }

  public Operator<? extends Serializable> getCurrTopOp() {
    return currTopOp;
  }

  public void setCurrTopOp(Operator<? extends Serializable> currTopOp) {
    this.currTopOp = currTopOp;
  }

  public UnionOperator getCurrUnionOp() {
    return currUnionOp;
  }

  public void setCurrUnionOp(UnionOperator currUnionOp) {
    this.currUnionOp = currUnionOp;
  }

  public MapJoinOperator getCurrMapJoinOp() {
    return currMapJoinOp;
  }

  public void setCurrMapJoinOp(MapJoinOperator currMapJoinOp) {
    this.currMapJoinOp = currMapJoinOp;
  }

  public String getCurrAliasId() {
    return currAliasId;
  }

  public void setCurrAliasId(String currAliasId) {
    this.currAliasId = currAliasId;
  }

  public GenMRUnionCtx getUnionTask(UnionOperator op) {
    return unionTaskMap.get(op);
  }

  public void setUnionTask(UnionOperator op, GenMRUnionCtx uTask) {
    unionTaskMap.put(op, uTask);
  }

  public GenMRMapJoinCtx getMapJoinCtx(MapJoinOperator op) {
    return mapJoinTaskMap.get(op);
  }

  public void setMapJoinCtx(MapJoinOperator op, GenMRMapJoinCtx mjCtx) {
    mapJoinTaskMap.put(op, mjCtx);
  }

  public Set<ReadEntity> getInputs() {
    return inputs;
  }

  public Set<WriteEntity> getOutputs() {
    return outputs;
  }

  public HiveConf getConf() {
    return conf;
  }

  public void setConf(HiveConf conf) {
    this.conf = conf;
  }
}
