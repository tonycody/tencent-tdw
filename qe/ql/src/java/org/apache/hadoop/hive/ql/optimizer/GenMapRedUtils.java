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

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.TreeSet;
import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hive.metastore.MultiHdfsInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.plan.fetchWork;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.plan.mapredLocalWork;
import org.apache.hadoop.hive.ql.plan.mapJoinDesc;
import org.apache.hadoop.hive.ql.plan.reduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.ql.plan.partitionDesc;
import org.apache.hadoop.hive.ql.plan.fileSinkDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.tableScanDesc;
import org.apache.hadoop.hive.ql.plan.mapredLocalWork.HashMapJoinContext;
import org.apache.hadoop.hive.ql.metadata.*;
import org.apache.hadoop.hive.ql.parse.*;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMapRedCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMRUnionCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMRMapJoinCtx;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcContext;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcContext.UnionParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;

public class GenMapRedUtils {
  private static Log LOG;

  static {
    LOG = LogFactory
        .getLog("org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils");
  }

  public static void initPlan(ReduceSinkOperator op, GenMRProcContext opProcCtx)
      throws SemanticException {
    Operator<? extends Serializable> reducer = op.getChildOperators().get(0);
    Map<Operator<? extends Serializable>, GenMapRedCtx> mapCurrCtx = opProcCtx
        .getMapCurrCtx();
    GenMapRedCtx mapredCtx = mapCurrCtx.get(op.getParentOperators().get(0));
    Task<? extends Serializable> currTask = mapredCtx.getCurrTask();
    mapredWork plan = (mapredWork) currTask.getWork();
    HashMap<Operator<? extends Serializable>, Task<? extends Serializable>> opTaskMap = opProcCtx
        .getOpTaskMap();
    Operator<? extends Serializable> currTopOp = opProcCtx.getCurrTopOp();

    opTaskMap.put(reducer, currTask);
    plan.setReducer(reducer);
    reduceSinkDesc desc = (reduceSinkDesc) op.getConf();

    plan.setNumReduceTasks(desc.getNumReducers());

    List<Task<? extends Serializable>> rootTasks = opProcCtx.getRootTasks();

    rootTasks.add(currTask);
    if (reducer.getClass() == JoinOperator.class)
      plan.setNeedsTagging(true);

    assert currTopOp != null;
    List<Operator<? extends Serializable>> seenOps = opProcCtx.getSeenOps();
    String currAliasId = opProcCtx.getCurrAliasId();

    seenOps.add(currTopOp);
    setTaskPlan(currAliasId, currTopOp, plan, false, opProcCtx);

    currTopOp = null;
    currAliasId = null;

    opProcCtx.setCurrTask(currTask);
    opProcCtx.setCurrTopOp(currTopOp);
    opProcCtx.setCurrAliasId(currAliasId);
  }

  public static void initMapJoinPlan(Operator<? extends Serializable> op,
      GenMRProcContext opProcCtx, boolean readInputMapJoin,
      boolean readInputUnion, boolean setReducer, int pos)
      throws SemanticException {
    Map<Operator<? extends Serializable>, GenMapRedCtx> mapCurrCtx = opProcCtx
        .getMapCurrCtx();
    assert (((pos == -1) && (readInputMapJoin)) || (pos != -1));
    int parentPos = (pos == -1) ? 0 : pos;
    GenMapRedCtx mapredCtx = mapCurrCtx.get(op.getParentOperators().get(
        parentPos));
    Task<? extends Serializable> currTask = mapredCtx.getCurrTask();
    mapredWork plan = (mapredWork) currTask.getWork();
    HashMap<Operator<? extends Serializable>, Task<? extends Serializable>> opTaskMap = opProcCtx
        .getOpTaskMap();
    Operator<? extends Serializable> currTopOp = opProcCtx.getCurrTopOp();

    if (readInputMapJoin) {
      MapJoinOperator currMapJoinOp = opProcCtx.getCurrMapJoinOp();
      assert currMapJoinOp != null;
      boolean local = ((pos == -1) || (pos == ((mapJoinDesc) currMapJoinOp
          .getConf()).getPosBigTable())) ? false : true;

      if (setReducer) {
        Operator<? extends Serializable> reducer = op.getChildOperators()
            .get(0);
        plan.setReducer(reducer);
        opTaskMap.put(reducer, currTask);
        if (reducer.getClass() == JoinOperator.class)
          plan.setNeedsTagging(true);
        reduceSinkDesc desc = (reduceSinkDesc) op.getConf();
        plan.setNumReduceTasks(desc.getNumReducers());
      } else
        opTaskMap.put(op, currTask);

      if (!readInputUnion) {
        GenMRMapJoinCtx mjCtx = opProcCtx.getMapJoinCtx(currMapJoinOp);
        String taskTmpDir;
        tableDesc tt_desc;
        Operator<? extends Serializable> rootOp;

        if (mjCtx.getOldMapJoin() == null) {
          taskTmpDir = mjCtx.getTaskTmpDir();
          tt_desc = mjCtx.getTTDesc();
          rootOp = mjCtx.getRootMapJoinOp();
        } else {
          GenMRMapJoinCtx oldMjCtx = opProcCtx.getMapJoinCtx(mjCtx
              .getOldMapJoin());
          taskTmpDir = oldMjCtx.getTaskTmpDir();
          tt_desc = oldMjCtx.getTTDesc();
          rootOp = oldMjCtx.getRootMapJoinOp();
        }

        setTaskPlan(taskTmpDir, taskTmpDir, rootOp, plan, local, tt_desc);
        setupHashMapJoinInfo(plan, currMapJoinOp);
      } else {
        initUnionPlan(opProcCtx, currTask, false);
      }

      opProcCtx.setCurrMapJoinOp(null);
    } else {
      mapJoinDesc desc = (mapJoinDesc) op.getConf();

      opTaskMap.put(op, currTask);

      List<Task<? extends Serializable>> rootTasks = opProcCtx.getRootTasks();
      rootTasks.add(currTask);

      assert currTopOp != null;
      List<Operator<? extends Serializable>> seenOps = opProcCtx.getSeenOps();
      String currAliasId = opProcCtx.getCurrAliasId();

      seenOps.add(currTopOp);
      boolean local = (pos == desc.getPosBigTable()) ? false : true;
      setTaskPlan(currAliasId, currTopOp, plan, local, opProcCtx);
      setupHashMapJoinInfo(plan, (MapJoinOperator) op);
    }

    opProcCtx.setCurrTask(currTask);
    opProcCtx.setCurrTopOp(null);
    opProcCtx.setCurrAliasId(null);
  }

  private static void setupHashMapJoinInfo(mapredWork plan,
      MapJoinOperator currMapJoinOp) {
    mapredLocalWork localPlan = plan.getMapLocalWork();
    if (localPlan != null && currMapJoinOp != null) {
      LinkedHashMap<String, LinkedHashMap<String, ArrayList<String>>> aliasHashPathNameMapping = currMapJoinOp
          .getConf().getAliasHashPathNameMapping();
      if (aliasHashPathNameMapping != null) {
        HashMapJoinContext hashMJCxt = new HashMapJoinContext();
        localPlan.setHashMapjoinContext(hashMJCxt);
        hashMJCxt.setAliasHashParMapping(aliasHashPathNameMapping);
        hashMJCxt.setMapJoinBigTableAlias(currMapJoinOp.getConf()
            .getBigTableAlias());
      }
    }
  }

  public static void initUnionPlan(ReduceSinkOperator op,
      GenMRProcContext opProcCtx) throws SemanticException {
    Operator<? extends Serializable> reducer = op.getChildOperators().get(0);
    Map<Operator<? extends Serializable>, GenMapRedCtx> mapCurrCtx = opProcCtx
        .getMapCurrCtx();
    GenMapRedCtx mapredCtx = mapCurrCtx.get(op.getParentOperators().get(0));
    Task<? extends Serializable> currTask = mapredCtx.getCurrTask();
    mapredWork plan = (mapredWork) currTask.getWork();
    HashMap<Operator<? extends Serializable>, Task<? extends Serializable>> opTaskMap = opProcCtx
        .getOpTaskMap();

    opTaskMap.put(reducer, currTask);
    plan.setReducer(reducer);
    reduceSinkDesc desc = (reduceSinkDesc) op.getConf();

    plan.setNumReduceTasks(desc.getNumReducers());

    if (reducer.getClass() == JoinOperator.class)
      plan.setNeedsTagging(true);

    ParseContext parseCtx = opProcCtx.getParseCtx();
    UnionProcContext uCtx = parseCtx.getUCtx();
    UnionParseContext upc = uCtx.getUnionParseContext(opProcCtx
        .getCurrMapUnionOp());

    if (upc == null)
      initUnionPlan(opProcCtx, currTask, false);
    else {
      if (!upc.isIssetTaskPlan())
        initUnionPlan(opProcCtx, currTask, false);
    }

  }

  public static void initUnionPlan(GenMRProcContext opProcCtx,
      Task<? extends Serializable> currTask, boolean local) {
    mapredWork plan = (mapredWork) currTask.getWork();
    UnionOperator currUnionOp = opProcCtx.getCurrUnionOp();
    assert currUnionOp != null;
    GenMRUnionCtx uCtx = opProcCtx.getUnionTask(currUnionOp);
    assert uCtx != null;

    List<String> taskTmpDirLst = uCtx.getTaskTmpDir();
    List<tableDesc> tt_descLst = uCtx.getTTDesc();
    assert !taskTmpDirLst.isEmpty() && !tt_descLst.isEmpty();
    assert taskTmpDirLst.size() == tt_descLst.size();
    int size = taskTmpDirLst.size();
    assert local == false;

    for (int pos = 0; pos < size; pos++) {
      String taskTmpDir = taskTmpDirLst.get(pos);

      tableDesc tt_desc = tt_descLst.get(pos);
      if (plan.getPathToAliases().get(taskTmpDir) == null) {
        plan.getPathToAliases().put(taskTmpDir, new ArrayList<String>());
        plan.getPathToAliases().get(taskTmpDir).add(taskTmpDir);
        plan.getPathToPartitionInfo().put(taskTmpDir,
            new partitionDesc(tt_desc, null));
        plan.getAliasToWork().put(taskTmpDir, currUnionOp);
      }
    }
  }

  public static void joinPlan(Operator<? extends Serializable> op,
      Task<? extends Serializable> oldTask, Task<? extends Serializable> task,
      GenMRProcContext opProcCtx, int pos, boolean split,
      boolean readMapJoinData, boolean readUnionData) throws SemanticException {
    Task<? extends Serializable> currTask = task;
    mapredWork plan = (mapredWork) currTask.getWork();
    Operator<? extends Serializable> currTopOp = opProcCtx.getCurrTopOp();
    List<Task<? extends Serializable>> parTasks = null;

    if (split) {
      assert oldTask != null;
      splitTasks((ReduceSinkOperator) op, oldTask, currTask, opProcCtx, true,
          false, 0);
    } else {
      if ((oldTask != null) && (oldTask.getParentTasks() != null)
          && !oldTask.getParentTasks().isEmpty()) {
        parTasks = new ArrayList<Task<? extends Serializable>>();
        parTasks.addAll(oldTask.getParentTasks());

        Object[] parTaskArr = parTasks.toArray();
        for (int i = 0; i < parTaskArr.length; i++)
          ((Task<? extends Serializable>) parTaskArr[i])
              .removeDependentTask(oldTask);
      }
    }

    if (currTopOp != null) {
      List<Operator<? extends Serializable>> seenOps = opProcCtx.getSeenOps();
      String currAliasId = opProcCtx.getCurrAliasId();

      if (!seenOps.contains(currTopOp)) {
        seenOps.add(currTopOp);
        boolean local = false;
        if (pos != -1)
          local = (pos == ((mapJoinDesc) op.getConf()).getPosBigTable()) ? false
              : true;
        setTaskPlan(currAliasId, currTopOp, plan, local, opProcCtx);
      }
      currTopOp = null;
      opProcCtx.setCurrTopOp(currTopOp);
    } else if (opProcCtx.getCurrMapJoinOp() != null) {
      MapJoinOperator mjOp = opProcCtx.getCurrMapJoinOp();
      if (readUnionData) {
        initUnionPlan(opProcCtx, currTask, false);
      } else {
        GenMRMapJoinCtx mjCtx = opProcCtx.getMapJoinCtx(mjOp);

        MapJoinOperator oldMapJoin = mjCtx.getOldMapJoin();
        String taskTmpDir = null;
        tableDesc tt_desc = null;
        Operator<? extends Serializable> rootOp = null;

        if (oldMapJoin == null) {
          taskTmpDir = mjCtx.getTaskTmpDir();
          tt_desc = mjCtx.getTTDesc();
          rootOp = mjCtx.getRootMapJoinOp();
        } else {
          GenMRMapJoinCtx oldMjCtx = opProcCtx.getMapJoinCtx(oldMapJoin);
          assert oldMjCtx != null;
          taskTmpDir = oldMjCtx.getTaskTmpDir();
          tt_desc = oldMjCtx.getTTDesc();
          rootOp = oldMjCtx.getRootMapJoinOp();
        }

        boolean local = ((pos == -1) || (pos == ((mapJoinDesc) mjOp.getConf())
            .getPosBigTable())) ? false : true;
        setTaskPlan(taskTmpDir, taskTmpDir, rootOp, plan, local, tt_desc);
        setupHashMapJoinInfo(plan, oldMapJoin);
      }
      opProcCtx.setCurrMapJoinOp(null);

      if ((oldTask != null) && (parTasks != null)) {
        for (Task<? extends Serializable> parTask : parTasks)
          parTask.addDependentTask(currTask);
      }
      
      if (opProcCtx.getRootTasks().contains(currTask))
        opProcCtx.getRootTasks().remove(currTask);
    }
    
    opProcCtx.setCurrTask(currTask);
  }

  public static void splitPlan(ReduceSinkOperator op, GenMRProcContext opProcCtx)
      throws SemanticException {
    mapredWork cplan = getMapRedWork();
    ParseContext parseCtx = opProcCtx.getParseCtx();
    Task<? extends Serializable> redTask = TaskFactory.get(cplan,
        parseCtx.getConf());
    Operator<? extends Serializable> reducer = op.getChildOperators().get(0);

    cplan.distinctNum = op.getConf().getDistinctColumnIndices() != null ? op
        .getConf().getDistinctColumnIndices().size() : 0;
    cplan.reduceFactor = op.getConf().getReduceNumFactor();

    cplan.setReducer(reducer);
    reduceSinkDesc desc = (reduceSinkDesc) op.getConf();

    cplan.setNumReduceTasks(new Integer(desc.getNumReducers()));

    HashMap<Operator<? extends Serializable>, Task<? extends Serializable>> opTaskMap = opProcCtx
        .getOpTaskMap();
    opTaskMap.put(reducer, redTask);
    Task<? extends Serializable> currTask = opProcCtx.getCurrTask();

    splitTasks(op, currTask, redTask, opProcCtx, true, false, 0);
    opProcCtx.getRootOps().add(op);
  }

  public static void setTaskPlan(String alias_id,
      Operator<? extends Serializable> topOp, mapredWork plan, boolean local,
      GenMRProcContext opProcCtx) throws SemanticException {

    ParseContext parseCtx = opProcCtx.getParseCtx();
    Set<ReadEntity> inputs = opProcCtx.getInputs();

    ArrayList<Path> partDir = new ArrayList<Path>();
    ArrayList<partitionDesc> partDesc = new ArrayList<partitionDesc>();

    Path tblDir = null;
    tableDesc tblDesc = null;

    PrunedPartitionList partsList = null;
    try {
      if (!opProcCtx.getConf().getBoolVar(HiveConf.ConfVars.HIVEOPTPPD)
          || !opProcCtx.getConf().getBoolVar(HiveConf.ConfVars.HIVEOPTPPR)) {
        partsList = parseCtx.getAliasToPruner().get(alias_id).prune();
      } else {
        partsList = org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner
            .prune(parseCtx.getTopToTable().get(topOp), parseCtx
                .getOpToPartPruner().get(topOp), opProcCtx.getConf(), alias_id);
      }
    } catch (SemanticException e) {
      throw e;
    } catch (HiveException e) {
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
      throw new SemanticException(e.getMessage(), e);
    }

    Set<String> parts = null;
    List<String> partPathList = new ArrayList<String>();
    TablePartition table = parseCtx.getTopToTable().get(topOp);

    partitionDesc aliasPartnDesc = new partitionDesc(
        Utilities.getTableDesc(table.getTbl()), null);
    plan.getAliasToPartnInfo().put(alias_id, aliasPartnDesc);

    parts = partsList.getTargetPartnPaths();
    if (!table.isPartitioned()) {
      tblDir = table.getPath();
    } else {
      Set<String> partsFromTablePartition = new TreeSet<String>();
      for (Path p : table.getPaths()) {
        partsFromTablePartition.add(p.toString());
      }
      parts.retainAll(partsFromTablePartition);
      partPathList.addAll(parts);
    }

    tblDesc = Utilities.getTableDesc(table.getTbl());

    for (String s : parts) {
      partDir.add(new Path(s));
    }

    if (!local) {

      partitionDesc pd = new partitionDesc(tblDesc, null);
      for (Path path : partDir) {
        if (plan.getPathToAliases().get(path.toString()) == null) {
          plan.getPathToAliases().put(path.toString(), new ArrayList<String>());
        }
        plan.getPathToAliases().get(path.toString()).add(alias_id);
        plan.getPathToPartitionInfo().put(path.toString(), pd);
        LOG.info("Information added for path " + path.toString());
      }

      assert plan.getAliasToWork().get(alias_id) == null;
      plan.getAliasToWork().put(alias_id, topOp);
    } else {
      mapredLocalWork localPlan = plan.getMapLocalWork();
      if (localPlan == null)
        localPlan = new mapredLocalWork(
            new LinkedHashMap<String, Operator<? extends Serializable>>(),
            new LinkedHashMap<String, fetchWork>());

      assert localPlan.getAliasToWork().get(alias_id) == null;
      assert localPlan.getAliasToFetchWork().get(alias_id) == null;
      localPlan.getAliasToWork().put(alias_id, topOp);
      if (tblDir == null)
        localPlan.getAliasToFetchWork().put(alias_id,
            new fetchWork(partPathList, tblDesc));
      else
        localPlan.getAliasToFetchWork().put(alias_id,
            new fetchWork(tblDir.toString(), tblDesc));
      plan.setMapLocalWork(localPlan);
    }
  }

  public static void setTaskPlan(String path, String alias,
      Operator<? extends Serializable> topOp, mapredWork plan, boolean local,
      tableDesc tt_desc) throws SemanticException {

    if (!local) {
      if (plan.getPathToAliases().get(path) == null)
        plan.getPathToAliases().put(path, new ArrayList<String>());
      plan.getPathToAliases().get(path).add(alias);
      plan.getPathToPartitionInfo().put(path, new partitionDesc(tt_desc, null));
      plan.getAliasToWork().put(alias, topOp);
    } else {
      mapredLocalWork localPlan = plan.getMapLocalWork();
      if (localPlan == null)
        localPlan = new mapredLocalWork(
            new LinkedHashMap<String, Operator<? extends Serializable>>(),
            new LinkedHashMap<String, fetchWork>());

      assert localPlan.getAliasToWork().get(alias) == null;
      assert localPlan.getAliasToFetchWork().get(alias) == null;
      localPlan.getAliasToWork().put(alias, topOp);
      localPlan.getAliasToFetchWork().put(alias, new fetchWork(alias, tt_desc));
      plan.setMapLocalWork(localPlan);
    }
  }

  public static void setKeyAndValueDesc(mapredWork plan,
      Operator<? extends Serializable> topOp) {
    if (topOp == null)
      return;

    if (topOp instanceof ReduceSinkOperator) {
      ReduceSinkOperator rs = (ReduceSinkOperator) topOp;
      plan.setKeyDesc(rs.getConf().getKeySerializeInfo());
      int tag = Math.max(0, rs.getConf().getTag());
      List<tableDesc> tagToSchema = plan.getTagToValueDesc();
      while (tag + 1 > tagToSchema.size()) {
        tagToSchema.add(null);
      }
      tagToSchema.set(tag, rs.getConf().getValueSerializeInfo());
    } else {
      List<Operator<? extends Serializable>> children = topOp
          .getChildOperators();
      if (children != null) {
        for (Operator<? extends Serializable> op : children) {
          setKeyAndValueDesc(plan, op);
        }
      }
    }
  }

  public static mapredWork getMapRedWork() {
    mapredWork work = new mapredWork();
    work.setPathToAliases(new LinkedHashMap<String, ArrayList<String>>());
    work.setPathToPartitionInfo(new LinkedHashMap<String, partitionDesc>());
    work.setAliasToWork(new LinkedHashMap<String, Operator<? extends Serializable>>());
    work.setTagToValueDesc(new ArrayList<tableDesc>());
    work.setReducer(null);
    return work;
  }

  @SuppressWarnings("nls")
  private static Operator<? extends Serializable> putOpInsertMap(
      Operator<? extends Serializable> op, RowResolver rr, ParseContext parseCtx) {
    OpParseContext ctx = new OpParseContext(rr);
    parseCtx.getOpParseCtx().put(op, ctx);
    return op;
  }

  @SuppressWarnings("nls")
  public static void splitTasks(Operator<? extends Serializable> op,
      Task<? extends Serializable> parentTask,
      Task<? extends Serializable> childTask, GenMRProcContext opProcCtx,
      boolean setReducer, boolean local, int posn) throws SemanticException {
    mapredWork plan = (mapredWork) childTask.getWork();
    Operator<? extends Serializable> currTopOp = opProcCtx.getCurrTopOp();

    ParseContext parseCtx = opProcCtx.getParseCtx();
    parentTask.addDependentTask(childTask);

    List<Task<? extends Serializable>> rootTasks = opProcCtx.getRootTasks();
    if (rootTasks.contains(childTask))
      rootTasks.remove(childTask);

    Context baseCtx = parseCtx.getContext();
    MultiHdfsInfo multiHdfsInfo = opProcCtx.getMultiHdfsInfo();
    String taskTmpDir = null;
    if (!multiHdfsInfo.isMultiHdfsEnable()) {
      taskTmpDir = baseCtx.getMRTmpFileURI();
    } else {
      taskTmpDir = baseCtx.getExternalTmpFileURI(new Path(multiHdfsInfo
          .getTmpHdfsScheme()).toUri());
    }

    Operator<? extends Serializable> parent = op.getParentOperators().get(posn);
    tableDesc tt_desc = PlanUtils.getIntermediateFileTableDesc(PlanUtils
        .getFieldSchemasFromRowSchema(parent.getSchema(), "temporarycol"));

    Operator<? extends Serializable> fs_op = putOpInsertMap(
        OperatorFactory.get(new fileSinkDesc(taskTmpDir, tt_desc, parseCtx
            .getConf().getBoolVar(HiveConf.ConfVars.COMPRESSINTERMEDIATE)),
            parent.getSchema()), null, parseCtx);

    List<Operator<? extends Serializable>> childOpList = parent
        .getChildOperators();
    for (int pos = 0; pos < childOpList.size(); pos++) {
      if (childOpList.get(pos) == op) {
        childOpList.set(pos, fs_op);
        break;
      }
    }

    List<Operator<? extends Serializable>> parentOpList = new ArrayList<Operator<? extends Serializable>>();
    parentOpList.add(parent);
    fs_op.setParentOperators(parentOpList);

    Operator<? extends Serializable> ts_op = putOpInsertMap(
        OperatorFactory.get(tableScanDesc.class, parent.getSchema()), null,
        parseCtx);

    childOpList = new ArrayList<Operator<? extends Serializable>>();
    childOpList.add(op);
    ts_op.setChildOperators(childOpList);
    op.getParentOperators().set(posn, ts_op);

    Map<Operator<? extends Serializable>, GenMapRedCtx> mapCurrCtx = opProcCtx
        .getMapCurrCtx();
    mapCurrCtx.put(ts_op, new GenMapRedCtx(childTask, null, null));

    String streamDesc = taskTmpDir;
    mapredWork cplan = (mapredWork) childTask.getWork();

    if (setReducer) {
      Operator<? extends Serializable> reducer = op.getChildOperators().get(0);

      if (reducer.getClass() == JoinOperator.class) {
        String origStreamDesc;
        streamDesc = "$INTNAME";
        origStreamDesc = streamDesc;
        int pos = 0;
        while (cplan.getAliasToWork().get(streamDesc) != null)
          streamDesc = origStreamDesc.concat(String.valueOf(++pos));
      }

      if (reducer.getClass() == JoinOperator.class)
        cplan.setNeedsTagging(true);
    }

    setTaskPlan(taskTmpDir, streamDesc, ts_op, cplan, local, tt_desc);

    if (op instanceof ReduceSinkOperator) {
      ReduceSinkOperator op1 = (ReduceSinkOperator) op;
      cplan.distinctNum = op1.getConf().getDistinctColumnIndices() != null ? op1
          .getConf().getDistinctColumnIndices().size()
          : 0;
      cplan.reduceFactor = op1.getConf().getReduceNumFactor();
    }

    if (op instanceof MapJoinOperator) {
      MapJoinOperator mjOp = (MapJoinOperator) op;
      opProcCtx.setCurrMapJoinOp(mjOp);
      GenMRMapJoinCtx mjCtx = opProcCtx.getMapJoinCtx(mjOp);
      if (mjCtx == null)
        mjCtx = new GenMRMapJoinCtx(taskTmpDir, tt_desc, ts_op, null);
      else {
        mjCtx.setTaskTmpDir(taskTmpDir);
        mjCtx.setTTDesc(tt_desc);
        mjCtx.setRootMapJoinOp(ts_op);
      }
      opProcCtx.setMapJoinCtx(mjOp, mjCtx);
      opProcCtx.getMapCurrCtx().put(parent,
          new GenMapRedCtx(childTask, null, null));
      setupHashMapJoinInfo(plan, mjOp);
    }

    currTopOp = null;
    String currAliasId = null;

    opProcCtx.setCurrTopOp(currTopOp);
    opProcCtx.setCurrAliasId(currAliasId);
    opProcCtx.setCurrTask(childTask);
  }

  static public void mergeMapJoinUnion(UnionOperator union,
      GenMRProcContext ctx, int pos) throws SemanticException {
    ParseContext parseCtx = ctx.getParseCtx();
    UnionProcContext uCtx = parseCtx.getUCtx();

    UnionParseContext uPrsCtx = uCtx.getUnionParseContext(union);
    assert uPrsCtx != null;

    Task<? extends Serializable> currTask = ctx.getCurrTask();

    GenMRUnionCtx uCtxTask = ctx.getUnionTask(union);
    Task<? extends Serializable> uTask = null;

    Operator<? extends Serializable> parent = union.getParentOperators().get(
        pos);
    mapredWork uPlan = null;

    if (uCtxTask == null) {
      uCtxTask = new GenMRUnionCtx();
      uPlan = GenMapRedUtils.getMapRedWork();
      uTask = TaskFactory.get(uPlan, parseCtx.getConf());
      uCtxTask.setUTask(uTask);
      ctx.setUnionTask(union, uCtxTask);
    } else {
      uTask = uCtxTask.getUTask();
      uPlan = (mapredWork) uTask.getWork();
    }

    if (uPrsCtx.getMapJoinSubq(pos)) {
      GenMRMapJoinCtx mjCtx = ctx.getMapJoinCtx(ctx.getCurrMapJoinOp());
      String taskTmpDir = mjCtx.getTaskTmpDir();
      if (uPlan.getPathToAliases().get(taskTmpDir) == null) {
        uPlan.getPathToAliases().put(taskTmpDir, new ArrayList<String>());
        uPlan.getPathToAliases().get(taskTmpDir).add(taskTmpDir);
        uPlan.getPathToPartitionInfo().put(taskTmpDir,
            new partitionDesc(mjCtx.getTTDesc(), null));
        uPlan.getAliasToWork().put(taskTmpDir, mjCtx.getRootMapJoinOp());
      }

      for (Task t : currTask.getParentTasks())
        t.addDependentTask(uTask);
      try {
        boolean notDone = true;
        while (notDone) {
          for (Task t : currTask.getParentTasks())
            t.removeDependentTask(currTask);
          notDone = false;
        }
      } catch (java.util.ConcurrentModificationException e) {
      }

    } else
      setTaskPlan(ctx.getCurrAliasId(), ctx.getCurrTopOp(), uPlan, false, ctx);

    uPrsCtx.setIssetTaskPlan(true);

    ctx.setCurrTask(uTask);
    ctx.setCurrAliasId(null);
    ctx.setCurrTopOp(null);
    ctx.setCurrMapJoinOp(null);

    ctx.getMapCurrCtx().put((Operator<? extends Serializable>) union,
        new GenMapRedCtx(ctx.getCurrTask(), null, null));
  }
}
