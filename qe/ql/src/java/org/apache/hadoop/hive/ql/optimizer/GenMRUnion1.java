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
import java.util.Stack;
import java.io.Serializable;
import java.io.File;
import java.util.Map;

import org.apache.hadoop.hive.metastore.MultiHdfsInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.ql.plan.partitionDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.plan.fileSinkDesc;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMapRedCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMRUnionCtx;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMRMapJoinCtx;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcFactory;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcContext;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcContext.UnionParseContext;

public class GenMRUnion1 implements NodeProcessor {

  public GenMRUnion1() {
  }

  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx opProcCtx,
      Object... nodeOutputs) throws SemanticException {
    UnionOperator union = (UnionOperator) nd;
    GenMRProcContext ctx = (GenMRProcContext) opProcCtx;
    ParseContext parseCtx = ctx.getParseCtx();
    UnionProcContext uCtx = parseCtx.getUCtx();

    Map<Operator<? extends Serializable>, GenMapRedCtx> mapCurrCtx = ctx
        .getMapCurrCtx();

    if (uCtx.isMapOnlySubq()) {
      UnionParseContext uPrsCtx = uCtx.getUnionParseContext(union);
      if ((uPrsCtx != null) && (uPrsCtx.getMapJoinQuery())) {
        GenMapRedUtils.mergeMapJoinUnion(union, ctx,
            UnionProcFactory.getPositionParent(union, stack));
      } else
        mapCurrCtx.put((Operator<? extends Serializable>) nd, new GenMapRedCtx(
            ctx.getCurrTask(), ctx.getCurrTopOp(), ctx.getCurrAliasId()));
      ctx.setCurrMapUnionOp(union);
      return null;
    }

    ctx.setCurrUnionOp(union);

    UnionParseContext uPrsCtx = uCtx.getUnionParseContext(union);
    assert uPrsCtx != null;

    Task<? extends Serializable> currTask = ctx.getCurrTask();
    int pos = UnionProcFactory.getPositionParent(union, stack);

    if (uPrsCtx.getRootTask(pos) && (!ctx.getRootTasks().contains(currTask)))
      ctx.getRootTasks().add(currTask);

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
      MapJoinOperator mjOp = ctx.getCurrMapJoinOp();
      assert mjOp != null;
      GenMRMapJoinCtx mjCtx = ctx.getMapJoinCtx(mjOp);
      assert mjCtx != null;
      mapredWork plan = (mapredWork) currTask.getWork();

      String taskTmpDir = mjCtx.getTaskTmpDir();
      tableDesc tt_desc = mjCtx.getTTDesc();
      assert plan.getPathToAliases().get(taskTmpDir) == null;
      plan.getPathToAliases().put(taskTmpDir, new ArrayList<String>());
      plan.getPathToAliases().get(taskTmpDir).add(taskTmpDir);
      plan.getPathToPartitionInfo().put(taskTmpDir,
          new partitionDesc(tt_desc, null));
      plan.getAliasToWork().put(taskTmpDir, mjCtx.getRootMapJoinOp());
    }

    tableDesc tt_desc = PlanUtils.getIntermediateFileTableDesc(PlanUtils
        .getFieldSchemasFromRowSchema(parent.getSchema(), "temporarycol"));

    Context baseCtx = parseCtx.getContext();
    MultiHdfsInfo multiHdfsInfo = ctx.getMultiHdfsInfo();
    String taskTmpDir = null;
    if (!multiHdfsInfo.isMultiHdfsEnable()) {
      taskTmpDir = baseCtx.getMRTmpFileURI();
    } else {
      taskTmpDir = baseCtx.getExternalTmpFileURI(new Path(multiHdfsInfo
          .getTmpHdfsScheme()).toUri());
    }

    uCtxTask.addTaskTmpDir(taskTmpDir);
    uCtxTask.addTTDesc(tt_desc);

    Operator<? extends Serializable> fs_op = OperatorFactory.get(
        new fileSinkDesc(taskTmpDir, tt_desc, parseCtx.getConf().getBoolVar(
            HiveConf.ConfVars.COMPRESSINTERMEDIATE)), parent.getSchema());

    assert parent.getChildOperators().size() == 1;
    parent.getChildOperators().set(0, fs_op);

    List<Operator<? extends Serializable>> parentOpList = new ArrayList<Operator<? extends Serializable>>();
    parentOpList.add(parent);
    fs_op.setParentOperators(parentOpList);

    currTask.addDependentTask(uTask);

    if (uPrsCtx.getMapOnlySubq(pos) && uPrsCtx.getRootTask(pos))
      GenMapRedUtils.setTaskPlan(ctx.getCurrAliasId(), ctx.getCurrTopOp(),
          (mapredWork) currTask.getWork(), false, ctx);

    ctx.setCurrTask(uTask);
    ctx.setCurrAliasId(null);
    ctx.setCurrTopOp(null);

    mapCurrCtx.put((Operator<? extends Serializable>) nd,
        new GenMapRedCtx(ctx.getCurrTask(), null, null));

    return null;
  }
}
