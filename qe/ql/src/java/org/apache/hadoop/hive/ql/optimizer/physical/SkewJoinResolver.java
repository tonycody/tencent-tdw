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

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.metastore.MultiHdfsInfo;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.mapredWork;

public class SkewJoinResolver implements PhysicalPlanResolver {
  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    Dispatcher disp = new SkewJoinTaskDispatcher(pctx);
    GraphWalker ogw = new DefaultGraphWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.rootTasks);
    ogw.startWalking(topNodes, null);
    return null;
  }

  class SkewJoinTaskDispatcher implements Dispatcher {

    private PhysicalContext physicalContext;

    public SkewJoinTaskDispatcher(PhysicalContext context) {
      super();
      this.physicalContext = context;
    }

    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs)
        throws SemanticException {
      Task<? extends Serializable> task = (Task<? extends Serializable>) nd;

      if (!task.isMapRedTask() || task instanceof ConditionalTask
          || ((mapredWork) task.getWork()).getReducer() == null)
        return null;

      SkewJoinProcCtx skewJoinProcContext = new SkewJoinProcCtx(task,
          this.physicalContext.getParseContext());
      skewJoinProcContext.setMultiHdfsInfo(this.physicalContext
          .getMultiHdfsInfo());

      Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
      opRules.put(new RuleRegExp("R1", "JOIN%"),
          SkewJoinProcFactory.getJoinProc());

      Dispatcher disp = new DefaultRuleDispatcher(
          SkewJoinProcFactory.getDefaultProc(), opRules, skewJoinProcContext);
      GraphWalker ogw = new DefaultGraphWalker(disp);

      ArrayList<Node> topNodes = new ArrayList<Node>();
      topNodes.add(((mapredWork) task.getWork()).getReducer());
      ogw.startWalking(topNodes, null);
      return null;
    }

    public PhysicalContext getPhysicalContext() {
      return physicalContext;
    }

    public void setPhysicalContext(PhysicalContext physicalContext) {
      this.physicalContext = physicalContext;
    }
  }

  public static class SkewJoinProcCtx implements NodeProcessorCtx {
    private Task<? extends Serializable> currentTask;
    private ParseContext parseCtx;

    private MultiHdfsInfo multiHdfsInfo = null;

    public MultiHdfsInfo getMultiHdfsInfo() {
      return multiHdfsInfo;
    }

    public void setMultiHdfsInfo(MultiHdfsInfo multiHdfsInfo) {
      this.multiHdfsInfo = multiHdfsInfo;
    }

    public SkewJoinProcCtx(Task<? extends Serializable> task,
        ParseContext parseCtx) {
      this.currentTask = task;
      this.parseCtx = parseCtx;
    }

    public Task<? extends Serializable> getCurrentTask() {
      return currentTask;
    }

    public void setCurrentTask(Task<? extends Serializable> currentTask) {
      this.currentTask = currentTask;
    }

    public ParseContext getParseCtx() {
      return parseCtx;
    }

    public void setParseCtx(ParseContext parseCtx) {
      this.parseCtx = parseCtx;
    }
  }
}
