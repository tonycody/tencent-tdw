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
package org.apache.hadoop.hive.ql.ppd;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;

public class ExprWalkerInfo implements NodeProcessorCtx {

  private static class ExprInfo {

    public boolean isCandidate = false;

    public String alias = null;

    public exprNodeDesc convertedExpr = null;

    public ExprInfo() {
    }

    public ExprInfo(boolean isCandidate, String alias, exprNodeDesc replacedNode) {
      this.isCandidate = isCandidate;
      this.alias = alias;
      this.convertedExpr = replacedNode;
    }
  }

  protected static final Log LOG = LogFactory.getLog(OpProcFactory.class
      .getName());;
  private Operator<? extends Serializable> op = null;
  private RowResolver toRR = null;

  private Map<String, List<exprNodeDesc>> pushdownPreds;

  private Map<exprNodeDesc, ExprInfo> exprInfoMap;
  private boolean isDeterministic = true;

  public ExprWalkerInfo() {
    this.pushdownPreds = new HashMap<String, List<exprNodeDesc>>();
    this.exprInfoMap = new HashMap<exprNodeDesc, ExprInfo>();
  }

  public ExprWalkerInfo(Operator<? extends Serializable> op,
      final RowResolver toRR) {
    this.op = op;
    this.toRR = toRR;

    this.pushdownPreds = new HashMap<String, List<exprNodeDesc>>();
    this.exprInfoMap = new HashMap<exprNodeDesc, ExprInfo>();
  }

  public Operator<? extends Serializable> getOp() {
    return op;
  }

  public RowResolver getToRR() {
    return toRR;
  }

  public exprNodeDesc getConvertedNode(Node nd) {
    ExprInfo ei = exprInfoMap.get(nd);
    if (ei == null)
      return null;
    return ei.convertedExpr;
  }

  public void addConvertedNode(exprNodeDesc oldNode, exprNodeDesc newNode) {
    ExprInfo ei = exprInfoMap.get(oldNode);
    if (ei == null) {
      ei = new ExprInfo();
      exprInfoMap.put(oldNode, ei);
    }
    ei.convertedExpr = newNode;
    exprInfoMap.put(newNode, new ExprInfo(ei.isCandidate, ei.alias, null));
  }

  public boolean isCandidate(exprNodeDesc expr) {
    ExprInfo ei = exprInfoMap.get(expr);
    if (ei == null)
      return false;
    return ei.isCandidate;
  }

  public void setIsCandidate(exprNodeDesc expr, boolean b) {
    ExprInfo ei = exprInfoMap.get(expr);
    if (ei == null) {
      ei = new ExprInfo();
      exprInfoMap.put(expr, ei);
    }
    ei.isCandidate = b;
  }

  public String getAlias(exprNodeDesc expr) {
    ExprInfo ei = exprInfoMap.get(expr);
    if (ei == null)
      return null;
    return ei.alias;
  }

  public void addAlias(exprNodeDesc expr, String alias) {
    if (alias == null)
      return;
    ExprInfo ei = exprInfoMap.get(expr);
    if (ei == null) {
      ei = new ExprInfo();
      exprInfoMap.put(expr, ei);
    }
    ei.alias = alias;
  }

  public void addFinalCandidate(exprNodeDesc expr) {
    String alias = this.getAlias(expr);
    if (pushdownPreds.get(alias) == null) {
      pushdownPreds.put(alias, new ArrayList<exprNodeDesc>());
    }
    pushdownPreds.get(alias).add((exprNodeDesc) expr.clone());
  }

  public Map<String, List<exprNodeDesc>> getFinalCandidates() {
    return pushdownPreds;
  }

  public void merge(ExprWalkerInfo ewi) {
    if (ewi == null)
      return;
    for (Entry<String, List<exprNodeDesc>> e : ewi.getFinalCandidates()
        .entrySet()) {
      List<exprNodeDesc> predList = pushdownPreds.get(e.getKey());
      if (predList != null) {
        predList.addAll(e.getValue());
      } else {
        pushdownPreds.put(e.getKey(), e.getValue());
      }
    }
  }

  public void setDeterministic(boolean b) {
    isDeterministic = b;
  }

  public boolean isDeterministic() {
    return isDeterministic;
  }
}
