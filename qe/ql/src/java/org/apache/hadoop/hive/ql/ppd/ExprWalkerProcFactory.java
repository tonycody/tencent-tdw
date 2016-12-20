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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeFieldDesc;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.plan.exprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.UDFOPAnd;
import org.apache.hadoop.hive.ql.udf.UDFType;

public class ExprWalkerProcFactory {

  public static class ColumnExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ExprWalkerInfo ctx = (ExprWalkerInfo) procCtx;
      exprNodeColumnDesc colref = (exprNodeColumnDesc) nd;
      RowResolver toRR = ctx.getToRR();
      Operator<? extends Serializable> op = ctx.getOp();
      String[] colAlias = toRR.reverseLookup(colref.getColumn());

      if (op.getColumnExprMap() != null) {
        exprNodeDesc exp = op.getColumnExprMap().get(colref.getColumn());
        if (exp == null) {
          ctx.setIsCandidate(colref, false);
          return false;
        }
        ctx.addConvertedNode(colref, exp);
        ctx.setIsCandidate(exp, true);
        ctx.addAlias(exp, colAlias[0]);
      } else {
        if (colAlias == null)
          assert false;
        ctx.addAlias(colref, colAlias[0]);
      }
      ctx.setIsCandidate(colref, true);
      return true;
    }

  }

  public static class FieldExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ExprWalkerInfo ctx = (ExprWalkerInfo) procCtx;
      String alias = null;
      exprNodeFieldDesc expr = (exprNodeFieldDesc) nd;

      boolean isCandidate = true;
      assert (nd.getChildren().size() == 1);
      exprNodeDesc ch = (exprNodeDesc) nd.getChildren().get(0);
      exprNodeDesc newCh = ctx.getConvertedNode(ch);
      if (newCh != null) {
        expr.setDesc(newCh);
        ch = newCh;
      }
      String chAlias = ctx.getAlias(ch);

      isCandidate = isCandidate && ctx.isCandidate(ch);
      if (isCandidate && chAlias != null) {
        if (alias == null) {
          alias = chAlias;
        } else if (!chAlias.equalsIgnoreCase(alias)) {
          isCandidate = false;
        }
      }

      ctx.addAlias(expr, alias);
      ctx.setIsCandidate(expr, isCandidate);
      return isCandidate;
    }

  }

  public static class GenericFuncExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ExprWalkerInfo ctx = (ExprWalkerInfo) procCtx;
      String alias = null;
      exprNodeGenericFuncDesc expr = (exprNodeGenericFuncDesc) nd;

      if (!FunctionRegistry.isDeterministic(expr.getGenericUDF())) {
        ctx.setIsCandidate(expr, false);
        ctx.setDeterministic(false);
        return false;
      }

      boolean isCandidate = true;
      for (int i = 0; i < nd.getChildren().size(); i++) {
        exprNodeDesc ch = (exprNodeDesc) nd.getChildren().get(i);
        exprNodeDesc newCh = ctx.getConvertedNode(ch);
        if (newCh != null) {
          expr.getChildExprs().set(i, newCh);
          ch = newCh;
        }
        String chAlias = ctx.getAlias(ch);

        isCandidate = isCandidate && ctx.isCandidate(ch);
        if (isCandidate && chAlias != null) {
          if (alias == null) {
            alias = chAlias;
          } else if (!chAlias.equalsIgnoreCase(alias)) {
            isCandidate = false;
          }
        }

        if (!isCandidate)
          break;
      }
      ctx.addAlias(expr, alias);
      ctx.setIsCandidate(expr, isCandidate);
      return isCandidate;
    }

  }

  public static class DefaultExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ExprWalkerInfo ctx = (ExprWalkerInfo) procCtx;
      ctx.setIsCandidate((exprNodeDesc) nd, true);
      return true;
    }
  }

  public static NodeProcessor getDefaultExprProcessor() {
    return new DefaultExprProcessor();
  }

  public static NodeProcessor getGenericFuncProcessor() {
    return new GenericFuncExprProcessor();
  }

  public static NodeProcessor getColumnProcessor() {
    return new ColumnExprProcessor();
  }

  private static NodeProcessor getFieldProcessor() {
    return new FieldExprProcessor();
  }

  public static ExprWalkerInfo extractPushdownPreds(OpWalkerInfo opContext,
      Operator<? extends Serializable> op, exprNodeDesc pred)
      throws SemanticException {
    List<exprNodeDesc> preds = new ArrayList<exprNodeDesc>();
    preds.add(pred);
    return extractPushdownPreds(opContext, op, preds);
  }

  public static ExprWalkerInfo extractPushdownPreds(OpWalkerInfo opContext,
      Operator<? extends Serializable> op, List<exprNodeDesc> preds)
      throws SemanticException {
    ExprWalkerInfo exprContext = new ExprWalkerInfo(op,
        opContext.getRowResolver(op));

    Map<Rule, NodeProcessor> exprRules = new LinkedHashMap<Rule, NodeProcessor>();
    exprRules.put(
        new RuleRegExp("R1", exprNodeColumnDesc.class.getName() + "%"),
        getColumnProcessor());
    exprRules.put(
        new RuleRegExp("R2", exprNodeFieldDesc.class.getName() + "%"),
        getFieldProcessor());
    exprRules.put(new RuleRegExp("R3", exprNodeGenericFuncDesc.class.getName()
        + "%"), getGenericFuncProcessor());

    Dispatcher disp = new DefaultRuleDispatcher(getDefaultExprProcessor(),
        exprRules, exprContext);
    GraphWalker egw = new DefaultGraphWalker(disp);

    List<Node> startNodes = new ArrayList<Node>();
    List<exprNodeDesc> clonedPreds = new ArrayList<exprNodeDesc>();
    for (exprNodeDesc node : preds) {
      clonedPreds.add((exprNodeDesc) node.clone());
    }
    startNodes.addAll(clonedPreds);

    egw.startWalking(startNodes, null);

    for (exprNodeDesc pred : clonedPreds) {
      extractFinalCandidates(pred, exprContext);
    }
    return exprContext;
  }

  private static void extractFinalCandidates(exprNodeDesc expr,
      ExprWalkerInfo ctx) {
    if (ctx.isCandidate(expr)) {
      ctx.addFinalCandidate(expr);
      return;
    }

    if (FunctionRegistry.isOpAnd(expr)) {
      for (Node ch : expr.getChildren()) {
        extractFinalCandidates((exprNodeDesc) ch, ctx);
      }
    }

  }
}
