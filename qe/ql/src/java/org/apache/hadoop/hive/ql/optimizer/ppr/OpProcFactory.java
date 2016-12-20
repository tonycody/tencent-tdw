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

package org.apache.hadoop.hive.ql.optimizer.ppr;

import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;

public class OpProcFactory {

  public static class FilterPPR implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      OpWalkerCtx owc = (OpWalkerCtx) procCtx;
      FilterOperator fop = (FilterOperator) nd;
      FilterOperator fop2 = null;

      Node tmp = stack.pop();
      Node tmp2 = stack.pop();
      TableScanOperator top = null;
      if (tmp2 instanceof TableScanOperator) {
        top = (TableScanOperator) tmp2;
      } else {
        top = (TableScanOperator) stack.peek();
        fop2 = (FilterOperator) tmp2;
      }
      stack.push(tmp2);
      stack.push(tmp);

      if (fop2 != null && !fop2.getConf().getIsSamplingPred())
        return null;

      if (fop.getConf().getIsSamplingPred()) {
        return null;
      }

      exprNodeDesc predicate = fop.getConf().getPredicate();
      String alias = top.getConf().getAlias();

      Boolean hasNonPartCols = false;
      exprNodeDesc ppr_pred = ExprProcFactory.genPruner(alias, predicate,
          hasNonPartCols);
      owc.addHasNonPartCols(hasNonPartCols);
      
      // if 'NOT(AND(part column,...))', do not partition prun
      if (!hasNonPartCols && predicate != null && predicate instanceof exprNodeGenericFuncDesc) {
        GenericUDF udfNot = ((exprNodeGenericFuncDesc)predicate).getGenericUDF();
        if (udfNot != null && udfNot instanceof GenericUDFOPNot) {
          List<exprNodeDesc> children = ((exprNodeGenericFuncDesc)predicate).getChildExprs();
          if (children != null && children.size() == 1) {
            exprNodeDesc expr = children.get(0);
            if (expr != null && expr instanceof exprNodeGenericFuncDesc) {
              GenericUDF udfAnd = ((exprNodeGenericFuncDesc)expr).getGenericUDF();
              if (udfAnd != null && udfAnd instanceof GenericUDFOPAnd) {
                return null;
              }
            }
          }
        }
      }

      addPruningPred(owc.getOpToPartPruner(), top, ppr_pred);

      return null;
    }

    private void addPruningPred(Map<TableScanOperator, exprNodeDesc> opToPPR,
        TableScanOperator top, exprNodeDesc new_ppr_pred) {
      exprNodeDesc old_ppr_pred = opToPPR.get(top);
      exprNodeDesc ppr_pred = null;
      if (old_ppr_pred != null) {
        ppr_pred = TypeCheckProcFactory.DefaultExprProcessor
            .getFuncExprNodeDesc("OR", old_ppr_pred, new_ppr_pred);
      } else {
        ppr_pred = new_ppr_pred;
      }

      opToPPR.put(top, ppr_pred);

      return;
    }
  }

  public static class DefaultPPR implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      return null;
    }
  }

  public static NodeProcessor getFilterProc() {
    return new FilterPPR();
  }

  public static NodeProcessor getDefaultProc() {
    return new DefaultPPR();
  }

}
