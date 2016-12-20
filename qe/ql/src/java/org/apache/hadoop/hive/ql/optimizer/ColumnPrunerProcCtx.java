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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.selectDesc;

public class ColumnPrunerProcCtx implements NodeProcessorCtx {

  private Map<Operator<? extends Serializable>, List<String>> prunedColLists;

  private HashMap<Operator<? extends Serializable>, OpParseContext> opToParseCtxMap;

  private Map<CommonJoinOperator, Map<Byte, List<String>>> joinPrunedColLists;

  public ColumnPrunerProcCtx(
      HashMap<Operator<? extends Serializable>, OpParseContext> opToParseContextMap) {
    prunedColLists = new HashMap<Operator<? extends Serializable>, List<String>>();
    this.opToParseCtxMap = opToParseContextMap;
    joinPrunedColLists = new HashMap<CommonJoinOperator, Map<Byte, List<String>>>();
  }

  public Map<CommonJoinOperator, Map<Byte, List<String>>> getJoinPrunedColLists() {
    return joinPrunedColLists;
  }

  public List<String> getPrunedColList(Operator<? extends Serializable> op) {
    return prunedColLists.get(op);
  }

  public HashMap<Operator<? extends Serializable>, OpParseContext> getOpToParseCtxMap() {
    return opToParseCtxMap;
  }

  public Map<Operator<? extends Serializable>, List<String>> getPrunedColLists() {
    return prunedColLists;
  }

  public List<String> genColLists(Operator<? extends Serializable> curOp)
      throws SemanticException {
    List<String> colList = new ArrayList<String>();
    if (curOp.getChildOperators() != null) {
      for (Operator<? extends Serializable> child : curOp.getChildOperators()) {
        if (child instanceof CommonJoinOperator) {
          int tag = child.getParentOperators().indexOf(curOp);
          List<String> prunList = joinPrunedColLists.get(
              (CommonJoinOperator) child).get((byte) tag);
          colList = Utilities.mergeUniqElems(colList, prunList);
        } else {
          colList = Utilities
              .mergeUniqElems(colList, prunedColLists.get(child));
        }
      }
    }
    return colList;
  }

  public List<String> getColsFromSelectExpr(SelectOperator op) {
    List<String> cols = new ArrayList<String>();
    selectDesc conf = op.getConf();
    ArrayList<exprNodeDesc> exprList = conf.getColList();
    for (exprNodeDesc expr : exprList)
      cols = Utilities.mergeUniqElems(cols, expr.getCols());
    return cols;
  }

  public List<String> getSelectColsFromChildren(SelectOperator op,
      List<String> colList) {
    List<String> cols = new ArrayList<String>();
    selectDesc conf = op.getConf();

    if (conf.isSelStarNoCompute()) {
      cols.addAll(colList);
      return cols;
    }

    ArrayList<exprNodeDesc> selectExprs = conf.getColList();

    ArrayList<String> outputColumnNames = conf.getOutputColumnNames();
    for (int i = 0; i < outputColumnNames.size(); i++) {
      if (colList.contains(outputColumnNames.get(i))) {
        exprNodeDesc expr = selectExprs.get(i);
        cols = Utilities.mergeUniqElems(cols, expr.getCols());
      }
    }

    return cols;
  }
}
