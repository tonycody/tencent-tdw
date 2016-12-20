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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.AnalysisOperator;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewForwardOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.PartitionerOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowExtendOperator;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UDTFOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewJoinOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.aggregationDesc;
import org.apache.hadoop.hive.ql.plan.analysisDesc;
import org.apache.hadoop.hive.ql.plan.analysisEvaluatorDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.groupByDesc;
import org.apache.hadoop.hive.ql.plan.joinDesc;
import org.apache.hadoop.hive.ql.plan.mapJoinDesc;
import org.apache.hadoop.hive.ql.plan.reduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.rowExtendDesc;
import org.apache.hadoop.hive.ql.plan.selectDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;

public class ColumnPrunerProcFactory {
  private static final Log LOG = LogFactory.getLog(ColumnPrunerProcFactory.class);
  public static class ColumnPrunerFilterProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      FilterOperator op = (FilterOperator) nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      exprNodeDesc condn = op.getConf().getPredicate();
      List<String> cl = condn.getCols();
      cppCtx.getPrunedColLists().put(op,
          Utilities.mergeUniqElems(cppCtx.genColLists(op), cl));

      return null;
    }
  }

  public static ColumnPrunerFilterProc getFilterProc() {
    return new ColumnPrunerFilterProc();
  }

  public static class ColumnPrunerGroupByProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      GroupByOperator op = (GroupByOperator) nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      List<String> colLists = new ArrayList<String>();
      groupByDesc conf = op.getConf();
      ArrayList<exprNodeDesc> keys = conf.getKeys();
      for (exprNodeDesc key : keys)
        colLists = Utilities.mergeUniqElems(colLists, key.getCols());

      if (conf.getTag2AggrParamExpr() != null) {
        for (ArrayList<exprNodeDesc> tagedExprs : conf.getTag2AggrParamExpr()) {
          for (exprNodeDesc expr : tagedExprs) {
            colLists = Utilities.mergeUniqElems(colLists, expr.getCols());
          }
        }
      }

      ArrayList<aggregationDesc> aggrs = conf.getAggregators();
      for (aggregationDesc aggr : aggrs) {
        if (aggr == null)
          continue;
        ArrayList<exprNodeDesc> params = aggr.getParameters();
        for (exprNodeDesc param : params)
          colLists = Utilities.mergeUniqElems(colLists, param.getCols());
      }

      cppCtx.getPrunedColLists().put(op, colLists);
      return null;
    }
  }

  public static ColumnPrunerGroupByProc getGroupByProc() {
    return new ColumnPrunerGroupByProc();
  }

  public static class ColumnPrunerAnalysisProc implements NodeProcessor {

    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {

      AnalysisOperator op = (AnalysisOperator) nd;
      pruneAnalysisOperator(ctx, op, op.getConf(), op.getColumnExprMap());
      return null;
    }
  }

  public static ColumnPrunerAnalysisProc getAnalysisProc() {
    return new ColumnPrunerAnalysisProc();
  }

  private static void pruneAnalysisOperator(NodeProcessorCtx ctx,
      AnalysisOperator op, analysisDesc conf,
      Map<String, exprNodeDesc> columnExprMap) throws SemanticException {
    ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
    List<String> prunedColList = new ArrayList<String>();

    /* added by roachxiang begin */
    List<String> outputColsMerges = new ArrayList<String>();
    int AnalysisColCount = 0;
    /* added by roachxiang end */

    ArrayList<exprNodeDesc> partitionKeys = conf.getPartitionByKeys();
    for (exprNodeDesc partitionKey : partitionKeys) {
      AnalysisColCount++;
      prunedColList = Utilities.mergeUniqElems(prunedColList,
          partitionKey.getCols());
    }
    ArrayList<exprNodeDesc> orderKeys = conf.getOrderByKeys();
    for (exprNodeDesc orderKey : orderKeys) {
      AnalysisColCount++;
      prunedColList = Utilities.mergeUniqElems(prunedColList,
          orderKey.getCols());
    }

    ArrayList<analysisEvaluatorDesc> analysises = conf.getAnalysises();
    for (analysisEvaluatorDesc analysis : analysises) {
      ArrayList<exprNodeDesc> params = analysis.getParameters();
      if (params != null) {
        for (exprNodeDesc param : params) {
          if (param.getCols() != null)
            AnalysisColCount++;

          prunedColList = Utilities.mergeUniqElems(prunedColList,
              param.getCols());
        }
        if(params.size() == 0)
          AnalysisColCount++;
      }
    }

    List<String> childColLists = cppCtx
        .genColLists((Operator<? extends Serializable>) op);

    RowResolver anaRR = cppCtx.getOpToParseCtxMap().get(op).getRR();
    RowResolver newAnaRR = new RowResolver();
    Vector<ColumnInfo> rs = new Vector<ColumnInfo>();
    Map<String, exprNodeDesc> newColExprMap = new HashMap<String, exprNodeDesc>();
    ArrayList<String> outputCols = new ArrayList<String>();

    for (int i = 0; i < conf.getOutputColumnNames().size(); i++) {
      String internalName = conf.getOutputColumnNames().get(i);
      exprNodeDesc desc = columnExprMap.get(internalName);

      if (!childColLists.contains(internalName)) {
        if (conf.getOtherColumns().contains(desc))
          conf.getOtherColumns().remove(desc);
      } else {
        outputCols.add(internalName);
        if (desc != null)
          newColExprMap.put(internalName, desc);
      }

      if (i < AnalysisColCount)
        outputColsMerges.add(internalName);
    }

    for (int i = 0; i < outputCols.size(); i++) {
      String internalName = outputCols.get(i);
      String[] nm = anaRR.reverseLookup(internalName);
      ColumnInfo col = anaRR.get(nm[0], nm[1]);
      newAnaRR.put(nm[0], nm[1], col);
      rs.add(col);
    }

    op.setColumnExprMap(newColExprMap);

    outputColsMerges = Utilities.mergeUniqElems(outputColsMerges, outputCols);
    conf.setOutputColumnNames((ArrayList<String>) outputColsMerges);

    op.getSchema().setSignature(rs);
    cppCtx.getOpToParseCtxMap().get(op).setRR(newAnaRR);
    prunedColList = Utilities.mergeUniqElems(prunedColList, outputCols);
    cppCtx.getPrunedColLists().put(op,
        Utilities.mergeUniqElems(cppCtx.genColLists(op), prunedColList));
  }

  public static class ColumnPrunerDefaultProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      cppCtx.getPrunedColLists().put((Operator<? extends Serializable>) nd,
          cppCtx.genColLists((Operator<? extends Serializable>) nd));

      return null;
    }
  }

  public static ColumnPrunerDefaultProc getDefaultProc() {
    return new ColumnPrunerDefaultProc();
  }

  public static class ColumnPrunerTableScanProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      TableScanOperator scanOp = (TableScanOperator) nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      List<String> cols = cppCtx
          .genColLists((Operator<? extends Serializable>) nd);
      cppCtx.getPrunedColLists().put((Operator<? extends Serializable>) nd,
          cols);
      ArrayList<Integer> needed_columns = new ArrayList<Integer>();
      RowResolver inputRR = cppCtx.getOpToParseCtxMap().get(scanOp).getRR();
      boolean isFullScan=false;
      for (int i = 0; i < cols.size(); i++) {
        int position = inputRR.getPosition(cols.get(i));
        if(position<0){
          isFullScan=true;
          LOG.info("table "+scanOp.getConf().getAlias()+" should full scan");
        }
        needed_columns.add(position);
      }
      if(isFullScan){
        needed_columns.clear();
        int columnNum=inputRR.getColumnInfos().size();
        for(int i=0;i<columnNum;i++){
          needed_columns.add(i);
        }
      }
      scanOp.setNeededColumnIDs(needed_columns);
      return null;
    }
  }

  public static ColumnPrunerTableScanProc getTableScanProc() {
    return new ColumnPrunerTableScanProc();
  }

  public static class ColumnPrunerReduceSinkProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      ReduceSinkOperator op = (ReduceSinkOperator) nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      HashMap<Operator<? extends Serializable>, OpParseContext> opToParseCtxMap = cppCtx
          .getOpToParseCtxMap();
      RowResolver redSinkRR = opToParseCtxMap.get(op).getRR();
      reduceSinkDesc conf = op.getConf();
      List<Operator<? extends Serializable>> childOperators = op
          .getChildOperators();
      List<Operator<? extends Serializable>> parentOperators = op
          .getParentOperators();

      List<String> colLists = new ArrayList<String>();
      ArrayList<exprNodeDesc> keys = conf.getKeyCols();
      for (exprNodeDesc key : keys)
        colLists = Utilities.mergeUniqElems(colLists, key.getCols());

      if ((childOperators.size() == 1)
          && (childOperators.get(0) instanceof JoinOperator)) {
        assert parentOperators.size() == 1;
        Operator<? extends Serializable> par = parentOperators.get(0);
        JoinOperator childJoin = (JoinOperator) childOperators.get(0);
        RowResolver parRR = opToParseCtxMap.get(par).getRR();
        List<String> childJoinCols = cppCtx.getJoinPrunedColLists()
            .get(childJoin).get((byte) conf.getTag());
        boolean[] flags = new boolean[conf.getValueCols().size()];
        for (int i = 0; i < flags.length; i++)
          flags[i] = false;
        if (childJoinCols != null && childJoinCols.size() > 0) {
          Map<String, exprNodeDesc> exprMap = op.getColumnExprMap();
          for (String childCol : childJoinCols) {
            exprNodeDesc desc = exprMap.get(childCol);
            int index = conf.getValueCols().indexOf(desc);
            flags[index] = true;
            String[] nm = redSinkRR.reverseLookup(childCol);
            if (nm != null) {
              ColumnInfo cInfo = parRR.get(nm[0], nm[1]);
              if (!colLists.contains(cInfo.getInternalName()))
                colLists.add(cInfo.getInternalName());
            }
          }
        }
        Collections.sort(colLists);
        pruneReduceSinkOperator(flags, op, cppCtx);
      } else {
        ArrayList<exprNodeDesc> vals = conf.getValueCols();
        for (exprNodeDesc val : vals)
          colLists = Utilities.mergeUniqElems(colLists, val.getCols());
        if (conf.getTag2AggrParamORValueExpr() != null) {
          for (ArrayList<exprNodeDesc> tagedExprs : conf
              .getTag2AggrParamORValueExpr()) {
            for (exprNodeDesc expr : tagedExprs) {
              colLists = Utilities.mergeUniqElems(colLists, expr.getCols());
            }
          }
        }
      }

      cppCtx.getPrunedColLists().put(op, colLists);
      return null;
    }
  }

  public static ColumnPrunerReduceSinkProc getReduceSinkProc() {
    return new ColumnPrunerReduceSinkProc();
  }

  public static class ColumnPrunerLateralViewJoinProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      LateralViewJoinOperator op = (LateralViewJoinOperator) nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      List<String> cols = new ArrayList<String>();

      cols = cppCtx.genColLists(op);
      Map<String, exprNodeDesc> colExprMap = op.getColumnExprMap();

      List<String> colsAfterReplacement = new ArrayList<String>();
      for (String col : cols) {
        if (colExprMap.containsKey(col)) {
          exprNodeDesc expr = colExprMap.get(col);
          colsAfterReplacement.addAll(expr.getCols());
        } else {
          colsAfterReplacement.add(col);
        }
      }

      cppCtx.getPrunedColLists().put(op, colsAfterReplacement);
      return null;
    }
  }

  public static class ColumnPrunerSelectProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      SelectOperator op = (SelectOperator) nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      List<String> cols = new ArrayList<String>();

      if (op.getChildOperators() != null) {
        for (Operator<? extends Serializable> child : op.getChildOperators()) {
          if ((child instanceof FileSinkOperator)
              || (child instanceof PartitionerOperator)
              || (child instanceof ScriptOperator)
              || (child instanceof UDTFOperator)
              || (child instanceof LimitOperator)
              || (child instanceof UnionOperator)
              || (child instanceof LateralViewForwardOperator)) {
            cppCtx.getPrunedColLists()
                .put(op, cppCtx.getColsFromSelectExpr(op));
            return null;
          }
        }
      }
      cols = cppCtx.genColLists(op);

      selectDesc conf = op.getConf();
      cppCtx.getPrunedColLists().put(op,
          cppCtx.getSelectColsFromChildren(op, cols));

      if (conf.isSelStarNoCompute())
        return null;

      List<exprNodeDesc> originalColList = op.getConf().getColList();
      List<String> columns = new ArrayList<String>();
      for (exprNodeDesc expr : originalColList)
        Utilities.mergeUniqElems(columns, expr.getCols());
      ArrayList<String> originalOutputColumnNames = conf.getOutputColumnNames();
      if (cols.size() < originalOutputColumnNames.size()) {
        ArrayList<exprNodeDesc> newColList = new ArrayList<exprNodeDesc>();
        ArrayList<String> newOutputColumnNames = new ArrayList<String>();
        Vector<ColumnInfo> rs_oldsignature = op.getSchema().getSignature();
        Vector<ColumnInfo> rs_newsignature = new Vector<ColumnInfo>();
        RowResolver old_rr = cppCtx.getOpToParseCtxMap().get(op).getRR();
        RowResolver new_rr = new RowResolver();
        for (String col : cols) {
          int index = originalOutputColumnNames.indexOf(col);
          newOutputColumnNames.add(col);
          newColList.add(originalColList.get(index));
          rs_newsignature.add(rs_oldsignature.get(index));
          String[] tabcol = old_rr.reverseLookup(col);
          ColumnInfo columnInfo = old_rr.get(tabcol[0], tabcol[1]);
          new_rr.put(tabcol[0], tabcol[1], columnInfo);
        }
        cppCtx.getOpToParseCtxMap().get(op).setRR(new_rr);
        op.getSchema().setSignature(rs_newsignature);
        conf.setColList(newColList);
        conf.setOutputColumnNames(newOutputColumnNames);
        handleChildren(op, cols, cppCtx);
      }
      return null;
    }

    private void handleChildren(SelectOperator op,
        List<String> retainedSelOutputCols, ColumnPrunerProcCtx cppCtx)
        throws SemanticException {
      for (Operator<? extends Serializable> child : op.getChildOperators()) {
        if (child instanceof ReduceSinkOperator) {
          boolean[] flags = getPruneReduceSinkOpRetainFlags(
              retainedSelOutputCols, (ReduceSinkOperator) child);
          pruneReduceSinkOperator(flags, (ReduceSinkOperator) child, cppCtx);
        } else if (child instanceof FilterOperator) {

          try {
            RowResolver oldRR = cppCtx.getOpToParseCtxMap().get(child).getRR();
            RowResolver newRR = new RowResolver();
            Vector<ColumnInfo> sig = new Vector<ColumnInfo>();

            List<String> colNameList = op.getConf().getOutputColumnNames();
            int listLen = colNameList.size();

            for (int i = 0; i < listLen; i++) {
              String outputCol = colNameList.get(i);
              String[] nm = oldRR.reverseLookup(outputCol);

              if (nm == null) {
                outputCol = Utilities.ReduceField.VALUE.toString() + "."
                    + outputCol;
                nm = oldRR.reverseLookup(outputCol);
              }

              ColumnInfo colInfo = oldRR.get(nm[0], nm[1]);
              if (colInfo != null) {
                newRR.put(nm[0], nm[1], colInfo);
                sig.add(colInfo);
              }
            }
            cppCtx.getOpToParseCtxMap().get(child).setRR(newRR);
            child.getSchema().setSignature(sig);
          } catch (Exception x) {

          }
          for (Operator<? extends Serializable> filterChild : child
              .getChildOperators()) {
            if (filterChild instanceof ReduceSinkOperator) {
              boolean[] flags = getPruneReduceSinkOpRetainFlags(
                  retainedSelOutputCols, (ReduceSinkOperator) filterChild);
              pruneReduceSinkOperator(flags, (ReduceSinkOperator) filterChild,
                  cppCtx);
            }
          }
        }
      }
    }
  }

  private static boolean[] getPruneReduceSinkOpRetainFlags(
      List<String> retainedParentOpOutputCols, ReduceSinkOperator reduce) {
    reduceSinkDesc reduceConf = reduce.getConf();
    java.util.ArrayList<exprNodeDesc> originalValueEval = reduceConf
        .getValueCols();
    boolean[] flags = new boolean[originalValueEval.size()];
    for (int i = 0; i < originalValueEval.size(); i++) {
      flags[i] = false;
      List<String> current = originalValueEval.get(i).getCols();
      if (current == null || current.size() == 0) {
        flags[i] = true;
      } else {
        for (int j = 0; j < current.size(); j++) {
          if (retainedParentOpOutputCols.contains(current.get(j))) {
            flags[i] = true;
            break;
          }
        }
      }
    }
    return flags;
  }

  private static void pruneReduceSinkOperator(boolean[] retainFlags,
      ReduceSinkOperator reduce, ColumnPrunerProcCtx cppCtx)
      throws SemanticException {
    reduceSinkDesc reduceConf = reduce.getConf();
    Map<String, exprNodeDesc> oldMap = reduce.getColumnExprMap();
    Map<String, exprNodeDesc> newMap = new HashMap<String, exprNodeDesc>();
    Vector<ColumnInfo> sig = new Vector<ColumnInfo>();
    RowResolver oldRR = cppCtx.getOpToParseCtxMap().get(reduce).getRR();
    RowResolver newRR = new RowResolver();
    ArrayList<String> originalValueOutputColNames = reduceConf
        .getOutputValueColumnNames();
    java.util.ArrayList<exprNodeDesc> originalValueEval = reduceConf
        .getValueCols();
    ArrayList<String> newOutputColNames = new ArrayList<String>();
    java.util.ArrayList<exprNodeDesc> newValueEval = new ArrayList<exprNodeDesc>();
    for (int i = 0; i < retainFlags.length; i++) {
      if (retainFlags[i]) {
        newValueEval.add(originalValueEval.get(i));
        String outputCol = originalValueOutputColNames.get(i);
        newOutputColNames.add(outputCol);
        String[] nm = oldRR.reverseLookup(outputCol);
        if (nm == null) {
          outputCol = Utilities.ReduceField.VALUE.toString() + "." + outputCol;
          nm = oldRR.reverseLookup(outputCol);
        }
        newMap.put(outputCol, oldMap.get(outputCol));
        ColumnInfo colInfo = oldRR.get(nm[0], nm[1]);
        newRR.put(nm[0], nm[1], colInfo);
        sig.add(colInfo);
      }
    }

    ArrayList<exprNodeDesc> keyCols = reduceConf.getKeyCols();
    List<String> keys = new ArrayList<String>();
    RowResolver parResover = cppCtx.getOpToParseCtxMap()
        .get(reduce.getParentOperators().get(0)).getRR();
    for (int i = 0; i < keyCols.size(); i++) {
      keys = Utilities.mergeUniqElems(keys, keyCols.get(i).getCols());
    }
    for (int i = 0; i < keys.size(); i++) {
      String outputCol = keys.get(i);
      String[] nm = parResover.reverseLookup(outputCol);
      ColumnInfo colInfo = oldRR.get(nm[0], nm[1]);
      if (colInfo != null)
        newRR.put(nm[0], nm[1], colInfo);
    }

    cppCtx.getOpToParseCtxMap().get(reduce).setRR(newRR);
    reduce.setColumnExprMap(newMap);
    reduce.getSchema().setSignature(sig);
    reduceConf.setOutputValueColumnNames(newOutputColNames);
    reduceConf.setValueCols(newValueEval);
    tableDesc newValueTable = PlanUtils.getReduceValueTableDesc(PlanUtils
        .getFieldSchemasFromColumnList(reduceConf.getValueCols(),
            newOutputColNames, 0, ""));
    reduceConf.setValueSerializeInfo(newValueTable);
  }

  public static ColumnPrunerSelectProc getSelectProc() {
    return new ColumnPrunerSelectProc();
  }

  public static class ColumnPrunerJoinProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      JoinOperator op = (JoinOperator) nd;
      pruneJoinOperator(ctx, op, op.getConf(), op.getColumnExprMap(), null,
          false);
      return null;
    }
  }

  public static ColumnPrunerJoinProc getJoinProc() {
    return new ColumnPrunerJoinProc();
  }

  public static ColumnPrunerLateralViewJoinProc getLateralViewJoinProc() {
    return new ColumnPrunerLateralViewJoinProc();
  }

  public static class ColumnPrunerMapJoinProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      MapJoinOperator op = (MapJoinOperator) nd;
      pruneJoinOperator(ctx, op, op.getConf(), op.getColumnExprMap(), op
          .getConf().getRetainList(), true);
      return null;
    }
  }

  private static void pruneJoinOperator(NodeProcessorCtx ctx,
      CommonJoinOperator op, joinDesc conf,
      Map<String, exprNodeDesc> columnExprMap,
      Map<Byte, List<Integer>> retainMap, boolean mapJoin)
      throws SemanticException {
    ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
    Map<Byte, List<String>> prunedColLists = new HashMap<Byte, List<String>>();
    List<Operator<? extends Serializable>> childOperators = op
        .getChildOperators();

    for (Operator<? extends Serializable> child : childOperators) {
      if (child instanceof FileSinkOperator
          || child instanceof PartitionerOperator)
        return;
    }

    List<String> childColLists = cppCtx
        .genColLists((Operator<? extends Serializable>) op);

    Set<Map.Entry<Byte, List<exprNodeDesc>>> filters = conf.getFilters()
        .entrySet();
    Iterator<Map.Entry<Byte, List<exprNodeDesc>>> iter = filters.iterator();
    while (iter.hasNext()) {
      Map.Entry<Byte, List<exprNodeDesc>> entry = iter.next();
      Byte tag = entry.getKey();
      for (exprNodeDesc desc : entry.getValue()) {
        List<String> cols = prunedColLists.get(tag);
        cols = Utilities.mergeUniqElems(cols, desc.getCols());
        prunedColLists.put(tag, cols);
      }
    }

    RowResolver joinRR = cppCtx.getOpToParseCtxMap().get(op).getRR();
    RowResolver newJoinRR = new RowResolver();
    ArrayList<String> outputCols = new ArrayList<String>();
    Vector<ColumnInfo> rs = new Vector<ColumnInfo>();
    Map<String, exprNodeDesc> newColExprMap = new HashMap<String, exprNodeDesc>();

    for (int i = 0; i < conf.getOutputColumnNames().size(); i++) {
      String internalName = conf.getOutputColumnNames().get(i);
      exprNodeDesc desc = columnExprMap.get(internalName);
      Byte tag = conf.getReversedExprs().get(internalName);
      if (!childColLists.contains(internalName)) {
        int index = conf.getExprs().get(tag).indexOf(desc);
        if (index < 0)
          continue;
        conf.getExprs().get(tag).remove(desc);
        if (retainMap != null)
          retainMap.get(tag).remove(index);
      } else {
        List<String> prunedRSList = prunedColLists.get(tag);
        if (prunedRSList == null) {
          prunedRSList = new ArrayList<String>();
          prunedColLists.put(tag, prunedRSList);
        }
        prunedRSList = Utilities.mergeUniqElems(prunedRSList, desc.getCols());
        outputCols.add(internalName);
        newColExprMap.put(internalName, desc);
      }
    }

    if (mapJoin) {
      List<tableDesc> valueTableDescs = new ArrayList<tableDesc>();
      for (int pos = 0; pos < op.getParentOperators().size(); pos++) {
        List<exprNodeDesc> valueCols = conf.getExprs()
            .get(new Byte((byte) pos));
        StringBuilder keyOrder = new StringBuilder();
        for (int i = 0; i < valueCols.size(); i++) {
          keyOrder.append("+");
        }

        tableDesc valueTableDesc = PlanUtils.getMapJoinValueTableDesc(PlanUtils
            .getFieldSchemasFromColumnList(valueCols, "mapjoinvalue"));

        valueTableDescs.add(valueTableDesc);
      }
      ((mapJoinDesc) conf).setValueTblDescs(valueTableDescs);

      Set<Map.Entry<Byte, List<exprNodeDesc>>> exprs = ((mapJoinDesc) conf)
          .getKeys().entrySet();
      Iterator<Map.Entry<Byte, List<exprNodeDesc>>> iters = exprs.iterator();
      while (iters.hasNext()) {
        Map.Entry<Byte, List<exprNodeDesc>> entry = iters.next();
        List<exprNodeDesc> lists = entry.getValue();
        for (int j = 0; j < lists.size(); j++) {
          exprNodeDesc desc = lists.get(j);
          Byte tag = entry.getKey();
          List<String> cols = prunedColLists.get(tag);
          cols = Utilities.mergeUniqElems(cols, desc.getCols());
          prunedColLists.put(tag, cols);
        }
      }

    }

    for (Operator<? extends Serializable> child : childOperators) {
      if (child instanceof ReduceSinkOperator) {
        boolean[] flags = getPruneReduceSinkOpRetainFlags(childColLists,
            (ReduceSinkOperator) child);
        pruneReduceSinkOperator(flags, (ReduceSinkOperator) child, cppCtx);
      }
    }

    for (int i = 0; i < outputCols.size(); i++) {
      String internalName = outputCols.get(i);
      String[] nm = joinRR.reverseLookup(internalName);
      ColumnInfo col = joinRR.get(nm[0], nm[1]);
      newJoinRR.put(nm[0], nm[1], col);
      rs.add(col);
    }

    op.setColumnExprMap(newColExprMap);
    conf.setOutputColumnNames(outputCols);
    op.getSchema().setSignature(rs);
    cppCtx.getOpToParseCtxMap().get(op).setRR(newJoinRR);
    cppCtx.getJoinPrunedColLists().put(op, prunedColLists);
  }

  public static ColumnPrunerMapJoinProc getMapJoinProc() {
    return new ColumnPrunerMapJoinProc();
  }

  public static class ColumnPrunerRowExtendGroupByProc implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      RowExtendOperator op = (RowExtendOperator) nd;
      ColumnPrunerProcCtx cppCtx = (ColumnPrunerProcCtx) ctx;
      List<String> colLists = new ArrayList<String>();
      rowExtendDesc conf = op.getConf();
      ArrayList<exprNodeDesc> keys = conf.getGroupByKeys();
      for (exprNodeDesc key : keys)
        colLists = Utilities.mergeUniqElems(colLists, key.getCols());

      ArrayList<exprNodeDesc> values = conf.getValues();
      for (exprNodeDesc value : values)
        colLists = Utilities.mergeUniqElems(colLists, value.getCols());

      cppCtx.getPrunedColLists().put(op, colLists);
      return null;
    }
  }

  public static NodeProcessor getRowExtendGroupByProc() {
    return new ColumnPrunerRowExtendGroupByProc();
  }
}
