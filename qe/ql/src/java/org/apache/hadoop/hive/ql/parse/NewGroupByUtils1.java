/**
* Tencent is pleased to support the open source community by making TDW available.
* Copyright (C) 2014 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use 
* this file except in compliance with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed 
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS 
* OF ANY KIND, either express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/
package org.apache.hadoop.hive.ql.parse;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.GenericUDAFInfo;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.aggregationDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.groupByDesc;
import org.apache.hadoop.hive.ql.plan.groupByDesc.Mode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class NewGroupByUtils1 {
  static final private Log LOG = LogFactory.getLog(NewGroupByUtils1.class
      .getName());

  public static final String _CUBE_ROLLUP_GROUPINGSETS_TAG_ = "_CUBE_ROLLUP_GROUPINGSETS_TAG_";
  private static final String _AGGRPARTTAG_ = "_AGGRPARTTAG_";

  private static final String _GBY_AGGRPART_OUTPUT_COLNAME_ = "_GBY_AGGRPART_OUTPUT_COLNAME_";
  private LinkedHashMap<Operator<? extends Serializable>, OpParseContext> opParseCtx;
  private HiveConf conf;

  public void initialize(HiveConf conf,
      LinkedHashMap<Operator<? extends Serializable>, OpParseContext> opParseCtx) {
    this.conf = conf;
    this.opParseCtx = opParseCtx;
  }

  @SuppressWarnings("unchecked")
  public Operator genGroupByPlanMapAggr1MR(String dest, QB qb,
      Operator inputOperatorInfo) throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();
    ArrayList<ArrayList<Integer>> distTag2AggrPos = new ArrayList<ArrayList<Integer>>();
    ArrayList<ArrayList<ASTNode>> distTag2AggrParamAst = new ArrayList<ArrayList<ASTNode>>();
    HashMap<Integer, ArrayList<Integer>> nonDistPos2TagOffs = new HashMap<Integer, ArrayList<Integer>>();

    genDistTag2Aggr(qb, dest, distTag2AggrPos, distTag2AggrParamAst,
        nonDistPos2TagOffs);

    ArrayList<GenericUDAFEvaluator> genericUDAFEvaluators = genAllGenericUDAFEvaluators(
        qb, dest, opParseCtx.get(inputOperatorInfo).getRR());

    GroupByOperator groupByOperatorInfo = (GroupByOperator) genNewGroupByPlanGroupByOperator(
        qb, dest, inputOperatorInfo, groupByDesc.Mode.HASH,
        genericUDAFEvaluators, distTag2AggrPos, distTag2AggrParamAst,
        nonDistPos2TagOffs);

    int numReducers = -1;

    List<ASTNode> grpByExprs = SemanticAnalyzer.getGroupByForClause(parseInfo,
        dest);
    if (grpByExprs.isEmpty())
      numReducers = 1;

    Operator reduceSinkOperatorInfo = genNewGroupByPlanReduceSinkOperator(qb,
        dest, groupByOperatorInfo, numReducers, true, false, true,
        distTag2AggrPos, distTag2AggrParamAst);

    return (GroupByOperator) genNewGroupByPlanGroupByOperator(qb, dest,
        reduceSinkOperatorInfo, groupByDesc.Mode.MERGEPARTIAL,
        genericUDAFEvaluators, distTag2AggrPos, distTag2AggrParamAst,
        nonDistPos2TagOffs);
  }

  @SuppressWarnings("unchecked")
  public Operator genGroupByPlanMapAggr2MR(String dest, QB qb,
      Operator inputOperatorInfo) throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();
    ArrayList<ArrayList<Integer>> distTag2AggrPos = new ArrayList<ArrayList<Integer>>();
    ArrayList<ArrayList<ASTNode>> distTag2AggrParamAst = new ArrayList<ArrayList<ASTNode>>();
    HashMap<Integer, ArrayList<Integer>> nonDistPos2TagOffs = new HashMap<Integer, ArrayList<Integer>>();

    genDistTag2Aggr(qb, dest, distTag2AggrPos, distTag2AggrParamAst,
        nonDistPos2TagOffs);

    ArrayList<GenericUDAFEvaluator> genericUDAFEvaluators = genAllGenericUDAFEvaluators(
        qb, dest, opParseCtx.get(inputOperatorInfo).getRR());

    GroupByOperator groupByOperatorInfo = (GroupByOperator) genNewGroupByPlanGroupByOperator(
        qb, dest, inputOperatorInfo, groupByDesc.Mode.HASH,
        genericUDAFEvaluators, distTag2AggrPos, distTag2AggrParamAst,
        nonDistPos2TagOffs);

    if (!optimizeMapAggrGroupBy(dest, qb)) {
      Operator reduceSinkOperatorInfo = genNewGroupByPlanReduceSinkOperator(qb,
          dest, groupByOperatorInfo, -1, true, true, true, distTag2AggrPos,
          distTag2AggrParamAst);

      Operator groupByOperatorInfo2 = genNewGroupByPlanGroupByOperator(qb,
          dest, reduceSinkOperatorInfo, groupByDesc.Mode.PARTIALS,
          genericUDAFEvaluators, distTag2AggrPos, distTag2AggrParamAst,
          nonDistPos2TagOffs);

      int numReducers = -1;
      List<ASTNode> grpByExprs = SemanticAnalyzer.getGroupByForClause(
          parseInfo, dest);
      if (grpByExprs.isEmpty())
        numReducers = 1;

      Operator reduceSinkOperatorInfo2 = genNewGroupByPlanReduceSinkOperator(
          qb, dest, groupByOperatorInfo2, numReducers, true, false, false,
          distTag2AggrPos, distTag2AggrParamAst);

      return genNewGroupByPlanGroupByOperator(qb, dest,
          reduceSinkOperatorInfo2, groupByDesc.Mode.FINAL,
          genericUDAFEvaluators, distTag2AggrPos, distTag2AggrParamAst,
          nonDistPos2TagOffs);
    } else {
      Operator reduceSinkOperatorInfo = genNewGroupByPlanReduceSinkOperator(qb,
          dest, groupByOperatorInfo, 1, true, false, true, distTag2AggrPos,
          distTag2AggrParamAst);

      return (GroupByOperator) genNewGroupByPlanGroupByOperator(qb, dest,
          reduceSinkOperatorInfo, groupByDesc.Mode.MERGEPARTIAL,
          genericUDAFEvaluators, distTag2AggrPos, distTag2AggrParamAst,
          nonDistPos2TagOffs);
    }
  }

  @SuppressWarnings("unchecked")
  public Operator genGroupByPlan1MR(String dest, QB qb,
      Operator inputOperatorInfo) throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();
    ArrayList<ArrayList<Integer>> distTag2AggrPos = new ArrayList<ArrayList<Integer>>();
    ArrayList<ArrayList<ASTNode>> distTag2AggrParamAst = new ArrayList<ArrayList<ASTNode>>();
    HashMap<Integer, ArrayList<Integer>> nonDistPos2TagOffs = new HashMap<Integer, ArrayList<Integer>>();

    genDistTag2Aggr(qb, dest, distTag2AggrPos, distTag2AggrParamAst,
        nonDistPos2TagOffs);

    ArrayList<GenericUDAFEvaluator> genericUDAFEvaluators = genAllGenericUDAFEvaluators(
        qb, dest, opParseCtx.get(inputOperatorInfo).getRR());

    int numReducers = -1;
    List<ASTNode> grpByExprs = SemanticAnalyzer.getGroupByForClause(parseInfo,
        dest);
    if (grpByExprs.isEmpty())
      numReducers = 1;

    Operator reduceSinkOperatorInfo = genNewGroupByPlanReduceSinkOperator(qb,
        dest, inputOperatorInfo, numReducers, false, false, true,
        distTag2AggrPos, distTag2AggrParamAst);

    Operator groupByOperatorInfo = genNewGroupByPlanGroupByOperator(qb, dest,
        reduceSinkOperatorInfo, groupByDesc.Mode.COMPLETE,
        genericUDAFEvaluators, distTag2AggrPos, distTag2AggrParamAst,
        nonDistPos2TagOffs);

    return groupByOperatorInfo;
  }

  @SuppressWarnings("unchecked")
  public Operator genGroupByPlan2MR(String dest, QB qb,
      Operator inputOperatorInfo) throws SemanticException {

    QBParseInfo parseInfo = qb.getParseInfo();
    ArrayList<ArrayList<Integer>> distTag2AggrPos = new ArrayList<ArrayList<Integer>>();
    ArrayList<ArrayList<ASTNode>> distTag2AggrParamAst = new ArrayList<ArrayList<ASTNode>>();
    HashMap<Integer, ArrayList<Integer>> nonDistPos2TagOffs = new HashMap<Integer, ArrayList<Integer>>();

    genDistTag2Aggr(qb, dest, distTag2AggrPos, distTag2AggrParamAst,
        nonDistPos2TagOffs);

    ArrayList<GenericUDAFEvaluator> genericUDAFEvaluators = genAllGenericUDAFEvaluators(
        qb, dest, opParseCtx.get(inputOperatorInfo).getRR());

    Operator reduceSinkOperatorInfo = genNewGroupByPlanReduceSinkOperator(qb,
        dest, inputOperatorInfo, -1, false, true, true, distTag2AggrPos,
        distTag2AggrParamAst);

    Operator groupByOperatorInfo = genNewGroupByPlanGroupByOperator(qb, dest,
        reduceSinkOperatorInfo, groupByDesc.Mode.PARTIAL1,
        genericUDAFEvaluators, distTag2AggrPos, distTag2AggrParamAst,
        nonDistPos2TagOffs);

    int numReducers = -1;
    List<ASTNode> grpByExprs = SemanticAnalyzer.getGroupByForClause(parseInfo,
        dest);
    if (grpByExprs.isEmpty())
      numReducers = 1;

    Operator reduceSinkOperatorInfo2 = genNewGroupByPlanReduceSinkOperator(qb,
        dest, groupByOperatorInfo, numReducers, true, false, false,
        distTag2AggrPos, distTag2AggrParamAst);

    Operator groupByOperatorInfo2 = genNewGroupByPlanGroupByOperator(qb, dest,
        reduceSinkOperatorInfo2, groupByDesc.Mode.FINAL, genericUDAFEvaluators,
        distTag2AggrPos, distTag2AggrParamAst, nonDistPos2TagOffs);

    return groupByOperatorInfo2;
  }

  private void genDistTag2Aggr(QB qb, String dest,
      ArrayList<ArrayList<Integer>> tag2AggrPos,
      ArrayList<ArrayList<ASTNode>> distTag2AggrParamAst,
      HashMap<Integer, ArrayList<Integer>> nonDistPos2TagOffs) {

    HashMap<String, ASTNode> aggregationTrees = qb.getParseInfo()
        .getAggregationExprsForClause(dest);
    if (aggregationTrees == null || aggregationTrees.isEmpty()) {
      return;
    }
    tag2AggrPos.clear();
    distTag2AggrParamAst.clear();
    tag2AggrPos.add(new ArrayList<Integer>());
    distTag2AggrParamAst.add(new ArrayList<ASTNode>());
    HashMap<String, Integer> treeidx = new HashMap<String, Integer>();
    HashMap<Integer, HashSet<String>> tag2paramaststr = new HashMap<Integer, HashSet<String>>();
    int pos = 0;
    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ASTNode value = entry.getValue();
      String[] params = new String[value.getChildCount() - 1];
      for (int i = 1; i < value.getChildCount(); i++) {
        params[i - 1] = value.getChild(i).toStringTree();
      }
      Arrays.sort(params);
      ArrayList<String> params1 = new ArrayList<String>();
      params1.add(params[0]);
      String curr = params[0];
      for (int i = 1; i < params.length; i++) {
        if (!curr.equalsIgnoreCase(params[i])) {
          params1.add(params[i]);
          curr = params[i];
        }
      }

      StringBuffer sb = new StringBuffer();
      sb.append(value.getToken().getType());
      for (int i = 0; i < params1.size(); i++) {
        sb.append(params1.get(i));
      }
      String asttree = sb.toString();

      if (!treeidx.containsKey(asttree)) {
        if (value.getToken().getType() == HiveParser.TOK_FUNCTIONDI) {
          int disttag = tag2AggrPos.size();
          treeidx.put(asttree, disttag);
          tag2AggrPos.add(new ArrayList<Integer>());
          distTag2AggrParamAst.add(new ArrayList<ASTNode>());
          if (!tag2paramaststr.containsKey(disttag)) {
            tag2paramaststr.put(disttag, new HashSet<String>());
          }
          for (int i = 1; i < value.getChildCount(); i++) {
            ASTNode param = (ASTNode) value.getChild(i);
            if (!tag2paramaststr.get(disttag).contains(param.toStringTree())) {
              tag2paramaststr.get(disttag).add(param.toStringTree());
              distTag2AggrParamAst.get(distTag2AggrParamAst.size() - 1).add(
                  param);
            }
          }
        } else {
          if (!tag2paramaststr.containsKey(0)) {
            tag2paramaststr.put(0, new HashSet<String>());
          }
          treeidx.put(asttree, 0);
          for (int i = 1; i < value.getChildCount(); i++) {
            ASTNode param = (ASTNode) value.getChild(i);
            if (!tag2paramaststr.get(0).contains(param.toStringTree())) {
              tag2paramaststr.get(0).add(param.toStringTree());
              distTag2AggrParamAst.get(0).add(param);
            }
          }
        }
      }
      if (value.getToken().getType() != HiveParser.TOK_FUNCTIONDI) {
        nonDistPos2TagOffs.put(pos, new ArrayList<Integer>());
        for (int i = 1; i < value.getChildCount(); i++) {
          String param = value.getChild(i).toStringTree();
          int idx = -1;
          for (int j = 0; j < distTag2AggrParamAst.get(0).size(); j++) {
            if (distTag2AggrParamAst.get(0).get(j).toStringTree().equals(param)) {
              idx = j;
              break;
            }
          }
          nonDistPos2TagOffs.get(pos).add(idx);
        }
      }

      tag2AggrPos.get(treeidx.get(asttree)).add(pos);
      pos++;
    }

    LOG.debug("distTag2AggrPos:\t" + tag2AggrPos);
    for (int i = 0; i < tag2AggrPos.size(); i++) {
      LOG.debug("distTag2AggrPos[" + i + "]:\t" + tag2AggrPos.get(i));
    }
    LOG.debug("distTag2AggrParamAst:\t" + distTag2AggrParamAst);
    for (int i = 0; i < distTag2AggrParamAst.size(); i++) {
      StringBuffer sb = new StringBuffer();
      for (int j = 0; j < distTag2AggrParamAst.get(i).size(); j++) {
        sb.append(distTag2AggrParamAst.get(i).get(j).toStringTree()).append(
            "\t");
      }
      LOG.debug("distTag2AggrParamAst[" + i + "]:\t" + sb.toString());
    }
    LOG.debug("nonDistPos2TagOffs:\t" + nonDistPos2TagOffs);
    for (Integer key : nonDistPos2TagOffs.keySet()) {
      LOG.debug("nonDistPos2TagOffs[" + key + "]:\t"
          + nonDistPos2TagOffs.get(key));
    }
  }

  private ArrayList<GenericUDAFEvaluator> genAllGenericUDAFEvaluators(QB qb,
      String dest, RowResolver rowResolver) throws SemanticException {
    ArrayList<GenericUDAFEvaluator> genericUDAFEvaluators = new ArrayList<GenericUDAFEvaluator>();
    QBParseInfo parseInfo = qb.getParseInfo();
    LinkedHashMap<String, ASTNode> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);

    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ASTNode value = entry.getValue();
      String aggName = value.getChild(0).getText();
      ArrayList<exprNodeDesc> aggParameters = new ArrayList<exprNodeDesc>();

      for (int i = 1; i < value.getChildCount(); i++) {
        ASTNode paraExpr = (ASTNode) value.getChild(i);

        exprNodeDesc paraExprDesc = SemanticAnalyzer.genExprNodeDesc(paraExpr,
            rowResolver, qb, -1, conf);

        aggParameters.add(paraExprDesc);
      }

      boolean isDistinct = value.getType() == HiveParser.TOK_FUNCTIONDI;
      boolean isAllColumns = value.getType() == HiveParser.TOK_FUNCTIONSTAR;

      GenericUDAFEvaluator genericUDAFEvaluator = SemanticAnalyzer
          .getGenericUDAFEvaluator(aggName, aggParameters, value, isDistinct,
              isAllColumns);

      genericUDAFEvaluators.add(genericUDAFEvaluator);
    }

    return genericUDAFEvaluators;
  }

  @SuppressWarnings("unchecked")
  private GroupByOperator genNewGroupByPlanGroupByOperator(QB qb, String dest,
      Operator inputOperatorInfo, Mode mode,
      ArrayList<GenericUDAFEvaluator> genericUDAFEvaluators,
      ArrayList<ArrayList<Integer>> tag2AggrPos,
      ArrayList<ArrayList<ASTNode>> tag2AggrParamAst,
      HashMap<Integer, ArrayList<Integer>> nonDistPos2TagOffs)
      throws SemanticException {

    RowResolver groupByInputRowResolver = opParseCtx.get(inputOperatorInfo)
        .getRR();
    QBParseInfo parseInfo = qb.getParseInfo();
    RowResolver groupByOutputRowResolver = new RowResolver();
    groupByOutputRowResolver.setIsExprResolver(true);
    RowSchema operatorRowSchema = new RowSchema();
    operatorRowSchema.setSignature(new Vector<ColumnInfo>());
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();

    ArrayList<exprNodeDesc> groupByKeys = new ArrayList<exprNodeDesc>();
    ArrayList<String> outputColumnNames = new ArrayList<String>();

    List<ASTNode> grpByExprs = SemanticAnalyzer.getGroupByForClause(parseInfo,
        dest);
    int colid = 0;
    if (qb.getParseInfo().getDestContainsGroupbyCubeOrRollupClause(dest)) {
      String colName = getColumnInternalName(colid++);
      outputColumnNames.add(colName);
      ColumnInfo info = groupByInputRowResolver.get("",
          NewGroupByUtils1._CUBE_ROLLUP_GROUPINGSETS_TAG_);
      exprNodeDesc grpByExprNode = new exprNodeColumnDesc(info.getType(),
          info.getInternalName(), info.getAlias(), info.getIsPartitionCol());
      groupByKeys.add(grpByExprNode);
      ColumnInfo colInfo = new ColumnInfo(colName, grpByExprNode.getTypeInfo(),
          "", false);

      groupByOutputRowResolver.put("",
          NewGroupByUtils1._CUBE_ROLLUP_GROUPINGSETS_TAG_, colInfo);

      operatorRowSchema.getSignature().add(colInfo);
      colExprMap.put(colName, grpByExprNode);
    }
    for (int i = 0; i < grpByExprs.size(); i++) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      exprNodeDesc grpByExprNode = SemanticAnalyzer.genExprNodeDesc(grpbyExpr,
          groupByInputRowResolver, qb, -1, conf);
      groupByKeys.add(grpByExprNode);
      String colName = getColumnInternalName(colid++);
      outputColumnNames.add(colName);
      ColumnInfo colInfo = new ColumnInfo(colName, grpByExprNode.getTypeInfo(),
          "", false);
      groupByOutputRowResolver.putExpression(grpbyExpr, colInfo);
      operatorRowSchema.getSignature().add(colInfo);
      colExprMap.put(colName, grpByExprNode);
    }

    boolean containsfunctions = tag2AggrPos != null && tag2AggrPos.size() > 0;
    boolean containsnondistinctfunctions = containsfunctions
        && tag2AggrPos.get(0).size() > 0;

    LinkedHashMap<String, ASTNode> aggregationTrees = parseInfo
        .getAggregationExprsForClause(dest);

    ArrayList<ASTNode> aggregationTreesArray = new ArrayList<ASTNode>(
        aggregationTrees.size());
    aggregationTreesArray.addAll(aggregationTrees.values());

    HashMap<Integer, Integer> pos2tag = new HashMap<Integer, Integer>();
    for (int tag = 0; tag < tag2AggrPos.size(); tag++) {
      for (Integer pos : tag2AggrPos.get(tag)) {
        pos2tag.put(pos, tag);
      }
    }

    ArrayList<ArrayList<exprNodeDesc>> tag2AggrParamORValueExpr = new ArrayList<ArrayList<exprNodeDesc>>();
    ArrayList<aggregationDesc> aggregations = null;
    aggregations = new ArrayList<aggregationDesc>(aggregationTrees.size());
    for (int i = 0; i < aggregationTrees.size(); i++) {
      aggregations.add(null);
    }
    exprNodeDesc aggrPartExpr = null;

    if (mode == Mode.HASH) {

      if (containsfunctions) {
        String colNameAggrPart = getColumnInternalName(colid++);
        outputColumnNames.add(colNameAggrPart);

        List<TypeInfo> unionTypes = new ArrayList<TypeInfo>();

        for (int tag = 0; tag < tag2AggrParamAst.size(); tag++) {
          tag2AggrParamORValueExpr.add(new ArrayList<exprNodeDesc>());
          ArrayList<exprNodeDesc> aggParameters = new ArrayList<exprNodeDesc>();

          for (int j = 0; j < tag2AggrParamAst.get(tag).size(); j++) {
            ASTNode paraExpr = (ASTNode) tag2AggrParamAst.get(tag).get(j);
            exprNodeDesc exprNode = SemanticAnalyzer.genExprNodeDesc(paraExpr,
                groupByInputRowResolver, qb, -1, conf);
            tag2AggrParamORValueExpr.get(tag).add(exprNode);
            aggParameters.add(exprNode);
          }

          ArrayList<String> names = new ArrayList<String>();
          ArrayList<TypeInfo> typeInfos = new ArrayList<TypeInfo>();
          if (tag == 0) {
            if (!containsnondistinctfunctions) {
              names.add("nondistnull");
              typeInfos.add(TypeInfoFactory.voidTypeInfo);
            } else {
              int posoff = 0;
              for (Integer pos : tag2AggrPos.get(tag)) {
                ASTNode value = aggregationTreesArray.get(pos);
                String aggName = value.getChild(0).getText();

                boolean isDistinct = value.getType() == HiveParser.TOK_FUNCTIONDI;
                GenericUDAFEvaluator.Mode amode = SemanticAnalyzer
                    .groupByDescModeToUDAFMode(mode, isDistinct);

                GenericUDAFEvaluator genericUDAFEvaluator = genericUDAFEvaluators
                    .get(pos);
                assert (genericUDAFEvaluator != null);

                ArrayList<exprNodeDesc> aggParameters1 = aggParameters;
                ArrayList<Integer> offs = nonDistPos2TagOffs.get(pos);
                aggParameters1 = new ArrayList<exprNodeDesc>();
                for (Integer off : offs) {
                  aggParameters1.add(aggParameters.get(off));
                }

                GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo(
                    genericUDAFEvaluator, amode, aggParameters1);

                aggregations.set(pos, new aggregationDesc(
                    aggName.toLowerCase(), udaf.genericUDAFEvaluator,
                    udaf.convertedParameters, isDistinct, amode));

                String innername = getColumnInternalName(posoff);
                String field = colNameAggrPart + ":" + tag + "." + innername;

                ColumnInfo outColInfo = new ColumnInfo(field, udaf.returnType,
                    "", false);
                groupByOutputRowResolver.put("", _AGGRPARTTAG_ + tag + "_"
                    + posoff, outColInfo);

                posoff++;

                names.add(innername);
                typeInfos.add(udaf.returnType);
              }
            }
          } else {
            for (int i = 0; i < tag2AggrParamORValueExpr.get(tag).size(); i++) {

              String innername = getColumnInternalName(i);
              TypeInfo innertype = tag2AggrParamORValueExpr.get(tag).get(i)
                  .getTypeInfo();

              String field = colNameAggrPart + ":" + tag + "." + innername;
              ColumnInfo outColInfo = new ColumnInfo(field, innertype, "",
                  false);
              groupByOutputRowResolver.put("", _AGGRPARTTAG_ + tag + "_" + i,
                  outColInfo);

              names.add(innername);
              typeInfos.add(innertype);
            }
          }
          unionTypes.add(TypeInfoFactory.getStructTypeInfo(names, typeInfos));
        }

        ColumnInfo outColInfo = new ColumnInfo(colNameAggrPart,
            TypeInfoFactory.getUnionTypeInfo(unionTypes), "", false);
        groupByOutputRowResolver.put("", _GBY_AGGRPART_OUTPUT_COLNAME_,
            outColInfo);
        operatorRowSchema.getSignature().add(outColInfo);
      }

    } else if (mode == Mode.PARTIAL1 || mode == Mode.PARTIALS) {
      if (containsfunctions) {

        ColumnInfo aggrPartInfo = groupByInputRowResolver.get("",
            _GBY_AGGRPART_OUTPUT_COLNAME_);
        aggrPartExpr = new exprNodeColumnDesc(aggrPartInfo.getType(),
            aggrPartInfo.getInternalName(), "", false);

        String colNameAggrPart = getColumnInternalName(colid++);
        outputColumnNames.add(colNameAggrPart);
        List<TypeInfo> unionTypes = new ArrayList<TypeInfo>();

        for (int tag = 0; tag < tag2AggrParamAst.size(); tag++) {
          tag2AggrParamORValueExpr.add(new ArrayList<exprNodeDesc>());
          ArrayList<exprNodeDesc> aggParameters = new ArrayList<exprNodeDesc>();

          int paramlen = (tag == 0 && mode == Mode.PARTIALS) ? tag2AggrPos.get(
              tag).size() : tag2AggrParamAst.get(tag).size();
          for (int j = 0; j < paramlen; j++) {
            ColumnInfo inputColInfo = groupByInputRowResolver.get("",
                _AGGRPARTTAG_ + tag + "_" + j);

            exprNodeDesc exprNode = new exprNodeColumnDesc(
                inputColInfo.getType(), inputColInfo.getInternalName(), "",
                false);

            tag2AggrParamORValueExpr.get(tag).add(exprNode);
            aggParameters.add(exprNode);
          }

          ArrayList<String> names = new ArrayList<String>();
          ArrayList<TypeInfo> typeInfos = new ArrayList<TypeInfo>();

          int posoff = 0;
          for (Integer pos : tag2AggrPos.get(tag)) {
            ASTNode value = aggregationTreesArray.get(pos);
            String aggName = value.getChild(0).getText();

            boolean isDistinct = value.getType() == HiveParser.TOK_FUNCTIONDI;
            GenericUDAFEvaluator.Mode amode = SemanticAnalyzer
                .groupByDescModeToUDAFMode(mode, isDistinct);

            GenericUDAFEvaluator genericUDAFEvaluator = genericUDAFEvaluators
                .get(pos);
            assert (genericUDAFEvaluator != null);

            ArrayList<exprNodeDesc> aggParameters1 = aggParameters;
            if (tag == 0 && mode == Mode.PARTIAL1) {
              ArrayList<Integer> offs = nonDistPos2TagOffs.get(pos);
              aggParameters1 = new ArrayList<exprNodeDesc>();
              for (Integer off : offs) {
                aggParameters1.add(aggParameters.get(off));
              }
            } else if (tag == 0) {
              aggParameters1 = new ArrayList<exprNodeDesc>();
              aggParameters1.add(aggParameters.get(posoff));
            }

            GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo(
                genericUDAFEvaluator, amode, aggParameters1);

            aggregations.set(pos, new aggregationDesc(aggName.toLowerCase(),
                udaf.genericUDAFEvaluator, udaf.convertedParameters,
                isDistinct, amode));

            String innername = getColumnInternalName(posoff);
            String field = colNameAggrPart + ":" + tag + "." + innername;

            ColumnInfo outColInfo = new ColumnInfo(field, udaf.returnType, "",
                false);
            groupByOutputRowResolver.put("",
                _AGGRPARTTAG_ + tag + "_" + posoff, outColInfo);

            posoff++;

            names.add(innername);
            typeInfos.add(udaf.returnType);
          }

          if (names.isEmpty()) {
            names.add("nondistnull");
            typeInfos.add(TypeInfoFactory.voidTypeInfo);
          }

          unionTypes.add(TypeInfoFactory.getStructTypeInfo(names, typeInfos));
        }

        ColumnInfo outColInfo = new ColumnInfo(colNameAggrPart,
            TypeInfoFactory.getUnionTypeInfo(unionTypes), "", false);
        groupByOutputRowResolver.put("", _GBY_AGGRPART_OUTPUT_COLNAME_,
            outColInfo);
        operatorRowSchema.getSignature().add(outColInfo);
      }

    } else if (mode == Mode.MERGEPARTIAL || mode == Mode.FINAL
        || mode == Mode.COMPLETE) {

      if (containsfunctions) {

        ColumnInfo aggrPartInfo = groupByInputRowResolver.get("",
            _GBY_AGGRPART_OUTPUT_COLNAME_);
        aggrPartExpr = new exprNodeColumnDesc(aggrPartInfo.getType(),
            aggrPartInfo.getInternalName(), "", false);

        HashMap<Integer, String> pos2colname = new HashMap<Integer, String>();
        for (int pos = 0; pos < aggregationTreesArray.size(); pos++) {
          String colName = getColumnInternalName(colid++);
          outputColumnNames.add(colName);
          pos2colname.put(pos, colName);
        }

        HashMap<Integer, ColumnInfo> pos2valueInfo = new HashMap<Integer, ColumnInfo>();
        for (int tag = 0; tag < tag2AggrPos.size(); tag++) {
          tag2AggrParamORValueExpr.add(new ArrayList<exprNodeDesc>());
          ArrayList<exprNodeDesc> aggParameters = new ArrayList<exprNodeDesc>();

          int aggrlen = (mode == Mode.FINAL) ? tag2AggrPos.get(tag).size()
              : ((mode == Mode.COMPLETE) ? tag2AggrParamAst.get(tag).size()
                  : ((tag == 0) ? tag2AggrPos.get(tag).size()
                      : tag2AggrParamAst.get(tag).size()));
          for (int j = 0; j < aggrlen; j++) {
            ColumnInfo inputColInfo = groupByInputRowResolver.get("",
                _AGGRPARTTAG_ + tag + "_" + j);

            exprNodeDesc exprNode = new exprNodeColumnDesc(
                inputColInfo.getType(), inputColInfo.getInternalName(), "",
                false);

            tag2AggrParamORValueExpr.get(tag).add(exprNode);
            aggParameters.add(exprNode);
          }

          int posoff = 0;
          for (Integer pos : tag2AggrPos.get(tag)) {
            ASTNode value = aggregationTreesArray.get(pos);
            String aggName = value.getChild(0).getText();

            boolean isDistinct = value.getType() == HiveParser.TOK_FUNCTIONDI;
            GenericUDAFEvaluator.Mode amode = SemanticAnalyzer
                .groupByDescModeToUDAFMode(mode, isDistinct);

            GenericUDAFEvaluator genericUDAFEvaluator = genericUDAFEvaluators
                .get(pos);
            assert (genericUDAFEvaluator != null);

            ArrayList<exprNodeDesc> aggParameters1 = aggParameters;
            if (tag == 0 && mode == Mode.COMPLETE) {
              ArrayList<Integer> offs = nonDistPos2TagOffs.get(pos);
              aggParameters1 = new ArrayList<exprNodeDesc>();
              for (Integer off : offs) {
                aggParameters1.add(aggParameters.get(off));
              }
            } else if (tag == 0 || mode == Mode.FINAL) {
              aggParameters1 = new ArrayList<exprNodeDesc>();
              aggParameters1.add(aggParameters.get(posoff));
            }

            GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo(
                genericUDAFEvaluator, amode, aggParameters1);

            aggregations.set(pos, new aggregationDesc(aggName.toLowerCase(),
                udaf.genericUDAFEvaluator, udaf.convertedParameters,
                isDistinct, amode));

            ColumnInfo valueColInfo = new ColumnInfo(pos2colname.get(pos),
                udaf.returnType, "", false);
            pos2valueInfo.put(pos, valueColInfo);

            posoff++;

          }
        }

        for (int pos = 0; pos < aggregationTreesArray.size(); pos++) {
          groupByOutputRowResolver.putExpression(
              aggregationTreesArray.get(pos), pos2valueInfo.get(pos));
          operatorRowSchema.getSignature().add(pos2valueInfo.get(pos));
        }
      }

    } else if (mode == Mode.PARTIAL2) {
    }

    GroupByOperator op = (GroupByOperator) putOpInsertMap(
        OperatorFactory.getAndMakeChild(new groupByDesc(mode,
            outputColumnNames, groupByKeys, aggregations, tag2AggrPos,
            tag2AggrParamORValueExpr, aggrPartExpr), operatorRowSchema,
            inputOperatorInfo), groupByOutputRowResolver);
    op.setColumnExprMap(colExprMap);
    return op;
  }

  @SuppressWarnings("unchecked")
  private Operator genNewGroupByPlanReduceSinkOperator(QB qb, String dest,
      Operator inputOperatorInfo, int numReducers, boolean mapAggrDone,
      boolean partitionByGbkeyanddistinctkey, boolean isFirstReduce,
      ArrayList<ArrayList<Integer>> tag2AggrPos,
      ArrayList<ArrayList<ASTNode>> tag2AggrParamAst) throws SemanticException {

    RowResolver reduceSinkInputRowResolver = opParseCtx.get(inputOperatorInfo)
        .getRR();
    QBParseInfo parseInfo = qb.getParseInfo();
    RowResolver reduceSinkOutputRowResolver = new RowResolver();
    reduceSinkOutputRowResolver.setIsExprResolver(true);
    Map<String, exprNodeDesc> colExprMap = new HashMap<String, exprNodeDesc>();

    ArrayList<exprNodeDesc> reduceKeys = new ArrayList<exprNodeDesc>();
    ArrayList<exprNodeDesc> reduceValues = new ArrayList<exprNodeDesc>();

    List<String> outputKeyColumnNames = new ArrayList<String>();
    List<String> outputValueColumnNames = new ArrayList<String>();

    List<ASTNode> grpByExprs = SemanticAnalyzer.getGroupByForClause(parseInfo,
        dest);

    int colid = 0;
    if (qb.getParseInfo().getDestContainsGroupbyCubeOrRollupClause(dest)) {
      String colName = getColumnInternalName(colid++);
      outputKeyColumnNames.add(colName);
      ColumnInfo info = reduceSinkInputRowResolver.get("",
          NewGroupByUtils1._CUBE_ROLLUP_GROUPINGSETS_TAG_);
      exprNodeDesc grpByExprNode = new exprNodeColumnDesc(info.getType(),
          info.getInternalName(), info.getAlias(), info.getIsPartitionCol());
      reduceKeys.add(grpByExprNode);
      String field = Utilities.ReduceField.KEY.toString() + "." + colName;

      reduceSinkOutputRowResolver.put("",
          NewGroupByUtils1._CUBE_ROLLUP_GROUPINGSETS_TAG_, new ColumnInfo(
              field, grpByExprNode.getTypeInfo(), "", false));

      colExprMap.put(colName, grpByExprNode);
    }

    for (int i = 0; i < grpByExprs.size(); i++) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      exprNodeDesc grpByExprNode = SemanticAnalyzer.genExprNodeDesc(grpbyExpr,
          reduceSinkInputRowResolver, qb, -1, conf);
      reduceKeys.add(grpByExprNode);
      String colName = getColumnInternalName(colid++);
      outputKeyColumnNames.add(colName);
      String field = Utilities.ReduceField.KEY.toString() + "." + colName;
      reduceSinkOutputRowResolver.putExpression(grpbyExpr, new ColumnInfo(
          field, grpByExprNode.getTypeInfo(), "", false));
      colExprMap.put(field, grpByExprNode);
    }

    boolean containsfunctions = tag2AggrPos != null && tag2AggrPos.size() > 0;

    exprNodeDesc aggrPartExpr = null;
    ArrayList<ArrayList<exprNodeDesc>> tag2AggrParamORValueExpr = null;
    TypeInfo aggrPartTypeInfo = null;
    if (containsfunctions) {

      String colNameAggrPart = getColumnInternalName(colid++);
      if (isFirstReduce) {
        outputKeyColumnNames.add(colNameAggrPart);
      } else {
        outputValueColumnNames.add(colNameAggrPart);
      }

      if (mapAggrDone) {
        ColumnInfo aggrPartInfo = reduceSinkInputRowResolver.get("",
            _GBY_AGGRPART_OUTPUT_COLNAME_);
        aggrPartExpr = new exprNodeColumnDesc(aggrPartInfo.getType(),
            aggrPartInfo.getInternalName(), "", false);

      } else {

        tag2AggrParamORValueExpr = new ArrayList<ArrayList<exprNodeDesc>>();

        for (int tag = 0; tag < tag2AggrParamAst.size(); tag++) {
          tag2AggrParamORValueExpr.add(new ArrayList<exprNodeDesc>());

          for (int j = 0; j < tag2AggrParamAst.get(tag).size(); j++) {
            ASTNode paraExpr = (ASTNode) tag2AggrParamAst.get(tag).get(j);
            exprNodeDesc exprNode = SemanticAnalyzer.genExprNodeDesc(paraExpr,
                reduceSinkInputRowResolver, qb, -1, conf);
            tag2AggrParamORValueExpr.get(tag).add(exprNode);
          }

        }
      }

      List<TypeInfo> unionTypes = new ArrayList<TypeInfo>();

      for (int tag = 0; tag < tag2AggrPos.size(); tag++) {

        ArrayList<String> names = new ArrayList<String>();
        ArrayList<TypeInfo> typeInfos = new ArrayList<TypeInfo>();

        int paramLen = (tag == 0 && mapAggrDone) ? tag2AggrPos.get(tag).size()
            : (isFirstReduce ? tag2AggrParamAst.get(tag).size() : tag2AggrPos
                .get(tag).size());

        for (int j = 0; j < paramLen; j++) {
          TypeInfo inputTypeInfo = null;
          if (mapAggrDone) {
            inputTypeInfo = reduceSinkInputRowResolver.get("",
                _AGGRPARTTAG_ + tag + "_" + j).getType();
          } else {
            ASTNode paraExpr = (ASTNode) tag2AggrParamAst.get(tag).get(j);
            exprNodeDesc exprNode = SemanticAnalyzer.genExprNodeDesc(paraExpr,
                reduceSinkInputRowResolver, qb, -1, conf);

            inputTypeInfo = exprNode.getTypeInfo();
          }

          String field1 = (isFirstReduce ? Utilities.ReduceField.KEY.toString()
              : Utilities.ReduceField.VALUE.toString())
              + "."
              + colNameAggrPart
              + ":" + tag + "." + getColumnInternalName(j);

          ColumnInfo outColInfo = new ColumnInfo(field1, inputTypeInfo, "",
              false);
          reduceSinkOutputRowResolver.put("", _AGGRPARTTAG_ + tag + "_" + j,
              outColInfo);

          names.add(getColumnInternalName(j));
          typeInfos.add(inputTypeInfo);

        }

        if (names.size() == 0) {
          names.add("nondistnull");
          typeInfos.add(TypeInfoFactory.voidTypeInfo);
        }
        unionTypes.add(TypeInfoFactory.getStructTypeInfo(names, typeInfos));
      }

      aggrPartTypeInfo = TypeInfoFactory.getUnionTypeInfo(unionTypes);

      ColumnInfo outColInfo = new ColumnInfo(
          (isFirstReduce ? Utilities.ReduceField.KEY.toString()
              : Utilities.ReduceField.VALUE.toString()) + "." + colNameAggrPart,
          aggrPartTypeInfo, "", false);
      reduceSinkOutputRowResolver.put("", _GBY_AGGRPART_OUTPUT_COLNAME_,
          outColInfo);
    }

    ReduceSinkOperator rsOp = (ReduceSinkOperator) putOpInsertMap(
        OperatorFactory.getAndMakeChild(PlanUtils.getReduceSinkDesc2(
            reduceKeys, reduceValues, outputKeyColumnNames,
            outputValueColumnNames, -1, numReducers, mapAggrDone,
            isFirstReduce, partitionByGbkeyanddistinctkey, tag2AggrPos,
            aggrPartExpr, tag2AggrParamORValueExpr, aggrPartTypeInfo),
            new RowSchema(reduceSinkOutputRowResolver.getColumnInfos()),
            inputOperatorInfo), reduceSinkOutputRowResolver);
    rsOp.setColumnExprMap(colExprMap);

    return rsOp;
  }

  private <T extends Serializable> Operator<T> putOpInsertMap(Operator<T> op,
      RowResolver rr) {
    OpParseContext ctx = new OpParseContext(rr);
    opParseCtx.put(op, ctx);
    return op;
  }

  private String getColumnInternalName(int pos) {
    return HiveConf.getColumnInternalName(pos);
  }

  private boolean optimizeMapAggrGroupBy(String dest, QB qb) {
    List<ASTNode> grpByExprs = SemanticAnalyzer.getGroupByForClause(
        qb.getParseInfo(), dest);
    if ((grpByExprs != null) && !grpByExprs.isEmpty())
      return false;

    if (!qb.getParseInfo().getDistinctFuncExprsForClause(dest).isEmpty())
      return false;

    return true;
  }

}
