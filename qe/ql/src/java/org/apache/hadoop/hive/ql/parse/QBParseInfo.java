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

package org.apache.hadoop.hive.ql.parse;

import java.util.*;

import org.antlr.runtime.tree.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class QBParseInfo {

  private boolean isSubQ;
  private String alias;
  private ASTNode joinExpr;
  private ASTNode hints;
  private ASTNode noColsPartTree = null;
  private HashMap<String, ASTNode> aliasToSrc;
  private HashMap<String, ASTNode> nameToDest;
  private HashMap<String, Boolean> nameToDestOverwrites;
  private HashMap<String, TableSample> nameToSample;

  private final Map<ASTNode, String> exprToColumnAlias;
  private HashMap<String, ASTNode> destToSelExpr;
  private HashMap<String, ASTNode> destToWhereExpr;
  private HashMap<String, ASTNode> destToGroupby;
  private HashMap<String, Boolean> destContainsGroupbyCubeOrRollup;
  private HashMap<String, ArrayList<ASTNode>> destToGroupbyNodes;
  private final Map<String, ASTNode> destToHaving;

  private HashMap<String, ASTNode> destToClusterby;

  private HashMap<String, ASTNode> destToDistributeby;

  private HashMap<String, ASTNode> destToSortby;

  private HashMap<String, ArrayList<ASTNode>> aliasToLateralViews;

  /* Order by clause */
  private HashMap<String, ASTNode> destToOrderby;
  private HashMap<String, Integer> destToLimit;
  private int outerQueryLimit;

  private LinkedHashMap<String, LinkedHashMap<String, ASTNode>> destToAggregationExprs;
  private final HashMap<String, List<ASTNode>> destToDistinctFuncExprs;

  private LinkedHashMap<String, LinkedHashMap<String, ASTNode>> destToAnalysisExprs;

  private HashMap<String, List<ASTNode>> destToDistinctFuncOverExpr;

  private HashMap<String, ASTNode> destToValues;
  private HashMap<String, LinkedHashMap<String, ASTNode>> destToGroupingExprs;

  private HashMap<String, Boolean> destToHasAggr;
  private HashMap<String, Boolean> destToHasAna;

  private ASTNode withNode;

  private boolean isSelectQuery = false;
  private boolean hasFromClause = false;

  public boolean getIsSelectQuery() {
    return isSelectQuery;
  }

  public void setIsSelectQuery(boolean is) {
    isSelectQuery = is;
  }

  public boolean getHasFromClause() {
    return hasFromClause;
  }

  public void setHasFromClause(boolean is) {
    hasFromClause = is;
  }

  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(QBParseInfo.class.getName());

  public QBParseInfo(String alias, boolean isSubQ) {
    this.aliasToSrc = new HashMap<String, ASTNode>();
    this.nameToDest = new HashMap<String, ASTNode>();
    this.nameToDestOverwrites = new HashMap<String, Boolean>();
    this.nameToSample = new HashMap<String, TableSample>();
    this.exprToColumnAlias = new HashMap<ASTNode, String>();
    this.destToSelExpr = new HashMap<String, ASTNode>();
    this.destToWhereExpr = new HashMap<String, ASTNode>();
    this.destToGroupby = new HashMap<String, ASTNode>();
    this.destToHaving = new HashMap<String, ASTNode>();
    this.destToClusterby = new HashMap<String, ASTNode>();
    this.destToDistributeby = new HashMap<String, ASTNode>();
    this.destToSortby = new HashMap<String, ASTNode>();
    this.destToOrderby = new HashMap<String, ASTNode>();
    this.destToLimit = new HashMap<String, Integer>();

    this.destToAggregationExprs = new LinkedHashMap<String, LinkedHashMap<String, ASTNode>>();
    this.destToDistinctFuncExprs = new HashMap<String, List<ASTNode>>();

    this.destContainsGroupbyCubeOrRollup = new HashMap<String, Boolean>();
    this.destToGroupbyNodes = new HashMap<String, ArrayList<ASTNode>>();
    this.destToGroupingExprs = new HashMap<String, LinkedHashMap<String, ASTNode>>();

    this.destToAnalysisExprs = new LinkedHashMap<String, LinkedHashMap<String, ASTNode>>();
    this.destToDistinctFuncOverExpr = new HashMap<String, List<ASTNode>>();

    this.destToValues = new HashMap<String, ASTNode>();

    this.alias = alias;
    this.isSubQ = isSubQ;
    this.outerQueryLimit = -1;
    this.aliasToLateralViews = new HashMap<String, ArrayList<ASTNode>>();

    this.destToHasAggr = new HashMap<String, Boolean>();
    this.destToHasAna = new HashMap<String, Boolean>();
  }

  public void addHasAggrForClause(String clause, boolean is) {
    destToHasAggr.put(clause, is);
  }

  public void addHasAnaForClause(String clause, boolean is) {
    destToHasAna.put(clause, is);
  }

  public Boolean getHasAggrForClause(String clause) {
    return destToHasAggr.get(clause);
  }

  public Boolean getHasAnaForClause(String clause) {
    return destToHasAna.get(clause);
  }

  public void addAggregationExprsForClause(String clause,
      LinkedHashMap<String, ASTNode> aggregationTrees) {
    if (destToAggregationExprs.containsKey(clause)) {
      destToAggregationExprs.get(clause).putAll(aggregationTrees);
    } else {
      destToAggregationExprs.put(clause, aggregationTrees);
    }
  }

  public void setAggregationExprsForClause(String clause,
      LinkedHashMap<String, ASTNode> aggregationTrees) {
    this.destToAggregationExprs.put(clause, aggregationTrees);
  }

  public LinkedHashMap<String, ASTNode> getAggregationExprsForClause(
      String clause) {
    return this.destToAggregationExprs.get(clause);
  }

  public void setAnalysisExprsForClause(String clause,
      LinkedHashMap<String, ASTNode> analysisTrees) {
    this.destToAnalysisExprs.put(clause, analysisTrees);
  }

  public HashMap<String, ASTNode> getAnalysisExprsForClause(String clause) {
    return this.destToAnalysisExprs.get(clause);
  }

  public void setDistinctFuncOverExprForClause(String clause, List<ASTNode> ast) {
    this.destToDistinctFuncOverExpr.put(clause, ast);
  }

  public List<ASTNode> getDistinctFuncOverExprForClause(String clause) {
    return this.destToDistinctFuncOverExpr.get(clause);
  }

  public void setDistinctFuncExprsForClause(String clause, List<ASTNode> ast) {
    destToDistinctFuncExprs.put(clause, ast);
  }

  public List<ASTNode> getDistinctFuncExprsForClause(String clause) {
    return destToDistinctFuncExprs.get(clause);
  }

  public void setSelExprForClause(String clause, ASTNode ast) {
    this.destToSelExpr.put(clause, ast);
  }

  public void setWhrExprForClause(String clause, ASTNode ast) {
    this.destToWhereExpr.put(clause, ast);
  }

  public void setHavingExprForClause(String clause, ASTNode ast) {
    this.destToHaving.put(clause, ast);
  }

  public void setGroupByExprForClause(String clause, ASTNode ast) {
    this.destToGroupby.put(clause, ast);
    this.destContainsGroupbyCubeOrRollup.put(clause, false);
    if (ast != null) {
      for (int i = 0; i < ast.getChildCount(); ++i) {
        ASTNode grpbyExpr = (ASTNode) ast.getChild(i);
        if (grpbyExpr.getType() == HiveParser.TOK_CUBE
            || grpbyExpr.getType() == HiveParser.TOK_ROLLUP
            || grpbyExpr.getType() == HiveParser.TOK_GROUPINGSETS) {
          this.destContainsGroupbyCubeOrRollup.put(clause, true);
          break;
        }
      }
    }
  }

  public void setDestForClause(String clause, ASTNode ast, boolean overwrite) {
    this.nameToDest.put(clause, ast);
    this.nameToDestOverwrites.put(clause, overwrite);
  }

  public void setClusterByExprForClause(String clause, ASTNode ast) {
    this.destToClusterby.put(clause, ast);
  }

  public void setDistributeByExprForClause(String clause, ASTNode ast) {
    this.destToDistributeby.put(clause, ast);
  }

  public void setSortByExprForClause(String clause, ASTNode ast) {
    this.destToSortby.put(clause, ast);
  }

  public void setOrderByExprForClause(String clause, ASTNode ast) {
    this.destToOrderby.put(clause, ast);
  }

  public void setSrcForAlias(String alias, ASTNode ast) {
    this.aliasToSrc.put(alias.toLowerCase(), ast);
  }

  public Set<String> getClauseNames() {
    return this.destToSelExpr.keySet();
  }

  public Set<String> getClauseNamesForDest() {
    return this.nameToDest.keySet();
  }

  public ASTNode getDestForClause(String clause) {
    return this.nameToDest.get(clause);
  }

  public Boolean getDestOverwrittenForClause(String clause) {
    return this.nameToDestOverwrites.get(clause);
  }

  public ASTNode getWhrForClause(String clause) {
    return this.destToWhereExpr.get(clause);
  }

  public HashMap<String, ASTNode> getDestToWhereExpr() {
    return destToWhereExpr;
  }

  public ASTNode getGroupByForClause(String clause) {
    return this.destToGroupby.get(clause);
  }

  public HashMap<String, ASTNode> getDestToGroupBy() {
    return this.destToGroupby;
  }

  public ASTNode getHavingForClause(String clause) {
    return destToHaving.get(clause);
  }

  public Map<String, ASTNode> getDestToHaving() {
    return destToHaving;
  }

  public ASTNode getSelForClause(String clause) {
    return this.destToSelExpr.get(clause);
  }

  public ASTNode getClusterByForClause(String clause) {
    return this.destToClusterby.get(clause);
  }

  public HashMap<String, ASTNode> getDestToClusterBy() {
    return destToClusterby;
  }

  public ASTNode getDistributeByForClause(String clause) {
    return this.destToDistributeby.get(clause);
  }

  public HashMap<String, ASTNode> getDestToDistributeBy() {
    return destToDistributeby;
  }

  public ASTNode getSortByForClause(String clause) {
    return this.destToSortby.get(clause);
  }

  public ASTNode getOrderByForClause(String clause) {
    return this.destToOrderby.get(clause);
  }

  public HashMap<String, ASTNode> getDestToSortBy() {
    return destToSortby;
  }

  public HashMap<String, ASTNode> getDestToOrderBy() {
    return destToOrderby;
  }

  public ASTNode getSrcForAlias(String alias) {
    return this.aliasToSrc.get(alias.toLowerCase());
  }

  public String getAlias() {
    return this.alias;
  }

  public boolean getIsSubQ() {
    return this.isSubQ;
  }

  public ASTNode getJoinExpr() {
    return this.joinExpr;
  }

  public void setJoinExpr(ASTNode joinExpr) {
    this.joinExpr = joinExpr;
  }

  public TableSample getTabSample(String alias) {
    return this.nameToSample.get(alias.toLowerCase());
  }

  public void setTabSample(String alias, TableSample tableSample) {
    this.nameToSample.put(alias.toLowerCase(), tableSample);
  }

  public String getExprToColumnAlias(ASTNode expr) {
    return exprToColumnAlias.get(expr);
  }

  public Map<ASTNode, String> getAllExprToColumnAlias() {
    return exprToColumnAlias;
  }

  public boolean hasExprToColumnAlias(ASTNode expr) {
    return exprToColumnAlias.containsKey(expr);
  }

  public void setExprToColumnAlias(ASTNode expr, String alias) {
    exprToColumnAlias.put(expr, alias);
  }

  public void setDestLimit(String dest, Integer limit) {
    this.destToLimit.put(dest, limit);
  }

  public Integer getDestLimit(String dest) {
    return this.destToLimit.get(dest);
  }

  public void setDestToValues(String clause, ASTNode valuesTree) {
    this.destToValues.put(clause, valuesTree);
  }

  public ASTNode getDestToValues(String clause) {
    return this.destToValues.get(clause);
  }

  public int getOuterQueryLimit() {
    return outerQueryLimit;
  }

  public void setOuterQueryLimit(int outerQueryLimit) {
    this.outerQueryLimit = outerQueryLimit;
  }

  public boolean isSelectStarQuery() {
    if (isSubQ || (joinExpr != null) || (!nameToSample.isEmpty())
        || (!destToGroupby.isEmpty()) || (!destToClusterby.isEmpty())
        || (!aliasToLateralViews.isEmpty()))
      return false;

    Iterator<Map.Entry<String, LinkedHashMap<String, ASTNode>>> aggrIter = destToAggregationExprs
        .entrySet().iterator();
    while (aggrIter.hasNext()) {
      HashMap<String, ASTNode> h = aggrIter.next().getValue();
      if ((h != null) && (!h.isEmpty()))
        return false;
    }

    Iterator<Map.Entry<String, LinkedHashMap<String, ASTNode>>> anaIter = destToAnalysisExprs
        .entrySet().iterator();
    while (anaIter.hasNext()) {
      HashMap<String, ASTNode> h = anaIter.next().getValue();
      if ((h != null) && (!h.isEmpty()))
        return false;
    }

    if (!destToDistinctFuncExprs.isEmpty()) {
      for (List<ASTNode> distn : destToDistinctFuncExprs.values()) {
        if (distn != null && distn.size() > 0)
          return false;
      }
    }
    Iterator<Map.Entry<String, ASTNode>> iter = nameToDest.entrySet()
        .iterator();
    while (iter.hasNext()) {
      Map.Entry<String, ASTNode> entry = iter.next();
      ASTNode v = entry.getValue();
      if (!(((ASTNode) v.getChild(0)).getToken().getType() == HiveParser.TOK_TMP_FILE))
        return false;
    }

    iter = destToSelExpr.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<String, ASTNode> entry = iter.next();
      ASTNode selExprList = entry.getValue();
      for (int i = 0; i < selExprList.getChildCount(); ++i) {

        ASTNode selExpr = (ASTNode) selExprList.getChild(i);
        ASTNode sel = (ASTNode) selExpr.getChild(0);

        if (sel.getToken().getType() != HiveParser.TOK_ALLCOLREF)
          return false;
      }
    }
    return true;
  }

  public Map<String, ArrayList<ASTNode>> getAliasToLateralViews() {
    return this.aliasToLateralViews;
  }

  public List<ASTNode> getLateralViewsForAlias(String alias) {
    return aliasToLateralViews.get(alias.toLowerCase());
  }

  public void addLateralViewForAlias(String alias, ASTNode lateralView) {
    String lowerAlias = alias.toLowerCase();
    ArrayList<ASTNode> lateralViews = aliasToLateralViews.get(lowerAlias);
    if (lateralViews == null) {
      lateralViews = new ArrayList<ASTNode>();
      aliasToLateralViews.put(alias, lateralViews);
    }
    lateralViews.add(lateralView);
  }

  public boolean isFitToOp() {
    if (isSubQ || (joinExpr != null) || (!nameToSample.isEmpty())
        || (!destToGroupby.isEmpty()) || (!destToClusterby.isEmpty())
        || (!aliasToLateralViews.isEmpty()))
      return false;

    Iterator<Map.Entry<String, LinkedHashMap<String, ASTNode>>> aggrIter = destToAggregationExprs
        .entrySet().iterator();
    while (aggrIter.hasNext()) {
      HashMap<String, ASTNode> h = aggrIter.next().getValue();
      if ((h != null) && (!h.isEmpty()))
        return false;
    }

    Iterator<Map.Entry<String, LinkedHashMap<String, ASTNode>>> anaIter = destToAnalysisExprs
        .entrySet().iterator();
    while (anaIter.hasNext()) {
      HashMap<String, ASTNode> h = anaIter.next().getValue();
      if ((h != null) && (!h.isEmpty()))
        return false;
    }

    if (!destToDistinctFuncExprs.isEmpty()) {
      Iterator<List<ASTNode>> distn = destToDistinctFuncExprs.values()
          .iterator();
      while (distn.hasNext()) {
        List<ASTNode> ct = distn.next();
        if (ct != null && ct.size() > 0)
          return false;
      }
    }

    Iterator<Map.Entry<String, ASTNode>> iter = nameToDest.entrySet()
        .iterator();
    while (iter.hasNext()) {
      Map.Entry<String, ASTNode> entry = iter.next();
      ASTNode v = entry.getValue();
      if (!(((ASTNode) v.getChild(0)).getToken().getType() == HiveParser.TOK_TMP_FILE))
        return false;
    }

    return true;
  }

  public void setHints(ASTNode hint) {
    this.hints = hint;
  }

  public ASTNode getHints() {
    return hints;
  }

  public HashMap<String, Boolean> getDestContainsGroupbyCubeOrRollup() {
    return destContainsGroupbyCubeOrRollup;
  }

  public boolean getDestContainsGroupbyCubeOrRollupClause(String dest) {
    if (!destContainsGroupbyCubeOrRollup.containsKey(dest)) {
      return false;
    }
    return destContainsGroupbyCubeOrRollup.get(dest);
  }

  public void setDestToGroupbyNodes(String dest, ArrayList<ASTNode> nodes) {
    this.destToGroupbyNodes.put(dest, nodes);
  }

  public ArrayList<ASTNode> getDestToGroupbyNodes(String dest) {
    if (destToGroupbyNodes.get(dest) == null) {
      ArrayList<ASTNode> nodes = analysis(this.getDestToGroupBy().get(dest));
      this.setDestToGroupbyNodes(dest, nodes);
    }
    return destToGroupbyNodes.get(dest);
  }

  private ArrayList<ASTNode> analysis(ASTNode astNode) {
    ArrayList<ASTNode> nodes = new ArrayList<ASTNode>();
    for (int ii = 0; ii < astNode.getChildCount(); ii++) {
      ASTNode grpbyExpr = (ASTNode) astNode.getChild(ii);
      if (grpbyExpr.getType() == HiveParser.TOK_CUBE
          || grpbyExpr.getType() == HiveParser.TOK_ROLLUP
          || grpbyExpr.getType() == HiveParser.TOK_GROUPINGSETS) {
        for (int i = 0; i < grpbyExpr.getChildCount(); i++) {
          ASTNode child = (ASTNode) grpbyExpr.getChild(i);
          if (child.getType() == HiveParser.TOK_GROUP) {
            for (int j = 0; j < child.getChildCount(); j++) {
              nodes.add((ASTNode) child.getChild(j));
            }
          } else {
            nodes.add(child);
          }
        }
      } else {
        nodes.add(grpbyExpr);
      }
    }
    return nodes;
  }

  public void setGroupingExprsForClause(String dest,
      LinkedHashMap<String, ASTNode> groupings) {
    this.destToGroupingExprs.put(dest, groupings);
  }

  public LinkedHashMap<String, ASTNode> getGroupingExprsForClause(String dest) {
    return this.destToGroupingExprs.get(dest);
  }

  public void setWithExpr(ASTNode withNode) {
    this.withNode = withNode;
  }

  public ASTNode getWithExpr() {
    return this.withNode;
  }

  public ASTNode getNoColsPartTree() {
    return noColsPartTree;
  }

  public void setNoColsPartTree(ASTNode noColsPartTree) {
    this.noColsPartTree = noColsPartTree;
  }
}
