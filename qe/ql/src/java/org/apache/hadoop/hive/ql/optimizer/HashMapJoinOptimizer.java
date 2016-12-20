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
package org.apache.hadoop.hive.ql.optimizer;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.mapJoinDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.TablePartition;

public class HashMapJoinOptimizer implements Transform {

  private static final Log LOG = LogFactory.getLog(HashMapJoinOptimizer.class
      .getName());

  public HashMapJoinOptimizer() {
  }

  public ParseContext transform(ParseContext pctx) throws SemanticException {

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    HashMapjoinOptProcCtx hashMapJoinOptimizeCtx = new HashMapjoinOptProcCtx();

    opRules.put(new RuleRegExp("R1", "MAPJOIN%"), getHashMapjoinProc(pctx));

    opRules.put(new RuleRegExp("R2", "RS%.*MAPJOIN"),
        getHashMapjoinRejectProc(pctx));
    opRules.put(new RuleRegExp(new String("R3"), "UNION%.*MAPJOIN%"),
        getHashMapjoinRejectProc(pctx));
    opRules.put(new RuleRegExp(new String("R4"), "MAPJOIN%.*MAPJOIN%"),
        getHashMapjoinRejectProc(pctx));

    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules,
        hashMapJoinOptimizeCtx);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);

    return pctx;
  }

  private NodeProcessor getHashMapjoinRejectProc(ParseContext pctx) {
    return new NodeProcessor() {
      @Override
      public Object process(Node nd, Stack<Node> stack,
          NodeProcessorCtx procCtx, Object... nodeOutputs)
          throws SemanticException {

        MapJoinOperator mapJoinOp = (MapJoinOperator) nd;
        HashMapjoinOptProcCtx context = (HashMapjoinOptProcCtx) procCtx;
        context.listOfRejectedMapjoins.add(mapJoinOp);
        LOG.info("Reject to optimize " + mapJoinOp);

        return null;
      }
    };
  }

  private NodeProcessor getHashMapjoinProc(ParseContext pctx) {

    return new HashMapjoinOptProc(pctx);
  }

  private NodeProcessor getDefaultProc() {
    return new NodeProcessor() {
      @Override
      public Object process(Node nd, Stack<Node> stack,
          NodeProcessorCtx procCtx, Object... nodeOutputs)
          throws SemanticException {

        return null;
      }
    };
  }

  class HashMapjoinOptProc implements NodeProcessor {

    protected ParseContext pGraphContext;

    public HashMapjoinOptProc(ParseContext pGraphContext) {
      super();
      this.pGraphContext = pGraphContext;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      MapJoinOperator mapJoinOp = (MapJoinOperator) nd;
      HashMapjoinOptProcCtx context = (HashMapjoinOptProcCtx) procCtx;

      if (context.getListOfRejectedMapjoins().contains(mapJoinOp))
        return null;

      QBJoinTree joinCxt = this.pGraphContext.getMapJoinContext()
          .get(mapJoinOp);
      if (joinCxt == null)
        return null;

      List<String> joinAliases = new ArrayList<String>();
      String[] srcs = joinCxt.getBaseSrc();
      String[] left = joinCxt.getLeftAliases();
      List<String> mapAlias = joinCxt.getMapAliases();
      String baseBigAlias = null;
      for (String s : left) {
        if (s != null && !joinAliases.contains(s)) {
          joinAliases.add(s);
          if (!mapAlias.contains(s)) {
            baseBigAlias = s;
          }
        }
      }
      for (String s : srcs) {
        if (s != null && !joinAliases.contains(s)) {
          joinAliases.add(s);
          if (!mapAlias.contains(s)) {
            baseBigAlias = s;
          }
        }
      }

      mapJoinDesc mjDecs = mapJoinOp.getConf();

      LinkedHashMap<String, Integer> aliasToHashPartitionNumber = new LinkedHashMap<String, Integer>();
      LinkedHashMap<String, LinkedHashMap<Integer, ArrayList<String>>> aliasToHashParititionPathNames = new LinkedHashMap<String, LinkedHashMap<Integer, ArrayList<String>>>();

      Map<String, Operator<? extends Serializable>> topOps = this.pGraphContext
          .getTopOps();
      Map<TableScanOperator, TablePartition> topToTable = this.pGraphContext
          .getTopToTable();

      List<Integer> hashPartitionNumbers = new ArrayList<Integer>();
      for (int index = 0; index < joinAliases.size(); index++) {
        String alias = joinAliases.get(index);
        TableScanOperator tso = (TableScanOperator) topOps.get(alias);
        TablePartition tbl = topToTable.get(tso);
        if (!tbl.isPartitioned())
          return null;

        PrunedPartitionList prunedParts = null;
        try {
          prunedParts = PartitionPruner.prune(tbl, pGraphContext
              .getOpToPartPruner().get(tso), pGraphContext.getConf(), alias);

        } catch (HiveException e) {
          LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
          throw new SemanticException(e.getMessage(), e);
        }

        LinkedHashMap<Integer, ArrayList<String>> parNumToPaths = new LinkedHashMap<Integer, ArrayList<String>>();

        if (tbl.getTTable().getSubPartition() == null) {
          Table table = tbl.getTTable();
          Partition par = table.getPriPartition();
          if (!par.getParType().equalsIgnoreCase("HASH"))
            return null;
          LOG.info(tbl.getName() + " is partitioned by hash!");

          if (!checkHashColumns(par.getParKey().getName(), mjDecs, index))
            return null;
          LOG.info(tbl.getName() + " is hash map joined on the hashed column!");

          Integer num = new Integer(par.getParSpacesSize());
          aliasToHashPartitionNumber.put(alias, num);

          Set<String> parspaces = par.getParSpaces().keySet();
          int i = 0;
          for (String parspace : parspaces) {
            ArrayList<String> paths = new ArrayList<String>();
            String path = tbl.getDataLocation().toString() + "/" + parspace;
            paths.add(path);
            parNumToPaths.put(i, paths);
            i++;
          }
          aliasToHashParititionPathNames.put(alias, parNumToPaths);

        } else {
          Table table = tbl.getTTable();
          Partition subPar = table.getSubPartition();
          Partition priPar = table.getPriPartition();

          if (!subPar.getParType().equalsIgnoreCase("HASH"))
            return null;

          if (!checkHashColumns(subPar.getParKey().getName(), mjDecs, index))
            return null;

          Integer num = new Integer(subPar.getParSpacesSize());
          aliasToHashPartitionNumber.put(alias, num);

          for (String path : prunedParts.getTargetPartnPaths()) {
            Set<String> parspaces = subPar.getParSpaces().keySet();
            int i = 0;
            for (String parspace : parspaces) {
              if (path.contains(parspace)) {
                ArrayList<String> paths = null;
                if (parNumToPaths.containsKey(i)) {
                  paths = parNumToPaths.get(i);
                } else {
                  paths = new ArrayList<String>();
                  parNumToPaths.put(i, paths);
                }
                paths.add(path);
                break;
              }
              i++;
            }
          }
          aliasToHashParititionPathNames.put(alias, parNumToPaths);
        }
      }

      int hashPartitionNoInBigTbl = aliasToHashPartitionNumber
          .get(baseBigAlias);
      Iterator<Integer> iter = aliasToHashPartitionNumber.values().iterator();
      while (iter.hasNext()) {
        int nxt = iter.next().intValue();
        if (nxt != hashPartitionNoInBigTbl) {
          LOG.info("Not equal in hash partition number!");
          return null;
        }

      }

      mapJoinDesc desc = mapJoinOp.getConf();

      LinkedHashMap<String, LinkedHashMap<String, ArrayList<String>>> aliasHashPathNameMapping = new LinkedHashMap<String, LinkedHashMap<String, ArrayList<String>>>();

      for (int j = 0; j < joinAliases.size(); j++) {
        String alias = joinAliases.get(j);

        if (alias.equals(baseBigAlias))
          continue;

        LinkedHashMap<String, ArrayList<String>> mapping = new LinkedHashMap<String, ArrayList<String>>();
        aliasHashPathNameMapping.put(alias, mapping);
        for (int i = 0; i < hashPartitionNoInBigTbl; i++) {
          if (i < 10)
            mapping.put("Hash_000" + i,
                aliasToHashParititionPathNames.get(alias).get(i));
          else if (i < 100)
            mapping.put("Hash_00" + i, aliasToHashParititionPathNames
                .get(alias).get(i));
          else if (i < 1000)
            mapping.put("Hash_0" + i, aliasToHashParititionPathNames.get(alias)
                .get(i));
          else
            return null;
        }
      }

      desc.setAliasHashPathNameMapping(aliasHashPathNameMapping);
      desc.setBigTableAlias(baseBigAlias);

      return null;
    }

    private boolean checkHashColumns(String hashColumn, mapJoinDesc mjDesc,
        int index) {
      List<exprNodeDesc> keys = mjDesc.getKeys().get((byte) index);
      if (keys == null || hashColumn == null)
        return false;

      List<String> joinCols = new ArrayList<String>();
      List<exprNodeDesc> joinKeys = new ArrayList<exprNodeDesc>();
      joinKeys.addAll(keys);
      while (joinKeys.size() > 0) {
        exprNodeDesc node = joinKeys.remove(0);
        if (node instanceof exprNodeColumnDesc) {
          joinCols.addAll(node.getCols());
        } else if (node instanceof exprNodeGenericFuncDesc) {
          exprNodeGenericFuncDesc udfNode = ((exprNodeGenericFuncDesc) node);
          joinKeys.addAll(0, udfNode.getChildExprs());
        } else if (node instanceof exprNodeGenericFuncDesc) {
          exprNodeGenericFuncDesc exNode = (exprNodeGenericFuncDesc) node;
          joinKeys.addAll(0, exNode.getChildExprs());
        } else {
          return false;
        }
      }

      if (joinCols.size() != 1) {
        return false;
      }
      for (String col : joinCols) {
        if (!hashColumn.equals(col))
          return false;
      }

      return true;
    }

  }

  class HashMapjoinOptProcCtx implements NodeProcessorCtx {
    Set<MapJoinOperator> listOfRejectedMapjoins = new HashSet<MapJoinOperator>();

    public Set<MapJoinOperator> getListOfRejectedMapjoins() {

      return listOfRejectedMapjoins;
    }
  }
}
