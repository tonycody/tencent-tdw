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

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.loadFileDesc;
import org.apache.hadoop.hive.ql.plan.loadTableDesc;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.TablePartition;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcContext;

public class ParseContext {
  private QB qb;
  private ASTNode ast;
  private HashMap<String, ASTPartitionPruner> aliasToPruner;
  private HashMap<TableScanOperator, exprNodeDesc> opToPartPruner;
  private HashMap<String, SamplePruner> aliasToSamplePruner;
  private LinkedHashMap<String, Operator<? extends Serializable>> topOps;
  private HashMap<String, Operator<? extends Serializable>> topSelOps;
  private LinkedHashMap<Operator<? extends Serializable>, OpParseContext> opParseCtx;
  private Map<JoinOperator, QBJoinTree> joinContext;
  private Map<MapJoinOperator, QBJoinTree> mapJoinContext;
  private HashMap<TableScanOperator, TablePartition> topToTable;
  private List<loadTableDesc> loadTableWork;
  private List<loadFileDesc> loadFileWork;
  private Context ctx;
  private HiveConf conf;
  private HashMap<String, String> idToTableNameMap;
  private int destTableId;
  private UnionProcContext uCtx;
  private List<MapJoinOperator> listMapJoinOpsNoReducer;

  private boolean hasNonPartCols;

  public ParseContext() {
  }

  public ParseContext(
      HiveConf conf,
      QB qb,
      ASTNode ast,
      HashMap<String, ASTPartitionPruner> aliasToPruner,
      HashMap<TableScanOperator, exprNodeDesc> opToPartPruner,
      HashMap<String, SamplePruner> aliasToSamplePruner,
      LinkedHashMap<String, Operator<? extends Serializable>> topOps,
      HashMap<String, Operator<? extends Serializable>> topSelOps,
      LinkedHashMap<Operator<? extends Serializable>, OpParseContext> opParseCtx,
      Map<JoinOperator, QBJoinTree> joinContext,
      HashMap<TableScanOperator, TablePartition> topToTable,
      List<loadTableDesc> loadTableWork, List<loadFileDesc> loadFileWork,
      Context ctx, HashMap<String, String> idToTableNameMap, int destTableId,
      UnionProcContext uCtx, List<MapJoinOperator> listMapJoinOpsNoReducer) {
    this.conf = conf;
    this.qb = qb;
    this.ast = ast;
    this.aliasToPruner = aliasToPruner;
    this.opToPartPruner = opToPartPruner;
    this.aliasToSamplePruner = aliasToSamplePruner;
    this.joinContext = joinContext;
    this.topToTable = topToTable;
    this.loadFileWork = loadFileWork;
    this.loadTableWork = loadTableWork;
    this.opParseCtx = opParseCtx;
    this.topOps = topOps;
    this.topSelOps = topSelOps;
    this.ctx = ctx;
    this.idToTableNameMap = idToTableNameMap;
    this.destTableId = destTableId;
    this.uCtx = uCtx;
    this.listMapJoinOpsNoReducer = listMapJoinOpsNoReducer;
    this.hasNonPartCols = false;
  }

  public QB getQB() {
    return qb;
  }

  public void setQB(QB qb) {
    this.qb = qb;
  }

  public Context getContext() {
    return ctx;
  }

  public void setContext(Context ctx) {
    this.ctx = ctx;
  }

  public HiveConf getConf() {
    return conf;
  }

  public void setConf(HiveConf conf) {
    this.conf = conf;
  }

  public ASTNode getParseTree() {
    return ast;
  }

  public void setParseTree(ASTNode ast) {
    this.ast = ast;
  }

  public HashMap<String, ASTPartitionPruner> getAliasToPruner() {
    return aliasToPruner;
  }

  public void setAliasToPruner(HashMap<String, ASTPartitionPruner> aliasToPruner) {
    this.aliasToPruner = aliasToPruner;
  }

  public HashMap<TableScanOperator, exprNodeDesc> getOpToPartPruner() {
    return opToPartPruner;
  }

  public void setOpToPartPruner(
      HashMap<TableScanOperator, exprNodeDesc> opToPartPruner) {
    this.opToPartPruner = opToPartPruner;
  }

  public HashMap<TableScanOperator, TablePartition> getTopToTable() {
    return topToTable;
  }

  public void setTopToTable(
      HashMap<TableScanOperator, TablePartition> topToTable) {
    this.topToTable = topToTable;
  }

  public HashMap<String, SamplePruner> getAliasToSamplePruner() {
    return aliasToSamplePruner;
  }

  public void setAliasToSamplePruner(
      HashMap<String, SamplePruner> aliasToSamplePruner) {
    this.aliasToSamplePruner = aliasToSamplePruner;
  }

  public LinkedHashMap<String, Operator<? extends Serializable>> getTopOps() {
    return topOps;
  }

  public void setTopOps(
      LinkedHashMap<String, Operator<? extends Serializable>> topOps) {
    this.topOps = topOps;
  }

  public HashMap<String, Operator<? extends Serializable>> getTopSelOps() {
    return topSelOps;
  }

  public void setTopSelOps(
      HashMap<String, Operator<? extends Serializable>> topSelOps) {
    this.topSelOps = topSelOps;
  }

  public LinkedHashMap<Operator<? extends Serializable>, OpParseContext> getOpParseCtx() {
    return opParseCtx;
  }

  public void setOpParseCtx(
      LinkedHashMap<Operator<? extends Serializable>, OpParseContext> opParseCtx) {
    this.opParseCtx = opParseCtx;
  }

  public List<loadTableDesc> getLoadTableWork() {
    return loadTableWork;
  }

  public void setLoadTableWork(List<loadTableDesc> loadTableWork) {
    this.loadTableWork = loadTableWork;
  }

  public List<loadFileDesc> getLoadFileWork() {
    return loadFileWork;
  }

  public void setLoadFileWork(List<loadFileDesc> loadFileWork) {
    this.loadFileWork = loadFileWork;
  }

  public HashMap<String, String> getIdToTableNameMap() {
    return idToTableNameMap;
  }

  public void setIdToTableNameMap(HashMap<String, String> idToTableNameMap) {
    this.idToTableNameMap = idToTableNameMap;
  }

  public int getDestTableId() {
    return destTableId;
  }

  public void setDestTableId(int destTableId) {
    this.destTableId = destTableId;
  }

  public UnionProcContext getUCtx() {
    return uCtx;
  }

  public void setUCtx(UnionProcContext uCtx) {
    this.uCtx = uCtx;
  }

  public Map<JoinOperator, QBJoinTree> getJoinContext() {
    return joinContext;
  }

  public void setJoinContext(Map<JoinOperator, QBJoinTree> joinContext) {
    this.joinContext = joinContext;
  }

  public List<MapJoinOperator> getListMapJoinOpsNoReducer() {
    return listMapJoinOpsNoReducer;
  }

  public void setListMapJoinOpsNoReducer(
      List<MapJoinOperator> listMapJoinOpsNoReducer) {
    this.listMapJoinOpsNoReducer = listMapJoinOpsNoReducer;
  }

  public void setHasNonPartCols(boolean val) {
    this.hasNonPartCols = val;
  }

  public boolean getHasNonPartCols() {
    return this.hasNonPartCols;
  }

  public Map<MapJoinOperator, QBJoinTree> getMapJoinContext() {
    return mapJoinContext;
  }

  public void setMapJoinContext(Map<MapJoinOperator, QBJoinTree> mapJoinContext) {
    this.mapJoinContext = mapJoinContext;
  }
}
