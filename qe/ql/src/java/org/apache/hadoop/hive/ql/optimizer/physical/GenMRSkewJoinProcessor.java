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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MultiHdfsInfo;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ConditionalResolverSkewJoin;
import org.apache.hadoop.hive.ql.plan.ConditionalWork;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.fetchWork;
import org.apache.hadoop.hive.ql.plan.joinDesc;
import org.apache.hadoop.hive.ql.plan.mapJoinDesc;
import org.apache.hadoop.hive.ql.plan.mapredLocalWork;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.plan.partitionDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.ql.plan.tableScanDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class GenMRSkewJoinProcessor {

  public GenMRSkewJoinProcessor() {
  }

  public static void processSkewJoin(JoinOperator joinOp,
      Task<? extends Serializable> currTask, ParseContext parseCtx,
      MultiHdfsInfo multiHdfsInfo) throws SemanticException {

    if (!GenMRSkewJoinProcessor.skewJoinEnabled(parseCtx.getConf(), joinOp))
      return;

    String baseTmpDir = null;
    if (!multiHdfsInfo.isMultiHdfsEnable()) {
      baseTmpDir = parseCtx.getContext().getMRTmpFileURI();
    } else {
      baseTmpDir = parseCtx.getContext().getExternalTmpFileURI(
          new Path(multiHdfsInfo.getTmpHdfsScheme()).toUri());
    }

    joinDesc joinDescriptor = joinOp.getConf();
    Map<Byte, List<exprNodeDesc>> joinValues = joinDescriptor.getExprs();
    int numAliases = joinValues.size();

    Map<Byte, String> bigKeysDirMap = new HashMap<Byte, String>();
    Map<Byte, Map<Byte, String>> smallKeysDirMap = new HashMap<Byte, Map<Byte, String>>();
    Map<Byte, String> skewJoinJobResultsDir = new HashMap<Byte, String>();
    Byte[] tags = joinDescriptor.getTagOrder();
    for (int i = 0; i < numAliases; i++) {
      Byte alias = tags[i];
      String bigKeysDir = getBigKeysDir(baseTmpDir, alias);
      bigKeysDirMap.put(alias, bigKeysDir);
      Map<Byte, String> smallKeysMap = new HashMap<Byte, String>();
      smallKeysDirMap.put(alias, smallKeysMap);
      for (Byte src2 : tags) {
        if (!src2.equals(alias)) {
          String smallKeysDir = getSmallKeysDir(baseTmpDir, alias, src2);
          smallKeysMap.put(src2, smallKeysDir);
        }
      }

      String BigKeysSkewJoinResultDir = getBigKeysSkewJoinResultDir(baseTmpDir,
          alias);
      skewJoinJobResultsDir.put(alias, BigKeysSkewJoinResultDir);
    }

    joinDescriptor.setHandleSkewJoin(true);
    joinDescriptor.setBigKeysDirMap(bigKeysDirMap);
    joinDescriptor.setSmallKeysDirMap(smallKeysDirMap);
    joinDescriptor.setSkewKeyDefinition(HiveConf.getIntVar(parseCtx.getConf(),
        HiveConf.ConfVars.HIVESKEWJOINKEY));

    Map<String, Task<? extends Serializable>> bigKeysDirToTaskMap = new HashMap<String, Task<? extends Serializable>>();
    List<Serializable> listWorks = new ArrayList<Serializable>();
    List<Task<? extends Serializable>> listTasks = new ArrayList<Task<? extends Serializable>>();
    mapredWork currPlan = (mapredWork) currTask.getWork();

    tableDesc keyTblDesc = (tableDesc) currPlan.getKeyDesc().clone();
    List<String> joinKeys = Utilities
        .getColumnNames(keyTblDesc.getProperties());
    List<String> joinKeyTypes = Utilities.getColumnTypes(keyTblDesc
        .getProperties());

    Map<Byte, tableDesc> tableDescList = new HashMap<Byte, tableDesc>();
    Map<Byte, List<exprNodeDesc>> newJoinValues = new HashMap<Byte, List<exprNodeDesc>>();
    Map<Byte, List<exprNodeDesc>> newJoinKeys = new HashMap<Byte, List<exprNodeDesc>>();
    List<tableDesc> newJoinValueTblDesc = new ArrayList<tableDesc>();

    for (int i = 0; i < tags.length; i++)
      newJoinValueTblDesc.add(null);

    for (int i = 0; i < numAliases; i++) {
      Byte alias = tags[i];
      List<exprNodeDesc> valueCols = joinValues.get(alias);
      String colNames = "";
      String colTypes = "";
      int columnSize = valueCols.size();
      List<exprNodeDesc> newValueExpr = new ArrayList<exprNodeDesc>();
      List<exprNodeDesc> newKeyExpr = new ArrayList<exprNodeDesc>();

      boolean first = true;
      for (int k = 0; k < columnSize; k++) {
        TypeInfo type = valueCols.get(k).getTypeInfo();
        String newColName = i + "_VALUE_" + k;
        newValueExpr
            .add(new exprNodeColumnDesc(type, newColName, "" + i, false));
        if (!first) {
          colNames = colNames + ",";
          colTypes = colTypes + ",";
        }
        first = false;
        colNames = colNames + newColName;
        colTypes = colTypes + valueCols.get(k).getTypeString();
      }

      for (int k = 0; k < joinKeys.size(); k++) {
        if (!first) {
          colNames = colNames + ",";
          colTypes = colTypes + ",";
        }
        first = false;
        colNames = colNames + joinKeys.get(k);
        colTypes = colTypes + joinKeyTypes.get(k);
        newKeyExpr.add(new exprNodeColumnDesc(TypeInfoFactory
            .getPrimitiveTypeInfo(joinKeyTypes.get(k)), joinKeys.get(k),
            "" + i, false));
      }

      newJoinValues.put(alias, newValueExpr);
      newJoinKeys.put(alias, newKeyExpr);
      tableDescList.put(alias, Utilities.getTableDesc(colNames, colTypes));

      String valueColNames = "";
      String valueColTypes = "";
      first = true;
      for (int k = 0; k < columnSize; k++) {
        String newColName = i + "_VALUE_" + k;
        if (!first) {
          valueColNames = valueColNames + ",";
          valueColTypes = valueColTypes + ",";
        }
        valueColNames = valueColNames + newColName;
        valueColTypes = valueColTypes + valueCols.get(k).getTypeString();
        first = false;
      }
      newJoinValueTblDesc.set(Byte.valueOf((byte) i),
          Utilities.getTableDesc(valueColNames, valueColTypes));
    }

    joinDescriptor.setSkewKeysValuesTables(tableDescList);
    joinDescriptor.setKeyTableDesc(keyTblDesc);

    for (int i = 0; i < numAliases - 1; i++) {
      Byte src = tags[i];
      mapredWork newPlan = PlanUtils.getMapRedWork();
      mapredWork clonePlan = null;
      try {
        String xmlPlan = currPlan.toXML();
        StringBuffer sb = new StringBuffer(xmlPlan);
        ByteArrayInputStream bis;
        bis = new ByteArrayInputStream(sb.toString().getBytes("UTF-8"));
        clonePlan = Utilities.deserializeMapRedWork(bis, parseCtx.getConf());
      } catch (UnsupportedEncodingException e) {
        throw new SemanticException(e);
      }

      Operator<? extends Serializable>[] parentOps = new TableScanOperator[tags.length];
      for (int k = 0; k < tags.length; k++) {
        Operator<? extends Serializable> ts = OperatorFactory.get(
            tableScanDesc.class, (RowSchema) null);
        parentOps[k] = ts;
      }
      Operator<? extends Serializable> tblScan_op = parentOps[i];

      ArrayList<String> aliases = new ArrayList<String>();
      String alias = src.toString();
      aliases.add(alias);
      String bigKeyDirPath = bigKeysDirMap.get(src);
      newPlan.getPathToAliases().put(bigKeyDirPath, aliases);
      newPlan.getAliasToWork().put(alias, tblScan_op);
      partitionDesc part = new partitionDesc(tableDescList.get(src), null);
      newPlan.getPathToPartitionInfo().put(bigKeyDirPath, part);
      newPlan.getAliasToPartnInfo().put(alias, part);

      Operator<? extends Serializable> reducer = clonePlan.getReducer();
      assert reducer instanceof JoinOperator;
      JoinOperator cloneJoinOp = (JoinOperator) reducer;

      mapJoinDesc mapJoinDescriptor = new mapJoinDesc(newJoinKeys, keyTblDesc,
          newJoinValues, newJoinValueTblDesc,
          joinDescriptor.getOutputColumnNames(), i, joinDescriptor.getConds(),
          joinDescriptor.getFilters(), joinDescriptor.getNoOuterJoin());
      mapJoinDescriptor.setNoOuterJoin(joinDescriptor.isNoOuterJoin());
      mapJoinDescriptor.setTagOrder(tags);
      mapJoinDescriptor.setHandleSkewJoin(false);

      mapredLocalWork localPlan = new mapredLocalWork(
          new LinkedHashMap<String, Operator<? extends Serializable>>(),
          new LinkedHashMap<String, fetchWork>());
      Map<Byte, String> smallTblDirs = smallKeysDirMap.get(src);

      for (int j = 0; j < numAliases; j++) {
        if (j == i)
          continue;
        Byte small_alias = tags[j];
        Operator<? extends Serializable> tblScan_op2 = parentOps[j];
        localPlan.getAliasToWork().put(small_alias.toString(), tblScan_op2);
        Path tblDir = new Path(smallTblDirs.get(small_alias));
        localPlan.getAliasToFetchWork().put(small_alias.toString(),
            new fetchWork(tblDir.toString(), tableDescList.get(small_alias)));
      }

      newPlan.setMapLocalWork(localPlan);

      MapJoinOperator mapJoinOp = (MapJoinOperator) OperatorFactory
          .getAndMakeChild(mapJoinDescriptor, (RowSchema) null, parentOps);
      List<Operator<? extends Serializable>> childOps = cloneJoinOp
          .getChildOperators();
      for (Operator<? extends Serializable> childOp : childOps)
        childOp.replaceParent(cloneJoinOp, mapJoinOp);
      mapJoinOp.setChildOperators(childOps);

      HiveConf jc = new HiveConf(parseCtx.getConf(),
          GenMRSkewJoinProcessor.class);
      HiveConf
          .setVar(jc, HiveConf.ConfVars.HIVEINPUTFORMAT,
              org.apache.hadoop.hive.ql.io.HiveInputFormat.class
                  .getCanonicalName());
      Task<? extends Serializable> skewJoinMapJoinTask = TaskFactory.get(
          newPlan, jc);
      bigKeysDirToTaskMap.put(bigKeyDirPath, skewJoinMapJoinTask);
      listWorks.add(skewJoinMapJoinTask.getWork());
      listTasks.add(skewJoinMapJoinTask);
    }

    ConditionalWork cndWork = new ConditionalWork(listWorks);
    ConditionalTask cndTsk = (ConditionalTask) TaskFactory.get(cndWork,
        parseCtx.getConf());
    cndTsk.setListTasks(listTasks);
    cndTsk.setResolver(new ConditionalResolverSkewJoin());
    cndTsk
        .setResolverCtx(new ConditionalResolverSkewJoin.ConditionalResolverSkewJoinCtx(
            bigKeysDirToTaskMap));
    List<Task<? extends Serializable>> oldChildTasks = currTask.getChildTasks();
    currTask.setChildTasks(new ArrayList<Task<? extends Serializable>>());
    currTask.addDependentTask(cndTsk);

    if (oldChildTasks != null) {
      for (Task<? extends Serializable> tsk : cndTsk.getListTasks())
        for (Task<? extends Serializable> oldChild : oldChildTasks)
          tsk.addDependentTask(oldChild);
    }
    return;
  }

  public static boolean skewJoinEnabled(HiveConf conf, JoinOperator joinOp) {

    if (conf != null && !conf.getBoolVar(HiveConf.ConfVars.HIVESKEWJOIN))
      return false;

    if (!joinOp.getConf().isNoOuterJoin())
      return false;

    byte pos = 0;
    for (Byte tag : joinOp.getConf().getTagOrder()) {
      if (tag != pos)
        return false;
      pos++;
    }

    return true;
  }

  private static String skewJoinPrefix = "hive_skew_join";
  private static String UNDERLINE = "_";
  private static String BIGKEYS = "bigkeys";
  private static String SMALLKEYS = "smallkeys";
  private static String RESULTS = "results";

  static String getBigKeysDir(String baseDir, Byte srcTbl) {
    return baseDir + File.separator + skewJoinPrefix + UNDERLINE + BIGKEYS
        + UNDERLINE + srcTbl;
  }

  static String getBigKeysSkewJoinResultDir(String baseDir, Byte srcTbl) {
    return baseDir + File.separator + skewJoinPrefix + UNDERLINE + BIGKEYS
        + UNDERLINE + RESULTS + UNDERLINE + srcTbl;
  }

  static String getSmallKeysDir(String baseDir, Byte srcTblBigTbl,
      Byte srcTblSmallTbl) {
    return baseDir + File.separator + skewJoinPrefix + UNDERLINE + SMALLKEYS
        + UNDERLINE + srcTblBigTbl + UNDERLINE + srcTblSmallTbl;
  }

}
