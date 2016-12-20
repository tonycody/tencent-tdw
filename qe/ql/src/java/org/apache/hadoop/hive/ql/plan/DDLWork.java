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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.Set;

import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;

public class DDLWork implements Serializable {
  private static final long serialVersionUID = 1L;
  private createTableDesc createTblDesc;
  private createTableLikeDesc createTblLikeDesc;
  private createViewDesc createVwDesc;
  private dropTableDesc dropTblDesc;
  private truncateTableDesc truncTblDesc;
  private alterTableDesc alterTblDesc;
  private showTablesDesc showTblsDesc;
  private showProcesslistDesc showprosDesc;
  private killqueryDesc kqueryDesc;
  private showqueryDesc squeryDesc;
  private clearqueryDesc clearDesc;
  private showInfoDesc infoDesc;
  private showTableSizeDesc tablesizedesc;
  private showDatabaseSizeDesc dbsizedesc;
  private showRowCountDesc rowcountdesc;
  private showFunctionsDesc showFuncsDesc;
  private descFunctionDesc descFunctionDesc;
  private showPartitionsDesc showPartsDesc;
  private descTableDesc descTblDesc;
  private AddPartitionDesc addPartitionDesc;
  private MsckDesc msckDesc;
  private execExtSQLDesc execsqldesc;
  private showViewTablesDesc viewTablesDesc;
  private addDefaultPartitionDesc addDefaultPartDesc;
  private truncatePartitionDesc truncatePartDesc;
  private alterCommentDesc alterComDesc;

  private createDatabaseDesc createDbDesc;
  private dropDatabaseDesc dropDbDesc;

  private useDatabaseDesc useDbDesc;

  private showDBDesc showDbDesc;

  private showIndexDesc showIndexsDesc;

  private showCreateTableDesc showCT;

  private showVersionDesc versionDesc;

  private Set<ReadEntity> inputs;

  private Set<WriteEntity> outputs;

  public DDLWork() {
  }

  public DDLWork(showVersionDesc versionDesc) {
    super();
    this.versionDesc = versionDesc;
  }

  public showVersionDesc getVersionDesc() {
    return versionDesc;
  }

  public DDLWork(createDatabaseDesc createDbDesc) {
    super();
    this.createDbDesc = createDbDesc;
  }

  public DDLWork(execExtSQLDesc execExtSQLdesc) {
    super();
    this.execsqldesc = execExtSQLdesc;
  }

  public DDLWork(showViewTablesDesc vtdesc) {
    super();
    this.viewTablesDesc = vtdesc;
  }

  public DDLWork(showCreateTableDesc showCT) {
    super();
    this.showCT = showCT;
  }

  public showCreateTableDesc getShowCT() {
    return showCT;
  }

  public void setShowCT(showCreateTableDesc showCT) {
    this.showCT = showCT;
  }

  public DDLWork(showDBDesc desc) {
    super();
    this.showDbDesc = desc;
  }

  public showDBDesc getShowDbDesc() {
    return showDbDesc;
  }

  public void setShowDbDesc(showDBDesc showDbDesc) {
    this.showDbDesc = showDbDesc;
  }

  public useDatabaseDesc getUseDbDesc() {
    return useDbDesc;
  }

  public void setUseDbDesc(useDatabaseDesc useDbDesc) {
    this.useDbDesc = useDbDesc;
  }

  public DDLWork(useDatabaseDesc useDbDesc) {
    super();
    this.useDbDesc = useDbDesc;
  }

  public createDatabaseDesc getCreateDbDesc() {
    return createDbDesc;
  }

  public void setCreateDbDesc(createDatabaseDesc createDbDesc) {
    this.createDbDesc = createDbDesc;
  }

  public dropDatabaseDesc getDropDbDesc() {
    return dropDbDesc;
  }

  public void setDropDbDesc(dropDatabaseDesc dropDbDesc) {
    this.dropDbDesc = dropDbDesc;
  }

  public DDLWork(dropDatabaseDesc dropDbDesc) {
    super();
    this.dropDbDesc = dropDbDesc;
  }

  public DDLWork(alterTableDesc alterTblDesc) {
    this.alterTblDesc = alterTblDesc;
  }

  public DDLWork(truncatePartitionDesc tpd) {
    this.truncatePartDesc = tpd;
  }

  public truncatePartitionDesc getTruncatePartDesc() {
    return truncatePartDesc;
  }

  public void setTruncatePartDesc(truncatePartitionDesc truncatePartDesc) {
    this.truncatePartDesc = truncatePartDesc;
  }

  public DDLWork(addDefaultPartitionDesc adpd) {
    this.addDefaultPartDesc = adpd;
  }

  public DDLWork(createTableDesc createTblDesc) {
    this.createTblDesc = createTblDesc;
  }

  public DDLWork(createTableLikeDesc createTblLikeDesc) {
    this.createTblLikeDesc = createTblLikeDesc;
  }

  public DDLWork(alterCommentDesc acd) {
    this.alterComDesc = acd;
  }

  public alterCommentDesc getAlterCommentDesc() {
    return this.alterComDesc;
  }

  public void setAlterCommentDesc(alterCommentDesc acd) {
    this.alterComDesc = acd;
  }

  public DDLWork(Set<ReadEntity> inputs, Set<WriteEntity> outputs,
      createViewDesc createVwDesc) {
    this(inputs, outputs);

    this.createVwDesc = createVwDesc;
  }

  public execExtSQLDesc getExecsqldesc() {
    return execsqldesc;
  }

  public void setExecsqldesc(execExtSQLDesc execsqldesc) {
    this.execsqldesc = execsqldesc;
  }

  public showViewTablesDesc getViewTablesDesc() {
    return viewTablesDesc;
  }

  public void setViewTablesDesc(showViewTablesDesc viewTablesDesc) {
    this.viewTablesDesc = viewTablesDesc;
  }

  public DDLWork(dropTableDesc dropTblDesc) {
    this.dropTblDesc = dropTblDesc;
  }

  public DDLWork(truncateTableDesc truncTblDesc) {
    this.truncTblDesc = truncTblDesc;
  }

  public DDLWork(descTableDesc descTblDesc) {
    this.descTblDesc = descTblDesc;
  }

  public DDLWork(showTablesDesc showTblsDesc) {
    this.showTblsDesc = showTblsDesc;
  }

  public DDLWork(showProcesslistDesc showprosDesc) {
    this.showprosDesc = showprosDesc;
  }

  public DDLWork(killqueryDesc kq) {
    this.kqueryDesc = new killqueryDesc(kq.getQueryId());
  }

  public DDLWork(showqueryDesc sq) {
    this.squeryDesc = new showqueryDesc(sq.getQueryId(), sq.getResFile());
  }

  public DDLWork(clearqueryDesc cq) {
    this.clearDesc = cq;
  }

  public DDLWork(showInfoDesc info) {
    this.infoDesc = info;
  }

  public DDLWork(showTableSizeDesc ssize) {
    this.tablesizedesc = ssize;
  }

  public DDLWork(showDatabaseSizeDesc dbsize) {
    this.dbsizedesc = dbsize;
  }

  public DDLWork(showRowCountDesc rcount) {
    this.rowcountdesc = rcount;
  }

  public DDLWork(showFunctionsDesc showFuncsDesc) {
    this.showFuncsDesc = showFuncsDesc;
  }

  public DDLWork(descFunctionDesc descFuncDesc) {
    this.descFunctionDesc = descFuncDesc;
  }

  public DDLWork(showPartitionsDesc showPartsDesc) {
    this.showPartsDesc = showPartsDesc;
  }

  public DDLWork(AddPartitionDesc addPartitionDesc) {
    this.addPartitionDesc = addPartitionDesc;
  }

  public DDLWork(MsckDesc checkDesc) {
    this.msckDesc = checkDesc;
  }

  @explain(displayName = "Create Table Operator")
  public createTableDesc getCreateTblDesc() {
    return createTblDesc;
  }

  public void setCreateTblDesc(createTableDesc createTblDesc) {
    this.createTblDesc = createTblDesc;
  }

  @explain(displayName = "Create Table Operator")
  public createTableLikeDesc getCreateTblLikeDesc() {
    return createTblLikeDesc;
  }

  public void setCreateTblLikeDesc(createTableLikeDesc createTblLikeDesc) {
    this.createTblLikeDesc = createTblLikeDesc;
  }

  @explain(displayName = "Create View Operator")
  public createViewDesc getCreateViewDesc() {
    return createVwDesc;
  }

  public void setCreateViewDesc(createViewDesc createVwDesc) {
    this.createVwDesc = createVwDesc;
  }

  @explain(displayName = "Drop Table Operator")
  public dropTableDesc getDropTblDesc() {
    return dropTblDesc;
  }

  public void setDropTblDesc(dropTableDesc dropTblDesc) {
    this.dropTblDesc = dropTblDesc;
  }

  @explain(displayName = "Truncate Table Operator")
  public truncateTableDesc getTruncTblDesc() {
    return truncTblDesc;
  }

  public void setTruncTblDesc(truncateTableDesc truncTblDesc) {
    this.truncTblDesc = truncTblDesc;
  }

  @explain(displayName = "Alter Table Operator")
  public alterTableDesc getAlterTblDesc() {
    return alterTblDesc;
  }

  public void setAlterTblDesc(alterTableDesc alterTblDesc) {
    this.alterTblDesc = alterTblDesc;
  }

  @explain(displayName = "Show Table Operator")
  public showTablesDesc getShowTblsDesc() {
    return showTblsDesc;
  }

  public void setShowTblsDesc(showTablesDesc showTblsDesc) {
    this.showTblsDesc = showTblsDesc;
  }

  @explain(displayName = "Show Processlist Operator")
  public showProcesslistDesc getShowProcesslistDesc() {
    return showprosDesc;
  }

  public killqueryDesc getKillQueryDesc() {
    return kqueryDesc;
  }

  public showqueryDesc getShowQueryDesc() {
    return squeryDesc;
  }

  public clearqueryDesc getClearQueryDesc() {
    return clearDesc;
  }

  public showInfoDesc getShowInfoDesc() {
    return infoDesc;
  }

  public showTableSizeDesc getShowTableSizeDesc() {
    return tablesizedesc;
  }

  public showDatabaseSizeDesc getShowDatabaseSizeDesc() {
    return dbsizedesc;
  }

  public showRowCountDesc getShowRowCountDesc() {
    return rowcountdesc;
  }

  public void setShowTblsDesc(showProcesslistDesc showprosDesc) {
    this.showprosDesc = showprosDesc;
  }

  public void setKillQueryDesc(killqueryDesc in) {
    this.kqueryDesc = in;
  }

  public void setShowQueryDesc(showqueryDesc in) {
    this.squeryDesc = in;
  }

  public void setClearQueryDesc(clearqueryDesc in) {
    this.clearDesc = in;
  }

  public void setShowInfoDesc(showInfoDesc ss) {
    this.infoDesc = ss;
  }

  public void setShowTableSizeDesc(showTableSizeDesc ss) {
    this.tablesizedesc = ss;
  }

  public void setShowDatabaseSizeDesc(showDatabaseSizeDesc ss) {
    this.dbsizedesc = ss;
  }

  public void setShowRowCountDesc(showRowCountDesc ssss) {
    this.rowcountdesc = ssss;
  }

  @explain(displayName = "Show Function Operator")
  public showFunctionsDesc getShowFuncsDesc() {
    return showFuncsDesc;
  }

  @explain(displayName = "Show Function Operator")
  public descFunctionDesc getDescFunctionDesc() {
    return descFunctionDesc;
  }

  public void setShowFuncsDesc(showFunctionsDesc showFuncsDesc) {
    this.showFuncsDesc = showFuncsDesc;
  }

  public void setDescFuncDesc(descFunctionDesc descFuncDesc) {
    this.descFunctionDesc = descFuncDesc;
  }

  @explain(displayName = "Show Partitions Operator")
  public showPartitionsDesc getShowPartsDesc() {
    return showPartsDesc;
  }

  public void setShowPartsDesc(showPartitionsDesc showPartsDesc) {
    this.showPartsDesc = showPartsDesc;
  }

  @explain(displayName = "Describe Table Operator")
  public descTableDesc getDescTblDesc() {
    return descTblDesc;
  }

  public void setDescTblDesc(descTableDesc descTblDesc) {
    this.descTblDesc = descTblDesc;
  }

  public AddPartitionDesc getAddPartitionDesc() {
    return addPartitionDesc;
  }

  public void setAddPartitionDesc(AddPartitionDesc addPartitionDesc) {
    this.addPartitionDesc = addPartitionDesc;
  }

  public DDLWork(showIndexDesc showIndexsDesc) {
    this.showIndexsDesc = showIndexsDesc;
  }

  public DDLWork(Set<ReadEntity> inputs, Set<WriteEntity> outputs) {
    this.inputs = inputs;
    this.outputs = outputs;
  }

  public DDLWork(Set<ReadEntity> inputs, Set<WriteEntity> outputs,
      alterTableDesc alterTblDesc) {
    this(inputs, outputs);
    this.alterTblDesc = alterTblDesc;
  }

  public DDLWork(Set<ReadEntity> inputs, Set<WriteEntity> outputs,
      createTableDesc crtTblDesc) {
    this.inputs = inputs;
    this.outputs = outputs;
    this.createTblDesc = crtTblDesc;
  }

  public DDLWork(Set<ReadEntity> inputs, Set<WriteEntity> outputs,
      createTableLikeDesc crtTblLikeDesc) {
    this.inputs = inputs;
    this.outputs = outputs;
    this.createTblLikeDesc = crtTblLikeDesc;
  }

  public showIndexDesc getShowIndexDesc() {
    return showIndexsDesc;
  }

  public void setShowIndexDesc(showIndexDesc showIndexsDesc) {
    this.showIndexsDesc = showIndexsDesc;
  }

  public MsckDesc getMsckDesc() {
    return msckDesc;
  }

  public void setMsckDesc(MsckDesc msckDesc) {
    this.msckDesc = msckDesc;
  }

  public addDefaultPartitionDesc getAddDefaultPartDesc() {
    return addDefaultPartDesc;
  }

  public void setAddDefaultPartDesc(addDefaultPartitionDesc addDefaultPartDesc) {
    this.addDefaultPartDesc = addDefaultPartDesc;
  }

}
