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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.antlr.runtime.CommonToken;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.dataToDB.StoreAsPgdata;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.DeleteTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.DeleteWork;
import org.apache.hadoop.hive.ql.plan.UpdateWork;
import org.apache.hadoop.hive.ql.plan.createTableDesc;
import org.apache.hadoop.hive.ql.plan.deleteTableDesc;
import org.apache.hadoop.hive.ql.plan.fetchWork;
import org.apache.hadoop.hive.ql.plan.loadFileDesc;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.ql.plan.updateTableDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.util.StringUtils;

import antlr.Token;

public class UPLSemanticAnalyzer extends BaseSemanticAnalyzer {

  private QB qb;
  private ASTNode ast;
  private fetchWork new_work;
  private HashMap<String, ASTNode> columns = new HashMap<String, ASTNode>();
  private ArrayList<String> isnullcolumns = new ArrayList<String>();
  private String db_name = null;
  private String table_name = null;
  private boolean is_part = false;
  private boolean one_level_part = true;
  private String part_path = null;
  private boolean hasWhere = false;
  private Table t = null;
  private ArrayList<String> cols = new ArrayList<String>();
  private ArrayList<String> partcols = new ArrayList<String>();
  private ArrayList<String> types = new ArrayList<String>();
  private ArrayList<Integer> typesnum = new ArrayList<Integer>();
  private ArrayList<Integer> index = new ArrayList<Integer>();
  private ArrayList<ASTNode> astlist = new ArrayList<ASTNode>();

  public UPLSemanticAnalyzer(HiveConf conf) throws SemanticException {
    super(conf);
  }

  private void getTreeColumns(ASTNode expr) throws SemanticException {
    String column_name;

    if (expr.getType() == HiveParser.KW_EXISTS) {
      columns.clear();
      isnullcolumns.clear();
      throw new SemanticException(ErrorMsg.UPDATE_EXIST_ERROR.getMsg(""));
    }

    if (expr.getType() == HiveParser.TOK_FUNCTION && expr.getChild(0) != null
        && expr.getChild(0).getType() == HiveParser.TOK_ISNULL) {
      ASTNode child = (ASTNode) expr.getChild(1);
      if (child.getType() == HiveParser.TOK_TABLE_OR_COL) {
        column_name = child.getChild(0).getText();
        if (!isnullcolumns.contains(column_name))
          isnullcolumns.add(column_name);
      } else if (child.getType() == HiveParser.DOT && child.getChild(0) != null
          && child.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL) {
        column_name = child.getChild(1).getText();
        if (!isnullcolumns.contains(column_name))
          isnullcolumns.add(column_name);
      } else {

      }
    }

    if (expr.getType() == HiveParser.TOK_TABLE_OR_COL) {
      column_name = expr.getChild(0).getText();
      if (!columns.containsKey(column_name))
        columns.put(column_name, expr);
    } else if (expr.getType() == HiveParser.DOT && expr.getChild(0) != null
        && expr.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL) {
      column_name = expr.getChild(1).getText();
      if (!columns.containsKey(column_name))
        columns.put(expr.getChild(1).getText(), expr);
    } else {
      for (int i = 0; i < expr.getChildCount(); i++) {
        getTreeColumns((ASTNode) expr.getChild(i));
      }
    }
  }

  private void pruneColumns() {
    for (String tmp : isnullcolumns) {
      if (columns.containsKey(tmp)) {
        columns.remove(tmp);
      }
    }
  }

  private void changeASTTreesForUpdate(ASTNode ast) throws HiveException {

    ASTNode querytree = (ASTNode) ast.getChild(0);
    ASTNode wheretree = (ASTNode) ((ASTNode) querytree.getChild(1)).getChild(2);
    ASTNode selecttree = (ASTNode) querytree.getChild(1).getChild(1);

    if (wheretree != null) {
      hasWhere = true;
      wheretree = (ASTNode) ((ASTNode) querytree.getChild(1)).deleteChild(2);
      if (wheretree.getChild(0).getType() == HiveParser.KW_EXISTS) {
        throw new SemanticException(ErrorMsg.UPDATE_EXIST_ERROR.getMsg(""));
      }
    }

    t = db.getTable(db_name, table_name);
    if (t.isPartitioned()) {
      for (FieldSchema fff : t.getPartCols()) {
        LOG.info("partcols:  " + fff.getName() + "  " + fff.getType());
        partcols.add(fff.getName());
      }
    }
    for (FieldSchema ff : t.getCols()) {
      if (ff == null) {
        continue;
      }
      cols.add(ff.getName());
      index.add(-1);
      types.add(ff.getType());
      boolean flag = false;
      for (int i : DDLSemanticAnalyzer.TokenToTypeName.keySet()) {
        if (DDLSemanticAnalyzer.TokenToTypeName.get(i).equalsIgnoreCase(
            ff.getType())) {
          typesnum.add(i);
          flag = true;
        }
      }
      if (!flag) {
        throw new SemanticException(ErrorMsg.UPDATE_COLTYPE_NOTFIND.getMsg(ff
            .getName()));
      }
      LOG.info("cols:  " + ff.getName() + "  " + ff.getType());
    }

    if (t.isPartitioned()) {
      if (!is_part) {
        throw new SemanticException(
            ErrorMsg.UPDATE_PARTITION1_TABLE.getMsg(table_name));
      } else {
        ArrayList<ArrayList<String>> parts = db.getPartitionNames(db_name, t,
            Short.MAX_VALUE);
        if (parts.get(0).get(0).startsWith("hash(")) {
          throw new SemanticException(
              ErrorMsg.UPDATE_HASH_TABLE.getMsg(table_name));
        }
        if (parts.size() == 2 && parts.get(1) != null) {
          if (parts.get(1).get(0).startsWith("hash(")) {
            throw new SemanticException(
                ErrorMsg.UPDATE_HASH_TABLE.getMsg(table_name));
          }
        }
        if (one_level_part && (parts.size() == 2)) {
          throw new SemanticException(
              ErrorMsg.UPDATE_PARTITION2_TABLE.getMsg(table_name));
        }
        if ((!one_level_part) && (parts.size() == 1)) {
          throw new SemanticException(
              ErrorMsg.UPDATE_PARTITION2_TABLE.getMsg(table_name));
        }
        if (one_level_part) {
          if (!(parts.get(0).contains(part_path))) {
            throw new SemanticException(
                ErrorMsg.UPDATE_PARTITION_NOT_EXIST_ERROR.getMsg(part_path));
          }
        } else {
          String[] tmp_part_path = part_path.split("/");
          if ((!(parts.get(0).contains(tmp_part_path[0])))
              || (!(parts.get(1).contains(tmp_part_path[1])))) {
            throw new SemanticException(
                ErrorMsg.UPDATE_PARTITION_NOT_EXIST_ERROR.getMsg(part_path));
          }
        }
      }
    }

    if (t.getParameters().containsKey("EXTERNAL")
        && t.getParameters().get("EXTERNAL").equalsIgnoreCase("TRUE")) {
      throw new SemanticException(
          ErrorMsg.UPDATE_EXTERNAL_TABLE.getMsg(table_name));
    } else if (t.isView()) {
      throw new SemanticException(ErrorMsg.UPDATE_VIEW.getMsg(table_name));
    } else {
      int cc = selecttree.getChildCount();
      for (int i = 0; i < cc; i++) {
        ASTNode astttt = (ASTNode) selecttree.getChild(i);
        LOG.info("astttt:  " + astttt.dump());
        String tmpcol = astttt.getChild(0).getText().toLowerCase();
        if (is_part && partcols.contains(tmpcol)) {
          throw new SemanticException(
              ErrorMsg.UPDATE_PARTITIONCOL_ERROR.getMsg(tmpcol));
        }
        int jj = cols.indexOf(tmpcol);
        if (jj < 0) {
          throw new SemanticException(
              ErrorMsg.UPDATE_COL_NOTFIND.getMsg(tmpcol));
        }
        index.set(jj, i);
        astttt.deleteChild(0);
      }
      for (int i = 0; i < cols.size(); i++) {
        if (index.get(i) == -1) {
          ASTNode tttt = new ASTNode(new CommonToken(HiveParser.TOK_SELEXPR,
              "TOK_SELEXPR"));
          tttt.addChild(new ASTNode(new CommonToken(HiveParser.Identifier, cols
              .get(i))));
          astlist.add(tttt);
          LOG.info("ori col:  " + i + "  " + tttt.dump());
        } else {
          if (hasWhere) {
            ASTNode tttt = (ASTNode) selecttree.getChild(index.get(i));
            ASTNode ch0 = new ASTNode(new CommonToken(HiveParser.TOK_FUNCTION,
                "TOK_FUNCTION"));
            ASTNode ch1 = new ASTNode(new CommonToken(HiveParser.KW_IF, "if"));
            ch0.addChild(ch1);
            wheretree = (wheretree).repNode();
            ch0.addChild(wheretree.getChild(0));
            ASTNode ch2 = new ASTNode(new CommonToken(HiveParser.TOK_FUNCTION,
                "TOK_FUNCTION"));
            ASTNode ch3 = new ASTNode(new CommonToken(typesnum.get(i), "TOK_"
                + types.get(i).toUpperCase()));
            ch2.addChild(ch3);
            ch2.addChild(tttt.getChild(0));
            ch0.addChild(ch2);
            ASTNode ori = new ASTNode(new CommonToken(
                HiveParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL"));
            ori.addChild(new ASTNode(new CommonToken(HiveParser.Identifier,
                cols.get(i))));
            ch0.addChild(ori);
            tttt.deleteChild(0);
            tttt.addChild(ch0);
            astlist.add(tttt);
            LOG.info("chg col wh:  " + i + "  " + (tttt).dump());
          } else {
            ASTNode tttt = (ASTNode) selecttree.getChild(index.get(i));
            ASTNode chch = new ASTNode(new CommonToken(HiveParser.TOK_FUNCTION,
                "TOK_FUNCTION"));
            ASTNode chchch = new ASTNode(new CommonToken(typesnum.get(i),
                "TOK_" + types.get(i).toUpperCase()));
            chch.addChild(chchch);
            chch.addChild(tttt.getChild(0));
            tttt.deleteChild(0);
            tttt.addChild(chch);
            astlist.add(tttt);
            LOG.info("chg col:  " + i + "  " + (tttt).dump());
          }
        }
      }
      for (int i = 0; i < cc; i++) {
        selecttree.deleteChild(0);
      }
      for (int i = 0; i < astlist.size(); i++) {
        selecttree.addChild(astlist.get(i));
      }

    }

  }

  private void genQueryPlan(ASTNode ast) throws SemanticException {
    ASTNode querytree = (ASTNode) ast.getChild(0);
    LOG.info("new tree:  " + querytree.dump());

    SemanticAnalyzer sem_1 = new SemanticAnalyzer(conf);
    sem_1.analyze(querytree, ctx);
    sem_1.validate();
    rootTasks.addAll(sem_1.getRootTasks());
    new_work = (fetchWork) sem_1.getFetchTask().getWork();

  }

  private void getLeafTasks(List<Task<? extends Serializable>> rootTasks,
      HashSet<Task<? extends Serializable>> leaves) {

    for (Task<? extends Serializable> root : rootTasks) {
      getLeafTasks(root, leaves);
    }
  }

  private void getLeafTasks(Task<? extends Serializable> task,
      HashSet<Task<? extends Serializable>> leaves) {
    if (task.getChildTasks() == null) {
      if (!leaves.contains(task)) {
        leaves.add(task);
      }
    } else {
      getLeafTasks(task.getChildTasks(), leaves);
    }
  }

  private void genUpdatePlan(ASTNode ast) throws SemanticException {
    mapredWork mapredwork = (mapredWork) rootTasks.get(0).getWork();

    ArrayList<Operator<? extends Serializable>> collect = new ArrayList<Operator<? extends Serializable>>();

    if (mapredwork.getReducer() != null)
      collect.add(mapredwork.getReducer());
    else
      collect.addAll(mapredwork.getAliasToWork().values());

    FileSinkOperator fsop = null;

    for (Operator<? extends Serializable> op : collect) {
      Operator<? extends Serializable> children;

      while ((children = op.getChildOperators().get(0)) != null) {
        op = children;

        if (op.getName().equals("FS")) {
          fsop = (FileSinkOperator) op;
          tableDesc table_desc = Utilities.getTableDesc(t);
          fsop.getConf().setTableInfo(table_desc);
          break;
        }

        if (op instanceof ReduceSinkOperator
            || op.getChildOperators().size() == 0)
          break;
      }
    }

    String location = null;
    String tmp_dir_new = null;
    String tmp_dir_old = null;

    try {
      Table table = db.getTable(db_name, table_name);
      Path tpath = table.getPath();
      if (is_part)
        tpath = new Path(tpath, part_path);
      location = tpath.toString();
      LOG.info("Origin table location:" + location);
    } catch (HiveException e) {
      e.printStackTrace();
      throw new SemanticException(e.getMessage(), e);
    }

    tmp_dir_new = new_work.getFetchDir().get(0);
    LOG.info("Origin select result location: " + tmp_dir_new);

    String tblAuthority = getAuthorityFromPathUrl(location);
    String tmpAuthotity = getAuthorityFromPathUrl(tmp_dir_new);
    if (tblAuthority == null || tmpAuthotity == null)
      throw new SemanticException("Invalid table location");
    if (!tblAuthority.equals(tmpAuthotity)) {
      tmp_dir_new = tblAuthority + tmp_dir_new.substring(tmpAuthotity.length());
      if (fsop != null) {
        fsop.getConf().setDirName(tmp_dir_new);
      }
    }

    UpdateWork update = null;

    update = new UpdateWork(new updateTableDesc(db_name, table_name, location,
        tmp_dir_new, tmp_dir_old));
    HashSet<Task<? extends Serializable>> leaves = new HashSet<Task<? extends Serializable>>();
    getLeafTasks(rootTasks, leaves);
    assert (leaves.size() > 0);
    Task<? extends Serializable> updateTask = TaskFactory
        .get(update, this.conf);
    for (Task<? extends Serializable> task : leaves) {
      task.addDependentTask(updateTask);
    }
  }

  private void checkAuth() throws SemanticException {
    try {
      if (!this.db.hasAuth(SessionState.get().getUserName(),
          Hive.Privilege.UPDATE_PRIV, db_name, table_name)) {
        if (SessionState.get() != null)
          SessionState.get().ssLog(
              "user : " + SessionState.get().getUserName()
                  + " do not have update privilege on table : " + db_name
                  + "::" + table_name);
        throw new SemanticException("user : "
            + SessionState.get().getUserName()
            + " do not have update privilege on table : " + db_name + "::"
            + table_name);
      }

    } catch (InvalidTableException e) {
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "Table " + e.getTableName() + " does not exist");
      LOG.debug(StringUtils.stringifyException(e));
      throw new SemanticException("Table " + e.getTableName()
          + " does not exist");

    } catch (HiveException e) {
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "FAILED: Error in metadata: " + e.getMessage() + "\n"
                + StringUtils.stringifyException(e));
      LOG.debug(StringUtils.stringifyException(e));
      throw new SemanticException("FAILED: Error in metadata: "
          + e.getMessage());
    } catch (Exception e) {
      console.printError("Failed with exception " + e.getMessage(), "\n"
          + StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "Failed with exception " + e.getMessage() + "\n"
                + StringUtils.stringifyException(e));
      throw new SemanticException("Failed with exception " + e.getMessage());
    }
  }

  private String getPartName(ASTNode ast) throws SemanticException {
    if (ast.getChildCount() == 2) {
      one_level_part = false;
      return ast.getChild(0).getText().toLowerCase() + "/"
          + ast.getChild(1).getText().toLowerCase();
    } else {
      if (ast.getText().equalsIgnoreCase("TOK_SUBPARTITIONREF")) {
        throw new SemanticException("Subpartition can not be used here! ");
      } else {
        return ast.getChild(0).getText().toLowerCase();
      }
    }
  }

  private void getMetaData(ASTNode ast) throws SemanticException, HiveException {
    ASTNode frm = (ASTNode) ast.getChild(0).getChild(0).getChild(0).getChild(0);
    LOG.info("frm:   " + frm.dump());
    if (frm.getChildCount() == 3) {
      is_part = true;
      table_name = frm.getChild(0).getText().toLowerCase();
      db_name = frm.getChild(2).getText().toLowerCase();
      part_path = getPartName((ASTNode) frm.getChild(1));
    } else if (frm.getChildCount() == 1) {
      db_name = SessionState.get().getDbName();
      table_name = frm.getChild(0).getText().toLowerCase();
    } else {
      table_name = frm.getChild(0).getText().toLowerCase();
      String tmpstr = frm.getChild(1).getText();
      if (tmpstr.equalsIgnoreCase("TOK_SUBPARTITIONREF")
          || tmpstr.equalsIgnoreCase("TOK_PARTITIONREF")
          || tmpstr.equalsIgnoreCase("TOK_COMPPARTITIONREF")) {
        is_part = true;
        db_name = SessionState.get().getDbName();
        part_path = getPartName((ASTNode) frm.getChild(1));
      } else {
        db_name = frm.getChild(1).getText().toLowerCase();
      }
    }
    LOG.info(table_name + "  " + db_name + "  " + part_path);
  }

  private void analyzeUpdateTable(ASTNode ast, QB qb) throws HiveException {

    getMetaData(ast);

    checkAuth();

    Table tbl = db.getTable(db_name, table_name);
    if (tbl.getTableStorgeType().equalsIgnoreCase("pgdata")) {
      LOG.info("update external table stored as pgdata");
      String sql = ctx.getTokenRewriteStream().toString(
          this.ast.getTokenStartIndex(), this.ast.getTokenStopIndex());
      StoreAsPgdata sto = new StoreAsPgdata();
      sto.sendUpdateTableToPg(tbl, sql);
      return;
    }

    changeASTTreesForUpdate(ast);

    genQueryPlan(ast);

    genUpdatePlan(ast);
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {

    QB qb = new QB(null, null, false);
    this.qb = qb;
    this.ast = ast;

    LOG.info("Starting UPL Semantic Analysis");
    if (SessionState.get() != null)
      SessionState.get().ssLog("Starting UPL Semantic Analysis");

    switch (ast.getToken().getType()) {
    case HiveParser.TOK_UPDATE:
      try {
        analyzeUpdateTable(ast, qb);
      } catch (HiveException e) {
        throw new SemanticException("Failed with exception " + e.getMessage());
      }
      return;
    default:
      return;
    }
  }
  
  private String getAuthorityFromPathUrl(String path) {
    int start = path.indexOf("//");
    int end = path.indexOf("/", start+2);
    if (start == -1 || end == -1)
      return null;
    String authority = path.substring(0, end);
    return authority;
  }

}
