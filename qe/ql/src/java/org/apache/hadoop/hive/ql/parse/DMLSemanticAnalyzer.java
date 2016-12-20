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
/* File:DMLSemanticAnalyzer.java
 * Author:roachxiang
 * */

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
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.DeleteWork;
import org.apache.hadoop.hive.ql.plan.createTableDesc;
import org.apache.hadoop.hive.ql.plan.deleteTableDesc;
import org.apache.hadoop.hive.ql.plan.fetchWork;
import org.apache.hadoop.hive.ql.plan.loadFileDesc;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.ql.plan.truncatePartitionDesc;
import org.apache.hadoop.hive.ql.plan.truncateTableDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.util.StringUtils;

public class DMLSemanticAnalyzer extends BaseSemanticAnalyzer {

  private QB qb;
  private ASTNode ast;
  private fetchWork work;
  private HashMap<String, ASTNode> columns = new HashMap<String, ASTNode>();
  private ArrayList<String> isnullcolumns = new ArrayList<String>();
  private String db_name = null;
  private String table_name = null;
  private boolean is_part = false;
  private boolean one_level_part = true;
  private String part_path = null;

  public DMLSemanticAnalyzer(HiveConf conf) throws SemanticException {
    super(conf);
  }

  private void getTreeColumns(ASTNode expr) {
    String column_name;

    if (expr.getType() == HiveParser.KW_EXISTS) {
      columns.clear();
      isnullcolumns.clear();
      return;
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

  private void changeASTTreesForDelete(ASTNode ast) {

    ASTNode querytree = (ASTNode) ast.getChild(0);
    ASTNode wheretree = (ASTNode) querytree.getChild(1).getChild(2);

    ASTNode frm = (ASTNode) querytree.getChild(0);

    /* save the dbname and table name */

    /* step1: add a not astnode */
    ASTNode child = (ASTNode) wheretree.deleteChild(0);
    ASTNode not = new ASTNode(new CommonToken(HiveParser.KW_NOT, "not"));
    wheretree.addChild(not);

    /* step2: add is not null node for column value is null */
    getTreeColumns(child);
    pruneColumns();

    if (columns.size() != 0) {
      ASTNode and = new ASTNode(new CommonToken(HiveParser.KW_AND, "and"));
      not.addChild(and);

      int count = 0;
      for (ASTNode tmp : columns.values()) {
        ASTNode isnotnull = new ASTNode(new CommonToken(
            HiveParser.TOK_FUNCTION, "TOK_FUNCTION"));
        isnotnull.addChild(new ASTNode(new CommonToken(
            HiveParser.TOK_ISNOTNULL, "TOK_ISNOTNULL")));
        isnotnull.addChild(tmp);

        if (count < columns.size() - 1) {
          and.addChild(new ASTNode(new CommonToken(HiveParser.KW_AND, "and")));
          and.addChild(isnotnull);
          and = (ASTNode) and.getChild(0);
        } else {
          and.addChild(child);
          and.addChild(isnotnull);
        }

        count++;
      }
    } else {
      not.addChild(child);
    }
  }

  private void genQueryPlan(ASTNode ast) throws SemanticException {
    ASTNode querytree = (ASTNode) ast.getChild(0);

    SemanticAnalyzer sem = new SemanticAnalyzer(conf);
    sem.analyze(querytree, ctx);
    sem.validate();

    rootTasks.addAll(sem.getRootTasks());
    assert rootTasks.size() == 1;
    work = (fetchWork) sem.getFetchTask().getWork();
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

  private void genDeletePlan(ASTNode ast) throws SemanticException {
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
          Table dest_tab = null;

          try {
            dest_tab = this.db.getTable(db_name, table_name);

            if (dest_tab.isPartitioned()) {
              if (!is_part) {
                throw new SemanticException(
                    ErrorMsg.UPDATE_PARTITION1_TABLE.getMsg(table_name));
              } else {
                ArrayList<ArrayList<String>> parts = db.getPartitionNames(
                    db_name, dest_tab, Short.MAX_VALUE);
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
              }

              ArrayList<ArrayList<String>> parts = db.getPartitionNames(
                  db_name, dest_tab, Short.MAX_VALUE);
              if (one_level_part) {
                if (!(parts.get(0).contains(part_path))) {
                  throw new SemanticException(
                      ErrorMsg.UPDATE_PARTITION_NOT_EXIST_ERROR
                          .getMsg(part_path));
                }
              } else {
                String[] tmp_part_path = part_path.split("/");
                if ((!(parts.get(0).contains(tmp_part_path[0])))
                    || (!(parts.get(1).contains(tmp_part_path[1])))) {
                  throw new SemanticException(
                      ErrorMsg.UPDATE_PARTITION_NOT_EXIST_ERROR
                          .getMsg(part_path));
                }
              }
            }
            if (dest_tab.getParameters().containsKey("EXTERNAL")
                && dest_tab.getParameters().get("EXTERNAL")
                    .equalsIgnoreCase("TRUE")) {
              throw new SemanticException(
                  ErrorMsg.DELETE_EXTERNAL_TABLE.getMsg(table_name));
            }
            if (dest_tab.isView()) {
              throw new SemanticException(
                  ErrorMsg.UPDATE_VIEW.getMsg(table_name));
            }
          } catch (InvalidTableException ite) {
            throw new SemanticException(ErrorMsg.INVALID_TABLE.getMsg(qb
                .getParseInfo().getSrcForAlias(table_name)));
          } catch (HiveException e) {
            throw new SemanticException(e.getMessage(), e);
          }

          tableDesc table_desc = Utilities.getTableDesc(dest_tab);
          fsop.getConf().setTableInfo(table_desc);
          break;
        }

        if (op instanceof ReduceSinkOperator
            || op.getChildOperators().size() == 0)
          break;
      }
    }

    String location = "";
    String tmp_dir = "";

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

    assert work.getFetchDir().size() == 1;
    tmp_dir = work.getFetchDir().get(0);
    LOG.info("Origin select result location:" + tmp_dir);

    DeleteWork delete = null;

    String tblAuthority = getAuthorityFromPathUrl(location);
    String tmpAuthotity = getAuthorityFromPathUrl(tmp_dir);
    if (tblAuthority == null || tmpAuthotity == null)
      throw new SemanticException("Invalid table location");
    if (!tblAuthority.equals(tmpAuthotity)) {
      tmp_dir = tblAuthority + tmp_dir.substring(tmpAuthotity.length());
      if (fsop != null) {
        fsop.getConf().setDirName(tmp_dir);
      }
    }

    delete = new DeleteWork(new deleteTableDesc(db_name, table_name, location,
        tmp_dir));
    HashSet<Task<? extends Serializable>> leaves = new HashSet<Task<? extends Serializable>>();
    getLeafTasks(rootTasks, leaves);
    Task<? extends Serializable> deleteTask = TaskFactory
        .get(delete, this.conf);
    assert (leaves.size() > 0);
    for (Task<? extends Serializable> task : leaves) {
      task.addDependentTask(deleteTask);
    }
  }

  private void checkAuth() throws SemanticException {
    try {
      if (!this.db.hasAuth(SessionState.get().getUserName(),
          Hive.Privilege.DELETE_PRIV, db_name, table_name)) {
        if (SessionState.get() != null)
          SessionState.get().ssLog(
              "user : " + SessionState.get().getUserName()
                  + " do not have Delete privilege on table : " + db_name
                  + "::" + table_name);
        throw new SemanticException("user : "
            + SessionState.get().getUserName()
            + " do not have Delete privilege on table : " + db_name + "::"
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
    LOG.info(ast.dump());
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

  private void analyzeDeleteTable(ASTNode ast, QB qb) throws HiveException {

    getMetaData(ast);
    
    Table dest_tab = this.db.getTable(db_name, table_name);
    if (dest_tab.getParameters().containsKey("EXTERNAL")
            && dest_tab.getParameters().get("EXTERNAL").equalsIgnoreCase("TRUE")
            && !dest_tab.getTableStorgeType().equalsIgnoreCase("pgdata")) {
          throw new SemanticException(
              ErrorMsg.DELETE_EXTERNAL_TABLE.getMsg(table_name));
    }
    
    checkAuth();

    Table tbl = db.getTable(db_name, table_name);
    if (tbl.getTableStorgeType().equalsIgnoreCase("pgdata")) {
      LOG.info("delete external table stored as pgdata");
      String sql = ctx.getTokenRewriteStream().toString(
          this.ast.getTokenStartIndex(), this.ast.getTokenStopIndex());
      StoreAsPgdata sto = new StoreAsPgdata();
      sto.sendDeleteTableToPg(tbl, sql);
      return;
    }

    changeASTTreesForDelete(ast);

    genQueryPlan(ast);

    genDeletePlan(ast);
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    QB qb = new QB(null, null, false);
    this.qb = qb;
    this.ast = ast;

    LOG.info("1111:  " + ast.dump());
    LOG.info("Starting DML Semantic Analysis");
    if (SessionState.get() != null)
      SessionState.get().ssLog("Starting DML Semantic Analysis");

    switch (ast.getToken().getType()) {
    case HiveParser.TOK_DELETE:
      try {
        if (((ASTNode) (ast.getChild(0))).getToken().getType() == HiveParser.TOK_QUERY) {
          analyzeDeleteTable(ast, qb);
        } else {
          ASTNode astast = (ASTNode) ast.getChild(0);
          if (astast.getChild(0).getChildCount() == 1) {
            LOG.info("2222:  " + astast.dump());
            table_name = unescapeIdentifier(astast.getChild(0).getChild(0)
                .getText());
            db_name = SessionState.get().getDbName();
            checkAuth();
            truncateTableDesc truncTblDesc = new truncateTableDesc(table_name);
            rootTasks.add(TaskFactory.get(new DDLWork(truncTblDesc), conf));
          } else if (astast.getChild(0).getChildCount() == 3) {
            LOG.info("3333:  " + astast.dump());
            String pri = null;
            String sub = null;
            ASTNode part = (ASTNode) (astast.getChild(0).getChild(1));
            if (part.getToken().getType() == HiveParser.TOK_COMPPARTITIONREF) {
              pri = unescapeIdentifier(part.getChild(0).getText().toLowerCase());
              sub = unescapeIdentifier(part.getChild(1).getText().toLowerCase());
            } else if (part.getToken().getType() == HiveParser.TOK_SUBPARTITIONREF) {
              sub = unescapeIdentifier(part.getChild(0).getText().toLowerCase());
            } else {
              pri = unescapeIdentifier(part.getChild(0).getText().toLowerCase());
            }
            table_name = unescapeIdentifier(astast.getChild(0).getChild(0)
                .getText());
            db_name = unescapeIdentifier(astast.getChild(0).getChild(2)
                .getText());
            checkAuth();
            truncatePartitionDesc tpd = new truncatePartitionDesc(db_name,
                table_name, pri, sub);
            rootTasks.add(TaskFactory.get(new DDLWork(tpd), conf));
          } else if (astast.getChild(0).getChildCount() == 2) {
            table_name = unescapeIdentifier(astast.getChild(0).getChild(0)
                .getText());
            ASTNode part = (ASTNode) (astast.getChild(0).getChild(1));
            if (part.getToken().getType() == HiveParser.TOK_COMPPARTITIONREF
                || part.getToken().getType() == HiveParser.TOK_SUBPARTITIONREF
                || part.getToken().getType() == HiveParser.TOK_PARTITIONREF) {
              LOG.info("4444:  " + astast.dump());
              String pri = null;
              String sub = null;
              if (part.getToken().getType() == HiveParser.TOK_COMPPARTITIONREF) {
                pri = unescapeIdentifier(part.getChild(0).getText()
                    .toLowerCase());
                sub = unescapeIdentifier(part.getChild(1).getText()
                    .toLowerCase());
              } else if (part.getToken().getType() == HiveParser.TOK_SUBPARTITIONREF) {
                sub = unescapeIdentifier(part.getChild(0).getText()
                    .toLowerCase());
              } else {
                pri = unescapeIdentifier(part.getChild(0).getText()
                    .toLowerCase());
              }
              db_name = SessionState.get().getDbName();
              checkAuth();
              truncatePartitionDesc tpd = new truncatePartitionDesc(db_name,
                  table_name, pri, sub);
              rootTasks.add(TaskFactory.get(new DDLWork(tpd), conf));
            } else {
              LOG.info("5555:  " + astast.dump());
              db_name = unescapeIdentifier(part.getText());
              checkAuth();
              truncateTableDesc truncTblDesc = new truncateTableDesc(db_name,
                  table_name);
              rootTasks.add(TaskFactory.get(new DDLWork(truncTblDesc), conf));
            }
          } else {
            LOG.info("!!!!!!!!!!!");
          }
        }
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
