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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;
import java.util.TimeZone;
import org.antlr.runtime.tree.CommonTree;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.io.GBKIgnoreKeyOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.MsckDesc;
import org.apache.hadoop.hive.ql.plan.addDefaultPartitionDesc;
import org.apache.hadoop.hive.ql.plan.alterCommentDesc;
import org.apache.hadoop.hive.ql.plan.alterTableDesc;
import org.apache.hadoop.hive.ql.plan.clearqueryDesc;
import org.apache.hadoop.hive.ql.plan.createDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.createTableDesc;
import org.apache.hadoop.hive.ql.plan.createTableLikeDesc;
import org.apache.hadoop.hive.ql.plan.descFunctionDesc;
import org.apache.hadoop.hive.ql.plan.descTableDesc;
import org.apache.hadoop.hive.ql.plan.dropDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.dropTableDesc;
import org.apache.hadoop.hive.ql.plan.execExtSQLDesc;
import org.apache.hadoop.hive.ql.plan.fetchWork;
import org.apache.hadoop.hive.ql.plan.killqueryDesc;
import org.apache.hadoop.hive.ql.plan.showCreateTableDesc;
import org.apache.hadoop.hive.ql.plan.showDBDesc;
import org.apache.hadoop.hive.ql.plan.indexInfoDesc;
import org.apache.hadoop.hive.ql.plan.showDatabaseSizeDesc;
import org.apache.hadoop.hive.ql.plan.showFunctionsDesc;
import org.apache.hadoop.hive.ql.plan.showIndexDesc;
import org.apache.hadoop.hive.ql.plan.showInfoDesc;
import org.apache.hadoop.hive.ql.plan.showPartitionsDesc;
import org.apache.hadoop.hive.ql.plan.showProcesslistDesc;
import org.apache.hadoop.hive.ql.plan.showRowCountDesc;
import org.apache.hadoop.hive.ql.plan.showTableSizeDesc;
import org.apache.hadoop.hive.ql.plan.showTablesDesc;
import org.apache.hadoop.hive.ql.plan.showVersionDesc;
import org.apache.hadoop.hive.ql.plan.showViewTablesDesc;
import org.apache.hadoop.hive.ql.plan.showqueryDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.ql.plan.truncatePartitionDesc;
import org.apache.hadoop.hive.ql.plan.truncateTableDesc;
import org.apache.hadoop.hive.ql.plan.useDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.alterTableDesc.alterTableTypes;
import org.apache.hadoop.hive.ql.plan.showIndexDesc.showIndexTypes;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.protobuf.ProtobufSerDe;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import com.google.protobuf.DescriptorProtos.*;
import com.google.protobuf.Descriptors.*;
import java.lang.reflect.Method;
import java.nio.charset.Charset;

public class DDLSemanticAnalyzer extends BaseSemanticAnalyzer {
  private static final String ORACLE = "oracle";
  private static final String DBTYPE2 = "db_type";
  public static final String PGSQL = "pgsql";
  private static final Log LOG = LogFactory
      .getLog("hive.ql.parse.DDLSemanticAnalyzer");
  public static final Map<Integer, String> TokenToTypeName = new HashMap<Integer, String>();

  static {
    TokenToTypeName.put(HiveParser.TOK_BOOLEAN, Constants.BOOLEAN_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_TINYINT, Constants.TINYINT_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_SMALLINT, Constants.SMALLINT_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_INT, Constants.INT_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_BIGINT, Constants.BIGINT_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_FLOAT, Constants.FLOAT_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_DOUBLE, Constants.DOUBLE_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_STRING, Constants.STRING_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_DATE, Constants.DATE_TYPE_NAME);
    TokenToTypeName.put(HiveParser.TOK_DATETIME, Constants.DATETIME_TYPE_NAME);
    TokenToTypeName
        .put(HiveParser.TOK_TIMESTAMP, Constants.TIMESTAMP_TYPE_NAME);
  }

  public static String getTypeName(int token) {
    return TokenToTypeName.get(token);
  }

  public DDLSemanticAnalyzer(HiveConf conf) throws SemanticException {
    super(conf);
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {

    if (ast.getToken().getType() == HiveParser.TOK_DROPTABLE)
      analyzeDropTable(ast, false);
    else if (ast.getToken().getType() == HiveParser.TOK_TRUNCATETABLE)
      analyzeTruncateTable(ast);
    else if (ast.getToken().getType() == HiveParser.TOK_DESCTABLE) {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeDescribeTable(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_SHOWTABLES) {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowTables(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_SHOWPROCESSLIST) {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowProcesslist(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_KILLQUERY) {
      analyzeKillQuery(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_SHOWQUERY) {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowQuery(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_CLEARQUERY) {
      try {
        analyzeClearQuery(ast);
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else if (ast.getToken().getType() == HiveParser.TOK_SHOWSTATINFO) {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowInfo(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_SHOWSTATINFO) {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowInfo(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_SHOWTABLESIZE) {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowTableSize(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_SHOWDATABASESIZE) {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowDatabaseSize(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_SHOWROWCOUNT) {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowRowCount(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_SHOWFUNCTIONS) {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowFunctions(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_DESCFUNCTION) {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeDescFunction(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_MSCK) {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeMetastoreCheck(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_RENAME)
      analyzeAlterTableRename(ast);
    else if (ast.getToken().getType() == HiveParser.TOK_DROPVIEW)
      analyzeDropTable(ast, true);
    else if (ast.getToken().getType() == HiveParser.TOK_ALTERVIEW_PROPERTIES)
      analyzeAlterTableProps(ast, true);
    else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_ADDCOLS)
      analyzeAlterTableModifyCols(ast, alterTableTypes.ADDCOLS);
    else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_REPLACECOLS)
      analyzeAlterTableModifyCols(ast, alterTableTypes.REPLACECOLS);
    else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_RENAMECOL)
      analyzeAlterTableRenameCol(ast);
    else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_ADDPARTS) {
      analyzeAlterTableAddParts(ast, false);
    } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_ADDSUBPARTS) {
      analyzeAlterTableAddParts(ast, true);
    } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_DROPPARTS)
      analyzeAlterTableDropParts(ast);
    else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_PROPERTIES)
      analyzeAlterTableProps(ast, false);
    else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_SERDEPROPERTIES)
      analyzeAlterTableSerdeProps(ast);
    else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_SERIALIZER)
      analyzeAlterTableSerde(ast);
    else if (ast.getToken().getType() == HiveParser.TOK_SHOWPARTITIONS) {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowPartitions(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_CREATE_DATABASE) {
      analyzeCreateDatabase(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_DROP_DATABASE) {
      analyzeDropDatabase(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_USE_DATABASE) {
      analyzeUseDatabase(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_ADDDEFAULTPARTITION) {
      analyzeAddDefaultPartition(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_TRUNCATE_PARTITION) {
      analyzeTruncatePartition(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_SHOW_DATABASES) {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowDatabases(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_ADDINDEX) {
      analyzeAlterTableAddIndex(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_ALTERTABLE_DROPINDEX) {
      analyzeAlterTableDropIndex(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_SHOWTABLEINDEXS) {
      analyzeShowTableIndex(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_SHOWALLINDEXS) {
      analyzeShowAllIndex(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_SHOW_CREATE_TABLE) {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowCreateTable(ast);
    } else if (ast.getToken().getType() == HiveParser.TOK_SHOWVERSION) {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowVersion(ast);
    }

    else if (ast.getToken().getType() == HiveParser.TOK_EXECEXTSQL) {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeExecExtSQL(ast);
    }

    else if (ast.getToken().getType() == HiveParser.TOK_SHOWVIEWTABLES) {
      ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
      analyzeShowViewTables(ast);
    }

    else if (ast.getToken().getType() == HiveParser.TOK_NEWCOMMENT) {
      analyzeComment(ast);
    } else {
      throw new SemanticException("Unsupported DDL command.");
    }
  }

  private void analyzeComment(ASTNode ast) throws SemanticException {
    alterCommentDesc acd = null;
    String comment = null;
    String dbName = null;
    String tblName = null;
    String colName = null;
    if (ast.getChild(0).getType() == HiveParser.TOK_TAB) {
      ASTNode tabOrCol = (ASTNode) ast.getChild(1);
      assert (tabOrCol.getToken().getType() == HiveParser.TOK_TABLE_OR_COL);
      if (tabOrCol.getChildCount() == 2) {
        ASTNode dbNode = (ASTNode) tabOrCol.getChild(1);
        dbName = dbNode.toStringTree().toLowerCase();
      }
      ASTNode tblNode = (ASTNode) tabOrCol.getChild(0);
      tblName = tblNode.toStringTree().toLowerCase();
      assert (!tblName.contains("."));
      if (tblName.contains("."))
        throw new SemanticException(
            "You should not use column name when comment on table!");
      if (ast.getChild(2).getType() != HiveParser.TOK_NULL) {
        comment = unescapeSQLString(ast.getChild(2).getText());
      }
      acd = new alterCommentDesc(dbName, tblName, comment, false);

    } else if (ast.getChild(0).getType() == HiveParser.TOK_TABCOL) {
      ASTNode tabAndCol = (ASTNode) ast.getChild(1);
      if (tabAndCol.getType() != HiveParser.DOT)
        throw new SemanticException(
            "You should point out table name and column name just like: table.column or db::table.column!");
      if (ast.getChild(2).getType() != HiveParser.TOK_NULL) {
        comment = unescapeSQLString(ast.getChild(2).getText());
      }

      ASTNode tabOrCol = (ASTNode) tabAndCol.getChild(0);
      if (tabOrCol.getChildCount() == 2) {
        ASTNode dbNode = (ASTNode) tabOrCol.getChild(1);
        dbName = dbNode.toStringTree().toLowerCase();
      }
      ASTNode tblNode = (ASTNode) tabOrCol.getChild(0);
      tblName = tblNode.toStringTree().toLowerCase();
      ASTNode colNode = (ASTNode) tabAndCol.getChild(1);
      colName = colNode.toStringTree().toLowerCase();
      acd = new alterCommentDesc(dbName, tblName, colName, comment);

    } else if (ast.getChild(0).getType() == HiveParser.TOK_VIEW) {
      ASTNode tabOrCol = (ASTNode) ast.getChild(1);
      assert (tabOrCol.getToken().getType() == HiveParser.TOK_TABLE_OR_COL);
      if (tabOrCol.getChildCount() == 2) {
        ASTNode dbNode = (ASTNode) tabOrCol.getChild(1);
        dbName = dbNode.toStringTree().toLowerCase();
      }
      ASTNode tblNode = (ASTNode) tabOrCol.getChild(0);
      tblName = tblNode.toStringTree().toLowerCase();
      assert (!tblName.contains("."));
      if (tblName.contains("."))
        throw new SemanticException(
            "You should not use column name when comment on view!");
      if (ast.getChild(2).getType() != HiveParser.TOK_NULL) {
        comment = unescapeSQLString(ast.getChild(2).getText());
      }
      acd = new alterCommentDesc(dbName, tblName, comment, true);
    }

    rootTasks.add(TaskFactory.get(new DDLWork(acd), conf));
  }

  private void analyzeShowViewTables(ASTNode ast) {
    String vn = ast.getChild(0).getText();
    vn = unescapeIdentifier(vn);
    showViewTablesDesc viewDesc = new showViewTablesDesc(vn, ctx.getResFile());
    rootTasks.add(TaskFactory.get(new DDLWork(viewDesc), conf));
    setFetchTask(createFetchTask(viewDesc.getSchema()));
  }

  private void analyzeAlterTableDropIndex(ASTNode ast) {
    int childNum = ast.getChildCount();
    String tableName = unescapeIdentifier(ast.getChild(0).getText());

    indexInfoDesc indexInfo = new indexInfoDesc();
    indexInfo.name = unescapeIdentifier(ast.getChild(1).getChild(0).getText());

    alterTableDesc alterTblDesc = new alterTableDesc(tableName, null,
        alterTableTypes.DROPINDEX);
    alterTblDesc.setIndexInfo(indexInfo);
    rootTasks.add(TaskFactory.get(new DDLWork(alterTblDesc), conf));
  }

  private void analyzeExecExtSQL(ASTNode ast) throws SemanticException {
    Properties prop = new Properties();
    String sql = unescapeSQLString(ast.getChild(0).getText());
    String dbtype = null;

    for (int i = 0; i < ast.getChild(1).getChildCount(); ++i) {

      String key = ((ASTNode) ast.getChild(1).getChild(i).getChild(0))
          .getText();
      String value = ((ASTNode) ast.getChild(1).getChild(i).getChild(1))
          .getText();

      if (key.endsWith("'") || key.endsWith("\""))
        key = unescapeSQLString(key);
      else
        key = unescapeIdentifier(key);

      if (value.endsWith("'") || value.endsWith("\""))
        value = unescapeSQLString(value);
      else {
        if (key.equals("pwd"))
          throw new SemanticException("the value of pwd must be string!");
        value = unescapeIdentifier(value);
      }

      LOG.debug("set prop: key :" + key + ", value:" + value);
      prop.setProperty(key.toLowerCase(), value);
    }

    String[] propkeys = new String[] {};

    if (prop.getProperty(DBTYPE2) != null) {
      if (prop.getProperty(DBTYPE2).equals(ORACLE)
          || prop.getProperty(DBTYPE2).toLowerCase().equals("ora"))
        dbtype = ORACLE;
      else if (prop.getProperty(DBTYPE2).equals(PGSQL)
          || prop.getProperty(DBTYPE2).toLowerCase().equals("pg"))
        dbtype = PGSQL;
      else
        throw new SemanticException(
            "DBTYPE error! TDW can only support PostgreSQL or Oracle SQL with ExecExtSQL cmd!");
    } else
      dbtype = PGSQL;

    LOG.debug("dbtype : " + dbtype);
    execExtSQLDesc execSQLdesc = new execExtSQLDesc(sql, dbtype, prop,
        ctx.getResFile());

    rootTasks.add(TaskFactory.get(new DDLWork(execSQLdesc), conf));
    setFetchTask(createFetchTask(execSQLdesc.getSchema()));

  }

  private void analyzeAlterTableAddIndex(ASTNode ast) {
    int childNum = ast.getChildCount();
    String tableName = unescapeIdentifier(ast.getChild(0).getText());

    indexInfoDesc indexInfo = new indexInfoDesc();

    for (int i = 1; i < childNum; i++) {
      ASTNode child = (ASTNode) ast.getChild(i);
      if (child.getToken().getType() == HiveParser.TOK_INDEXNAME) {
        indexInfo.name = child.getChild(0).getText();
      }

      if (child.getToken().getType() == HiveParser.TOK_INDEXFIELD) {
        int fieldNum = child.getChildCount();

        for (int k = 0; k < fieldNum; k++) {
          ASTNode fieldNode = (ASTNode) child.getChild(k);

          indexInfo.fieldList.add(fieldNode.getText());
        }
      }
    }

    if (indexInfo.fieldList.size() > 1) {
      indexInfo.indexType = 2;
    } else {
      indexInfo.indexType = 1;
    }

    alterTableDesc alterTblDesc = new alterTableDesc(tableName, null,
        alterTableTypes.ADDINDEX);
    alterTblDesc.setIndexInfo(indexInfo);

    rootTasks.add(TaskFactory.get(new DDLWork(alterTblDesc), conf));
  }

  private void analyzeTruncatePartition(ASTNode ast) throws SemanticException {
    String tableName = unescapeIdentifier(ast.getChild(0).getText());
    String pri = null;
    String sub = null;

    if (((ASTNode) (ast.getChild(1))).getToken().getType() == HiveParser.TOK_COMPPARTITIONREF) {
      pri = unescapeIdentifier(ast.getChild(1).getChild(0).getText()
          .toLowerCase());
      sub = unescapeIdentifier(ast.getChild(1).getChild(1).getText()
          .toLowerCase());
    } else if (((ASTNode) (ast.getChild(1))).getToken().getType() == HiveParser.TOK_SUBPARTITIONREF) {
      sub = unescapeIdentifier(ast.getChild(1).getChild(0).getText()
          .toLowerCase());
    } else {
      pri = unescapeIdentifier(ast.getChild(1).getChild(0).getText()
          .toLowerCase());
    }

    truncatePartitionDesc tpd = new truncatePartitionDesc(SessionState.get()
        .getDbName(), tableName, pri, sub);
    rootTasks.add(TaskFactory.get(new DDLWork(tpd), conf));

  }

  private void analyzeDropTable(ASTNode ast, boolean expectView)
      throws SemanticException {
    String tableName = unescapeIdentifier(ast.getChild(0).getText());
    Boolean reserveData = false;
    if (ast.getChildCount() > 1) {
      reserveData = true;
    }
    dropTableDesc dropTblDesc = new dropTableDesc(tableName, reserveData,
        expectView);
    rootTasks.add(TaskFactory.get(new DDLWork(dropTblDesc), conf));
  }

  private void analyzeTruncateTable(ASTNode ast) throws SemanticException {
    String tableName = unescapeIdentifier(ast.getChild(0).getText());
    truncateTableDesc truncTblDesc = new truncateTableDesc(tableName);
    rootTasks.add(TaskFactory.get(new DDLWork(truncTblDesc), conf));
  }

  private void analyzeAlterTableProps(ASTNode ast, boolean expectView)
      throws SemanticException {
    String tableName = unescapeIdentifier(ast.getChild(0).getText());
    HashMap<String, String> mapProp = getProps((ASTNode) (ast.getChild(1))
        .getChild(0));
    alterTableDesc alterTblDesc = new alterTableDesc(alterTableTypes.ADDPROPS,
        expectView);
    alterTblDesc.setProps(mapProp);
    alterTblDesc.setOldName(tableName);
    rootTasks.add(TaskFactory.get(new DDLWork(alterTblDesc), conf));
  }

  private void analyzeAlterTableSerdeProps(ASTNode ast)
      throws SemanticException {
    String tableName = unescapeIdentifier(ast.getChild(0).getText());
    HashMap<String, String> mapProp = getProps((ASTNode) (ast.getChild(1))
        .getChild(0));
    alterTableDesc alterTblDesc = new alterTableDesc(
        alterTableTypes.ADDSERDEPROPS);
    alterTblDesc.setProps(mapProp);
    alterTblDesc.setOldName(tableName);
    rootTasks.add(TaskFactory.get(new DDLWork(alterTblDesc), conf));
  }

  private void analyzeAlterTableSerde(ASTNode ast) throws SemanticException {
    String tableName = unescapeIdentifier(ast.getChild(0).getText());
    String serdeName = unescapeSQLString(ast.getChild(1).getText());
    alterTableDesc alterTblDesc = new alterTableDesc(alterTableTypes.ADDSERDE);
    if (ast.getChildCount() > 2) {
      HashMap<String, String> mapProp = getProps((ASTNode) (ast.getChild(2))
          .getChild(0));
      alterTblDesc.setProps(mapProp);
    }
    alterTblDesc.setOldName(tableName);
    alterTblDesc.setSerdeName(serdeName);
    rootTasks.add(TaskFactory.get(new DDLWork(alterTblDesc), conf));
  }

  private HashMap<String, String> getProps(ASTNode prop) {
    HashMap<String, String> mapProp = new HashMap<String, String>();
    for (int propChild = 0; propChild < prop.getChildCount(); propChild++) {
      String key = unescapeSQLString(prop.getChild(propChild).getChild(0)
          .getText());
      String value = unescapeSQLString(prop.getChild(propChild).getChild(1)
          .getText());
      mapProp.put(key, value);
    }
    return mapProp;
  }

  private String getFullyQualifiedName(ASTNode ast) {
    if (ast.getChildCount() == 0) {
      return ast.getText();
    }

    return getFullyQualifiedName((ASTNode) ast.getChild(0)) + "."
        + getFullyQualifiedName((ASTNode) ast.getChild(1));
  }

  private Task<? extends Serializable> createFetchTask(String schema) {
    Properties prop = new Properties();

    prop.setProperty(Constants.SERIALIZATION_FORMAT, "9");
    prop.setProperty(Constants.SERIALIZATION_NULL_FORMAT, " ");
    String[] colTypes = schema.split("#");
    prop.setProperty("columns", colTypes[0]);
    prop.setProperty("columns.types", colTypes[1]);
    LOG.debug(ctx.getResFile());
    fetchWork fetch = new fetchWork(ctx.getResFile().toString(), new tableDesc(
        LazySimpleSerDe.class, TextInputFormat.class,
        IgnoreKeyTextOutputFormat.class, prop), -1);
    fetch.setSerializationNullFormat(" ");
    return TaskFactory.get(fetch, this.conf);
  }

  private void analyzeDescribeTable(ASTNode ast) throws SemanticException {
    ASTNode tableTypeExpr = (ASTNode) ast.getChild(0);
    String tableName = getFullyQualifiedName((ASTNode) tableTypeExpr
        .getChild(0));
    LOG.info("tablename: " + tableName);

    String dbname = null;
    if (tableTypeExpr.getChildCount() == 2) {
      dbname = getFullyQualifiedName((ASTNode) tableTypeExpr.getChild(1));
      LOG.info("dbname: " + dbname);
    }

    String partSpec = null;
    if (tableTypeExpr.getChildCount() == 2) {

    }

    boolean isExt = false;
    String pattern = null;
    if (ast.getChildCount() == 2) {
      String token = stripQuotes(ast.getChild(1).getText());
      if (token.compareToIgnoreCase("EXTENDED") == 0) {
        isExt = true;
      } else {
        pattern = token;
      }
    } else if (ast.getChildCount() == 3) {
      String token = ast.getChild(1).getText();
      if (token.compareToIgnoreCase("EXTENDED") == 0) {
        isExt = false;
        pattern = stripQuotes(ast.getChild(2).getText());
      }
    }
    descTableDesc descTblDesc = new descTableDesc(ctx.getResFile(), dbname,
        tableName, partSpec, pattern, isExt);
    rootTasks.add(TaskFactory.get(new DDLWork(descTblDesc), conf));
    setFetchTask(createFetchTask(descTblDesc.getSchema()));
    LOG.info("analyzeDescribeTable done");
    if (SessionState.get() != null)
      SessionState.get().ssLog("analyzeDescribeTable done");
  }

  private void analyzeShowPartitions(ASTNode ast) throws SemanticException {
    showPartitionsDesc showPartsDesc;
    String tableName = unescapeIdentifier(ast.getChild(0).getText());
    showPartsDesc = new showPartitionsDesc(tableName, ctx.getResFile());
    rootTasks.add(TaskFactory.get(new DDLWork(showPartsDesc), conf));
    setFetchTask(createFetchTask(showPartsDesc.getSchema()));
  }

  private void analyzeAddDefaultPartition(ASTNode ast) throws SemanticException {

    String tableName = unescapeIdentifier(ast.getChild(0).getText());
    boolean isSub = (((ASTNode) (ast.getChild(1))).getToken().getType() == HiveParser.TOK_DEFAULTSUBPARTITION ? true
        : false);

    addDefaultPartitionDesc adpd = new addDefaultPartitionDesc(SessionState
        .get().getDbName(), tableName, isSub);

    rootTasks.add(TaskFactory.get(new DDLWork(adpd), conf));
  }

  private void analyzeShowTables(ASTNode ast) throws SemanticException {
    showTablesDesc showTblsDesc;
    if (ast.getChildCount() == 1) {
      String tableNames = unescapeSQLString(ast.getChild(0).getText());
      showTblsDesc = new showTablesDesc(ctx.getResFile(), tableNames);
    } else {
      showTblsDesc = new showTablesDesc(ctx.getResFile());
    }
    rootTasks.add(TaskFactory.get(new DDLWork(showTblsDesc), conf));
    setFetchTask(createFetchTask(showTblsDesc.getSchema()));
  }

  private void analyzeShowProcesslist(ASTNode ast) throws SemanticException {
    showProcesslistDesc showprosDesc;
    showprosDesc = new showProcesslistDesc(ctx.getResFile());

    if (ast.getChildCount() == 1) {
      String argstr = ast.getChild(0).getText();
      LOG.info(ast.dump() + "  " + argstr + "  " + ast.getChildCount());
      if (argstr.equalsIgnoreCase("local")) {
        showprosDesc.setIsLocal(true);
      } else {
        showprosDesc.setUname(argstr);
      }
    } else if (ast.getChildCount() == 2) {
      String argstr = ast.getChild(0).getText();
      String uname = ast.getChild(1).getText();
      if (argstr.equalsIgnoreCase("local")) {
        showprosDesc.setIsLocal(true);
      }
      showprosDesc.setUname(uname);
    }
    rootTasks.add(TaskFactory.get(new DDLWork(showprosDesc), conf));
    setFetchTask(createFetchTask(showprosDesc.getSchema()));
  }

  private void analyzeShowDatabaseSize(ASTNode ast) throws SemanticException {
    if (ast.getChildCount() == 1) {
      showDatabaseSizeDesc dbsizedesc = new showDatabaseSizeDesc(
          ctx.getResFile(), ast.getChild(0).getText(), false);
      rootTasks.add(TaskFactory.get(new DDLWork(dbsizedesc), conf));
      setFetchTask(createFetchTask(dbsizedesc.getSchema()));
    } else if (ast.getChildCount() == 2) {
      showDatabaseSizeDesc dbsizedesc = new showDatabaseSizeDesc(
          ctx.getResFile(), ast.getChild(1).getText(), true);
      rootTasks.add(TaskFactory.get(new DDLWork(dbsizedesc), conf));
      setFetchTask(createFetchTask(dbsizedesc.getSchema()));
    }
  }

  private void analyzeShowRowCount(ASTNode ast) throws SemanticException {
    boolean isextended = false;
    String tblname = null;
    String dbname = null;
    Vector<String> partitions = null;
    Vector<String> subpartitions = null;
    Vector<String> pairpartitions = null;
    String token = stripQuotes(ast.getChild(0).getText());
    if (token.equalsIgnoreCase("extended")) {
      isextended = true;
      if (ast.getChildCount() == 2) {
        tblname = ast.getChild(1).getText();
        showRowCountDesc rowcountdesc = new showRowCountDesc(ctx.getResFile(),
            tblname, isextended);
        rootTasks.add(TaskFactory.get(new DDLWork(rowcountdesc), conf));
        setFetchTask(createFetchTask(rowcountdesc.getSchema()));
      } else if (ast.getChildCount() == 3) {
        tblname = ast.getChild(1).getText();
        partitions = new Vector<String>();
        subpartitions = new Vector<String>();
        pairpartitions = new Vector<String>();
        partitions.clear();
        subpartitions.clear();
        pairpartitions.clear();
        int sss = ast.getChildCount();
        for (int i = 2; i < sss; i++) {
          ASTNode newast = (ASTNode) ast.getChild(i);
          LOG.info("child " + i + " : " + newast.dump());
          int nnn = newast.getChildCount();
          if (nnn == 1) {
            if (newast.getToken().getType() == HiveParser.TOK_PARTITIONREF) {
              String part = unescapeIdentifier(newast.getChild(0).getText());
              if (!partitions.contains(part.toLowerCase())) {
                partitions.add(part.toLowerCase());
              }
              LOG.info("partitions add: " + part);
            } else if (newast.getToken().getType() == HiveParser.TOK_SUBPARTITIONREF) {
              String spart = unescapeIdentifier(newast.getChild(0).getText());
              if (!subpartitions.contains(spart.toLowerCase())) {
                subpartitions.add(spart.toLowerCase());
              }
              LOG.info("subpartitions add: " + spart);
            }
          } else if (nnn == 2
              && newast.getToken().getType() == HiveParser.TOK_COMPPARTITIONREF) {
            String part = unescapeIdentifier(newast.getChild(0).getText());
            String spart = unescapeIdentifier(newast.getChild(1).getText());
            String toadd = part.toLowerCase() + "	" + spart.toLowerCase();
            if (!pairpartitions.contains(toadd)) {
              pairpartitions.add(toadd);
            }
            LOG.info("pairpartitions add: " + part + " " + spart);
          }
        }
        showRowCountDesc rowcountdesc = new showRowCountDesc(ctx.getResFile(),
            tblname, isextended, partitions, subpartitions, pairpartitions);
        rootTasks.add(TaskFactory.get(new DDLWork(rowcountdesc), conf));
        setFetchTask(createFetchTask(rowcountdesc.getSchema()));
      } else if (ast.getChildCount() == 4) {
        LOG.info(ast.dump());
        dbname = ast.getChild(2).getText();
        tblname = ast.getChild(3).getText();
        showRowCountDesc rowcountdesc = new showRowCountDesc(ctx.getResFile(),
            dbname, tblname, isextended);
        rootTasks.add(TaskFactory.get(new DDLWork(rowcountdesc), conf));
        setFetchTask(createFetchTask(rowcountdesc.getSchema()));
      } else {
        LOG.info(ast.dump());
        dbname = ast.getChild(2).getText();
        tblname = ast.getChild(3).getText();
        partitions = new Vector<String>();
        subpartitions = new Vector<String>();
        pairpartitions = new Vector<String>();
        partitions.clear();
        subpartitions.clear();
        pairpartitions.clear();
        int sss = ast.getChildCount();
        for (int i = 4; i < sss; i++) {
          ASTNode newast = (ASTNode) ast.getChild(i);
          LOG.info("child " + i + " : " + newast.dump());
          int nnn = newast.getChildCount();
          if (nnn == 1) {
            if (newast.getToken().getType() == HiveParser.TOK_PARTITIONREF) {
              String part = unescapeIdentifier(newast.getChild(0).getText());
              if (!partitions.contains(part.toLowerCase())) {
                partitions.add(part.toLowerCase());
              }
              LOG.info("partitions add: " + part);
            } else if (newast.getToken().getType() == HiveParser.TOK_SUBPARTITIONREF) {
              String spart = unescapeIdentifier(newast.getChild(0).getText());
              if (!subpartitions.contains(spart.toLowerCase())) {
                subpartitions.add(spart.toLowerCase());
              }
              LOG.info("subpartitions add: " + spart);
            }
          } else if (nnn == 2
              && newast.getToken().getType() == HiveParser.TOK_COMPPARTITIONREF) {
            String part = unescapeIdentifier(newast.getChild(0).getText());
            String spart = unescapeIdentifier(newast.getChild(1).getText());
            String toadd = part.toLowerCase() + "	" + spart.toLowerCase();
            if (!pairpartitions.contains(toadd)) {
              pairpartitions.add(toadd);
            }
            LOG.info("pairpartitions add: " + part + " " + spart);
          }
        }
        showRowCountDesc rowcountdesc = new showRowCountDesc(ctx.getResFile(),
            dbname, tblname, isextended, partitions, subpartitions,
            pairpartitions);
        rootTasks.add(TaskFactory.get(new DDLWork(rowcountdesc), conf));
        setFetchTask(createFetchTask(rowcountdesc.getSchema()));
      }
    } else {
      isextended = false;
      if (ast.getChildCount() == 1) {
        tblname = ast.getChild(0).getText();
        showRowCountDesc rowcountdesc = new showRowCountDesc(ctx.getResFile(),
            tblname, isextended);
        rootTasks.add(TaskFactory.get(new DDLWork(rowcountdesc), conf));
        setFetchTask(createFetchTask(rowcountdesc.getSchema()));
      } else if (ast.getChildCount() == 2) {
        tblname = ast.getChild(0).getText();
        partitions = new Vector<String>();
        subpartitions = new Vector<String>();
        pairpartitions = new Vector<String>();
        partitions.clear();
        subpartitions.clear();
        pairpartitions.clear();
        int sss = ast.getChildCount();
        for (int i = 1; i < sss; i++) {
          ASTNode newast = (ASTNode) ast.getChild(i);
          LOG.info("child " + i + " : " + newast.dump());
          int nnn = newast.getChildCount();
          if (nnn == 1) {
            if (newast.getToken().getType() == HiveParser.TOK_PARTITIONREF) {
              String part = unescapeIdentifier(newast.getChild(0).getText());
              if (!partitions.contains(part.toLowerCase())) {
                partitions.add(part.toLowerCase());
              }
              LOG.info("partitions add: " + part);
            } else if (newast.getToken().getType() == HiveParser.TOK_SUBPARTITIONREF) {
              String spart = unescapeIdentifier(newast.getChild(0).getText());
              if (!subpartitions.contains(spart.toLowerCase())) {
                subpartitions.add(spart.toLowerCase());
              }
              LOG.info("subpartitions add: " + spart);
            }
          } else if ((nnn == 2)
              && (newast.getToken().getType() == HiveParser.TOK_COMPPARTITIONREF)) {
            String part = unescapeIdentifier(newast.getChild(0).getText());
            String spart = unescapeIdentifier(newast.getChild(1).getText());
            String toadd = part.toLowerCase() + "	" + spart.toLowerCase();
            if (!pairpartitions.contains(toadd)) {
              pairpartitions.add(toadd);
            }
            LOG.info("pairpartitions add: " + part + " " + spart);
          }
        }
        showRowCountDesc rowcountdesc = new showRowCountDesc(ctx.getResFile(),
            tblname, isextended, partitions, subpartitions, pairpartitions);
        rootTasks.add(TaskFactory.get(new DDLWork(rowcountdesc), conf));
        setFetchTask(createFetchTask(rowcountdesc.getSchema()));
      } else if (ast.getChildCount() == 3) {
        dbname = ast.getChild(1).getText();
        tblname = ast.getChild(2).getText();
        showRowCountDesc rowcountdesc = new showRowCountDesc(ctx.getResFile(),
            dbname, tblname, isextended);
        rootTasks.add(TaskFactory.get(new DDLWork(rowcountdesc), conf));
        setFetchTask(createFetchTask(rowcountdesc.getSchema()));
      } else {
        dbname = ast.getChild(1).getText();
        tblname = ast.getChild(2).getText();
        partitions = new Vector<String>();
        subpartitions = new Vector<String>();
        pairpartitions = new Vector<String>();
        partitions.clear();
        subpartitions.clear();
        pairpartitions.clear();
        int sss = ast.getChildCount();
        for (int i = 3; i < sss; i++) {
          ASTNode newast = (ASTNode) ast.getChild(i);
          LOG.info("child " + i + " : " + newast.dump());
          int nnn = newast.getChildCount();
          if (nnn == 1) {
            if (newast.getToken().getType() == HiveParser.TOK_PARTITIONREF) {
              String part = unescapeIdentifier(newast.getChild(0).getText());
              if (!partitions.contains(part.toLowerCase())) {
                partitions.add(part.toLowerCase());
              }
              LOG.info("partitions add: " + part);
            } else if (newast.getToken().getType() == HiveParser.TOK_SUBPARTITIONREF) {
              String spart = unescapeIdentifier(newast.getChild(0).getText());
              if (!subpartitions.contains(spart.toLowerCase())) {
                subpartitions.add(spart.toLowerCase());
              }
              LOG.info("subpartitions add: " + spart);
            }
          } else if ((nnn == 2)
              && (newast.getToken().getType() == HiveParser.TOK_COMPPARTITIONREF)) {
            String part = unescapeIdentifier(newast.getChild(0).getText());
            String spart = unescapeIdentifier(newast.getChild(1).getText());
            String toadd = part.toLowerCase() + "	" + spart.toLowerCase();
            if (!pairpartitions.contains(toadd)) {
              pairpartitions.add(toadd);
            }
            LOG.info("pairpartitions add: " + part + " " + spart);
          }
        }
        showRowCountDesc rowcountdesc = new showRowCountDesc(ctx.getResFile(),
            dbname, tblname, isextended, partitions, subpartitions,
            pairpartitions);
        rootTasks.add(TaskFactory.get(new DDLWork(rowcountdesc), conf));
        setFetchTask(createFetchTask(rowcountdesc.getSchema()));
      }
    }
  }

  private void analyzeShowTableSize(ASTNode ast) throws SemanticException {
    boolean isextended = false;
    String tblname = null;
    String dbname = null;
    Vector<String> partitions = null;
    Vector<String> subpartitions = null;
    Vector<String> pairpartitions = null;
    String token = stripQuotes(ast.getChild(0).getText());
    if (token.equalsIgnoreCase("extended")) {
      isextended = true;
      if (ast.getChildCount() == 2) {
        tblname = ast.getChild(1).getText();
        showTableSizeDesc info = new showTableSizeDesc(ctx.getResFile(),
            tblname, isextended);
        rootTasks.add(TaskFactory.get(new DDLWork(info), conf));
        setFetchTask(createFetchTask(info.getSchema()));
      } else if (ast.getChildCount() == 4) {
        LOG.info(ast.dump());
        dbname = ast.getChild(2).getText();
        tblname = ast.getChild(3).getText();
        showTableSizeDesc rowcountdesc = new showTableSizeDesc(
            ctx.getResFile(), dbname, tblname, isextended);
        rootTasks.add(TaskFactory.get(new DDLWork(rowcountdesc), conf));
        setFetchTask(createFetchTask(rowcountdesc.getSchema()));
      } else if (ast.getChildCount() == 3) {
        tblname = ast.getChild(1).getText();
        partitions = new Vector<String>();
        subpartitions = new Vector<String>();
        pairpartitions = new Vector<String>();
        partitions.clear();
        subpartitions.clear();
        pairpartitions.clear();
        int sss = ast.getChildCount();
        for (int i = 2; i < sss; i++) {
          ASTNode newast = (ASTNode) ast.getChild(i);
          LOG.info("child " + i + " : " + newast.dump());
          int nnn = newast.getChildCount();
          if (nnn == 1) {
            if (newast.getToken().getType() == HiveParser.TOK_PARTITIONREF) {
              String part = unescapeIdentifier(newast.getChild(0).getText());
              if (!partitions.contains(part.toLowerCase())) {
                partitions.add(part.toLowerCase());
              }
              LOG.info("partitions add: " + part);
            } else if (newast.getToken().getType() == HiveParser.TOK_SUBPARTITIONREF) {
              String spart = unescapeIdentifier(newast.getChild(0).getText());
              if (!subpartitions.contains(spart.toLowerCase())) {
                subpartitions.add(spart.toLowerCase());
              }
              LOG.info("subpartitions add: " + spart);
            }
          } else if (nnn == 2
              && newast.getToken().getType() == HiveParser.TOK_COMPPARTITIONREF) {
            String part = unescapeIdentifier(newast.getChild(0).getText());
            String spart = unescapeIdentifier(newast.getChild(1).getText());
            String toadd = part.toLowerCase() + "	" + spart.toLowerCase();
            if (!pairpartitions.contains(toadd)) {
              pairpartitions.add(toadd);
            }
            LOG.info("pairpartitions add: " + part + " " + spart);
          }
        }
        showTableSizeDesc info = new showTableSizeDesc(ctx.getResFile(),
            tblname, isextended, partitions, subpartitions, pairpartitions);
        rootTasks.add(TaskFactory.get(new DDLWork(info), conf));
        setFetchTask(createFetchTask(info.getSchema()));
      } else {
        LOG.info(ast.dump());
        dbname = ast.getChild(2).getText();
        tblname = ast.getChild(3).getText();
        partitions = new Vector<String>();
        subpartitions = new Vector<String>();
        pairpartitions = new Vector<String>();
        partitions.clear();
        subpartitions.clear();
        pairpartitions.clear();
        int sss = ast.getChildCount();
        for (int i = 4; i < sss; i++) {
          ASTNode newast = (ASTNode) ast.getChild(i);
          LOG.info("child " + i + " : " + newast.dump());
          int nnn = newast.getChildCount();
          if (nnn == 1) {
            if (newast.getToken().getType() == HiveParser.TOK_PARTITIONREF) {
              String part = unescapeIdentifier(newast.getChild(0).getText());
              if (!partitions.contains(part.toLowerCase())) {
                partitions.add(part.toLowerCase());
              }
              LOG.info("partitions add: " + part);
            } else if (newast.getToken().getType() == HiveParser.TOK_SUBPARTITIONREF) {
              String spart = unescapeIdentifier(newast.getChild(0).getText());
              if (!subpartitions.contains(spart.toLowerCase())) {
                subpartitions.add(spart.toLowerCase());
              }
              LOG.info("subpartitions add: " + spart);
            }
          } else if (nnn == 2
              && newast.getToken().getType() == HiveParser.TOK_COMPPARTITIONREF) {
            String part = unescapeIdentifier(newast.getChild(0).getText());
            String spart = unescapeIdentifier(newast.getChild(1).getText());
            String toadd = part.toLowerCase() + "	" + spart.toLowerCase();
            if (!pairpartitions.contains(toadd)) {
              pairpartitions.add(toadd);
            }
            LOG.info("pairpartitions add: " + part + " " + spart);
          }
        }
        showTableSizeDesc info = new showTableSizeDesc(ctx.getResFile(),
            dbname, tblname, isextended, partitions, subpartitions,
            pairpartitions);
        rootTasks.add(TaskFactory.get(new DDLWork(info), conf));
        setFetchTask(createFetchTask(info.getSchema()));
      }
    } else {
      isextended = false;
      if (ast.getChildCount() == 1) {
        tblname = ast.getChild(0).getText();
        showTableSizeDesc info = new showTableSizeDesc(ctx.getResFile(),
            tblname, isextended);
        rootTasks.add(TaskFactory.get(new DDLWork(info), conf));
        setFetchTask(createFetchTask(info.getSchema()));
      } else if (ast.getChildCount() == 3) {
        dbname = ast.getChild(1).getText();
        tblname = ast.getChild(2).getText();
        showTableSizeDesc rowcountdesc = new showTableSizeDesc(
            ctx.getResFile(), dbname, tblname, isextended);
        rootTasks.add(TaskFactory.get(new DDLWork(rowcountdesc), conf));
        setFetchTask(createFetchTask(rowcountdesc.getSchema()));
      } else if (ast.getChildCount() == 2) {
        tblname = ast.getChild(0).getText();
        partitions = new Vector<String>();
        subpartitions = new Vector<String>();
        pairpartitions = new Vector<String>();
        partitions.clear();
        subpartitions.clear();
        pairpartitions.clear();
        int sss = ast.getChildCount();
        for (int i = 1; i < sss; i++) {
          ASTNode newast = (ASTNode) ast.getChild(i);
          LOG.info("child " + i + " : " + newast.dump());
          int nnn = newast.getChildCount();
          if (nnn == 1) {
            if (newast.getToken().getType() == HiveParser.TOK_PARTITIONREF) {
              String part = unescapeIdentifier(newast.getChild(0).getText());
              if (!partitions.contains(part.toLowerCase())) {
                partitions.add(part.toLowerCase());
              }
              LOG.info("partitions add: " + part);
            } else if (newast.getToken().getType() == HiveParser.TOK_SUBPARTITIONREF) {
              String spart = unescapeIdentifier(newast.getChild(0).getText());
              if (!subpartitions.contains(spart.toLowerCase())) {
                subpartitions.add(spart.toLowerCase());
              }
              LOG.info("subpartitions add: " + spart);
            }
          } else if ((nnn == 2)
              && (newast.getToken().getType() == HiveParser.TOK_COMPPARTITIONREF)) {
            String part = unescapeIdentifier(newast.getChild(0).getText());
            String spart = unescapeIdentifier(newast.getChild(1).getText());
            String toadd = part.toLowerCase() + "	" + spart.toLowerCase();
            if (!pairpartitions.contains(toadd)) {
              pairpartitions.add(toadd);
            }
            LOG.info("pairpartitions add: " + part + " " + spart);
          }
        }
        showTableSizeDesc info = new showTableSizeDesc(ctx.getResFile(),
            tblname, isextended, partitions, subpartitions, pairpartitions);
        rootTasks.add(TaskFactory.get(new DDLWork(info), conf));
        setFetchTask(createFetchTask(info.getSchema()));
      } else {
        dbname = ast.getChild(1).getText();
        tblname = ast.getChild(2).getText();
        partitions = new Vector<String>();
        subpartitions = new Vector<String>();
        pairpartitions = new Vector<String>();
        partitions.clear();
        subpartitions.clear();
        pairpartitions.clear();
        int sss = ast.getChildCount();
        for (int i = 3; i < sss; i++) {
          ASTNode newast = (ASTNode) ast.getChild(i);
          LOG.info("child " + i + " : " + newast.dump());
          int nnn = newast.getChildCount();
          if (nnn == 1) {
            if (newast.getToken().getType() == HiveParser.TOK_PARTITIONREF) {
              String part = unescapeIdentifier(newast.getChild(0).getText());
              if (!partitions.contains(part.toLowerCase())) {
                partitions.add(part.toLowerCase());
              }
              LOG.info("partitions add: " + part);
            } else if (newast.getToken().getType() == HiveParser.TOK_SUBPARTITIONREF) {
              String spart = unescapeIdentifier(newast.getChild(0).getText());
              if (!subpartitions.contains(spart.toLowerCase())) {
                subpartitions.add(spart.toLowerCase());
              }
              LOG.info("subpartitions add: " + spart);
            }
          } else if ((nnn == 2)
              && (newast.getToken().getType() == HiveParser.TOK_COMPPARTITIONREF)) {
            String part = unescapeIdentifier(newast.getChild(0).getText());
            String spart = unescapeIdentifier(newast.getChild(1).getText());
            String toadd = part.toLowerCase() + "	" + spart.toLowerCase();
            if (!pairpartitions.contains(toadd)) {
              pairpartitions.add(toadd);
            }
            LOG.info("pairpartitions add: " + part + " " + spart);
          }
        }
        showTableSizeDesc info = new showTableSizeDesc(ctx.getResFile(),
            dbname, tblname, isextended, partitions, subpartitions,
            pairpartitions);
        rootTasks.add(TaskFactory.get(new DDLWork(info), conf));
        setFetchTask(createFetchTask(info.getSchema()));
      }
    }
  }

  private void analyzeShowInfo(ASTNode ast) throws SemanticException {
    if (ast.getChildCount() == 1) {
      showInfoDesc info = new showInfoDesc(ctx.getResFile(), ast.getChild(0)
          .getText());
      rootTasks.add(TaskFactory.get(new DDLWork(info), conf));
      setFetchTask(createFetchTask(info.getSchema()));
    }
  }

  private void analyzeKillQuery(ASTNode ast) throws SemanticException {
    if (ast.getChildCount() == 1) {
      killqueryDesc killDesc = new killqueryDesc(ast.getChild(0).getText());
      rootTasks.add(TaskFactory.get(new DDLWork(killDesc), conf));
    }
  }

  private void analyzeShowQuery(ASTNode ast) throws SemanticException {
    if (ast.getChildCount() == 1) {
      showqueryDesc showDesc = new showqueryDesc(ast.getChild(0).getText(),
          ctx.getResFile());
      rootTasks.add(TaskFactory.get(new DDLWork(showDesc), conf));
      setFetchTask(createFetchTask(showDesc.getSchema()));
    }
  }

  private void analyzeClearQuery(ASTNode ast) throws Exception {
    if (ast.getChildCount() == 1) {
      int days = 0;
      try {
        days = Integer.parseInt(ast.getChild(0).getText());
      } catch (Exception e) {
        throw new Exception("the input should be integer!");
      }
      clearqueryDesc clearDesc = new clearqueryDesc(days);
      rootTasks.add(TaskFactory.get(new DDLWork(clearDesc), conf));
    } else {
      clearqueryDesc clearDesc = new clearqueryDesc();
      rootTasks.add(TaskFactory.get(new DDLWork(clearDesc), conf));
    }
  }

  private void analyzeShowFunctions(ASTNode ast) throws SemanticException {
    showFunctionsDesc showFuncsDesc;
    if (ast.getChildCount() == 1) {
      String funcNames = unescapeSQLString(ast.getChild(0).getText());
      showFuncsDesc = new showFunctionsDesc(ctx.getResFile(), funcNames);
    } else {
      showFuncsDesc = new showFunctionsDesc(ctx.getResFile());
    }
    rootTasks.add(TaskFactory.get(new DDLWork(showFuncsDesc), conf));
    setFetchTask(createFetchTask(showFuncsDesc.getSchema()));
  }

  private void analyzeDescFunction(ASTNode ast) throws SemanticException {
    String funcName;
    boolean isExtended;

    if (ast.getChildCount() == 1) {
      funcName = ast.getChild(0).getText();
      isExtended = false;
    } else if (ast.getChildCount() == 2) {
      funcName = ast.getChild(0).getText();
      isExtended = true;
    } else {
      throw new SemanticException("Unexpected Tokens at DESCRIBE FUNCTION");
    }

    descFunctionDesc descFuncDesc = new descFunctionDesc(ctx.getResFile(),
        funcName, isExtended);
    rootTasks.add(TaskFactory.get(new DDLWork(descFuncDesc), conf));
    setFetchTask(createFetchTask(descFuncDesc.getSchema()));
  }

  private void analyzeAlterTableRename(ASTNode ast) throws SemanticException {
    alterTableDesc alterTblDesc = new alterTableDesc(unescapeIdentifier(ast
        .getChild(0).getText()), unescapeIdentifier(ast.getChild(1).getText()));
    rootTasks.add(TaskFactory.get(new DDLWork(alterTblDesc), conf));
  }

  private void analyzeAlterTableRenameCol(ASTNode ast) throws SemanticException {
    String tblName = unescapeIdentifier(ast.getChild(0).getText());
    String newComment = null;
    String newType = null;
    newType = getTypeStringFromAST((ASTNode) ast.getChild(3));
    int l = newType == null ? 5 : 6;
    boolean first = false;
    String flagCol = null;
    ASTNode positionNode = null;
    if (ast.getChildCount() == l) {
      newComment = unescapeSQLString(ast.getChild(l - 2).getText());
      positionNode = (ASTNode) ast.getChild(l - 1);
    } else if (ast.getChildCount() == l - 1) {
      if (ast.getChild(l - 2).getType() == HiveParser.StringLiteral)
        newComment = unescapeSQLString(ast.getChild(l - 2).getText());
      else
        positionNode = (ASTNode) ast.getChild(l - 2);
    }

    if (positionNode != null) {
      if (!HiveConf.getBoolVar(conf, ConfVars.ALTERSCHEMAACTIVATEFIRSTAFTER)) {
        throw new SemanticException(
            "first or after kewword is not support rightnow");
      }
      if (positionNode.getChildCount() == 0)
        first = true;
      else
        flagCol = unescapeIdentifier(positionNode.getChild(0).getText());
    }

    alterTableDesc alterTblDesc = new alterTableDesc(tblName,
        unescapeIdentifier(ast.getChild(1).getText()), unescapeIdentifier(ast
            .getChild(2).getText()), newType, newComment, first, flagCol);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(),
        alterTblDesc), conf));
  }

  private void analyzeAlterTableModifyCols(ASTNode ast,
      alterTableTypes alterType) throws SemanticException {
    String tblName = unescapeIdentifier(ast.getChild(0).getText());
    List<FieldSchema> newCols = getColumns((ASTNode) ast.getChild(1));
    alterTableDesc alterTblDesc = new alterTableDesc(tblName, newCols,
        alterType);
    rootTasks.add(TaskFactory.get(new DDLWork(alterTblDesc), conf));
  }

  private void analyzeAlterTableDropParts(ASTNode ast) throws SemanticException {
    String tblName = unescapeIdentifier(ast.getChild(0).getText());

    ArrayList<String> partNames = new ArrayList<String>();
    Boolean isSub = ((ASTNode) ast.getChild(1)).getToken().getType() == HiveParser.TOK_SUBPARTITIONREF ? true
        : false;
    partNames.add(unescapeIdentifier(ast.getChild(1).getChild(0).getText())
        .toLowerCase());
    for (int i = 2; i < ast.getChildCount(); ++i) {
      if (((ASTNode) ast.getChild(i - 1)).getToken().getType() != ((ASTNode) ast
          .getChild(i)).getToken().getType())
        throw new SemanticException(
            "only support one level partition drop once,do not inclde pri and sub partition in one statement!");
      String partName = unescapeIdentifier(ast.getChild(i).getChild(0)
          .getText());
      if (partNames.contains(partName.toLowerCase())) {
        throw new SemanticException("find duplicate partition names: "
            + partName);
      }
      partNames.add(partName.toLowerCase());
    }

    dropTableDesc dropTblDesc = new dropTableDesc(SessionState.get()
        .getDbName(), tblName, isSub, partNames, true);
    rootTasks.add(TaskFactory.get(new DDLWork(dropTblDesc), conf));
  }

  public static String getPartitionDefineValue(ASTNode partdef)
      throws SemanticException {
    String tmpPartDef = null;

    if (partdef.getToken().getType() == HiveParser.CharSetLiteral) {
      tmpPartDef = BaseSemanticAnalyzer.charSetString(partdef.getChild(0)
          .getText(), partdef.getChild(1).getText());
    } else if (partdef.getToken().getType() == HiveParser.StringLiteral) {
      tmpPartDef = unescapeSQLString(partdef.getText());
    } else {
      tmpPartDef = partdef.getText();

    }

    return tmpPartDef;
  }

  private void analyzeAlterTableAddParts(CommonTree ast, Boolean isSubPartition)
      throws SemanticException {

    String tblName = unescapeIdentifier(ast.getChild(0).getText());

    PartitionDesc partDesc = new PartitionDesc();

    LinkedHashMap<String, List<String>> partSpace = new LinkedHashMap<String, List<String>>();

    String parName = unescapeIdentifier(ast.getChild(1).getChild(0).getText());
    List<String> partDef = new ArrayList<String>();

    Boolean isRange = false;
    ASTNode partDefTree = (ASTNode) ast.getChild(1).getChild(1);

    if (partDefTree.getToken().getType() == HiveParser.TOK_LISTPARTITIONDEFINE) {
      LOG.info("add to list partition");
      if (SessionState.get() != null)
        SessionState.get().ssLog("add to list partition");

      partDesc.setPartitionType(PartitionType.LIST_PARTITION);
      for (int i = 0; i < partDefTree.getChildCount(); ++i)
        partDef
            .add(getPartitionDefineValue((ASTNode) (partDefTree.getChild(i))));
    } else if (partDefTree.getToken().getType() == HiveParser.TOK_RANGEPARTITIONDEFINE) {
      LOG.info("add to range partition");
      if (SessionState.get() != null)
        SessionState.get().ssLog("add to range partition");

      isRange = true;
      partDesc.setPartitionType(PartitionType.RANGE_PARTITION);
      partDef.add(getPartitionDefineValue((ASTNode) (partDefTree.getChild(0))));

    } else
      throw new SemanticException("add unknow partition type!");

    partSpace.put(parName.toLowerCase(), partDef);

    if (ast.getChildCount() > 2 && isRange) {
      for (int i = 2; i < ast.getChildCount(); ++i) {
        partDef = new ArrayList<String>();
        partDef.add(getPartitionDefineValue((ASTNode) (ast.getChild(i)
            .getChild(1).getChild(0))));
        if (partSpace.containsKey(unescapeIdentifier(
            ast.getChild(i).getChild(0).getText()).toLowerCase())) {
          throw new SemanticException("duplicate partition name : "
              + unescapeIdentifier(ast.getChild(i).getChild(0).getText()));
        }
        partSpace.put(unescapeIdentifier(ast.getChild(i).getChild(0).getText())
            .toLowerCase(), partDef);
      }

    } else if (ast.getChildCount() > 2 && !isRange) {
      for (int i = 2; i < ast.getChildCount(); ++i) {
        partDef = new ArrayList<String>();
        partDefTree = (ASTNode) ast.getChild(i).getChild(1);
        for (int j = 0; j < partDefTree.getChildCount(); ++j) {
          partDef.add(getPartitionDefineValue((ASTNode) (partDefTree
              .getChild(j))));
        }
        if (partSpace.containsKey(unescapeIdentifier(
            ast.getChild(i).getChild(0).getText()).toLowerCase())) {
          throw new SemanticException("duplicate partition name : "
              + unescapeIdentifier(ast.getChild(i).getChild(0).getText()));
        }
        partSpace.put(unescapeIdentifier(ast.getChild(i).getChild(0).getText())
            .toLowerCase(), partDef);
      }

    }

    partDesc.setPartitionSpaces(partSpace);
    if (true) {
      AddPartitionDesc addPartitionDesc = new AddPartitionDesc(SessionState
          .get().getDbName(), tblName, partDesc, isSubPartition);
      rootTasks.add(TaskFactory.get(new DDLWork(addPartitionDesc), conf));
    }

  }

  private void analyzeMetastoreCheck(CommonTree ast) throws SemanticException {
    String tableName = null;

    QB.tableRef trf = null;

    if (ast.getChildCount() >= 1) {
      tableName = unescapeIdentifier(ast.getChild(0).getText());

      if (ast.getChildCount() == 2) {

        String pri = null;
        String sub = null;
        if (((ASTNode) (ast.getChild(1))).getToken().getType() == HiveParser.TOK_PARTITIONREF)
          pri = unescapeIdentifier(ast.getChild(1).getChild(0).getText());
        else
          sub = unescapeIdentifier(ast.getChild(1).getChild(0).getText());
        trf = new QB.tableRef(SessionState.get().getDbName(), tableName, null,
            pri, sub);
      }
    }

    MsckDesc checkDesc = new MsckDesc(tableName, trf, ctx.getResFile());
    rootTasks.add(TaskFactory.get(new DDLWork(checkDesc), conf));
  }

  private List<Map<String, String>> getPartitionSpecs(CommonTree ast)
      throws SemanticException {
    List<Map<String, String>> partSpecs = new ArrayList<Map<String, String>>();
    int childIndex = 0;

    return partSpecs;
  }

  private void analyzeCreateDatabase(ASTNode ast) throws SemanticException {
    createDatabaseDesc cdd = new createDatabaseDesc(unescapeIdentifier(ast
        .getChild(0).getText()));
    if (ast.getChild(1) != null) {
      ASTNode child = (ASTNode) ast.getChild(1);
      if (child.getToken().getType() == HiveParser.TOK_PROPERTIESWITH) {
        int childnum = child.getChildCount();

        for (int index = 0; index < childnum; index++) {
          ASTNode prop = (ASTNode) child.getChild(index);
          if (prop.getToken().getType() != HiveParser.TOK_TABLEPROPERTY) {
            throw (new SemanticException(
                "create database only support set hdfsschema now"));
          }

          String key = prop.getChild(0).getText();
          String value = prop.getChild(1).getText();
          if (key.endsWith("'") || key.endsWith("\""))
            key = unescapeSQLString(key);
          else
            key = unescapeIdentifier(key);

          if (value.endsWith("'") || value.endsWith("\""))
            value = unescapeSQLString(value);
          else
            value = unescapeIdentifier(value);

          if (key.equalsIgnoreCase("hdfsscheme") || key.equalsIgnoreCase("hdfsschema")) {
            if (!value.trim().startsWith("hdfs://")) {
              throw (new SemanticException(
                  "create database only support set hdfsschema now"));
            }

            cdd.setHdfsschema(value.trim());
            //metastore is for set this database' meta data segment db url
            //it is not public supported now bacause we use hash value route the segment
          } else if (key.equalsIgnoreCase("metastore")) {
            cdd.setDBStore(value);
          } else {
            throw (new SemanticException(
                "create database only support set hdfsschema now"));
          }
        }

        // if(!value.trim().startsWith("hdfs://")){

      } else {
        throw (new SemanticException(
            "create database only support with properies now"));
      }
    }
    rootTasks.add(TaskFactory.get(new DDLWork(cdd), conf));
  }

  private void analyzeDropDatabase(ASTNode ast) {
    dropDatabaseDesc ddd = new dropDatabaseDesc(unescapeIdentifier(ast
        .getChild(0).getText()));
    rootTasks.add(TaskFactory.get(new DDLWork(ddd), conf));
  }

  private void analyzeUseDatabase(ASTNode ast) {
    useDatabaseDesc udd = new useDatabaseDesc(unescapeIdentifier(ast
        .getChild(0).getText()));
    rootTasks.add(TaskFactory.get(new DDLWork(udd), conf));
  }

  private void analyzeShowDatabases(ASTNode ast) throws SemanticException {
    showDBDesc showDbDesc = new showDBDesc(ctx.getResFile());

    if (ast.getChild(0) != null) {
      ASTNode child = (ASTNode) ast.getChild(0);

      if (child.getToken().getType() == HiveParser.TOK_PROPERTIESWITH) {
        int childnum = child.getChildCount();

        if (childnum != 1) {
          throw (new SemanticException(
              "show database only support set owner now"));
        }

        for (int index = 0; index < childnum; index++) {
          ASTNode prop = (ASTNode) child.getChild(index);
          if (prop.getToken().getType() != HiveParser.TOK_TABLEPROPERTY) {
            throw (new SemanticException(
                "show database only support set owner now"));
          }

          String key = prop.getChild(0).getText();
          String value = prop.getChild(1).getText();
          if (key.endsWith("'") || key.endsWith("\""))
            key = unescapeSQLString(key);
          else
            key = unescapeIdentifier(key);

          if (value.endsWith("'") || value.endsWith("\""))
            value = unescapeSQLString(value);
          else
            value = unescapeIdentifier(value);

          if (key.equalsIgnoreCase("owner")) {
            showDbDesc.setOwner(value);
          } else {
            throw new SemanticException(
                "show database only support set owner now");
          }
        }
      } else {
        throw new SemanticException(
            "create database only support with properies now");
      }
    }

    LOG.debug("resFile : " + ctx.getResFile());
    rootTasks.add(TaskFactory.get(new DDLWork(showDbDesc), conf));
    setFetchTask(createFetchTask(showDbDesc.getSchema()));
  }

  private void analyzeShowCreateTable(ASTNode ast) {
    showCreateTableDesc showCT = new showCreateTableDesc(ctx.getResFile());
    showCT.setTable(unescapeIdentifier(ast.getChild(0).getText()));
    LOG.debug("resFile : " + ctx.getResFile());
    rootTasks.add(TaskFactory.get(new DDLWork(showCT), conf));
    setFetchTask(createFetchTask(showCT.getSchema()));
  }

  private void analyzeShowTableIndex(ASTNode ast) {
    String tblName = ast.getChild(0).getText();
    showIndexDesc showIndexsDesc = new showIndexDesc("", tblName,
        showIndexTypes.SHOWTABLEINDEX);

    rootTasks.add(TaskFactory.get(new DDLWork(showIndexsDesc), conf));
  }

  private void analyzeShowAllIndex(ASTNode ast) {
    showIndexDesc showIndexsDesc = new showIndexDesc("", "",
        showIndexTypes.SHOWALLINDEX);

    rootTasks.add(TaskFactory.get(new DDLWork(showIndexsDesc), conf));
  }

  private void analyzeShowVersion(ASTNode ast) {
    showVersionDesc ShowVersionDesc = new showVersionDesc(ctx.getResFile());
    rootTasks.add(TaskFactory.get(new DDLWork(ShowVersionDesc), conf));
    setFetchTask(createFetchTask(ShowVersionDesc.getSchema()));
  }
}
