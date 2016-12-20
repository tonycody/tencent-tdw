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

package org.apache.hadoop.hive.ql.exec;

import java.io.BufferedWriter;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.lang.annotation.Annotation;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Vector;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AddSerdeDesc;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.IndexItem;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.RenameColDesc;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.tdw_query_info;
import org.apache.hadoop.hive.ql.dataImport.ExtractConfig;
import org.apache.hadoop.hive.ql.dataToDB.BaseDBExternalDataLoad;
import org.apache.hadoop.hive.ql.dataToDB.LoadConfig;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.GBKIgnoreKeyOutputFormat;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.metadata.CheckResult;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveMetaStoreChecker;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.SemanticException;
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
import org.apache.hadoop.hive.ql.plan.createViewDesc;
import org.apache.hadoop.hive.ql.plan.descFunctionDesc;
import org.apache.hadoop.hive.ql.plan.descTableDesc;
import org.apache.hadoop.hive.ql.plan.dropDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.dropTableDesc;
import org.apache.hadoop.hive.ql.plan.execExtSQLDesc;
import org.apache.hadoop.hive.ql.plan.killqueryDesc;
import org.apache.hadoop.hive.ql.plan.loadTableDesc;
import org.apache.hadoop.hive.ql.plan.showCreateTableDesc;
import org.apache.hadoop.hive.ql.plan.showDBDesc;
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
import org.apache.hadoop.hive.ql.plan.truncatePartitionDesc;
import org.apache.hadoop.hive.ql.plan.truncateTableDesc;
import org.apache.hadoop.hive.ql.plan.useDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.showIndexDesc.showIndexTypes;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.dynamic_type.DynamicSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.protobuf.ProtobufSerDe;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;

import StorageEngineClient.FormatStorageSerDe;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.lib.Node;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;

import org.apache.hadoop.hive.ql.dataToDB.StoreAsPgdata;

import org.apache.commons.lang.StringEscapeUtils;

public class DDLTask extends Task<DDLWork> implements Serializable {
  private static final long serialVersionUID = 1L;
  static final private Log LOG = LogFactory.getLog("hive.ql.exec.DDLTask");

  transient HiveConf conf;
  static final private int separator = Utilities.tabCode;
  static final private int terminator = Utilities.newLineCode;


  public void initialize(HiveConf conf, DriverContext ctx) {
    super.initialize(conf, ctx);
    this.conf = conf;
  }

  public int execute() {

    Hive db;
    try {
      db = Hive.get(conf);

      createTableDesc crtTbl = work.getCreateTblDesc();
      if (crtTbl != null) {
        return createTable(db, crtTbl);
      }

      truncatePartitionDesc tpd = work.getTruncatePartDesc();
      if (tpd != null) {
        return truncatePartition(db, tpd);
      }

      createTableLikeDesc crtTblLike = work.getCreateTblLikeDesc();
      if (crtTblLike != null) {
        return createTableLike(db, crtTblLike);
      }

      dropTableDesc dropTbl = work.getDropTblDesc();
      if (dropTbl != null) {
        return dropTable(db, dropTbl);
      }

      truncateTableDesc truncTbl = work.getTruncTblDesc();
      if (truncTbl != null) {
        return truncateTable(db, truncTbl);
      }

      alterTableDesc alterTbl = work.getAlterTblDesc();
      if (alterTbl != null) {
        return alterTable(db, alterTbl);
      }

      alterCommentDesc acd = work.getAlterCommentDesc();
      if (acd != null) {
        return alterComment(db, acd);
      }

      createViewDesc crtView = work.getCreateViewDesc();
      if (crtView != null) {
        return createView(db, crtView);
      }

      AddPartitionDesc addPartitionDesc = work.getAddPartitionDesc();
      if (addPartitionDesc != null) {
        return addPartition(db, addPartitionDesc);
      }

      MsckDesc msckDesc = work.getMsckDesc();
      if (msckDesc != null) {
        return msck(db, msckDesc);
      }

      descTableDesc descTbl = work.getDescTblDesc();
      if (descTbl != null) {
        return describeTable(db, descTbl);
      }

      descFunctionDesc descFunc = work.getDescFunctionDesc();
      if (descFunc != null) {
        return describeFunction(descFunc);
      }

      showTablesDesc showTbls = work.getShowTblsDesc();
      if (showTbls != null) {
        return showTables(db, showTbls);
      }

      showRowCountDesc showrcount = work.getShowRowCountDesc();
      if (showrcount != null) {
        return showRowCount(db, showrcount);
      }

      showDatabaseSizeDesc showdbsz = work.getShowDatabaseSizeDesc();
      if (showdbsz != null) {
        return showDBSize(db, showdbsz);
      }

      showTableSizeDesc showtblsz = work.getShowTableSizeDesc();
      if (showtblsz != null) {
        return showTableSize(db, showtblsz);
      }

      showProcesslistDesc showpros = work.getShowProcesslistDesc();
      if (showpros != null) {
        return showProcesslist(db, showpros);
      }

      killqueryDesc killDesc = work.getKillQueryDesc();
      if (killDesc != null) {
        return killQuery(db, killDesc);
      }

      showqueryDesc showDesc = work.getShowQueryDesc();
      if (showDesc != null) {
        return showQuery(db, showDesc);
      }

      clearqueryDesc cDesc = work.getClearQueryDesc();
      if (cDesc != null) {
        return clearQuery(db, cDesc);
      }

      showInfoDesc infoDesc = work.getShowInfoDesc();
      if (infoDesc != null) {
        return showInfo(db, infoDesc);
      }

      showFunctionsDesc showFuncs = work.getShowFuncsDesc();
      if (showFuncs != null) {
        return showFunctions(showFuncs);
      }

      showPartitionsDesc showParts = work.getShowPartsDesc();
      if (showParts != null) {
        return showPartitions(db, showParts);
      }

      addDefaultPartitionDesc addDefaultPart = work.getAddDefaultPartDesc();
      if (addDefaultPart != null) {
        return addDefaultPartition(db, addDefaultPart);
      }

      createDatabaseDesc createDb = work.getCreateDbDesc();
      if (createDb != null) {
        return createDatabase(db, createDb);
      }

      dropDatabaseDesc dropDb = work.getDropDbDesc();
      if (dropDb != null) {
        return dropDatabase(db, dropDb);
      }

      useDatabaseDesc useDb = work.getUseDbDesc();
      if (useDb != null) {
        return useDatabase(db, useDb);
      }

      showDBDesc showDb = work.getShowDbDesc();

      if (showDb != null) {
        return showDatabases(db, showDb);
      }
      showIndexDesc showIndex = work.getShowIndexDesc();
      if (showIndex != null) {
        return showIndex(db, showIndex);
      }

      showCreateTableDesc showCT = work.getShowCT();
      if (showCT != null) {
        return showCreateTable(db, showCT);
      }

      showVersionDesc versionDesc = work.getVersionDesc();
      if (versionDesc != null) {
        return showVersion(db, versionDesc);
      }

      execExtSQLDesc execExtSqldesc = work.getExecsqldesc();
      if (execExtSqldesc != null)
        return exeExtSQL(execExtSqldesc);
      showViewTablesDesc viewTableDesc = work.getViewTablesDesc();
      if (viewTableDesc != null)
        return exeViewTables(db, viewTableDesc);

    } catch (InvalidTableException e) {
      console.printError("Table " + e.getTableName() + " does not exist");
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "Table " + e.getTableName() + " does not exist");
      LOG.debug(StringUtils.stringifyException(e));
      return 1;
    } catch (HiveException e) {
      console.printError("FAILED: Error in metadata: " + e.getMessage(), "\n"
          + StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "FAILED: Error in metadata: " + e.getMessage() + "\n"
                + StringUtils.stringifyException(e));
      LOG.debug(StringUtils.stringifyException(e));
      return 1;
    } catch (Exception e) {
      console.printError("Failed with exception " + e.getMessage(), "\n"
          + StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "Failed with exception " + e.getMessage() + "\n"
                + StringUtils.stringifyException(e));
      return (1);
    }
    assert false;
    return 0;
  }

  private int addPartition(Hive db, AddPartitionDesc addPartitionDesc)
      throws HiveException, MetaException, TException,
      InvalidOperationException {
    try {
      if (!db.hasAuth(SessionState.get().getUserName(),
          Hive.Privilege.ALTER_PRIV, addPartitionDesc.getDbName(),
          addPartitionDesc.getTableName())) {
        if(db.isLhotseImportExport()){
          if (SessionState.get() != null)
            SessionState.get().ssLog("user : " + SessionState.get().getUserName()
                + " do not have alter privilege on table : "
                + addPartitionDesc.getDbName() + "."
                + addPartitionDesc.getTableName()
                + ",but it's a lhotse import or export task");
        }else{       
          console.printError("user : " + SessionState.get().getUserName()
              + " do not have alter privilege on table : "
              + addPartitionDesc.getDbName() + "."
              + addPartitionDesc.getTableName());

          if (SessionState.get() != null)
            SessionState.get().ssLog(
                "user : " + SessionState.get().getUserName()
                + " do not have alter privilege on table : "
                + addPartitionDesc.getDbName() + "."
                + addPartitionDesc.getTableName());
          return 1;
        }
      }

      db.addPartitions(addPartitionDesc.getDbName(),
          addPartitionDesc.getTableName(), addPartitionDesc);
    } catch (Exception e) {
      console.printError("add partition error: " + e.getMessage());
      e.printStackTrace();
      if (SessionState.get() != null) {
        SessionState.get().ssLog("add partition error: " + e.getMessage());
      }
      return 1;
    }

    return 0;
  }

  private int exeExtSQL(execExtSQLDesc desc) throws HiveException,
      ClassNotFoundException, IOException {

    String url = null;
    LOG.debug("dbtype : " + desc.getDbtype());
    if (desc.getDbtype().equals("pgsql")) {
      Class.forName("org.postgresql.Driver");
      url = "jdbc:postgresql://" + desc.getProp().getProperty("ip") + ":"
          + desc.getProp().getProperty("port") + "/"
          + desc.getProp().getProperty("db_name");
    } else if (desc.getDbtype().equals("oracle")) {
      Class.forName("oracle.jdbc.driver.OracleDriver");
      url = "jdbc:oracle:thin:@" + desc.getProp().getProperty("ip") + ":"
          + desc.getProp().getProperty("port") + ":"
          + desc.getProp().getProperty("db_name");
    }

    LOG.debug("get connect : " + url);
    Connection conn = null;
    int rt = 0;

    desc.getProp().setProperty("password", desc.getProp().getProperty("pwd"));
    desc.getProp().setProperty("user", desc.getProp().getProperty("user_name"));

    try {
      conn = DriverManager.getConnection(url, desc.getProp());
      try {
        String processName = java.lang.management.ManagementFactory
            .getRuntimeMXBean().getName();
        String processID = processName.substring(0, processName.indexOf('@'));
        String appinfo = "exeExtSQL_" + processID + "_"
            + SessionState.get().getSessionName();
        conn.setClientInfo("ApplicationName", appinfo);
      } catch (Exception e) {
        e.printStackTrace();
      }

      rt = conn.createStatement().executeUpdate(desc.getSql());
      conn.close();
    } catch (SQLException e) {
      LOG.debug(e.getLocalizedMessage());
      LOG.debug(e.getMessage());
      LOG.debug(e.getSQLState());
      LOG.debug(e.getErrorCode());
      throw new HiveException("run external SQL error! message : "
          + e.getMessage() + " ,SQLState : " + e.getSQLState()
          + " ,ErrorCode : " + e.getErrorCode());

    }

    FileSystem fs = desc.getResFile().getFileSystem(conf);
    DataOutput outStream = (DataOutput) fs.create(desc.getResFile());
    outStream.writeBytes("" + rt + " rows affected");
    outStream.write(terminator);
    ((FSDataOutputStream) outStream).close();

    return 0;
  }

  private int exeViewTables(Hive db, showViewTablesDesc desc)
      throws HiveException, ClassNotFoundException, IOException {

    Table tbl = db
        .getTable(SessionState.get().getDbName(), desc.getView_name());
    String tables = tbl.getViewTables();
    String[] tts = tables.split(";");
    FileSystem fs = desc.getResFile().getFileSystem(conf);
    DataOutput outStream = (DataOutput) fs.create(desc.getResFile());
    for (String s : tts) {
      outStream.writeBytes(desc.getView_name() + "   ");
      String[] names = s.split("::");
      outStream.writeBytes(s + "   ");
      Table tbl2 = null;
      try {
        tbl2 = db.getTable(names[0], names[1]);
      } catch (Exception e) {
        tbl2 = null;
      }
      if (tbl2 == null)
        outStream.writeBytes("N   ");
      else
        outStream.writeBytes("Y   ");
      outStream.write(terminator);
    }
    outStream.write(terminator);
    ((FSDataOutputStream) outStream).close();

    return 0;
  }

  private int addDefaultPartition(Hive db, addDefaultPartitionDesc adpd)
      throws HiveException, MetaException, TException,
      InvalidOperationException {
    try {
      if (!db.hasAuth(SessionState.get().getUserName(),
          Hive.Privilege.ALTER_PRIV, adpd.getDbName(), adpd.getTblName())) {
        if(db.isLhotseImportExport()){
          if (SessionState.get() != null)
            SessionState.get().ssLog("user : " + SessionState.get().getUserName()
                + " do not have alter privilege on table : "
                + adpd.getDbName() + "."
                + adpd.getTblName()
                + ",but it's a lhotse import or export task");
        }else{
          console.printError("user : " + SessionState.get().getUserName()
              + " do not have privilege on table : " + adpd.getDbName() + "."
              + adpd.getTblName());

          if (SessionState.get() != null) {
            SessionState.get().ssLog(
                "user : " + SessionState.get().getUserName()
                + " do not have privilege on table : " + adpd.getDbName()
                + "." + adpd.getTblName());
          }
        }
      }

      db.addDefalutPriPartition(adpd.getDbName(), adpd.getTblName(),
          adpd.isSub() ? 1 : 0);
    } catch (Exception e) {
      console.printError("add default partition error : " + e.getMessage());
      if (SessionState.get() != null) {
        SessionState.get().ssLog(
            "add default partition error : " + e.getMessage());
      }
      return 1;
    }

    return 0;
  }

  private int truncatePartition(Hive db, truncatePartitionDesc tpd)
      throws HiveException, MetaException, TException,
      InvalidOperationException {
    try {

      if (!db.hasAuth(SessionState.get().getUserName(),
          Hive.Privilege.DELETE_PRIV, tpd.getDbName(), tpd.getTblName())) {
        if(db.isLhotseImportExport()){
          if (SessionState.get() != null)
            SessionState.get().ssLog("user : " + SessionState.get().getUserName()
                + " do not have delete (or trancate) privilege on table : "
                + tpd.getDbName() + "."
                + tpd.getTblName()
                + ",but it's a lhotse import or export task");
        }else{
          console.printError("user : " + SessionState.get().getUserName()
              + " do not have delete (or trancate) privilege on table : "
              + tpd.getDbName() + "." + tpd.getTblName());

          if (SessionState.get() != null) {
            SessionState.get().ssLog(
                "user : " + SessionState.get().getUserName()
                + " do not have delete (or trancate) privilege on table : "
                + tpd.getDbName() + "." + tpd.getTblName());
          }

          return 1;
        }
      }

      Table tbl = db.getTable(tpd.getDbName(), tpd.getTblName());
      if (tbl.isView()) {
        throw new HiveException("Cannot use ALTER TABLE on a view");
      }
      if (!tbl.isPartitioned()) {
        console.printError("table: " + tpd.getDbName() + "." + tpd.getTblName()
            + " is not partitioned!");
        if (SessionState.get() != null) {
          SessionState.get().ssLog(
              "table: " + tpd.getDbName() + "." + tpd.getTblName()
                  + " is not partitioned!");
        }
        return 1;
      }

      if (tpd.getPriPartName() != null
          && !tbl.getTTable().getPriPartition().getParSpaces()
              .containsKey(tpd.getPriPartName().toLowerCase())) {
        console.printError("table: " + tpd.getDbName() + "." + tpd.getTblName()
            + " do not contain pri partition : " + tpd.getPriPartName());
        if (SessionState.get() != null) {
          SessionState.get().ssLog(
              "table: " + tpd.getDbName() + "." + tpd.getTblName()
                  + " do not contain pri partition : " + tpd.getPriPartName());
        }

        return 2;
      }
      if (tpd.getSubPartName() != null
          && !tbl.getTTable().getSubPartition().getParSpaces()
              .containsKey(tpd.getSubPartName().toLowerCase())) {
        console.printError("table: " + tpd.getDbName() + "." + tpd.getTblName()
            + " do not contain sub partition : " + tpd.getSubPartName());
        if (SessionState.get() != null) {
          SessionState.get().ssLog(
              "table: " + tpd.getDbName() + "." + tpd.getTblName()
                  + " do not contain sub partition : " + tpd.getSubPartName());
        }

        return 3;
      }

      db.truncatePartition(tbl, tpd.getPriPartName(), tpd.getSubPartName());
    } catch (Exception e) {
      console.printError("truncate partition error: " + e.getMessage());
      if (SessionState.get() != null) {
        SessionState.get().ssLog("truncate partition error: " + e.getMessage());
      }

      return 1;

    }

    return 0;
  }

  private int msck(Hive db, MsckDesc msckDesc) {

    CheckResult result = new CheckResult();
    try {
      HiveMetaStoreChecker checker = new HiveMetaStoreChecker(db);
      checker.checkMetastore(SessionState.get().getDbName(),
          msckDesc.getTableName(), msckDesc.getTrf(), result);
    } catch (HiveException e) {
      LOG.warn("Failed to run metacheck: ", e);
      if (SessionState.get() != null)
        SessionState.get().ssLog("Failed to run metacheck: " + e);
      return 1;
    } catch (IOException e) {
      LOG.warn("Failed to run metacheck: ", e);
      if (SessionState.get() != null)
        SessionState.get().ssLog("Failed to run metacheck: " + e);
      return 1;
    } finally {

      BufferedWriter resultOut = null;
      try {
        FileSystem fs = msckDesc.getResFile().getFileSystem(conf);
        resultOut = new BufferedWriter(new OutputStreamWriter(
            fs.create(msckDesc.getResFile())));

        boolean firstWritten = false;
        firstWritten |= writeMsckResult(result.getTablesNotInMs(),
            "Tables not in metastore:", resultOut, firstWritten);
        firstWritten |= writeMsckResult(result.getTablesNotOnFs(),
            "Tables missing on filesystem:", resultOut, firstWritten);
        firstWritten |= writeMsckResult(result.getPartitionsNotInMs(),
            "Partitions not in metastore:", resultOut, firstWritten);
        firstWritten |= writeMsckResult(result.getPartitionsNotOnFs(),
            "Partitions missing from filesystem:", resultOut, firstWritten);
      } catch (IOException e) {
        LOG.warn("Failed to save metacheck output: ", e);
        if (SessionState.get() != null)
          SessionState.get().ssLog("Failed to save metacheck output: " + e);
        return 1;
      } finally {
        if (resultOut != null) {
          try {
            resultOut.close();
          } catch (IOException e) {
            LOG.warn("Failed to close output file: ", e);
            if (SessionState.get() != null)
              SessionState.get().ssLog("Failed to close output file: " + e);
            return 1;
          }
        }
      }
    }

    return 0;
  }

  private boolean writeMsckResult(List<? extends Object> result, String msg,
      Writer out, boolean wrote) throws IOException {

    if (!result.isEmpty()) {
      if (wrote) {
        out.write(terminator);
      }

      out.write(msg);
      for (Object entry : result) {
        out.write(separator);
        out.write(entry.toString());
      }
      return true;
    }

    return false;
  }

  private int showPartitions(Hive db, showPartitionsDesc showParts)
      throws HiveException {
    String tabName = showParts.getTabName();
    List<List<String>> parts = null;

    String usrname = SessionState.get().getUserName();
    String dbname = SessionState.get().getDbName();
    if (!db.hasAuth(usrname, Hive.Privilege.SELECT_PRIV, dbname, tabName)) {
      console.printError("user : " + usrname
          + " do not have privilege on table : " + dbname + "." + tabName);
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "user : " + usrname + " do not have privilege on table : " + dbname
                + "." + tabName);
      return 1;
    }

    try {
      parts = db.getPartitionNames(dbname, tabName, Short.MAX_VALUE);
    } catch (HiveException e) {
      console.printError(e.getMessage());
      if (SessionState.get() != null) {
        SessionState.get().ssLog(e.getMessage());
      }

      return 1;
    }

    try {
      FileSystem fs = showParts.getResFile().getFileSystem(conf);
      DataOutput outStream = (DataOutput) fs.create(showParts.getResFile());

      Iterator<String> iterParts = parts.get(0).iterator();

      outStream.writeBytes("pri partitions:");
      outStream.write(terminator);

      while (iterParts.hasNext()) {
        outStream.writeBytes(iterParts.next());
        outStream.write(terminator);
      }

      if (parts.size() == 2 && parts.get(1) != null && parts.get(1).size() > 0) {
        iterParts = parts.get(1).iterator();
        outStream.writeBytes("sub partitions:");
        outStream.write(terminator);

        while (iterParts.hasNext()) {
          outStream.writeBytes(iterParts.next());
          outStream.write(terminator);
        }
      }

      ((FSDataOutputStream) outStream).close();
    } catch (FileNotFoundException e) {
      LOG.info("show partitions: " + StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "show partitions: " + StringUtils.stringifyException(e));
      throw new HiveException(e.toString());
    } catch (IOException e) {
      LOG.info("show partitions: " + StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "show partitions: " + StringUtils.stringifyException(e));
      throw new HiveException(e.toString());
    } catch (Exception e) {
      throw new HiveException(e.toString());
    }

    return 0;
  }

  private int showRowCount(Hive db, showRowCountDesc showrcount)
      throws Exception {
    String testatfir = HiveConf.getVar(conf,
        HiveConf.ConfVars.OPENSHOWTABLESIZE);
    if (testatfir != null && testatfir.equalsIgnoreCase("false")) {
      LOG.info("SHOW RowCount IS CLOSED!");
      return 0;
    }

    String dbname = null;
    if (showrcount.getDBName() == null) {
      dbname = SessionState.get().getDbName();
    } else {
      dbname = showrcount.getDBName();
    }

    String usrname = SessionState.get().getUserName();
    String tbname = showrcount.getTableName();
    if (!db.hasAuth(usrname, Hive.Privilege.SELECT_PRIV, dbname, tbname)) {
      console.printError("user : " + usrname
          + " do not have privilege on table : " + dbname + "." + tbname);
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "user : " + usrname + " do not have privilege on table : " + dbname
                + "." + tbname);
      return 1;
    }

    ArrayList<ArrayList<String>> parts = null;
    Table tbl = null;
    Vector<String> parttitions = null;
    Vector<String> subparttitions = null;
    Vector<String> pairs = null;
    Vector<String> result = null;
    boolean hashashpart = false;

    try {
      tbl = db.getTable(dbname, tbname);
    } catch (Exception e) {
      throw new Exception("table " + dbname + "::" + tbname
          + " can not be found!");
    }
    if (tbl == null) {
      throw new Exception("table can not be found!");
    }

    if (tbl.isView()) {
      throw new Exception("Should not use Show RowCount with View!");
    }

    String tmpprop = tbl.getProperty("type");
    LOG.info("type:  " + tmpprop);

    int tbType = -1;
    if (tmpprop.equalsIgnoreCase("format")) {
      tbType = 1;
    } else if (tmpprop.equalsIgnoreCase("rcfile")) {
      tbType = 4;
    }
    if (tmpprop == null || tbType == -1) {
      throw new Exception("row count only support format table!");
    }

    if (!tbl.isPartitioned() && showrcount.getIsPartition()) {
      LOG.info("Table " + tbname + " is not a partitioned table");
      throw new Exception("Table " + tbname + " is not a partitioned table");
    }

    if (!tbl.isPartitioned()) {
      Path pp = tbl.getPath();
      LOG.info("Path: " + pp.toString());
      long re = db.getRowCount(pp, tbType);
      if (re < 0) {
        throw new Exception("failed to get the row count of " + tbname);
      }
      try {
        FileSystem fs = showrcount.getResFile().getFileSystem(conf);
        DataOutput outStream = (DataOutput) fs.create(showrcount.getResFile());
        outStream.writeBytes("row count: ");
        outStream.write(terminator);
        outStream.writeBytes(tbname + "	" + re);
        outStream.write(terminator);
        ((FSDataOutputStream) outStream).close();
      } catch (FileNotFoundException e) {
        LOG.warn("showtablesize: " + StringUtils.stringifyException(e));
        return 1;
      } catch (IOException e) {
        LOG.warn("showtablesize: " + StringUtils.stringifyException(e));
        return 1;
      } catch (Exception e) {
        throw new HiveException(e.toString());
      }

      return 0;
    } else {
      Path pp = tbl.getPath();
      parts = db.getPartitionNames(tbl.getDbName(), tbl.getTTable(),
          Short.MAX_VALUE);

      parttitions = showrcount.getPartitions();
      subparttitions = showrcount.getSubpartitions();
      pairs = showrcount.getPairpartitions();

      if (parts.size() == 2 && parts.get(1) != null) {
        if (parts.get(1).get(0).startsWith("hash(")) {
          if (subparttitions != null && subparttitions.size() > 0) {
            throw new Exception("subparttition is hash partition, can not show");
          }
          if (pairs != null && pairs.size() > 0) {
            throw new Exception("subparttition is hash partition, can not show");
          }

          long totalsize = 0;
          hashashpart = true;
          result = new Vector<String>();

          if (parttitions == null) {
            if (showrcount.getIsExtended()) {
              for (String mypa : parts.get(0)) {
                Path path2 = new Path(pp, mypa);
                long re = db.getRowCountForHash(path2, tbType);
                if (re > 0) {
                  totalsize += re;
                  result.add(mypa + "	" + re);
                } else {
                  result.add(mypa + "	" + 0);
                }
              }
              result.add("row count:" + "	" + totalsize);
            } else {
              for (String mypa : parts.get(0)) {
                Path path2 = new Path(pp, mypa);
                long re = db.getRowCountForHash(path2, tbType);
                if (re > 0)
                  totalsize += re;
              }
              result.add("row count:" + "	" + totalsize);
            }
          } else {
            if (parttitions.size() > 0) {
              for (String partname : parttitions) {
                Path path2 = new Path(pp, partname);
                long re = db.getRowCountForHash(path2, tbType);
                if (re > 0)
                  result.add(partname + "	" + re);
                else
                  result.add(partname + "	" + 0);
              }
            }
          }
        }
      } else {
        if (parts.get(0).get(0).startsWith("hash(")) {
          if (subparttitions != null && subparttitions.size() > 0) {
            throw new Exception("subparttition is not exist");
          }
          if (pairs != null && pairs.size() > 0) {
            throw new Exception("subparttition is not exist");
          }
          if (parttitions != null && parttitions.size() > 0) {
            throw new Exception("pripartition is hash partition");
          }

          hashashpart = true;
          long re = db.getRowCountForHash(pp, tbType);
          result = new Vector<String>();
          if (re > 0)
            result.add("row count:" + "	" + re);
          else
            result.add("row count:" + "	" + 0);
        }
      }

      if ((parttitions == null) && (subparttitions == null) && (pairs == null)
          && (!hashashpart)) {
        if (showrcount.getIsExtended()) {
          result = db.getAllPartitionsRowCount(pp, parts, tbType);
        } else {
          result = new Vector<String>();
          long re = db.getTableTotalRowCount(pp, parts, tbType);
          if (re > 0)
            result.add("row count:" + "	" + re);
          else
            result.add("row count:" + "	" + 0);
        }
      } else if (!hashashpart) {
        result = new Vector<String>();
        if (parttitions.size() > 0) {
          result.add("pri partitions:");
          for (String part : parttitions) {
            long tmpre = db.getPartitionRowCount(pp, parts, part, tbType);
            if (tmpre == -2) {
              throw new Exception(part + " is not a pri partition name!");
            } else if (tmpre == -4) {
              throw new Exception("get row count failed for pri partition: "
                  + part);
            }
            result.add(part + "	" + tmpre);
          }
        }
        if (subparttitions.size() > 0) {
          result.add("sub partitions:");
          for (String part : subparttitions) {
            long tmpre = db.getSubPartitionRowCount(pp, parts, part, tbType);
            if (tmpre == -1) {
              throw new Exception(tbname + " has no sub partition!");
            } else if (tmpre == -3) {
              throw new Exception(part + " is not a sub partition name!");
            } else if (tmpre == -4) {
              throw new Exception("get size failed for sub partition: " + part);
            }
            result.add(part + "	" + tmpre);
          }
        }
        if (pairs.size() > 0) {
          result.add("partitions:");
          for (String part : pairs) {
            String[] strpair = part.split("	");
            if (strpair.length != 2) {
              throw new Exception(part + " is not right!");
            }
            String partname = strpair[0];
            String spartname = strpair[1];
            long tmpre = db.getOnePartitionRowCount(pp, parts, partname,
                spartname, tbType);
            if (tmpre == -1) {
              throw new Exception(tbname + " has no sub partition!");
            } else if (tmpre == -2) {
              throw new Exception(partname + " is not a pri partition name!");
            } else if (tmpre == -3) {
              throw new Exception(spartname + " is not a sub partition name!");
            } else if (tmpre == -4) {
              throw new Exception("get size failed for partition: " + part);
            }
            result.add(partname + " " + spartname + "	" + tmpre);
          }
        }
      }

    }

    try {
      FileSystem fs = showrcount.getResFile().getFileSystem(conf);
      DataOutput outStream = (DataOutput) fs.create(showrcount.getResFile());
      for (String str : result) {
        outStream.writeBytes(str);
        outStream.write(terminator);
      }
      ((FSDataOutputStream) outStream).close();
    } catch (FileNotFoundException e) {
      LOG.warn("showrowcount: " + StringUtils.stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.warn("showrowcount: " + StringUtils.stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString());
    }
    return 0;
  }

  private int showDBSize(Hive db, showDatabaseSizeDesc showdbsz)
      throws Exception {
    String testatfir = HiveConf.getVar(conf, HiveConf.ConfVars.OPENSHOWDBSIZE);
    if (testatfir != null && testatfir.equalsIgnoreCase("false")) {
      LOG.info("SHOW DBSIZE IS CLOSED!");
      return 0;
    }

    String usrname = SessionState.get().getUserName();
    String db_name = showdbsz.getDBName().toLowerCase();
    if (!db.hasAuth(usrname, Hive.Privilege.SELECT_PRIV, db_name, null)) {
      console.printError("user : " + usrname
          + " do not have privilege on db : " + db_name);
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "user : " + usrname + " do not have privilege on db : " + db_name);
      return 1;
    }

    List<String> re = db.getDatabases();
    List<String> result = new ArrayList<String>();
    String result_plus = null;
    if (!re.contains(db_name)) {
      throw new Exception("the dbname is not exist!");
    }

    List<String> tblnames = db.getTablesByDB(db_name);
    long totalsum = 0;
    for (String nnn : tblnames) {
      Table tbl = db.getTable(db_name, nnn);

      if (tbl.isView())
        continue;

      if (tbl.isPartitioned()) {
        ArrayList<ArrayList<String>> parts = db.getPartitionNames(db_name,
            tbl.getTTable(), Short.MAX_VALUE);
        if ((parts.size() == 2 && parts.get(1) != null)
            && (parts.get(1).get(0).startsWith("hash("))) {
          long tblsize = 0;
          for (String mypa : parts.get(0)) {
            Path path2 = new Path(tbl.getPath(), mypa);
            long rerere = db.getPathFileSizeForHash(path2);
            if (rerere > 0)
              tblsize += rerere;
          }
          totalsum += tblsize;
          result.add(nnn + "	" + tblsize);
        } else if ((parts.size() == 1)
            && (parts.get(0).get(0).startsWith("hash("))) {
          long rerere = db.getPathFileSizeForHash(tbl.getPath());
          if (rerere > 0) {
            totalsum += rerere;
            result.add(nnn + "	" + rerere);
          } else {
            result.add(nnn + "	" + 0);
          }
        } else {
          long tmpre = db.getTableTotalSize(tbl.getPath(), parts);
          if (tmpre > 0) {
            totalsum += tmpre;
            result.add(nnn + "	" + tmpre);
          } else {
            result.add(nnn + "	" + 0);
          }

        }
      } else {
        long tmpre = db.getPathFileSize(tbl.getPath());
        if (tmpre < 0)
          tmpre = 0;
        totalsum += tmpre;
        result.add(nnn + "	" + tmpre);
      }
    }

    result.add(db_name + " totalsize is" + "	" + totalsum);
    result_plus = db_name + " totalsize is" + "	" + totalsum;

    try {
      FileSystem fs = showdbsz.getResFile().getFileSystem(conf);
      DataOutput outStream = (DataOutput) fs.create(showdbsz.getResFile());
      if (showdbsz.getExtended()) {
        for (String rrr : result) {
          outStream.writeBytes(rrr);
          outStream.write(terminator);
        }
      } else {
        outStream.writeBytes(result_plus);
        outStream.write(terminator);
      }
      ((FSDataOutputStream) outStream).close();
    } catch (FileNotFoundException e) {
      LOG.warn("showdbsize: " + StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "showdbsize: " + StringUtils.stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.warn("showdbsize: " + StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "showdbsize: " + StringUtils.stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString());
    }
    return 0;
  }

  private int showTableSize(Hive db, showTableSizeDesc showtblsz)
      throws Exception {
    String testatfir = HiveConf.getVar(conf,
        HiveConf.ConfVars.OPENSHOWTABLESIZE);
    if (testatfir != null && testatfir.equalsIgnoreCase("false")) {
      LOG.info("SHOW TableSize IS CLOSED!");
      return 0;
    }

    String dbname = null;
    if (showtblsz.getDBName() == null) {
      dbname = SessionState.get().getDbName();
    } else {
      dbname = showtblsz.getDBName();
    }

    String usrname = SessionState.get().getUserName();
    String table_name = showtblsz.getTableName();
    if (!db.hasAuth(usrname, Hive.Privilege.SELECT_PRIV, dbname, table_name)) {
      console.printError("user : " + usrname
          + " do not have privilege on table : " + dbname + "." + table_name);
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "user : " + usrname + " do not have privilege on table : " + dbname
                + "." + table_name);
      return 1;
    }

    ArrayList<ArrayList<String>> parts = null;
    Table tbl = null;
    Vector<String> parttitions = null;
    Vector<String> subparttitions = null;
    Vector<String> pairs = null;
    Vector<String> result = null;
    boolean hashashpart = false;

    try {
      tbl = db.getTable(dbname, table_name);
    } catch (Exception e) {
      throw new Exception("table " + dbname + "." + table_name
          + " can not be found!");
    }
    if (tbl == null) {
      throw new Exception("table can not be found!");
    }

    if (tbl.isView())
      throw new Exception("Should not use Show TableSize with View!");

    if (!tbl.isPartitioned() && showtblsz.getIsPartition()) {
      LOG.info("Table " + table_name + " is not a partitioned table");
      throw new Exception("Table " + table_name + " is not a partitioned table");
    }

    if (!tbl.isPartitioned()) {
      Path pp = tbl.getPath();
      LOG.info("Path: " + pp.toString());
      long re = db.getPathFileSize(pp);
      if (re < 0) {
        throw new Exception("failed to get the table size of " + table_name);
      }

      try {
        FileSystem fs = showtblsz.getResFile().getFileSystem(conf);
        DataOutput outStream = (DataOutput) fs.create(showtblsz.getResFile());
        outStream.writeBytes("table size: ");
        outStream.write(terminator);
        outStream.writeBytes(table_name + "	" + re);
        outStream.write(terminator);
        ((FSDataOutputStream) outStream).close();
      } catch (FileNotFoundException e) {
        LOG.warn("showtablesize: " + StringUtils.stringifyException(e));
        return 1;
      } catch (IOException e) {
        LOG.warn("showtablesize: " + StringUtils.stringifyException(e));
        return 1;
      } catch (Exception e) {
        throw new HiveException(e.toString());
      }

      return 0;
    } else {
      Path pp = tbl.getPath();
      parts = db.getPartitionNames(tbl.getDbName(), tbl.getTTable(),
          Short.MAX_VALUE);
      parttitions = showtblsz.getPartitions();
      subparttitions = showtblsz.getSubpartitions();
      pairs = showtblsz.getPairpartitions();

      if (parts.size() == 2 && parts.get(1) != null) {
        if (parts.get(1).get(0).startsWith("hash(")) {
          if (subparttitions != null && subparttitions.size() > 0) {
            throw new Exception("subparttition is hash partition, can not show");
          }
          if (pairs != null && pairs.size() > 0) {
            throw new Exception("subparttition is hash partition, can not show");
          }

          long totalsize = 0;
          hashashpart = true;
          result = new Vector<String>();

          if (parttitions == null) {
            if (showtblsz.getIsExtended()) {
              for (String mypa : parts.get(0)) {
                Path path2 = new Path(pp, mypa);
                long re = db.getPathFileSizeForHash(path2);
                if (re > 0) {
                  totalsize += re;
                  result.add(mypa + "	" + re);
                } else {
                  result.add(mypa + "	" + 0);
                }
              }
              result.add("table size:" + "	" + totalsize);
            } else {
              for (String mypa : parts.get(0)) {
                Path path2 = new Path(pp, mypa);
                long re = db.getPathFileSizeForHash(path2);
                if (re > 0)
                  totalsize += re;
              }
              result.add("table size:" + "	" + totalsize);
            }
          } else {
            if (parttitions.size() > 0) {
              for (String partname : parttitions) {
                Path path2 = new Path(pp, partname);
                long re = db.getPathFileSizeForHash(path2);
                if (re > 0)
                  result.add(partname + "	" + re);
                else
                  result.add(partname + "	" + 0);
              }
            }
          }

        }
      } else {
        if (parts.get(0).get(0).startsWith("hash(")) {
          if (subparttitions != null && subparttitions.size() > 0) {
            throw new Exception("subparttition is not exist");
          }
          if (pairs != null && pairs.size() > 0) {
            throw new Exception("subparttition is not exist");
          }
          if (parttitions != null && parttitions.size() > 0) {
            throw new Exception("pripartition is hash partition");
          }

          hashashpart = true;
          long re = db.getPathFileSizeForHash(pp);
          result = new Vector<String>();
          if (re > 0)
            result.add("table size:" + "	" + re);
          else
            result.add("table size:" + "	" + 0);
        }
      }

      if ((parttitions == null) && (subparttitions == null) && (pairs == null)
          && (!hashashpart)) {
        if (showtblsz.getIsExtended()) {
          result = db.getAllPartitions(pp, parts);
        } else {
          result = new Vector<String>();
          long re = db.getTableTotalSize(pp, parts);
          if (re > 0)
            result.add("total size:" + "	" + re);
          else
            result.add("total size:" + "	" + 0);
        }
      } else if ((!hashashpart)) {
        result = new Vector<String>();
        if (parttitions.size() > 0) {
          result.add("pri partitions:");
          for (String part : parttitions) {
            long tmpre = db.getPartitionSize(pp, parts, part);
            if (tmpre == -2) {
              throw new Exception(part + " is not a pri partition name!");
            } else if (tmpre == -4) {
              throw new Exception("get size failed for pri partition: " + part);
            }
            result.add(part + "	" + tmpre);
          }
        }
        if (subparttitions.size() > 0) {
          result.add("sub partitions:");
          for (String part : subparttitions) {
            long tmpre = db.getSubPartitionSize(pp, parts, part);
            if (tmpre == -1) {
              throw new Exception(table_name + " has no sub partition!");
            } else if (tmpre == -3) {
              throw new Exception(part + " is not a sub partition name!");
            } else if (tmpre == -4) {
              throw new Exception("get size failed for sub partition: " + part);
            }
            result.add(part + "	" + tmpre);
          }
        }
        if (pairs.size() > 0) {
          result.add("partitions:");
          for (String part : pairs) {
            String[] strpair = part.split("	");
            if (strpair.length != 2) {
              throw new Exception(part + " is not right!");
            }
            String partname = strpair[0];
            String spartname = strpair[1];
            long tmpre = db.getOnePartitionSize(pp, parts, partname, spartname);
            if (tmpre == -1) {
              throw new Exception(table_name + " has no sub partition!");
            } else if (tmpre == -2) {
              throw new Exception(partname + " is not a pri partition name!");
            } else if (tmpre == -3) {
              throw new Exception(spartname + " is not a sub partition name!");
            } else if (tmpre == -4) {
              throw new Exception("get size failed for partition: " + part);
            }
            result.add(partname + " " + spartname + "	" + tmpre);
          }
        }
      }
    }

    try {
      FileSystem fs = showtblsz.getResFile().getFileSystem(conf);
      DataOutput outStream = (DataOutput) fs.create(showtblsz.getResFile());
      for (String str : result) {
        outStream.writeBytes(str);
        outStream.write(terminator);
      }
      ((FSDataOutputStream) outStream).close();
    } catch (FileNotFoundException e) {
      LOG.warn("showtablesize: " + StringUtils.stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.warn("showtablesize: " + StringUtils.stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString());
    }

    return 0;
  }

  private int showInfo(Hive db, showInfoDesc infoDesc) throws Exception {
    String table_name = infoDesc.getTableName();
    String re = db.showinfo(table_name);
    String first = "table_name	table_size	cols_distinctvalue";
    if (table_name == null)
      throw new Exception("table name is not right!");
    else if (re == null)
      throw new Exception("table info can not be found!");

    try {
      FileSystem fs = infoDesc.getResFile().getFileSystem(conf);
      DataOutput outStream = (DataOutput) fs.create(infoDesc.getResFile());
      outStream.writeBytes(first);
      outStream.write(terminator);
      outStream.writeBytes(re);
      outStream.write(terminator);
      ((FSDataOutputStream) outStream).close();
    } catch (FileNotFoundException e) {
      LOG.warn("showinfo: " + StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "showinfo: " + StringUtils.stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.warn("showinfo: " + StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "showinfo: " + StringUtils.stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString());
    }
    return 0;
  }

  private int showProcesslist(Hive db, showProcesslistDesc showPros)
      throws Exception {
    String uname = SessionState.get().getUserName();
    boolean isLocal = showPros.getIsLocal();
    String filtname = showPros.getUname();

    if (uname.equalsIgnoreCase("root") && filtname != null) {
      uname = filtname;
    }
    List<String> processlsit = db.get_tdw_query(uname, isLocal);

    try {
      LOG.debug("ResFile:   " + showPros.getResFile().toString());
      FileSystem fs = showPros.getResFile().getFileSystem(conf);
      DataOutput outStream = (DataOutput) fs.create(showPros.getResFile());
      Iterator<String> iterpros = processlsit.iterator();

      while (iterpros.hasNext()) {
        String outout = iterpros.next();
        outStream.writeBytes(outout);
        outStream.write(terminator);
      }

      ((FSDataOutputStream) outStream).close();
    } catch (FileNotFoundException e) {
      LOG.warn("show processlist: " + StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "show processlist: " + StringUtils.stringifyException(e));

      return 1;
    } catch (IOException e) {
      LOG.warn("show processlist: " + StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "show processlist: " + StringUtils.stringifyException(e));

      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString());
    }

    return 0;
  }

  private int showQuery(Hive db, showqueryDesc kq) throws Exception {
    String qid = kq.getQueryId();
    String uname = SessionState.get().getUserName();
    String result = db.get_query(qid, uname);
    if (result == null) {
      throw new Exception(qid + " can not be found in the database!");
    }
    LOG.info("Query:  " + result);
    try {
      FileSystem fs = kq.getResFile().getFileSystem(conf);
      DataOutput outStream = (DataOutput) fs.create(kq.getResFile());
      outStream.writeBytes(qid);
      outStream.write(separator);
      outStream.writeBytes(result);
      outStream.write(terminator);
      ((FSDataOutputStream) outStream).close();
    } catch (FileNotFoundException e) {
      LOG.warn("show query: " + StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "show query: " + StringUtils.stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.warn("show query: " + StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "show query: " + StringUtils.stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString());
    }
    return 0;
  }

  private int killQuery(Hive db, killqueryDesc kq) throws Exception {
    String qid = kq.getQueryId();
    RunningJob thejob = SessionState.getRunningJob(qid);
    if (thejob == null) {
      LOG.info("this is in another hiveserver");
      String jid = db.get_jobid(qid, SessionState.get().getUserName());
      if (jid != null) {
        db.kill_jobid(jid);
        return 0;
      } else {
        throw new Exception(qid + " may have finished or stopped!");
      }
    }
    String killed = SessionState.getUserName(qid);
    String killer = SessionState.get().getUserName();
    LOG.debug("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
    LOG.debug("Killer:  " + killer);
    LOG.debug("Killed:  " + killed);
    LOG.debug("Query:  " + db.get_query(qid, killer));
    LOG.debug("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
    if ((!killer.equalsIgnoreCase(killed))
        && (!killer.equalsIgnoreCase("root"))) {
      throw new Exception(killer + " has no right to kill this query!");
    }
    try {
      thejob.killJob();
    } catch (IOException ioe) {
      LOG.warn("kill query: " + StringUtils.stringifyException(ioe));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "kill query: " + StringUtils.stringifyException(ioe));
      return 1;
    }
    return 0;
  }

  private int clearQuery(Hive db, clearqueryDesc kq) throws Exception {
    int daysforhistory = 100;
    String killer = SessionState.get().getUserName();
    int days = kq.getDays();
    if (days > daysforhistory) {
      days = daysforhistory;
    }
    if (days < 0) {
      days = 0;
    }
    if (killer.equalsIgnoreCase("root")) {
      if (db.clear_tdw_query(days)) {
        return 0;
      } else {
        throw new Exception("clear action failed!");
      }

    } else {
      throw new Exception(killer + " has no right to clear query history!");
    }
  }

  private int showTables(Hive db, showTablesDesc showTbls) throws HiveException {
    List<String> tbls = null;
    if (showTbls.getPattern() != null) {
      LOG.info("pattern: " + showTbls.getPattern());
      if (SessionState.get() != null) {
        SessionState.get().ssLog("pattern: " + showTbls.getPattern());
      }

      tbls = db.getTablesByPattern(showTbls.getPattern());

      LOG.info("results : " + tbls.size());
      if (SessionState.get() != null) {
        SessionState.get().ssLog("results : " + tbls.size());
      }
    } else {
      tbls = db.getAllTables();
    }

    try {
      FileSystem fs = showTbls.getResFile().getFileSystem(conf);
      DataOutput outStream = (DataOutput) fs.create(showTbls.getResFile());
      SortedSet<String> sortedTbls = new TreeSet<String>(tbls);
      Iterator<String> iterTbls = sortedTbls.iterator();

      while (iterTbls.hasNext()) {
        outStream.writeBytes(iterTbls.next());
        outStream.write(terminator);
      }
      ((FSDataOutputStream) outStream).close();
    } catch (FileNotFoundException e) {
      LOG.warn("show table: " + StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "show table: " + StringUtils.stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.warn("show table: " + StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "show table: " + StringUtils.stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString());
    }
    return 0;
  }

  private int showFunctions(showFunctionsDesc showFuncs) throws HiveException {
    Set<String> funcs = null;
    if (showFuncs.getPattern() != null) {
      LOG.info("pattern: " + showFuncs.getPattern());
      if (SessionState.get() != null) {
        SessionState.get().ssLog("pattern: " + showFuncs.getPattern());
      }

      funcs = FunctionRegistry.getFunctionNames(showFuncs.getPattern());
      LOG.info("results : " + funcs.size());
      if (SessionState.get() != null) {
        SessionState.get().ssLog("results : " + funcs.size());
      }
    } else {
      funcs = FunctionRegistry.getFunctionNames();
    }

    try {
      FileSystem fs = showFuncs.getResFile().getFileSystem(conf);
      DataOutput outStream = (DataOutput) fs.create(showFuncs.getResFile());
      SortedSet<String> sortedFuncs = new TreeSet<String>(funcs);
      Iterator<String> iterFuncs = sortedFuncs.iterator();

      while (iterFuncs.hasNext()) {
        outStream.writeBytes(iterFuncs.next());
        outStream.write(terminator);
      }
      ((FSDataOutputStream) outStream).close();
    } catch (FileNotFoundException e) {
      LOG.warn("show function: " + StringUtils.stringifyException(e));
      if (SessionState.get() != null) {
        SessionState.get().ssLog(
            "show function: " + StringUtils.stringifyException(e));
      }
      return 1;
    } catch (IOException e) {
      LOG.warn("show function: " + StringUtils.stringifyException(e));
      if (SessionState.get() != null) {
        SessionState.get().ssLog(
            "show function: " + StringUtils.stringifyException(e));
      }
      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString());
    }
    return 0;
  }

  private int describeFunction(descFunctionDesc descFunc) throws HiveException {
    String name = descFunc.getName();

    try {
      FileSystem fs = descFunc.getResFile().getFileSystem(conf);
      DataOutput outStream = (DataOutput) fs.create(descFunc.getResFile());

      description desc = null;
      FunctionInfo fi = FunctionRegistry.getFunctionInfo(name);

      Class<?> funcClass = null;
      GenericUDF udf = fi.getGenericUDF();

      if (udf != null) {
        if (udf instanceof GenericUDFBridge) {
          funcClass = ((GenericUDFBridge) udf).getUdfClass();
        } else {
          funcClass = udf.getClass();
        }
      } else if (fi.getGenericUDAFResolver() != null) {
        funcClass = fi.getGenericUDAFResolver().getClass();
      }

      if (funcClass != null) {
        desc = funcClass.getAnnotation(description.class);
      }

      if (desc != null) {
        outStream.writeBytes(desc.value().replace("_FUNC_", name));
        if (descFunc.isExtended() && desc.extended().length() > 0) {
          outStream.writeBytes("\n" + desc.extended().replace("_FUNC_", name));
        }
      } else {
        outStream.writeBytes("Function " + name + " does not exist or cannot"
            + " find documentation for it.");
      }

      outStream.write(terminator);

      ((FSDataOutputStream) outStream).close();
    } catch (FileNotFoundException e) {
      LOG.warn("describe function: " + StringUtils.stringifyException(e));
      if (SessionState.get() != null) {
        SessionState.get().ssLog(
            "describe function: " + StringUtils.stringifyException(e));
      }
      return 1;
    } catch (IOException e) {
      LOG.warn("describe function: " + StringUtils.stringifyException(e));
      if (SessionState.get() != null) {
        SessionState.get().ssLog(
            "describe function: " + StringUtils.stringifyException(e));
      }
      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString());
    }
    return 0;
  }

  private int describeTable(Hive db, descTableDesc descTbl)
      throws HiveException {
    String colPath = descTbl.getTableName();
    String dbname = descTbl.getDBName();
    String tableName = colPath.substring(0,
        colPath.indexOf('.') == -1 ? colPath.length() : colPath.indexOf('.'));

    if (dbname == null) {
      dbname = SessionState.get().getDbName();
    }

    String usrname = SessionState.get().getUserName();
    if (!db.hasAuth(usrname, Hive.Privilege.SELECT_PRIV, dbname, tableName)) {
      console.printError("user : " + usrname
          + " do not have privilege on table : " + dbname + "." + tableName);
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "user : " + usrname + " do not have privilege on table : " + dbname
                + "." + tableName);
      return 1;
    }

    Table tbl = db.getTable(dbname, tableName, false);
    Partition part = null;

    try {
      if (tbl == null) {
        FileSystem fs = descTbl.getResFile().getFileSystem(conf);
        DataOutput outStream = (DataOutput) fs.create(descTbl.getResFile());
        String errMsg = "Table " + tableName + " does not exist";
        outStream.write(errMsg.getBytes("UTF-8"));
        ((FSDataOutputStream) outStream).close();
        return 0;
      }
      if (descTbl.getPartName() != null) {
        part = null;
        if (part == null) {
          FileSystem fs = descTbl.getResFile().getFileSystem(conf);
          DataOutput outStream = (DataOutput) fs.create(descTbl.getResFile());
          String errMsg = "Partition " + descTbl.getPartName() + " for table "
              + tableName + " does not exist";
          outStream.write(errMsg.getBytes("UTF-8"));
          ((FSDataOutputStream) outStream).close();
          return 0;
        }
      }
    } catch (FileNotFoundException e) {
      LOG.info("describe table: " + StringUtils.stringifyException(e));
      if (SessionState.get() != null) {
        SessionState.get().ssLog(
            "describe table: " + StringUtils.stringifyException(e));
      }
      return 1;
    } catch (IOException e) {
      LOG.info("describe table: " + StringUtils.stringifyException(e));
      if (SessionState.get() != null) {
        SessionState.get().ssLog(
            "describe table: " + StringUtils.stringifyException(e));
      }
      return 1;
    }

    try {
      LOG.info("DDLTask: got data for " + tbl.getName());
      if (SessionState.get() != null) {
        SessionState.get().ssLog("DDLTask: got data for " + tbl.getName());
      }

      List<FieldSchema> cols = null;
      if (colPath.equals(tableName)) {
        cols = tbl.getCols();
      } else {
        cols = Hive.getFieldsFromDeserializer(colPath, tbl.getDeserializer());
      }

      FileSystem fs = descTbl.getResFile().getFileSystem(conf);
      DataOutput outStream = (DataOutput) fs.create(descTbl.getResFile());
      String pattern = descTbl.getPattern();
      LOG.info("Pattern String: " + pattern);

      if (SessionState.get() != null) {
        SessionState.get().ssLog("Pattern String: " + pattern);
      }

      boolean ignorePattern = false;
      Pattern p = null;
      if (pattern == null) {
        ignorePattern = true;
      } else {
        try {
          p = Pattern.compile(pattern, Pattern.CASE_INSENSITIVE);
        } catch (PatternSyntaxException e) {
          ignorePattern = true;
          LOG.info("Ignore illegal pattern syntax: " + pattern);

          if (SessionState.get() != null) {
            SessionState.get().ssLog(
                "Ignore illegal pattern syntax: " + pattern);
          }
        }
      }

      Iterator<FieldSchema> iterCols = cols.iterator();
      while (iterCols.hasNext()) {
        FieldSchema col = iterCols.next();
        String colName = col.getName();
        LOG.info("colName String: " + colName);

        if (SessionState.get() != null) {
          SessionState.get().ssLog("colName String: " + colName);
        }

        if (ignorePattern || p.matcher(colName).find()) {
          outStream.writeBytes(colName);
          outStream.write(separator);
          outStream.writeBytes(col.getType());
          outStream.write(separator);
          outStream.write((col.getComment() == null ? "" : col.getComment())
              .getBytes("UTF-8"));

          outStream.write(terminator);
        }
      }

      if (descTbl.isExt()) {
        outStream.write(terminator);
        outStream.writeBytes("Detailed Table Information");
        outStream.write(separator);

        outStream.write(tbl.getTTable().toString().getBytes("UTF-8"));
        outStream.write(separator);
        outStream.write(terminator);
      }

      LOG.info("DDLTask: written data for " + tbl.getName());
      if (SessionState.get() != null) {
        SessionState.get().ssLog("DDLTask: written data for " + tbl.getName());
      }

      ((FSDataOutputStream) outStream).close();
    } catch (FileNotFoundException e) {
      LOG.info("describe table: " + StringUtils.stringifyException(e));

      if (SessionState.get() != null) {
        SessionState.get().ssLog(
            "describe table: " + StringUtils.stringifyException(e));
      }

      return 1;
    } catch (IOException e) {
      LOG.info("describe table: " + StringUtils.stringifyException(e));

      if (SessionState.get() != null) {
        SessionState.get().ssLog(
            "describe table: " + StringUtils.stringifyException(e));
      }

      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString());
    }

    return 0;
  }

  private int alterTable(Hive db, alterTableDesc alterTbl) throws HiveException {

    if (!db.hasAuth(SessionState.get().getUserName(),
        Hive.Privilege.ALTER_PRIV, SessionState.get().getDbName(),
        alterTbl.getOldName())) {
      if(db.isLhotseImportExport() && 
          db.isHdfsExternalTable(SessionState.get().getDbName(), alterTbl.getOldName())){
        if (SessionState.get() != null)
          SessionState.get().ssLog("user : " + SessionState.get().getUserName()
              + " do not have alter privilege on table : "
              + SessionState.get().getDbName() + "."
              + alterTbl.getOldName()
              + ",but it's a lhotse import or export task");
      }else{
        console.printError("user : " + SessionState.get().getUserName()
            + " do not have privilege on table : "
            + SessionState.get().getDbName() + "." + alterTbl.getOldName());

        if (SessionState.get() != null) {
          SessionState.get().ssLog(
              "user : " + SessionState.get().getUserName()
              + " do not have privilege on table : "
              + SessionState.get().getDbName() + "." + alterTbl.getOldName());
        }
        return 1;
      }
    }

    if (alterTbl.getOp() == alterTableDesc.alterTableTypes.RENAME) {
      try {
        return db.renameTable(SessionState.get().getDbName(),
            alterTbl.getOldName(), SessionState.get().getUserName(),
            alterTbl.getNewName());
      } catch (InvalidOperationException e) {
        console.printError("Invalid alter operation: " + e.getMessage());
        LOG.info("alter table: " + StringUtils.stringifyException(e));
        if (SessionState.get() != null) {
          SessionState.get()
              .ssLog("Invalid alter operation: " + e.getMessage());
          SessionState.get().ssLog(
              "alter table: " + StringUtils.stringifyException(e));
        }
        return 1;
      }
    } else if (alterTbl.getOp() == alterTableDesc.alterTableTypes.ADDCOLS) {
      try {
        return db.addCols(SessionState.get().getDbName(),
            alterTbl.getOldName(), SessionState.get().getUserName(),
            alterTbl.getNewCols());
      } catch (InvalidOperationException e) {
        console.printError("Invalid alter operation: " + e.getMessage());
        LOG.info("alter table: " + StringUtils.stringifyException(e));
        if (SessionState.get() != null) {
          SessionState.get()
              .ssLog("Invalid alter operation: " + e.getMessage());
          SessionState.get().ssLog(
              "alter table: " + StringUtils.stringifyException(e));
        }
        return 1;
      }
    } else if (alterTbl.getOp() == alterTableDesc.alterTableTypes.RENAMECOLUMN) {
      RenameColDesc renameColDesc = new RenameColDesc();
      renameColDesc.setAfterCol(alterTbl.getAfterCol());
      renameColDesc.setComment(alterTbl.getNewColComment());
      renameColDesc.setDbName(SessionState.get().getDbName());
      renameColDesc.setIsFirst(alterTbl.getFirst());
      renameColDesc.setNewName(alterTbl.getNewColName());
      renameColDesc.setOldName(alterTbl.getOldColName());
      renameColDesc.setType(alterTbl.getNewColType());
      renameColDesc.setUser(SessionState.get().getUserName());

      try {
        return db.renameCol(SessionState.get().getDbName(),
            alterTbl.getOldName(), renameColDesc);
      } catch (InvalidOperationException e) {
        console.printError("Invalid alter operation: " + e.getMessage());
        LOG.info("alter table: " + StringUtils.stringifyException(e));
        if (SessionState.get() != null) {
          SessionState.get()
              .ssLog("Invalid alter operation: " + e.getMessage());
          SessionState.get().ssLog(
              "alter table: " + StringUtils.stringifyException(e));
        }
        return 1;
      }
    } else if (alterTbl.getOp() == alterTableDesc.alterTableTypes.REPLACECOLS) {
      try {
        return db.replaceCols(SessionState.get().getDbName(),
            alterTbl.getOldName(), SessionState.get().getUserName(),
            alterTbl.getNewCols());
      } catch (InvalidOperationException e) {
        console.printError("Invalid alter operation: " + e.getMessage());
        LOG.info("alter table: " + StringUtils.stringifyException(e));
        if (SessionState.get() != null) {
          SessionState.get()
              .ssLog("Invalid alter operation: " + e.getMessage());
          SessionState.get().ssLog(
              "alter table: " + StringUtils.stringifyException(e));
        }
        return 1;
      }
    } else if (alterTbl.getOp() == alterTableDesc.alterTableTypes.ADDPROPS) {
      try {
        return db.addTableProps(SessionState.get().getDbName(),
            alterTbl.getOldName(), SessionState.get().getUserName(),
            alterTbl.getProps());
      } catch (InvalidOperationException e) {
        console.printError("Invalid alter operation: " + e.getMessage());
        LOG.info("alter table: " + StringUtils.stringifyException(e));
        if (SessionState.get() != null) {
          SessionState.get()
              .ssLog("Invalid alter operation: " + e.getMessage());
          SessionState.get().ssLog(
              "alter table: " + StringUtils.stringifyException(e));
        }
        return 1;
      }
    } else if (alterTbl.getOp() == alterTableDesc.alterTableTypes.ADDSERDEPROPS) {
      try {
        return db.addSerdeProps(SessionState.get().getDbName(),
            alterTbl.getOldName(), SessionState.get().getUserName(),
            alterTbl.getProps());
      } catch (InvalidOperationException e) {
        console.printError("Invalid alter operation: " + e.getMessage());
        LOG.info("alter table: " + StringUtils.stringifyException(e));
        if (SessionState.get() != null) {
          SessionState.get()
              .ssLog("Invalid alter operation: " + e.getMessage());
          SessionState.get().ssLog(
              "alter table: " + StringUtils.stringifyException(e));
        }
        return 1;
      }
    } else if (alterTbl.getOp() == alterTableDesc.alterTableTypes.ADDSERDE) {
      AddSerdeDesc addSerdeDesc = new AddSerdeDesc();
      addSerdeDesc.setDbName(SessionState.get().getDbName());
      addSerdeDesc.setProps(alterTbl.getProps());
      addSerdeDesc.setSerdeName(alterTbl.getSerdeName());
      addSerdeDesc.setTableName(alterTbl.getOldName());
      addSerdeDesc.setUser(SessionState.get().getUserName());

      try {
        return db.addSerde(SessionState.get().getDbName(),
            alterTbl.getOldName(), addSerdeDesc);
      } catch (InvalidOperationException e) {
        console.printError("Invalid alter operation: " + e.getMessage());
        LOG.info("alter table: " + StringUtils.stringifyException(e));
        if (SessionState.get() != null) {
          SessionState.get()
              .ssLog("Invalid alter operation: " + e.getMessage());
          SessionState.get().ssLog(
              "alter table: " + StringUtils.stringifyException(e));
        }
        return 1;
      }
    } else {
      console.printError("Unsupported Alter commnad");
      if (SessionState.get() != null)
        SessionState.get().ssLog("Unsupported Alter commnad");
      return 1;
    }
  }

  private boolean checktype(FieldSchema col, String type) {
    if (HiveConf.getBoolVar(conf, ConfVars.ALTERSCHEMAACTIVATENOTYPELIMIT)) {
      return true;
    }
    if (type == null)
      return true;
    if (type.equalsIgnoreCase(Constants.TINYINT_TYPE_NAME)
        || type.equalsIgnoreCase(Constants.SMALLINT_TYPE_NAME)
        || type.equalsIgnoreCase(Constants.INT_TYPE_NAME)
        || type.equalsIgnoreCase(Constants.BOOLEAN_TYPE_NAME)
        || type.equalsIgnoreCase(Constants.FLOAT_TYPE_NAME)) {
      console.printInfo("alter a column type to " + type
          + " type is not supported");
      if (SessionState.get() != null) {
        SessionState.get().ssLog(
            "alter a column type to " + type + " type is not supported");
      }
      return false;
    }

    if (col.getType().equalsIgnoreCase(Constants.BOOLEAN_TYPE_NAME)) {
      console.printInfo("boolean type can not convert to any other type!!! ");
      if (SessionState.get() != null) {
        SessionState.get().ssLog(
            "boolean type can not convert to any other type!!! ");
      }
      return false;
    }

    if (col.getType().equalsIgnoreCase(Constants.STRING_TYPE_NAME)) {
      console.printInfo("string type can not convert to any other type!!! ");
      if (SessionState.get() != null) {
        SessionState.get().ssLog(
            "string type can not convert to any other type!!! ");
      }

      return false;
    }
    return true;
  }

  private int dropTable(Hive db, dropTableDesc dropTbl) throws HiveException {
    if (!dropTbl.getExpectView()) {
      if (dropTbl.getIsDropPartition() == null
          || dropTbl.getIsDropPartition() == false) {
        if (!db.hasAuth(SessionState.get().getUserName(),
            Hive.Privilege.DROP_PRIV, SessionState.get().getDbName(),
            dropTbl.getTableName())) {
          if(db.isLhotseImportExport() && 
              db.isHdfsExternalTable(SessionState.get().getDbName(), dropTbl.getTableName())){
            if (SessionState.get() != null)
              SessionState.get().ssLog("user : " + SessionState.get().getUserName()
                  + " do not have drop privilege on table : "
                  + dropTbl.getDbName() + "."
                  + dropTbl.getTableName()
                  + ",but it's a lhotse import or export task");
          }else{
            console.printError("user : " + SessionState.get().getUserName()
                + " do not have drop privilege on table : "
                + SessionState.get().getDbName() + "." + dropTbl.getTableName());

            if (SessionState.get() != null) {
              SessionState.get().ssLog(
                  "user : " + SessionState.get().getUserName()
                  + " do not have privilege on table : "
                  + SessionState.get().getDbName() + "."
                  + dropTbl.getTableName());
            }

            return 1;
          }
        }
      } else {
        if (!db.hasAuth(SessionState.get().getUserName(),
            Hive.Privilege.ALTER_PRIV, SessionState.get().getDbName(),
            dropTbl.getTableName())) {

          console.printError("user : " + SessionState.get().getUserName()
              + " do not have alter privilege on TABLE : "
              + SessionState.get().getDbName() + "::" + dropTbl.getTableName());

          if (SessionState.get() != null) {
            SessionState.get().ssLog(
                "user : " + SessionState.get().getUserName()
                    + " do not have privilege on DB : "
                    + SessionState.get().getDbName() + "::"
                    + dropTbl.getTableName());
          }

          return 1;
        }
      }
    } else {
      if (!db
          .hasAuth(SessionState.get().getUserName(),
              Hive.Privilege.CREATE_VIEW_PRIV, SessionState.get().getDbName(),
              null)) {

        console.printError("user : " + SessionState.get().getUserName()
            + " do not have create view privilege on DB : "
            + SessionState.get().getDbName());

        if (SessionState.get() != null) {
          SessionState.get().ssLog(
              "user : " + SessionState.get().getUserName()
                  + " do not have create view privilege on DB : "
                  + SessionState.get().getDbName());
        }

        return 1;
      }
    }

    boolean is = false;

    try {
      is = db.isView(SessionState.get().getDbName(), dropTbl.getTableName());
    } catch (NoSuchObjectException x) {
      return 0;
    }

    if (is) {
      if (!dropTbl.getExpectView()) {
        throw new HiveException("Cannot drop a view with DROP TABLE");
      }
    } else {
      if (dropTbl.getExpectView()) {
        throw new HiveException("Cannot drop a base table with DROP VIEW");
      }
    }

    Boolean reserveData = dropTbl.getIsReserveData();
    if (dropTbl.getIsDropPartition() == null
        || dropTbl.getIsDropPartition() == false) {
      if (!reserveData) {
        db.dropTable(SessionState.get().getDbName(), dropTbl.getTableName());
      } else {
        db.dropTable(SessionState.get().getDbName(), dropTbl.getTableName(),
            false, true);
      }
    } else {
      db.dropPartition(dropTbl, true);
    }
    return 0;
  }

  private int truncateTable(Hive db, truncateTableDesc truncTbl)
      throws HiveException, MetaException {
    if (!db.hasAuth(SessionState.get().getUserName(),
        Hive.Privilege.DELETE_PRIV, truncTbl.getDbName(),
        truncTbl.getTableName())) {
      if(db.isLhotseImportExport()){
        if (SessionState.get() != null)
          SessionState.get().ssLog("user : " + SessionState.get().getUserName()
              + " do not have delete (or trancate) privilege on table : "
              + truncTbl.getDbName() + "."
              + truncTbl.getTableName()
              + ",but it's a lhotse import or export task");
      }else{
        console.printError("user : " + SessionState.get().getUserName()
            + " do not have privilege on table : " + truncTbl.getDbName() + "::"
            + truncTbl.getTableName());

        if (SessionState.get() != null) {
          SessionState.get().ssLog(
              "user : " + SessionState.get().getUserName()
              + " do not have privilege on table : " + truncTbl.getDbName()
              + "::" + truncTbl.getTableName());
        }
        return 1;
      }
    }

    db.truncateTable(truncTbl.getDbName(), truncTbl.getTableName());

    return 0;
  }

  private void validateSerDe(String serdeName) throws HiveException {
    try {
      Deserializer d = SerDeUtils.lookupDeserializer(serdeName);
      if (d != null) {
        System.out.println("Found class for " + serdeName);
      }
    } catch (SerDeException e) {
      throw new HiveException("Cannot validate serde: " + serdeName, e);
    }
  }

  private int createTable(Hive db, createTableDesc crtTbl) throws HiveException {
    if (!db.hasAuth(SessionState.get().getUserName(),
        Hive.Privilege.CREATE_PRIV, SessionState.get().getDbName(), null)) {
      if(db.isLhotseImportExport() && crtTbl.isExternal() && !(crtTbl.getTblType() == 3)){
        if (SessionState.get() != null)
          SessionState.get().ssLog("user : " + SessionState.get().getUserName()
              + " do not have create privilege on DB : "
              + SessionState.get().getDbName()
              + ",but it's a lhotse import or export task");
      }else{
        console.printError("user : " + SessionState.get().getUserName()
            + " do not have privilege on DB : " + SessionState.get().getDbName());

        if (SessionState.get() != null) {
          SessionState.get().ssLog(
              "user : " + SessionState.get().getUserName()
              + " do not have privilege on DB : "
              + SessionState.get().getDbName());
        }
        return 1;
      }
    }

    Table tbl = new Table(crtTbl.getTableName());
    StorageDescriptor tblStorDesc = tbl.getTTable().getSd();

    if (crtTbl.getBucketCols() != null)
      tblStorDesc.setBucketCols(crtTbl.getBucketCols());
    if (crtTbl.getSortCols() != null)
      tbl.setSortCols(crtTbl.getSortCols());
    if (crtTbl.getPartdesc() != null)
      tbl.setPartitions(crtTbl.getPartdesc());
    if (crtTbl.getNumBuckets() != -1)
      tblStorDesc.setNumBuckets(crtTbl.getNumBuckets());

    if (crtTbl.getSerName() != null) {
      tbl.setSerializationLib(crtTbl.getSerName());

      if (crtTbl.getMapProp() != null) {
        Iterator<Entry<String, String>> iter = crtTbl.getMapProp().entrySet()
            .iterator();
        while (iter.hasNext()) {
          Entry<String, String> m = (Entry<String, String>) iter.next();
          tbl.setSerdeParam(m.getKey(), m.getValue());
          LOG.debug(m.getKey() + ":" + m.getValue());
        }
      }
    } else {
      if (crtTbl.getFieldDelim() != null) {
        tbl.setSerdeParam(Constants.FIELD_DELIM, crtTbl.getFieldDelim());
        tbl.setSerdeParam(Constants.SERIALIZATION_FORMAT,
            crtTbl.getFieldDelim());
      }

      if (crtTbl.getFieldEscape() != null) {
        tbl.setSerdeParam(Constants.ESCAPE_CHAR, crtTbl.getFieldEscape());
      }

      if (crtTbl.getCollItemDelim() != null)
        tbl.setSerdeParam(Constants.COLLECTION_DELIM, crtTbl.getCollItemDelim());
      if (crtTbl.getMapKeyDelim() != null)
        tbl.setSerdeParam(Constants.MAPKEY_DELIM, crtTbl.getMapKeyDelim());
      if (crtTbl.getLineDelim() != null)
        tbl.setSerdeParam(Constants.LINE_DELIM, crtTbl.getLineDelim());

      if (crtTbl.getMapPropOther() != null) {
        Iterator<Entry<String, String>> iter = crtTbl.getMapPropOther()
            .entrySet().iterator();
        while (iter.hasNext()) {
          Entry<String, String> m = (Entry<String, String>) iter.next();
          tbl.setSerdeParam(m.getKey(), m.getValue());
          if (m.getKey().equalsIgnoreCase(Constants.SERIALIZATION_CHARSET)) {
            if (m.getValue().equalsIgnoreCase("gbk")) {
              tblStorDesc.setOutputFormat(GBKIgnoreKeyOutputFormat.class
                  .getName());
              tbl.setOutputFormatClass(GBKIgnoreKeyOutputFormat.class.getName());
            } else if (m.getValue().equalsIgnoreCase("utf8")
                || m.getValue().equalsIgnoreCase("utf-8")) {
              tblStorDesc.setOutputFormat(IgnoreKeyTextOutputFormat.class
                  .getName());
              tbl.setOutputFormatClass(IgnoreKeyTextOutputFormat.class
                  .getName());
            }
          }
        }
      }

    }

    if (crtTbl.getSerName() == null) {
      LOG.info("Default to LazySimpleSerDe for table " + crtTbl.getTableName());
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "Default to LazySimpleSerDe for table " + crtTbl.getTableName());
      tbl.setSerializationLib(org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class
          .getName());
    } else {
      validateSerDe(crtTbl.getSerName());
    }

    if (crtTbl.getComment() != null)
      tbl.setProperty("comment", crtTbl.getComment());
    if (crtTbl.getLocation() != null)
      tblStorDesc.setLocation(crtTbl.getLocation());

    tbl.setInputFormatClass(crtTbl.getInputFormat());
    tbl.setOutputFormatClass(crtTbl.getOutputFormat());

    if (crtTbl.isExternal()) {
      tbl.setProperty("EXTERNAL", "TRUE");
      tbl.setTableType(TableType.EXTERNAL_TABLE);
    }
    if (crtTbl.getSerName() != null
        && crtTbl.getSerName().equalsIgnoreCase(ProtobufSerDe.class.getName())) {
      tbl.setProperty("PB_TABLE", "TRUE");

      tbl.setProperty("PB_FILE", crtTbl.getPb_file_path());
      tbl.setProperty(Constants.PB_MSG_NAME, tbl.getName().toLowerCase());
      String jarPath = "./auxlib/" + crtTbl.getPb_outer_name() + ".jar";
      tbl.setProperty(Constants.PB_JAR_PATH, jarPath);
      tbl.setProperty(Constants.PB_OUTER_CLASS_NAME, crtTbl.getPb_outer_name());
    }

    if ((tbl.getBucketCols() != null) && (tbl.getSortCols() != null)) {
      List<String> bucketCols = tbl.getBucketCols();
      List<Order> sortCols = tbl.getSortCols();

      if ((sortCols.size() > 0) && (sortCols.size() >= bucketCols.size())) {
        boolean found = true;

        Iterator<String> iterBucketCols = bucketCols.iterator();
        while (iterBucketCols.hasNext()) {
          String bucketCol = iterBucketCols.next();
          boolean colFound = false;
          for (int i = 0; i < bucketCols.size(); i++) {
            if (bucketCol.equals(sortCols.get(i).getCol())) {
              colFound = true;
              break;
            }
          }
          if (colFound == false) {
            found = false;
            break;
          }
        }
        if (found)
          tbl.setProperty("SORTBUCKETCOLSPREFIX", "TRUE");
      }
    }

    int rc = setGenericTableAttributes(tbl);
    if (rc != 0) {
      return rc;
    }

    if (crtTbl.getCols() != null) {
      tbl.setFields(crtTbl.getCols());
    }

    if (crtTbl.getIfCompressed()) {
      tbl.setCompressed(true);
    } else {
      tbl.setCompressed(false);
    }

    if (crtTbl.getTblType() == 0) {
      tbl.setTableStorageType("text");
    } else if (crtTbl.getTblType() == 1) {
      tbl.setTableStorageType("format");
    } else if (crtTbl.getTblType() == 2) {
      tbl.setTableStorageType("column");
    } else if (crtTbl.getTblType() == 3) {
      tbl.setTableStorageType("pgdata");
    } else if (crtTbl.getTblType() == 4) {
      tbl.setTableStorageType("rcfile");
    } else {

    }

    if (crtTbl.getTblType() == 2) {
      ArrayList<ArrayList<String>> projectionInfos = crtTbl
          .getProjectionInfos();

      LOG.error("projectionInfos null:" + (projectionInfos == null));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "projectionInfos null:" + (projectionInfos == null));
      String projectionString = "";
      if (projectionInfos != null) {
        ArrayList<String> allProjectionFields = new ArrayList<String>(10);
        for (int i = 0; i < projectionInfos.size(); i++) {
          ArrayList<String> subProjection = projectionInfos.get(i);

          String subProjectionString = "";
          for (int j = 0; j < subProjection.size(); j++) {
            if (j == 0) {
              subProjectionString = getFieldIndxByName(subProjection.get(j),
                  crtTbl.getCols());
            } else {
              subProjectionString += ","
                  + getFieldIndxByName(subProjection.get(j), crtTbl.getCols());
            }

            allProjectionFields.add(subProjection.get(j));
          }

          if (i == 0) {
            projectionString = subProjectionString;
          } else {
            projectionString += ";" + subProjectionString;
          }
        }

        String remainFieldString = "";
        int counter = 0;
        List<FieldSchema> fields = crtTbl.getCols();
        if (fields != null) {
          for (int i = 0; i < fields.size(); i++) {
            boolean found = false;
            String fieldString = fields.get(i).getName();
            for (int j = 0; j < allProjectionFields.size(); j++) {
              if (fieldString.equalsIgnoreCase(allProjectionFields.get(j))) {
                found = true;
                break;
              }
            }

            if (!found) {
              if (counter == 0) {
                remainFieldString = ("" + i);
                counter++;
              } else {
                remainFieldString += "," + ("" + i);
              }
            }
          }
        }

        projectionString += ";" + remainFieldString;
      } else {
        List<FieldSchema> fields = crtTbl.getCols();
        if (fields != null) {
          for (int i = 0; i < fields.size(); i++) {
            if (i == 0) {
              projectionString = ("" + i);
            } else {
              projectionString += ";" + ("" + i);
            }
          }
        }
      }

      tbl.setProjection(projectionString);
    }

    String indexName = "";
    int indexType = 1;
    if (crtTbl.getIndexInfo() != null) {
      indexName = crtTbl.getIndexInfo().name;
      List<String> fieldList = crtTbl.getIndexInfo().fieldList;
      indexType = crtTbl.getIndexInfo().indexType;

      if (indexName == null || indexName.length() == 0) {
        indexName = fieldList.get(0) + "0";
      }

      int indexNum = 0;
      String indexNumString = tbl.getParameters().get("indexNum");
      if (indexNumString != null && indexNumString.length() != 0) {
        indexNum = Integer.valueOf(indexNumString);
      }
      for (int i = 0; i < indexNum; i++) {
        String indexInfoString = tbl.getParameters().get("index" + i);
        if (indexInfoString != null) {
          String[] indexInfo = indexInfoString.split(";");

          if (indexInfo != null && indexName.equalsIgnoreCase(indexInfo[0])) {
            console.printError("index already exist");
            if (SessionState.get() != null)
              SessionState.get().ssLog("index already exist");
            return 1;
          }
        }
      }

      String fieldListString = "";
      for (int i = 0; i < fieldList.size(); i++) {
        if (i == 0) {
          fieldListString = getFieldIndxByName(fieldList.get(i),
              crtTbl.getCols());
        } else {
          fieldListString += ","
              + getFieldIndxByName(fieldList.get(i), crtTbl.getCols());
        }
      }

      String indexInfoString = indexName + ";" + fieldListString + ";"
          + indexType;

      tbl.setIndexNum("" + (indexNum + 1));
      tbl.setIndexInfo(indexNum, indexInfoString);

      db.createIndex(tbl);
    }

    if (crtTbl.getTblType() == 3) {
      LOG.info("create table stored as pgdata");
      StoreAsPgdata stoAsPg = new StoreAsPgdata();

      List<FieldSchema> cols = crtTbl.getCols();
      String tableName = crtTbl.getTableName();
      Connection conn;

      if (tbl.getSerdeParam(Constants.ISCTAS).equalsIgnoreCase("true")) {
        String pgSQL = stoAsPg.tdwSqlToPgsql(tableName, cols);
        conn = stoAsPg.sendCreatTableToPg(tbl, pgSQL);

        String location;
        try {
          Warehouse wh = new Warehouse(conf);
          location = wh.getDefaultTablePath(SessionState.get().getDbName(),
              tableName).toString();
          LOG.info("CTAS stored as pgdata location:" + location);
        } catch (MetaException e) {
          throw new SemanticException(e.getMessage(), e);
        }

        LoadConfig config = new LoadConfig();
        String tableServer = tbl.getSerdeParam(Constants.TABLE_SERVER);
        String port = tbl.getSerdeParam(Constants.TABLE_PORT);
        String db_name = tbl.getSerdeParam(Constants.TBALE_DB);
        String user = tbl.getSerdeParam(Constants.DB_URSER);
        String pwd = tbl.getSerdeParam(Constants.DB_PWD);
        String type = tbl.getSerdeParam(Constants.DB_TYPE);
        String table = tbl.getSerdeParam(Constants.TABLE_NAME);
        config.setHost(tableServer);
        config.setPort(port);
        config.setDbName(db_name);
        config.setUser(user);
        config.setPwd(pwd);
        config.setDbType(type);
        config.setTable(tbl);
        config.setDbTable(table);
        config.setConf(conf);
        config.setFilePath(location);
        BaseDBExternalDataLoad load = new BaseDBExternalDataLoad(config, cols,
            StoreAsPgdata.get_stat());
        load.loadDataToDBTable(crtTbl.getFieldDelim());
      } else {
        conn = stoAsPg.sendCreatTableToPg(tbl);
      }

      try {
        db.createTable(tbl, crtTbl.getIfNotExists());
        Table retTbl = db.getTable(SessionState.get().getDbName(),
            tbl.getName());
      } catch (Exception e) {
        try {
          conn.rollback();
          throw new SemanticException(e.getMessage());
        } catch (Exception exc) {
          throw new SemanticException(exc.getMessage());
        }
      }
      stoAsPg.sendCommitToPg(conn);
    } else {
      db.createTable(tbl, crtTbl.getIfNotExists());
    }

    return 0;
  }

  private String getFieldIndxByName(String name, List<FieldSchema> fields) {
    for (int i = 0; i < fields.size(); i++) {
      if (name.equalsIgnoreCase(fields.get(i).getName())) {
        return "" + i;
      }
    }

    return "" + -1;
  }

  private int createTableLike(Hive db, createTableLikeDesc crtTbl)
      throws HiveException {
    if (!db.hasAuth(SessionState.get().getUserName(),
        Hive.Privilege.CREATE_PRIV, SessionState.get().getDbName(), null)) {
      if (SessionState.get() != null) {
        SessionState.get().ssLog(
            "user : " + SessionState.get().getUserName()
                + " do not have privilege on DB : "
                + SessionState.get().getDbName());
      }

      console.printError("user : " + SessionState.get().getUserName()
          + " do not have privilege on DB : " + SessionState.get().getDbName());
      return 1;
    }

    Table tbl = db.getTable(SessionState.get().getDbName(),
        crtTbl.getLikeTableName());
    StorageDescriptor tblStorDesc = tbl.getTTable().getSd();

    tbl.getTTable().setTableName(crtTbl.getTableName());
    if (tbl.isPartitioned() && crtTbl.isExternal()) {
      throw new SemanticException(
          "exteranl table do not allow partitioned and table : "
              + tbl.getName() + " is partitioned");
    }

    if (tbl.isPartitioned()) {
      tbl.getTTable().getPriPartition().setTableName(crtTbl.getTableName());
      if (tbl.getTTable().getSubPartition() != null) {
        tbl.getTTable().getSubPartition().setTableName(crtTbl.getTableName());
      }
    }

    if (crtTbl.isExternal()) {
      tbl.setProperty("EXTERNAL", "TRUE");
    } else {
    }

    if (crtTbl.getLocation() != null) {
      tblStorDesc.setLocation(crtTbl.getLocation());
    } else {
      tblStorDesc.setLocation(null);
      tblStorDesc.unsetLocation();
    }

    tbl.setOwner(SessionState.get().getUserName());
    db.createIndex(tbl);

    db.createTable(tbl, crtTbl.getIfNotExists());
    return 0;
  }

  private int createDatabase(Hive db, createDatabaseDesc createDb) {

    try {
      if (!db.hasAuth(SessionState.get().getUserName(),
          Hive.Privilege.CREATE_PRIV, null, null)) {
        console.printError("user : " + SessionState.get().getUserName()
            + " do not have privilege to create DB!");
        if (SessionState.get() != null)
          SessionState.get().ssLog(
              "user : " + SessionState.get().getUserName()
                  + " do not have privilege to create DB!");
        return 1;
      }

      Database database = new Database();
      database.setName(createDb.getDbname());
      database.setHdfsscheme(createDb.getHdfsschema());
      database.setMetastore(createDb.getDBStore());

      db.createDatabase(database);

    } catch (AlreadyExistsException e) {
      console.printError("database : " + createDb.getDbname()
          + " already exist!");
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "database : " + createDb.getDbname() + " already exist!");
      return 1;
    } catch (MetaException e) {
      console.printError("create database : " + createDb.getDbname()
          + " meta error!");
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "create database : " + createDb.getDbname() + " meta error!");
      return 1;
    } catch (TException e) {
      console.printError("create database TException : " + e.getMessage());
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "create database TException : " + e.getMessage());
      return 1;
    } catch (HiveException e) {
      console.printError("create database HiveException : " + e.getMessage());
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "create database HiveException : " + e.getMessage());
      return 1;
    }
    return 0;
  }

  private int dropDatabase(Hive db, dropDatabaseDesc dropDb) {
    if (dropDb.getDbname().equalsIgnoreCase(SessionState.get().getDbName())) {
      console.printError("can't drop current database : " + dropDb.getDbname());
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "can't drop current database : " + dropDb.getDbname());
      return 2;
    }

    try {
      if (!db.hasAuth(SessionState.get().getUserName(),
          Hive.Privilege.DROP_PRIV, null, null)) {
        console.printError("user : " + SessionState.get().getUserName()
            + " do not have privilege to drop DB!");
        if (SessionState.get() != null)
          SessionState.get().ssLog(
              "user : " + SessionState.get().getUserName()
                  + " do not have privilege to drop DB!");
        return 1;
      }

      db.dropDatabase(dropDb.getDbname());
    } catch (MetaException e) {
      console.printError("drop database : " + dropDb.getDbname()
          + " meta error!");
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "drop database : " + dropDb.getDbname() + " meta error!");
      return 1;
    } catch (TException e) {
      console.printError("drop database TException : " + e.getMessage());
      if (SessionState.get() != null)
        SessionState.get()
            .ssLog("drop database TException : " + e.getMessage());
      return 1;
    } catch (HiveException e) {
      console.printError("drop database HiveException : " + e.getMessage());
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "drop database HiveException : " + e.getMessage());
      return 1;
    }
    return 0;

  }

  private int useDatabase(Hive db, useDatabaseDesc useDb) {
    Object newDb = null;
    try {
      newDb = db.getDatabase(useDb.getChangeToDB());
    } catch (Exception e) {
      console.printError("error: can't ues database :" + useDb.getChangeToDB()
          + " ,it does not exisit!");
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "error: can't ues database :" + useDb.getChangeToDB()
                + " ,it does not exisit!");
      return 1;
    }

    SessionState.get().setDbName(useDb.getChangeToDB());
    return 0;
  }

  private int showDatabases(Hive db, showDBDesc showDb) throws HiveException {
    List<String> dbs = null;
    if (showDb.getOwner() != null) {
      dbs = db.getDatabases(showDb.getOwner());
    } else {
      dbs = db.getDatabases();
    }

    try {
      FileSystem fs = showDb.getResFile().getFileSystem(conf);
      DataOutput outStream = (DataOutput) fs.create(showDb.getResFile());
      SortedSet<String> sortedDBs = new TreeSet<String>(dbs);
      Iterator<String> iterDBs = sortedDBs.iterator();

      while (iterDBs.hasNext()) {
        outStream.writeBytes(iterDBs.next());
        outStream.write(terminator);
      }
      ((FSDataOutputStream) outStream).close();
    } catch (FileNotFoundException e) {
      LOG.warn("show databases: " + StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "show databases: " + StringUtils.stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.warn("show databases: " + StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "show databases: " + StringUtils.stringifyException(e));
      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString());
    }
    return 0;
  }

  private int showCreateTable(Hive db, showCreateTableDesc showCTD)
      throws HiveException {
    String dbname = SessionState.get().getDbName();
    String tableName = showCTD.getTable().toLowerCase();
    String usrname = SessionState.get().getUserName();

    if (!db.hasAuth(usrname, Hive.Privilege.SELECT_PRIV, dbname, tableName)) {
      console.printError("user : " + usrname
          + " do not have privilege on table : " + dbname + "." + tableName);
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "user : " + usrname + " do not have privilege on table : " + dbname
                + "." + tableName);
      return 1;
    }

    Table tbl = db.getTable(SessionState.get().getDbName(), showCTD.getTable());
    if (tbl.isView()) {
      throw new HiveException("Could not use Show Create Table with VIEW!");
    }

    try {
      FileSystem fs = showCTD.getResFile().getFileSystem(conf);
      DataOutput outStream = (DataOutput) fs.create(showCTD.getResFile());

      String external = (tbl.getProperty("EXTERNAL") != null && tbl
          .getProperty("EXTERNAL").equalsIgnoreCase("TRUE")) ? "EXTERNAL " : "";

      String[] colnames = tbl
          .getSchema()
          .getProperty(
              org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_COLUMNS)
          .split(",");
      String[] coltypes = tbl
          .getSchema()
          .getProperty(
              org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_COLUMN_TYPES)
          .split(":");
      ArrayList<String> colcomments = tbl.getColComments();
      org.apache.hadoop.hive.metastore.api.Partition priPart = tbl.getTTable()
          .getPriPartition();
      org.apache.hadoop.hive.metastore.api.Partition subPart = tbl.getTTable()
          .getSubPartition();

      LOG.debug("get priPartition and subPartition OK");
      outStream.writeBytes("CREATE " + external + "TABLE " + tableName + "(");

      for (int i = 0; i < colnames.length; ++i) {
        if (i != 0) {
          outStream.writeBytes(",");
        }
        outStream.write(terminator);
        outStream.writeBytes("    " + colnames[i].toLowerCase() + " "
            + coltypes[i].toUpperCase());

        if (colcomments.get(i) != null) {
          outStream
              .write((" COMMENT " + "'" + escapestring(colcomments.get(i)) + "'")
                  .getBytes("UTF-8"));
        }
      }
      outStream.write(terminator);
      outStream.writeBytes(")");

      String tablecomment = tbl.getProperty("comment");
      if (tablecomment != null) {
        outStream.write(terminator);
        outStream.write(("COMMENT " + "'" + escapestring(tablecomment) + "'")
            .getBytes("UTF-8"));
      }

      String tableServer = tbl.getSerdeParam(Constants.TABLE_SERVER);

      if (tableServer != null && !tableServer.isEmpty()
          && !tbl.getTableStorgeType().equalsIgnoreCase("pgdata")) {
        String port = tbl.getSerdeParam(Constants.TABLE_PORT);
        String db_name = tbl.getSerdeParam(Constants.TBALE_DB);
        String user = tbl.getSerdeParam(Constants.DB_URSER);
        String pwd = tbl.getSerdeParam(Constants.DB_PWD);
        String type = tbl.getSerdeParam(Constants.DB_TYPE);
        String table = tbl.getSerdeParam(Constants.TABLE_NAME);
        outStream.writeBytes(" WITH ( ip='" + tableServer + "',port='" + port
            + "',db_name='" + db_name + "',user_name='" + user + "',pwd='"
            + pwd + "',");

        if (table != null && !table.isEmpty()) {
          outStream.writeBytes("table_name='" + table + "'");
        } else {
          String sql = tbl.getSerdeParam(Constants.TBALE_SQL);
          outStream.writeBytes("sql='" + sql + "'");
        }
        outStream.writeBytes(",db_type='" + type + "')");
        ((FSDataOutputStream) outStream).close();
        return 0;
      }

      if (tbl.getTableStorgeType().equalsIgnoreCase("pgdata")) {
        outStream.writeBytes("stored as pgdata");
        ((FSDataOutputStream) outStream).close();
        return 0;
      }

      if (priPart != null) {
        Map<String, List<String>> partSpace = priPart.getParSpaces();
        if (partSpace != null && !partSpace.isEmpty()
            && partSpace.containsKey("default")) {
          List<String> defSpace = partSpace.remove("default");
          partSpace.put("default", defSpace);
        }

        outStream.write(terminator);

        if (priPart.getParType().equalsIgnoreCase("hash")) {
          outStream.writeBytes("PARTITION BY " + "HASHKEY" + "( "
              + priPart.getParKey().getName().toLowerCase() + " )");
        } else {
          outStream.writeBytes("PARTITION BY "
              + priPart.getParType().toUpperCase() + "( "
              + priPart.getParKey().getName().toLowerCase() + " )");

          LOG.debug("priPart.getParKey().getName().toLowerCase() OK!");

          String pripttp = priPart.getParKey().getType();

          if (subPart != null) {
            LOG.debug("will get sub partition OK!");

            Map<String, List<String>> subPartSpace = subPart.getParSpaces();
            if (subPartSpace != null && !subPartSpace.isEmpty()
                && subPartSpace.containsKey("default")) {
              List<String> defSpace = subPartSpace.remove("default");
              subPartSpace.put("default", defSpace);
            }

            outStream.write(terminator);

            if (subPart.getParType().equalsIgnoreCase("hash")) {
              outStream.writeBytes("SUBPARTITION BY " + "HASHKEY" + "( "
                  + subPart.getParKey().getName().toLowerCase() + " )");
            } else {
              outStream.writeBytes("SUBPARTITION BY "
                  + subPart.getParType().toUpperCase() + "( "
                  + subPart.getParKey().getName().toLowerCase() + " )(");
              outStream.write(terminator);
              String op = subPart.getParType().toLowerCase().equals("list") ? " VALUES IN "
                  : " VALUES LESS THAN ";
              String subpttp = subPart.getParKey().getType();
              Iterator<Map.Entry<String, List<String>>> itr = subPart
                  .getParSpaces().entrySet().iterator();

              while (itr.hasNext()) {
                Map.Entry<String, List<String>> sub_cur = itr.next();
                if (sub_cur.getKey().equalsIgnoreCase("default")) {
                  outStream.writeBytes("    SUBPARTITION "
                      + sub_cur.getKey().toLowerCase());
                } else {
                  outStream.writeBytes("    SUBPARTITION "
                      + sub_cur.getKey().toLowerCase() + op + "( "
                      + getPartValues(sub_cur.getValue(), subpttp) + " )");
                }

                if (itr.hasNext())
                  outStream.writeBytes(",");

                outStream.write(terminator);
              }
              outStream.writeBytes(")");
            }

          }

          LOG.debug("will get sub partition done !");
          outStream.write(terminator);
          outStream.writeBytes("(");
          outStream.write(terminator);
          String pri_op = priPart.getParType().toLowerCase().equals("list") ? " VALUES IN "
              : " VALUES LESS THAN ";
          Iterator<Map.Entry<String, List<String>>> pri_itr = priPart
              .getParSpaces().entrySet().iterator();

          while (pri_itr.hasNext()) {
            Map.Entry<String, List<String>> cur = pri_itr.next();
            if (cur.getKey().equalsIgnoreCase("default")) {
              outStream.writeBytes("    PARTITION "
                  + cur.getKey().toLowerCase());
            } else {
              outStream.writeBytes("    PARTITION "
                  + cur.getKey().toLowerCase() + pri_op + "( "
                  + getPartValues(cur.getValue(), pripttp) + " )");
            }

            if (pri_itr.hasNext())
              outStream.writeBytes(",");
            outStream.write(terminator);
          }
          outStream.writeBytes(")");
        }
      }

      LOG.debug("external : "
          + external
          + ",format :"
          + tbl.getTTable().getSd().getSerdeInfo().getParameters()
              .get(Constants.SERIALIZATION_FORMAT) + ",serde info size : "
          + tbl.getTTable().getSd().getSerdeInfo().getParameters().size());

      if (!external.equalsIgnoreCase("")
          && (tbl.getTTable().getSd().getSerdeInfo().getParameters().size() > 1)) {
        outStream.write(terminator);
        outStream.writeBytes("ROW FORMAT DELIMITED");

        if (tbl.getTTable().getSd().getSerdeInfo().getParameters()
            .containsKey(Constants.FIELD_DELIM)) {
          outStream.write(terminator);
          outStream.writeBytes("FIELDS TERMINATED BY '".toUpperCase()
              + tbl.getTTable().getSd().getSerdeInfo().getParameters()
                  .get(Constants.FIELD_DELIM) + "'");
        }

        if (tbl.getTTable().getSd().getSerdeInfo().getParameters()
            .containsKey(Constants.COLLECTION_DELIM)) {

          outStream.write(terminator);
          outStream.writeBytes("COLLECTION ITEMS TERMINATED BY '".toUpperCase()
              + tbl.getTTable().getSd().getSerdeInfo().getParameters()
                  .get(Constants.COLLECTION_DELIM) + "'");
        }

        if (tbl.getTTable().getSd().getSerdeInfo().getParameters()
            .containsKey(Constants.MAPKEY_DELIM)) {
          outStream.write(terminator);
          outStream.writeBytes("MAP KEYS TERMINATED BY '".toUpperCase()
              + tbl.getTTable().getSd().getSerdeInfo().getParameters()
                  .get(Constants.MAPKEY_DELIM) + "'");
        }

        if (tbl.getTTable().getSd().getSerdeInfo().getParameters()
            .containsKey(Constants.LINE_DELIM)) {
          outStream.write(terminator);
          outStream.writeBytes("LINES TERMINATED BY '".toUpperCase()
              + tbl.getTTable().getSd().getSerdeInfo().getParameters()
                  .get(Constants.LINE_DELIM) + "'");
        }
      }

      if (tbl.getTTable().getParameters().containsKey("type")) {
        outStream.write(terminator);
        if (tbl.getTTable().getParameters().get("type")
            .equalsIgnoreCase("format")) {
          outStream.writeBytes("STORED AS FORMATFILE");
        } else if (tbl.getTTable().getParameters().get("type")
            .equalsIgnoreCase("rcfile")) {
          outStream.writeBytes("STORED AS RCFILE");
        } else if (tbl.getTTable().getParameters().get("type")
            .equalsIgnoreCase("column")) {
          outStream.writeBytes("STORED AS COLUMNFILE");
          if (tbl.getTTable().getParameters().get("projection") != null) {
            outStream.writeBytes(" PROJECTION");
            String proj = tbl.getTTable().getParameters().get("projection");
            String[] projs = proj.split(";");
            List<FieldSchema> cols = tbl.getTTable().getSd().getCols();
            boolean first = true;

            for (String s : projs) {
              if (first) {
                first = false;
              } else {
                outStream.writeBytes(",");
              }

              outStream.writeBytes(" ( ");

              boolean flag = true;
              String[] element = s.split(",");
              for (int colidex = 0; colidex < element.length; ++colidex) {
                if (flag) {
                  flag = false;
                } else {
                  outStream.writeBytes(" , ");
                }

                outStream.writeBytes(cols
                    .get(Integer.parseInt((element[colidex]))).getName()
                    .toLowerCase());
              }
              outStream.writeBytes(" )");
            }
          }
        }

        if (tbl.getTTable().getSd().isCompressed()) {
          outStream.writeBytes(" COMPRESS");
        }
      }

      if (tbl.getParameters().get("PB_TABLE") != null
          && tbl.getParameters().get("PB_TABLE").equalsIgnoreCase("TRUE")) {
        throw new HiveException(
            "SHOW CREATE TABLE can not show protobuf Table DDL!");
      }

      if (tbl.getTTable().getSd().getSerdeInfo().getParameters()
          .get(Constants.SERIALIZATION_CHARSET) != null) {
        outStream.write(terminator);
        if (tbl.getTTable().getSd().getSerdeInfo().getParameters()
            .get(Constants.SERIALIZATION_CHARSET).equalsIgnoreCase("gbk")) {
          outStream.writeBytes("WITH ( CHARSET = 'GBK' )");
        } else if (tbl.getTTable().getSd().getSerdeInfo().getParameters()
            .get(Constants.SERIALIZATION_CHARSET).equalsIgnoreCase("utf8")
            || tbl.getTTable().getSd().getSerdeInfo().getParameters()
                .get(Constants.SERIALIZATION_CHARSET).equalsIgnoreCase("utf-8")) {
          outStream.writeBytes("WITH ( CHARSET = 'UTF8' )");
        }
      }

      if (!external.equalsIgnoreCase("")) {
        outStream.write(terminator);
        outStream.writeBytes("LOCATION '"
            + tbl.getTTable().getSd().getLocation() + "'");
      }

      outStream.writeBytes(";");

      if (tbl.getTTable().getParameters().containsKey("indexNum")) {
        int indexNum = Integer.parseInt(tbl.getTTable().getParameters()
            .get("indexNum"));
        String indexKey = null;
        String indexPara = null;
        String[] element = null;
        String indexName = null;
        String indexKeys = null;
        String[] keys = null;
        if (indexNum > 0) {
          for (int i = 0; i < indexNum; ++i) {
            indexKey = "index" + i;
            assert tbl.getTTable().getParameters().containsKey(indexKey);
            indexPara = tbl.getTTable().getParameters().get(indexKey);
            element = indexPara.split(";");
            indexName = element[0];
            indexKeys = element[1];

            outStream.write(terminator);
            outStream.writeBytes("ALTER TABLE " + tableName + " ADD INDEX "
                + indexName + " ( ");
            keys = indexKeys.split(",");
            for (int j = 0; j < keys.length; ++j) {
              if (j != 0) {
                outStream.writeBytes(" , ");
              }
              outStream.writeBytes(colnames[Integer.parseInt(keys[j])]);
            }

            outStream.writeBytes(" );");
          }
        }
      }

      ((FSDataOutputStream) outStream).close();
    } catch (FileNotFoundException e) {
      LOG.warn("show create table: " + StringUtils.stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.warn("show create table: " + StringUtils.stringifyException(e));
      return 1;
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e.getMessage());
    }

    return 0;
  }

  private int showIndex(Hive db, showIndexDesc showIndexsDesc)
      throws HiveException {
    if (showIndexsDesc.getOP() == showIndexTypes.SHOWTABLEINDEX) {
      String tblName = showIndexsDesc.getTblName();
      String dbName = showIndexsDesc.getDBName();

      if (dbName.length() == 0) {
        dbName = SessionState.get().getDbName();
        showIndexsDesc.setDBName(dbName);
      }

      if (tblName == null || tblName.length() == 0 || dbName.length() == 0) {
        console.printError("Table name error");
        if (SessionState.get() != null)
          SessionState.get().ssLog("Table name error");
        return 1;
      }

      List<IndexItem> indexInfo = db.getAllIndexTable(dbName, tblName);
      if (indexInfo == null || indexInfo.isEmpty()) {
        System.out.println("No index found in table:" + tblName);
        return 0;
      }
      for (int i = 0; i < indexInfo.size(); i++) {
        System.out.println(i + ", fieldList:" + indexInfo.get(i).getFieldList()
            + ", type:" + indexInfo.get(i).getType() + ", location:"
            + indexInfo.get(i).getLocation() + ", status:"
            + indexInfo.get(i).getStatus());
      }

      System.out.println(indexInfo.size() + " index found");
      return 0;
    } else {
      List<IndexItem> indexInfo = db.getAllIndexSys();
      if (indexInfo == null || indexInfo.isEmpty()) {
        System.out.println("No index found in sys");
        return 0;
      }
      for (int i = 0; i < indexInfo.size(); i++) {
        System.out.println(i + ", db:" + indexInfo.get(i).getDb() + ", tbl:"
            + indexInfo.get(i).getTbl() + ", fieldList:"
            + indexInfo.get(i).getFieldList() + ", type:"
            + indexInfo.get(i).getType() + ", location:"
            + indexInfo.get(i).getLocation() + ", status:"
            + indexInfo.get(i).getStatus());
      }

      System.out.println(indexInfo.size() + " index found");

      return 0;
    }
  }

  private int showVersion(Hive db, showVersionDesc versionDesc)
      throws HiveException {
    try {
      FileSystem fs = versionDesc.getResFile().getFileSystem(conf);
      DataOutput outStream = (DataOutput) fs.create(versionDesc.getResFile());

      outStream.writeBytes("Tencent Data Warehouse version: "
          + HiveConf.getHiveServerVersion());
      ((FSDataOutputStream) outStream).close();
    } catch (FileNotFoundException e) {
      LOG.warn("show version: " + StringUtils.stringifyException(e));
      return 1;
    } catch (IOException e) {
      LOG.warn("show version: " + StringUtils.stringifyException(e));
      return 1;
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e.getMessage());
    }

    return 0;
  }

  private String getPartValues(List<String> lst, String type) {
    String rt = "";
    for (int i = 0; i < lst.size(); ++i) {
      if (i != 0)
        rt += " , ";
      if (type.equalsIgnoreCase("string"))
        rt = rt + "'" + lst.get(i) + "'";
      else
        rt += lst.get(i);
    }

    return rt;
  }

  private int alterComment(Hive db, alterCommentDesc acd) throws HiveException {
    String dbName = acd.getDbName();
    if (dbName == null)
      dbName = SessionState.get().getDbName();

    if (!db.hasAuth(SessionState.get().getUserName(),
        Hive.Privilege.ALTER_PRIV, dbName, acd.getTblName())) {
      console.printError("user : " + SessionState.get().getUserName()
          + " do not have privilege on table : " + dbName + "."
          + acd.getTblName());
      if (SessionState.get() != null) {
        SessionState.get().ssLog(
            "user : " + SessionState.get().getUserName()
                + " do not have privilege on table : " + dbName + "."
                + acd.getTblName());
      }

      return 1;
    }

    try {
      boolean isView;
      try {
        isView = db.isView(dbName, acd.getTblName());
      } catch (NoSuchObjectException e) {
        console.printError(e.getMessage());
        if (SessionState.get() != null)
          SessionState.get().ssLog(e.getMessage());
        return 1;
      }

      if (acd.getCT() == alterCommentDesc.alterCommentTypes.TABLECOMMENT) {
        if (isView) {
          console.printError("Can not comment on table for a view!");
          if (SessionState.get() != null)
            SessionState.get().ssLog("Can not comment on table for a view!");
          return 1;
        }

        db.modifyTableComment(dbName, acd.getTblName(), acd.getComment());
      } else if (acd.getCT() == alterCommentDesc.alterCommentTypes.VIEWCOMMENT) {
        if (!isView) {
          console.printError("Can not comment on view for a table!");
          if (SessionState.get() != null)
            SessionState.get().ssLog("Can not comment on view for a table!");
          return 1;
        }

        db.modifyTableComment(dbName, acd.getTblName(), acd.getComment());
      } else {
        try {
          db.modifyColumnComment(dbName, acd.getTblName(), acd.getColName(),
              acd.getComment());
        } catch (Exception x) {
          console.printError(x.getMessage());
          if (SessionState.get() != null)
            SessionState.get().ssLog(x.getMessage());
          return 1;
        }
      }
    }

    catch (InvalidOperationException e) {
      console.printError("Invalid alter comment operation: " + e.getMessage());
      LOG.info("alter comment: " + StringUtils.stringifyException(e));
      if (SessionState.get() != null) {
        SessionState.get().ssLog("Invalid alter comment: " + e.getMessage());
        SessionState.get().ssLog(
            "alter comment: " + StringUtils.stringifyException(e));
      }
      return 1;
    } catch (HiveException e) {
      e.printStackTrace();
      return 1;
    }

    return 0;

  }

  private int createView(Hive db, createViewDesc crtView) throws HiveException {
    if (!db.hasAuth(SessionState.get().getUserName(),
        Hive.Privilege.CREATE_VIEW_PRIV, SessionState.get().getDbName(), null)) {

      console.printError("user : " + SessionState.get().getUserName()
          + " do not have create view privilege on DB : "
          + SessionState.get().getDbName());

      if (SessionState.get() != null) {
        SessionState.get().ssLog(
            "user : " + SessionState.get().getUserName()
                + " do not have privilege on DB : "
                + SessionState.get().getDbName());
      }
      return 1;
    }

    Table tbl = new Table(crtView.getViewName());
    tbl.setTableType(TableType.VIRTUAL_VIEW);
    tbl.setSerializationLib(null);
    tbl.getTTable().getSd().getSerdeInfo().getParameters().clear();
    tbl.setViewOriginalText(crtView.getViewOriginalText());
    tbl.setViewExpandedText(crtView.getViewExpandedText());
    tbl.setFields(crtView.getSchema());
    List<String> vTables = crtView.getVtables();

    String view_table = "";
    int count = 0;

    for (String s : vTables) {
      if (count == 0)
        view_table += s;
      else

        view_table += ";" + s;

      count++;
    }

    tbl.setViewTables(view_table);

    if (crtView.getComment() != null) {
      tbl.setProperty("comment", crtView.getComment());
    }
    if (crtView.getTblProps() != null) {
      tbl.getTTable().getParameters().putAll(crtView.getTblProps());
    }

    int rc = setGenericTableAttributes(tbl);

    if (rc != 0) {
      return rc;
    }

    tbl.getTTable().setIsReplaceOnExit(crtView.getOrReplace());
    db.createTable(tbl, crtView.getIfNotExists());
    return 0;
  }

  private int setGenericTableAttributes(Table tbl) {
    try {
      tbl.setOwner(SessionState.get().getUserName());
    } catch (Exception e) {
      console.printError("Unable to get current user: " + e.getMessage(),
          StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "Unable to get current user: " + e.getMessage()
                + StringUtils.stringifyException(e));
      return 1;
    }

    tbl.getTTable().setCreateTime((int) (System.currentTimeMillis() / 1000));
    return 0;
  }

  @Override
  public List<? extends Node> getChildren() {
    return super.getChildTasks();
  }

  @Override
  public String getName() {
    return "DDL";
  }

  public static boolean needOctalEscape(char currentChar) {
    char[] needOctalEscape = { 0, 1, 2, 3, 4, 5, 6, 7, 11, 14, 15, 16, 17, 18,
        19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 127, 129, 141, 143,
        144, 157 };

    for (int i = 0; i < needOctalEscape.length; i++) {
      if (currentChar == needOctalEscape[i])
        return true;
    }
    return false;
  }

  public static String escapestring(String str) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < str.length(); i++) {
      char c = str.charAt(i);

      if (!needOctalEscape(c)) {
        switch (c) {
        case '\n':
          sb.append("\\n");
          break;
        case '\t':
          sb.append("\\t");
          break;
        case '\b':
          sb.append("\\b");
          break;
        case '\f':
          sb.append("\\f");
          break;
        case '\r':
          sb.append("\\r");
          break;
        case '"':
          sb.append("\"");
          break;
        case '\'':
          sb.append("\\'");
          break;
        case '\\':
          sb.append("\\\\");
          break;
        default:
          sb.append(c);
        }
      } else {
        int tmp = c;
        int i3 = tmp % 8;
        tmp = tmp / 8;
        int i2 = 0;
        if (tmp > 0)
          i2 = tmp % 8;
        tmp = tmp / 8;
        int i1 = 0;
        if (tmp > 0)
          i1 = tmp % 8;

        if ((i1 >= 0 && i1 <= 3) && (i2 >= 0 && i2 <= 7)
            && (i3 >= 0 && i3 <= 7)) {
          sb.append("\\");
          sb.append(i1);
          sb.append(i2);
          sb.append(i3);
        }
      }
    }
    return sb.toString();
  }
}
