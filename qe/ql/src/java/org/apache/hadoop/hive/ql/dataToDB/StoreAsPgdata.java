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
package org.apache.hadoop.hive.ql.dataToDB;

import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.dataImport.ExtractConfig;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.loadTableDesc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.Constants;

import java.sql.Types;

public class StoreAsPgdata {
  private Log log = LogFactory.getLog(ExtractConfig.class);
  private String driver = "org.postgresql.Driver";
  private String dbType = "pg";
  private String host;
  private String port;
  private String url;
  private String dbName;
  private String user;
  private String pwd;
  private HiveConf conf;
  private String fd = "";
  private String rd = System.getProperty("line.separator");
  private int bufferLimit = 50;
  private Table table;
  private String dbTable;
  private String filePath;
  private boolean structure = false;
  private boolean text = true;
  private boolean outoCommit;
  private loadTableDesc desc;

  private String oText = null;

  private static Statement stat;

  private List<FieldSchema> cols;
  private String tableName;
  private String tdwSql;

  public StoreAsPgdata(String tableName, List<FieldSchema> cols) {
    this.tableName = tableName;
    this.cols = cols;
  }

  public StoreAsPgdata() {

  }

  public static String toPgType(String sqlType) {
    String type = " ";
    if (sqlType.equalsIgnoreCase("boolean")) {
      type = "boolean";
      return type;
    } else if (sqlType.equalsIgnoreCase("smallint")) {
      type = "smallint";
      return type;
    } else if (sqlType.equalsIgnoreCase("tinyint")) {
      type = "integer";
      return type;
    } else if (sqlType.equalsIgnoreCase("int")) {
      type = "integer";
      return type;
    } else if (sqlType.equalsIgnoreCase("bigint")) {
      type = "bigint";
      return type;
    } else if (sqlType.equalsIgnoreCase("float")) {
      type = "real";
      return type;
    } else if (sqlType.equalsIgnoreCase("double")) {
      type = "double precision";
      return type;
    } else if (sqlType.equalsIgnoreCase("timestamp")) {
      type = "timestamp";
      return type;
    } else if (sqlType.equalsIgnoreCase("string")) {
      type = "text";
      return type;
    } else {
      return sqlType;
    }
  }

  public String tdwSqlToPgsql(String tableName, List<FieldSchema> cols)
      throws SemanticException {
    tdwSql = "create table " + tableName + "(";
    int colNum = cols.size();

    if (colNum <= 0) {
      throw new SemanticException(
          "cannot convert tdw sql to pg sql because of no columns");
    }

    for (int i = 0; i < colNum - 1; i++) {
      tdwSql = tdwSql + cols.get(i).getName() + " ";
      tdwSql = tdwSql + toPgType(cols.get(i).getType()) + ", ";
    }

    tdwSql = tdwSql + cols.get(colNum - 1).getName() + " ";
    tdwSql = tdwSql + toPgType(cols.get(colNum - 1).getType()) + ")";

    return tdwSql;
  }

  public Connection sendCreatTableToPg(Table tbl) throws SemanticException {
    Connection conn = null;
    checkProperty(tbl);

    ExtractConfig config = new ExtractConfig();
    setConfig(tbl, config);
    String sql = config.getSql();
    log.info("url: " + config.getUrl());
    log.info("user: " + config.getUser());
    log.info("pwd: " + config.getPwd());
    log.info("sql: " + sql);
    try {
      if (sql == null || sql.equals("")) {
        throw new Exception(Constants.TBALE_SQL + " and "
            + Constants.TABLE_NAME + " both empty!");
      }

      conn = DriverManager.getConnection(config.getUrl(), config.getUser(),
          config.getPwd());
      try {
        String processName = java.lang.management.ManagementFactory
            .getRuntimeMXBean().getName();
        String processID = processName.substring(0, processName.indexOf('@'));
        String appinfo = "sendCreatTableToPg_" + processID + "_"
            + SessionState.get().getSessionName();
        conn.setClientInfo("ApplicationName", appinfo);
      } catch (Exception e) {
        e.printStackTrace();
      }
      conn.setAutoCommit(false);
      Statement s = conn.createStatement();

      s.execute(sql);
    } catch (Exception e) {
      try {
        if (conn != null) {
          conn.rollback();
        }
        throw new SemanticException(e.getMessage());
      } catch (Exception exc) {
        throw new SemanticException(exc.getMessage());
      }
    }
    return conn;
  }

  public Connection sendCreatTableToPg(Table tbl, String sql)
      throws SemanticException {
    Connection conn = null;
    checkProperty(tbl);

    ExtractConfig config = new ExtractConfig();
    setConfig(tbl, config);
    log.info("url: " + config.getUrl());
    log.info("user: " + config.getUser());
    log.info("pwd: " + config.getPwd());
    log.info("sql: " + sql);
    try {
      if (sql == null || sql.equals("")) {
        throw new Exception(Constants.TBALE_SQL + " and "
            + Constants.TABLE_NAME + " both empty!");
      }

      conn = DriverManager.getConnection(config.getUrl(), config.getUser(),
          config.getPwd());
      try {
        String processName = java.lang.management.ManagementFactory
            .getRuntimeMXBean().getName();
        String processID = processName.substring(0, processName.indexOf('@'));
        String appinfo = "sendCreatTableToPg_" + processID + "_"
            + SessionState.get().getSessionName();
        conn.setClientInfo("ApplicationName", appinfo);
      } catch (Exception e) {
        e.printStackTrace();
      }

      conn.setAutoCommit(false);
      stat = conn.createStatement();

      stat.execute(sql);
    } catch (Exception e) {
      try {
        if (conn != null) {
          conn.rollback();
        }
        throw new SemanticException(e.getMessage());
      } catch (Exception exc) {
        throw new SemanticException(exc.getMessage());
      }
    }
    return conn;
  }

  public Connection sendDropTableToPg(Table tbl) throws SemanticException {
    Connection conn = null;
    checkProperty(tbl);
    try {
      ExtractConfig config = new ExtractConfig();
      setConfig(tbl, config);
      String table = tbl.getName();
      String sql = "drop table " + table;

      log.info("get connection");
      log.info("url: " + config.getUrl());
      log.info("user: " + config.getUser());
      log.info("pwd: " + config.getPwd());

      conn = DriverManager.getConnection(config.getUrl(), config.getUser(),
          config.getPwd());
      try {
        String processName = java.lang.management.ManagementFactory
            .getRuntimeMXBean().getName();
        String processID = processName.substring(0, processName.indexOf('@'));
        String appinfo = "sendDropTableToPg_" + processID + "_"
            + SessionState.get().getSessionName();
        conn.setClientInfo("ApplicationName", appinfo);
      } catch (Exception e) {
        e.printStackTrace();
      }

      conn.setAutoCommit(false);
      log.info("setAutoCommit");

      Statement s = conn.createStatement();
      log.info("createStatement");

      boolean ret;
      ret = s.execute(sql);

      log.info("executeQuery");

    } catch (Exception e) {
      String errStr = e.getMessage().toString();
      if (errStr.contains("not exist")) {
        log.info("the tdw external table is drop in pg");
        return conn;
      } else {
        try {
          if (conn != null) {
            conn.rollback();
          }
          throw new SemanticException(e.getMessage());
        } catch (Exception exc) {
          throw new SemanticException(exc.getMessage());
        }
      }
    }

    return conn;
  }

  public int sendCommitToPg(Connection conn) throws SemanticException {
    try {
      log.info("commit");
      conn.commit();
      if (conn != null) {
        conn.close();
      }

      return 0;
    } catch (Exception e) {
      try {
        if (conn != null) {
          conn.rollback();
        }
        throw new SemanticException(e.getMessage());
      } catch (Exception exc) {
        throw new SemanticException(exc.getMessage());
      }
    }
  }

  public int sendTruncateTableToPg(Table tbl) throws SemanticException {
    Connection conn = null;
    checkProperty(tbl);
    try {
      ExtractConfig config = new ExtractConfig();
      setConfig(tbl, config);
      String table = tbl.getName();
      String sql = "truncate table " + table;
      log.info("sql: " + sql);

      conn = DriverManager.getConnection(config.getUrl(), config.getUser(),
          config.getPwd());
      try {
        String processName = java.lang.management.ManagementFactory
            .getRuntimeMXBean().getName();
        String processID = processName.substring(0, processName.indexOf('@'));
        String appinfo = "sendTruncateTableToPg_" + processID + "_"
            + SessionState.get().getSessionName();
        conn.setClientInfo("ApplicationName", appinfo);
      } catch (Exception e) {
        e.printStackTrace();
      }

      conn.setAutoCommit(false);
      log.info("setAutoCommit");

      Statement s = conn.createStatement();
      log.info("createStatement");

      s.execute(sql);
      conn.commit();
      if (conn != null) {
        conn.close();
      }
    } catch (Exception e) {
      try {
        if (conn != null) {
          conn.rollback();
        }
        throw new SemanticException(e.getMessage());
      } catch (Exception exc) {
        throw new SemanticException(exc.getMessage());
      }
    }

    return 0;
  }

  public int sendUpdateTableToPg(Table tbl, String sql)
      throws SemanticException {
    Connection conn = null;
    checkProperty(tbl);
    try {
      ExtractConfig config = new ExtractConfig();
      setConfig(tbl, config);
      log.info("update sql: " + sql);

      conn = DriverManager.getConnection(config.getUrl(), config.getUser(),
          config.getPwd());
      try {
        String processName = java.lang.management.ManagementFactory
            .getRuntimeMXBean().getName();
        String processID = processName.substring(0, processName.indexOf('@'));
        String appinfo = "sendUpdateTableToPg_" + processID + "_"
            + SessionState.get().getSessionName();
        conn.setClientInfo("ApplicationName", appinfo);
      } catch (Exception e) {
        e.printStackTrace();
      }

      conn.setAutoCommit(false);
      log.info("setAutoCommit");

      Statement s = conn.createStatement();
      log.info("createStatement");

      s.execute(sql);
      conn.commit();
      if (conn != null) {
        conn.close();
      }
    } catch (Exception e) {
      try {
        if (conn != null) {
          conn.rollback();
        }
        throw new SemanticException(e.getMessage());
      } catch (Exception exc) {
        throw new SemanticException(exc.getMessage());
      }
    }

    return 0;
  }

  public int sendDeleteTableToPg(Table tbl, String sql)
      throws SemanticException {
    Connection conn = null;
    checkProperty(tbl);
    try {
      ExtractConfig config = new ExtractConfig();
      setConfig(tbl, config);
      log.info("delete sql: " + sql);

      conn = DriverManager.getConnection(config.getUrl(), config.getUser(),
          config.getPwd());
      try {
        String processName = java.lang.management.ManagementFactory
            .getRuntimeMXBean().getName();
        String processID = processName.substring(0, processName.indexOf('@'));
        String appinfo = "sendDeleteTableToPg_" + processID + "_"
            + SessionState.get().getSessionName();
        conn.setClientInfo("ApplicationName", appinfo);
      } catch (Exception e) {
        e.printStackTrace();
      }

      conn.setAutoCommit(false);
      log.info("setAutoCommit");

      Statement s = conn.createStatement();
      log.info("createStatement");

      s.execute(sql);
      conn.commit();
      if (conn != null) {
        conn.close();
      }
    } catch (Exception e) {
      try {
        if (conn != null) {
          conn.rollback();
        }
        throw new SemanticException(e.getMessage());
      } catch (Exception exc) {
        throw new SemanticException(exc.getMessage());
      }
    }

    return 0;
  }

  public int sendDelteTableToPg(Table tbl) throws SemanticException {
    Connection conn = null;
    checkProperty(tbl);
    try {
      ExtractConfig config = new ExtractConfig();
      setConfig(tbl, config);
      String table = tbl.getName();
      String sql = "delete table " + table;

      conn = DriverManager.getConnection(config.getUrl(), config.getUser(),
          config.getPwd());
      try {
        String processName = java.lang.management.ManagementFactory
            .getRuntimeMXBean().getName();
        String processID = processName.substring(0, processName.indexOf('@'));
        String appinfo = "sendDelteTableToPg_" + processID + "_"
            + SessionState.get().getSessionName();
        conn.setClientInfo("ApplicationName", appinfo);
      } catch (Exception e) {
        e.printStackTrace();
      }

      conn.setAutoCommit(false);
      log.info("setAutoCommit");

      Statement s = conn.createStatement();
      log.info("createStatement");

      s.execute(sql);
      conn.commit();
      if (conn != null) {
        conn.close();
      }
    } catch (Exception e) {
      try {
        if (conn != null) {
          conn.rollback();
        }
        throw new SemanticException(e.getMessage());
      } catch (Exception exc) {
        throw new SemanticException(exc.getMessage());
      }
    }

    return 0;
  }

  public boolean checkProperty(Table tbl) throws SemanticException {
    boolean flag = true;
    if (checkEmpty(tbl.getSerdeParam(Constants.TABLE_SERVER))) {
      throw new SemanticException(Constants.TABLE_SERVER + " is empty!");
    }
    if (checkEmpty(tbl.getSerdeParam(Constants.TABLE_PORT))) {
      throw new SemanticException(Constants.TABLE_PORT + " is empty!");
    }
    if (checkEmpty(tbl.getSerdeParam(Constants.DB_TYPE))) {
      throw new SemanticException(Constants.DB_TYPE + " is empty!");
    }
    if (checkEmpty(tbl.getSerdeParam(Constants.TBALE_DB))) {
      throw new SemanticException(Constants.TBALE_DB + " is empty!");
    }
    if (checkEmpty(tbl.getSerdeParam(Constants.DB_URSER))) {
      throw new SemanticException(Constants.DB_URSER + " is empty!");
    }
    if (checkEmpty(tbl.getSerdeParam(Constants.DB_PWD))) {
      throw new SemanticException(Constants.DB_PWD + " is empty!");
    }

    return flag;
  }

  public static boolean checkEmpty(String str) {
    if (str == null || str.trim().equals(""))
      return true;
    else
      return false;
  }

  public static Statement get_stat() {
    Statement s = stat;
    return s;
  }

  public void setConfig(Table tbl, ExtractConfig config) {
    config.setHost(tbl.getSerdeParam(Constants.TABLE_SERVER));
    config.setPort(tbl.getSerdeParam(Constants.TABLE_PORT));
    config.setDbType(tbl.getSerdeParam(Constants.DB_TYPE));
    config.setDbName(tbl.getSerdeParam(Constants.TBALE_DB));
    config.setUser(tbl.getSerdeParam(Constants.DB_URSER));
    config.setPwd(tbl.getSerdeParam(Constants.DB_PWD));
    config.setSql(tbl.getSerdeParam(Constants.TBALE_SQL));
  }

}
