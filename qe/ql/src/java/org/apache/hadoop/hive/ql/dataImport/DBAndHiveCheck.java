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
package org.apache.hadoop.hive.ql.dataImport;

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

public class DBAndHiveCheck {

  public static final String[] DBTYPES = { "pg", "pgsql", "ora", "oracle" };
  private Map<String, String> props;

  private List<FieldSchema> cols;

  public DBAndHiveCheck(Map<String, String> props, List<FieldSchema> cols) {
    this.props = props;
    this.cols = cols;
  }

  public boolean isValidCols() throws SemanticException {
    ResultSetMetaData rsMD = getMetaData();
    try {
      int count = rsMD.getColumnCount();
      if (count != cols.size()) {
        throw new SemanticException("col num is wrong!");
      } else {
        for (int i = 1; i <= count; i++) {
          int type = rsMD.getColumnType(i);
          String cName = rsMD.getColumnName(i);
          String hiveType = HiveTypes.toHiveType(type);
          if (hiveType == null)
            throw new SemanticException(cName + " type doesn't match "
                + cols.get(i - 1).getName() + "!");
          else if (!cols.get(i - 1).getType().trim().toUpperCase()
              .equals(hiveType)) {
            throw new SemanticException(cName + " type doesn't match "
                + cols.get(i - 1).getName() + " " + cols.get(i - 1).getType()
                + "!");
          }
        }
      }
    } catch (SQLException e) {
      throw new SemanticException(e.getMessage());
    }
    return true;
  }

  private ResultSetMetaData getMetaData() throws SemanticException {
    Connection conn = null;
    try {
      if (checkEmpty(props.get(Constants.TABLE_SERVER))) {
        throw new Exception(Constants.TABLE_SERVER + " is empty!");
      }
      if (checkEmpty(props.get(Constants.TABLE_PORT))) {
        throw new Exception(Constants.TABLE_PORT + " is empty!");
      }
      if (checkEmpty(props.get(Constants.DB_TYPE))) {
        throw new Exception(Constants.DB_TYPE + " is empty!");
      }
      if (!isValidDBType(props.get(Constants.DB_TYPE))) {
        throw new Exception("only support pgsql/pg or oracle/ora!");
      }

      if (checkEmpty(props.get(Constants.TBALE_DB))) {
        throw new Exception(Constants.TBALE_DB + " is empty!");
      }
      if (checkEmpty(props.get(Constants.DB_URSER))) {
        throw new Exception(Constants.DB_URSER + " is empty!");
      }
      if (checkEmpty(props.get(Constants.DB_PWD))) {
        throw new Exception(Constants.DB_PWD + " is empty!");
      }
      ExtractConfig config = new ExtractConfig();
      config.setHost(props.get(Constants.TABLE_SERVER));
      config.setPort(props.get(Constants.TABLE_PORT));
      config.setDbType(props.get(Constants.DB_TYPE));
      config.setDbName(props.get(Constants.TBALE_DB));
      config.setUser(props.get(Constants.DB_URSER));
      config.setPwd(props.get(Constants.DB_PWD));
      String table = props.get(Constants.TABLE_NAME);
      String sql = "";
      if (table != null && !table.isEmpty()) {
        sql = "select * from " + table;
      } else {
        sql = props.get(Constants.TBALE_SQL);
      }
      if (sql == null || sql.equals("")) {
        throw new Exception(Constants.TBALE_SQL + " and "
            + Constants.TABLE_NAME + " both empty!");
      }
      Class.forName(config.getDriver());
      conn = DriverManager.getConnection(config.getUrl(), config.getUser(),
          config.getPwd());
      try {
        String processName = java.lang.management.ManagementFactory
            .getRuntimeMXBean().getName();
        String processID = processName.substring(0, processName.indexOf('@'));
        String appinfo = "getMetaData_" + processID + "_"
            + SessionState.get().getSessionName();
        conn.setClientInfo("ApplicationName", appinfo);
      } catch (Exception e) {
        e.printStackTrace();
      }
      conn.setAutoCommit(false);
      Statement s = conn.createStatement();
      s.setFetchSize(1);
      ResultSet set = s.executeQuery(sql);
      conn.commit();
      ResultSetMetaData meta = set.getMetaData();
      return meta;

    } catch (Exception e) {
      throw new SemanticException(e.getMessage());
    }
  }

  public static boolean checkEmpty(String str) {
    if (str == null || str.trim().equals(""))
      return true;
    else
      return false;
  }

  private boolean isValidDBType(String dbType) {
    boolean flag = false;
    for (String db : DBTYPES) {
      if (dbType.equalsIgnoreCase(db)) {
        flag = true;
        break;
      }
    }
    return flag;
  }
}
