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
package org.apache.hadoop.hive.ql.dataImport.fetchSize;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.apache.hadoop.hive.ql.dataImport.ExtractConfig;

public class MemoryAdaptiveFetchSize implements IFetchSize {
  private ExtractConfig config;
  private Connection conn;
  private static int memorysize;

  public MemoryAdaptiveFetchSize(ExtractConfig config) throws HiveException {
    super();
    this.config = config;
    memorysize = config.getConf().getInt("db.external.memory.size", 50) * 1024 * 1024;
    initConn();
  }

  @Override
  public int computeFetchSize(ExtractConfig config) {
    String sql = config.getSql();
    try {
      conn.setAutoCommit(false);
      Statement s = conn.createStatement();
      s.setFetchSize(1);
      ResultSet rs = s.executeQuery(sql);
      ResultSetMetaData rsm = rs.getMetaData();
      int total_size = 0;
      for (int i = 1; i <= rsm.getColumnCount(); i++) {
        int cSize = typeSize(rsm.getColumnType(i), rsm.getColumnTypeName(i));
        total_size += cSize;
      }
      int fetchSize = memorysize / total_size;
      if (fetchSize > 200)
        return config.getConf().getInt("db.external.fetch.size", 50);
      else
        return fetchSize;

    } catch (SQLException e) {
      int size = config.getConf().getInt("db.external.fetch.size", 50);
      return size;
    } finally {
      if (conn != null)
        try {
          conn.close();
        } catch (SQLException e) {
          int size = config.getConf().getInt("db.external.fetch.size", 50);
          return size;
        }
    }
  }

  private void initConn() throws HiveException {
    if (config != null) {
      try {
        Class.forName(config.getDriver());
        conn = DriverManager.getConnection(config.getUrl(), config.getUser(),
            config.getPwd());
        try {
          String processName = java.lang.management.ManagementFactory
              .getRuntimeMXBean().getName();
          String processID = processName.substring(0, processName.indexOf('@'));
          String appinfo = "initConn_" + processID + "_"
              + SessionState.get().getSessionName();
          conn.setClientInfo("ApplicationName", appinfo);
        } catch (Exception e) {
          e.printStackTrace();
        }
      } catch (ClassNotFoundException e) {
        throw new HiveException(e.getMessage());
      } catch (SQLException e) {
        throw new HiveException(e.getMessage());
      }
    }
  }

  private int typeSize(int sqlType, String type_name) {

    switch (sqlType) {
    case Types.INTEGER:
    case Types.SMALLINT:
      return 4;
    case Types.DATE:
    case Types.TIME:
    case Types.TIMESTAMP:
      return 22;
    case Types.VARCHAR:
    case Types.CHAR:
    case Types.LONGVARCHAR:
    case Types.NVARCHAR:
    case Types.NCHAR:
    case Types.LONGNVARCHAR:
    case Types.CLOB:
    case Types.OTHER:
      return 4000 * 2;
    case Types.NUMERIC:
    case Types.DECIMAL:
    case Types.FLOAT:
    case Types.DOUBLE:
    case Types.REAL:
      return 12;
    case Types.BIT:
    case Types.BOOLEAN:
      return 2;
    case Types.TINYINT:
      return 4;
    case Types.BIGINT:
      return 12;
    default:
      return 22;
    }
  }

}
