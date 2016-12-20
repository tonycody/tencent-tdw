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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;

public class BaseDataExtract {
  private Log log = LogFactory.getLog(BaseDataExtract.class);
  private ExtractConfig config = null;
  private Connection conn;
  private FileSystem fs;
  private boolean dataSwitch = false;

  public BaseDataExtract() {

  }

  public BaseDataExtract(ExtractConfig config) {
    this.config = config;
  }

  public void extractDataToHive() throws HiveException {
    try {
      dataSwitch = config.getConf().getBoolean("data.line.seperator.filter",
          false);
      initConn();
      ResultSet rs = this.executeQuery(config.getSql());
      ResultSetMetaData meta = rs.getMetaData();
      int num = meta.getColumnCount();
      FSDataOutputStream fsDOS = fs.create(new Path(config.getFilePath()),
          true, 1024 * 1024 * 16);
      if (rs.next()) {
        if (num > 0) {
          String s1 = rs.getString(1);
          if (s1 != null) {
            if (dataSwitch)
              s1 = StringUtil.replaceBlank(s1);
            fsDOS.write(s1.getBytes());
          } else {
            fsDOS.write("\\N".getBytes());
          }
          for (int i = 2; i <= num; i++) {
            fsDOS.write((byte) 1);
            s1 = rs.getString(i);
            if (s1 != null) {
              if (dataSwitch)
                s1 = StringUtil.replaceBlank(s1);
              fsDOS.write(s1.getBytes());
            } else {
              fsDOS.write("\\N".getBytes());
            }
          }
        }
      }

      int recordcnt = 1;
      while (rs.next()) {
        fsDOS.write(config.getRd().getBytes());
        String s1 = rs.getString(1);
        if (s1 != null) {
          if (dataSwitch)
            s1 = StringUtil.replaceBlank(s1);
          fsDOS.write(s1.getBytes());
        } else {
          fsDOS.write("\\N".getBytes());
        }
        for (int i = 2; i <= num; i++) {
          fsDOS.write((byte) 1);
          s1 = rs.getString(i);
          if (s1 != null) {
            if (dataSwitch)
              s1 = StringUtil.replaceBlank(s1);
            fsDOS.write(s1.getBytes());
          } else {
            fsDOS.write("\\N".getBytes());
          }
        }
        if (recordcnt % 10000 == 0 && SessionState.get() != null)
          SessionState.get().ssLog("Extract reocord to hive:" + recordcnt);
        recordcnt++;
      }
      fsDOS.flush();
      fsDOS.close();
    } catch (IOException e) {
      log.error(e.getMessage());
      throw new HiveException(e.getMessage());
    } catch (SQLException e) {
      log.error(e.getMessage());
      throw new HiveException(e.getMessage());
    } finally {
      close();
    }

  }

  public ResultSet executeQuery(String sql) throws HiveException {
    try {
      Statement st = conn.createStatement();
      st.setFetchSize(config.getBufferLimit());
      return st.executeQuery(sql);
    } catch (SQLException e) {
      throw new HiveException(e.getMessage());
    }
  }

  public void close() throws HiveException {
    if (conn != null) {
      try {
        conn.close();
      } catch (SQLException e) {
        log.info(e.getMessage());
      }
    }
  }

  public ExtractConfig getConfig() {
    return config;
  }

  public void setConfig(ExtractConfig config) {
    this.config = config;
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
        conn.setAutoCommit(false);
        if (config.getHdfsfilePath() == null) {
          fs = FileSystem.get(config.getConf());
        } else {
          fs = config.getHdfsfilePath().getFileSystem(config.getConf());
        }
      } catch (ClassNotFoundException e) {
        log.info(e.getMessage());
        throw new HiveException(e.getMessage());

      } catch (SQLException e) {
        log.info(e.getMessage());
        throw new HiveException(e.getMessage());
      } catch (IOException e) {
        log.info(e.getMessage());
        throw new HiveException(e.getMessage());
      }
    }
  }

  public static void main(String[] args) throws HiveException {

  }
}
