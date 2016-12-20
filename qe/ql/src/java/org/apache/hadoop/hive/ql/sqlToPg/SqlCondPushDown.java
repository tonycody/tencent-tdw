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
package org.apache.hadoop.hive.ql.sqlToPg;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.dataImport.ExtractConfig;
import org.apache.hadoop.hive.ql.dataImport.StringUtil;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlCondPushDown {
  private HashMap<String, Table> tblMap;
  private HashMap<String, String> tblNameMap;

  private Log log = LogFactory.getLog(ExtractConfig.class);
  private String host;
  private String port;
  private String url;
  private String dbName;
  private String sql;

  private FileSystem fs;
  private boolean dataSwitch = false;
  private Connection conn;
  private ExtractConfig config = null;

  public SqlCondPushDown(HashMap<String, Table> tbl,
      HashMap<String, String> tblName) {
    this.tblMap = tbl;
    this.tblNameMap = tblName;
  }

  public SqlCondPushDown() {
    this.tblMap = new HashMap<String, Table>();
  }

  public int sendSqlToPg(String sql) {
    String pgSql = sql;

    log.info("pg sql: " + pgSql);

    try {
      conn = DriverManager.getConnection(config.getUrl(), config.getUser(),
          config.getPwd());
      try {
        String processName = java.lang.management.ManagementFactory
            .getRuntimeMXBean().getName();
        String processID = processName.substring(0, processName.indexOf('@'));
        String appinfo = "sendSqlToPg_" + processID + "_"
            + SessionState.get().getSessionName();
        conn.setClientInfo("ApplicationName", appinfo);
      } catch (Exception e) {
        e.printStackTrace();
      }
      conn.setAutoCommit(false);
    } catch (SQLException e) {
      log.info("cannot connect to PG: " + e.getMessage());
    }

    if (pgSql.isEmpty()) {
      return 1;
    }

    String explainSql = "explain " + pgSql;

    try {
      Statement s = conn.createStatement();

      log.info("execute explain sql in postgresql");

      boolean ret;
      ret = s.execute(explainSql);
      if (ret) {
        String[] array = sql.split("[\\s]");
        if (!array[0].trim().equalsIgnoreCase("select")) {
          return 1;
        } else {
          getSqlResultToHive();
        }
      } else {
        log.info("execute sql: " + explainSql + "failed");
        return 1;
      }
    } catch (Exception e) {
      log.info("execute failed in postgresql: " + e.getMessage());
      try {
        if (conn != null) {
          conn.close();
          return 1;
        }
      } catch (Exception ex) {
        return 1;
      }

      return 1;
    }

    return 0;
  }

  public String tdwSqlToPgsql(String tdwSql) {
    String pgSql = "";

    String[] array = tdwSql.split("[\\s]");

    if (array[0].equalsIgnoreCase("insert")
        && array[1].equalsIgnoreCase("into")) {
      if (array[2].contains("(")) {
        String tmpArray[] = array[2].split("\\(");
        String tblName = tmpArray[0];
        array[2] = tblNameMap.get(tblName) + "(" + tmpArray[1];
      } else {
        for (int i = 0; i < array.length; i++) {
          array[i] = replaceTblName(array[i]);
        }
      }
    } else {
      for (int i = 0; i < array.length; i++) {
        array[i] = replaceTblName(array[i]);
      }
    }

    for (int i = 0; i < array.length; i++) {
      pgSql = pgSql + array[i] + " ";
    }

    return pgSql;
  }

  public String replaceTblName(String str) {
    String resStr;
    resStr = str;

    for (Entry<String, String> entry : tblNameMap.entrySet()) {
      String tab_name = entry.getKey();
      String pgTblName = entry.getValue();

    }

    String patternString = "[^a-zA-Z_0-9]";
    try {
      Pattern pattern = Pattern
          .compile(patternString, Pattern.CASE_INSENSITIVE);

      if (str.contains(".")) {
        String[] array = str.split("\\.");
        String[] tmpArray = array;
        for (int i = 0; i < array.length; i++) {

          Matcher matcher = pattern.matcher(array[i]);
          if (matcher.find()) {
            int start = matcher.start();
            int end = matcher.end();
            String match = array[i].substring(start, end);

            if (end < array[i].length() - 1) {
              String tblName = array[i].substring(end);
              if (tblNameMap.containsKey(tblName)) {
                tmpArray[i] = array[i].substring(0, start) + match
                    + tblNameMap.get(tblName);
              } else {
                tmpArray[i] = array[i].substring(0, start) + match + tblName;
              }
            }
          } else {
            String tblName = array[i];
            if (tblNameMap.containsKey(tblName)) {
              tmpArray[i] = tblNameMap.get(tblName);
            } else {
              tmpArray[i] = tblName;
            }
          }
        }

        String tmpStr = "";
        for (int i = 0; i < tmpArray.length; i++) {
          if (i != tmpArray.length - 1) {
            tmpStr = tmpStr + tmpArray[i] + ".";
          } else {
            tmpStr = tmpStr + tmpArray[i];
          }
        }
        resStr = tmpStr;
      } else if (str.contains("::")) {
        String[] array = str.split("::");
        String match = "";
        String newTbl = "";

        Matcher matcher = pattern.matcher(array[1]);
        if (matcher.find()) {
          int start = matcher.start();
          match = array[1].substring(0, start);

          newTbl = tblNameMap.get(match);
          resStr = newTbl + array[1].substring(start);
        } else {
          resStr = tblNameMap.get(array[1]);
        }
      } else {
        boolean found = true;
        int matchNum = 0;
        String leftStr = str;
        ArrayList<Integer> startList = new ArrayList<Integer>();
        ArrayList<Integer> endList = new ArrayList<Integer>();
        int start = 0;
        int end = 0;
        while (found) {
          Matcher matcher = pattern.matcher(leftStr);
          found = matcher.find();
          if (found) {
            start = matcher.start() + end;
            end = matcher.end() + end;
            startList.add(start);
            endList.add(end);
            leftStr = str.substring(end);
            matchNum++;
          }
        }

        if (matchNum > 0) {
          String tempStr = "";
          String matchStr = "";
          String aStr = "";
          if (startList.get(0) == 0) {
            for (int i = 1; i < startList.size(); i++) {
              tempStr = str.substring(endList.get(i - 1), startList.get(i));
              matchStr = str
                  .substring(startList.get(i - 1), endList.get(i - 1));

              if (tblNameMap.containsKey(tempStr)) {
                aStr = aStr + matchStr + tblNameMap.get(tempStr);
              } else {
                aStr = aStr + matchStr + tempStr;
              }
            }

            if (endList.get(endList.size() - 1) < str.length()) {
              tempStr = str.substring(endList.get(endList.size() - 1));
              matchStr = str.substring(startList.get(startList.size() - 1),
                  endList.get(endList.size() - 1));

              if (tblNameMap.containsKey(tempStr)) {
                aStr = aStr + matchStr + tblNameMap.get(tempStr);
              } else {
                aStr = aStr + matchStr + tempStr;
              }
            } else {
              matchStr = str.substring(startList.get(startList.size() - 1),
                  endList.get(endList.size() - 1));
              aStr = aStr + matchStr;
            }
          } else {
            tempStr = str.substring(0, startList.get(0));
            matchStr = str.substring(startList.get(0), endList.get(0));

            if (tblNameMap.containsKey(tempStr)) {
              aStr = aStr + tblNameMap.get(tempStr) + matchStr;
            } else {
              aStr = aStr + tempStr + matchStr;
            }

            for (int i = 1; i < startList.size(); i++) {
              tempStr = str.substring(endList.get(i - 1), startList.get(i));
              matchStr = str.substring(startList.get(i), endList.get(i));

              if (tblNameMap.containsKey(tempStr)) {
                aStr = aStr + tblNameMap.get(tempStr) + matchStr;
              } else {
                aStr = aStr + tempStr + matchStr;
              }
            }

            if (endList.get(endList.size() - 1) < str.length()) {
              tempStr = str.substring(endList.get(endList.size() - 1));

              if (tblNameMap.containsKey(tempStr)) {
                aStr = aStr + tblNameMap.get(tempStr);
              } else {
                aStr = aStr + tempStr;
              }
            }
          }

          resStr = aStr;
        } else {
          if (tblNameMap.containsKey(str)) {
            resStr = tblNameMap.get(str);
          } else {
            resStr = str;
          }
        }
      }
    } catch (Exception e) {
      log.info("execute failed in postgresql: " + e.getMessage());
      return resStr;
    }

    return resStr;
  }

  public String getUrl() {
    url = "jdbc:postgresql://" + host + ":" + port + "/" + dbName;
    return url;
  }

  public void getSqlResultToHive() throws SQLException {
    try {
      dataSwitch = config.getConf().getBoolean("data.line.seperator.filter",
          false);
      ResultSet rs = this.executeQuery(config.getSql());
      ResultSetMetaData meta = rs.getMetaData();
      int num = meta.getColumnCount();

      fs = FileSystem.get(config.getConf());
      FSDataOutputStream fsDOS = fs.create(new Path(config.getFilePath()),
          true, 1024 * 1024 * 16);
      log.info("config.getFilePath(): " + config.getFilePath());
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
      log.error("xxx" + e.getMessage());
      throw new SQLException(e.getMessage());
    } catch (HiveException e) {
      log.error("xxx" + e.getMessage());
      throw new SQLException(e.getMessage());
    } catch (SQLException e) {
      log.error("xxx" + e.getMessage());
      throw new SQLException(e.getMessage());
    } finally {
      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException e) {
          log.info("xxx" + e.getMessage());
          throw new SQLException(e.getMessage());
        }
      }
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

  public void setConfig(ExtractConfig config) {
    this.config = config;
  }

}
