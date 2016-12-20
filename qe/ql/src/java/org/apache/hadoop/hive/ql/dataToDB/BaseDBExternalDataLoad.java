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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.dataImport.BaseDataExtract;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ErrorMsg;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.io.Text;

import StorageEngineClient.MyLineReader;

public class BaseDBExternalDataLoad {
  private Log LOG = LogFactory.getLog(BaseDataExtract.class);
  private LoadConfig config = null;
  private Connection conn;
  private Statement statement;
  private FileSystem fs;
  private Map<String, String> fields;
  private boolean newinsert;
  private boolean tolerent;
  private int tolerentnum;
  private Text textstr = new Text();
  private int bytescosumed = 0;
  private String tmpline = null;
  private int errcnt = 0;

  private List<FieldSchema> cols;

  public BaseDBExternalDataLoad(LoadConfig config) {
    this.fields = new HashMap<String, String>();
    this.config = config;
    try {
      initConn();
      initFields();
    } catch (Exception e) {

    }
    HiveConf sconf = SessionState.get().getConf();
    newinsert = HiveConf.getBoolVar(sconf, HiveConf.ConfVars.BIINSERTNEW);
    tolerent = HiveConf.getBoolVar(sconf, HiveConf.ConfVars.BIINSERTTOLERENT);
    tolerentnum = HiveConf.getIntVar(sconf,
        HiveConf.ConfVars.BIINSERTTOLERENTNUM);

    SessionState.get().ssLog("Load use new insert:" + newinsert);
    SessionState.get().ssLog("Load use tolerent:" + tolerent);
    if (tolerent)
      SessionState.get().ssLog("Load tolerent num:" + tolerentnum);
  }

  private void initConn(List<FieldSchema> cols) throws HiveException {
    if (config != null) {
      try {
        boolean multihdfsenable = HiveConf.getBoolVar(config.getConf(),
            HiveConf.ConfVars.MULTIHDFSENABLE);
        if (!multihdfsenable)
          fs = FileSystem.get(config.getConf());
        else
          fs = new Path(config.getFilePath()).getFileSystem(config.getConf());
        this.cols = cols;
      } catch (IOException e) {
        LOG.info(e.getMessage());
        throw new HiveException(e.getMessage());
      }
    }
  }

  public BaseDBExternalDataLoad(LoadConfig config, List<FieldSchema> cols,
      Statement stat) {
    this.fields = new HashMap<String, String>();
    this.config = config;
    this.statement = stat;
    try {
      initConn(cols);
    } catch (Exception e) {

    }

    HiveConf sconf = SessionState.get().getConf();
    newinsert = HiveConf.getBoolVar(sconf, HiveConf.ConfVars.BIINSERTNEW);
    tolerent = HiveConf.getBoolVar(sconf, HiveConf.ConfVars.BIINSERTTOLERENT);
    tolerentnum = HiveConf.getIntVar(sconf,
        HiveConf.ConfVars.BIINSERTTOLERENTNUM);

    SessionState.get().ssLog("Load use new insert:" + newinsert);
    SessionState.get().ssLog("Load use tolerent:" + tolerent);
    if (tolerent)
      SessionState.get().ssLog("Load tolerent num:" + tolerentnum);
  }

  public void loadDataToDBTable(String delimiter) throws HiveException {
    if (config.isStructure()) {
      loadStructuredDataToDBTable();
    } else if (config.isText()) {
      Statement stat = this.statement;
      try {
        if (newinsert && tolerent) {
          stat.getConnection().setAutoCommit(true);
        }
        loadTextDataToDBTable(stat, delimiter);
        if (newinsert && tolerent) {
          stat.getConnection().setAutoCommit(false);
        }
      } catch (HiveException e) {
        throw new HiveException(e.getMessage());
      } catch (SQLException e) {
        throw new HiveException(e.getMessage());
      }
    } else {
      throw new HiveException(ErrorMsg.INVALID_FILEFORMAT.getMsg());
    }
  }

  private void loadTextDataToDBTable(Statement stat, String delimiter)
      throws HiveException {
    try {
      String file = config.getFilePath();
      Path srcf = new Path(file);
      FileStatus[] srcs;
      srcs = fs.globStatus(srcf);

      if (srcs == null) {
        LOG.info("No sources specified to insert: " + srcf);
        return;
      }
      checkPaths(srcs);

      for (int i = 0; i < srcs.length; i++) {
        insertDir(srcs[i].getPath(), stat, delimiter);
      }
    } catch (IOException e) {
      throw new HiveException("insert Files: error while insert files!!!", e);
    }
  }

  private void insertSingleFile(Path file_path, Statement stat, String delimiter)
      throws HiveException {
    try {
      FSDataInputStream fdis = fs.open(file_path);
      Object reader;
      if (newinsert) {
        reader = new MyLineReader(fdis);
      } else {
        reader = new BufferedReader(new InputStreamReader(fdis));
      }
      String line = "";
      String deli = delimiter;
      if (deli == null || deli.isEmpty()) {
        deli = new String(new char[] { '\01' });
      }
      List<FieldSchema> fss = this.cols;

      int recordcnt = 1;
      String basesql = "insert into " + config.getDbTable() + " values ";
      int insertsize = HiveConf.getIntVar(config.getConf(),
          HiveConf.ConfVars.HIVEBIROWNUMPERINSERT);
      if (insertsize <= 0 || insertsize >= 100000) {
        insertsize = 10000;
      }
      ArrayList<String> valuelist = new ArrayList<String>(insertsize);
      StringBuffer lineBuffer = new StringBuffer();
      while (readLine(reader, lineBuffer)) {
        line = lineBuffer.toString();
        lineBuffer.setLength(0);
        ArrayList<Integer> arrays = getIndexes(line, deli);
        String values = "(";
        int m = arrays.get(0);
        int n = arrays.get(1);
        int count = 0;
        for (int j = 1; j < arrays.size(); j++) {
          String c = "";
          n = arrays.get(j);
          if (n == (m + 1)) {
            c = toSQLInsertStr(fss.get(count).getType(), "");
          } else {
            c = toSQLInsertStr(fss.get(count).getType(),
                line.substring(m + 1, n));
          }

          m = n;
          if (count == 0)
            values += c;
          else
            values += "," + c;
          count++;
        }
        values += ")";
        valuelist.add(values);
        if (recordcnt % insertsize == 0) {
          insertValues(stat, basesql, valuelist);
          valuelist.clear();
        }
        if (recordcnt % 10000 == 0 && SessionState.get() != null)
          SessionState.get().ssLog("Load reocord to postgre:" + recordcnt);
        recordcnt++;
      }

      if (!valuelist.isEmpty()) {
        insertValues(stat, basesql, valuelist);
        valuelist.clear();
      }

      closestream(reader);

    } catch (IOException e) {
      LOG.debug(e.getMessage());
      throw new HiveException(e.getMessage());

    } catch (SQLException e) {
      LOG.debug(e.getMessage());
      throw new HiveException(e.getMessage());
    }
  }

  private boolean readLine(Object reader, StringBuffer lineBuffer)
      throws IOException {
    if (newinsert) {
      if (((MyLineReader) reader).readLine(textstr) > 0) {
        lineBuffer.append(textstr.toString());
        return true;
      } else {
        return false;
      }
    } else {
      tmpline = ((BufferedReader) reader).readLine();
      if (tmpline != null) {
        lineBuffer.append(tmpline);
        return true;
      } else {
        return false;
      }
    }
  }

  private void closestream(Object reader) throws IOException {
    if (newinsert) {
      ((MyLineReader) reader).close();
    } else {
      ((BufferedReader) reader).close();
    }

  }

  private void insertDir(Path srcf, Statement stat, String delimiter)
      throws IOException, HiveException {
    FileStatus[] items = fs.listStatus(srcf);
    String deli = delimiter;
    for (int i = 0; i < items.length; i++) {
      if (items[i].isDir()) {
        insertDir(items[i].getPath(), stat, deli);
      } else {
        insertSingleFile(items[i].getPath(), stat, deli);
      }
    }
  }

  public void loadDataToDBTable() throws HiveException {
    if (config.getoText() != null) {
      try {
        String sql = config.getoText();
        for (String col : fields.keySet()) {
          if (sql.indexOf(col) != -1)
            sql = sql.replaceFirst(col, fields.get(col));
        }
        Statement stat = conn.createStatement();
        stat.executeUpdate(sql);
        conn.commit();
        stat.close();
      } catch (SQLException e) {
        throw new HiveException(e.getMessage());
      } finally {
        try {
          if (conn != null)
            conn.close();
        } catch (SQLException e) {

        }
      }
      return;
    }
    if (config.isStructure()) {
      loadStructuredDataToDBTable();
    } else if (config.isText()) {
      Statement stat;
      try {
        stat = conn.createStatement();
        if (newinsert && tolerent) {
          conn.setAutoCommit(true);
        }
        loadTextDataToDBTable(stat);
        if (newinsert && tolerent) {
          conn.setAutoCommit(false);
        }
        conn.commit();
        stat.close();
      } catch (SQLException e) {
        throw new HiveException(e.getMessage());
      } finally {
        try {
          if (conn != null)
            conn.close();
        } catch (SQLException e) {

        }
      }
    } else {
      throw new HiveException(ErrorMsg.INVALID_FILEFORMAT.getMsg());
    }
  }

  private void loadStructuredDataToDBTable() {

  }

  private void loadTextDataToDBTable(Statement stat) throws HiveException {
    try {
      String file = config.getFilePath();
      Path srcf = new Path(file);
      FileStatus[] srcs;
      srcs = fs.globStatus(srcf);

      if (srcs == null) {
        LOG.info("No sources specified to insert: " + srcf);
        return;
      }
      checkPaths(srcs);

      for (int i = 0; i < srcs.length; i++) {
        insertDir(srcs[i].getPath(), stat);
      }
    } catch (IOException e) {
      throw new HiveException("insert Files: error while insert files!!!", e);
    }
  }

  private void insertDir(Path srcf, Statement stat) throws IOException,
      HiveException {
    FileStatus[] items = fs.listStatus(srcf);
    for (int i = 0; i < items.length; i++) {
      if (items[i].isDir()) {
        insertDir(items[i].getPath(), stat);
      } else {
        insertSingleFile(items[i].getPath(), stat);
      }
    }
  }

  private void insertSingleFile(Path file_path, Statement stat)
      throws HiveException {
    try {
      FSDataInputStream fdis = fs.open(file_path);
      Object reader;
      if (newinsert) {
        reader = new MyLineReader(fdis);
      } else {
        reader = new BufferedReader(new InputStreamReader(fdis));
      }
      String line = "";
      String deli = config.getTable().getSerdeParam(Constants.FIELD_DELIM);
      if (deli == null || deli.isEmpty()) {
        deli = new String(new char[] { '\01' });
      }
      List<FieldSchema> fss = config.getTable().getCols();

      int recordcnt = 1;
      String basesql = "insert into " + config.getDbTable() + " values ";
      int insertsize = HiveConf.getIntVar(config.getConf(),
          HiveConf.ConfVars.HIVEBIROWNUMPERINSERT);
      if (insertsize <= 0 || insertsize >= 100000) {
        insertsize = 10000;
      }
      ArrayList<String> valuelist = new ArrayList<String>(insertsize);
      StringBuffer lineBuffer = new StringBuffer();
      while (readLine(reader, lineBuffer)) {
        line = lineBuffer.toString();
        lineBuffer.setLength(0);
        ArrayList<Integer> arrays = getIndexes(line, deli);
        String values = "(";
        int m = arrays.get(0);
        int n = arrays.get(1);
        int count = 0;
        for (int j = 1; j < arrays.size(); j++) {
          String c = "";
          n = arrays.get(j);
          if (n == (m + 1)) {
            c = toSQLInsertStr(fss.get(count).getType(), "");
          } else {
            c = toSQLInsertStr(fss.get(count).getType(),
                line.substring(m + 1, n));
          }

          m = n;
          if (count == 0)
            values += c;
          else
            values += "," + c;
          count++;
        }
        values += ")";
        valuelist.add(values);
        if (recordcnt % insertsize == 0) {
          insertValues(stat, basesql, valuelist);
          valuelist.clear();
        }
        if (recordcnt % 10000 == 0 && SessionState.get() != null)
          SessionState.get().ssLog("Load reocord to postgre:" + recordcnt);
        recordcnt++;
      }

      if (!valuelist.isEmpty()) {
        insertValues(stat, basesql, valuelist);
        valuelist.clear();
      }

      closestream(reader);

    } catch (IOException e) {
      LOG.debug(e.getMessage());
      throw new HiveException(e.getMessage());

    } catch (SQLException e) {
      LOG.debug(e.getMessage());
      throw new HiveException(e.getMessage());
    }
  }

  private void insertValues(Statement stat, String basesql,
      ArrayList<String> valuelist) throws SQLException {
    if (newinsert && tolerent) {
      try {
        stat.executeUpdate(basesql + StringUtils.join(valuelist, ','));
      } catch (SQLException e1) {
        for (String onevalue : valuelist) {
          try {
            stat.executeUpdate(basesql + onevalue);
          } catch (SQLException e2) {
            SessionState.get().ssLog(
                "Load failed:" + onevalue + " " + e2.getMessage());
            errcnt++;
            if (errcnt > tolerentnum) {
              throw e2;
            }
          }
        }
      }
    } else {
      stat.executeUpdate(basesql + StringUtils.join(valuelist, ','));
    }
  }

  private void checkPaths(FileStatus[] srcs) throws HiveException {
    try {
      for (int i = 0; i < srcs.length; i++) {
        FileStatus[] items = fs.listStatus(srcs[i].getPath());
        for (int j = 0; j < items.length; j++) {

          if (Utilities.isTempPath(items[j])) {
            fs.delete(items[j].getPath(), true);
            continue;
          }
        }
      }
    } catch (IOException e) {
      throw new HiveException("checkPaths: filesystem error in check phase", e);
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
        if (!config.isOutoCommit()) {
          conn.setAutoCommit(false);
        }
        boolean multihdfsenable = HiveConf.getBoolVar(config.getConf(),
            HiveConf.ConfVars.MULTIHDFSENABLE);
        if (!multihdfsenable)
          fs = FileSystem.get(config.getConf());
        else
          fs = new Path(config.getFilePath()).getFileSystem(config.getConf());
      } catch (ClassNotFoundException e) {
        LOG.info(e.getMessage());
        throw new HiveException(e.getMessage());
      } catch (SQLException e) {
        LOG.info(e.getMessage());
        throw new HiveException(e.getMessage());
      } catch (IOException e) {
        LOG.info(e.getMessage());
        throw new HiveException(e.getMessage());
      }
    }
  }

  public String replaceZeroOne(String value) {
    String str = "";
    if (value != null) {
      char a = ' ';
      for (int i = 0; i < value.length(); i++) {
        a = value.charAt(i);
        if (a == '\00') {
          a = '?';
        }
        str = str + a;
      }
    }
    return str;
  }

  public String toSQLInsertStr(String type, String value) {
    if (value.equals("\\N")) {
      return "null";
    }
    if (type.toUpperCase().equals("STRING")) {
      return "'" + StringEscapeUtils.escapeSql(replaceZeroOne(value)) + "'";
    } else if (value.trim().equals("")) {
      return "null";
    } else
      return value + "";
  }

  public void initFields() {
    ResultSetMetaData rsm;
    try {
      rsm = getMetaData();

      Table t = config.getTable();
      int total = rsm.getColumnCount();
      List<FieldSchema> fss = t.getCols();
      for (int i = 1; i <= total; i++) {
        fields.put(fss.get(i - 1).getName(), rsm.getColumnName(i));
      }
    } catch (SemanticException e) {

    } catch (SQLException e) {

    }
  }

  private ArrayList<Integer> getIndexes(String line, String c) {
    ArrayList<Integer> arrays = new ArrayList<Integer>();
    arrays.add(-1);
    int i = line.indexOf(c);
    while (i != -1) {
      arrays.add(i);
      i = line.indexOf(c, i + 1);
    }
    arrays.add(line.length());
    return arrays;
  }

  private ResultSetMetaData getMetaData() throws SemanticException {
    Connection conn = null;
    try {
      String sql = "select * from " + config.getDbTable();
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

  public static void main(String[] args) {
    String c = new String(new char[] { '\01' });
    StringBuilder sb = new StringBuilder();
    sb.append(c + "tt" + c + c + c + "aa" + c);
    String test = sb.toString();
    ArrayList<Integer> arrays = new ArrayList<Integer>();
    arrays.add(-1);
    int i = test.indexOf(c);
    while (i != -1) {
      arrays.add(i);
      System.out.println(i);
      i = test.indexOf(c, i + 1);

    }
    arrays.add(test.length());
    int m = arrays.get(0);
    int n = arrays.get(1);
    int count = 0;
    String sql = "(";
    for (int j = 1; j < arrays.size(); j++) {

      n = arrays.get(j);
      if (n == (m + 1))
        System.out.println("null");
      else
        System.out.println(test.substring(m + 1, n));
      m = n;
      if (count == 0)
        sql += c;
      else
        sql += "," + c;
      count++;
      count++;
    }
    System.out.println(count);
    System.out.println(sql);
    System.out.println("END");
  }
}
