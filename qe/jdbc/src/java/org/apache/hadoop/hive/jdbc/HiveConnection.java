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

package org.apache.hadoop.hive.jdbc;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.service.HiveClient;
import org.apache.hadoop.hive.service.HiveInterface;
import org.apache.hadoop.hive.service.HiveServer;
import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.Socket;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class HiveConnection implements java.sql.Connection, TDWConnection {
  private TTransport transport;
  public HiveInterface client;
  public boolean isClosed = true;
  private SQLWarning warningChain = null;

  private Socket socket;

  private String currentDB = null;
  private String currentUser = null;
  private String source = null;

  private static final String URI_PREFIX = "jdbc:hive://";

  public HiveConnection(String uri, Properties info) throws SQLException {
    if (!uri.startsWith(URI_PREFIX)) {
      throw new SQLException("Invalid URL: " + uri, "08S01");
    }

    if (info != null) {
      source = info.getProperty("source");
    }

    String db = "default_db";
    uri = uri.substring(URI_PREFIX.length());

    if (uri.isEmpty()) {
      try {
        client = new HiveServer.HiveServerHandler();

        try {
          if (client.audit(info.getProperty("user"),
              info.getProperty("password"), db) != 0) {
            throw new SQLException("user :" + info.getProperty("user")
                + ",pass : " + info.getProperty("password") + ",db : " + db
                + " auldit failed!");
          }
        } catch (HiveServerException e) {
          e.printStackTrace();
          throw new SQLException(e.getMessage());
        } catch (TException e) {
          e.printStackTrace();
          throw new SQLException(e.getMessage());
        }

      } catch (MetaException e) {
        throw new SQLException("Error accessing Hive metastore: "
            + e.getMessage(), "08S01");
      }
    } else {
      String[] parts = uri.split("/");
      db = parts[1];
      String[] hostport = parts[0].split(":");
      int port = 10000;
      String host = hostport[0];
      try {
        port = Integer.parseInt(hostport[1]);
      } catch (Exception e) {
      }

      try {
        socket = new Socket(host, port);
        socket.setKeepAlive(true);
        transport = new TSocket(socket);
      } catch (Exception x) {
        x.printStackTrace();
        throw new SQLException("Could not establish connecton to " + uri + ": "
            + x.getMessage(), "08S01");
      }

      TProtocol protocol = new TBinaryProtocol(transport);
      client = new HiveClient(protocol);

      try {
        if (client.audit(info.getProperty("user"),
            info.getProperty("password"), db) != 0) {
          throw new SQLException("user :" + info.getProperty("user")
              + ",pass : " + info.getProperty("password") + ",db : " + db
              + " auldit failed!");
        }

        Random rd = new Random();
        int randID = rd.nextInt(100000000);
        String sessionName = info.getProperty("user") + "_"
            + System.getProperty("user.name") + "_"
            + new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
            + "_" + randID;

        if (source != null) {
          sessionName += source;
        }
        client.createSession(sessionName);

      } catch (HiveServerException e) {
        e.printStackTrace();
        throw new SQLException(e.getMessage());
      } catch (TException e) {
        e.printStackTrace();
        throw new SQLException(e.getMessage());
      }

    }
    currentDB = db;
    currentUser = info.getProperty("user");
    isClosed = false;
    configureConnection();

  }

  private void configureConnection() throws SQLException {
    Statement stmt = createStatement();
    stmt.execute("set hive.fetch.output.serde = org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
    stmt.execute("set tdw.username=" + currentUser);
    stmt.close();
  }

  public void clearWarnings() throws SQLException {
    warningChain = null;
  }

  public void close() throws SQLException {
    try {
      client.dropSession(null, null);
    } catch (Exception x) {

    }

    try {
      if (transport != null) {
        transport.close();
      }
    } finally {
      isClosed = true;
    }
  }

  public void commit() throws SQLException {
    throw new SQLException("Method HiveConnection.commit() not supported");
  }

  public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
    throw new SQLException(
        "Method HiveConnection.createArrayOf(String arg0, Object[] arg1) not supported");
  }

  public Blob createBlob() throws SQLException {
    throw new SQLException("Method HiveConnection.createBlob() not supported");
  }

  public Clob createClob() throws SQLException {
    throw new SQLException("Method HiveConnection.createClob() not supported");
  }

  public NClob createNClob() throws SQLException {
    throw new SQLException("Method HiveConnection.createNClob() not supported");
  }

  public SQLXML createSQLXML() throws SQLException {
    throw new SQLException("Method HiveConnection.createSQLXML() not supported");
  }

  public Statement createStatement() throws SQLException {
    if (isClosed) {
      throw new SQLException("Can't create Statement, connection is closed");
    }
    HiveStatement stmt = new HiveStatement(client);
    stmt.setSocket(socket);
    return stmt;
  }

  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    throw new SQLException(
        "Method HiveConnection.createStatement(int resultSetType, int resultSetConcurrency) not supported");
  }

  public Statement createStatement(int resultSetType, int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    throw new SQLException(
        "Method  HiveConnection.createStatement(int resultSetType, int resultSetConcurrency,int resultSetHoldability) not supported");
  }

  public Struct createStruct(String typeName, Object[] attributes)
      throws SQLException {
    throw new SQLException(
        "Method HiveConnection.createStruct(String typeName, Object[] attributes) not supported");
  }

  public boolean getAutoCommit() throws SQLException {
    return true;
  }

  public String getCatalog() throws SQLException {
    return currentDB;
  }

  public Properties getClientInfo() throws SQLException {
    throw new SQLException(
        "Method HiveConnection.getClientInfo() not supported");
  }

  public String getClientInfo(String name) throws SQLException {
    throw new SQLException(
        "Method HiveConnection.getClientInfo(String name) not supported");
  }

  public int getHoldability() throws SQLException {
    throw new SQLException(
        "Method HiveConnection.getHoldability() not supported");
  }

  public DatabaseMetaData getMetaData() throws SQLException {
    return new HiveDatabaseMetaData(client, currentDB);
  }

  public int getTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw new SQLException("Method HiveConnection.getTypeMap() not supported");
  }

  public SQLWarning getWarnings() throws SQLException {
    return warningChain;
  }

  public boolean isClosed() throws SQLException {
    return isClosed;
  }

  public boolean isReadOnly() throws SQLException {
    return false;
  }

  public boolean isValid(int timeout) throws SQLException {
    throw new SQLException(
        "Method HiveConnection.isValid(int timeout) not supported");
  }

  public String nativeSQL(String sql) throws SQLException {
    throw new SQLException(
        "Method HiveConnection.nativeSQL(String sql) not supported");
  }

  public CallableStatement prepareCall(String sql) throws SQLException {
    throw new SQLException(
        "Method HiveConnection.prepareCall(String sql) not supported");
  }

  public CallableStatement prepareCall(String sql, int resultSetType,
      int resultSetConcurrency) throws SQLException {
    throw new SQLException(
        "Method  HiveConnection.prepareCall(String sql, int resultSetType,int resultSetConcurrency) not supported");
  }

  public CallableStatement prepareCall(String sql, int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new SQLException(
        "Method  HiveConnection.prepareCall(String sql, int resultSetType,int resultSetConcurrency, int resultSetHoldability) not supported");
  }

  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return new HivePreparedStatement(client, sql);
  }

  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
      throws SQLException {
    return new HivePreparedStatement(client, sql);
  }

  public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
      throws SQLException {
    throw new SQLException(
        "Method HiveConnection.prepareStatement(String sql, int[] columnIndexes) not supported");
  }

  public PreparedStatement prepareStatement(String sql, String[] columnNames)
      throws SQLException {
    throw new SQLException(
        "Method HiveConnection.prepareStatement(String sql, String[] columnNames) not supported");
  }

  public PreparedStatement prepareStatement(String sql, int resultSetType,
      int resultSetConcurrency) throws SQLException {
    return new HivePreparedStatement(client, sql);
  }

  public PreparedStatement prepareStatement(String sql, int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new SQLException(
        "Method  HiveConnection.prepareStatement(String sql, int resultSetType,int resultSetConcurrency, int resultSetHoldability) not supported");
  }

  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw new SQLException(
        "Method HiveConnection.releaseSavepoint(Savepoint savepoint) not supported");
  }

  public void rollback() throws SQLException {
    throw new SQLException("Method HiveConnection.rollback() not supported");
  }

  public void rollback(Savepoint savepoint) throws SQLException {
    throw new SQLException(
        "Method HiveConnection.rollback(Savepoint savepoint) not supported");
  }

  public void setAutoCommit(boolean autoCommit) throws SQLException {
    throw new SQLException(
        "Method HiveConnection.setAutoCommit(boolean autoCommit) not supported");
  }

  public void setCatalog(String catalog) throws SQLException {
    throw new SQLException(
        "Method HiveConnection.setCatalog(String catalog) not supported");
  }

  public void setClientInfo(Properties properties)
      throws SQLClientInfoException {
    throw new SQLClientInfoException(
        "Method  HiveConnection.setClientInfo(Properties properties) not supported",
        null);
  }

  public void setClientInfo(String name, String value)
      throws SQLClientInfoException {
    throw new SQLClientInfoException(
        "Method HiveConnection.setClientInfo(String name, String value) not supported",
        null);
  }

  public void setHoldability(int holdability) throws SQLException {
    throw new SQLException(
        "Method HiveConnection.setHoldability(int holdability) not supported");
  }

  public void setReadOnly(boolean readOnly) throws SQLException {
    throw new SQLException(
        "Method HiveConnection.setReadOnly(boolean readOnly) not supported");
  }

  public Savepoint setSavepoint() throws SQLException {
    throw new SQLException("Method HiveConnection.setSavepoint() not supported");
  }

  public Savepoint setSavepoint(String name) throws SQLException {
    throw new SQLException(
        "Method HiveConnection.setSavepoint(String name) not supported");
  }

  public void setTransactionIsolation(int level) throws SQLException {
    throw new SQLException(
        "Method HiveConnection.setTransactionIsolation(int level) not supported");
  }

  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    throw new SQLException(
        "Method HiveConnection.setTypeMap(Map<String, Class<?>> map) not supported");
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLException(
        "Method HiveConnection.isWrapperFor(Class<?> iface) not supported");
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException(
        "Method HiveConnection.unwrap(Class<T> iface) not supported");
  }

  public boolean compile(String path) throws SQLException, IOException {
    if (isClosed) {
      throw new SQLException("Can't execute after statement has been closed");
    }

    BufferedReader in = new BufferedReader(new FileReader(path));

    boolean curSQLOpen = false;
    int linenr = 0;
    String curSQL = "";
    String curLine = null;
    int first = 0;
    int end = 0;
    String rt = "";
    while (true) {
      curLine = in.readLine();
      if (curLine != null)
        linenr += 1;
      else
        return true;

      while (curLine.contains("'''")) {
        if (curSQLOpen) {
          end = curLine.indexOf("'''");
          curSQL += curLine.substring(0, end);

          if (curSQL.trim().length() == 0) {
            curSQL = "";
            curSQLOpen = false;
            curLine = curLine.substring(end + 3);
          } else {
            try {
              rt = client.compile(curSQL);
            } catch (HiveServerException e) {
              throw new SQLException(e.getMessage());
            } catch (TException e) {
              throw new SQLException(e.getMessage());
            }

            System.out.println("Check SQL @L :" + linenr + " " + curSQL + " "
                + rt);
            if (rt != null && !rt.equalsIgnoreCase("success"))
              return false;

            curSQL = "";
            curSQLOpen = false;
            curLine = curLine.substring(end + 3);
          }
        } else {
          first = curLine.indexOf("'''");
          curLine = curLine.substring(first + 3);
          curSQLOpen = true;
        }
      }
      if (curSQLOpen)
        curSQL += curLine + " ";

    }

  }

}
