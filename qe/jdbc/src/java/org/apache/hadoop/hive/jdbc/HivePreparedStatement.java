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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import org.apache.hadoop.hive.service.HiveInterface;
import org.apache.hadoop.hive.service.HiveServerException;

public class HivePreparedStatement implements PreparedStatement {
  private String sql;
  private HiveInterface client;

  private ResultSet resultSet = null;

  private final int maxRows = 0;

  private final SQLWarning warningChain = null;

  private boolean isClosed = false;

  public HivePreparedStatement(HiveInterface client, String sql) {
    this.client = client;
    this.sql = sql;
  }

  public void addBatch() throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.addBatch() not supported");
  }

  public void clearParameters() throws SQLException {
  }

  public boolean execute() throws SQLException {
    ResultSet rs = executeImmediate(sql);
    return rs != null;
  }

  public ResultSet executeQuery() throws SQLException {
    return executeImmediate(sql);
  }

  public int executeUpdate() throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.executeUpdate() not supported");
  }

  protected ResultSet executeImmediate(String sql) throws SQLException {
    if (isClosed) {
      throw new SQLException("Can't execute after statement has been closed");
    }

    try {
      resultSet = null;
      client.execute(sql);
    } catch (HiveServerException e) {
      throw new SQLException(e.getMessage(), e.getSQLState(), e.getErrorCode());
    } catch (Exception ex) {
      throw new SQLException(ex.toString(), "08S01");
    }
    resultSet = new HiveQueryResultSet(client, maxRows);
    return resultSet;
  }

  public ResultSetMetaData getMetaData() throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.getMetaData() not supported");
  }

  public ParameterMetaData getParameterMetaData() throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.getParameterMetaData() not supported");
  }

  public void setArray(int i, Array x) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setArray(int i, Array x) not supported");
  }

  public void setAsciiStream(int parameterIndex, InputStream x)
      throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setAsciiStream(int parameterIndex, InputStream x) not supported");
  }

  public void setAsciiStream(int parameterIndex, InputStream x, int length)
      throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setAsciiStream(int parameterIndex, InputStream x, int length) not supported");
  }

  public void setAsciiStream(int parameterIndex, InputStream x, long length)
      throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setAsciiStream(int parameterIndex, InputStream x, long length) not supported");
  }

  public void setBigDecimal(int parameterIndex, BigDecimal x)
      throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setBigDecimal(int parameterIndex, BigDecimal x) not supported");
  }

  public void setBinaryStream(int parameterIndex, InputStream x)
      throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setBinaryStream(int parameterIndex, InputStream x) not supported");
  }

  public void setBinaryStream(int parameterIndex, InputStream x, int length)
      throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setBinaryStream(int parameterIndex, InputStream x, int length) not supported");
  }

  public void setBinaryStream(int parameterIndex, InputStream x, long length)
      throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setBinaryStream(int parameterIndex, InputStream x, long length) not supported");
  }

  public void setBlob(int i, Blob x) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setBlob(int i, Blob x) not supported");
  }

  public void setBlob(int parameterIndex, InputStream inputStream)
      throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setBlob(int parameterIndex, InputStream inputStream) not supported");
  }

  public void setBlob(int parameterIndex, InputStream inputStream, long length)
      throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setBlob(int parameterIndex, InputStream inputStream, long length) not supported");
  }

  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setBoolean(int parameterIndex, boolean x) not supported");
  }

  public void setByte(int parameterIndex, byte x) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setByte(int parameterIndex, byte x) not supported");
  }

  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setBytes(int parameterIndex, byte[] x) not supported");
  }

  public void setCharacterStream(int parameterIndex, Reader reader)
      throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setCharacterStream(int parameterIndex, Reader reader) not supported");
  }

  public void setCharacterStream(int parameterIndex, Reader reader, int length)
      throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setCharacterStream(int parameterIndex, Reader reader, int length) not supported");
  }

  public void setCharacterStream(int parameterIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setCharacterStream(int parameterIndex, Reader reader, long length) not supported");
  }

  public void setClob(int i, Clob x) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setClob(int i, Clob x) not supported");
  }

  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setClob(int parameterIndex, Reader reader) not supported");
  }

  public void setClob(int parameterIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setClob(int parameterIndex, Reader reader, long length) not supported");
  }

  public void setDate(int parameterIndex, Date x) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setDate(int parameterIndex, Date x) not supported");
  }

  public void setDate(int parameterIndex, Date x, Calendar cal)
      throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setDate(int parameterIndex, Date x, Calendar cal) not supported");
  }

  public void setDouble(int parameterIndex, double x) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setDouble(int parameterIndex, double x) not supported");
  }

  public void setFloat(int parameterIndex, float x) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setFloat(int parameterIndex, float x) not supported");
  }

  public void setInt(int parameterIndex, int x) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setInt(int parameterIndex, int x) not supported");
  }

  public void setLong(int parameterIndex, long x) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setLong(int parameterIndex, long x) not supported");
  }

  public void setNCharacterStream(int parameterIndex, Reader value)
      throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setNCharacterStream(int parameterIndex, Reader value) not supported");
  }

  public void setNCharacterStream(int parameterIndex, Reader value, long length)
      throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setNCharacterStream(int parameterIndex, Reader value, long length) not supported");
  }

  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setNClob(int parameterIndex, NClob value) not supported");
  }

  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setNClob(int parameterIndex, Reader reader) not supported");
  }

  public void setNClob(int parameterIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setNClob(int parameterIndex, Reader reader, long length) not supported");
  }

  public void setNString(int parameterIndex, String value) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setNString(int parameterIndex, String value) not supported");
  }

  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setNull(int parameterIndex, int sqlType) not supported");
  }

  public void setNull(int paramIndex, int sqlType, String typeName)
      throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setNull(int paramIndex, int sqlType, String typeName) not supported");
  }

  public void setObject(int parameterIndex, Object x) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setObject(int parameterIndex, Object x) not supported");
  }

  public void setObject(int parameterIndex, Object x, int targetSqlType)
      throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setObject(int parameterIndex, Object x, int targetSqlType) not supported");
  }

  public void setObject(int parameterIndex, Object x, int targetSqlType,
      int scale) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setObject(int parameterIndex, Object x, int targetSqlType, int scale) not supported");
  }

  public void setRef(int i, Ref x) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setRef(int i, Ref x) not supported");
  }

  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setRowId(int parameterIndex, RowId x) not supported");
  }

  public void setSQLXML(int parameterIndex, SQLXML xmlObject)
      throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setSQLXML(int parameterIndex, SQLXML xmlObject) not supported");
  }

  public void setShort(int parameterIndex, short x) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setShort(int parameterIndex, short x) not supported");
  }

  public void setString(int parameterIndex, String x) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setString(int parameterIndex, String x) not supported");
  }

  public void setTime(int parameterIndex, Time x) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setTime(int parameterIndex, Time x) not supported");
  }

  public void setTime(int parameterIndex, Time x, Calendar cal)
      throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setTime(int parameterIndex, Time x, Calendar cal) not supported");
  }

  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setTimestamp(int parameterIndex, Timestamp x) not supported");
  }

  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
      throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setTimestamp(int parameterIndex, Timestamp x, Calendar cal) not supported");
  }

  public void setURL(int parameterIndex, URL x) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setURL(int parameterIndex, URL x) not supported");
  }

  public void setUnicodeStream(int parameterIndex, InputStream x, int length)
      throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setUnicodeStream(int parameterIndex, InputStream x, int length) not supported");
  }

  public void addBatch(String sql) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.addBatch(String sql) not supported");
  }

  public void cancel() throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.cancel() not supported");
  }

  public void clearBatch() throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.clearBatch() not supported");
  }

  public void clearWarnings() throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.clearWarnings() not supported");
  }

  public void close() throws SQLException {
    client = null;
    if (resultSet != null) {
      resultSet.close();
      resultSet = null;
    }
    isClosed = true;
  }

  public boolean execute(String sql) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.execute(String sql) not supported");
  }

  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.execute(String sql, int autoGeneratedKeys) not supported");
  }

  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.execute(String sql, int[] columnIndexes) not supported");
  }

  public boolean execute(String sql, String[] columnNames) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.execute(String sql, String[] columnNames) not supported");
  }

  public int[] executeBatch() throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.executeBatch() not supported");
  }

  public ResultSet executeQuery(String sql) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.executeQuery(String sql) not supported");
  }

  public int executeUpdate(String sql) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.executeUpdate(String sql) not supported");
  }

  public int executeUpdate(String sql, int autoGeneratedKeys)
      throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.executeUpdate(String sql, int autoGeneratedKeys) not supported");
  }

  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.executeUpdate(String sql, int[] columnIndexes) not supported");
  }

  public int executeUpdate(String sql, String[] columnNames)
      throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.executeUpdate(String sql, String[] columnNames) not supported");
  }

  public Connection getConnection() throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.getConnection() not supported");
  }

  public int getFetchDirection() throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.getFetchDirection() not supported");
  }

  public int getFetchSize() throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.getFetchSize() not supported");
  }

  public ResultSet getGeneratedKeys() throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.getGeneratedKeys() not supported");
  }

  public int getMaxFieldSize() throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.getMaxFieldSize() not supported");
  }

  public int getMaxRows() throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.getMaxRows() not supported");
  }

  public boolean getMoreResults() throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.getMoreResults() not supported");
  }

  public boolean getMoreResults(int current) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.getMoreResults(int current) not supported");
  }

  public int getQueryTimeout() throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.getQueryTimeout() not supported");
  }

  public ResultSet getResultSet() throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.getResultSet() not supported");
  }

  public int getResultSetConcurrency() throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.getResultSetConcurrency() not supported");
  }

  public int getResultSetHoldability() throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.getResultSetHoldability() not supported");
  }

  public int getResultSetType() throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.getResultSetType() not supported");
  }

  public int getUpdateCount() throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.getUpdateCount() not supported");
  }

  public SQLWarning getWarnings() throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.getWarnings() not supported");
  }

  public boolean isClosed() throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.isClosed() not supported");
  }

  public boolean isPoolable() throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.isPoolable() not supported");
  }

  public void setCursorName(String name) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setCursorName(String name) not supported");
  }

  public void setEscapeProcessing(boolean enable) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setEscapeProcessing(boolean enable) not supported");
  }

  public void setFetchDirection(int direction) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setFetchDirection(int direction) not supported");
  }

  public void setFetchSize(int rows) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setFetchSize(int rows) not supported");
  }

  public void setMaxFieldSize(int max) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setMaxFieldSize(int max) not supported");
  }

  public void setMaxRows(int max) throws SQLException {
  }

  public void setPoolable(boolean poolable) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.setPoolable(boolean poolable) not supported");
  }

  public void setQueryTimeout(int seconds) throws SQLException {
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.isWrapperFor(Class<?> iface) not supported");
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException(
        "Method HivePreparedStatement.unwrap(Class<T> iface) not supported");
  }

}
