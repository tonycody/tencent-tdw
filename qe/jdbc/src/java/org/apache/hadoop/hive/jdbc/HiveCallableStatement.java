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
import java.util.Map;

public class HiveCallableStatement implements java.sql.CallableStatement {

  public HiveCallableStatement() {
  }

  public Array getArray(int i) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getArray(int i) not supported");
  }

  public Array getArray(String parameterName) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getArray(String parameterName) not supported");
  }

  public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getBigDecimal(int parameterIndex) not supported");
  }

  public BigDecimal getBigDecimal(String parameterName) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getBigDecimal(String parameterName) not supported");
  }

  public BigDecimal getBigDecimal(int parameterIndex, int scale)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getBigDecimal(int parameterIndex, int scale) not supported");
  }

  public Blob getBlob(int i) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getBlob(int i) not supported");
  }

  public Blob getBlob(String parameterName) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getBlob(String parameterName) not supported");
  }

  public boolean getBoolean(int parameterIndex) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getBoolean(int parameterIndex) not supported");
  }

  public boolean getBoolean(String parameterName) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getBoolean(String parameterName) not supported");
  }

  public byte getByte(int parameterIndex) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getByte(int parameterIndex) not supported");
  }

  public byte getByte(String parameterName) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getByte(String parameterName) not supported");
  }

  public byte[] getBytes(int parameterIndex) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getBytes(int parameterIndex) not supported");
  }

  public byte[] getBytes(String parameterName) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getBytes(String parameterName) not supported");
  }

  public Reader getCharacterStream(int arg0) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getCharacterStream(int arg0) not supported");
  }

  public Reader getCharacterStream(String arg0) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getCharacterStream(String arg0) not supported");
  }

  public Clob getClob(int i) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getClob(int i) not supported");
  }

  public Clob getClob(String parameterName) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getClob(String parameterName) not supported");
  }

  public Date getDate(int parameterIndex) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getDate(int parameterIndex) not supported");
  }

  public Date getDate(String parameterName) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getDate(String parameterName) not supported");
  }

  public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getDate(int parameterIndex, Calendar cal) not supported");
  }

  public Date getDate(String parameterName, Calendar cal) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getDate(String parameterName, Calendar cal) not supported");
  }

  public double getDouble(int parameterIndex) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getDouble(int parameterIndex) not supported");
  }

  public double getDouble(String parameterName) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getDouble(String parameterName) not supported");
  }

  public float getFloat(int parameterIndex) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getFloat(int parameterIndex) not supported");
  }

  public float getFloat(String parameterName) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getFloat(String parameterName) not supported");
  }

  public int getInt(int parameterIndex) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getInt(int parameterIndex) not supported");
  }

  public int getInt(String parameterName) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getInt(String parameterName) not supported");
  }

  public long getLong(int parameterIndex) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getLong(int parameterIndex) not supported");
  }

  public long getLong(String parameterName) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getLong(String parameterName) not supported");
  }

  public Reader getNCharacterStream(int parameterIndex) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getNCharacterStream(int parameterIndex) not supported");
  }

  public Reader getNCharacterStream(String parameterName) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getNCharacterStream(String parameterName) not supported");
  }

  public NClob getNClob(int parameterIndex) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getNClob(int parameterIndex) not supported");
  }

  public NClob getNClob(String parameterName) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getNClob(String parameterName) not supported");
  }

  public String getNString(int parameterIndex) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getNString(int parameterIndex) not supported");
  }

  public String getNString(String parameterName) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getNString(String parameterName) not supported");
  }

  public Object getObject(int parameterIndex) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getObject(int parameterIndex) not supported");
  }

  public Object getObject(String parameterName) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getObject(String parameterName) not supported");
  }

  public Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getObject(int i, Map<String, Class<?>> map) not supported");
  }

  public Object getObject(String parameterName, Map<String, Class<?>> map)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getObject(String parameterName, Map<String, Class<?>> map) not supported");
  }

  public Ref getRef(int i) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getRef(int i) not supported");
  }

  public Ref getRef(String parameterName) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getRef(String parameterName) not supported");
  }

  public RowId getRowId(int parameterIndex) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getRowId(int parameterIndex) not supported");
  }

  public RowId getRowId(String parameterName) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getRowId(String parameterName) not supported");
  }

  public SQLXML getSQLXML(int parameterIndex) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getSQLXML(int parameterIndex) not supported");
  }

  public SQLXML getSQLXML(String parameterName) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getSQLXML(String parameterName) not supported");
  }

  public short getShort(int parameterIndex) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getShort(int parameterIndex) not supported");
  }

  public short getShort(String parameterName) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getShort(String parameterName) not supported");
  }

  public String getString(int parameterIndex) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getString(int parameterIndex) not supported");
  }

  public String getString(String parameterName) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getString(String parameterName) not supported");
  }

  public Time getTime(int parameterIndex) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getTime(int parameterIndex) not supported");
  }

  public Time getTime(String parameterName) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getTime(String parameterName) not supported");
  }

  public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getTime(int parameterIndex, Calendar cal) not supported");
  }

  public Time getTime(String parameterName, Calendar cal) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getTime(String parameterName, Calendar cal) not supported");
  }

  public Timestamp getTimestamp(int parameterIndex) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getTimestamp(int parameterIndex) not supported");
  }

  public Timestamp getTimestamp(String parameterName) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getTimestamp(String parameterName) not supported");
  }

  public Timestamp getTimestamp(int parameterIndex, Calendar cal)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getTimestamp(int parameterIndex, Calendar cal) HiveCallableStatement.getTimestamp(int parameterIndex, Calendar cal) not supported");
  }

  public Timestamp getTimestamp(String parameterName, Calendar cal)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getTimestamp(String parameterName, Calendar cal) not supported");
  }

  public URL getURL(int parameterIndex) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getURL(int parameterIndex) not supported");
  }

  public URL getURL(String parameterName) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getURL(String parameterName) not supported");
  }

  public void registerOutParameter(int parameterIndex, int sqlType)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.registerOutParameter(int parameterIndex, int sqlType) not supported");
  }

  public void registerOutParameter(String parameterName, int sqlType)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.registerOutParameter(String parameterName, int sqlType) not supported");
  }

  public void registerOutParameter(int parameterIndex, int sqlType, int scale)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.registerOutParameter(int parameterIndex, int sqlType, int scale) not supported");
  }

  public void registerOutParameter(int paramIndex, int sqlType, String typeName)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.registerOutParameter(int paramIndex, int sqlType, String typeName) not supported");
  }

  public void registerOutParameter(String parameterName, int sqlType, int scale)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.registerOutParameter(String parameterName, int sqlType, int scale) not supported");
  }

  public void registerOutParameter(String parameterName, int sqlType,
      String typeName) throws SQLException {
    throw new SQLException(
        "Method  HiveCallableStatement.registerOutParameter(String parameterName, int sqlType,String typeName) not supported");
  }

  public void setAsciiStream(String parameterName, InputStream x)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setAsciiStream(String parameterName, InputStream x) not supported");
  }

  public void setAsciiStream(String parameterName, InputStream x, int length)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setAsciiStream(String parameterName, InputStream x, int length) not supported");
  }

  public void setAsciiStream(String parameterName, InputStream x, long length)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setAsciiStream(String parameterName, InputStream x, long length) not supported");
  }

  public void setBigDecimal(String parameterName, BigDecimal x)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setBigDecimal(String parameterName, BigDecimal x) not supported");
  }

  public void setBinaryStream(String parameterName, InputStream x)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setBinaryStream(String parameterName, InputStream x) not supported");
  }

  public void setBinaryStream(String parameterName, InputStream x, int length)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setBinaryStream(String parameterName, InputStream x, int length) not supported");
  }

  public void setBinaryStream(String parameterName, InputStream x, long length)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setBinaryStream(String parameterName, InputStream x, long length) not supported");
  }

  public void setBlob(String parameterName, Blob x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setBlob(String parameterName, Blob x) not supported");
  }

  public void setBlob(String parameterName, InputStream inputStream)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setBlob(String parameterName, InputStream inputStream) not supported");
  }

  public void setBlob(String parameterName, InputStream inputStream, long length)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setBlob(String parameterName, InputStream inputStream, long length) not supported");
  }

  public void setBoolean(String parameterName, boolean x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setBoolean(String parameterName, boolean x) not supported");
  }

  public void setByte(String parameterName, byte x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setByte(String parameterName, byte x) not supported");
  }

  public void setBytes(String parameterName, byte[] x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setBytes(String parameterName, byte[] x) not supported");
  }

  public void setCharacterStream(String parameterName, Reader reader)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setCharacterStream(String parameterName, Reader reader) not supported");
  }

  public void setCharacterStream(String parameterName, Reader reader, int length)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setCharacterStream(String parameterName, Reader reader, int length) not supported");
  }

  public void setCharacterStream(String parameterName, Reader reader,
      long length) throws SQLException {
    throw new SQLException(
        "Method  HiveCallableStatement.setCharacterStream(String parameterName, Reader reader,long length) not supported");
  }

  public void setClob(String parameterName, Clob x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setClob(String parameterName, Clob x) not supported");
  }

  public void setClob(String parameterName, Reader reader) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setClob(String parameterName, Reader reader) not supported");
  }

  public void setClob(String parameterName, Reader reader, long length)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setClob(String parameterName, Reader reader, long length) not supported");
  }

  public void setDate(String parameterName, Date x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setDate(String parameterName, Date x) not supported");
  }

  public void setDate(String parameterName, Date x, Calendar cal)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setDate(String parameterName, Date x, Calendar cal) not supported");
  }

  public void setDouble(String parameterName, double x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setDouble(String parameterName, double x) not supported");
  }

  public void setFloat(String parameterName, float x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setFloat(String parameterName, float x) not supported");
  }

  public void setInt(String parameterName, int x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setInt(String parameterName, int x) not supported");
  }

  public void setLong(String parameterName, long x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setLong(String parameterName, long x) not supported");
  }

  public void setNCharacterStream(String parameterName, Reader value)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setNCharacterStream(String parameterName, Reader value) not supported");
  }

  public void setNCharacterStream(String parameterName, Reader value,
      long length) throws SQLException {
    throw new SQLException(
        "Method  HiveCallableStatement.setNCharacterStream(String parameterName, Reader value,long length) not supported");
  }

  public void setNClob(String parameterName, NClob value) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setNClob(String parameterName, NClob value) not supported");
  }

  public void setNClob(String parameterName, Reader reader) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setNClob(String parameterName, Reader reader) not supported");
  }

  public void setNClob(String parameterName, Reader reader, long length)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setNClob(String parameterName, Reader reader, long length) not supported");
  }

  public void setNString(String parameterName, String value)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setNString(String parameterName, String value) not supported");
  }

  public void setNull(String parameterName, int sqlType) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setNull(String parameterName, int sqlType) not supported");
  }

  public void setNull(String parameterName, int sqlType, String typeName)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setNull(String parameterName, int sqlType, String typeName) not supported");
  }

  public void setObject(String parameterName, Object x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setObject(String parameterName, Object x) not supported");
  }

  public void setObject(String parameterName, Object x, int targetSqlType)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setObject(String parameterName, Object x, int targetSqlType) not supported");
  }

  public void setObject(String parameterName, Object x, int targetSqlType,
      int scale) throws SQLException {
    throw new SQLException(
        "Method  HiveCallableStatement.setObject(String parameterName, Object x, int targetSqlType,int scale) not supported");
  }

  public void setRowId(String parameterName, RowId x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setRowId(String parameterName, RowId x) not supported");
  }

  public void setSQLXML(String parameterName, SQLXML xmlObject)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setSQLXML(String parameterName, SQLXML xmlObject) not supported");
  }

  public void setShort(String parameterName, short x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setShort(String parameterName, short x) not supported");
  }

  public void setString(String parameterName, String x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setString(String parameterName, String x) not supported");
  }

  public void setTime(String parameterName, Time x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setTime(String parameterName, Time x) not supported");
  }

  public void setTime(String parameterName, Time x, Calendar cal)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setTime(String parameterName, Time x, Calendar cal) not supported");
  }

  public void setTimestamp(String parameterName, Timestamp x)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setTimestamp(String parameterName, Timestamp x) not supported");
  }

  public void setTimestamp(String parameterName, Timestamp x, Calendar cal)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setTimestamp(String parameterName, Timestamp x, Calendar cal) not supported");
  }

  public void setURL(String parameterName, URL val) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setURL(String parameterName, URL val) not supported");
  }

  public boolean wasNull() throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.wasNull() not supported");
  }

  public void addBatch() throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.addBatch() not supported");
  }

  public void clearParameters() throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.clearParameters() not supported");
  }

  public boolean execute() throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.execute() not supported");
  }

  public ResultSet executeQuery() throws SQLException {
    return new HiveQueryResultSet(null);
  }

  public int executeUpdate() throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.executeUpdate() not supported");
  }

  public ResultSetMetaData getMetaData() throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getMetaData() not supported");
  }

  public ParameterMetaData getParameterMetaData() throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getParameterMetaData() not supported");
  }

  public void setArray(int i, Array x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setArray(int i, Array x) not supported");
  }

  public void setAsciiStream(int arg0, InputStream arg1) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setAsciiStream(int arg0, InputStream arg1) not supported");
  }

  public void setAsciiStream(int parameterIndex, InputStream x, int length)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setAsciiStream(int parameterIndex, InputStream x, int length) not supported");
  }

  public void setAsciiStream(int arg0, InputStream arg1, long arg2)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setAsciiStream(int arg0, InputStream arg1, long arg2) not supported");
  }

  public void setBigDecimal(int parameterIndex, BigDecimal x)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setBigDecimal(int parameterIndex, BigDecimal x) not supported");
  }

  public void setBinaryStream(int parameterIndex, InputStream x)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setBinaryStream(int parameterIndex, InputStream x) not supported");
  }

  public void setBinaryStream(int parameterIndex, InputStream x, int length)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setBinaryStream(int parameterIndex, InputStream x, int length) not supported");
  }

  public void setBinaryStream(int parameterIndex, InputStream x, long length)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setBinaryStream(int parameterIndex, InputStream x, long length) not supported");
  }

  public void setBlob(int i, Blob x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setBlob(int i, Blob x) not supported");
  }

  public void setBlob(int parameterIndex, InputStream inputStream)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setBlob(int parameterIndex, InputStream inputStream) not supported");
  }

  public void setBlob(int parameterIndex, InputStream inputStream, long length)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setBlob(int parameterIndex, InputStream inputStream, long length) not supported");
  }

  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setBoolean(int parameterIndex, boolean x) not supported");
  }

  public void setByte(int parameterIndex, byte x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setByte(int parameterIndex, byte x) not supported");
  }

  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setBytes(int parameterIndex, byte[] x) not supported");
  }

  public void setCharacterStream(int parameterIndex, Reader reader)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setCharacterStream(int parameterIndex, Reader reader) not supported");
  }

  public void setCharacterStream(int parameterIndex, Reader reader, int length)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setCharacterStream(int parameterIndex, Reader reader, int length) not supported");
  }

  public void setCharacterStream(int parameterIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setCharacterStream(int parameterIndex, Reader reader, long length) not supported");
  }

  public void setClob(int i, Clob x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setClob(int i, Clob x) not supported");
  }

  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setClob(int parameterIndex, Reader reader) not supported");
  }

  public void setClob(int parameterIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setClob(int parameterIndex, Reader reader, long length) not supported");
  }

  public void setDate(int parameterIndex, Date x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setDate(int parameterIndex, Date x) not supported");
  }

  public void setDate(int parameterIndex, Date x, Calendar cal)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setDate(int parameterIndex, Date x, Calendar cal) not supported");
  }

  public void setDouble(int parameterIndex, double x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setDouble(int parameterIndex, double x) not supported");
  }

  public void setFloat(int parameterIndex, float x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setFloat(int parameterIndex, float x) not supported");
  }

  public void setInt(int parameterIndex, int x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setInt(int parameterIndex, int x) not supported");
  }

  public void setLong(int parameterIndex, long x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setLong(int parameterIndex, long x) not supported");
  }

  public void setNCharacterStream(int parameterIndex, Reader value)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setNCharacterStream(int parameterIndex, Reader value) not supported");
  }

  public void setNCharacterStream(int parameterIndex, Reader value, long length)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setNCharacterStream(int parameterIndex, Reader value, long length) not supported");
  }

  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setNClob(int parameterIndex, NClob value) not supported");
  }

  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setNClob(int parameterIndex, Reader reader) not supported");
  }

  public void setNClob(int parameterIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setNClob(int parameterIndex, Reader reader, long length) not supported");
  }

  public void setNString(int parameterIndex, String value) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setNString(int parameterIndex, String value) not supported");
  }

  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setNull(int parameterIndex, int sqlType) not supported");
  }

  public void setNull(int paramIndex, int sqlType, String typeName)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setNull(int paramIndex, int sqlType, String typeName) not supported");
  }

  public void setObject(int parameterIndex, Object x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setObject(int parameterIndex, Object x) not supported");
  }

  public void setObject(int parameterIndex, Object x, int targetSqlType)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setObject(int parameterIndex, Object x, int targetSqlType) not supported");
  }

  public void setObject(int parameterIndex, Object x, int targetSqlType,
      int scale) throws SQLException {
    throw new SQLException(
        "Method  HiveCallableStatement.setObject(int parameterIndex, Object x, int targetSqlType,int scale) not supported");
  }

  public void setRef(int i, Ref x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setRef(int i, Ref x) not supported");
  }

  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setRowId(int parameterIndex, RowId x) not supported");
  }

  public void setSQLXML(int parameterIndex, SQLXML xmlObject)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setSQLXML(int parameterIndex, SQLXML xmlObject) not supported");
  }

  public void setShort(int parameterIndex, short x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setShort(int parameterIndex, short x) not supported");
  }

  public void setString(int parameterIndex, String x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setString(int parameterIndex, String x) not supported");
  }

  public void setTime(int parameterIndex, Time x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setTime(int parameterIndex, Time x) not supported");
  }

  public void setTime(int parameterIndex, Time x, Calendar cal)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setTime(int parameterIndex, Time x, Calendar cal) not supported");
  }

  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setTimestamp(int parameterIndex, Timestamp x) not supported");
  }

  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setTimestamp(int parameterIndex, Timestamp x, Calendar cal) not supported");
  }

  public void setURL(int parameterIndex, URL x) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setURL(int parameterIndex, URL x) not supported");
  }

  public void setUnicodeStream(int parameterIndex, InputStream x, int length)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setUnicodeStream(int parameterIndex, InputStream x, int length) not supported");
  }

  public void addBatch(String sql) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.addBatch(String sql) not supported");
  }

  public void cancel() throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.cancel() not supported");
  }

  public void clearBatch() throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.clearBatch() not supported");
  }

  public void clearWarnings() throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.clearWarnings() not supported");
  }

  public void close() throws SQLException {
    throw new SQLException("Method HiveCallableStatement.close() not supported");
  }

  public boolean execute(String sql) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.execute(String sql) not supported");
  }

  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.execute(String sql, int autoGeneratedKeys) not supported");
  }

  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.execute(String sql, int[] columnIndexes) not supported");
  }

  public boolean execute(String sql, String[] columnNames) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.execute(String sql, String[] columnNames) not supported");
  }

  public int[] executeBatch() throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.executeBatch() not supported");
  }

  public ResultSet executeQuery(String sql) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.executeQuery(String sql) not supported");
  }

  public int executeUpdate(String sql) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.executeUpdate(String sql) not supported");
  }

  public int executeUpdate(String sql, int autoGeneratedKeys)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.executeUpdate(String sql, int autoGeneratedKeys) not supported");
  }

  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.executeUpdate(String sql, int[] columnIndexes) not supported");
  }

  public int executeUpdate(String sql, String[] columnNames)
      throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.executeUpdate(String sql, String[] columnNames) not supported");
  }

  public Connection getConnection() throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getConnection() not supported");
  }

  public int getFetchDirection() throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getFetchDirection() not supported");
  }

  public int getFetchSize() throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getFetchSize() not supported");
  }

  public ResultSet getGeneratedKeys() throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getGeneratedKeys() not supported");
  }

  public int getMaxFieldSize() throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getMaxFieldSize() not supported");
  }

  public int getMaxRows() throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getMaxRows() not supported");
  }

  public boolean getMoreResults() throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getMoreResults() not supported");
  }

  public boolean getMoreResults(int current) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getMoreResults(int current) not supported");
  }

  public int getQueryTimeout() throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getQueryTimeout() not supported");
  }

  public ResultSet getResultSet() throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getResultSet() not supported");
  }

  public int getResultSetConcurrency() throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getResultSetConcurrency() not supported");
  }

  public int getResultSetHoldability() throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getResultSetHoldability() not supported");
  }

  public int getResultSetType() throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getResultSetType() not supported");
  }

  public int getUpdateCount() throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getUpdateCount() not supported");
  }

  public SQLWarning getWarnings() throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.getWarnings() not supported");
  }

  public boolean isClosed() throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.isClosed() not supported");
  }

  public boolean isPoolable() throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.isPoolable() not supported");
  }

  public void setCursorName(String name) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setCursorName(String name) not supported");
  }

  public void setEscapeProcessing(boolean enable) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setEscapeProcessing(boolean enable) not supported");
  }

  public void setFetchDirection(int direction) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setFetchDirection(int direction) not supported");
  }

  public void setFetchSize(int rows) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setFetchSize(int rows) not supported");
  }

  public void setMaxFieldSize(int max) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setMaxFieldSize(int max) not supported");
  }

  public void setMaxRows(int max) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setMaxRows(int max) not supported");
  }

  public void setPoolable(boolean arg0) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setPoolable(boolean arg0) not supported");
  }

  public void setQueryTimeout(int seconds) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.setQueryTimeout(int seconds) not supported");
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.isWrapperFor(Class<?> iface) not supported");
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException(
        "Method HiveCallableStatement.unwrap(Class<T> iface) not supported");
  }

}
