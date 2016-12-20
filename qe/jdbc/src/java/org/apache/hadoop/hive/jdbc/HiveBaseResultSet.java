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
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

public abstract class HiveBaseResultSet implements ResultSet {
  protected SQLWarning warningChain = null;
  protected boolean wasNull = false;
  protected List<Object> row;
  protected List<String> columnNames;
  protected List<String> columnTypes;

  public boolean absolute(int row) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.absolute(int row) not supported");
  }

  public void afterLast() throws SQLException {
    throw new SQLException("Method HiveBaseResultSet.afterLast() not supported");
  }

  public void beforeFirst() throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.beforeFirst() not supported");
  }

  public void cancelRowUpdates() throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.cancelRowUpdates() not supported");
  }

  public void deleteRow() throws SQLException {
    throw new SQLException("Method HiveBaseResultSet.deleteRow() not supported");
  }

  public int findColumn(String columnName) throws SQLException {
    int columnIndex = columnNames.indexOf(columnName);
    if (columnIndex == -1) {
      throw new SQLException();
    } else {
      return ++columnIndex;
    }
  }

  public boolean first() throws SQLException {
    throw new SQLException("Method HiveBaseResultSet.first() not supported");
  }

  public Array getArray(int i) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getArray(int i) not supported");
  }

  public Array getArray(String colName) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getArray(String colName) not supported");
  }

  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getAsciiStream(int columnIndex) not supported");
  }

  public InputStream getAsciiStream(String columnName) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getAsciiStream(String columnName) not supported");
  }

  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getBigDecimal(int columnIndex) not supported");
  }

  public BigDecimal getBigDecimal(String columnName) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getBigDecimal(String columnName) not supported");
  }

  public BigDecimal getBigDecimal(int columnIndex, int scale)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getBigDecimal(int columnIndex, int scale) not supported");
  }

  public BigDecimal getBigDecimal(String columnName, int scale)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getBigDecimal(String columnName, int scale) not supported");
  }

  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getBinaryStream(int columnIndex) not supported");
  }

  public InputStream getBinaryStream(String columnName) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getBinaryStream(String columnName) not supported");
  }

  public Blob getBlob(int i) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getBlob(int i) not supported");
  }

  public Blob getBlob(String colName) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getBlob(String colName) not supported");
  }

  public boolean getBoolean(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (Boolean.class.isInstance(obj)) {
      return (Boolean) obj;
    } else if (obj == null) {
      return false;
    } else if (Number.class.isInstance(obj)) {
      return ((Number) obj).intValue() != 0;
    } else if (String.class.isInstance(obj)) {
      return !((String) obj).equals("0");
    }
    throw new SQLException("Cannot convert column " + columnIndex
        + " to boolean");
  }

  public boolean getBoolean(String columnName) throws SQLException {
    return getBoolean(findColumn(columnName));
  }

  public byte getByte(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (Number.class.isInstance(obj)) {
      return ((Number) obj).byteValue();
    } else if (obj == null) {
      return 0;
    }
    throw new SQLException("Cannot convert column " + columnIndex + " to byte");
  }

  public byte getByte(String columnName) throws SQLException {
    return getByte(findColumn(columnName));
  }

  public byte[] getBytes(int columnIndex) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getBytes(int columnIndex) not supported");
  }

  public byte[] getBytes(String columnName) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getBytes(String columnName) not supported");
  }

  public Reader getCharacterStream(int columnIndex) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getCharacterStream(int columnIndex) not supported");
  }

  public Reader getCharacterStream(String columnName) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getCharacterStream(String columnName) not supported");
  }

  public Clob getClob(int i) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getClob(int i) not supported");
  }

  public Clob getClob(String colName) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getClob(String colName) not supported");
  }

  public int getConcurrency() throws SQLException {
    return ResultSet.CONCUR_READ_ONLY;
  }

  public String getCursorName() throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getCursorName() not supported");
  }

  public Date getDate(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (obj == null) {
      return null;
    }

    try {
      return Date.valueOf((String) obj);
    } catch (Exception e) {
      throw new SQLException("Cannot convert column " + columnIndex
          + " to date: " + e.toString());
    }
  }

  public Date getDate(String columnName) throws SQLException {
    return getDate(findColumn(columnName));
  }

  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getDate(int columnIndex, Calendar cal) not supported");
  }

  public Date getDate(String columnName, Calendar cal) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getDate(String columnName, Calendar cal) not supported");
  }

  public double getDouble(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (Number.class.isInstance(obj)) {
        return ((Number) obj).doubleValue();
      } else if (obj == null) {
        return 0;
      } else if (String.class.isInstance(obj)) {
        return Double.valueOf((String) obj);
      }
      throw new Exception("Illegal conversion");
    } catch (Exception e) {
      throw new SQLException("Cannot convert column " + columnIndex
          + " to double: " + e.toString());
    }
  }

  public double getDouble(String columnName) throws SQLException {
    return getDouble(findColumn(columnName));
  }

  public int getFetchDirection() throws SQLException {
    return ResultSet.FETCH_FORWARD;
  }

  public int getFetchSize() throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getFetchSize() not supported");
  }

  public float getFloat(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (Number.class.isInstance(obj)) {
        return ((Number) obj).floatValue();
      } else if (obj == null) {
        return 0;
      } else if (String.class.isInstance(obj)) {
        return Float.valueOf((String) obj);
      }
      throw new Exception("Illegal conversion");
    } catch (Exception e) {
      throw new SQLException("Cannot convert column " + columnIndex
          + " to float: " + e.toString());
    }
  }

  public float getFloat(String columnName) throws SQLException {
    return getFloat(findColumn(columnName));
  }

  public int getHoldability() throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getHoldability() not supported");
  }

  public int getInt(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (Number.class.isInstance(obj)) {
        return ((Number) obj).intValue();
      } else if (obj == null) {
        return 0;
      } else if (String.class.isInstance(obj)) {
        return Integer.valueOf((String) obj);
      }
      throw new Exception("Illegal conversion");
    } catch (Exception e) {
      throw new SQLException("Cannot convert column " + columnIndex
          + " to integer" + e.toString());
    }
  }

  public int getInt(String columnName) throws SQLException {
    return getInt(findColumn(columnName));
  }

  public long getLong(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (Number.class.isInstance(obj)) {
        return ((Number) obj).longValue();
      } else if (obj == null) {
        return 0;
      } else if (String.class.isInstance(obj)) {
        return Long.valueOf((String) obj);
      }
      throw new Exception("Illegal conversion");
    } catch (Exception e) {
      throw new SQLException("Cannot convert column " + columnIndex
          + " to long: " + e.toString());
    }
  }

  public long getLong(String columnName) throws SQLException {
    return getLong(findColumn(columnName));
  }

  public ResultSetMetaData getMetaData() throws SQLException {
    return new HiveResultSetMetaData(columnNames, columnTypes);
  }

  public Reader getNCharacterStream(int arg0) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getNCharacterStream(int arg0) not supported");
  }

  public Reader getNCharacterStream(String arg0) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getNCharacterStream(String arg0) not supported");
  }

  public NClob getNClob(int arg0) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getNClob(int arg0) not supported");
  }

  public NClob getNClob(String columnLabel) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getNClob(String columnLabel) not supported");
  }

  public String getNString(int columnIndex) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getNString(int columnIndex) not supported");
  }

  public String getNString(String columnLabel) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getNString(String columnLabel) not supported");
  }

  public Object getObject(int columnIndex) throws SQLException {
    if (row == null) {
      throw new SQLException("No row found.");
    }

    if (columnIndex > row.size()) {
      throw new SQLException("Invalid columnIndex: " + columnIndex);
    }

    try {
      wasNull = false;
      if (row.get(columnIndex - 1) == null) {
        wasNull = true;
      }

      return row.get(columnIndex - 1);
    } catch (Exception e) {
      throw new SQLException(e.toString());
    }
  }

  public Object getObject(String columnName) throws SQLException {
    return getObject(findColumn(columnName));
  }

  public Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getObject(int i, Map<String, Class<?>> map) not supported");
  }

  public Object getObject(String colName, Map<String, Class<?>> map)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getObject(String colName, Map<String, Class<?>> map) not supported");
  }

  public Ref getRef(int i) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getRef(int i) not supported");
  }

  public Ref getRef(String colName) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getRef(String colName) not supported");
  }

  public int getRow() throws SQLException {
    throw new SQLException("Method HiveBaseResultSet.getRow() not supported");
  }

  public RowId getRowId(int columnIndex) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getRowId(int columnIndex) not supported");
  }

  public RowId getRowId(String columnLabel) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getRowId(String columnLabel) not supported");
  }

  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getSQLXML(int columnIndex) not supported");
  }

  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getSQLXML(String columnLabel) not supported");
  }

  public short getShort(int columnIndex) throws SQLException {
    try {
      Object obj = getObject(columnIndex);
      if (Number.class.isInstance(obj)) {
        return ((Number) obj).shortValue();
      } else if (obj == null) {
        return 0;
      } else if (String.class.isInstance(obj)) {
        return Short.valueOf((String) obj);
      }
      throw new Exception("Illegal conversion");
    } catch (Exception e) {
      throw new SQLException("Cannot convert column " + columnIndex
          + " to short: " + e.toString());
    }
  }

  public short getShort(String columnName) throws SQLException {
    return getShort(findColumn(columnName));
  }

  public Statement getStatement() throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getStatement() not supported");
  }

  public String getString(int columnIndex) throws SQLException {
    Object obj = getObject(columnIndex);
    if (obj == null) {
      return null;
    }

    return obj.toString();
  }

  public String getString(String columnName) throws SQLException {
    return getString(findColumn(columnName));
  }

  public Time getTime(int columnIndex) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getTime(int columnIndex) not supported");
  }

  public Time getTime(String columnName) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getTime(String columnName) not supported");
  }

  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getTime(int columnIndex, Calendar cal) not supported");
  }

  public Time getTime(String columnName, Calendar cal) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getTime(String columnName, Calendar cal) not supported");
  }

  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getTimestamp(int columnIndex) not supported");
  }

  public Timestamp getTimestamp(String columnName) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getTimestamp(String columnName) not supported");
  }

  public Timestamp getTimestamp(int columnIndex, Calendar cal)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getTimestamp(int columnIndex, Calendar cal) not supported");
  }

  public Timestamp getTimestamp(String columnName, Calendar cal)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getTimestamp(String columnName, Calendar cal) not supported");
  }

  public int getType() throws SQLException {
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  public URL getURL(int columnIndex) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getURL(int columnIndex) not supported");
  }

  public URL getURL(String columnName) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getURL(String columnName) not supported");
  }

  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getUnicodeStream(int columnIndex) not supported");
  }

  public InputStream getUnicodeStream(String columnName) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.getUnicodeStream(String columnName) not supported");
  }

  public void insertRow() throws SQLException {
    throw new SQLException("Method HiveBaseResultSet.insertRow() not supported");
  }

  public boolean isAfterLast() throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.isAfterLast() not supported");
  }

  public boolean isBeforeFirst() throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.isBeforeFirst() not supported");
  }

  public boolean isClosed() throws SQLException {
    throw new SQLException("Method HiveBaseResultSet.isClosed() not supported");
  }

  public boolean isFirst() throws SQLException {
    throw new SQLException("Method HiveBaseResultSet.isFirst() not supported");
  }

  public boolean isLast() throws SQLException {
    throw new SQLException("Method HiveBaseResultSet.isLast() not supported");
  }

  public boolean last() throws SQLException {
    throw new SQLException("Method HiveBaseResultSet.last() not supported");
  }

  public void moveToCurrentRow() throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.moveToCurrentRow() not supported");
  }

  public void moveToInsertRow() throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.moveToInsertRow() not supported");
  }

  public boolean previous() throws SQLException {
    throw new SQLException("Method HiveBaseResultSet.previous() not supported");
  }

  public void refreshRow() throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.refreshRow() not supported");
  }

  public boolean relative(int rows) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.relative(int rows) not supported");
  }

  public boolean rowDeleted() throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.rowDeleted() not supported");
  }

  public boolean rowInserted() throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.rowInserted() not supported");
  }

  public boolean rowUpdated() throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.rowUpdated() not supported");
  }

  public void setFetchDirection(int direction) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.setFetchDirection(int direction) not supported");
  }

  public void setFetchSize(int rows) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.setFetchSize(int rows) not supported");
  }

  public void updateArray(int columnIndex, Array x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateArray(int columnIndex, Array x) not supported");
  }

  public void updateArray(String columnName, Array x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateArray(String columnName, Array x) not supported");
  }

  public void updateAsciiStream(int columnIndex, InputStream x)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateAsciiStream(int columnIndex, InputStream x) not supported");
  }

  public void updateAsciiStream(String columnLabel, InputStream x)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateAsciiStream(String columnLabel, InputStream x) not supported");
  }

  public void updateAsciiStream(int columnIndex, InputStream x, int length)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateAsciiStream(int columnIndex, InputStream x, int length) not supported");
  }

  public void updateAsciiStream(String columnName, InputStream x, int length)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateAsciiStream(String columnName, InputStream x, int length) not supported");
  }

  public void updateAsciiStream(int columnIndex, InputStream x, long length)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateAsciiStream(int columnIndex, InputStream x, long length) not supported");
  }

  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateAsciiStream(String columnLabel, InputStream x, long length) not supported");
  }

  public void updateBigDecimal(int columnIndex, BigDecimal x)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateBigDecimal(int columnIndex, BigDecimal x) not supported");
  }

  public void updateBigDecimal(String columnName, BigDecimal x)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateBigDecimal(String columnName, BigDecimal x) not supported");
  }

  public void updateBinaryStream(int columnIndex, InputStream x)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateBinaryStream(int columnIndex, InputStream x) not supported");
  }

  public void updateBinaryStream(String columnLabel, InputStream x)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateBinaryStream(String columnLabel, InputStream x) not supported");
  }

  public void updateBinaryStream(int columnIndex, InputStream x, int length)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateBinaryStream(int columnIndex, InputStream x, int length) not supported");
  }

  public void updateBinaryStream(String columnName, InputStream x, int length)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateBinaryStream(String columnName, InputStream x, int length) not supported");
  }

  public void updateBinaryStream(int columnIndex, InputStream x, long length)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateBinaryStream(int columnIndex, InputStream x, long length) not supported");
  }

  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateBinaryStream(String columnLabel, InputStream x, long length) not supported");
  }

  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateBlob(int columnIndex, Blob x) not supported");
  }

  public void updateBlob(String columnName, Blob x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateBlob(String columnName, Blob x) not supported");
  }

  public void updateBlob(int columnIndex, InputStream inputStream)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateBlob(int columnIndex, InputStream inputStream) not supported");
  }

  public void updateBlob(String columnLabel, InputStream inputStream)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateBlob(String columnLabel, InputStream inputStream) not supported");
  }

  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateBlob(int columnIndex, InputStream inputStream, long length) not supported");
  }

  public void updateBlob(String columnLabel, InputStream inputStream,
      long length) throws SQLException {
    throw new SQLException(
        "Method HiveBastResultSet.updateBlob(String columnLabel, InputStream inputStream,long length)not supported");
  }

  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateBoolean(int columnIndex, boolean x) not supported");
  }

  public void updateBoolean(String columnName, boolean x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateBoolean(String columnName, boolean x) not supported");
  }

  public void updateByte(int columnIndex, byte x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateByte(int columnIndex, byte x) not supported");
  }

  public void updateByte(String columnName, byte x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateByte(String columnName, byte x) not supported");
  }

  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateBytes(int columnIndex, byte[] x) not supported");
  }

  public void updateBytes(String columnName, byte[] x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateBytes(String columnName, byte[] x) not supported");
  }

  public void updateCharacterStream(int columnIndex, Reader x)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateCharacterStream(int columnIndex, Reader x) not supported");
  }

  public void updateCharacterStream(String columnLabel, Reader reader)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateCharacterStream(String columnLabel, Reader reader) not supported");
  }

  public void updateCharacterStream(int columnIndex, Reader x, int length)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateCharacterStream(int columnIndex, Reader x, int length) not supported");
  }

  public void updateCharacterStream(String columnName, Reader reader, int length)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateCharacterStream(String columnName, Reader reader, int length) not supported");
  }

  public void updateCharacterStream(int columnIndex, Reader x, long length)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateCharacterStream(int columnIndex, Reader x, long length) not supported");
  }

  public void updateCharacterStream(String columnLabel, Reader reader,
      long length) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateCharacterStream(String columnLabel, Reader reader,long length) not supported");
  }

  public void updateClob(int columnIndex, Clob x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateClob(int columnIndex, Clob x) not supported");
  }

  public void updateClob(String columnName, Clob x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateClob(String columnName, Clob x) not supported");
  }

  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateClob(int columnIndex, Reader reader) not supported");
  }

  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateClob(String columnLabel, Reader reader) not supported");
  }

  public void updateClob(int columnIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateClob(int columnIndex, Reader reader, long length) not supported");
  }

  public void updateClob(String columnLabel, Reader reader, long length)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateClob(String columnLabel, Reader reader, long length) not supported");
  }

  public void updateDate(int columnIndex, Date x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateDate(int columnIndex, Date x) not supported");
  }

  public void updateDate(String columnName, Date x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateDate(String columnName, Date x) not supported");
  }

  public void updateDouble(int columnIndex, double x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateDouble(int columnIndex, double x) not supported");
  }

  public void updateDouble(String columnName, double x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateDouble(String columnName, double x) not supported");
  }

  public void updateFloat(int columnIndex, float x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateFloat(int columnIndex, float x) not supported");
  }

  public void updateFloat(String columnName, float x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateFloat(String columnName, float x) not supported");
  }

  public void updateInt(int columnIndex, int x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateInt(int columnIndex, int x) not supported");
  }

  public void updateInt(String columnName, int x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateInt(String columnName, int x) not supported");
  }

  public void updateLong(int columnIndex, long x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateLong(int columnIndex, long x) not supported");
  }

  public void updateLong(String columnName, long x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateLong(String columnName, long x) not supported");
  }

  public void updateNCharacterStream(int columnIndex, Reader x)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateNCharacterStream(int columnIndex, Reader x) not supported");
  }

  public void updateNCharacterStream(String columnLabel, Reader reader)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateNCharacterStream(String columnLabel, Reader reader) not supported");
  }

  public void updateNCharacterStream(int columnIndex, Reader x, long length)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateNCharacterStream(int columnIndex, Reader x, long length) not supported");
  }

  public void updateNCharacterStream(String columnLabel, Reader reader,
      long length) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateNCharacterStream(String columnLabel, Reader reader,long length) not supported");
  }

  public void updateNClob(int columnIndex, NClob clob) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateNClob(int columnIndex, NClob clob) not supported");
  }

  public void updateNClob(String columnLabel, NClob clob) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateNClob(String columnLabel, NClob clob) not supported");
  }

  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateNClob(int columnIndex, Reader reader) not supported");
  }

  public void updateNClob(String columnLabel, Reader reader)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateNClob(String columnLabel, Reader reader) not supported");
  }

  public void updateNClob(int columnIndex, Reader reader, long length)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateNClob(int columnIndex, Reader reader, long length) not supported");
  }

  public void updateNClob(String columnLabel, Reader reader, long length)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateNClob(String columnLabel, Reader reader, long length) not supported");
  }

  public void updateNString(int columnIndex, String string) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateNString(int columnIndex, String string) not supported");
  }

  public void updateNString(String columnLabel, String string)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateNString(String columnLabel, String string) not supported");
  }

  public void updateNull(int columnIndex) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateNull(int columnIndex) not supported");
  }

  public void updateNull(String columnName) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateNull(String columnName) not supported");
  }

  public void updateObject(int columnIndex, Object x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateObject(int columnIndex, Object x) not supported");
  }

  public void updateObject(String columnName, Object x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateObject(String columnName, Object x) not supported");
  }

  public void updateObject(int columnIndex, Object x, int scale)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateObject(int columnIndex, Object x, int scale) not supported");
  }

  public void updateObject(String columnName, Object x, int scale)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateObject(String columnName, Object x, int scale) not supported");
  }

  public void updateRef(int columnIndex, Ref x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateRef(int columnIndex, Ref x) not supported");
  }

  public void updateRef(String columnName, Ref x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateRef(String columnName, Ref x) not supported");
  }

  public void updateRow() throws SQLException {
    throw new SQLException("Method HiveBaseResultSet.updateRow() not supported");
  }

  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateRowId(int columnIndex, RowId x) not supported");
  }

  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateRowId(String columnLabel, RowId x) not supported");
  }

  public void updateSQLXML(int columnIndex, SQLXML xmlObject)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateSQLXML(int columnIndex, SQLXML xmlObject) not supported");
  }

  public void updateSQLXML(String columnLabel, SQLXML xmlObject)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateSQLXML(String columnLabel, SQLXML xmlObject) not supported");
  }

  public void updateShort(int columnIndex, short x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateShort(int columnIndex, short x) not supported");
  }

  public void updateShort(String columnName, short x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateShort(String columnName, short x) not supported");
  }

  public void updateString(int columnIndex, String x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateString(int columnIndex, String x) not supported");
  }

  public void updateString(String columnName, String x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateString(String columnName, String x) not supported");
  }

  public void updateTime(int columnIndex, Time x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateTime(int columnIndex, Time x) not supported");
  }

  public void updateTime(String columnName, Time x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateTime(String columnName, Time x) not supported");
  }

  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateTimestamp(int columnIndex, Timestamp x) not supported");
  }

  public void updateTimestamp(String columnName, Timestamp x)
      throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.updateTimestamp(String columnName, Timestamp x) not supported");
  }

  public SQLWarning getWarnings() throws SQLException {
    return warningChain;
  }

  public void clearWarnings() throws SQLException {
    warningChain = null;
  }

  public void close() throws SQLException {
    throw new SQLException("Method HiveBaseResultSet.close() not supported");
  }

  public boolean wasNull() throws SQLException {
    return wasNull;
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.isWrapperFor(Class<?> iface) not supported");
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException(
        "Method HiveBaseResultSet.unwrap(Class<T> iface) not supported");
  }
}
