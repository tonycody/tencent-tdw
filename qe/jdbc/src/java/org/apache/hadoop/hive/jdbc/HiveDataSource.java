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

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

public class HiveDataSource implements DataSource {

  public HiveDataSource() {
  }

  public Connection getConnection() throws SQLException {
    return getConnection("", "");
  }

  public Connection getConnection(String username, String password)
      throws SQLException {
    try {
      return new HiveConnection("", null);
    } catch (Exception ex) {
      throw new SQLException();
    }
  }

  public PrintWriter getLogWriter() throws SQLException {
    throw new SQLException("Method HiveDataSource.getLogWriter() not supported");
  }

  public int getLoginTimeout() throws SQLException {
    throw new SQLException(
        "Method HiveDataSource.getLoginTimeout() not supported");
  }

  public void setLogWriter(PrintWriter arg0) throws SQLException {
    throw new SQLException(
        "Method HiveDataSource.setLogWriter(PrintWriter arg0) not supported");
  }

  public void setLoginTimeout(int arg0) throws SQLException {
    throw new SQLException(
        "Method HiveDataSource.setLoginTimeout(int arg0) not supported");
  }

  public boolean isWrapperFor(Class<?> arg0) throws SQLException {
    throw new SQLException(
        "Method HiveDataSource.isWrapperFor(Class<?> arg0) not supported");
  }

  public <T> T unwrap(Class<T> arg0) throws SQLException {
    throw new SQLException(
        "Method HiveDataSource.unwrap(Class<T> arg0) not supported");
  }

}
