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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Pattern;

public class HiveDriver implements Driver {
  static {
    try {
      java.sql.DriverManager.registerDriver(new HiveDriver());
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  private static final int MAJOR_VERSION = 0;

  private static final int MINOR_VERSION = 110704;

  private static final boolean JDBC_COMPLIANT = false;

  private static final String URL_PREFIX = "jdbc:hive://";

  private static final String DEFAULT_PORT = "10000";

  private static final String DEFAULT_DB = "default_db";

  private static final String DBNAME_PROPERTY_KEY = "DBNAME";

  private static final String HOST_PROPERTY_KEY = "HOST";

  private static final String PORT_PROPERTY_KEY = "PORT";

  private static final String CONNECT_USER = "user";

  private static final String CONNECT_PASS = "password";

  public HiveDriver() {
    SecurityManager security = System.getSecurityManager();
    if (security != null) {
      security.checkWrite("foobah");
    }
  }

  public boolean acceptsURL(String url) throws SQLException {
    return Pattern.matches(URL_PREFIX + ".*", url);
  }

  public Connection connect(String url, Properties info) throws SQLException {
    return new HiveConnection(url, info);
  }

  public int getMajorVersion() {
    return MAJOR_VERSION;
  }

  public int getMinorVersion() {
    return MINOR_VERSION;
  }

  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
      throws SQLException {
    if (info == null) {
      info = new Properties();
    }

    if ((url != null) && url.startsWith(URL_PREFIX)) {
      info = parseURL(url, info);
    }

    DriverPropertyInfo hostProp = new DriverPropertyInfo(HOST_PROPERTY_KEY,
        info.getProperty(HOST_PROPERTY_KEY, ""));
    hostProp.required = true;
    hostProp.description = "Hostname of Hive Server";

    DriverPropertyInfo portProp = new DriverPropertyInfo(PORT_PROPERTY_KEY,
        info.getProperty(PORT_PROPERTY_KEY, ""));
    portProp.required = true;
    portProp.description = "Port number of Hive Server";

    DriverPropertyInfo dbProp = new DriverPropertyInfo(DBNAME_PROPERTY_KEY,
        info.getProperty(DBNAME_PROPERTY_KEY, "default_db"));
    dbProp.required = true;
    dbProp.description = "Database name";

    DriverPropertyInfo userProp = new DriverPropertyInfo(CONNECT_USER,
        info.getProperty(CONNECT_USER));
    dbProp.required = true;
    dbProp.description = "Connect User Name";

    DriverPropertyInfo passProp = new DriverPropertyInfo(CONNECT_PASS,
        info.getProperty(CONNECT_PASS));
    dbProp.required = true;
    dbProp.description = "Connect Password";

    DriverPropertyInfo[] dpi = new DriverPropertyInfo[5];

    dpi[0] = hostProp;
    dpi[1] = portProp;
    dpi[2] = dbProp;
    dpi[3] = userProp;
    dpi[4] = passProp;

    return dpi;
  }

  public boolean jdbcCompliant() {
    return JDBC_COMPLIANT;
  }

  private Properties parseURL(String url, Properties defaults)
      throws SQLException {
    Properties urlProps = (defaults != null) ? new Properties(defaults)
        : new Properties();

    if (url == null || !url.startsWith(URL_PREFIX)) {
      throw new SQLException("Invalid connection url: " + url);
    }

    if (url.length() <= URL_PREFIX.length()) {
      return urlProps;
    }

    String connectionInfo = url.substring(URL_PREFIX.length());

    String[] hostPortAndDatabase = connectionInfo.split("/", 2);

    if (hostPortAndDatabase[0].length() > 0) {
      String[] hostAndPort = hostPortAndDatabase[0].split(":", 2);
      urlProps.put(HOST_PROPERTY_KEY, hostAndPort[0]);
      if (hostAndPort.length > 1) {
        urlProps.put(PORT_PROPERTY_KEY, hostAndPort[1]);
      } else {
        urlProps.put(PORT_PROPERTY_KEY, DEFAULT_PORT);
      }
    }

    if (hostPortAndDatabase.length > 1) {
      urlProps.put(DBNAME_PROPERTY_KEY, hostPortAndDatabase[1]);
    } else
      urlProps.put(DBNAME_PROPERTY_KEY, DEFAULT_DB);

    return urlProps;
  }
}
