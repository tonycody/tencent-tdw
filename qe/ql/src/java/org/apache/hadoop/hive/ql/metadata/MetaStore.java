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
package org.apache.hadoop.hive.ql.metadata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.User;
import org.apache.hadoop.hive.ql.session.SessionState;

public class MetaStore {

  private int waittime = SessionState.get().getConf().getInt("hive.pg.timeout", 10);
//  private int waittime = 6;
  private String url;
  private Properties prop = null;
  private static Log LOG = LogFactory.getLog(MetaStore.class.getName());

  public MetaStore(Properties prop) {
    this.url = prop.getProperty("url");
    this.prop = prop;
    this.prop.setProperty("loginTimeout", String.valueOf(waittime));
  }

  public Connection openConnect() {
    try {
      Class.forName("com.mysql.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      LOG.error(" get com.mysql.jdbc.Driver failed! ");
      e.printStackTrace();
    }
    Connection conn = null;
    try {
      LOG.info(" Connecting: " + url);
      DriverManager.setLoginTimeout(waittime);
      conn = DriverManager.getConnection(url, this.prop);
      LOG.info(" get Connection ok: " + url);
    } catch (SQLException e) {
      LOG.error(" get Connection failed: " + url);
      e.printStackTrace();
    }
    return conn;
  }

  public void closeConnect(Connection cc) {
    if (cc == null) {
      return;
    }
    try {
      cc.close();
    } catch (SQLException e) {
      LOG.error(" close Connection of mysql failed! ");
      e.printStackTrace();
    }
  }

  public String getPwd(Connection cc, String uname) {
    if (cc == null || uname == null) {
      return null;
    }
    String rt = null;
    PreparedStatement pstmt;
    try {
      pstmt = cc
          .prepareStatement(" select PASSWD from USER where USER_NAME= ? ");
      pstmt.setString(1, uname);
      ResultSet rs = pstmt.executeQuery();
      rs.first();
      rt = (String) rs.getObject(1);
      rs.close();
      LOG.info(" get user pwd: " + uname + " " + rt);
    } catch (SQLException e) {
      LOG.error(" get user pwd failed: " + uname);
      e.printStackTrace();
    }
    return rt;
  }

  public User getUser(Connection cc, String uname) {
    if (cc == null || uname == null) {
      return null;
    }
    User rt = null;
    PreparedStatement pstmt;
    try {
      pstmt = cc
          .prepareStatement(" select USER_ID,GROUP_NAME,PASSWD,USER_NAME,"
              + "ALTER_PRIV,CREATE_PRIV,CREATEVIEW_PRIV,DBA_PRIV,SHOWVIEW_PRIV,"
              + "DELETE_PRIV,DROP_PRIV,INDEX_PRIV,INSERT_PRIV,SELECT_PRIV,UPDATE_PRIV from USER where USER_NAME= ? ");
      pstmt.setString(1, uname);
      ResultSet rs = pstmt.executeQuery();
      rs.first();
      long USER_ID = (long) rs.getLong(1);
      String GROUP_NAME = (String) rs.getObject(2);
      String PASSWD = (String) rs.getObject(3);
      String USER_NAME = (String) rs.getObject(4);
      boolean ALTER_PRIV = (boolean) rs.getBoolean(5);
      boolean CREATE_PRIV = (boolean) rs.getBoolean(6);
      boolean CREATEVIEW_PRIV = (boolean) rs.getBoolean(7);
      boolean DBA_PRIV = (boolean) rs.getBoolean(8);
      boolean SHOWVIEW_PRIV = (boolean) rs.getBoolean(9);
      boolean DELETE_PRIV = (boolean) rs.getBoolean(10);
      boolean DROP_PRIV = (boolean) rs.getBoolean(11);
      boolean INDEX_PRIV = (boolean) rs.getBoolean(12);
      boolean INSERT_PRIV = (boolean) rs.getBoolean(13);
      boolean SELECT_PRIV = (boolean) rs.getBoolean(14);
      boolean UPDATE_PRIV = (boolean) rs.getBoolean(15);
      rt = new User(USER_NAME, null, SELECT_PRIV, INSERT_PRIV, INDEX_PRIV,
          CREATE_PRIV, DROP_PRIV, DELETE_PRIV, ALTER_PRIV, UPDATE_PRIV,
          CREATEVIEW_PRIV, SHOWVIEW_PRIV, DBA_PRIV, GROUP_NAME);
      rs.close();
      LOG.info(" get user0  : " + rt.toString());
      List<String> playRoles = new LinkedList<String>();
      pstmt = cc
          .prepareStatement(" select ROLE_NAME from PLAY_ROLES join ROLE on PLAY_ROLES.ROLE_ID = ROLE.ROLE_ID where PLAY_ROLES.USER_ID= ? ");
      pstmt.setLong(1, USER_ID);
      rs = pstmt.executeQuery();
      while (rs.next()) {
        String tmpname = (String) rs.getObject(1);
        LOG.info(" ROLE_NAME : " + tmpname);
        playRoles.add(tmpname);
      }
      rs.close();
      rt.setPlayRoles(playRoles);
      LOG.info(" get user1  : " + USER_ID + " " + GROUP_NAME + " " + PASSWD
          + " " + USER_NAME);
      LOG.info(" get user2  : " + rt.toString());
    } catch (SQLException e) {
      LOG.error(" get user failed: " + uname);
      e.printStackTrace();
    }
    return rt;
  }

  public String getDB(Connection cc, String dbname) {
    if (cc == null || dbname == null) {
      return null;
    }
    String rt = null;
    PreparedStatement pstmt;
    try {
      pstmt = cc.prepareStatement(" select * from DBS where NAME= ? ");
      pstmt.setString(1, dbname);
      ResultSet rs = pstmt.executeQuery();
      rs.first();
      LOG.info((Long) rs.getObject(1));
      LOG.info((String) rs.getObject(2));
      rt = (String) rs.getObject(2);
      rs.close();
      LOG.info(" get db: " + dbname + " " + rt);
    } catch (SQLException e) {
      LOG.error(" get db failed: " + dbname);
      e.printStackTrace();
    }
    return rt;
  }
}
