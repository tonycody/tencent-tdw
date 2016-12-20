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
package org.apache.hadoop.hive.metastore;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SyncDBRouterThread implements Runnable {
  private static Map<String, DBRouterInfo> dbRouterNew = null;
  public static Map<String, DBRouterInfo> dbRouter = new ConcurrentHashMap<String, DBRouterInfo>();

  public static final Log LOG = LogFactory.getLog(SyncDBRouterThread.class
      .getName());

  private String masterUrl = null;
  private String user = null;
  private String passwd = null;
  private long syncTime = 10;

  public SyncDBRouterThread(String url, String use, String pass) {
    masterUrl = url;
    user = use;
    passwd = pass;
  }

  public SyncDBRouterThread(String url, String use, String pass, long time) {
    masterUrl = url;
    user = use;
    passwd = pass;
    syncTime = time;
  }

  @Override
  public void run() {
    Connection con = null;
    while (true) {
      try {
        String driverName = "org.postgresql.Driver";
        Class.forName(driverName).newInstance();

        con = DriverManager.getConnection(masterUrl, user, passwd);
        Statement stmt = con.createStatement();

        String sql = "select db_name, seg_addr, secondary_seg_addr, is_db_split, describe from router";
        ResultSet rs = stmt.executeQuery(sql);

        dbRouterNew = new ConcurrentHashMap<String, DBRouterInfo>();

        LOG.debug("**************************************************");
        while (rs.next()) {
          DBRouterInfo route = new DBRouterInfo();
          route.setDBName(rs.getString(1));
          route.setSegmentDBUrl(rs.getString(2));
          route.setSecondarySegmentDBUrl(rs.getString(3));
          route.setHasTableRouter(rs.getBoolean(4));
          route.setDetail(rs.getString(5));

          LOG.debug("db name is " + route.getDBName() + "\n" + " seg addr is "
              + route.getSegmentDBUrl() + "\n" + " second seg addr is "
              + route.getSecondarySlaveDBUrl() + "\n"
              + " is has table route is " + route.getHasTableRouter() + "\n"
              + " detail is " + route.getDetail());

          dbRouterNew.put(route.getDBName(), route);
        }

        LOG.debug("**************************************************");

        dbRouter = dbRouterNew;
      } catch (Exception x) {
        x.printStackTrace();
      } finally {
        try {
          con.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }

      try {
        if (syncTime < 1) {
          syncTime = 1;
        }

        Thread.sleep(1000 * syncTime);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public void init() {
    Connection con = null;
    try {
      String driverName = "org.postgresql.Driver";
      Class.forName(driverName).newInstance();

      con = DriverManager.getConnection(masterUrl, user, passwd);
      Statement stmt = con.createStatement();

      String sql = "select db_name, seg_addr, secondary_seg_addr, is_db_split, describe from router";
      ResultSet rs = stmt.executeQuery(sql);

      dbRouterNew = new ConcurrentHashMap<String, DBRouterInfo>();

      LOG.debug("**************************************************");
      while (rs.next()) {
        DBRouterInfo route = new DBRouterInfo();
        route.setDBName(rs.getString(1));
        route.setSegmentDBUrl(rs.getString(2));
        route.setSecondarySegmentDBUrl(rs.getString(3));
        route.setHasTableRouter(rs.getBoolean(4));
        route.setDetail(rs.getString(5));

        LOG.debug("db name is " + route.getDBName() + "\n" + " slave addr is "
            + route.getSegmentDBUrl() + "\n" + " second slave addr is "
            + route.getSecondarySlaveDBUrl() + "\n" + " is has table route is "
            + route.getHasTableRouter() + "\n" + " detail is "
            + route.getDetail());

        dbRouterNew.put(route.getDBName(), route);
      }

      LOG.debug("**************************************************");

      dbRouter = dbRouterNew;
    } catch (Exception x) {
      x.printStackTrace();
    } finally {
      try {
        con.close();
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

}
