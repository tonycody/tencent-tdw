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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hive.metastore.api.tdw_query_error_info;
import org.apache.hadoop.hive.metastore.api.tdw_query_info;
import org.apache.hadoop.hive.metastore.api.tdw_query_stat;
import org.apache.hadoop.hive.ql.exec.InsertExeInfo;
import org.apache.hadoop.hive.ql.session.SessionState;

public class BIStore {

  private int waittime = SessionState.get().getConf().getInt("hive.pg.timeout", 10);
//  private int waittime = 6;
  private String ip;
  private String dbName;
  private int port;
  private String userName;
  private String pwd;
  private String url;
  private Properties prop = null;
  private static Log LOG = LogFactory.getLog(BIStore.class.getName());

  private int ddlQueryResult;
  private String serverIp;

  public BIStore(Properties prop) {
    this.ip = prop.getProperty("ip");
    this.dbName = prop.getProperty("db_name");
    this.port = Integer.valueOf(prop.getProperty("port"));
    this.userName = prop.getProperty("user");
    this.pwd = prop.getProperty("password");
    this.url = "jdbc:postgresql://" + ip + ":" + port + "/" + dbName;
    this.prop = prop;
    this.prop.setProperty("loginTimeout", String.valueOf(waittime));
  }
  public BIStore(String url,String user,String passwd) {
	    this.userName = user;
	    this.pwd = passwd;
	    this.url = url;
	    this.prop = new Properties();
	    this.prop.setProperty("user", user);
	    this.prop.setProperty("password", passwd);
	    this.prop.setProperty("loginTimeout", String.valueOf(waittime));
	  }
  public Connection openConnect() {
    try {
      Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException e) {
      LOG.error(" get org.postgresql.Driver failed ");
      e.printStackTrace();
    }
    Connection conn = null;
    try {
      LOG.debug(" Connecting: " + url);
      DriverManager.setLoginTimeout(waittime);
      conn = DriverManager.getConnection(url, this.prop);
      try {
        String processName = java.lang.management.ManagementFactory
            .getRuntimeMXBean().getName();
        String processID = processName.substring(0, processName.indexOf('@'));
        String appinfo = "openConnect_" + processID + "_"
            + SessionState.get().getSessionName();
        conn.setClientInfo("ApplicationName", appinfo);
      } catch (Exception e) {
        e.printStackTrace();
      }
      LOG.debug(" get Connection ok: " + url);
    } catch (SQLException e) {
      LOG.error(" get Connection failed: " + url);
      e.printStackTrace();
    }
    return conn;
  }

  public tdw_query_info getQueryInfoItem(Connection cc, String queryID) {
    Statement stmt;
    tdw_query_info qinfo = null;

    try {
      stmt = cc.createStatement();
      String sql = "select a.mrnum, a.finishtime, a.queryid, a.querystring, "
          + "a.starttime, a.username, a.ip, a.taskid, a.port, a.clientip, a.dbname from tdw_query_info_new a where "
          + "a.queryid='" + queryID + "'";

      ResultSet querySet = stmt.executeQuery(sql);
      while (querySet.next()) {
        qinfo = new tdw_query_info();

        qinfo.setMRNum(querySet.getInt(1));

        qinfo.setQueryId(querySet.getString(3));
        qinfo.setQueryString(querySet.getString(4));
        qinfo.setStartTime(querySet.getTimestamp(5).toString());
        qinfo.setUserName(querySet.getString(6));
        qinfo.setIp(querySet.getString(7));
        qinfo.setTaskid(querySet.getString(8));
        qinfo.setPort(querySet.getString(9));
        qinfo.setClientIp(querySet.getString(10));
        qinfo.setDbName(querySet.getString(11));
      }

    } catch (SQLException e) {
      LOG.error(" show processlit, get query info error ");
      e.printStackTrace();
    }
    return qinfo;
  }

  public List<ShowProcessListResult> getQueryInfo(Connection cc, String user) {
    Statement stmt;
    List<ShowProcessListResult> rets = new ArrayList<ShowProcessListResult>();

    try {
      stmt = cc.createStatement();
      String sql = "select a.mrnum, a.finishtime, a.queryid, a.querystring, "
          + "a.starttime, a.username, a.ip, a.taskid, a.port, a.clientip,"
          + " a.dbname, b.mapnum, b.reducenum, "
          + "b.currmrfinishtime, b.currmrid, b.currmrindex, b.currmrstarttime, "
          + "b.queryid from tdw_query_info_new a, tdw_query_stat_new b where "
          + "a.queryid=b.queryid and a.username='" + user.toLowerCase() + "' "
          + "and a.finishtime is null and b.currmrfinishtime is null and "
          + "(now()-a.starttime) < '24:00:00'";

      ResultSet querySet = stmt.executeQuery(sql);
      while (querySet.next()) {
        tdw_query_info qinfo = new tdw_query_info();
        tdw_query_stat sinfo = new tdw_query_stat();
        ShowProcessListResult ret = new ShowProcessListResult();
        qinfo.setMRNum(querySet.getInt(1));

        qinfo.setQueryId(querySet.getString(3));
        qinfo.setQueryString(querySet.getString(4));
        qinfo.setStartTime(querySet.getTimestamp(5).toString());
        qinfo.setUserName(querySet.getString(6));
        qinfo.setIp(querySet.getString(7));
        qinfo.setTaskid(querySet.getString(8));
        qinfo.setPort(querySet.getString(9));
        qinfo.setClientIp(querySet.getString(10));
        qinfo.setDbName(querySet.getString(11));

        sinfo.setMapNum(querySet.getInt(12));
        sinfo.setReduceNum(querySet.getInt(13));
        sinfo.setCurrMRId(querySet.getString(15));
        sinfo.setCurrMRIndex(querySet.getInt(16));
        sinfo.setCurrMRStartTime(querySet.getTimestamp(17).toString());
        sinfo.setQueryId(querySet.getString(18));

        ret.queryInfo = qinfo;
        ret.queryStat = sinfo;
        rets.add(ret);
      }

    } catch (SQLException e) {
      LOG.error(" show processlit, get query info error ");
      e.printStackTrace();
    }
    return rets;
  }

  public int execDMLSQL(Connection cc, String sql) {
    if (cc == null || sql == null) {
      return -1;
    }
    int rt = -1;
    try {
      rt = cc.createStatement().executeUpdate(sql);
    } catch (SQLException e) {
      LOG.error(" exec sql failed: " + sql);
      e.printStackTrace();
    }
    return rt;
  }

  public int updateInfo(Connection cc, String qid, String ftime) {
    if (cc == null || qid == null || ftime == null) {
      return -1;
    }
    int rt = -1;
    PreparedStatement pstmt;
    try {
      pstmt = cc
          .prepareStatement("update TDW_QUERY_INFO_NEW set FINISHTIME = 'now()' WHERE QUERYID = ?");
      pstmt.setString(1, qid);
      rt = pstmt.executeUpdate();
    } catch (SQLException e) {
      LOG.error(" updateInfo failed: " + qid + "  " + ftime);
      e.printStackTrace();
    }
    return rt;
  }

  public int updateStat(Connection cc, String qid, int index) {
    if (cc == null || qid == null || (index <= 0)) {
      return -1;
    }
    int rt = -1;
    PreparedStatement pstmt;
    try {
      pstmt = cc
          .prepareStatement("update TDW_QUERY_STAT_NEW set CURRMRFINISHTIME = 'now()' WHERE QUERYID = ? and currmrindex = ?");
      pstmt.setString(1, qid);
      pstmt.setInt(2, index);
      rt = pstmt.executeUpdate();
    } catch (SQLException e) {
      LOG.error(" updateStat failed: " + qid);
      e.printStackTrace();
    }
    return rt;
  }

  public int insertInfo(Connection cc, tdw_query_info info) {
    if (cc == null || info == null) {
      return -1;
    }
    int rt = -1;
    PreparedStatement pstmt;
    try {
      pstmt = cc
          .prepareStatement("insert into TDW_QUERY_INFO_NEW(QUERYID,MRNUM,QUERYSTRING,USERNAME,IP,TASKID,PORT,CLIENTIP,DBNAME,SESSIONNAME) values (?,?,?,?,?,?,?,?,?,?)");
      pstmt.setString(1, info.getQueryId());
      pstmt.setInt(2, info.getMRNum());
      pstmt.setString(3, info.getQueryString());
      pstmt.setString(4, info.getUserName());
      pstmt.setString(5, info.getIp());
      pstmt.setString(6, info.getTaskid());
      pstmt.setString(7, info.getPort());
      pstmt.setString(8, info.getClientIp());
      pstmt.setString(9, info.getDbName());
      pstmt.setString(10, SessionState.get().getSessionName());
      rt = pstmt.executeUpdate();
    } catch (SQLException e) {
      LOG.error(" insertInfo failed: " + info.getQueryId() + "  "
          + info.getTaskid());
      e.printStackTrace();
    }
    return rt;
  }
  
  public int insertErrorInfo(Connection cc, tdw_query_error_info info) {
    if (cc == null || info == null) {
      return -1;
    }
    int rt = -1;
    PreparedStatement pstmt;
    try {
      pstmt = cc
          .prepareStatement("insert into TDW_QUERY_ERROR_INFO_NEW(QUERYID,TASKID,IP,PORT,CLIENTIP,ERRORSTRING,SESSIONNAME) values (?,?,?,?,?,?,?)");
      pstmt.setString(1, info.getQueryId());
      pstmt.setString(2, info.getTaskId());
      pstmt.setString(3, info.getIp());
      pstmt.setString(4, info.getPort());
      pstmt.setString(5, info.getClientIp());
      pstmt.setString(6, info.getErrorString());
      pstmt.setString(7, SessionState.get().getSessionName());
      rt = pstmt.executeUpdate();
    } catch (SQLException e) {
      LOG.error(" insertInfo failed: " + info.getQueryId() + "  "
          + info.getTaskId());
      e.printStackTrace();
    }
    return rt;
  }

  public int insertDDLInfo(Connection cc, String queryId, String queryCmd) {
    int rt = -1;
    PreparedStatement pstmt;
    try {
      pstmt = cc
          .prepareStatement("insert into TDW_DDL_QUERY_INFO(QUERYID,QUERYSTRING,IP,DBNAME,SESSIONNAME) values (?,?,?,?,?)");
      pstmt.setString(1, queryId);
      pstmt.setString(2, queryCmd);
      pstmt.setString(3, this.getDDLQueryIP());
      pstmt.setString(4, SessionState.get().getDbName());
      pstmt.setString(5, SessionState.get().getSessionName());
      rt = pstmt.executeUpdate();
    } catch (SQLException e) {
      LOG.error(" insertInfo failed: " + queryId);
      e.printStackTrace();
    }
    return rt;
  }

  public int insertBadPbFormatLog(Connection cc, String queryId, String mrid, long badfilenum) {
	    int rt = -1;
	    PreparedStatement pstmt;
	    try {
	      pstmt = cc
	          .prepareStatement("insert into TDW_BADPBFILE_SKIP_LOG(queryid, mrid, badfilenum) values (?, ?, ?)");
	      pstmt.setString(1, queryId);
	      pstmt.setString(2, mrid);
	      pstmt.setLong(3, badfilenum);
	      rt = pstmt.executeUpdate();
	    } catch (SQLException e) {
	      LOG.error(" insert bad pb format log failed: " + queryId);
	      e.printStackTrace();
	    }
	    return rt;
  }  
  
  public int insertMoveInfo(Connection cc, String queryId, String srcPath,
      String destPath, String tbname, String dbname, String taskid) {
    int rt = -1;
    PreparedStatement pstmt;
    try {
      pstmt = cc
          .prepareStatement("insert into TDW_MOVE_INFO(QUERYID,SRCDIR,DESTDIR,TBNAME,DBNAME,TASKID) values (?,?,?,?,?,?)");
      pstmt.setString(1, queryId);
      pstmt.setString(2, srcPath);
      pstmt.setString(3, destPath);
      pstmt.setString(4, tbname);
      pstmt.setString(5, dbname);
      pstmt.setString(6, taskid);
      rt = pstmt.executeUpdate();
    } catch (SQLException e) {
      LOG.error(" insertInfo failed: " + queryId);
      e.printStackTrace();
    }
    return rt;
  }

  public int updateDDLQueryInfo(Connection cc, String qid, boolean ddlQueryRes,
      String taskId, String userName) {
    int rt = -1;
    PreparedStatement pstmt;

    if (cc == null || qid == null) {
      return -1;
    }

    try {
      pstmt = cc
          .prepareStatement("update TDW_DDL_QUERY_INFO set FINISHTIME = 'now()', "
              + "USERNAME = ?, QUERYRESULT = ?, TASKID = ? WHERE QUERYID = ?");
      pstmt.setString(1, userName);
      pstmt.setBoolean(2, ddlQueryRes);
      pstmt.setString(3, taskId);
      pstmt.setString(4, qid);
      rt = pstmt.executeUpdate();
    } catch (SQLException e) {
      LOG.error(" updateInfo failed: " + qid);
      e.printStackTrace();
    }
    return rt;
  }

  public Connection openConnect(int timeout) {
    try {
      Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException e) {
      LOG.error(" get org.postgresql.Driver failed ");
      e.printStackTrace();
    }
    Connection conn = null;
    try {
      LOG.info(" Connecting: " + url);
      DriverManager.setLoginTimeout(timeout);
      this.prop.setProperty("loginTimeout", String.valueOf(timeout));
      conn = DriverManager.getConnection(url, this.prop);
      try {
        String processName = java.lang.management.ManagementFactory
            .getRuntimeMXBean().getName();
        String processID = processName.substring(0, processName.indexOf('@'));
        String appinfo = "openConnect_" + processID + "_"
            + SessionState.get().getSessionName();
        conn.setClientInfo("ApplicationName", appinfo);
      } catch (Exception e) {
        e.printStackTrace();
      }
      LOG.info(" get Connection ok: " + url);
    } catch (SQLException e) {
      LOG.error(" get Connection failed: " + url);
      e.printStackTrace();
    }
    return conn;
  }

  public int insertStat(Connection cc, tdw_query_stat stat) {
    if (cc == null || stat == null) {
      return -1;
    }
    int rt = -1;
    PreparedStatement pstmt;
    try {
      pstmt = cc
          .prepareStatement("insert into TDW_QUERY_STAT_NEW(MAPNUM,REDUCENUM,CURRMRID,CURRMRINDEX,QUERYID,JTIP,SESSIONNAME) values (?,?,?,?,?,?,?)");
      pstmt.setInt(1, stat.getMapNum());
      pstmt.setInt(2, stat.getReduceNum());
      pstmt.setString(3, stat.getCurrMRId());
      pstmt.setInt(4, stat.getCurrMRIndex());
      pstmt.setString(5, stat.getQueryId());
      pstmt.setString(6, stat.getJtIP());
      pstmt.setString(7, SessionState.get().getSessionName());
      rt = pstmt.executeUpdate();
    } catch (SQLException e) {
      LOG.error(" insertStat failed: " + stat.getQueryId() + "  "
          + stat.getCurrMRId());
      e.printStackTrace();
    }
    return rt;
  }

  public void closeConnect(Connection cc) {
    if (cc == null) {
      return;
    }
    try {
      cc.close();
    } catch (SQLException e) {
      LOG.error(" close Connection of pg failed ");
      e.printStackTrace();
    }
  }

  public void setDDLQueryResult(int res) {
    this.ddlQueryResult = res;
  }

  public int getDDLQueryResult() {
    return this.ddlQueryResult;
  }

  public void setDDLQueryIP(String IP) {
    this.serverIp = IP;
  }

  public String getDDLQueryIP() {
    return this.serverIp;
  }

  public int insertInsertExeInfo(Connection cc, InsertExeInfo insertInfo) {
    if (cc == null || insertInfo == null) {
      return -1;
    }
    int rt = -1;
    PreparedStatement pstmt;

    try {
      pstmt = cc
          .prepareStatement("insert into tdw_insert_info(queryid, desttable, successnum, rejectnum) values (?,?,?,?)");
      pstmt.setString(1, insertInfo.getQueryID());
      pstmt.setString(2, insertInfo.getDestTable());
      pstmt.setLong(3, insertInfo.getFsSuccessNum());
      pstmt.setLong(4, insertInfo.getFsRejectNum());

      rt = pstmt.executeUpdate();
    } catch (SQLException e) {
      LOG.error(" insert INSERT EXE Info failed: " + insertInfo.getQueryID()
          + "  " + insertInfo.getDestTable());
      e.printStackTrace();
    }
    return rt;
  }

  public int insertInsertExeInfo(Connection cc,
      Collection<InsertExeInfo> insertInfoList) {
    if (cc == null || insertInfoList == null || insertInfoList.isEmpty()) {
      return -1;
    }
    int rt = -1;
    PreparedStatement pstmt;
    String queryID = "";

    try {
      pstmt = cc
          .prepareStatement("insert into tdw_insert_info(queryid, desttable, successnum, rejectnum, ismultiinsert) values (?,?,?,?,?)");
      for (InsertExeInfo insertInfo : insertInfoList) {
        queryID = insertInfo.getQueryID();
        pstmt.setString(1, insertInfo.getQueryID());
        pstmt.setString(2, insertInfo.getDestTable());
        pstmt.setLong(3, insertInfo.getFsSuccessNum());
        pstmt.setLong(4, insertInfo.getFsRejectNum());
        pstmt.setBoolean(5, insertInfo.getIsMultiInsert());
        pstmt.addBatch();
      }

      pstmt.executeBatch();
      rt = 0;
    } catch (SQLException e) {
      LOG.error(" insert INSERT EXE Info failed: " + queryID);
      e.printStackTrace();
    }
    return rt;
  }

}
