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

package org.apache.hadoop.hive.service;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.protobuf.ProtobufSerDe;
import org.apache.hadoop.hive.service.HSSessionItem.jobType;

import org.apache.hadoop.hive.service.ThriftHive;
import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.DataImport;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.*;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobTracker;

import com.facebook.fb303.fb_status;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;


public class HiveServer extends ThriftHive {
  private final static String VERSION = "1";
  private static int port = 10000;

  private static HSSessionManager sessionManager;

  public static int clientLimit = 100000;

  private static Map<String, CommandProcessor> driverMap = new HashMap<String, CommandProcessor>();

  private static DataInOutPool dioPool;

  public static class HiveServerHandler extends HiveMetaStore.HMSHandler
      implements HiveInterface {
    public enum CheckingFlag {
      SESSION, AUDIT, SESSION_AUDIT, SESSION_KILL, ALL
    }

    private boolean audit;
    private TTransport trans;
    private String user;
    private String passwd;
    private String dbName;

    private HSSessionItem sessionItem;
    private HSSessionManager sm;

    private final ByteArrayOutputStream out, err;

    private Driver driver;

    private SessionState session;

    private boolean isHiveQuery;

    String fetchexecinfomode = null;

    public static final Log LOG = LogFactory.getLog(HiveServer.class.getName());
    public static final Log newlog = LogFactory.getLog("DayLog");
    private static final Exception HiveServerException = null;

    public HiveServerHandler() throws MetaException {
      super(HiveServer.class.getName());

      isHiveQuery = false;
      SessionState session = new SessionState(new HiveConf(SessionState.class));
      SessionState.start(session);
      session.in = null;
      out = new ByteArrayOutputStream();
      err = new ByteArrayOutputStream();
      session.out = new PrintStream(out);
      session.err = new PrintStream(err);
      sessionItem = null;
      audit = false;
      trans = null;
      sm = (sessionManager == null ? new HSSessionManager() : sessionManager);
      driver = new Driver();

      if (!sm.getGlobalNewJar().isEmpty()) {
        HiveServerHandler.LOG.info("X Have global jar candidate"
            + sm.getGlobalNewJar());
        String[] tokens = sm.getGlobalNewJar().split("\\s+");
        SessionState.ResourceType t = SessionState.find_resource_type("jar");
        if (t != null) {
          SessionState ss = SessionState.get();
          for (int i = 0; i < tokens.length; i++) {
            ss.add_resource(t, tokens[i]);
          }

          Set<String> cur = ss.list_resource(t, null);
          for (String res : cur) {
            HiveServerHandler.LOG.info("X have resource:" + res);
          }
        }
      }
    }

    public HiveServerHandler(TTransport trans) throws MetaException {
      super(HiveServer.class.getName());

      isHiveQuery = false;
      SessionState session = new SessionState(new HiveConf(SessionState.class));
      SessionState.start(session);
      session.in = null;
      out = new ByteArrayOutputStream();
      err = new ByteArrayOutputStream();
      session.out = new PrintStream(out);
      session.err = new PrintStream(err);
      sessionItem = null;
      audit = false;
      this.trans = trans;
      sm = sessionManager;
      driver = new Driver();
      if (!sm.getGlobalNewJar().isEmpty()) {
        HiveServerHandler.LOG.info("X Have global jar candidate"
            + sm.getGlobalNewJar());
        String[] tokens = sm.getGlobalNewJar().split("\\s+");
        SessionState.ResourceType t = SessionState.find_resource_type("jar");
        if (t != null) {
          SessionState ss = SessionState.get();
          for (int i = 0; i < tokens.length; i++) {
            ss.add_resource(t, tokens[i]);
          }

          Set<String> cur = ss.list_resource(t, null);
          for (String res : cur) {
            HiveServerHandler.LOG.info("X have resource:" + res);
          }
        }
      }
      fetchexecinfomode = SessionState.get().getConf().getVar(HiveConf.ConfVars.FETCH_EXEC_INFO_MODE);
    }

    public HiveServerHandler(TTransport trans, HSSessionManager sessionManager)
        throws MetaException {
      super(HiveServer.class.getName());

      isHiveQuery = false;
      SessionState session = new SessionState(new HiveConf(SessionState.class));
      SessionState.start(session);
      session.in = null;
      out = new ByteArrayOutputStream();
      err = new ByteArrayOutputStream();
      session.out = new PrintStream(out);
      session.err = new PrintStream(err);
      sessionItem = null;
      audit = false;
      this.trans = trans;
      sm = sessionManager;
      driver = new Driver();
      if (!sm.getGlobalNewJar().isEmpty()) {
        HiveServerHandler.LOG.info("X Have global jar candidate"
            + sm.getGlobalNewJar());
        String[] tokens = sm.getGlobalNewJar().split("\\s+");
        SessionState.ResourceType t = SessionState.find_resource_type("jar");
        if (t != null) {
          SessionState ss = SessionState.get();
          for (int i = 0; i < tokens.length; i++) {
            ss.add_resource(t, tokens[i]);
          }

          Set<String> cur = ss.list_resource(t, null);
          for (String res : cur) {
            HiveServerHandler.LOG.info("X have resource:" + res);
          }
        }
      }

    }

    private String genrandnum() {
      Calendar ccc = Calendar.getInstance();
      Date ddd = ccc.getTime();
      long now = ddd.getTime();
      now = now % 10000000000l;
      return String.valueOf(now);
    }

    public int getSQLProcess(String sqlID) throws HiveServerException {
      synchronized (driverMap) {
        if (driverMap == null)
          throw new HiveServerException("server uninit!");
        if (sqlID == null)
          throw new HiveServerException("invalid sql ID");
        CommandProcessor proc = driverMap.get(sqlID);
        if (proc == null)
          throw new HiveServerException("not exist sql");
        if (proc instanceof Driver) {
          Driver qp = (Driver) proc;
          int processing = qp.queryProcessCompute();
          try {

          } catch (Exception x) {
            LOG.error(x.getMessage());
          }
          return processing;
        } else
          return -1;
      }
    }

    public int killSQLQuery(String sqlID) throws HiveServerException {
      synchronized (driverMap) {
        if (driverMap == null)
          throw new HiveServerException("server uninit!");
        if (sqlID == null)
          throw new HiveServerException("invalid sql ID");
        CommandProcessor proc = driverMap.get(sqlID);
        if (proc == null)
          throw new HiveServerException("not exist sql");
        if (proc instanceof Driver) {
          Driver qp = (Driver) proc;
          try {
            qp.killCurrentJobs();
          } catch (IOException x) {
            throw new HiveServerException("kill Query SQL Job failed");
          }
          return 1;
        } else
          return -1;
      }
    }

    public void removeJdbcSQLJob(String sqlID) {
      if (sqlID == null)
        return;
      synchronized (driverMap) {
        try {
          driverMap.remove(sqlID);
        } catch (Exception x) {

        }
      }
    }

    public void addJdbcSQLJob(String sqlID, CommandProcessor proc) {
      if (sqlID == null || proc == null)
        return;
      synchronized (driverMap) {
        try {
          driverMap.put(sqlID, proc);
        } catch (Exception x) {

        }
      }
    }

    public String execute(String cmd) throws HiveServerException, TException {
      generic_checking(CheckingFlag.SESSION_KILL);
      SessionState ss = SessionState.get();
      LOG.info(ss.getSessionName() + " Thread Name: " + Thread.currentThread().getName());
      LOG.info(ss.getSessionName() + " Running the query: " + cmd);
      
      ss.ssLog(" Thread Name: " + Thread.currentThread().getName());
      ss.ssLog(" Running the query: " + cmd);

      if (ss != null) {
        LOG.info("user: " + ss.getUserName());
      }

      try {
        TSocket tSocket = (TSocket) trans;
        Socket socket = tSocket.getSocket();
        InetAddress addr = socket.getInetAddress();
        LOG.info(ss.getSessionName() + " client address: " + addr.getHostAddress() + " client port: " + socket.getPort());
      } catch (Exception e) {
        LOG.info(ss.getSessionName() + " get IP, PORT failed in create session");
      }

      int sqlLength = 0;
      sqlLength = cmd.length();
      int maxLength = ss.getConf()
          .getIntVar(HiveConf.ConfVars.HIVEMAXSQLLENGTH);
      if (maxLength > 100) {
        if (sqlLength > maxLength) {
          LOG.info(ss.getSessionName() + " Error: The SQL length is more than " + maxLength + ", exit");
          String errorMessage = "The SQL length is more than " + maxLength;
          throw new HiveServerException(errorMessage);
        }
      }

      int qid = 0;
      if (sessionItem != null) {
        qid = sessionItem.startQuery(cmd);
        ss.setiscli(false);
        ss.setSessionItemInFo(sessionItem.getSessionName(),
            sessionItem.getCurrnetQueryNum(), sessionItem.getCurrnetPrinter());
      }

      while (cmd.endsWith(";"))
        cmd = cmd.substring(0, cmd.length() - 1);

      String cmd_trimmed = cmd.trim();
      String[] tokens = cmd_trimmed.split("\\s+");
      String cmd_1 = cmd_trimmed.substring(tokens[0].length()).trim();

      String globaljars = "";
      if (tokens.length > 2 && tokens[0].toLowerCase().equals("add")
          && tokens[1].toLowerCase().equals("globaljar")) {
        globaljars = cmd_1.substring(tokens[1].length()).trim();
        cmd_1 = cmd_1.substring("global".length()).trim();
      }

      String res = "success";
      String errorMessage = "";
      ss.setUserName(user);

      if (sessionItem != null) {
        tdw_query_info tmpinfo = sessionItem.getQInfo();
        String tmpstr = tmpinfo.getQueryId();
        ss.setQueryIDForLog(tmpstr + "_" + genrandnum());
        LOG.info(ss.getSessionName() + " add query ID in execute: " + ss.getQueryIDForLog());
      }

      if (tokens[0].equalsIgnoreCase("delete")) {
        Vector<String> nexttoken = new Vector<String>();
        nexttoken.add("jar");
        nexttoken.add("file");
        nexttoken.add("from");
        if (tokens.length < 2 || !nexttoken.contains(tokens[1].toLowerCase())) {
          System.out.println("bb");
          errorMessage = "\nif delete resource:\n"
              + "Usage: delete [FILE|JAR] <value> [<value>]*\n"
              + "if delete table rows:\n"
              + "Usage: delete from tableName [where searchCondition]";
          throw new HiveServerException(errorMessage);
        }

      }
      
      if (tokens[0].equalsIgnoreCase("dfs") || tokens[0].equalsIgnoreCase("zktest")) {
        errorMessage = "\ntdw hive do not support " + tokens[0].toLowerCase() + " operation\n";  
        throw new HiveServerException(errorMessage);
      }

      int ret = 0;
      try {
        if (tokens[0].toLowerCase().equals("list")) {
          SessionState.ResourceType t;
          if (tokens.length < 2
              || (t = SessionState.find_resource_type(tokens[1])) == null) {
            throw new HiveServerException("Usage: list ["
                + StringUtils.join(SessionState.ResourceType.values(), "|")
                + "] [<value> [<value>]*]");
          } else {
            List<String> filter = null;
            if (tokens.length >= 3) {
              System.arraycopy(tokens, 2, tokens, 0, tokens.length - 2);
              filter = Arrays.asList(tokens);
            }
            Set<String> s = ss.list_resource(t, filter);
            if (s != null && !s.isEmpty())
              ss.out.println(StringUtils.join(s, "\n"));
          }
        } else {
          CommandProcessor proc = CommandProcessorFactory.get(tokens);
          if (proc != null) {
            if (proc instanceof Driver) {
              isHiveQuery = true;
              if (sessionItem != null) {
                tdw_query_info tmpinfo = sessionItem.getQInfo();
                String qstr = tmpinfo.getQueryString();
                if (qstr.length() > 9999) {
                  tmpinfo.setQueryString(qstr.substring(0, 9999));
                }
                tmpinfo.setBIQueryString(qstr);
                String tmpstr = tmpinfo.getQueryId();
                String tmpid = tmpstr + "_" + genrandnum();
                tmpinfo.setQueryId(tmpid);
                ss.setTdw_query_info(tmpinfo);
                
                tdw_query_error_info einfo = sessionItem.getEInfo();
                einfo.setQueryId(tmpid);
                ss.setTdw_query_error_info(einfo);
              }
              ((Driver) proc).setSocket(((TSocket) trans).getSocket());
              LOG.info(ss.getSessionName() + " begin process query cmd");
              ret = proc.run(cmd);
              LOG.info(ss.getSessionName() + " completed process query cmd");

              driver = (Driver) proc;
            } else {
              isHiveQuery = false;
              ret = proc.run(cmd_1);

            }
          }

          if (!globaljars.isEmpty()) {
            sm.addGlobalNewJar(globaljars);
          }
        }
      } catch (Exception e) {
        HiveServerException ex = new HiveServerException(
            "Error running query: " + sessionItem.removen(e.toString()));
        if (ss != null) {
          ss.out.flush();
          ss.err.flush();
        }
        out.reset();
        err.reset();
        if (sessionItem != null)
          sessionItem.endQuery(cmd, qid, ret,
              "Error running query: " + e.toString());
        ss.ssLog("Error running query: " + sessionItem.removen(e.toString()));
        throw ex;
      }

      if (ss != null) {
        ss.get().out.flush();
        ss.get().err.flush();
      }
      if (!isHiveQuery) {
        res = out.toString();
        res += err.toString();
      }
      if (ret != 0) {
        errorMessage = err.toString();
      }
      out.reset();
      err.reset();

      if (ret != 0) {
        if (sessionItem != null)
          sessionItem.endQuery(cmd, qid, ret, errorMessage);

        if (sessionItem != null)
          ss.ssLog(sessionItem.removen(errorMessage));
        throw new HiveServerException(sessionItem.removen(errorMessage));
      }
      if (sessionItem != null)
        sessionItem.endQuery(cmd, qid, ret, "");
      return res;
    }

    public String jdbc_execute(String cmd, String queryID)
        throws HiveServerException, TException {
      generic_checking(CheckingFlag.SESSION_KILL);

      SessionState ss = SessionState.get();
      
      LOG.info(ss.getSessionName() + " Thread Name: " + Thread.currentThread().getName());
      LOG.info(ss.getSessionName() + " jdbc_execute Running the query: " + cmd);
      
      LOG.info(ss.getSessionName() + " Query ID is " + queryID);
      
      ss.ssLog(" Thread Name: " + Thread.currentThread().getName());

      ss.setiscli(true);

      if (ss != null) {
        LOG.info("user: " + ss.getUserName());
      }

      try {
        TSocket tSocket = (TSocket) trans;
        Socket socket = tSocket.getSocket();
        InetAddress addr = socket.getInetAddress();
        LOG.info(ss.getSessionName() + " client address: " + addr.getHostAddress() + " client port: " + socket.getPort());
      } catch (Exception e) {
        LOG.info(ss.getSessionName() + " get IP, PORT failed in create session");
      }

      int sqlLength = 0;
      sqlLength = cmd.length();
      int maxLength = ss.getConf()
          .getIntVar(HiveConf.ConfVars.HIVEMAXSQLLENGTH);
      if (maxLength > 100) {
        if (sqlLength > maxLength) {
          LOG.info(ss.getSessionName() + " Error: The SQL length is more than " + maxLength + ", exit");
          String errorMessage = "The SQL length is more than " + maxLength;
          throw new HiveServerException(errorMessage);
        }
      }

      int qid = 0;
      if (sessionItem != null) {
        qid = sessionItem.startQuery(cmd);
        ss.setiscli(false);
        ss.setSessionItemInFo(sessionItem.getSessionName(),
            sessionItem.getCurrnetQueryNum(), sessionItem.getCurrnetPrinter());
      }

      while (cmd.endsWith(";"))
        cmd = cmd.substring(0, cmd.length() - 1);

      String cmd_trimmed = cmd.trim();

      String[] tokens = cmd_trimmed.split("\\s+");
      String cmd_1 = cmd_trimmed.substring(tokens[0].length()).trim();

      String globaljars = "";
      if (tokens.length > 2 && tokens[0].toLowerCase().equals("add")
          && tokens[1].toLowerCase().equals("globaljar")) {
        globaljars = cmd_1.substring(tokens[1].length()).trim();
        cmd_1 = cmd_1.substring("global".length()).trim();
      }

      String res = "success";
      String errorMessage = "";
      ss.setUserName(user);

      if (sessionItem != null) {
        tdw_query_info tmpinfo = sessionItem.getQInfo();
        String tmpstr = tmpinfo.getQueryId();
        ss.setQueryIDForLog(tmpstr + "_" + genrandnum());
        LOG.info(ss.getSessionName() + " add query ID in jdbc_execute" + ss.getQueryIDForLog());
      }

      if (tokens[0].equalsIgnoreCase("delete")) {
        Vector<String> nexttoken = new Vector<String>();
        nexttoken.add("jar");
        nexttoken.add("file");
        nexttoken.add("from");
        if (tokens.length < 2 || !nexttoken.contains(tokens[1].toLowerCase())) {
          System.out.println("bb");
          errorMessage = "\nif delete resource:\n"
              + "Usage: delete [FILE|JAR] <value> [<value>]*\n"
              + "if delete table rows:\n"
              + "Usage: delete from tableName [where searchCondition]";
          throw new HiveServerException(errorMessage);
        }

      }
      
      if (tokens[0].equalsIgnoreCase("dfs") || tokens[0].equalsIgnoreCase("zktest")) {
        errorMessage = "\ntdw hive do not support " + tokens[0].toLowerCase() + " operation\n";  
        throw new HiveServerException(errorMessage);
      }
      
      int ret = 0;
      try {
        if (tokens[0].toLowerCase().equals("list")) {
          SessionState.ResourceType t;
          if (tokens.length < 2
              || (t = SessionState.find_resource_type(tokens[1])) == null) {
            throw new HiveServerException("Usage: list ["
                + StringUtils.join(SessionState.ResourceType.values(), "|")
                + "] [<value> [<value>]*]");
          } else {
            List<String> filter = null;
            if (tokens.length >= 3) {
              System.arraycopy(tokens, 2, tokens, 0, tokens.length - 2);
              filter = Arrays.asList(tokens);
            }
            Set<String> s = ss.list_resource(t, filter);
            if (s != null && !s.isEmpty())
              ss.out.println(StringUtils.join(s, "\n"));
          }
        } else {
          CommandProcessor proc = CommandProcessorFactory.get(tokens);
          if (proc != null) {
            if (proc instanceof Driver) {
              addJdbcSQLJob(queryID, proc);

              if (sessionItem != null) {
                tdw_query_info tmpinfo = sessionItem.getQInfo();
                String qstr = tmpinfo.getQueryString();
                if (qstr.length() > 9999) {
                  tmpinfo.setQueryString(qstr.substring(0, 9999));
                }
                tmpinfo.setBIQueryString(qstr);
                String tmpstr = tmpinfo.getQueryId();
                String tmpid = tmpstr + "_" + genrandnum();
                tmpinfo.setQueryId(tmpid);
                ss.setTdw_query_info(tmpinfo);
                
                tdw_query_error_info einfo = sessionItem.getEInfo();
                einfo.setQueryId(tmpid);
                ss.setTdw_query_error_info(einfo);
              }
              isHiveQuery = true;

              ((Driver) proc).setSocket(((TSocket) trans).getSocket());
              LOG.info(ss.getSessionName() + " start process query cmd");
              ret = proc.run(cmd);
              LOG.info(ss.getSessionName() + " completed process query cmd");
              driver = (Driver) proc;
            } else {
              isHiveQuery = false;
              ret = proc.run(cmd_1);
            }
          }

          if (!globaljars.isEmpty()) {
            sm.addGlobalNewJar(globaljars);
          }
        }
      } catch (Exception e) {
        HiveServerException ex = new HiveServerException(
            "Error running query: " + e.toString());
        if (ss != null) {
          ss.out.flush();
          ss.err.flush();
        }
        out.reset();
        err.reset();

        removeJdbcSQLJob(queryID);

        if (sessionItem != null)
          sessionItem.endQuery(cmd, qid, ret,
              "Error running query: " + e.toString());

        throw ex;
      }

      if (ss != null) {
        ss.get().out.flush();
        ss.get().err.flush();
      }
      if (!isHiveQuery) {
        res = out.toString();
        res += err.toString();
      }
      if (ret != 0) {
        errorMessage = err.toString();
      }
      out.reset();
      err.reset();

      if (ret != 0) {
        removeJdbcSQLJob(queryID);
        if (sessionItem != null)
          sessionItem.endQuery(cmd, qid, ret, errorMessage);
        ss.ssLog(errorMessage);
        throw new HiveServerException("Query w/ errno: " + ret + " "
            + errorMessage);
      }

      removeJdbcSQLJob(queryID);
      if (sessionItem != null)
        sessionItem.endQuery(cmd, qid, ret, "");
      return res;
    }

    public HiveClusterStatus getClusterStatus() throws HiveServerException,
        TException {
      generic_checking(CheckingFlag.SESSION);
      HiveClusterStatus hcs;
      try {
        ClusterStatus cs = driver.getClusterStatus();
        JobTracker.State jbs = cs.getJobTrackerState();

        JobTrackerState state;
        switch (jbs) {
        case INITIALIZING:
          state = JobTrackerState.INITIALIZING;
          break;
        case RUNNING:
          state = JobTrackerState.RUNNING;
          break;
        default:
          String errorMsg = "Unrecognized JobTracker state: " + jbs.toString();
          throw new Exception(errorMsg);
        }

        hcs = new HiveClusterStatus(cs.getTaskTrackers(), cs.getMapTasks(),
            cs.getReduceTasks(), cs.getMaxMapTasks(), cs.getMaxReduceTasks(),
            state);
      } catch (Exception e) {
        LOG.error(e.toString());
        e.printStackTrace();
        throw new HiveServerException("Unable to get cluster status: "
            + e.toString());
      }
      return hcs;
    }

    public Schema getSchema() throws HiveServerException, TException {
      generic_checking(CheckingFlag.SESSION);
      if (!isHiveQuery)
        return new Schema();

      try {
        Schema schema = driver.getSchema();
        if (schema == null) {
          schema = new Schema();
        }
        LOG.info("Returning schema: " + schema.toString());
        return schema;
      } catch (Exception e) {
        LOG.error(e.toString());
        e.printStackTrace();
        throw new HiveServerException("Unable to get schema: " + e.toString());
      }
    }

    public Schema getThriftSchema() throws HiveServerException, TException {
      generic_checking(CheckingFlag.SESSION);
      if (!isHiveQuery)
        return new Schema();

      try {
        Schema schema = driver.getThriftSchema();
        if (schema == null) {
          schema = new Schema();
        }
        LOG.info("Returning schema: " + schema);
        return schema;
      } catch (Exception e) {
        LOG.error(e.toString());
        e.printStackTrace();
        throw new HiveServerException("Unable to get schema: " + e.toString());
      }
    }

    public String fetchOne() throws HiveServerException, TException {
      generic_checking(CheckingFlag.SESSION);
      if (!isHiveQuery)
        return "";

      Vector<String> result = new Vector<String>();
      driver.setMaxRows(1);
      try {
        if (driver.getResults(result)) {
          return result.get(0);
        }
        return "";
      } catch (IOException e) {
        driver.close();
        throw new HiveServerException(e.getMessage());
      }
    }

    public List<String> fetchN(int numRows) throws HiveServerException,
        TException {
      generic_checking(CheckingFlag.SESSION);
      if (numRows < 0) {
        throw new HiveServerException("Invalid argument for number of rows: "
            + numRows);
      }
      if (!isHiveQuery)
        return new Vector<String>();

      Vector<String> result = new Vector<String>();
      driver.setMaxRows(numRows);
      try {
        driver.getResults(result);
      } catch (IOException e) {
        driver.close();
        throw new HiveServerException(e.getMessage());
      }
      return result;
    }

    public List<String> fetchAll() throws HiveServerException, TException {
      fetchexecinfomode = SessionState.get().getConf().getVar(HiveConf.ConfVars.FETCH_EXEC_INFO_MODE);
      int fetchlimit = SessionState.get().getConf()
          .getInt("select.max.limit", 10000);
      if (fetchlimit >= 100 * 10000) {
        driver.close();
        throw new HiveServerException(
            "you use fetchAll interface!  and set select.max.limit = "
                + fetchlimit
                + ", "
                + "which is too large, please run the cmd \" set select.max.limit=[maxnum]\", and maxnum must be smaller than 1,000,000 or use fetchN interface");
      }
      generic_checking(CheckingFlag.SESSION);

      Vector<String> rows = new Vector<String>();
      Vector<String> result = new Vector<String>();
      if (isHiveQuery) {
        try {
          while (driver.getResults(result)) {
            int size = rows.size() + result.size();
            if (size > fetchlimit) {
              result.clear();
              break;
            }
            rows.addAll(result);
            result.clear();
          }
        } catch (IOException e) {
          driver.close();
          throw new HiveServerException(e.getMessage());
        }
      }
      if (fetchexecinfomode.equals("all")) {
        StringBuffer sb = new StringBuffer();
        if (driver.execinfos.size() > 0) {
          for (Task.ExecResInfo info : driver.execinfos) {
            if (info != null && info.taskid != null) {
              sb.append(info.taskid + "\n");
              sb.append("\t" + info.in_success + "\n");
              sb.append("\t" + info.in_error + "\n");
              sb.append("\t" + info.out_success + "\n");
              sb.append("\t" + info.out_error + "\n");
            }
          }
          rows.add(sb.toString());
          LOG.info(sb.toString());
          SessionState.get().ssLog(sb.toString());
        }
      } else if (fetchexecinfomode.equals("part")) {
        StringBuffer sb = new StringBuffer();
        if (driver.execinfos.size() > 0 && driver.execinfos.get(0) != null
            && driver.execinfos.get(0).taskid != null) {
          Task.ExecResInfo info1 = driver.execinfos.get(0);
          Task.ExecResInfo info2 = driver.execinfos
              .get(driver.execinfos.size() - 1);
          sb.append(info1.taskid + "\n");
          sb.append("\t" + info1.in_success + "\n");
          sb.append("\t" + info1.in_error + "\n");
          sb.append(info2.taskid + "\n");
          sb.append("\t" + info2.out_success + "\n");
          sb.append("\t" + info2.out_error + "\n");
          rows.add(sb.toString());
          LOG.info(sb.toString());
          SessionState.get().ssLog(sb.toString());
        }
      }

      driver.close();
      return rows;
    }

    @Override
    public int getStatus() {
      return 0;
    }

    @Override
    public String getVersion() {
      return VERSION;
    }

    public void setSessionItem(HSSessionItem sessionItem) {
      this.sessionItem = sessionItem;
    }

    public HSSessionItem getSessionItem() {
      return sessionItem;
    }

    @Override
    public List<String> createSession(String sessionName)
        throws HiveServerException, TException {
      List<String> l = new ArrayList<String>();
      String name;
      String errorMsg;

      generic_checking(CheckingFlag.AUDIT);
      if (sessionItem != null) {
        errorMsg = "You already connected to a session: "
            + sessionItem.toString();
        throw new HiveServerException(errorMsg);
      }
      HSAuth auth = new HSAuth(getUser(), getPasswd(), getDbName(), port);
      auth.setAuthid(Double.toString(Math.random()));
      if (sessionName == null || sessionName.equals("")) {
        name = Double.toString(Math.random()).substring(2);
      } else {
        name = sessionName;
      }
      try {
        sessionItem = new HSSessionItem(auth, name, trans);
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      }
      if (!sm.register(sessionItem)) {
        errorMsg = "Register Session: " + name + " failed, name conflicts.";
        sessionItem = null;
        throw new HiveServerException(errorMsg);
      }
      String session_user = "the one";
      if (sessionItem != null) {
        session_user = sessionItem.getAuth().getUser();
      }
      try {
        TSocket tSocket = (TSocket) trans;
        Socket socket = tSocket.getSocket();
        InetAddress addr = socket.getInetAddress();
        HiveServerHandler.LOG.info("Create a new Session " + name + ": "
            + "user: " + session_user + "| IP: " + addr.getHostAddress()
            + "| PORT: " + socket.getPort() + "| TIME: "
            + sessionItem.getDate().toLocaleString());
      } catch (Exception e) {
        LOG.info("get IP, PORT failed in create session");
      }

      l.add(sessionItem.getSessionName());

      if (sessionItem != null) {
        l.add(sessionItem.getAuth().toString());
      }

      return l;
    }

    @Override
    public int dropSession(String sid, String svid) throws HiveServerException,
        TException {
      generic_checking(CheckingFlag.SESSION);
      if (sessionItem == null) {
        HiveServerHandler.LOG
            .info("Unconnected clients could not do any dropSession operations.");
        return -1;
      }

      sessionItem.removeTrans(trans);
      if (sessionItem.isActive()) {
        HiveServerHandler.LOG
            .info("Another client attached to this session either, you cant discard it!");
        sessionItem = null;
        return -1;
      }
      configJob("type=ONESHOT;ti=0;");
      sessionItem.killIt();
      sm.unregister(sessionItem);
      sessionItem = null;

      return 0;
    }

    @Override
    public String requireSession(String sid, String svid)
        throws HiveServerException, TException {
      String logstr;
      generic_checking(CheckingFlag.AUDIT);
      if (sessionItem != null) {
        HiveServerHandler.LOG.info("You have already connected to a Session: "
            + sessionItem.getSessionName());
        return "";
      }
      sessionItem = sm.lookup(sid, this.user);
      if (sessionItem == null) {
        logstr = "require: " + sid + ":" + svid + " failed.";
        HiveServerHandler.LOG.info(logstr);
        return "";
      } else if (sessionItem.audit(user, passwd)) {
        logstr = "require: " + sid + ":" + svid + " granted.";
        HiveServerHandler.LOG.info(logstr);
        sessionItem.addTrans(trans);
        return sessionItem.getSessionName();
      } else {
        logstr = "require: " + sid + ":" + svid + " failed.";
        HiveServerHandler.LOG.info(logstr);
        String msg = "Bad user name or password for session "
            + sessionItem.getSessionName();
        sessionItem = null;
        throw new HiveServerException(msg);
      }
    }

    @Override
    public int detachSession(String sid, String svid)
        throws HiveServerException, TException {
      generic_checking(CheckingFlag.SESSION);
      if (sessionItem != null) {
        sessionItem.removeTrans(trans);
        sessionItem = null;
      }
      return 0;
    }

    public void generic_checking(CheckingFlag flag) throws HiveServerException,
        TException {

      if (sessionItem != null) {
        sessionItem.setOpdate();
      }

    }

    @Override
    public List<String> getEnv() throws HiveServerException, TException {
      return null;
    }

    @Override
    public List<String> getJobStatus(int jobid) throws HiveServerException,
        TException {
      generic_checking(CheckingFlag.SESSION);
      return sessionItem.getJobResult(jobid);
    }

    @Override
    public List<String> showSessions() throws HiveServerException, TException {
      generic_checking(CheckingFlag.AUDIT);
      return sm.showSessions(sessionItem);
    }

    @Override
    public int uploadJob(String job) throws HiveServerException, TException {
      generic_checking(CheckingFlag.SESSION);
      if (sessionItem.submitJob("python", job)) {
        HiveServerHandler.LOG.info("Submit job succeed.");
      } else {
        String msg = "Submit Job failed, another job is running or files failed to open.";
        throw new HiveServerException(msg);
      }

      sessionItem.runJob();

      return 0;
    }

    public boolean isAudit() {
      return audit;
    }

    public void setAudit(boolean audit) {
      this.audit = audit;
    }

    public void setUser(String user) {
      this.user = user;
    }

    public String getUser() {
      return user;
    }

    public void setPasswd(String passwd) {
      this.passwd = passwd;
    }

    public String getPasswd() {
      return passwd;
    }

    public void setDbName(String dbName) {
      this.dbName = dbName;
    }

    public String getDbName() {
      return dbName;
    }

    @Override
    public int audit(String user, String passwd, String dbName)
        throws HiveServerException, TException {
      Hive hive;
      boolean res = false;
      boolean dbexists = false;

      SessionState ss = SessionState.get();
      HiveConf conf = SessionState.get().getConf();
      if (HiveConf
          .getBoolVar(conf, HiveConf.ConfVars.HIVECLIENTCONNECTIONLIMIT)) {
        if (ss != null) {
          clientLimit = ss.getConf().getIntVar(
              HiveConf.ConfVars.HIVECLIENTCONNECTIONSLIMITNUMBER);
        }
        LOG.info("clientLimit: " + clientLimit);
        if (clientLimit < 1) {
          clientLimit = 1;
        }
        HSSessionManager sessionManager = HiveServer.sessionManager;
        if (sessionManager != null) {
          LOG.info("getSessionNum: " + sessionManager.getSessionNum());

          if (sessionManager.getSessionNum() >= clientLimit) {
            LOG.info("to much connection, please retry");
            throw new HiveServerException(
                "to much connection, please wait a minitue and retry");
          }
        }
      }

      try {
        hive = Hive.get();
      } catch (Exception e) {
        setAudit(false);
        LOG.error(e.toString());
        e.printStackTrace();
        throw new HiveServerException("Hive.get() in audit failed: " + e);
      }

      try {
        res = hive.isAUser(user, passwd);
      } catch (Exception e) {
        setAudit(false);
        LOG.error(e.toString());
        e.printStackTrace();
        throw new HiveServerException("isAUser in audit failed: " + e);
      }

      try {
        dbexists = (hive.getDatabase(dbName.toLowerCase()) != null);
      } catch (NoSuchObjectException e) {
        throw new HiveServerException("Auth returned: " + e.getMessage());
      } catch (MetaException e) {
        throw new HiveServerException("Auth returned: " + e.getMessage());
      } catch (Exception e) {
        throw new HiveServerException("Auth returned: " + e.getMessage());
      }

      if (res && dbexists) {
        setAudit(true);
        setDbName(dbName);
        ss.setDbName(dbName);
        setUser(user);
        setPasswd(passwd);

        ss.setPasswd(passwd);

        return 0;
      } else {
        setAudit(false);
        return -1;
      }
    }

    @Override
    public int configJob(String config) throws HiveServerException, TException {
      int idx, end;
      String type, typeValue, ti, tiValue;

      generic_checking(CheckingFlag.SESSION);
      idx = config.indexOf("type=") + 5;
      if (idx != 4) {
        type = config.substring(idx);

        end = type.indexOf(';');
        if (end == -1) {
          typeValue = sessionItem.getConfigJob().toString();
        } else {
          typeValue = type.substring(0, end);
        }
      } else {
        typeValue = sessionItem.getConfigJob().toString();
      }

      idx = config.indexOf("ti=") + 3;
      if (idx != 2) {
        ti = config.substring(idx);
        end = ti.indexOf(';');
        if (end == -1) {
          tiValue = Long.toString(sessionItem.getConfigJobTi());
        } else {
          tiValue = ti.substring(0, end);
        }
      } else {
        tiValue = Long.toString(sessionItem.getConfigJobTi());
      }

      HiveServerHandler.LOG.info("Got type=" + typeValue + "; ti=" + tiValue);

      jobType jType;

      if (typeValue.equals(jobType.ONESHOT.toString())) {
        jType = jobType.ONESHOT;
      } else if (typeValue.equals(jobType.REPEAT.toString())) {
        jType = jobType.REPEAT;
      } else {
        jType = jobType.ONESHOT;
      }

      sessionItem.configJob(jType, Integer.parseInt(tiValue));
      return 0;
    }

    @Override
    public int killJob() throws HiveServerException, TException {
      generic_checking(CheckingFlag.SESSION);
      sessionItem.killIt();
      return 0;
    }

    @Override
    public void setHistory(String sid, int jobid) throws HiveServerException,
        TException {
      generic_checking(CheckingFlag.SESSION);
      String infoFile = "TDW_PL_JOB_" + sessionItem.getSessionName() + "_"
          + Integer.toString(jobid);
      File infof = new File(sessionItem.getHome() + "/pl/" + infoFile + "_info");

      try {
        FileOutputStream fos = new FileOutputStream(infof);
        fos.write('\n');
        fos.close();
      } catch (java.io.FileNotFoundException fex) {
        HiveServerHandler.LOG.info("File Not found ", fex);
      } catch (java.io.IOException ex) {
        HiveServerHandler.LOG.info("IO exception", ex);
      }
    }

    @Override
    public String getHistory(int jobid) throws HiveServerException, TException {
      generic_checking(CheckingFlag.SESSION);
      return sessionItem.getHiveHistroy(jobid);
    }

    @Override
    public String compile(String cmd) throws HiveServerException, TException {
      generic_checking(CheckingFlag.SESSION);
      HiveServerHandler.LOG.info("Compile the query: " + cmd);
      SessionState ss = SessionState.get();

      ss.setCheck(true);
      ss.setUserName(user);
      if (sessionItem != null)
        ss.setUserName(sessionItem.getAuth().getUser());

      String cmd_trimmed = cmd.trim();
      String[] tokens = cmd_trimmed.split("\\s+");
      String errorMessage = "";
      String res = "success";

      if (tokens[0].equalsIgnoreCase("delete")) {
        Vector<String> nexttoken = new Vector<String>();
        nexttoken.add("jar");
        nexttoken.add("file");
        nexttoken.add("from");
        if (tokens.length < 2 || !nexttoken.contains(tokens[1].toLowerCase())) {
          System.out.println("bb");
          errorMessage = "\nif delete resource:\n"
              + "Usage: delete [FILE|JAR] <value> [<value>]*\n"
              + "if delete table rows:\n"
              + "Usage: delete from tableName [where searchCondition]";
          throw new HiveServerException(errorMessage);
        }

      }
      
      if (tokens[0].equalsIgnoreCase("dfs") || tokens[0].equalsIgnoreCase("zktest")) {
        errorMessage = "\ntdw hive do not support " + tokens[0].toLowerCase() + " operation\n";  
        throw new HiveServerException(errorMessage);
      }
      
      int ret = 0;

      try {
        CommandProcessor proc = CommandProcessorFactory.get(tokens);
        if (proc != null) {
          if (proc instanceof Driver) {
            isHiveQuery = true;
            ret = proc.run(cmd);
          } else {
            isHiveQuery = false;
            ret = -1;
            errorMessage = " This is not a valid SQL statement.";
          }
        }
      } catch (Exception e) {
        HiveServerException ex = new HiveServerException();
        ex.setMessage("Error compiling query: " + e.toString());
        SessionState.get().out.flush();
        SessionState.get().err.flush();
        out.reset();
        err.reset();
        ss.setCheck(false);
        throw ex;
      }

      SessionState.get().out.flush();
      SessionState.get().err.flush();
      ss.setCheck(false);

      if (ret != 0) {
        errorMessage = err.toString();
      }
      out.reset();
      err.reset();

      if (ret != 0) {
        throw new HiveServerException("Compile w/ errno: " + ret + " "
            + errorMessage);
      }
      return res;
    }

    @Override
    public ByteBuffer downloadModule(String user, String moduleName)
        throws HiveServerException, TException {
      generic_checking(CheckingFlag.SESSION);

      if (moduleName.contains("..")) {
        throw new HiveServerException(
            "Error, you can not issue moduleName contains '..'");
      }
      return ByteBuffer.wrap(sessionItem.downloadModule(user, moduleName));
    }

    @Override
    public List<String> listModule(String user) throws HiveServerException,
        TException {
      generic_checking(CheckingFlag.SESSION);

      return sessionItem.listModule(user);
    }

    public String upload(String rtype, String user, String fileName, String data)
        throws HiveServerException, TException {
      String res = "ok";

      generic_checking(CheckingFlag.SESSION);

      if (fileName.contains("..")) {
        throw new HiveServerException(
            "Error, you can not issue fileName contains '..'");
      }
      if (!getUser().equalsIgnoreCase("root")
          && !getUser().equalsIgnoreCase(user)) {
        throw new HiveServerException("Error, you can only access your ("
            + getUser() + ") own " + rtype + " file.");
      }
      if (!getUser().equalsIgnoreCase("root")) {
        user = getUser();
      }

      if (sessionItem.upload(rtype, user, fileName, data) == false) {
        throw new HiveServerException("Upload file " + fileName + " failed.");
      }

      return res;
    }

    public String makejar(String dbName, String tblName, String fileName,
        String userName) throws HiveServerException, TException {
      String res = "ok";

      generic_checking(CheckingFlag.SESSION);

      if (fileName.contains("..")) {
        throw new HiveServerException(
            "Error, you can not issue fileName contains '..'");
      }

      if (!getUser().equalsIgnoreCase("root")
          && !getUser().equalsIgnoreCase(user)) {
        throw new HiveServerException("Error, you can only access your ("
            + getUser() + ") own proto file.");
      }
      if (!getUser().equalsIgnoreCase("root")) {
        user = getUser();
      }

      res = sessionItem.makejar(dbName, tblName, fileName, userName);

      return res;
    }

    public String preproto(String dbName, String tblName, String fileName,
        String userName) throws HiveServerException, TException {
      String res = "ok";

      generic_checking(CheckingFlag.SESSION);

      if (fileName.contains("..")) {
        throw new HiveServerException(
            "Error, you can not issue fileName contains '..'");
      }

      if (!getUser().equalsIgnoreCase("root")
          && !getUser().equalsIgnoreCase(user)) {
        throw new HiveServerException("Error, you can only access your ("
            + getUser() + ") own proto file.");
      }
      if (!getUser().equalsIgnoreCase("root")) {
        user = getUser();
      }

      res = sessionItem.preproto(dbName, tblName, fileName, userName);

      return res;
    }

    public String genjar(String dbName, String tblName, String fileName)
        throws HiveServerException, TException {
      String res = "ok";

      generic_checking(CheckingFlag.SESSION);

      res = sessionItem.genjar(dbName, tblName, fileName);
      return res;
    }

    public String uploadModule(String user, String moduleName, byte[] module)
        throws HiveServerException, TException {
      String res = "ok";

      generic_checking(CheckingFlag.SESSION);

      if (moduleName.contains("..")) {
        throw new HiveServerException(
            "Error, you can not issue moduleName contains '..'");
      }
      if (!getUser().equalsIgnoreCase("root")
          && !getUser().equalsIgnoreCase(user)) {
        throw new HiveServerException("Error, you can only access your ("
            + getUser() + ") own library.");
      }
      if (!getUser().equalsIgnoreCase("root")) {
        user = getUser();
      }

      if (sessionItem.uploadModule(user, moduleName, module) == false) {
        throw new HiveServerException("Upload module " + moduleName
            + " failed.");
      }

      return res;
    }

    @Override
    public String getRowCount() throws HiveServerException, TException {
      String res = "0";
      return res;
    }

    @Override
    public void clean() throws TException {
      if (driver != null)
        driver.close();
    }

    public int dataSync(String createSQL, String tableName, String data)
        throws HiveServerException, TException {
      HiveConf conf = SessionState.get().getConf();
      Properties p = conf.getAllProperties();
      execute(createSQL);

      String hdfsDefalutName = null;
      hdfsDefalutName = p.getProperty("fs.defaultFS");
      if(hdfsDefalutName == null){
        hdfsDefalutName = p.getProperty("fs.default.name");
      }
      String warehauseDir = p.getProperty("hive.metastore.warehouse.dir");
      String saveDir = hdfsDefalutName + warehauseDir + "//default_db//"
          + tableName;

      String hadoopURL = saveDir + "//data.payniexiao";

      Configuration config = new Configuration();
      conf.set("fs.default.name", hdfsDefalutName);

      try {
        FileSystem fs = FileSystem.get(config);
        Path f = new Path(hadoopURL);
        FSDataOutputStream os = fs.create(f, true);
        os.write(data.getBytes());
        os.close();
      } catch (IOException x) {
        x.printStackTrace();
      }

      return 0;
    }

    public int dataSync(String createSQL, String tableName, String data,
        String partName, String subPartName, int mode)
        throws HiveServerException, TException {

      HiveConf conf = SessionState.get().getConf();
      Properties p = conf.getAllProperties();

      String dbDirName = SessionState.get().getDbName();
      if (!dbDirName.equalsIgnoreCase("default_db")) {
        dbDirName += ".db";
      }

      //String hdfsDefalutName = p.getProperty("fs.default.name");
      
      String hdfsDefalutName = null;
      hdfsDefalutName = p.getProperty("fs.defaultFS");
      if(hdfsDefalutName == null){
        hdfsDefalutName = p.getProperty("fs.default.name");
      }
      
      String warehauseDir = p.getProperty("hive.metastore.warehouse.dir");
      String tableSaveDir = hdfsDefalutName + warehauseDir + "//" + dbDirName
          + "//" + tableName;

      String fileSaveDir = tableSaveDir;
      String fileSavePath = tableSaveDir;

      String timerStr = new SimpleDateFormat("yyyyMMddHHmmssSSS")
          .format(new Date());
      String fileName = "data_" + timerStr;

      System.out.println(p.getProperty("hive.exec.scratchdir"));

      if (partName == null && subPartName == null) {
        fileSavePath += "//" + fileName;
      } else if (partName != null && subPartName == null) {
        fileSaveDir += "//" + partName;
        fileSavePath = fileSaveDir + "//" + fileName;
      } else if (partName != null && subPartName != null) {
        fileSaveDir += "//" + partName + "//" + subPartName;
        fileSavePath = fileSaveDir + "//" + fileName;
      } else {
        throw new HiveServerException("partion parameter error!!");
      }

      Configuration config = new Configuration();

      config.set("fs.default.name", hdfsDefalutName);
      config.set("hadoop.job.ugi", "tdwadmin,supergroup");

      try {
        System.out.println("save path is " + fileSavePath);
        System.out.println(createSQL);
        System.out.println(tableName);

        FileSystem fs = FileSystem.get(config);
        Path f = new Path(fileSavePath);
        FSDataOutputStream os = fs.create(f, true);
        os.write(data.getBytes());
        os.close();
      } catch (IOException x) {
        x.printStackTrace();
      }

      return 0;
    }

    public String getLoader(String destDB, String destTable, int mode)
        throws HiveServerException, TException {
      SessionState loaderSession = SessionState.get();
      loaderSession.ssLog("data import start");

      HiveConf conf = loaderSession.getConf();

      if (dioPool == null)
        throw new HiveServerException("internal error!!");

      DataImport dataImport = dioPool.getInstance();
      if (dataImport == null) {
        throw new HiveServerException(
            "system is busy, please try again one minute later!!");
      }

      dataImport.setDIState(DataImport.DataImportState.INIT);

      dataImport.serDBName(loaderSession.getDbName());
      dataImport.setUserName(loaderSession.getUserName());
      dataImport.setTableName(destTable);

      String loaderID = "data_loader"
          + new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date());
      dataImport.setLoaderID(loaderID);

      org.apache.hadoop.hive.ql.metadata.Table tbl = null;
      try {
        tbl = SessionState.get().getDb()
            .getTable(loaderSession.getDbName(), destTable);
      } catch (HiveException e) {
        throw new HiveServerException("can not find table:"
            + loaderSession.getDbName() + "/" + destTable);
      }

      if (tbl == null) {
        throw new HiveServerException("can not find table:"
            + loaderSession.getDbName() + "/" + destTable);
      }

      boolean isPartedTable = false;
      boolean isFormatTable = false;
      boolean isRCFileTable = false;
      if (tbl.isPartitioned()) {
        isPartedTable = true;
        dataImport.setIsPart(true);
      }

      if (tbl.getTTable().getParameters() != null
          && tbl.getTTable().getParameters().get("type") != null
          && tbl.getTTable().getParameters().get("type")
              .equalsIgnoreCase("format")) {
        isFormatTable = true;
        dataImport.setIsFormated(true);
      }

      if (tbl.getTTable().getParameters() != null
          && tbl.getTTable().getParameters().get("type") != null
          && tbl.getTTable().getParameters().get("type")
              .equalsIgnoreCase("rcfile")) {
        isRCFileTable = true;
        dataImport.setIsRCFile(true);
      }

      SessionState.get().setDataImport(dataImport);

      try {
        if (!isPartedTable && !isFormatTable && !isRCFileTable) {
          conf = null;
          conf = SessionState.get().getConf();

          Properties p = conf.getAllProperties();

          p.list(System.out);

          String saveDir = tbl.getTTable().getSd().getLocation();

          Database db = SessionState.get().getDb()
              .getDatabase(loaderSession.getDbName());

          if (db == null) {
            throw new HiveServerException("can not find db:"
                + loaderSession.getDbName());
          }

          String hdfsDefalutName = db.getHdfsscheme();

          if (hdfsDefalutName == null) {
            //hdfsDefalutName = p.getProperty("fs.default.name");
            
            hdfsDefalutName = p.getProperty("fs.defaultFS");
            if(hdfsDefalutName == null){
            hdfsDefalutName = p.getProperty("fs.default.name");
          }
          }

          String hadoopURL = saveDir + "//data_payniexiao"
              + new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date());

          LOG.info("text table file url is " + hadoopURL);

          Configuration config = new Configuration();
          config.set("hadoop.job.ugi", "tdwadmin,supergroup");

          config.set("fs.default.name", hdfsDefalutName);

          try {
            FileSystem fs = FileSystem.get(config);

            Path f = new Path(hadoopURL);
            FSDataOutputStream os = fs.create(f, true);
            dataImport.setTableOs(os);
            LOG.debug("for general table, hdfs path:" + hadoopURL);
          } catch (IOException x) {
            dataImport.setDIState(DataImport.DataImportState.ERROR);
            dioPool.releaseInstance(dataImport);
            throw new HiveServerException("can not open file: " + hadoopURL);
          }
        } else {
          conf = null;
          conf = SessionState.get().getConf();

          Properties p = conf.getAllProperties();

          p.list(System.out);

          Random rd = new Random();

          String tempTableName = "temp_table_payniexiao"
              + new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date())
              + rd.nextInt(1000000);
          ;
          dataImport.setTempTableName(tempTableName);
          List<FieldSchema> fields = get_fields_jdbc(SessionState.get()
              .getDbName(), destTable);

          String columnList = new String();

          if (fields != null && fields.size() == 0) {
            dataImport.setDIState(DataImport.DataImportState.ERROR);
            dioPool.releaseInstance(dataImport);
            throw new HiveServerException(
                "can not get columns information of table:" + destTable);
          }

          int fieldNum = fields.size();
          for (int i = 0; i < fieldNum; i++) {
            FieldSchema field = fields.get(i);
            if (i != fieldNum - 1) {
              columnList += field.getName() + " " + field.getType() + ",";
            } else {
              columnList += field.getName() + " " + field.getType();
            }
          }

          String tempTableDDL = "CREATE TABLE " + tempTableName + "("
              + columnList + ")" + " stored as textfile";
          LOG.debug("for data import, temp ddl is " + tempTableDDL);

          try {
            execute(tempTableDDL);
          } catch (Exception x) {
            x.printStackTrace();
          }

          org.apache.hadoop.hive.ql.metadata.Table tmptbl = null;
          try {
            tmptbl = SessionState.get().getDb()
                .getTable(loaderSession.getDbName(), tempTableName);
          } catch (HiveException e) {
            throw new HiveServerException("can not find tmp table:"
                + loaderSession.getDbName() + "/" + tempTableName);
          }

          if (tmptbl == null) {
            throw new HiveServerException("can not find tmp table:"
                + loaderSession.getDbName() + "/" + tempTableName);
          }

          String saveDir = tmptbl.getTTable().getSd().getLocation();
          String hadoopURL = saveDir + "//data_payniexiao"
              + new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date());

          Database db = SessionState.get().getDb()
              .getDatabase(loaderSession.getDbName());

          if (db == null) {
            throw new HiveServerException("can not find db:"
                + loaderSession.getDbName());
          }

          String hdfsDefalutName = db.getHdfsscheme();

          if (hdfsDefalutName == null) {
            //hdfsDefalutName = p.getProperty("fs.default.name");
            //String hdfsDefalutName = null;
            hdfsDefalutName = p.getProperty("fs.defaultFS");
            if(hdfsDefalutName == null){
            hdfsDefalutName = p.getProperty("fs.default.name");
          }
          }

          Configuration config = new Configuration();
          config.set("fs.default.name", hdfsDefalutName);

          try {
            FileSystem fs = FileSystem.get(config);

            Path f = new Path(hadoopURL);
            FSDataOutputStream os = fs.create(f, true);
            dataImport.setTempTableOs(os);
            LOG.debug("for data import temp table, hdfs path:" + hadoopURL);
          } catch (IOException x) {
            x.printStackTrace();
            if (dataImport.getTempTableName() != null) {
              LOG.error("ERROR1 " + dataImport.getTempTableName());
              execute("drop table " + dataImport.getTempTableName());
              dataImport.setTempTableName(null);
            }

            dataImport.setDIState(DataImport.DataImportState.ERROR);
            dioPool.releaseInstance(dataImport);
            throw new HiveServerException("can not open file: " + hadoopURL);
          }
        }
      } catch (Exception x) {
        x.printStackTrace();
        if (dataImport.getTempTableName() != null) {
          execute("drop table " + dataImport.getTempTableName());
          dataImport.setTempTableName(null);
        }

        dataImport.setDIState(DataImport.DataImportState.ERROR);
        dioPool.releaseInstance(dataImport);
        throw new HiveServerException(x.getMessage());
      }

      return loaderID;
    }

    public int releaseLoader(String loaderID) throws HiveServerException,
        TException {
      DataImport di = SessionState.get().getDataImport();
      if (di != null) {
        if (di.getTempTableName() != null) {
          String sql = "drop table " + di.getTempTableName();
          try {
            execute(sql);
            di.setTempTableName(null);
          } catch (Exception x) {
            x.printStackTrace();
          }
        }
        dioPool.releaseInstance(di);
      }

      return 0;
    }

    public int uploadData(String loaderID, String data)
        throws HiveServerException, TException {
      DataImport di = SessionState.get().getDataImport();
      if (di != null) {
        if (data == null) {
          if (di.getTempTableName() != null) {
            execute("drop table " + di.getTempTableName());
            di.setTempTableName(null);
          }

          di.setDIState(DataImport.DataImportState.ERROR);
          dioPool.releaseInstance(di);
          throw new HiveServerException(
              "unvalid parameters! data buffer can not be null");
        }

        if (di.getDIState() != DataImport.DataImportState.UPLOADING
            && di.getDIState() != DataImport.DataImportState.INIT) {
          if (di.getTempTableName() != null) {
            execute("drop table " + di.getTempTableName());
            di.setTempTableName(null);
          }

          di.setDIState(DataImport.DataImportState.ERROR);
          dioPool.releaseInstance(di);
          throw new HiveServerException("unvalid state!!!");
        }

        di.setDIState(DataImport.DataImportState.UPLOADING);
        if (!(di.getIsFormated()) && !(di.getIsPart()) && !(di.getIsRCFile())) {
          FSDataOutputStream os = di.getTableOs();
          if (os != null) {
            try {
              os.write(data.getBytes("UTF-8"));
            } catch (IOException x) {
              if (di.getTempTableName() != null) {
                execute("drop table " + di.getTempTableName());
                di.setTempTableName(null);
              }

              di.setDIState(DataImport.DataImportState.ERROR);
              dioPool.releaseInstance(di);
              throw new HiveServerException("HDFS file can not be writed!!");
            }
          } else {
            if (di.getTempTableName() != null) {
              execute("drop table " + di.getTempTableName());
              di.setTempTableName(null);
            }

            di.setDIState(DataImport.DataImportState.ERROR);
            dioPool.releaseInstance(di);
            throw new HiveServerException("HDFS file can not be writed!!");
          }
        } else {
          FSDataOutputStream os = di.getTempTableOs();
          if (os != null) {
            try {
              os.write(data.getBytes("UTF-8"));
            } catch (IOException x) {
              if (di.getTempTableName() != null) {
                execute("drop table " + di.getTempTableName());
                di.setTempTableName(null);
              }

              di.setDIState(DataImport.DataImportState.ERROR);
              dioPool.releaseInstance(di);
              throw new HiveServerException("HDFS file can not be writed!!");
            }
          } else {
            if (di.getTempTableName() != null) {
              execute("drop table " + di.getTempTableName());
              di.setTempTableName(null);
            }

            di.setDIState(DataImport.DataImportState.ERROR);
            dioPool.releaseInstance(di);
            throw new HiveServerException("HDFS file can not be writed!!");
          }
        }

      } else {
        throw new HiveServerException("unkown error!!");
      }
      return 0;
    }

    public int completeUploadData(String loaderID) throws HiveServerException,
        TException {
      DataImport di = SessionState.get().getDataImport();
      if (di != null) {
        if (di.getDIState() != DataImport.DataImportState.UPLOADING) {
          if (di.getTempTableName() != null) {
            execute("drop table " + di.getTempTableName());
            di.setTempTableName(null);
          }

          di.setDIState(DataImport.DataImportState.ERROR);
          dioPool.releaseInstance(di);
          throw new HiveServerException("unvalid state!!!");
        }
        if (!(di.getIsFormated()) && !(di.getIsPart()) && !(di.getIsRCFile())) {
          FSDataOutputStream os = di.getTableOs();
          if (os != null) {
            try {
              os.close();
              di.setDIState(DataImport.DataImportState.COMPLETE);
            } catch (IOException x) {
              if (di.getTempTableName() != null) {
                execute("drop table " + di.getTempTableName());
                di.setTempTableName(null);
              }

              di.setDIState(DataImport.DataImportState.ERROR);
              dioPool.releaseInstance(di);
              throw new HiveServerException("unkown error!");
            }
          } else {
            if (di.getTempTableName() != null) {
              execute("drop table " + di.getTempTableName());
              di.setTempTableName(null);
            }

            di.setDIState(DataImport.DataImportState.ERROR);
            dioPool.releaseInstance(di);
            throw new HiveServerException("unkown error!");
          }
        } else {
          FSDataOutputStream os = di.getTempTableOs();
          if (os != null) {
            try {
              os.close();
              String insertSQL = null;

              if (di.getIsOverWrite()) {
                insertSQL = "INSERT OVERWRITE TABLE " + di.getTableName()
                    + " SELECT * FROM " + di.getTempTableName();
              } else {
                insertSQL = "INSERT TABLE " + di.getTableName()
                    + " SELECT * FROM " + di.getTempTableName();

                LOG.debug("data import insert SQL:" + insertSQL);
              }

              di.setDIState(DataImport.DataImportState.INSERTING);
              jdbc_execute(insertSQL, di.getLoaderID());
              try {
                execute("drop table " + di.getTempTableName());
                di.setTempTableName(null);
              } catch (Exception x) {

              }
              di.setDIState(DataImport.DataImportState.COMPLETE);
            } catch (IOException x) {
              if (di.getTempTableName() != null) {
                execute("drop table " + di.getTempTableName());
                di.setTempTableName(null);
              }

              di.setDIState(DataImport.DataImportState.ERROR);
              dioPool.releaseInstance(di);
              throw new HiveServerException("can not insert values into talbe "
                  + di.getTableName());
            }
          } else {
            if (di.getTempTableName() != null) {
              execute("drop table " + di.getTempTableName());
              di.setTempTableName(null);
            }

            di.setDIState(DataImport.DataImportState.ERROR);
            dioPool.releaseInstance(di);
            throw new HiveServerException("unkown error!");
          }
        }

      }
      return 0;
    }

    @Override
    public int querySQLState(String sqlID) throws HiveServerException,
        TException {
      synchronized (driverMap) {
        if (sqlID == null)
          throw new HiveServerException("invalid sql ID");
        CommandProcessor proc = driverMap.get(sqlID);
        if (proc == null)
          throw new HiveServerException("not exist sql");

        if (proc instanceof Driver) {
          Driver sqlDriver = (Driver) proc;
          switch (sqlDriver.getProcessState()) {
          case INIT:
            return 1;
          case PROCESSING:
            return 2;
          case COMPLETE:
            driverMap.remove(sqlID);
            return 0;
          case ERROR:
            driverMap.remove(sqlID);
            throw new HiveServerException(driver.getErrorMsg());
          default:
            break;
          }
        } else {
          throw new HiveServerException("UNKOWN error");
        }
        return 0;
      }
    }

    @Override
    public String getJobID(String sqlID)
        throws org.apache.hadoop.hive.service.HiveServerException, TException {
      synchronized (driverMap) {
        if (driverMap == null)
          throw new HiveServerException("server uninit!");
        if (sqlID == null)
          throw new HiveServerException("invalid sql ID");
        CommandProcessor proc = driverMap.get(sqlID);
        if (proc == null)
          throw new HiveServerException("not exist sql");
        if (proc instanceof Driver) {
          Driver qp = (Driver) proc;
          String jobURL = null;
          try {
            jobURL = qp.getJobURL();

          } catch (IOException x) {
            throw new HiveServerException(x.getMessage());
          }

          if (jobURL == null) {
            throw new HiveServerException("current job is null");
          } else {
            return new String(jobURL);
          }
        } else
          return new String();
      }
    }

    @Override
    public List<String> getJobCounts(String sqlID)
        throws org.apache.hadoop.hive.service.HiveServerException, TException {
      Map<String, String> keyValue = null;
      List<String> values = new ArrayList<String>();
      System.out.println("start get running information");
      synchronized (driverMap) {
        if (driverMap == null)
          throw new HiveServerException("server uninit!");
        if (sqlID == null)
          throw new HiveServerException("invalid sql ID");
        CommandProcessor proc = driverMap.get(sqlID);
        if (proc == null)
          throw new HiveServerException("not exist sql");
        if (proc instanceof Driver) {
          Driver qp = (Driver) proc;

          try {
            System.out.println("before qp.getJobCounts()");
            keyValue = qp.getJobCounts();
            Set<String> keySet = keyValue.keySet();
            for (String item : keySet) {
              values.add(item + ":" + keyValue.get(item));
              LOG.error(item + ":" + keyValue.get(item));
            }
            System.out.println("end qp.getJobCounts()");
          } catch (Exception x) {
            LOG.error(x.getMessage());
          }
          return values;
        } else
          return values;
      }
    }

    @Override
    public List<String> getHiveLoad()
        throws org.apache.hadoop.hive.service.HiveServerException, TException {
      String[] shellCmd = new String[] { "sh", "-c",
          "netstat -apn | grep 10000 | wc -l" };
      List<String> kvList = new ArrayList<String>();

      try {
        Runtime runtime = Runtime.getRuntime();
        Process process = runtime.exec(shellCmd);
        InputStreamReader ir = new InputStreamReader(process.getInputStream());
        LineNumberReader lr = new LineNumberReader(ir);
        String line;

        while ((line = lr.readLine()) != null) {
          kvList.add("Connect Number:" + line);
          break;
        }
        process.destroy();
        process.waitFor();
      } catch (Exception sx) {
        sx.printStackTrace();
      }

      return kvList;
    }

    @Override
    public List<String> getSchemaForExplain(String sql)
        throws org.apache.hadoop.hive.service.HiveServerException, TException {
      String explainSql = "explain " + sql;
      List<String> retList = new ArrayList<String>();

      execute(explainSql);
      generic_checking(CheckingFlag.SESSION);

      if (!isHiveQuery)
        return retList;

      try {
        Schema schema = driver.getSchemaForExplain();
        if (schema != null) {
          List<FieldSchema> fields = schema.getFieldSchemas();
          if (fields != null && !fields.isEmpty()) {
            for (FieldSchema field : fields) {
              String col = field.getName() + ":" + field.getType();
              retList.add(col);
            }
          }
        }

        LOG.info("Returning schema: " + schema.toString());

        return retList;
      } catch (Exception e) {
        LOG.error(e.toString());
        e.printStackTrace();
        throw new HiveServerException("Unable to get schema: " + e.toString());
      }
    }

    @Override
    public String uploadModule(String user, String moduleName,
        ByteBuffer module1)
        throws org.apache.hadoop.hive.service.HiveServerException, TException {
      String res = "ok";

      generic_checking(CheckingFlag.SESSION);

      if (moduleName.contains("..")) {
        throw new HiveServerException(
            "Error, you can not issue moduleName contains '..'");
      }
      if (!getUser().equalsIgnoreCase("root")
          && !getUser().equalsIgnoreCase(user)) {
        throw new HiveServerException("Error, you can only access your ("
            + getUser() + ") own library.");
      }
      if (!getUser().equalsIgnoreCase("root")) {
        user = getUser();
      }

      if (sessionItem.uploadModule(user, moduleName, module1.array()) == false) {
        throw new HiveServerException("Upload module " + moduleName
            + " failed.");
      }

      return res;
    }

    public int replaceJar(String dbname, String tablename, String user)
        throws HiveServerException, TException {
      int res = 1;
      Hive hive;
      List<FieldSchema> cols = null;
      generic_checking(CheckingFlag.SESSION);
      String modified_time = null;
      try {
        hive = Hive.get();
      } catch (Exception e) {
        res = 1;
        LOG.error(e.toString());
        e.printStackTrace();
        throw new HiveServerException("Hive.get() in replaceJar failed: " + e);
      }

      LOG.info("isPBTable---" + "dbname: " + dbname + " tablename: "
          + tablename);
      if (hive.isPBTable(dbname, tablename) == false) {
        res = 1;
        LOG.info("dbname: " + dbname + " tablename: " + tablename
            + "is not pb table");
        throw new TException(dbname + "::" + tablename
            + "is not pb table, replace jar can only support pb table!");
      }
      if (sessionItem != null) {
        LOG.info("getColsFromJar---" + "dbname: " + dbname + " tablename: "
            + tablename);
        cols = sessionItem.getColsFromJar(dbname, tablename);
        if (cols == null) {
          res = 1;
          throw new HiveServerException("can not get cols from jar.");
        }
      }
      try {
        LOG.info("replaceCols---" + "dbname: " + dbname + " tablename: "
            + tablename + " user: " + user);
        hive.replaceCols(dbname, tablename, user, cols);
      } catch (Exception e) {
        res = 1;
        LOG.error(e.toString());
        e.printStackTrace();
        throw new HiveServerException("replaceCols in replaceJar failed: " + e);
      }
      modified_time = sessionItem.getPBTableModifiedTime(dbname, tablename);

      try {
        LOG.info("updatePBInfo---" + "dbname: " + dbname + " tablename: "
            + tablename + " modified_time: " + modified_time);
        hive.updatePBInfo(dbname, tablename, modified_time);
        res = 0;
      } catch (Exception e) {
        res = 1;
        LOG.error(e.toString());
        e.printStackTrace();
        throw new HiveServerException("updatePBInfo in replaceJar failed: " + e);
      }
      return res;
    }

    @Override
    public String uploadProto(String user, String fileName, ByteBuffer data)
        throws org.apache.hadoop.hive.service.HiveServerException, TException {
      String res = "ok";

      generic_checking(CheckingFlag.SESSION);

      if (fileName.contains("..")) {
        throw new HiveServerException(
            "Error, you can not issue fileName contains '..'");
      }
      if (!getUser().equalsIgnoreCase("root")
          && !getUser().equalsIgnoreCase(user)) {
        throw new HiveServerException("Error, you can only access your ("
            + getUser() + ") own proto file.");
      }
      if (!getUser().equalsIgnoreCase("root")) {
        user = getUser();
      }

      if (sessionItem.uploadProto(user, fileName, data.array()) == false) {
        throw new HiveServerException("Upload file " + fileName + " failed.");
      }
      return res;
    }
  }

  public static class ThriftHiveProcessorFactory extends TProcessorFactory {
    public ThriftHiveProcessorFactory(TProcessor processor) {
      super(processor);
    }

    public TProcessor getProcessor(TTransport trans) {
      Log LOG = LogFactory.getLog(HiveServer.class.getName());
      try {
        TSocket tSocket = (TSocket) trans;
        Socket socket = tSocket.getSocket();
        InetAddress addr = socket.getInetAddress();
        LOG.info("client address: " + addr.getHostAddress());
        LOG.info("client port: " + socket.getPort());
      } catch (Exception e) {
        LOG.info("get IP, PORT failed in create session");
      }

      try {
        Iface handler = new HiveServerHandler(trans);
        return new ThriftHive.Processor(handler);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static void main(String[] args) {
    try {

      if (args.length >= 1) {
        port = Integer.parseInt(args[0]);
      }
      
      //set dns cache to 3s;the negtive cache default value is 10s in java
      java.security.Security.setProperty("networkaddress.cache.ttl" , "3");
      
      SessionState.initHiveLog4j();
      sessionManager = new HSSessionManager();
      Thread t = new Thread(sessionManager);
      t.setDaemon(true);
      t.start();

      dioPool = new DataInOutPool(50);
      dioPool.init();
      Thread dioPoolThread = new Thread(dioPool);
      dioPoolThread.setDaemon(true);
      dioPoolThread.start();

      TServerTransport serverTransport = new TServerSocket(port);
      ThriftHiveProcessorFactory hfactory = new ThriftHiveProcessorFactory(null);

      Args serverArgs = new Args(serverTransport);
      serverArgs.processorFactory(hfactory);
      serverArgs.inputTransportFactory(new TTransportFactory());
      serverArgs.outputTransportFactory(new TTransportFactory());
      serverArgs.inputProtocolFactory(new TBinaryProtocol.Factory());
      serverArgs.outputProtocolFactory(new TBinaryProtocol.Factory());

      TServer server = new TThreadPoolServer(serverArgs);
      HiveServerHandler.LOG.info("Starting hive server on port " + port);

      server.serve();
    } catch (Exception x) {
      x.printStackTrace();
    }
  }
}
