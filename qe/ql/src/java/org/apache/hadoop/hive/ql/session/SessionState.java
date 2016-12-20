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

package org.apache.hadoop.hive.ql.session;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.*;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.tdw_query_error_info;
import org.apache.hadoop.hive.metastore.api.tdw_query_info;
import org.apache.hadoop.hive.ql.metadata.BIStore;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.history.HiveHistory;
import org.apache.hadoop.mapred.RunningJob;

import org.apache.commons.lang.StringUtils;

public class SessionState {

  private DataImport dataImporter;
  protected String userName = "root";

  protected String passwd = "tdwroot";

  protected String LogQueryID = "";
  protected boolean isCreateQuery = false;

  protected String dbName = MetaStoreUtils.DEFAULT_DATABASE_NAME;
  protected boolean isCheck = false;

  protected String tbName = null;

  public String getTbName() {
    return tbName;
  }

  public void setTbName(String tbName) {
    this.tbName = tbName;
  }

  BIStore bistore = null;

  public BIStore getBIStore() {
	  if (bistore == null )
		  initBIStore();
    return bistore;
  }

  public void setBIStore(Properties prop) {
    if (prop == null) {
      bistore = null;
      return;
    }
    if (bistore == null) {
      bistore = new BIStore(prop);
    }
  }
  
  public void initBIStore() {
	  if (bistore != null)
		  return;
	  else{
            if(conf.getVar(HiveConf.ConfVars.HIVE_QUERY_INFO_LOG_URL) == ""){
            	//fall back to old configure
            	Properties prop = new Properties();
            	prop.setProperty("user", conf.getVar(HiveConf.ConfVars.BISTOREUSER));
            	prop.setProperty("password", conf.getVar(HiveConf.ConfVars.BISTOREPWD));
            	prop.setProperty("db_name", conf.getVar(HiveConf.ConfVars.BISTOREDB));
            	prop.setProperty("port",
	            String.valueOf(conf.getIntVar(HiveConf.ConfVars.BISTOREPORT)));
            	prop.setProperty("ip", conf.getVar(HiveConf.ConfVars.BISTOREIP));
            	SessionState.get().setBIStore(prop);
            }
            else{//use new configure if it set
            	bistore = new BIStore(conf.getVar(HiveConf.ConfVars.HIVE_QUERY_INFO_LOG_URL),conf.getVar(HiveConf.ConfVars.HIVE_QUERY_INFO_LOG_USER),conf.getVar(HiveConf.ConfVars.HIVE_QUERY_INFO_LOG_PASSWD));
            }
	  }
  }

  static private Map<String, RunningJob> mrjobs = new ConcurrentHashMap<String, RunningJob>();
  static private Map<String, String> unames = new ConcurrentHashMap<String, String>();

  static public RunningJob getRunningJob(String qid) {
    return (RunningJob) mrjobs.get((String) qid);
  }

  static public String getUserName(String qid) {
    return (String) unames.get((String) qid);
  }

  static public boolean updateRunningJob(String qid, RunningJob rjob) {
    if (qid == null || rjob == null) {
      return false;
    }
    mrjobs.put(qid, rjob);
    return true;
  }

  static public boolean updateUserName(String qid, String uname) {
    if (qid == null || uname == null) {
      return false;
    }
    unames.put(qid, uname);
    return true;
  }

  static public boolean clearQeury(String qid) {
    if (qid == null) {
      return false;
    }
    if (mrjobs.containsKey(qid) && unames.containsKey(qid)) {
      RunningJob r_1 = mrjobs.remove(qid);
      String r_2 = unames.remove(qid);
      return ((r_1 != null) && (r_2 != null));
    } else {
      return false;
    }

  }

  private String sessionName = null;
  private int current_query_id = 0;
  private PrintWriter logStream = null;
  private tdw_query_info qinfo = null;
  private int current_mr_index = 0;
  private boolean iscli = true;
  private tdw_query_error_info einfo = null;
  
  public boolean getiscli() {
    return iscli;
  }

  public void setiscli(boolean cli) {
    iscli = cli;
  }

  public void setCurrMRIndex(int in) {
    current_mr_index = in;
  }

  public int getCurrMRIndex() {
    return current_mr_index;
  }

  public void setTdw_query_info(tdw_query_info input) {
    qinfo = input;
  }

  public tdw_query_info getTdw_query_info() {
    return qinfo;
  }

  public boolean updateTdw_query_info(int MRNum) {
    if (qinfo != null) {
      qinfo.setMRNum(MRNum);
      return true;
    } else {
      return false;
    }
  }
  
  public void setTdw_query_error_info(tdw_query_error_info input) {
    einfo = input;
  }

  public tdw_query_error_info getTdw_query_error_info() {
    return einfo;
  }

  public void setSessionItemInFo(String name, int id, PrintWriter pw) {
    sessionName = name;
    current_query_id = id;
    logStream = pw;
    
    this.getConf().setVar(HiveConf.ConfVars.HIVESESSIONID, sessionName);
    this.getConf().setVar(HiveConf.ConfVars.HIVEQUERYID, sessionName + "_" + Integer.toString(current_query_id));
  }
  
  public void setSessionName(String name){
	  sessionName = name;
	  this.getConf().setVar(HiveConf.ConfVars.HIVESESSIONID, sessionName);
  }
  
  public String getSessionName(){
	  return sessionName;
  }
  
  public void setCurrent_query_index(int id){
	  current_query_id = id;
	  this.getConf().setVar(HiveConf.ConfVars.HIVEQUERYID, sessionName + "_" + Integer.toString(current_query_id));
  }

  public void closePrinter() {
    if (logStream != null) {
      logStream.flush();
      logStream.close();
      logStream = null;
    }
  }

  public String getCurrnetQueryId() {
    return sessionName + "_" + Integer.toString(current_query_id);
  }

  public void ssLog(String str) {

    if (logStream == null)
      return;

    Date now = new Date();
    String tmp = now.toLocaleString() + " " + getCurrnetQueryId() + ":  " + str;

    logStream.println(tmp);
    logStream.flush();

  }

  public boolean isCheck() {
    return isCheck;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getPasswd() {
    return passwd;
  }

  public void setPasswd(String passwd) {
    this.passwd = passwd;
  }

  //session_name_query_index_random,attempt to be unique,only for tPG log
  public void setQueryIDForLog(String ID) {
    this.LogQueryID = ID;
  }

  public String getQueryIDForLog() {
    return this.LogQueryID;
  }

  public void setCreateQueryFlag(boolean flag) {
    this.isCreateQuery = flag;
  }

  public boolean getCreateQueryFlag() {
    return this.isCreateQuery;
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public void setCheck(boolean isCheck) {
    this.isCheck = isCheck;
  }

  protected HiveConf conf;

  protected boolean isSilent;

  protected Hive db;

  protected HiveHistory hiveHist;

  public PrintStream out;
  public InputStream in;
  public PrintStream err;

  public HiveConf getConf() {
    return conf;
  }

  public void setConf(HiveConf conf) {
    this.conf = conf;
  }

  public boolean getIsSilent() {
    return isSilent;
  }

  public void setIsSilent(boolean isSilent) {
    this.isSilent = isSilent;
  }

  public SessionState() {
    this(null, null);

  }

  public SessionState(HiveConf conf) {
    this(conf, null);
  }

  public SessionState(HiveConf conf, Hive db) {
    this.conf = conf;
    this.db = db;

    for (HiveConf.ConfVars oneVar : HiveConf.metaVars) {
      dbOptions.put(oneVar, conf.getVar(oneVar));
    }
  }

  private final HashMap<HiveConf.ConfVars, String> dbOptions = new HashMap<HiveConf.ConfVars, String>();

  public Hive getDb() throws HiveException {
    boolean needsRefresh = false;

    for (HiveConf.ConfVars oneVar : HiveConf.metaVars) {
      if (!StringUtils.isEmpty(StringUtils.difference(dbOptions.get(oneVar),
          conf.getVar(oneVar)))) {
        needsRefresh = true;
        break;
      }
    }

    if ((db == null) || needsRefresh) {
      db = Hive.get(conf, needsRefresh);
    }

    return db;
  }

  public void setCmd(String cmdString) {
    conf.setVar(HiveConf.ConfVars.HIVEQUERYSTRING, cmdString);
  }

  public String getCmd() {
    return (conf.getVar(HiveConf.ConfVars.HIVEQUERYSTRING));
  }

  public String getQueryId() {
    return (conf.getVar(HiveConf.ConfVars.HIVEQUERYID));
  }

  public String getSessionId() {
    return (conf.getVar(HiveConf.ConfVars.HIVESESSIONID));
  }

  public void setDataImport(DataImport di) {
    dataImporter = di;
  }

  public DataImport getDataImport() {
    return dataImporter;
  }

  private static ThreadLocal<SessionState> tss = new ThreadLocal<SessionState>();

  public static SessionState start(HiveConf conf) {
    SessionState ss = new SessionState(conf);
    LogHelper console = getConsole();
    ss.getConf().setVar(HiveConf.ConfVars.HIVESESSIONID, makeSessionId());
    console.printInfo("session : "
        + ss.getConf().getVar(HiveConf.ConfVars.HIVESESSIONID) + " start!");
    ss.hiveHist = new HiveHistory(ss);
    
    //this is only for CLI,for HiveServer,will be overwrited
    ss.setSessionName(ss.getConf().getVar(HiveConf.ConfVars.HIVESESSIONID));
    ss.setCurrent_query_index(-1);
    
    tss.set(ss);
    return (ss);
  }

  public static SessionState start(SessionState startSs) {

    tss.set(startSs);
    if (StringUtils.isEmpty(startSs.getConf().getVar(
        HiveConf.ConfVars.HIVESESSIONID))) {
      startSs.getConf()
          .setVar(HiveConf.ConfVars.HIVESESSIONID, makeSessionId());
    }
    LogHelper console = getConsole();
    console
        .printInfo("session : "
            + startSs.getConf().getVar(HiveConf.ConfVars.HIVESESSIONID)
            + " start!");

    if (startSs.hiveHist == null) {
      startSs.hiveHist = new HiveHistory(startSs);
    }
    
    //this is only for CLI,for HiveServer,will be overwrited
    startSs.setSessionName(startSs.getConf().getVar(HiveConf.ConfVars.HIVESESSIONID));
    startSs.setCurrent_query_index(-1);
    
    return startSs;
  }

  public static SessionState get() {
    return tss.get();
  }

  public HiveHistory getHiveHistory() {
    return hiveHist;
  }

  private static String makeSessionId() {
    GregorianCalendar gc = new GregorianCalendar();
    String userid = System.getProperty("user.name");

    return userid
        + "_"
        + String.format("%1$4d%2$02d%3$02d%4$02d%5$02d", gc.get(Calendar.YEAR),
            gc.get(Calendar.MONTH) + 1, gc.get(Calendar.DAY_OF_MONTH),
            gc.get(Calendar.HOUR_OF_DAY), gc.get(Calendar.MINUTE)) + "_"
        + Math.random();
  }

  public static final String HIVE_L4J = "hive-log4j.properties";

  public static void initHiveLog4j() {
    URL hive_l4j = SessionState.class.getClassLoader().getResource(HIVE_L4J);
    if (hive_l4j == null) {
      System.out.println(HIVE_L4J + " not found");
    } else {
      LogManager.resetConfiguration();
      PropertyConfigurator.configure(hive_l4j);
    }
  }

  public static class LogHelper {

    protected Log LOG;
    protected boolean isSilent;

    protected HiveConf conf;

    public LogHelper(Log LOG, HiveConf conf) {
      this(LOG, false);

      this.conf = conf;
    }

    public LogHelper(Log LOG) {
      this(LOG, false);
    }

    public LogHelper(Log LOG, boolean isSilent) {
      this.LOG = LOG;
      this.isSilent = isSilent;
    }

    public PrintStream getOutStream() {
      SessionState ss = SessionState.get();
      return ((ss != null) && (ss.out != null)) ? ss.out : System.out;
    }

    public PrintStream getErrStream() {
      SessionState ss = SessionState.get();
      return ((ss != null) && (ss.err != null)) ? ss.err : System.err;
    }

    public boolean getIsSilent() {
      SessionState ss = SessionState.get();
      return (ss != null) ? ss.getIsSilent() : isSilent;
    }

    public void printInfo(String info) {
      printInfo(info, null);
    }

    public void printInfo(String info, String detail) {
      if (!getIsSilent()) {
        getErrStream().println(info);
      }
      LOG.info(info + StringUtils.defaultString(detail));
    }

    public void printError(String error) {
      printError(error, null);
    }

    public void printError(String error, String detail) {
      getErrStream().println(error);
      LOG.error(error + StringUtils.defaultString(detail));
    }

    public void DEBUG(String msg) {
      if (conf.getBoolVar(HiveConf.ConfVars.DEBUG)) {
        printInfo("DEBUG: " + msg, "DEBUG: " + msg);
      }
    }
  }

  private static LogHelper _console;

  public static LogHelper getConsole() {
    if (_console == null) {
      Log LOG = LogFactory.getLog("SessionState");
      _console = new LogHelper(LOG);
    }
    return _console;
  }

  public static String validateFile(Set<String> curFiles, String newFile) {
    SessionState ss = SessionState.get();
    LogHelper console = getConsole();
    Configuration conf = (ss == null) ? new Configuration() : ss.getConf();

    try {
      if (Utilities.realFile(newFile, conf) != null)
        return newFile;
      else {
        console.printError(newFile + " does not exist");
        return null;
      }
    } catch (IOException e) {
      console.printError(
          "Unable to validate " + newFile + "\nException: " + e.getMessage(),
          "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return null;
    }
  }

  public static boolean registerJar(String newJar) {
    LogHelper console = getConsole();
    try {
      ClassLoader loader = JavaUtils.getpbClassLoader();
      JavaUtils.setpbClassLoader(Utilities.addToClassPath(loader,
          StringUtils.split(newJar, ",")));
      console.printInfo("Added " + newJar + " to class path");
      return true;
    } catch (Exception e) {
      console.printError(
          "Unable to register " + newJar + "\nException: " + e.getMessage(),
          "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return false;
    }
  }

  public static boolean unregisterJar(String jarsToUnregister) {
    LogHelper console = getConsole();
    try {
      Utilities.removeFromClassPath(StringUtils.split(jarsToUnregister, ","));
      console.printInfo("Deleted " + jarsToUnregister + " from class path");
      return true;
    } catch (Exception e) {
      console.printError("Unable to unregister " + jarsToUnregister
          + "\nException: " + e.getMessage(), "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return false;
    }
  }

  public static interface ResourceHook {
    public String preHook(Set<String> cur, String s);

    public boolean postHook(Set<String> cur, String s);
  }

  public static enum ResourceType {
    FILE(new ResourceHook() {
      public String preHook(Set<String> cur, String s) {
        return validateFile(cur, s);
      }

      public boolean postHook(Set<String> cur, String s) {
        return true;
      }
    }),

    JAR(new ResourceHook() {
      public String preHook(Set<String> cur, String s) {
        String newJar = validateFile(cur, s);
        if (newJar != null) {
          return (registerJar(newJar) ? newJar : null);
        } else {
          return null;
        }
      }

      public boolean postHook(Set<String> cur, String s) {
        return unregisterJar(s);
      }
    });

    public ResourceHook hook;

    ResourceType(ResourceHook hook) {
      this.hook = hook;
    }
  };

  public static ResourceType find_resource_type(String s) {

    s = s.trim().toUpperCase();

    try {
      return ResourceType.valueOf(s);
    } catch (IllegalArgumentException e) {
    }

    if (s.endsWith("S")) {
      s = s.substring(0, s.length() - 1);
    } else {
      return null;
    }

    try {
      return ResourceType.valueOf(s);
    } catch (IllegalArgumentException e) {
    }
    return null;
  }

  private HashMap<ResourceType, HashSet<String>> resource_map = new HashMap<ResourceType, HashSet<String>>();

  public void add_resource(ResourceType t, String value) {
    if (resource_map.get(t) == null) {
      resource_map.put(t, new HashSet<String>());
    }

    String fnlVal = value;
    if (t.hook != null) {
      fnlVal = t.hook.preHook(resource_map.get(t), value);
      if (fnlVal == null)
        return;
    }
    resource_map.get(t).add(fnlVal);
  }

  public boolean delete_resource(ResourceType t, String value) {
    if (resource_map.get(t) == null) {
      return false;
    }
    if (t.hook != null) {
      if (!t.hook.postHook(resource_map.get(t), value))
        return false;
    }
    return (resource_map.get(t).remove(value));
  }

  public Set<String> list_resource(ResourceType t, List<String> filter) {
    if (resource_map.get(t) == null) {
      return null;
    }
    Set<String> orig = resource_map.get(t);
    if (filter == null) {
      return orig;
    } else {
      Set<String> fnl = new HashSet<String>();
      for (String one : orig) {
        if (filter.contains(one)) {
          fnl.add(one);
        }
      }
      return fnl;
    }
  }

  public void delete_resource(ResourceType t) {
    if (resource_map.get(t) != null) {
      for (String value : resource_map.get(t)) {
        delete_resource(t, value);
      }
      resource_map.remove(t);
    }
  }

  public void finalize() {
    LogHelper console = getConsole();
    console.printInfo("session: " + getSessionId() + " destroy!");
  }
}
