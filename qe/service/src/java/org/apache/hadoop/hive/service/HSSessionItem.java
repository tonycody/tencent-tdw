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

package org.apache.hadoop.hive.service;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.GregorianCalendar;
import java.util.Calendar;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.tdw_query_error_info;
import org.apache.hadoop.hive.metastore.api.tdw_query_info;
import org.apache.hadoop.hive.ql.PBJarTool;
import org.apache.hadoop.hive.ql.history.HiveHistory.Keys;
import org.apache.hadoop.hive.ql.history.HiveHistory.RecordTypes;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import org.apache.hadoop.hive.serde.Constants;

public class HSSessionItem implements Comparable<HSSessionItem> {

  protected static final Log l4j = LogFactory.getLog(HSSessionItem.class
      .getName());
  protected static final Log newlog = LogFactory.getLog("NewDayLog");
  private int runSqlCnt = 0;

  public long to;

  public HashSet<TTransport> transSet;

  public final ReentrantLock lock = new ReentrantLock();

  private final Date date;

  public Date opdate;
  private Date freedate = null;
  public static final String protobufPackageName = "tdw";

  public enum SessionItemStatus {
    NEW, READY, JOB_SET, JOB_RUNNING, DESTROY, KILL_JOB
  };

  public enum jobType {
    ONESHOT {
      @Override
      public String toString() {
        return "ONESHOT";
      }
    },
    REPEAT {
      @Override
      public String toString() {
        return "REPEAT";
      }
    },
  }

  public enum JobStatus {
    INIT {
      @Override
      public String toString() {
        return "INIT";
      }
    },
    SET {
      @Override
      public String toString() {
        return "SET";
      }
    },
    RUNNING {
      @Override
      public String toString() {
        return "RUNNING";
      }
    },
    KILL {
      @Override
      public String toString() {
        return "KILL";
      }
    },
    DONE {
      @Override
      public String toString() {
        return "DONE";
      }
    }
  };

  private HSSessionItem.JobStatus jobStatus;

  private int jobid;

  private final String home;

  private final String sessionName;

  private String hostip = null;
  private String clientip = null;
  private String conf_file_loc = SessionState.get().getConf()
      .getVar(HiveConf.ConfVars.NEWLOGPATH);
  private String sessionFileName;
  private tdw_query_info qinfo = null;
  private String curr_starttime = null;
  private String curr_cmd = null;
  private int current_query_count = 0;
  private PrintWriter dayLogStream = null;
  private static final String DELIMITER = "|";
  private tdw_query_error_info einfo = null;

  public static enum RecordTypes {
    QueryStart, QueryEnd, SessionEnd
  };

  private Map<Integer, GregorianCalendar> queryInfo;

  private HSSessionItem.SessionItemStatus status;

  private CliSessionState ss;

  private HiveConf conf;

  private HSAuth auth;

  private String historyFile;

  private Thread runable;

  private JobRunner jr;

  private final SessionConfig config;

  public String suffix;

  public class SessionConfig {
    public jobType type;
    public long ti_repeat;

    public PeriodicJob pJob;

    public Timer timer;

    public SessionConfig(jobType type) {
      this.type = type;
      this.ti_repeat = -1;
      this.pJob = null;
      this.timer = null;
    }

    public void setJobType(jobType type, long ti) {
      if (type == jobType.REPEAT && ti > 0) {
        if (timer == null) {
          pJob = new PeriodicJob();
          timer = new Timer();
          timer.scheduleAtFixedRate(pJob, ti * 1000, ti * 1000);
          l4j.info("Set Timer to execute @ " + ti + " sec later.");
        } else if (ti > 0 && ti != this.ti_repeat) {
          cancelPeriodicJob();
          pJob = new PeriodicJob();
          timer = new Timer();
          timer.scheduleAtFixedRate(pJob, ti * 1000, ti * 1000);
          l4j.info("Set Timer to execute @ " + ti + " sec later.");
        }
      } else {
        l4j.info("Cancel the Timer now");
        cancelPeriodicJob();
      }
      this.type = type;
      this.ti_repeat = ti;
    }

    public void cancelPeriodicJob() {
      if (timer != null || pJob != null) {
        if (timer != null) {
          timer.cancel();
        }

        timer = null;
        pJob = null;
      }
    }
  }

  public void configJob(jobType type, long ti) {
    config.setJobType(type, ti);
  }

  public class PeriodicJob extends TimerTask {
    @Override
    public void run() {
      try {
        prepareJobFile();
      } catch (HiveServerException hex) {
        l4j.error("Trying to prepare the job file for jobid=" + jobid
            + " failed.");
        return;
      }

      l4j.info("Run a perodic job w/ jobid=" + jobid);
      runJob();
    }

  }

  public class JobRunner implements Runnable {

    private final String jobFile;

    private File resultFile;

    private File errorFile;

    private File infoFile;

    private int jobid;

    private String sid;

    private String svid;

    private final String user;

    private final String passwd;

    private final String dbName;

    private Process p;

    protected JobRunner(String jobFile, String sid, String svid, String user,
        String passwd, String dbName) {
      this.jobFile = jobFile;
      this.sid = sid;
      this.svid = svid;
      this.resultFile = null;
      this.errorFile = null;
      this.user = user;
      this.passwd = passwd;
      this.dbName = dbName;
    }

    public void run() {
      if (getResultFile() == null) {
        setResultFile(new File(getHome() + "/pl/TDW_PL_JOB_DEFAULT_result"));
      }
      if (getErrorFile() == null) {
        setErrorFile(new File(getHome() + "/pl/TDW_PL_JOB_DEFAULT_error"));
      }
      if (getInfoFile() == null) {
        setInfoFile(new File(getHome() + "/pl/TDW_PL_JOB_DEFAULT_info"));
      }
      String ocwd = System.getProperty("user.dir");
      String path = getHome() + "/pl/";

      List<String> cmd = new ArrayList<String>();
      cmd.add("python");
      cmd.add(ocwd + "/pl/tdw_loader.py");
      cmd.add("-f" + jobFile);
      cmd.add("-s" + sid);
      cmd.add("-a" + svid);
      cmd.add("-u" + user);
      cmd.add("-p" + passwd);
      cmd.add("-x" + path);
      cmd.add("-j" + jobid);
      cmd.add("-d" + dbName);
      cmd.add("-t" + getAuth().getPort());

      ProcessBuilder builder = new ProcessBuilder(cmd);
      Map<String, String> env = builder.environment();

      setJobStatus(HSSessionItem.JobStatus.RUNNING);
      try {
        p = builder.start();

        if (runable.isInterrupted()) {
          throw new InterruptedException();
        }
        InputStream is = p.getInputStream();
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);
        String line;

        FileOutputStream fos = new FileOutputStream(getResultFile());
        while ((line = br.readLine()) != null) {
          fos.write(line.getBytes());
          fos.write('\n');
        }

        if (runable.isInterrupted()) {
          throw new InterruptedException();
        }
        is = p.getErrorStream();
        isr = new InputStreamReader(is);
        br = new BufferedReader(isr);
        fos = new FileOutputStream(getErrorFile());
        while ((line = br.readLine()) != null) {
          fos.write(line.getBytes());
          fos.write('\n');
        }
      } catch (InterruptedException iex) {
        String msg = "Job Killed by the Session Owner.\n";
        try {
          FileOutputStream fos = new FileOutputStream(getResultFile());
          fos.write(msg.getBytes());
          fos.write('\n');
          fos.close();
          fos = new FileOutputStream(getErrorFile());
          fos.write(msg.getBytes());
          fos.write('\n');
          fos.close();
        } catch (java.io.FileNotFoundException fex) {
          l4j.error("File Not found ", fex);
        } catch (java.io.IOException ex) {
          l4j.error("IO exception", ex);
        }
      } catch (java.io.FileNotFoundException fex) {
        l4j.error("Execute job failed w/ FEX ", fex);
      } catch (java.io.IOException ex) {
        l4j.error("Execute job failed ", ex);
      }
      setJobStatus(HSSessionItem.JobStatus.DONE);
      setStatus(HSSessionItem.SessionItemStatus.JOB_SET);
    }

    public void jobRunnerConfig(String resultFile, String errorFile,
        String infoFile, int jobid) {
      String cwd = System.getProperty("user.dir");

      setResultFile(new File(cwd + "/pl/" + resultFile));
      setErrorFile(new File(cwd + "/pl/" + errorFile));
      setInfoFile(new File(cwd + "/pl/" + infoFile));
      setJobid(jobid);
    }

    public void jobKillIt() {
      if (p != null) {
        p.destroy();
      }
    }

    public void setJobid(int jobid) {
      this.jobid = jobid;
    }

    public void setSid(String sid) {
      this.sid = sid;
    }

    public String getSid() {
      return sid;
    }

    public void setSvid(String svid) {
      this.svid = svid;
    }

    public String getSvid() {
      return svid;
    }

    public File getResultFile() {
      return resultFile;
    }

    public void setResultFile(File resultFile) {
      this.resultFile = resultFile;
    }

    public File getErrorFile() {
      return errorFile;
    }

    public void setErrorFile(File errorFile) {
      this.errorFile = errorFile;
    }

    public void setInfoFile(File infoFile) {
      this.infoFile = infoFile;
    }

    public File getInfoFile() {
      return infoFile;
    }
  }

  public HSSessionItem(HSAuth auth, String sessionName, TTransport trans)
      throws FileNotFoundException {

    this.jobid = 0;
    this.auth = auth;
    this.sessionName = sessionName;
    this.home = ".";
    this.runable = null;
    this.jr = null;
    this.config = new SessionConfig(jobType.ONESHOT);
    this.jobStatus = HSSessionItem.JobStatus.INIT;
    this.date = new Date();
    this.opdate = this.date;
    if (SessionState.get() != null) {
      this.to = SessionState.get().getConf()
          .getIntVar(HiveConf.ConfVars.HIVESESSIONTIMEOUT);
      if (this.to > 7200 || this.to < 0) {
        this.to = 60;
      }
    } else {
      this.to = 7200;
    }

    this.transSet = new HashSet();
    if (trans != null)
      transSet.add(trans);
    l4j.debug("HSSessionItem created");
    status = SessionItemStatus.NEW;

    l4j.debug("Wait for NEW->READY transition");
    l4j.debug("NEW->READY transition complete");

    queryInfo = new HashMap<Integer, GregorianCalendar>();
    this.current_query_count = 0;

    InetAddress localhost = null;
    try {
      localhost = InetAddress.getLocalHost();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    if (localhost != null)
      hostip = localhost.getHostAddress();
    
    try {
      TSocket tSocket = (TSocket) trans;
      Socket socket = tSocket.getSocket();
      InetAddress addr = socket.getInetAddress();
      this.clientip = addr.getHostAddress();
      l4j.debug("client address: " + addr.getHostAddress() + " client port: " + socket.getPort());
    } catch (Exception e) {
      l4j.debug("get IP, PORT failed in create session");
    }
    
    if ((conf_file_loc == null) || conf_file_loc.length() == 0) {
      return;
    }

    File f = new File(conf_file_loc);
    if (!f.exists()) {
      if (!f.mkdir()) {
        return;
      }
    }

    try {
      sessionFileName = conf_file_loc + "/" + sessionName + ".txt";
      File thefile = new File(sessionFileName);
      if (!thefile.exists()) {
        dayLogStream = new PrintWriter(new FileWriter(sessionFileName, true));
      } else {
        dayLogStream = new PrintWriter(new FileWriter(sessionFileName, true));
        dayLogStream
            .print("+++++++++++++++++++++++++++++++++++++++++++++++++++");
        dayLogStream.print('\n');
        dayLogStream.print("A new session is appended here!  ");
        dayLogStream.print('\n');
        Calendar rightNow = Calendar.getInstance();
        Date time = rightNow.getTime();
        dayLogStream.print(time.toString());
        dayLogStream.print('\n');
        dayLogStream
            .print("+++++++++++++++++++++++++++++++++++++++++++++++++++");
        dayLogStream.print('\n');
      }
    } catch (Exception ex) {
    }
  }

  public tdw_query_info getQInfo() {
    String taskid = SessionState.get().getConf().get("usp.param");
    return new tdw_query_info(getCurrnetQueryId(), getAuth().getUser(),
        getSessionName(), curr_starttime, "", curr_cmd, 0, hostip, taskid,
        "running", null, getAuth().getPort() + "", clientip, getAuth().getDbName());
  }
  
  public tdw_query_error_info getEInfo() {
    String taskid = SessionState.get().getConf().get("usp.param");
    return new tdw_query_error_info(getCurrnetQueryId(), taskid, null,
        hostip, getAuth().getPort() + "", clientip, null, null);
  }

  public PrintWriter getCurrnetPrinter() {
    return dayLogStream;
  }

  public void openPrinter() {
    if (dayLogStream != null) {
      dayLogStream.flush();
      dayLogStream.close();
      dayLogStream = null;
    }
    try {
      if (sessionFileName != null) {
        dayLogStream = new PrintWriter(new FileWriter(sessionFileName, true));
      }
    } catch (Exception e) {
      dayLogStream = null;
      e.printStackTrace();
    }
  }

  public void closePrinter() {
    if (dayLogStream != null) {
      dayLogStream
          .println("session: " + this.sessionName + " has finished !!!");
      dayLogStream.flush();
      dayLogStream.close();
      dayLogStream = null;
    }

    GregorianCalendar gc = new GregorianCalendar();
    curr_starttime = String.format("%1$4d%2$02d%3$02d%4$02d%5$02d%6$02d",
        gc.get(Calendar.YEAR), gc.get(Calendar.MONTH) + 1,
        gc.get(Calendar.DAY_OF_MONTH), gc.get(Calendar.HOUR_OF_DAY),
        gc.get(Calendar.MINUTE), gc.get(Calendar.SECOND));
    String cmd = "DropSession";
    queryInfo.put(current_query_count, gc);
    StringBuffer sb = new StringBuffer();
    sb.append(curr_starttime);
    sb.append(DELIMITER);
    sb.append(getAuth().getUser());
    sb.append(DELIMITER);
    sb.append(getSessionName());
    sb.append(DELIMITER);
    sb.append(getCurrnetQueryId());
    sb.append(DELIMITER);
    sb.append(removen(cmd));
    sb.append(DELIMITER);
    sb.append(RecordTypes.SessionEnd);
    sb.append(DELIMITER);
    sb.append(DELIMITER);
    sb.append(DELIMITER);

    newlog.info(sb.toString());
  }

  public int getCurrnetQueryNum() {
    return current_query_count;
  }

  public String getCurrnetQueryId() {
    return sessionName + "_" + Integer.toString(current_query_count);
  }

  public String removen(String in) {
    if (in == null) {
      return "";
    }
    l4j.info("input:  " + in);
    Pattern p = Pattern.compile("\\n");
    Matcher m = p.matcher(in);
    String ttt = m.replaceAll("");
    l4j.debug("output:  " + ttt);
    return ttt;
  }

  public int startQuery(String cmd) {
    synchronized (this) {
      runSqlCnt++;
      freedate = null;
    }

    current_query_count++;
    GregorianCalendar gc = new GregorianCalendar();
    curr_starttime = String.format("%1$4d%2$02d%3$02d%4$02d%5$02d%6$02d",
        gc.get(Calendar.YEAR), gc.get(Calendar.MONTH) + 1,
        gc.get(Calendar.DAY_OF_MONTH), gc.get(Calendar.HOUR_OF_DAY),
        gc.get(Calendar.MINUTE), gc.get(Calendar.SECOND));
    curr_cmd = cmd;
    queryInfo.put(current_query_count, gc);
    StringBuffer sb = new StringBuffer();
    sb.append(curr_starttime);
    sb.append(DELIMITER);
    sb.append(getAuth().getUser());
    sb.append(DELIMITER);
    sb.append(getSessionName());
    sb.append(DELIMITER);
    sb.append(getCurrnetQueryId());
    sb.append(DELIMITER);
    sb.append(removen(cmd));
    sb.append(DELIMITER);
    sb.append(RecordTypes.QueryStart);
    sb.append(DELIMITER);
    sb.append(DELIMITER);
    sb.append(DELIMITER);

    newlog.info(sb.toString());
    return current_query_count;
  }

  private String getQueryTime(GregorianCalendar stime, GregorianCalendar etime) {

    long diff = (etime.getTimeInMillis() - stime.getTimeInMillis()) / 1000;
    if (diff < 60) {
      return String.format("%1$02d%2$02d%3$02d%4$02d", 0, 0, 0, diff);
    } else if (diff >= 60 && diff < 3600) {
      return String.format("%1$02d%2$02d%3$02d%4$02d", 0, 0, diff / 60,
          diff % 60);
    } else if (diff >= 3600 && diff < (24 * 3600)) {
      return String.format("%1$02d%2$02d%3$02d%4$02d", 0, diff % (24 * 3600)
          / 3600, diff % 3600 / 60, diff % 60);
    } else {
      int day = (int) (diff / (24 * 3600));
      int hour = (int) (diff % (24 * 3600) / 3600);
      int minute = (int) (diff % 3600 / 60);
      int second = (int) (diff % 60);
      return String.format("%1$02d%2$02d%3$02d%4$02d", day, hour, minute,
          second);
    }
  }

  public void endQuery(String cmd, int queryid, int flag, String err) {
    synchronized (this) {
      runSqlCnt--;
    }

    if (flag == 0) {
      GregorianCalendar eee = new GregorianCalendar();
      String etime = String.format("%1$4d%2$02d%3$02d%4$02d%5$02d%6$02d",
          eee.get(Calendar.YEAR), eee.get(Calendar.MONTH) + 1,
          eee.get(Calendar.DAY_OF_MONTH), eee.get(Calendar.HOUR_OF_DAY),
          eee.get(Calendar.MINUTE), eee.get(Calendar.SECOND));
      GregorianCalendar stime = queryInfo.get(queryid);
      StringBuffer sb = new StringBuffer();
      sb.append(etime);
      sb.append(DELIMITER);
      sb.append(getAuth().getUser());
      sb.append(DELIMITER);
      sb.append(getSessionName());
      sb.append(DELIMITER);
      sb.append(sessionName + "_" + Integer.toString(queryid));
      sb.append(DELIMITER);
      sb.append(removen(cmd));
      sb.append(DELIMITER);
      sb.append(RecordTypes.QueryEnd);
      sb.append(DELIMITER);
      sb.append(getQueryTime(stime, eee));
      sb.append(DELIMITER);
      sb.append(Integer.toString(flag));
      sb.append(DELIMITER);

      newlog.info(sb.toString());
      return;
    } else {
      GregorianCalendar eee = new GregorianCalendar();
      String etime = String.format("%1$4d%2$02d%3$02d%4$02d%5$02d%6$02d",
          eee.get(Calendar.YEAR), eee.get(Calendar.MONTH) + 1,
          eee.get(Calendar.DAY_OF_MONTH), eee.get(Calendar.HOUR_OF_DAY),
          eee.get(Calendar.MINUTE), eee.get(Calendar.SECOND));
      GregorianCalendar stime = queryInfo.get(queryid);
      StringBuffer sb = new StringBuffer();
      sb.append(etime);
      sb.append(DELIMITER);
      sb.append(getAuth().getUser());
      sb.append(DELIMITER);
      sb.append(getSessionName());
      sb.append(DELIMITER);
      sb.append(sessionName + "_" + Integer.toString(queryid));
      sb.append(DELIMITER);
      sb.append(removen(cmd));
      sb.append(DELIMITER);
      sb.append(RecordTypes.QueryEnd);
      sb.append(DELIMITER);
      sb.append(getQueryTime(stime, eee));
      sb.append(DELIMITER);
      sb.append(Integer.toString(flag));
      sb.append(DELIMITER);
      sb.append(removen(err));

      newlog.info(sb.toString());
      return;
    }

  }

  public jobType getConfigJob() {
    return config.type;
  }

  public long getConfigJobTi() {
    return config.ti_repeat;
  }

  public String getSessionName() {
    return sessionName;
  }

  public int compareTo(HSSessionItem other) {
    if (other == null) {
      return -1;
    }
    return getSessionName().compareTo(other.getSessionName());
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (!(other instanceof HSSessionItem)) {
      return false;
    }
    HSSessionItem o = (HSSessionItem) other;
    if (getSessionName().equals(o.getSessionName())) {
      return true;
    } else {
      return false;
    }
  }

  public SessionItemStatus getStatus() {
    return status;
  }

  public void setStatus(SessionItemStatus status) {
    this.status = status;
  }

  public HSAuth getAuth() {
    return auth;
  }

  protected void setAuth(HSAuth auth) {
    this.auth = auth;
  }

  protected synchronized void killIt() {
    config.cancelPeriodicJob();

    if (jobStatus != HSSessionItem.JobStatus.RUNNING) {
      return;
    }
    l4j.debug(getSessionName() + " Attempting kill.");
    try {
      if (runable != null)
        runable.interrupt();
      if (jr != null)
        jr.jobKillIt();
      if (runable != null)
        runable.join();
    } catch (InterruptedException ie) {
      l4j.error("Try to join thread failed ", ie);
    }
    jobStatus = HSSessionItem.JobStatus.DONE;
    status = HSSessionItem.SessionItemStatus.JOB_SET;
  }

  protected void freeIt() {
    int i;
    String jobFilePrefix = getHome() + "/pl/TDW_PL_JOB_" + getSessionName()
        + "_";

    killIt();
    for (i = 0; i < jobid; i++) {
      String fileName = jobFilePrefix + Integer.toString(i) + suffix;
      File f = new File(fileName);

      try {
        if (!f.exists()) {
          throw new IllegalArgumentException("Delete: no such file: "
              + fileName);
        }
        if (!f.canWrite()) {
          throw new IllegalArgumentException("Delete: write protected: "
              + fileName);
        }

        boolean success = f.delete();
        if (!success) {
          throw new IllegalArgumentException("Delete: deletion failed.");
        }
      } catch (IllegalArgumentException iae) {
        l4j.error("EX: ", iae);
      }

      if (suffix != null) {
        if (suffix.equals(".py")) {
          fileName = jobFilePrefix + Integer.toString(i) + ".pyc";
          f = new File(fileName);

          try {
            if (!f.exists()) {
              throw new IllegalArgumentException("Delete: no such file: "
                  + fileName);
            }
            if (!f.canWrite()) {
              throw new IllegalArgumentException("Delete: write protected: "
                  + fileName);
            }

            boolean success = f.delete();
            if (!success) {
              throw new IllegalArgumentException("Delete: deletion failed.");
            }
          } catch (IllegalArgumentException iae) {
            l4j.error("EX: ", iae);
          }
        }
      }
      fileName = jobFilePrefix + Integer.toString(i) + "_result";
      f = new File(fileName);

      try {
        if (!f.exists()) {
          throw new IllegalArgumentException("Delete: no such file: "
              + fileName);
        }
        if (!f.canWrite()) {
          throw new IllegalArgumentException("Delete: write protected: "
              + fileName);
        }

        boolean success = f.delete();
        if (!success) {
          throw new IllegalArgumentException("Delete: deletion failed.");
        }
      } catch (IllegalArgumentException iae) {
        l4j.error("EX: ", iae);
      }

      fileName = jobFilePrefix + Integer.toString(i) + "_error";
      f = new File(fileName);

      try {
        if (!f.exists()) {
          throw new IllegalArgumentException("Delete: no such file: "
              + fileName);
        }
        if (!f.canWrite()) {
          throw new IllegalArgumentException("Delete: write protected: "
              + fileName);
        }

        boolean success = f.delete();
        if (!success) {
          throw new IllegalArgumentException("Delete: deletion failed.");
        }
      } catch (IllegalArgumentException iae) {
        l4j.error("EX: ", iae);
      }

      fileName = jobFilePrefix + Integer.toString(i) + "_info";
      f = new File(fileName);

      try {
        if (!f.exists()) {
          throw new IllegalArgumentException("Delete: no such file: "
              + fileName);
        }
        if (!f.canWrite()) {
          throw new IllegalArgumentException("Delete: write protected: "
              + fileName);
        }

        boolean success = f.delete();
        if (!success) {
          throw new IllegalArgumentException("Delete: deletion failed.");
        }
      } catch (IllegalArgumentException iae) {
        l4j.error("EX (you can ignore it): ", iae);
      }

    }
  }

  @Override
  public String toString() {
    return sessionName + ":" + auth.toString();
  }

  public int getCurrentJobid() {
    return jobid;
  }

  public void incJobid() {
    jobid = jobid + 1;
  }

  public boolean submitJob(String jobType, String job) {
    boolean res = true;
    FileOutputStream fos = null;

    if (status == HSSessionItem.SessionItemStatus.JOB_RUNNING) {
      return false;
    } else {
      status = HSSessionItem.SessionItemStatus.JOB_SET;
      setJobStatus(HSSessionItem.JobStatus.SET);
    }

    if (jobType.equals("python")) {
      suffix = ".py";
    } else {
      suffix = "";
    }
    String jobFile = getHome() + "/pl/TDW_PL_JOB_" + getSessionName() + "_"
        + Integer.toString(jobid) + suffix;

    try {
      fos = new FileOutputStream(new File(jobFile));
    } catch (java.io.FileNotFoundException fex) {
      l4j.error(getSessionName() + " opening jobFile " + jobFile, fex);
      return false;
    }

    try {
      fos.write(job.getBytes());
      fos.close();
    } catch (java.io.IOException ex) {
      l4j.error(getSessionName() + " write jobFile " + jobFile, ex);
      res = false;
    }

    return res;
  }

  public String shellUtil(String cmd) throws HiveServerException {
    l4j.info("execute shell: " + cmd);
    Process subproc;
    StringBuffer res = new StringBuffer();

    try {
      subproc = Runtime.getRuntime().exec(cmd);
    } catch (IOException ioe) {
      l4j.error("Runtime.getRuntime().exec()" + cmd + " failed.");
      l4j.error(ioe.getMessage());
      throw new HiveServerException("Run cmd" + cmd + " failed:"
          + ioe.getMessage());
    }

    if (subproc == null) {
      l4j.error("Runtime.getRuntime().exec()" + cmd + " failed.");
      throw new HiveServerException("Run cmd" + cmd + " failed.");
    }
    BufferedReader outBr = new BufferedReader(new InputStreamReader(
        subproc.getInputStream()));
    BufferedReader errBr = new BufferedReader(new InputStreamReader(
        subproc.getErrorStream()));
    try {
      subproc.waitFor();
    } catch (InterruptedException ie) {
      l4j.error(ie.getMessage());
      throw new HiveServerException("Run cmd" + cmd + " failed:"
          + " waitFor() Interrupted");
    }

    int ret = subproc.exitValue();
    try {
      if (ret == 0) {
        String lineStr;
        while ((lineStr = outBr.readLine()) != null) {
          if (res.length() > 0) {
            res.append("\n");
          }
          res.append(lineStr);
        }
        l4j.info("Run cmd OK" + res.toString());
        return res.toString();
      } else {
        String lineStr;
        while ((lineStr = outBr.readLine()) != null) {
          if (res.length() > 0) {
            res.append("\n");
          }
          res.append(lineStr);
        }

        while ((lineStr = errBr.readLine()) != null) {
          if (res.length() > 0) {
            res.append("\n");
          }
          res.append(lineStr);
        }
        l4j.error(res.toString());
        throw new HiveServerException(res.toString());
      }
    } catch (IOException iex) {
      throw new HiveServerException("run cmd IOException");
    }
  }

  public String makejar(String dbName, String tblName, String fileName,
      String userName) throws HiveServerException {
    boolean res = true;
    String scriptPath = getHome() + "/bin/makejar.sh";
    conf = SessionState.get().getConf();
    String url = conf.get("hive.metastore.pbjar.url",
        "jdbc:postgresql://10.136.130.102:5432/pbjar");
    String user = conf.get("hive.metastore.user", "tdw");
    String passwd = conf.get("hive.metastore.passwd", "tdw");
    String protoVersion = conf.get("hive.protobuf.version", "2.3.0");
    
    l4j.info("url: " + url);
    l4j.info("user: " + user);
    l4j.info("passwd: " + passwd);
    String paras = url.toLowerCase() + " " + user.toLowerCase() + " "
        + passwd.toLowerCase() + " " + dbName.toLowerCase() + " "
        + tblName.toLowerCase() + " " + userName.toLowerCase() + " "
        + fileName.toLowerCase() + " " + protoVersion;
    StringBuffer cmd = new StringBuffer();
    cmd.append("sh ");
    cmd.append(scriptPath);
    cmd.append(" ");
    cmd.append(paras);

    return shellUtil(cmd.toString());
  }

  public String preproto(String dbName, String tblName, String fileName,
      String userName) throws HiveServerException {
    boolean res = true;
    String scriptPath = getHome() + "/bin/preproto.sh";
    String paras = dbName.toLowerCase() + " " + tblName.toLowerCase() + " "
        + userName.toLowerCase() + " " + fileName.toLowerCase();
    StringBuffer cmd = new StringBuffer();
    cmd.append("sh ");
    cmd.append(scriptPath);
    cmd.append(" ");
    cmd.append(paras);

    return shellUtil(cmd.toString());
  }

  public String genjar(String dbName, String tblName, String fileName)
      throws HiveServerException {
    boolean res = true;
    String scriptPath = getHome() + "/bin/genjar.sh";
    String paras = dbName.toLowerCase() + " " + tblName.toLowerCase() + " "
        + fileName.toLowerCase();
    StringBuffer cmd = new StringBuffer();
    cmd.append("sh ");
    cmd.append(scriptPath);
    cmd.append(" ");
    cmd.append(paras);

    return shellUtil(cmd.toString());
  }

  public boolean upload(String rtype, String user, String fileName, String data)
      throws HiveServerException {
    boolean res = true;
    String fname;
    if (rtype.equalsIgnoreCase("jar")) {
      fname = getHome() + "/auxlib/" + fileName;
    } else if (rtype.equalsIgnoreCase("proto")) {
      String dname = getHome() + "/protobuf/upload/" + user;
      File d = new File(dname);
      if (!d.exists()) {
        if (!d.mkdirs()) {
          l4j.error(getSessionName() + " try to mkdir " + dname + " failed.");
          throw new HiveServerException("Create user proto directory failed.");
        }
      }
      if (!fileName.trim().toLowerCase().endsWith(".proto")) {
        throw new HiveServerException(
            "Upload proto command can only handle .proto file, Check your file suffix");
      }

      fname = dname + "/" + fileName;
    } else {
      String errorMsg = "Can't upload filetype: " + rtype;
      l4j.error(getSessionName() + " upload failed: " + errorMsg);
      throw new HiveServerException("errorMsg");
    }

    RandomAccessFile raf;
    File f;
    try {
      f = new File(fname);
      if (!f.exists()) {
        if (!f.createNewFile()) {
          l4j.error("Try to create file " + fname + " failed.");
          throw new HiveServerException("Create user upload file " + fname
              + " failed.");
        }
      }
    } catch (java.io.IOException ex) {
      l4j.error(getSessionName() + " try to create file " + fname
          + " failed w/ " + ex);
      return false;
    }
    if (data.equalsIgnoreCase("")) {
      if (!f.delete()) {
        l4j.error("Try to delete file " + fname + " failed.");
        throw new HiveServerException("Delete user file " + fname + " failed.");
      } else {
        return true;
      }
    }

    try {
      raf = new RandomAccessFile(f, "rw");
    } catch (java.io.FileNotFoundException ex) {
      l4j.error(getSessionName() + " try to open file " + fname
          + " failed, not found.");
      return false;
    }
    try {
      raf.setLength(0);
      raf.seek(0);
      raf.write(data.getBytes());
    } catch (java.io.IOException ex) {
      l4j.error(getSessionName() + " try to truncate/write file " + fname
          + "failed w/ " + ex);
      return false;
    }
    return res;
  }

  public boolean uploadModule(String user, String moduleName, byte[] module)
      throws HiveServerException {
    boolean res = true;
    String fname = getHome() + "/pl/lib/" + user + "/" + moduleName;
    RandomAccessFile raf;
    File f;

    String dname = getHome() + "/pl/lib/" + user;
    File d = new File(dname);

    if (!d.exists()) {
      if (!d.mkdir()) {
        l4j.error(getSessionName() + " try to mkdir " + dname + " failed.");
        throw new HiveServerException("Create user library failed.");
      }
    }
    File i = new File(getHome() + "/pl/lib/" + user + "/__init__.py");

    if (!i.exists()) {
      try {
        i.createNewFile();
      } catch (java.io.IOException oe) {
      }
    }

    try {
      f = new File(fname);
      if (!f.exists()) {
        if (!f.createNewFile()) {
          l4j.error("Try to create file " + fname + " failed.");
          throw new HiveServerException("Create user package failed.");
        }
      }
    } catch (java.io.IOException ex) {
      l4j.error(getSessionName() + " try to create file " + fname
          + " failed w/ " + ex);
      return false;
    }
    if (module.length == 0) {
      if (!f.delete()) {
        l4j.error("Try to delete file " + fname + " failed.");
        throw new HiveServerException("Delete user package " + fname
            + " failed.");
      } else {
        return true;
      }
    }

    try {
      raf = new RandomAccessFile(f, "rw");
    } catch (java.io.FileNotFoundException ex) {
      l4j.error(getSessionName() + " try to open file " + fname
          + " failed, not found.");
      return false;
    }
    try {
      raf.setLength(0);
      raf.seek(0);
      raf.write(module);
    } catch (java.io.IOException ex) {
      l4j.error(getSessionName() + " try to truncate/write file " + fname
          + "failed w/ " + ex);
      return false;
    }
    return res;
  }

  public byte[] downloadModule(String user, String moduleName)
      throws HiveServerException {
    String module = "";
    String fname = getHome() + "/pl/lib/" + user + "/" + moduleName;
    File f;

    String dname = getHome() + "/pl/lib/" + user;
    File d = new File(dname);

    if (moduleName.startsWith("gxx_")) {
      fname = getHome() + "/pl/lib/global/" + moduleName;
    } else if (moduleName.contains("/")) {
      fname = getHome() + "/pl/lib/" + moduleName;
    }
    if (!d.exists()) {
      if (!d.mkdir()) {
        l4j.error(getSessionName() + " try to mkdir " + dname + " failed.");
        throw new HiveServerException("Create user library failed.");
      }
    }

    f = new File(fname);
    if (f.canRead() == false) {
      throw new HiveServerException("Try to read file " + fname + " failed.");
    }

    int size = 0;
    long size_org = f.length();
    if (size_org > Integer.MAX_VALUE)
      throw new HiveServerException("File is too large, no more than 2GB");
    else
      size = (int) size_org;

    byte[] bytes = new byte[size];

    try {
      FileInputStream fis = new FileInputStream(f);
      DataInputStream dis = new DataInputStream(fis);

      int read = 0;
      int numRead = 0;
      while (read < bytes.length
          && (numRead = dis.read(bytes, read, bytes.length - read)) >= 0) {
        read = read + numRead;
      }
      fis.close();
    } catch (java.io.IOException ex) {
      l4j.error("IO Exception ", ex);
      throw new HiveServerException("Read file " + fname + " failed w/ " + ex);
    }

    return bytes;
  }

  public List<String> listModule(String user) {
    List<String> list = new ArrayList<String>();
    String[] content;
    File d = new File(getHome() + "/pl/lib/" + user);

    l4j.info("List user lib: " + d.getPath());
    if (!d.exists()) {
      l4j.info("Empty user library: " + user);
      list.add("");
    } else {
      content = d.list();
      for (int i = 0; i < content.length; i++) {
        list.add(content[i]);
      }
    }

    return list;
  }

  public synchronized boolean runJob() {
    boolean res = false;
    String jobFile = "TDW_PL_JOB_" + getSessionName() + "_"
        + Integer.toString(jobid);

    if (getStatus() != HSSessionItem.SessionItemStatus.JOB_SET) {
      return false;
    }
    setStatus(HSSessionItem.SessionItemStatus.JOB_RUNNING);
    jr = new JobRunner(jobFile, getSessionName(), getAuth().toString(),
        getAuth().getUser(), getAuth().getPasswd(), getAuth().getDbName());
    jr.jobRunnerConfig(jobFile + "_result", jobFile + "_error", jobFile
        + "_info", jobid);
    runable = new Thread(jr);
    runable.start();

    incJobid();
    return res;
  }

  public String getHome() {
    return home;
  }

  public void setJobStatus(HSSessionItem.JobStatus jobStatus) {
    this.jobStatus = jobStatus;
  }

  public HSSessionItem.JobStatus getJobStatus() {
    return jobStatus;
  }

  public String getHiveHistroy(int jobid) throws HiveServerException {
    String res = "";

    if (jobid == -1) {
      jobid = this.jobid - 1;
    }
    if (jobid >= this.jobid) {
      res = "The job " + jobid + " does not exist.";
      return res;
    } else if (jobid < -1) {
      res = "The jobid " + jobid + " is invalid.";
      return res;
    } else if (jobid == -1) {
      res = "There is no job upload for now.";
      return res;
    }
    String infoFile = "TDW_PL_JOB_" + getSessionName() + "_"
        + Integer.toString(jobid);
    String histFile;
    File infof = new File(getHome() + "/pl/" + infoFile + "_info");

    try {
      FileInputStream fis = new FileInputStream(infof);
      InputStreamReader isr = new InputStreamReader(fis);
      BufferedReader br = new BufferedReader(isr);

      histFile = br.readLine();
      if (histFile != null) {
        fis.close();
        fis = new FileInputStream(histFile);
        isr = new InputStreamReader(fis);
        br = new BufferedReader(isr);
        String line;

        while ((line = br.readLine()) != null) {
          res += line;
        }
      }
      fis.close();
    } catch (java.io.FileNotFoundException fex) {
      l4j.error("File Not found ", fex);
    } catch (java.io.IOException ex) {
      l4j.error("IO exception", ex);
    }

    return res;
  }

  public List<String> getJobResult(int jobid) throws HiveServerException {
    List<String> res = new ArrayList<String>();

    if (jobid == -1) {
      jobid = this.jobid - 1;
    }
    if (jobid >= this.jobid) {
      res.add("The max jobid is: " + (this.jobid - 1));
      res.add("The job " + jobid + " does not exist.");
      return res;
    } else if (jobid < -1) {
      res.add("The max jobid is: " + (this.jobid - 1));
      res.add("The jobid " + jobid + " is invalid.");
      return res;
    }
    if (jobStatus != HSSessionItem.JobStatus.DONE && jobid == this.jobid - 1) {
      res.add("The max jobid is: " + (this.jobid - 1));
      res.add("The job is in state " + jobStatus.toString());
      return res;
    }

    res.add("The max jobid is: " + (this.jobid - 1));
    String jobFile = "TDW_PL_JOB_" + getSessionName() + "_"
        + Integer.toString(jobid);
    String cwd = System.getProperty("user.dir");
    try {
      FileInputStream fis = new FileInputStream(new File(cwd + "/pl/" + jobFile
          + "_result"));
      InputStreamReader isr = new InputStreamReader(fis);
      BufferedReader br = new BufferedReader(isr);
      String line;

      while ((line = br.readLine()) != null) {
        res.add(line);
      }
    } catch (java.io.FileNotFoundException fex) {
      l4j.error("Read job result file ", fex);
      throw new HiveServerException("The result/error file of job "
          + Integer.toString(jobid) + " is not found");
    } catch (java.io.IOException ex) {
      l4j.error("Read job result file ", ex);
      throw new HiveServerException(
          "IOException on reading result/error file of job "
              + Integer.toString(jobid));
    }
    try {
      FileInputStream fis = new FileInputStream(new File(cwd + "/pl/" + jobFile
          + "_error"));
      InputStreamReader isr = new InputStreamReader(fis);
      BufferedReader br = new BufferedReader(isr);
      String line;

      while ((line = br.readLine()) != null) {
        res.add(line);
      }
    } catch (java.io.FileNotFoundException fex) {
      l4j.error("Read job error file ", fex);
    } catch (java.io.IOException ex) {
      l4j.error("Read job error file ", ex);
    }

    return res;
  }

  public boolean audit(String user, String passwd) {
    if (getAuth().getUser() == null || getAuth().getPasswd() == null) {
      return true;
    }
    if (user == null) {
      return false;
    }
    if (getAuth().getUser().equals(user)) {
      return true;
    } else if (user.equals("root")) {
      return true;
    } else {
      return false;
    }
  }

  public void prepareJobFile() throws HiveServerException {
    String lastJobFile = getHome() + "/pl/TDW_PL_JOB_" + getSessionName() + "_"
        + Integer.toString(jobid - 1) + suffix;
    String thisJobFile = getHome() + "/pl/TDW_PL_JOB_" + getSessionName() + "_"
        + Integer.toString(jobid) + suffix;

    try {
      FileInputStream fis = new FileInputStream(new File(lastJobFile));
      FileOutputStream fos = new FileOutputStream(new File(thisJobFile));
      InputStreamReader isr = new InputStreamReader(fis);
      BufferedReader br = new BufferedReader(isr);
      String line;

      while ((line = br.readLine()) != null) {
        fos.write(line.getBytes());
        fos.write('\n');
      }
    } catch (java.io.FileNotFoundException fex) {
      throw new HiveServerException("File not found @ Timer.run() ");
    } catch (java.io.IOException ex) {
      throw new HiveServerException("File IOException @ Timer.run() ");
    }
  }

  public Date getDate() {
    return date;
  }

  public Date getOpdate() {
    return opdate;
  }

  public void setOpdate() {
    opdate = new Date();
  }

  public void addTrans(TTransport trans) {
    if (trans == null)
      return;

    lock.lock();
    try {
      transSet.add(trans);
    } finally {
      lock.unlock();
    }
  }

  public void removeTrans(TTransport trans) {
    if (trans == null)
      return;

    lock.lock();
    try {
      transSet.remove(trans);
    } finally {
      lock.unlock();
    }
  }

  public synchronized boolean isActive() {
    boolean res = false;

    lock.lock();
    try {
      Iterator<TTransport> i = transSet.iterator();

      while (i.hasNext()) {
        TTransport trans = (TTransport) i.next();

        if (trans.isOpen() == true) {
          res = true;
        } else {
          l4j.info("Session " + sessionName + " Connection disconnected!");
          i.remove();
        }
      }
    } finally {
      lock.unlock();
    }

    return res;
  }

  public synchronized boolean isInactive() {
    boolean res = true;
    Date currentDate = new Date();

    lock.lock();
    try {
      Iterator<TTransport> i = transSet.iterator();

      while (i.hasNext()) {
        TTransport trans = (TTransport) i.next();

        if (trans.isOpen() == true) {
          res = false;
        } else {
          l4j.info("Session " + sessionName + " Connection disconnected!");
          i.remove();
        }
      }
    } finally {
      lock.unlock();
    }

    if (!res) {
      if (this.runSqlCnt == 0) {
        if (this.freedate == null) {
          this.freedate = new Date();
        } else {
          if (currentDate.getTime() - this.freedate.getTime() >= 300 * 1000) {
            if (dayLogStream != null) {
              dayLogStream
                  .print("update session log, the connection is still alive");
              dayLogStream.print('\n');
              dayLogStream.flush();
            }
            this.freedate = new Date();
          }
        }
      }
      return false;
    }

    if (config.timer != null) {
      return false;
    }

    if (currentDate.getTime() - opdate.getTime() >= to * 1000) {
      l4j.info("Time @ " + currentDate.toString() + " Set empty: "
          + Boolean.toString((transSet.isEmpty())));
      return true;
    } else
      return false;
  }

  public String getPBTableModifiedTime(String DBName, String tableName)
      throws HiveServerException {
    l4j.info("get PB Table Modified Time of " + DBName + "::" + tableName);
    String modified_time = null;

    conf = SessionState.get().getConf();
    String url = conf.get("hive.metastore.pbjar.url",
        "jdbc:postgresql://10.136.130.102:5432/pbjar");
    String user = conf.get("hive.metastore.user", "tdw");
    String passwd = conf.get("hive.metastore.passwd", "tdw");
    String protoVersion = conf.get("hive.protobuf.version", "2.3.0");

    String driver = "org.postgresql.Driver";
    ResultSet rs = null;
    Connection connect = null;
    PreparedStatement ps = null;
    try {
      Class.forName(driver).newInstance();
    } catch (InstantiationException e2) {
      e2.printStackTrace();
    } catch (IllegalAccessException e2) {
      e2.printStackTrace();
    } catch (ClassNotFoundException e2) {
      e2.printStackTrace();
    }
    try {
      connect = DriverManager.getConnection(url, user, passwd);
      try {
        String processName = java.lang.management.ManagementFactory
            .getRuntimeMXBean().getName();
        String processID = processName.substring(0, processName.indexOf('@'));
        String appinfo = "getPBTableModifiedTime_" + processID + "_"
            + SessionState.get().getSessionName();
        connect.setClientInfo("ApplicationName", appinfo);
      } catch (Exception e) {
        e.printStackTrace();
      }
    } catch (SQLException e2) {
      e2.printStackTrace();
    }
    try {
      ps = connect
          .prepareStatement("SELECT to_char(modified_time,'yyyymmddhh24miss') as modified_time "
              + " FROM pb_proto_jar WHERE db_name=? and tbl_name=? and protobuf_version=? order by modified_time desc limit 1");
    } catch (SQLException e1) {
      e1.printStackTrace();
    }
    try {
      ps.setString(1, DBName.toLowerCase());
    } catch (SQLException e1) {
      e1.printStackTrace();
    }
    try {
      ps.setString(2, tableName.toLowerCase());
      ps.setString(3, protoVersion);
    } catch (SQLException e1) {
      e1.printStackTrace();
    }
    try {
      l4j.info("select modifiedtime from tpg");
      rs = ps.executeQuery();
    } catch (SQLException e1) {
      e1.printStackTrace();
    }
    try {
      if (rs.next()) {
        modified_time = rs.getString("modified_time");
      } else {
        l4j.info("Can not find the modified_time of " + DBName + "::"
            + tableName + " in the tPG.");
      }
    } catch (SQLException e1) {
      e1.printStackTrace();
    }
    try {
      rs.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    try {
      ps.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return modified_time;
  }

  public List<FieldSchema> getColsFromJar(String DBName, String tableName)
      throws HiveServerException {
    l4j.info("in getColsFromJar DBName: " + DBName + " tableName :" + tableName);
    Descriptor message;
    String modified_time = null;
    String jar_path = null;
    l4j.info("dbName: " + DBName);
    l4j.info("tableName: " + tableName);
    try {
      modified_time = PBJarTool.downloadjar(DBName.toLowerCase(), 
          tableName.toLowerCase(), SessionState.get().getConf());
    } catch (SemanticException e1) {
      // TODO Auto-generated catch block
      throw new HiveServerException(e1.getMessage());
    }
    jar_path = getHome() + "/auxlib/" + DBName.toLowerCase() + "_"
        + tableName.toLowerCase() + "_" + modified_time + ".jar";
    String full_name = PBJarTool.protobufPackageName + "." + DBName.toLowerCase() + "_"
        + tableName.toLowerCase() + "_" + modified_time + "$"
        + tableName.toLowerCase();
    boolean flag = SessionState.get().delete_resource(
        SessionState.ResourceType.JAR, jar_path);

    if (flag) {
      l4j.info("Deleted " + jar_path + " from class path");
    }
    SessionState.get();
    flag = SessionState.unregisterJar(jar_path);
    if (flag) {
      l4j.info("clear  " + jar_path + " from class path");
    }
    SessionState.get().add_resource(SessionState.ResourceType.JAR, jar_path);

    try {
      Class<?> outer = Class.forName(full_name, true,
          JavaUtils.getpbClassLoader());
      Method m = outer.getMethod("getDescriptor", (Class<?>[]) null);
      message = (Descriptor) m.invoke(null, (Object[]) null);
    } catch (ClassNotFoundException e) {
      throw new HiveServerException("Can't find Class: " + full_name
          + " check if " + DBName.toLowerCase() + "_" + tableName.toLowerCase()
          + "_" + modified_time + ".jar is created by makejar");
    } catch (Exception e) {
      throw new HiveServerException(e.getMessage());
    }
    List<FieldSchema> colList = new ArrayList<FieldSchema>();

    List<FieldDescriptor> fields = message.getFields();
    for (FieldDescriptor field : fields) {
      FieldSchema col = new FieldSchema();
      String name = field.getName();
      col.setName(name);
      String type = getTypeStringFromfield(field, true);
      col.setType(type);
      l4j.info("Xtodo !!add a col " + name + ": " + type);
      colList.add(col);
    }
    return colList;

  }

  private String getTypeStringFromfield(FieldDescriptor field,
      boolean checkrepeat) {
    String typeStr;
    if (checkrepeat && field.isRepeated()) {
      typeStr = Constants.LIST_TYPE_NAME + "<"
          + getTypeStringFromfield(field, false) + ">";
      l4j.info("Xtodo checkrepeat && field.isRepeated()" + typeStr);
      return typeStr;
    }

    if (field.getType() == FieldDescriptor.Type.MESSAGE
        || field.getType() == FieldDescriptor.Type.GROUP) {
      typeStr = getStructTypeStringFromMsg(field.getMessageType());
    } else {
      typeStr = getHivePrimitiveTypeFromPBPrimitiveType(field.getType()
          .toString());
    }

    return typeStr;
  }

  private String getStructTypeStringFromMsg(Descriptor msg) {
    String typeStr = Constants.STRUCT_TYPE_NAME + "<";
    List<FieldDescriptor> fields = msg.getFields();
    int children = fields.size();
    for (int i = 0; i < children; i++) {
      FieldDescriptor field = fields.get(i);
      typeStr += field.getName() + ":";
      typeStr += getTypeStringFromfield(field, true);
      if (i < children - 1) {
        typeStr += ",";
      }
    }
    typeStr += ">";
    return typeStr;
  }

  private String getHivePrimitiveTypeFromPBPrimitiveType(String type) {
    if (type.equalsIgnoreCase("double")) {
      return Constants.DOUBLE_TYPE_NAME;
    } else if (type.equalsIgnoreCase("int32")) {
      return Constants.INT_TYPE_NAME;
    } else if (type.equalsIgnoreCase("int64")) {
      return Constants.BIGINT_TYPE_NAME;
    } else if (type.equalsIgnoreCase("uint32")) {
      return Constants.INT_TYPE_NAME;
    } else if (type.equalsIgnoreCase("uint64")) {
      return Constants.BIGINT_TYPE_NAME;
    } else if (type.equalsIgnoreCase("sint32")) {
      return Constants.INT_TYPE_NAME;
    } else if (type.equalsIgnoreCase("sint64")) {
      return Constants.BIGINT_TYPE_NAME;
    } else if (type.equalsIgnoreCase("fixed32")) {
      return Constants.INT_TYPE_NAME;
    } else if (type.equalsIgnoreCase("fixed64")) {
      return Constants.BIGINT_TYPE_NAME;
    } else if (type.equalsIgnoreCase("sfixed32")) {
      return Constants.INT_TYPE_NAME;
    } else if (type.equalsIgnoreCase("sfixed64")) {
      return Constants.BIGINT_TYPE_NAME;
    } else if (type.equalsIgnoreCase("bool")) {
      return Constants.BOOLEAN_TYPE_NAME;
    } else if (type.equalsIgnoreCase("string")) {
      return Constants.STRING_TYPE_NAME;
    } else if (type.equalsIgnoreCase("float")) {
      return Constants.FLOAT_TYPE_NAME;
    } else {
      l4j.error("not hive type for pb type : " + type);
      return null;
    }
  }

  public boolean uploadProto(String user, String fileName, byte[] array)
      throws HiveServerException {
    boolean res = true;
    String fname;

    String dname = getHome() + "/protobuf/upload/" + user;
    File d = new File(dname);
    if (!d.exists()) {
      if (!d.mkdirs()) {
        l4j.error(getSessionName() + " try to mkdir " + dname + " failed.");
        throw new HiveServerException("Create user proto directory failed.");
      }
    }
    if (!fileName.trim().toLowerCase().endsWith(".proto")) {
      throw new HiveServerException(
          "Upload proto command can only handle .proto file, Check your file suffix");
    }

    fname = dname + "/" + fileName;

    RandomAccessFile raf;
    File f;
    try {
      f = new File(fname);
      if (!f.exists()) {
        if (!f.createNewFile()) {
          l4j.error("Try to create file " + fname + " failed.");
          throw new HiveServerException("Create user upload file " + fname
              + " failed.");
        }
      }
    } catch (java.io.IOException ex) {
      l4j.error(getSessionName() + " try to create file " + fname
          + " failed w/ " + ex);
      return false;
    }
    if (array.length == 0) {
      if (!f.delete()) {
        l4j.error("Try to delete file " + fname + " failed.");
        throw new HiveServerException("Delete user proto file " + fname
            + " failed.");
      } else {
        return true;
      }
    }
    try {
      raf = new RandomAccessFile(f, "rw");
    } catch (java.io.FileNotFoundException ex) {
      l4j.error(getSessionName() + " try to open file " + fname
          + " failed, not found.");
      return false;
    }
    try {
      raf.setLength(0);
      raf.seek(0);
      raf.write(array);
    } catch (java.io.IOException ex) {
      l4j.error(getSessionName() + " try to truncate/write file " + fname
          + "failed w/ " + ex);
      return false;
    }
    return res;
  }

}
