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

package org.apache.hadoop.hive.ql;

import java.io.DataInput;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.util.*;
import java.util.Map.Entry;

import org.antlr.runtime.CommonToken;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ASTNode;

import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.metastore.api.tdw_query_error_info;
import org.apache.hadoop.hive.metastore.api.tdw_query_info;
import org.apache.hadoop.hive.ql.parse.ACLSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.DDLSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.LoadSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ExplainSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.ExecDriver;
import org.apache.hadoop.hive.ql.exec.InsertExeInfo;
import org.apache.hadoop.hive.ql.exec.MapRedTask;
import org.apache.hadoop.hive.ql.exec.MoveTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.Task.ExecResultCounter;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.TaskResult;
import org.apache.hadoop.hive.ql.exec.TaskRunner;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.PreExecute;
import org.apache.hadoop.hive.ql.history.HiveHistory.Keys;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.plan.loadFileDesc;
import org.apache.hadoop.hive.ql.plan.loadTableDesc;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.plan.moveWork;
import org.apache.hadoop.hive.ql.plan.partitionDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.serde2.ByteStream;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Driver implements CommandProcessor {

  static final private Log LOG = LogFactory.getLog(Driver.class.getName());
  static final private LogHelper console = new LogHelper(LOG);

  private int maxRows = 100;
  ByteStream.Output bos = new ByteStream.Output();

  private HiveConf conf;
  private DataInput resStream;
  private Context ctx;
  private QueryPlan plan;

  private QueryPlan actualPlan = null;

  public ArrayList<Task.ExecResInfo> execinfos = new ArrayList<Task.ExecResInfo>();
  private Map<String, String> tmpFiles;

  private Map<String, String> tmpPgFiles;

  private List<String> withTmpTabs = null;
  private boolean hasWith = false;
  private boolean initialParallelMode = false;

  private Map<String, String> confMap = new HashMap<String, String>();
  private Map<String, String> runningMap = new HashMap<String, String>();
  private int jobNumber = 0;
  private int curJobIndex = 0;
  private Task currentTask = null;
  private TaskResult sequentialTaskResult = null;
  private boolean parellMode = false;
  private Collection<TaskRunner> runningJobs = null;
  private int jobHasDoneCount = 0;
  private boolean jobNumIsInit = false;

  private int execRet = -1;

  private SQLProcessState processState = SQLProcessState.INIT;

  private String errorMsg = null;

  private Socket socket = null;

  private List<Task<? extends Serializable>> moveTasks = new ArrayList<Task<? extends Serializable>>();
  private Map<String, InsertExeInfo> insertInfoMap = new HashMap<String, InsertExeInfo>();

  Random rand = new Random();
  public boolean isAInsertQuery() {
    return moveTasks.size() > 0;
  }

  public String getDestTableName(Task<? extends Serializable> task) {
    if (task instanceof MoveTask) {
      return getDestTableNameFromMoveTask(task);
    } else {
      return getDestTableNameFromCondTask(task);
    }
  }

  public String getDestTableNameFromMoveTask(Task<? extends Serializable> task) {
    if (task instanceof MoveTask) {
      moveWork mvWork = (moveWork) task.getWork();
      String tblName = "";
      if (mvWork.getLoadTableWork() != null) {
        tableDesc tblDesc = mvWork.getLoadTableWork().getTable();
        tblName += tblDesc.getDBName() + "::" + tblDesc.getTableName();
      } else if (mvWork.getLoadFileWork() != null) {
        tblName += mvWork.getLoadFileWork().getTargetDir();
      }

      return tblName;
    }

    return null;
  }

  public String getDestTableNameFromCondTask(Task<? extends Serializable> task) {
    if (task instanceof ConditionalTask) {
      ConditionalTask conTask = (ConditionalTask) task;
      List<Task<? extends Serializable>> conTaskList = conTask.getListTasks();
      if (conTaskList == null || conTaskList.isEmpty()) {
        return null;
      }

      Task<? extends Serializable> lastTask = getLastTask(conTaskList.get(0));

      if (lastTask instanceof MoveTask) {
        moveWork mvWork = (moveWork) lastTask.getWork();
        String tblName = "";
        if (mvWork.getLoadTableWork() != null) {
          tableDesc tblDesc = mvWork.getLoadTableWork().getTable();
          tblName += tblDesc.getDBName() + "::" + tblDesc.getTableName();
        } else if (mvWork.getLoadFileWork() != null) {
          tblName += mvWork.getLoadFileWork().getTargetDir();
        }

        return tblName;
      }

    }

    return null;
  }

  public Task<? extends Serializable> getLastTask(
      Task<? extends Serializable> task) {
    if (task == null)
      return null;

    if (task.getChildTasks() == null || task.getChildTasks().isEmpty()) {
      return task;
    } else {
      return getLastTask(task.getChildTasks().get(0));
    }
  }

  public void computeExecResultCounterForInsert(
      Task<? extends Serializable> task) {
    if (((task instanceof ConditionalTask) || (task instanceof MoveTask))) {
      List<Task<? extends Serializable>> parentTasks = task.getParentTasks();
      if (parentTasks == null || parentTasks.isEmpty()) {
        return;
      }

      Task<? extends Serializable> parentTask = parentTasks.get(0);
      boolean isSrcExt = false;

      if (parentTask.isMapRedTask()) {
        ExecDriver mrTask = (ExecDriver) parentTask;
        mapredWork mrConf = mrTask.getWork();
        Collection<partitionDesc> partitionDescList = mrConf
            .getAliasToPartnInfo().values();

        for (partitionDesc desc : partitionDescList) {
          tableDesc td = desc.getTableDesc();

          Properties tblProps = td.getProperties();
          if (tblProps.getProperty("EXTERNAL") != null
              && tblProps.getProperty("EXTERNAL").equalsIgnoreCase("true"))

          {
            isSrcExt = true;
          } else {
            isSrcExt = false;
            break;
          }
        }

        if (partitionDescList.size() > 1) {
          isSrcExt = false;
        }

        InsertExeInfo insertInfo = new InsertExeInfo();
        String tblName = getDestTableName(task);
        insertInfo.setDestTable(tblName);

        if (!isSrcExt) {
          ExecResultCounter execResultCountForInsert = parentTask.execResultCount;
          insertInfo
              .setFsSuccessNum(execResultCountForInsert.fileSinkSuccessNum);
          insertInfo.setFsRejectNum(execResultCountForInsert.fileSinkErrorNum);
        } else {
          ExecResultCounter execResultCountForInsert = parentTask.execResultCount;
          insertInfo
              .setFsSuccessNum(execResultCountForInsert.fileSinkSuccessNum);
          insertInfo.setFsRejectNum(execResultCountForInsert.readErrorNum
              + execResultCountForInsert.selectErrorNum);
        }

        insertInfoMap.put(tblName, insertInfo);
      }

      return;
    } else {
      computeExecResultCounterForInsert(task.getChildTasks());
      return;
    }
  }

  public void computeExecResultCounterForInsert(
      List<Task<? extends Serializable>> rootTasks) {
    if (rootTasks == null || rootTasks.size() == 0)
      return;

    for (Task<? extends Serializable> task : rootTasks) {
      computeExecResultCounterForInsert(task);
    }

    return;
  }

  public void adjustCounters() {
    int size = insertInfoMap.size();

    if (size > 1) {
      for (Entry<String, InsertExeInfo> info : insertInfoMap.entrySet()) {
        info.getValue().setFsSuccessNum(
            info.getValue().getFsSuccessNum() / size);
        info.getValue().setFsRejectNum(info.getValue().getFsRejectNum() / size);
      }
    }
  }

  public void setSocket(Socket socket) {
    this.socket = socket;
  }

  public int queryProcessCompute() {
    if (parellMode) {
      if (runningJobs == null || runningJobs.isEmpty()) {
        if (jobNumber < 0)
          return 0;
        if (jobNumber == 0 && jobNumIsInit)
          return 100;
        else
          return (int) (((float) jobHasDoneCount / jobNumber) * 100);
      } else {
        if (jobNumber < 0)
          return 0;
        if (jobNumber == 0 && jobNumIsInit)
          return 100;
        else {
          double processing = ((float) jobHasDoneCount / jobNumber) * 100;

          for (TaskRunner taskRunner : runningJobs) {
            ExecDriver mapRedTask = (ExecDriver) taskRunner.getTask();
            if (mapRedTask instanceof ExecDriver) {
              int mapRedProcess = mapRedTask.queryProcessComputer();
              processing += (float) mapRedProcess / jobNumber;
            }
          }

          return (int) (processing);
        }
      }
    } else {
      if (currentTask == null) {
        if (jobNumber < 0)
          return 0;
        if (jobNumber == 0 && jobNumIsInit)
          return 100;
        else
          return (int) (((float) curJobIndex / jobNumber) * 100);
      } else {
        if (jobNumber < 0)
          return 0;
        if (jobNumber == 0 && jobNumIsInit)
          return 100;
        else {
          if (currentTask instanceof ExecDriver) {
            ExecDriver mapRedTask = (ExecDriver) currentTask;
            int mapRedProcess = mapRedTask.queryProcessComputer();
            return (int) (((float) curJobIndex * 100 + (float) mapRedProcess) / jobNumber);
          }
          return (int) (((float) curJobIndex / jobNumber) * 100);
        }
      }
    }
  }

  public int killCurrentJobs() throws IOException {
    if (parellMode) {
      if (runningJobs == null || runningJobs.isEmpty())
        return -1;

      for (TaskRunner taskRunner : runningJobs) {
        ExecDriver mapRedTask = (ExecDriver) taskRunner.getTask();
        if (mapRedTask instanceof ExecDriver) {
          mapRedTask.killJob();
        }
      }
      return 1;
    } else {
      if (currentTask == null)
        return -1;

      if (currentTask instanceof ExecDriver) {
        ExecDriver mapRedTask = (ExecDriver) currentTask;
        mapRedTask.killJob();
        return 1;
      } else
        return -1;
    }
  }

  public String getJobID() throws IOException {
    if (parellMode) {
      if (runningJobs == null || runningJobs.isEmpty())
        return null;

      StringBuilder sb = new StringBuilder();
      for (TaskRunner taskRunner : runningJobs) {
        ExecDriver mapRedTask = (ExecDriver) taskRunner.getTask();
        if (mapRedTask instanceof ExecDriver) {
          String jobid = mapRedTask.getJobID();
          sb.append(jobid);
          sb.append(";");
        }
      }
      return sb.toString();
    } else {
      if (currentTask == null)
        return null;
      if (currentTask instanceof ExecDriver) {
        ExecDriver mapRedTask = (ExecDriver) currentTask;
        return mapRedTask.getJobID();
      } else
        return null;
    }
  }

  public String getJobURL() throws IOException {
    if (parellMode) {
      if (runningJobs == null || runningJobs.isEmpty())
        return null;

      StringBuilder sb = new StringBuilder();
      for (TaskRunner taskRunner : runningJobs) {
        ExecDriver mapRedTask = (ExecDriver) taskRunner.getTask();
        if (mapRedTask instanceof ExecDriver) {
          String jobid = mapRedTask.getJobURL();
          sb.append(jobid);
          sb.append(";");
        }
      }
      return sb.toString();
    } else {
      if (currentTask == null)
        return null;
      if (currentTask instanceof ExecDriver) {
        ExecDriver mapRedTask = (ExecDriver) currentTask;
        return mapRedTask.getJobURL();
      } else
        return null;
    }
  }

  public String getJobName() throws IOException {
    if (parellMode) {
      if (runningJobs == null || runningJobs.isEmpty())
        return null;

      StringBuilder sb = new StringBuilder();
      for (TaskRunner taskRunner : runningJobs) {
        ExecDriver mapRedTask = (ExecDriver) taskRunner.getTask();
        if (mapRedTask instanceof ExecDriver) {
          String jobid = mapRedTask.getJobName();
          sb.append(jobid);
          sb.append(";");
        }
      }
      return sb.toString();
    } else {
      if (currentTask == null)
        return null;
      if (currentTask instanceof ExecDriver) {
        ExecDriver mapRedTask = (ExecDriver) currentTask;
        return mapRedTask.getJobName();
      } else
        return null;
    }
  }

  public enum SQLProcessState {
    INIT, PROCESSING, COMPLETE, ERROR
  }

  public SQLProcessState getProcessState() {
    return processState;
  }

  public void setProcessState(SQLProcessState state) {
    processState = state;
  }

  public String getErrorMsg() {
    return errorMsg;
  }

  public void setErrorMsg(String msg) {
    errorMsg = msg;
  }

  public Map<String, String> getJobCounts() {
    if (currentTask == null)
      return null;
    if (currentTask instanceof ExecDriver) {
      ExecDriver mapRedTask = (ExecDriver) currentTask;
      return mapRedTask.getJobCounts();
    } else
      return null;
  }

  private int maxthreads = 8;
  private int sleeptime = 2000;

  public int countJobs(List<Task<? extends Serializable>> tasks) {
    return countJobs(tasks, new ArrayList<Task<? extends Serializable>>());
  }

  public int countJobs(List<Task<? extends Serializable>> tasks,
      List<Task<? extends Serializable>> seenTasks) {
    if (tasks == null)
      return 0;
    int jobs = 0;
    for (Task<? extends Serializable> task : tasks) {
      if (!seenTasks.contains(task)) {
        seenTasks.add(task);
        LOG.debug(task.toString());
        if (task instanceof ConditionalTask)
          jobs += countJobs(((ConditionalTask) task).getListTasks(), seenTasks);
        else if (task.isMapRedTask()) {
          jobs++;
        } else if (task instanceof MoveTask) {
          moveTasks.add((MoveTask) task);
        }

        jobs += countJobs(task.getChildTasks(), seenTasks);
      }
    }
    return jobs;
  }

  public ClusterStatus getClusterStatus() throws Exception {
    ClusterStatus cs;
    try {
      JobConf job = new JobConf(conf, ExecDriver.class);
      JobClient jc = new JobClient(job);
      cs = jc.getClusterStatus();
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    LOG.info("Returning cluster status: " + cs.toString());
    return cs;
  }

  public Schema getSchema() throws Exception {
    Schema schema;
    try {
      if (plan != null && plan.getPlan().getFetchTask() != null) {
        BaseSemanticAnalyzer sem = plan.getPlan();

        if (!sem.getFetchTaskInit()) {
          LOG.debug("getSchema...initFetchTask");
          sem.setFetchTaskInit(true);
          sem.getFetchTask().initialize(conf, null);
        }
        FetchTask ft = (FetchTask) sem.getFetchTask();

        tableDesc td = ft.getTblDesc();

        if (td == null) {
          throw new Exception("No table description found for fetch task: "
              + ft);
        }

        String tableName = "result";
        List<FieldSchema> lst = MetaStoreUtils.getFieldsFromDeserializer(
            tableName, td.getDeserializer());

        if (td.getIsSelectOp()) {
          lst.clear();
          Vector<Integer> mid = td.getSelectCols();
          List<FieldSchema> allfs = td.getColsSchema();
          Iterator<Integer> itt = mid.iterator();
          while (itt.hasNext()) {
            Integer index = (Integer) itt.next();
            if (index.intValue() < allfs.size())
              lst.add(allfs.get(index.intValue()));
          }
        }

        schema = new Schema(lst, null);
      } else {
        LOG.debug("new a schema");
        schema = new Schema();
      }

      LOG.debug("schema :" + schema.toString());
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    LOG.info("Returning Hive schema: " + schema);
    return schema;
  }

  public Schema getSchemaForExplain() throws Exception {
    Schema schema;
    try {
      if (actualPlan != null && actualPlan.getPlan().getFetchTask() != null) {
        BaseSemanticAnalyzer sem = actualPlan.getPlan();
        if (!sem.getFetchTaskInit()) {
          LOG.debug("getSchema...initFetchTask");
          sem.setFetchTaskInit(true);
          sem.getFetchTask().initialize(conf, null);
        }

        FetchTask ft = (FetchTask) sem.getFetchTask();
        tableDesc td = ft.getTblDesc();

        if (td == null) {
          throw new Exception("No table description found for fetch task: "
              + ft);
        }

        String tableName = "result";
        List<FieldSchema> lst = MetaStoreUtils.getFieldsFromDeserializer(
            tableName, td.getDeserializer());

        if (td.getIsSelectOp()) {
          lst.clear();
          Vector<Integer> mid = td.getSelectCols();
          List<FieldSchema> allfs = td.getColsSchema();
          Iterator<Integer> itt = mid.iterator();

          while (itt.hasNext()) {
            Integer index = (Integer) itt.next();
            if (index.intValue() < allfs.size())
              lst.add(allfs.get(index.intValue()));
          }
        }
        schema = new Schema(lst, null);
      } else {
        LOG.debug("new a schema");
        schema = new Schema();
      }

      LOG.debug("schema :" + schema.toString());
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }

    LOG.info("Returning Hive schema: " + schema);
    return schema;
  }

  public Schema getThriftSchema() throws Exception {
    Schema schema;
    try {
      schema = this.getSchema();
      if (schema != null) {
        List<FieldSchema> lst = schema.getFieldSchemas();
        if (lst != null) {
          for (FieldSchema f : lst) {
            f.setType(MetaStoreUtils.typeToThriftType(f.getType()));
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
    LOG.info("Returning Thrift schema: " + schema);
    return schema;
  }

  public int getMaxRows() {
    return maxRows;
  }

  public void setMaxRows(int maxRows) {
    this.maxRows = maxRows;
  }

  public boolean hasReduceTasks(List<Task<? extends Serializable>> tasks) {
    if (tasks == null)
      return false;

    boolean hasReduce = false;
    for (Task<? extends Serializable> task : tasks) {
      if (task.hasReduce()) {
        return true;
      }

      hasReduce = (hasReduce || hasReduceTasks(task.getChildTasks()));
    }
    return hasReduce;
  }

  public Driver(String user, String db) {
    if (SessionState.get() != null) {
      conf = SessionState.get().getConf();
    }
    ctx = new Context(conf, user, db);
  }

  public Driver(HiveConf conf, String user, String db) {
    this.conf = conf;
    getSessionState();
    ctx = new Context(conf, user, db);
  }

  public Driver(HiveConf conf) {
    this.conf = conf;
    getSessionState();
    ctx = new Context(conf, SessionState.get().getUserName(), SessionState
        .get().getDbName());
  }

  public Driver() {
    if (SessionState.get() != null) {
      conf = SessionState.get().getConf();
    }

    ctx = new Context(conf, SessionState.get().getUserName(), SessionState
        .get().getDbName());
  }

  private SessionState getSessionState() {
    if (SessionState.get() == null) {
      SessionState ss = new SessionState(conf);
      ss.in = System.in;
      try {
        ss.out = new PrintStream(System.out, true, "UTF-8");
        ss.err = new PrintStream(System.err, true, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        System.exit(3);
      }
      SessionState.start(ss);
    }
    return SessionState.get();

  }

  public int compile(String command) {
    if (plan != null) {
      close();
      plan = null;
    }

    TaskFactory.resetId();

    BaseSemanticAnalyzer tmp_sem = null;

    try {

      ParseDriver pd = new ParseDriver();
      ctx.setTokenRewriteStream(null);
      ASTNode tree = pd.parse(command, ctx);
      tree = ParseUtils.findRootNonNullToken(tree);

      if (pd.isCheck() && SessionState.get().isCheck()) {
        LOG.info("CHECK SQL OK,exit!");
        if (SessionState.get() != null)
          SessionState.get().ssLog("CHECK SQL OK,exit!");
        return 0;
      }

      BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(conf, tree);

      tmp_sem = sem;

      if (SessionState.get() != null)
          SessionState.get().ssLog("Semantic Analysis begin");      
      sem.analyze(tree, ctx);
      ctx.setTokenRewriteStream(null);
      LOG.info("Semantic Analysis Completed");
      if (SessionState.get() != null)
        SessionState.get().ssLog("Semantic Analysis Completed");
      if (sem instanceof SemanticAnalyzer) {
        tmpFiles = ((SemanticAnalyzer) sem).getAllDBDataTmpFiles();
        this.hasWith = ((SemanticAnalyzer) sem).getHasWith();
      }

      if (sem instanceof SemanticAnalyzer) {
        tmpPgFiles = ((SemanticAnalyzer) sem).getAllPgDataTmpFiles();
      }

      sem.validate();

      if (SessionState.get() != null)
          SessionState.get().ssLog("Validate query plan Completed");
      plan = new QueryPlan(command, sem);

      if (sem instanceof ExplainSemanticAnalyzer) {
        BaseSemanticAnalyzer actualSem = ((ExplainSemanticAnalyzer) sem)
            .getSem();
        actualPlan = new QueryPlan(command, actualSem);
      }

      return (0);
    } catch (SemanticException e) {
      console.printError(
          "FAILED: Error in semantic analysis: " + e.getMessage(), "\n"
              + org.apache.hadoop.util.StringUtils.stringifyException(e));
      cleanTmpDBDataFiles();

      if (tmp_sem instanceof SemanticAnalyzer) {
        tmpPgFiles = ((SemanticAnalyzer) tmp_sem).getAllPgDataTmpFiles();
      }
      cleanTmpPgDataFiles();

      if (SessionState.get() != null) {
        SessionState.get().ssLog(
            "FAILED: Error in semantic analysis: " + e.getMessage());
        SessionState.get().ssLog(
            org.apache.hadoop.util.StringUtils.stringifyException(e));
      }
      
      String errStr = "FAILED: Error in semantic analysis: " + e.getMessage() + "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e);
      insertErrorInfo(command, errStr);

      return (10);
    } catch (ParseException e) {
      console.printError("FAILED: Parse Error: " + e.getMessage(), "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      cleanTmpDBDataFiles();

      if (tmp_sem instanceof SemanticAnalyzer) {
        tmpPgFiles = ((SemanticAnalyzer) tmp_sem).getAllPgDataTmpFiles();
      }
      cleanTmpPgDataFiles();

      if (SessionState.get() != null) {
        SessionState.get().ssLog("FAILED: Parse Error: " + e.getMessage());
        SessionState.get().ssLog(
            org.apache.hadoop.util.StringUtils.stringifyException(e));
      }
     
      String errStr = "FAILED: Parse Error: " + e.getMessage() + "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e);
      insertErrorInfo(command, errStr);
      
      return (11);
    } catch (Exception e) {
      e.printStackTrace();
      console.printError("FAILED: Unknown exception : " + e.getMessage(), "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      cleanTmpDBDataFiles();

      if (tmp_sem instanceof SemanticAnalyzer) {
        tmpPgFiles = ((SemanticAnalyzer) tmp_sem).getAllPgDataTmpFiles();
      }
      cleanTmpPgDataFiles();

      if (SessionState.get() != null) {
        SessionState.get().ssLog(
            "FAILED: Unknown exception : " + e.getMessage());
        SessionState.get().ssLog(
            org.apache.hadoop.util.StringUtils.stringifyException(e));
      }

      String errStr = "FAILED: Unknown exception : " + e.getMessage() + "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e);
      insertErrorInfo(command, errStr);

      return (12);
    }
  }

  public QueryPlan getPlan() {
    return plan;
  }

  public int run(String command) {
	LOG.info(conf.getVar(HiveConf.ConfVars.HIVEQUERYID) +  " begin compile...");
    int ret = compile(command);
    if (ret != 0)
      return (ret);
    if (SessionState.get() != null && SessionState.get().isCheck()) {
      LOG.info(conf.getVar(HiveConf.ConfVars.HIVEQUERYID) + " check SQL semantic OK!");
      if (SessionState.get() != null)
        SessionState.get().ssLog("check SQL semantic OK!");
      return 0;
    }
    LOG.info(conf.getVar(HiveConf.ConfVars.HIVEQUERYID) +  " begin execute...");
    return execute();
  }

  private List<PreExecute> getPreExecHooks() throws Exception {
    ArrayList<PreExecute> pehooks = new ArrayList<PreExecute>();
    String pestr = conf.getVar(HiveConf.ConfVars.PREEXECHOOKS);
    pestr = pestr.trim();
    if (pestr.equals(""))
      return pehooks;

    String[] peClasses = pestr.split(",");

    for (String peClass : peClasses) {
      try {
        pehooks.add((PreExecute) Class.forName(peClass.trim(), true,
            JavaUtils.getClassLoader()).newInstance());
      } catch (ClassNotFoundException e) {
        console.printError("Pre Exec Hook Class not found:" + e.getMessage());
        throw e;
      }
    }

    return pehooks;
  }

  public int execute() {
    boolean noName = StringUtils.isEmpty(conf
        .getVar(HiveConf.ConfVars.HADOOPJOBNAME));
    int maxlen = conf.getIntVar(HiveConf.ConfVars.HIVEJOBNAMELENGTH);
    maxthreads = conf.getIntVar(conf, HiveConf.ConfVars.EXECPARALLELNUM);

    parellMode = HiveConf.getBoolVar(conf, HiveConf.ConfVars.EXECPARALLEL);

    boolean withOptimization = HiveConf.getBoolVar(conf,
        HiveConf.ConfVars.OPENWITHOPTIMIZE);
    if (!parellMode && this.hasWith && withOptimization) {
      this.initialParallelMode = parellMode;
      conf.setBoolean("hive.exec.parallel", true);
      parellMode = true;
    }

    String queryId = plan.getQueryId();
    String queryStr = plan.getQueryStr();

    int curJobNo = 0;

    String ddlQueryStartTime;
    String ddlQueryEndTime;
    long ddlQueryStartTimeMillis = 0;
    long ddlQueryEndMilTimeMillis = 0;
    String ddlQueryId = "abcdefghijkl";
    boolean isCreateQuery = false;
    int timeout = SessionState.get().getConf().getInt("hive.pg.timeout", 10);
//    int timeout = 1;

    boolean testinsert = false;
    int jobnumber = 0;
    try {
      if (SessionState.get().getBIStore() == null) {
    	  SessionState.get().initBIStore();
      }
    } catch (Exception e) {
      LOG.error("getBIStore failed!");
      e.printStackTrace();
    }

    queryId = SessionState.get().getCurrnetQueryId();
    String sessionId = SessionState.get().getSessionName();
    conf.setVar(HiveConf.ConfVars.HIVEQUERYID, queryId);
    conf.setVar(HiveConf.ConfVars.HIVESESSIONID, sessionId);
    conf.setVar(HiveConf.ConfVars.HIVEQUERYSTRING, queryStr);
    //for CLI mode
    if (SessionState.get() != null && SessionState.get().getTdw_query_info() == null){
    	SessionState ss = SessionState.get();	
    	
    	SessionState.get().setTdw_query_info(
    			new tdw_query_info(
      				  ss.getQueryId() + "_" + Integer.toString(rand.nextInt()),
      				  ss.getUserName(),
      				  ss.getSessionId(),
      				  null,
      				  null,
      				  ss.getCmd(),
      				  0,
      				  null,
      				  null,
      				  null,//state
      				  null,
      				  null,
      				  null,
      				  ss.getDbName()
      				  )
    	);
    }
    try {
      LOG.info(conf.getVar(HiveConf.ConfVars.HIVEQUERYID) + " Starting command: " + queryStr);
      if (SessionState.get() != null)
        SessionState.get().ssLog("Starting command: " + queryStr);

      resStream = null;

      BaseSemanticAnalyzer sem = plan.getPlan();

      LOG.debug("plan.getPlan() OK");
      for (PreExecute peh : getPreExecHooks()) {
        peh.run(SessionState.get(), 
            sem.getInputs(), sem.getOutputs(),
            ShimLoader.getHadoopShims().getUGIForConf(conf));       
      }

      int jobs = countJobs(sem.getRootTasks());
      jobnumber = jobs;

      jobNumber = jobs;
      jobNumIsInit = true;
      curJobIndex = 0;

      if (jobs > 0) {
        console.printInfo("Total MapReduce jobs = " + jobs);
        if (SessionState.get() != null /*&&& (!(SessionState.get().getiscli()))*/) {
          SessionState ss = SessionState.get();
          SessionState.get().ssLog("Total MapReduce jobs = " + jobs);
          if (ss.updateTdw_query_info(jobs)) {	
            Connection cc = null;
            try {
              if (SessionState.get().getBIStore() != null) {
                cc = ss.getBIStore().openConnect();
                if (cc != null) {
                	ss.getTdw_query_info().setQueryId(ss.getQueryId() + "_" + Integer.toString(rand.nextInt()));
                	ss.getTdw_query_info().setQueryString(queryStr);
                	ss.getTdw_query_info().setDbName(SessionState.get().getDbName());
                	ss.getBIStore().insertInfo(cc, ss.getTdw_query_info());
                	ss.getBIStore().closeConnect(cc);
                } else {
                  LOG.error("add_tdw_query_info into BI failed: "
                      + ss.getTdw_query_info().getQueryId());
                }
              }
            } catch (Exception e) {
              ss.getBIStore().closeConnect(cc);
              LOG.error("add_tdw_query_info into BI failed: "
                  + ss.getCurrnetQueryId());
              e.printStackTrace();
            }
          }
        }
      }

      if (SessionState.get() != null) {
        isCreateQuery = SessionState.get().getCreateQueryFlag();
        LOG.info("isCreateQuery = " + isCreateQuery);
      }
      if (sem instanceof DDLSemanticAnalyzer
          || sem instanceof ACLSemanticAnalyzer || isCreateQuery) {
        if (SessionState.get() != null
            && SessionState.get().getBIStore() != null) {
          SessionState ss = SessionState.get();

          InetAddress inet = InetAddress.getLocalHost();
          String IP = inet.getHostAddress().toString();
          SessionState.get().getBIStore().setDDLQueryIP(IP);

          ddlQueryId = ss.getQueryIDForLog();

          try {
            testinsert = true;
            Connection cc = null;
            try {
              if (SessionState.get().getBIStore() != null) {
                cc = ss.getBIStore().openConnect(timeout);
                if (cc != null) {
                  ss.getBIStore().insertDDLInfo(cc, ddlQueryId, queryStr);
                  ss.getBIStore().closeConnect(cc);
                } else {
                  LOG.error("add_tdw_query_info into BI failed: " + ddlQueryId);
                }
              }
            } catch (Exception e) {
              ss.getBIStore().closeConnect(cc);
              LOG.error("add_tdw_query_info into BI failed: " + ddlQueryId);
              testinsert = false;
            }
          } catch (Exception e) {
            testinsert = false;
          }

          if (!testinsert) {
            LOG.info("add_tdw_query_info failed: " + ddlQueryId);
          }
        }
      }

      String jobname = Utilities.abbreviate(queryStr, maxlen - 6);

      Queue<Task<? extends Serializable>> runnable = new LinkedList<Task<? extends Serializable>>();
      Map<TaskResult, TaskRunner> running = new HashMap<TaskResult, TaskRunner>();
      DriverContext driverCxt = new DriverContext(runnable);

      for (Task<? extends Serializable> tsk : sem.getRootTasks()) {
        addToRunnable(runnable, tsk);
      }

      while (running.size() != 0 || runnable.peek() != null) {
        if (parellMode) {
          while (runnable.peek() != null && running.size() < maxthreads) {
        	  
            Task<? extends Serializable> tsk = runnable.remove();
            curJobNo = launchTask(tsk, queryId, noName, driverCxt, running,
                jobname, jobs, curJobNo);
            LOG.info(conf.getVar(HiveConf.ConfVars.HIVEQUERYID) + "launch Task in parell Mode : " + tsk.getName() + "OK");
            runningJobs = running.values();
          }
        } else {
          while (runnable.peek() != null && running.size() < maxthreads) {
            sequentialTaskResult = null;
            Task<? extends Serializable> tsk = runnable.remove();
            curJobNo = launchTask(tsk, queryId, noName, driverCxt, running,
                jobname, jobs, curJobNo);
            LOG.info(conf.getVar(HiveConf.ConfVars.HIVEQUERYID) + "launch Task : " + tsk.getName());
            if (sequentialTaskResult != null) {
              if (socket != null) {
                int readret = 4;
                byte[] buffertmp = new byte[4];
                socket.setSoTimeout(10);
                try {
                  while (readret == 4) {
                    readret = socket.getInputStream().read(buffertmp);
                    LOG.debug(socket.getInetAddress().getHostAddress() + ":"
                        + socket.getPort() + " Read ret1:" + readret);
                  }
                } catch (SocketException e) {
                  LOG.debug(e.getClass().getName() + ':' + e.getMessage());
                  readret = -1;
                  LOG.debug(socket.getInetAddress().getHostAddress() + ":"
                      + socket.getPort() + " Read ret2:" + readret);
                } catch (Exception e) {
                  LOG.info(e.getClass().getName());
                  LOG.info(e.getMessage());
                }
                socket.setSoTimeout(0);

                if (readret == -1) {
                  sequentialTaskResult.setExitVal(996);
                  LOG.debug("Read socket error");
                }
              }

              if (sequentialTaskResult.getExitVal() != 0) {
                int exitVal = sequentialTaskResult.getExitVal();
                this.close();
                console.printError("FAILED: Execution Error, return code "
                    + exitVal + " from " + tsk.getClass().getName());
                LOG.info("killed the tsk:" + tsk.getId());
                if (SessionState.get() != null)
                  SessionState.get().ssLog(
                      "FAILED: Execution Error, return code " + exitVal
                          + " from " + tsk.getClass().getName());

                String errStr = "FAILED: Execution Error, return code " + exitVal
                    + " from " + tsk.getClass().getName();
                insertErrorInfo(errStr);

                if (SessionState.get() != null
                    && (plan.getPlan() instanceof DDLSemanticAnalyzer
                        || plan.getPlan() instanceof ACLSemanticAnalyzer || isCreateQuery)) {
                  if (SessionState.get().getBIStore() != null) {
                    SessionState.get().getBIStore().setDDLQueryResult(exitVal);
                  }
                }
                return 9;
              }
            }
          }
        }

        TaskResult tskRes = pollTasks(running.keySet());
        TaskRunner tskRun = running.remove(tskRes);
        Task<? extends Serializable> tsk = tskRun.getTask();

        int exitVal = tskRes.getExitVal();

        if (SessionState.get() != null) {
          isCreateQuery = SessionState.get().getCreateQueryFlag();
        }
        if (SessionState.get() != null
            && (plan.getPlan() instanceof DDLSemanticAnalyzer
                || plan.getPlan() instanceof ACLSemanticAnalyzer || isCreateQuery)) {
          if (SessionState.get().getBIStore() != null) {
            SessionState.get().getBIStore().setDDLQueryResult(exitVal);
          }
        }

        if (socket != null) {
          int readret = 4;
          byte[] buffertmp = new byte[4];
          socket.setSoTimeout(10);
          try {
            while (readret == 4) {
              readret = socket.getInputStream().read(buffertmp);
              LOG.debug(socket.getInetAddress().getHostAddress() + ":"
                  + socket.getPort() + " Read ret1:" + readret);
            }
          } catch (SocketException e) {
            LOG.info(e.getClass().getName() + ':' + e.getMessage());
            readret = -1;
            LOG.info(socket.getInetAddress().getHostAddress() + ":"
                + socket.getPort() + " Read ret2:" + readret);
          } catch (Exception e) {
            LOG.info(e.getClass().getName());
            LOG.info(e.getMessage());
          }
          socket.setSoTimeout(0);

          if (readret == -1) {
            exitVal = 996;
            LOG.debug("Read socket error");
          }
        }

        if (exitVal != 0) {
          this.close();
          console.printError("FAILED: Execution Error, return code " + exitVal
              + " from " + tsk.getClass().getName());
          if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.EXECPARALLEL))
            LOG.info("killed the tsk:" + tsk.getId());
          if (running.size() != 0) {
            taskCleanup(running);
          }
          if (SessionState.get() != null)
            SessionState.get().ssLog(
                "FAILED: Execution Error, return code " + exitVal + " from "
                    + tsk.getClass().getName());
          
          String errStr = "FAILED: Execution Error, return code " + exitVal
              + " from " + tsk.getClass().getName();
          insertErrorInfo(errStr);

          return 9;
        }
        tsk.setDone();
        LOG.info(conf.getVar(HiveConf.ConfVars.HIVEQUERYID) + "Task Done: " + tsk.getName());
        if (tsk.isMapRedTask()) {
          jobHasDoneCount++;
        }

        if (SessionState.get() != null) {
          SessionState
              .get()
              .getHiveHistory()
              .setTaskProperty(queryId, tsk.getId(), Keys.TASK_RET_CODE,
                  String.valueOf(exitVal));
          SessionState.get().getHiveHistory().endTask(queryId, tsk);
        }

        if (tsk.getChildTasks() != null) {
          for (Task<? extends Serializable> child : tsk.getChildTasks()) {
            if (isLaunchable(child)) {
              addToRunnable(runnable, child);
            }
          }
        }
      }

      try {
        if (isAInsertQuery()) {
          computeExecResultCounterForInsert(sem.getRootTasks());
          SessionState ss = SessionState.get();

          adjustCounters();

          if (!insertInfoMap.isEmpty()) {
            boolean isMultiInsert = insertInfoMap.size() > 1;
            Connection cc = null;
            try {
            for (Entry<String, InsertExeInfo> entry : insertInfoMap.entrySet()) {
              entry.getValue().setQueryID(ss.getTdw_query_info().getQueryId());
              entry.getValue().setIsMultiInsert(isMultiInsert);
            }
                  
              if (ss.getBIStore() != null) {
                cc = ss.getBIStore().openConnect();
                if (cc != null) {
                  ss.getBIStore().insertInsertExeInfo(cc,
                      insertInfoMap.values());
                  ss.getBIStore().closeConnect(cc);
                } else {
                  LOG.error("add insert execute info to  BI failed: "
                      + ss.getCurrnetQueryId());
                }
              }
            } catch (Exception e) {
              ss.getBIStore().closeConnect(cc);
              LOG.error("add insert execute info to  BI failed: "
                  + ss.getCurrnetQueryId());
              e.printStackTrace();
            }

          }
        }
      } catch (Exception x) {

      }

    } catch (Exception e) {
      this.close();
      if (SessionState.get() != null)
        SessionState.get().getHiveHistory()
            .setQueryProperty(queryId, Keys.QUERY_RET_CODE, String.valueOf(12));
      console.printError("FAILED: Unknown exception : " + e.getMessage(), "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
      if (SessionState.get() != null) {
        SessionState.get().ssLog(
            "FAILED: Unknown exception : " + e.getMessage());
        SessionState.get().ssLog(
            org.apache.hadoop.util.StringUtils.stringifyException(e));
      }

      String errStr = "FAILED: Unknown exception : " + e.getMessage() + "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e);
      insertErrorInfo(errStr);

      cleanTmpPgDataFiles();

      return (12);
    } finally {
      if (!initialParallelMode && this.hasWith && withOptimization) {
        conf.setBoolean("hive.exec.parallel", initialParallelMode);
        parellMode = initialParallelMode;
        this.hasWith = false;
      }

      conf.setVar(HiveConf.ConfVars.HIVEQUERYSTRING, "");

      cleanTmpDBDataFiles();
      if (SessionState.get() != null && jobnumber > 0
          /*&& (!(SessionState.get().getiscli()))*/) {
        GregorianCalendar gc = new GregorianCalendar();
        String endtime = String.format("%1$4d%2$02d%3$02d%4$02d%5$02d%6$02d",
            gc.get(Calendar.YEAR), gc.get(Calendar.MONTH) + 1,
            gc.get(Calendar.DAY_OF_MONTH), gc.get(Calendar.HOUR_OF_DAY),
            gc.get(Calendar.MINUTE), gc.get(Calendar.SECOND));

        SessionState ss = SessionState.get();
        Hive db;
        try {
          db = Hive.get(conf);
          boolean re = false;
          //in case cli
          if(ss.getTdw_query_info() != null){
          Connection cc = null;
          try {
            if (SessionState.get().getBIStore() != null) {
              cc = ss.getBIStore().openConnect();
              if (cc != null) {
                ss.getBIStore().updateInfo(cc,
                    ss.getTdw_query_info().getQueryId(), endtime);
                ss.getBIStore().closeConnect(cc);
              } else {
                LOG.error("update_tdw_query_info into BI failed: "
                    + ss.getTdw_query_info().getQueryId());
              }
            } else {
              LOG.info("SessionState.get().getBIStore() is null");
            }
          } catch (Exception e) {
            ss.getBIStore().closeConnect(cc);
            LOG.error("update_tdw_query_info into BI failed: "
                + ss.getTdw_query_info().getQueryId());
            e.printStackTrace();
          }

          LOG.debug("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
          LOG.debug("the finish time is: " + endtime);
          LOG.debug("the getQueryId is: " + ss.getTdw_query_info().getQueryId());
          LOG.debug("the result is: " + re);
          LOG.debug("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
          }
       } catch (HiveException e) {
          e.printStackTrace();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

      if (SessionState.get() != null) {
        isCreateQuery = SessionState.get().getCreateQueryFlag();
      }
      if (SessionState.get() != null
          && SessionState.get().getBIStore() != null
          && (plan.getPlan() instanceof DDLSemanticAnalyzer
              || plan.getPlan() instanceof ACLSemanticAnalyzer || isCreateQuery)) {
        GregorianCalendar gc = new GregorianCalendar();
        ddlQueryEndTime = String.format(
            "%1$4d%2$02d%3$02d%4$02d%5$02d%6$02d%7$04d", gc.get(Calendar.YEAR),
            gc.get(Calendar.MONTH) + 1, gc.get(Calendar.DAY_OF_MONTH),
            gc.get(Calendar.HOUR_OF_DAY), gc.get(Calendar.MINUTE),
            gc.get(Calendar.SECOND), gc.get(Calendar.MILLISECOND));

        ddlQueryEndMilTimeMillis = System.currentTimeMillis();

        boolean ddlQueryResult = SessionState.get().getBIStore()
            .getDDLQueryResult() == 0;

        SessionState ss = SessionState.get();
        try {
          try {
            Connection cc = null;
            try {
              if (SessionState.get().getBIStore() != null) {
                cc = ss.getBIStore().openConnect(timeout);
                if (cc != null) {
                  ss.getBIStore().updateDDLQueryInfo(cc, ddlQueryId,
                      ddlQueryResult, ss.getConf().get("usp.param"),
                      ss.getUserName());
                  ss.getBIStore().closeConnect(cc);
                } else {
                  LOG.error("update_tdw_query_info into BI failed: "
                      + ddlQueryId);
                }
              }
            } catch (Exception e) {
              ss.getBIStore().closeConnect(cc);
              LOG.error("update_tdw_query_info into BI failed: " + ddlQueryId);
            }
          } catch (Exception e) {
          }
        } catch (Exception e) {
        }
      }

      if (SessionState.get() != null) {
        SessionState.get().setCreateQueryFlag(false);
      }

      if (noName) {
        conf.setVar(HiveConf.ConfVars.HADOOPJOBNAME, "");
      }
    }

    console.printInfo("OK");
    if (SessionState.get() != null) {
      SessionState.get().ssLog("OK");
    }
    return (0);
  }

  public int launchTask(Task<? extends Serializable> tsk, String queryId,
      boolean noName, DriverContext driverCxt,
      Map<TaskResult, TaskRunner> running, String jobname, int jobs,
      int curJobNo) {

    if (SessionState.get() != null) {
      SessionState.get().getHiveHistory()
          .startTask(queryId, tsk, tsk.getClass().getName());
    }
    if (tsk.isMapRedTask() && !(tsk instanceof ConditionalTask)) {

      curJobNo++;
      if (SessionState.get() != null)
        SessionState.get().setCurrMRIndex(curJobNo);

      if (noName) {
        conf.setVar(HiveConf.ConfVars.HADOOPJOBNAME, jobname + "(" + curJobNo
            + "/" + jobs + ")");
      }

      console.printInfo("Launching Job " + curJobNo + " out of " + jobs);
      LOG.info(conf.getVar(HiveConf.ConfVars.HIVEQUERYID) + " Launching Job " + curJobNo + " out of " + jobs);
    }
    tsk.initialize(conf, driverCxt);
    TaskResult tskRes = new TaskResult();
    TaskRunner tskRun = new TaskRunner(tsk, tskRes, curJobNo, this.execinfos);

    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.EXECPARALLEL)
        && tsk.isMapRedTask()) {
      tskRun.start();
    } else {
      currentTask = tskRun.getTask();
      sequentialTaskResult = tskRes;
      tskRun.runSequential();
      curJobIndex++;
    }
    if (tsk.taskexecinfo != null && tsk.taskexecinfo.taskid != null)
      this.execinfos.add(tsk.taskexecinfo);
    running.put(tskRes, tskRun);
    return curJobNo;
  }

  public void taskCleanup() {
    System.exit(9);
  }

  private synchronized void taskCleanup(Map<TaskResult, TaskRunner> running) {
    LOG.info("need to kill another " + running.size() + " tasks automatically!");

    for (Map.Entry<TaskResult, TaskRunner> en : running.entrySet()) {
      Task<? extends Serializable> tsk = en.getValue().getTask();
      TaskResult tr = en.getKey();
      LOG.info("killing the tsk:" + tsk.getId());
      if (tsk.isMapRedTask()) {
        ExecDriver mrtsk = (ExecDriver) tsk;

        try {
          mrtsk.killJob();
        } catch (IOException e) {
          e.printStackTrace();
        }
      } else {
        LOG.info("A task can not be killed: " + tsk.getId());
      }
    }

    return;
  }

  public TaskResult pollTasks(Set<TaskResult> results) {
    Iterator<TaskResult> resultIterator = results.iterator();
    while (true) {
      while (resultIterator.hasNext()) {
        TaskResult tskRes = resultIterator.next();
        if (tskRes.isRunning() == false) {
          return tskRes;
        }
      }

      try {
        Thread.sleep(sleeptime);
      } catch (InterruptedException ie) {
        ;
      }
      resultIterator = results.iterator();
    }
  }

  public boolean isLaunchable(Task<? extends Serializable> tsk) {
    return !tsk.getQueued() && !tsk.getInitialized() && tsk.isRunnable();
  }

  public void addToRunnable(Queue<Task<? extends Serializable>> runnable,
      Task<? extends Serializable> tsk) {
    runnable.add(tsk);
    tsk.setQueued();
  }

  private void getWithTabs(ASTNode tree) {
    LOG.info(tree.dump());
    ASTNode child0 = (ASTNode) tree.getChild(0);
    if (tree.getType() == HiveParser.TOK_EXPLAIN)
      child0 = (ASTNode) child0.getChild(0);
    LOG.info("child0: " + child0.dump());

    if (child0.getType() == HiveParser.TOK_WITH) {
      if (withTmpTabs == null)
        withTmpTabs = new ArrayList<String>();
      for (int i = 0; i < child0.getChildCount() - 1; i++) {
        ASTNode child0i1 = (ASTNode) child0.getChild(i).getChild(1);
        withTmpTabs.add(child0i1.getText());
      }
    }
  }

  private void rewriteTreeForWith(ASTNode tree) {
    ASTNode withNode = (ASTNode) tree.getChild(0);
    ASTNode queryNode = tree;
    if (tree.getType() == HiveParser.TOK_EXPLAIN) {
      queryNode = withNode;
      withNode = (ASTNode) withNode.getChild(0);
    }

    int count = queryNode.getChildCount();
    LOG.info("count: " + count);
    for (int i = count - 1; i >= 0; i--)
      queryNode.deleteChild(i);

    queryNode.addChild(new ASTNode(new CommonToken(HiveParser.TOK_WITH,
        "TOK_WITH")));
    ASTNode oldQueryTree = (ASTNode) withNode
        .getChild(withNode.getChildCount() - 1);

    for (int i = 0; i < oldQueryTree.getChildCount(); i++) {
      queryNode.addChild(oldQueryTree.getChild(i));
    }
    ASTNode newWithNode = (ASTNode) queryNode.getChild(0);
    LOG.info("newWithNode: " + newWithNode.dump());

    for (int i = 0; i < withNode.getChildCount() - 1; i++) {
      newWithNode.addChild(new ASTNode(new CommonToken(HiveParser.TOK_SUBQUERY,
          "TOK_SUBQUERY")));
      ASTNode subQ = (ASTNode) newWithNode.getChild(i);
      subQ.addChild(new ASTNode(new CommonToken(HiveParser.TOK_QUERY,
          "TOK_QUERY")));
      ASTNode q = (ASTNode) subQ.getChild(0);
      q.addChild(new ASTNode(new CommonToken(HiveParser.TOK_CREATETABLE,
          "TOK_CREATETABLE")));

      ASTNode ctas = (ASTNode) q.getChild(0);
      ctas.addChild(new ASTNode(new CommonToken(HiveParser.Identifier,
          withTmpTabs.get(i))));
      ctas.addChild(new ASTNode(new CommonToken(HiveParser.TOK_LIKETABLE,
          "TOK_LIKETABLE")));
      ASTNode sSubQ = (ASTNode) withNode.getChild(i);
      ctas.addChild((ASTNode) sSubQ.getChild(0));
    }
    LOG.info("NEW TREE: " + tree.dump());

  }

  public boolean getResults(Vector<String> res) throws IOException {
    LOG.debug("will get result");

    if (plan != null && plan.getPlan().getFetchTask() != null) {
      BaseSemanticAnalyzer sem = plan.getPlan();
      if (!sem.getFetchTaskInit()) {
        LOG.debug("init fetchtask");
        sem.setFetchTaskInit(true);
        sem.getFetchTask().initialize(conf, null);
      }
      FetchTask ft = (FetchTask) sem.getFetchTask();

      ft.setMaxRows(maxRows);
      boolean flag = ft.fetch(res);
      if (!flag) {
        cleanTmpPgDataFiles();
      }
      return flag;
    }

    if (resStream == null)
      resStream = ctx.getStream();
    if (resStream == null)
      return false;

    int numRows = 0;
    String row = null;

    while (numRows < maxRows) {
      if (resStream == null) {
        if (numRows > 0)
          return true;
        else
          return false;
      }

      bos.reset();
      Utilities.streamStatus ss;
      try {
        ss = Utilities.readColumn(resStream, bos);
        if (bos.getCount() > 0)
          row = new String(bos.getData(), 0, bos.getCount(), "UTF-8");
        else if (ss == Utilities.streamStatus.TERMINATED)
          row = new String();

        if (row != null) {
          numRows++;
          LOG.debug("add row : " + row);
          res.add(row);
        }
      } catch (IOException e) {
        console.printError("FAILED: Unexpected IO exception : "
            + e.getMessage());

        insertErrorInfo("FAILED: Unexpected IO exception : "
            + e.getMessage());

        LOG.info("clean tmp pg data file");
        cleanTmpPgDataFiles();

        res = null;
        return false;
      }

      if (ss == Utilities.streamStatus.EOF)
        resStream = ctx.getStream();
    }

    LOG.info("clean tmp pg data file");
    cleanTmpPgDataFiles();

    return true;
  }

  public int close() {
    try {
      ctx.clear();
    } catch (Exception e) {
      console.printError("FAILED: Unknown exception : " + e.getMessage(), "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));

      insertErrorInfo("FAILED: Unknown exception : " + e.getMessage(), "\n"
          + org.apache.hadoop.util.StringUtils.stringifyException(e));

      return (13);
    }

    return (0);
  }

  private void cleanTmpDBDataFiles() {

    if (tmpFiles != null && tmpFiles.size() > 0) {
      try {
        boolean multihdfsenable = HiveConf.getBoolVar(conf,
            HiveConf.ConfVars.MULTIHDFSENABLE);
        FileSystem fs = null;
        if (!multihdfsenable) {
          fs = FileSystem.get(conf);
          for (String filePath : tmpFiles.values()) {
            fs.delete(new Path(filePath));
          }
          tmpFiles.clear();
        } else {
          for (String filePath : tmpFiles.values()) {
            fs = new Path(filePath).getFileSystem(conf);
            fs.delete(new Path(filePath));
          }
          tmpFiles.clear();
        }
      } catch (IOException e) {
        LOG.error("tmp data clean package!");
      }
    }

  }

  private void cleanTmpPgDataFiles() {

    if (tmpPgFiles != null && tmpPgFiles.size() > 0) {
      try {
        FileSystem fs = FileSystem.get(conf);
        for (String filePath : tmpPgFiles.values()) {
          fs.delete(new Path(filePath));
        }
        tmpPgFiles.clear();
      } catch (IOException e) {
        LOG.error("tmp data clean package!");
      }
    }

  }
  
  private void insertErrorInfo(String command, String error) {
    if (SessionState.get() != null) {
      Connection cc = null;
      SessionState ss = SessionState.get();
      try {
        if (ss.getBIStore() != null) {
          cc = ss.getBIStore().openConnect();
          if (cc != null) {
        	  
        	  //for CLI
        	  if(ss.getTdw_query_info() == null){
        		  ss.setTdw_query_info(new tdw_query_info(
        				  ss.getQueryId()+ "_" + Integer.toString(rand.nextInt()),
        				  ss.getUserName(),
        				  ss.getSessionId(),
        				  null,
        				  null,
        				  command,
        				  0,
        				  null,
        				  null,
        				  "error",
        				  null,
        				  null,
        				  null,
        				  ss.getDbName())
        				  );
        	  }
            ss.getTdw_query_info().setDbName(ss.get().getDbName());
            ss.getBIStore().insertInfo(cc, ss.getTdw_query_info());
            
            //for CLI mode
            if(ss.getTdw_query_error_info() == null){
            	ss.setTdw_query_error_info(new tdw_query_error_info(
            			ss.getTdw_query_info().getQueryId(),
            			ss.getTdw_query_info().getTaskid(),
            			ss.getTdw_query_info().getStartTime(),
            			ss.getTdw_query_info().getIp(),
            			ss.getTdw_query_info().getPort(),
            			ss.getTdw_query_info().getClientIp(),
            			null,
            			null
            	));
            }
            
            ss.getTdw_query_error_info().setErrorString(error);
            int ret = ss.getBIStore().insertErrorInfo(cc, ss.getTdw_query_error_info());
            ss.getBIStore().closeConnect(cc);
          } else {
            LOG.error("add_tdw_query_error_info into BI failed: "
                + ss.getQueryId());
          }
        }
      } catch (Exception e) {
    	  e.printStackTrace();
    	  ss.getBIStore().closeConnect(cc);
    	  LOG.error("add_tdw_query_error_info into BI failed: "
    			  + ss.getQueryId());
      }
    }
  }
  
  private void insertErrorInfo(String error) {
    if (SessionState.get() != null) {
      Connection cc = null;
      SessionState ss = SessionState.get();
      try {
        if (ss.getBIStore() != null) {
          cc = ss.getBIStore().openConnect();
          if (cc != null) { 
        	  
        	  //for CLI
        	  if(ss.getTdw_query_info() == null){
        		  ss.setTdw_query_info(new tdw_query_info(
        				  ss.getQueryId() + "_" + Integer.toString(rand.nextInt()),
        				  ss.getUserName(),
        				  ss.getSessionId(),
        				  null,
        				  null,
        				  ss.getCmd(),
        				  0,
        				  null,
        				  null,
        				  "error",
        				  null,
        				  null,
        				  null,
        				  ss.getDbName())
        				  );
        	  }
        	  
              //for CLI mode
              if(ss.getTdw_query_error_info() == null){
              	ss.setTdw_query_error_info(new tdw_query_error_info(
              			ss.getTdw_query_info().getQueryId(),
              			ss.getTdw_query_info().getTaskid(),
              			ss.getTdw_query_info().getStartTime(),
              			ss.getTdw_query_info().getIp(),
              			ss.getTdw_query_info().getPort(),
              			ss.getTdw_query_info().getClientIp(),
              			null,
              			null
              	));
              }             
            ss.getTdw_query_error_info().setErrorString(error);
            int ret = ss.getBIStore().insertErrorInfo(cc, ss.getTdw_query_error_info());
            ss.getBIStore().closeConnect(cc);
          } else {
            LOG.error("add_tdw_query_error_info into BI failed: "
                + ss.getQueryId());
          }
        }
      } catch (Exception e) {
          e.printStackTrace();
    	  ss.getBIStore().closeConnect(cc);
    	  LOG.error("add_tdw_query_error_info into BI failed: "
    			  + ss.getQueryId());

      }
    }
  }
}
