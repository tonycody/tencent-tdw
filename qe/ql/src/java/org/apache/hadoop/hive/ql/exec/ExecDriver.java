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

package org.apache.hadoop.hive.ql.exec;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.net.URI;
import java.net.URLEncoder;
import java.net.URLDecoder;
import java.net.URL;
import java.net.URLConnection;
import java.net.UnknownHostException;
import java.net.URLClassLoader;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.tdw_query_stat;
import org.apache.hadoop.hive.metastore.api.tdw_sys_fields_statistics;
import org.apache.hadoop.hive.ql.DriverContext;
//import org.apache.hadoop.hive.ql.plan.Operator;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.histogramDesc;
import org.apache.hadoop.hive.ql.plan.mapredLocalWork;
import org.apache.hadoop.hive.ql.plan.mapredWork;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.partitionDesc;
import org.apache.hadoop.hive.ql.plan.sampleDesc;
import org.apache.hadoop.hive.ql.plan.staticsInfoCollectWork;
import org.apache.hadoop.hive.ql.plan.statsDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.FileSinkCount;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.exec.MapOperator.Counter;
import org.apache.hadoop.hive.ql.exec.PartitionerOperator;
import org.apache.hadoop.hive.ql.exec.PartitionerOperator.PartitionFileSinkCount;
import org.apache.hadoop.hive.ql.history.HiveHistory.Keys;
import org.apache.hadoop.hive.ql.io.*;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.varia.NullAppender;
import org.apache.thrift.TException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import org.apache.hadoop.hive.ql.exec.errors.ErrorAndSolution;
import org.apache.hadoop.hive.ql.exec.errors.TaskLogProcessor;

public class ExecDriver extends Task<mapredWork> implements Serializable {

  private static final long serialVersionUID = 1L;

  transient protected JobConf job;

  public static Random randGen = new Random();

  private int mapProcess;
  private int reduceProcess;
  private RunningJob runJob;

  private Map<String, String> confMap = new HashMap<String, String>();
  private Map<String, String> runningMap = new HashMap<String, String>();

  public ExecDriver() {
    super();
    mapProcess = 0;
    reduceProcess = 0;
    runJob = null;
  }

  public int queryProcessComputer() {
    return (int) (((float) mapProcess + reduceProcess) / 2);
  }

  public int killJob() throws IOException {
    if (runJob == null)
      throw new IOException("current job is null");
    runJob.killJob();
    return 1;
  }

  public String getJobID() throws IOException {
    if (runJob == null)
      throw new IOException("current job is null");
    JobID jobID = runJob.getID();
    if (jobID != null)
      return jobID.getJtIdentifier();
    else
      return null;
  }

  public String getJobURL() throws IOException {
    if (runJob == null)
      throw new IOException("current job is null");

    return runJob.getTrackingURL();
  }

  public String getJobName() throws IOException {
    if (runJob == null)
      throw new IOException("current job is null");

    return runJob.getJobName();
  }

  public Map<String, String> getJobCounts() {
    Map<String, String> ret = new HashMap<String, String>();
    Set<String> keySet = confMap.keySet();
    for (String key : keySet) {
      ret.put(key, confMap.get(key));
    }

    keySet = runningMap.keySet();
    for (String key : keySet) {
      ret.put(key, runningMap.get(key));
    }

    return ret;
  }

  public static String getResourceFiles(Configuration conf,
      SessionState.ResourceType t) {
    SessionState ss = SessionState.get();
    Set<String> files = (ss == null) ? null : ss.list_resource(t, null);
    if (files != null) {
      ArrayList<String> realFiles = new ArrayList<String>(files.size());
      for (String one : files) {
        try {
          realFiles.add(Utilities.realFile(one, conf));
        } catch (IOException e) {
          throw new RuntimeException("Cannot validate file " + one
              + "due to exception: " + e.getMessage(), e);
        }
      }
      return StringUtils.join(realFiles, ",");
    } else {
      return "";
    }
  }

  private void initializeFiles(String prop, String files) {
    if (files != null && files.length() > 0) {
      job.set(prop, files);
      ShimLoader.getHadoopShims().setTmpFiles(prop, files);
    }
  }

  Hive _hive;

  public void initialize(HiveConf conf, DriverContext driverContext) {
    super.initialize(conf, driverContext);
    try {
      _hive = Hive.get(conf);
    } catch (HiveException e) {
      e.printStackTrace();
    }

    job = new JobConf(conf, ExecDriver.class);
    String addedFiles = getResourceFiles(job, SessionState.ResourceType.FILE);
    if (StringUtils.isNotBlank(addedFiles)) {
      HiveConf.setVar(job, ConfVars.HIVEADDEDFILES, addedFiles);
    }
    String addedJars = getResourceFiles(job, SessionState.ResourceType.JAR);
    if (StringUtils.isNotBlank(addedJars)) {
      HiveConf.setVar(job, ConfVars.HIVEADDEDJARS, addedJars);
    }
  }

  public ExecDriver(mapredWork plan, JobConf job, boolean isSilent)
      throws HiveException {
    setWork(plan);
    this.job = job;
    LOG = LogFactory.getLog(this.getClass().getName());
    console = new LogHelper(LOG, isSilent);
  }

  public static Map<String, String> runningJobKillURIs = Collections
      .synchronizedMap(new HashMap<String, String>());

  static {
    if (new org.apache.hadoop.conf.Configuration().getBoolean(
        "webinterface.private.actions", false)) {
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          synchronized (runningJobKillURIs) {
            for (Iterator<String> elems = runningJobKillURIs.values()
                .iterator(); elems.hasNext();) {
              String uri = elems.next();
              try {
                System.err.println("killing job with: " + uri);
                java.net.HttpURLConnection conn = (java.net.HttpURLConnection) new java.net.URL(
                    uri).openConnection();
                conn.setRequestMethod("POST");
                int retCode = conn.getResponseCode();
                if (retCode != 200) {
                  System.err
                      .println("Got an error trying to kill job with URI: "
                          + uri + " = " + retCode);
                }
              } catch (Exception e) {
                System.err.println("trying to kill job, caught: " + e);
              }
            }
          }
        }
      });
    }
  }

  public void jobInfo(RunningJob rj) {
    if (ShimLoader.getHadoopShims().isLocalMode(job))  {
      console.printInfo("Job running in-process (local Hadoop)");
      if (SessionState.get() != null)
        SessionState.get().ssLog("Job running in-process (local Hadoop)");
    } else {
      String hp = ShimLoader.getHadoopShims().getJobTrackerConf(job);
      if (SessionState.get() != null) {
        SessionState
            .get()
            .getHiveHistory()
            .setTaskProperty(SessionState.get().getQueryId(), getId(),
                Keys.TASK_HADOOP_ID, rj.getJobID());
      }
      console.printInfo(ExecDriver.getJobStartMsg(rj.getJobID())
          + ", Tracking URL = " + rj.getTrackingURL());
      if (SessionState.get() != null) {
        SessionState.get().ssLog(
            "Starting Job = " + rj.getJobID() + ", Tracking URL = "
                + rj.getTrackingURL());
        SessionState.get().ssLog(
            "Kill Command = "
                + HiveConf.getVar(job, HiveConf.ConfVars.HADOOPBIN)
                + " job  -Dmapred.job.tracker=" + hp + " -kill "
                + rj.getJobID());
      }
      console.printInfo("Kill Command = "
          + HiveConf.getVar(job, HiveConf.ConfVars.HADOOPBIN)
          + " job  -Dmapred.job.tracker=" + hp + " -kill " + rj.getJobID());
    }
  }

  public RunningJob jobProgress(JobClient jc, RunningJob rj) throws IOException {
    String lastReport = "";
    SimpleDateFormat dateFormat = new SimpleDateFormat(
        "yyyy-MM-dd hh:mm:ss,SSS");
    long reportTime = System.currentTimeMillis();
    long maxReportInterval = 60 * 1000;
    while (!rj.isComplete()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
      rj = jc.getJob(rj.getJobID());

      String report = " " + getId() + " map = "
          + Math.round(rj.mapProgress() * 100) + "%,  reduce ="
          + Math.round(rj.reduceProgress() * 100) + "%";

      mapProcess = Math.round(rj.mapProgress() * 100);
      reduceProcess = Math.round(rj.reduceProgress() * 100);

      try {
        runningMap.clear();
        runningMap.put("MapProgress", String.valueOf(mapProcess));
        runningMap.put("ReduceProgress", String.valueOf(reduceProcess));
      } catch (Exception x) {

      }

      if (!report.equals(lastReport)
          || System.currentTimeMillis() >= reportTime + maxReportInterval) {

        String output = dateFormat.format(Calendar.getInstance().getTime())
            + report;
        SessionState ss = SessionState.get();
        if (ss != null) {
          ss.getHiveHistory().setTaskCounters(SessionState.get().getQueryId(),
              getId(), rj);
          ss.getHiveHistory().setTaskProperty(SessionState.get().getQueryId(),
              getId(), Keys.TASK_HADOOP_PROGRESS, output);
          ss.getHiveHistory().progressTask(SessionState.get().getQueryId(),
              this);
        }
        if (!HiveConf.getBoolVar(job, HiveConf.ConfVars.EXECPARALLEL))

          if (SessionState.get() != null
              && SessionState.get().getConf().get("usp.param") == null) {
            console.printInfo(output);
          }

        if (SessionState.get() != null)
          SessionState.get().ssLog(output);
        lastReport = report;
        reportTime = System.currentTimeMillis();
      }
    }
    if (SessionState.get() != null
        && SessionState.get().getConf().get("usp.param") != null) {
      console.printInfo(lastReport);
    }
    
    execResultCount.readSuccessNum = rj.getCounters().getCounter(
        Counter.READ_SUCCESS_COUNT);
    execResultCount.readErrorNum = rj.getCounters().getCounter(
        Counter.READ_ERROR_COUNT);

    taskexecinfo.taskid = this.id;
    taskexecinfo.in_success = "READ_SUCCESS_COUNT:"
        + execResultCount.readSuccessNum;
    taskexecinfo.in_error = "READ_ERROR_COUNT:" + execResultCount.readErrorNum;

    long sinkSuccess = rj.getCounters().getCounter(
        FileSinkCount.FILESINK_SUCCESS_COUNT);
    if (sinkSuccess == 0) {
      sinkSuccess = rj.getCounters().getCounter(
          PartitionFileSinkCount.PARTITIONER_SUCCESS_COUNT);
    }

    execResultCount.fileSinkSuccessNum = sinkSuccess;

    taskexecinfo.out_success = "FILESINK_SUCCESS_COUNT:" + sinkSuccess;
    taskexecinfo.out_error = "FILESINK_ERROR_COUNT:"
        + rj.getCounters().getCounter(Counter.SELECT_ERROR_COUNT);

    if(conf == null)
    	conf = new HiveConf();
    boolean skipbad = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVEPBBADFILESKIP);
    if(SessionState.get() != null){
    	SessionState.get().ssLog("set skip badfile to " + skipbad);
    }
    if(skipbad){
    	int skipbadlimit = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVEPBBADFILELIMIT);
    	org.apache.hadoop.mapred.Counters.Counter badfileCounter = 
    		rj.getCounters().findCounter("newprotobuf.mapred.ProtobufRecordReader$Counter", "BADFORMAT_FILE_COUNT");
    	
    	long badfilecount = 0;
    	if(badfileCounter != null)
    	    badfilecount = badfileCounter.getCounter();
        if(SessionState.get() != null){
        	SessionState.get().ssLog("skip badfile limit is " + skipbadlimit);
        	SessionState.get().ssLog("bad file count is " + badfilecount);
        }
        
        if(badfilecount > 0){
        	if (SessionState.get() != null /*&& (!(SessionState.get().getiscli()))*/) {
        	    SessionState ss = SessionState.get();
        	    if (this.getIndex() >= 0 && ss.getTdw_query_info() != null) {
    	            Connection cc = null;
    	            try {
    	                if (SessionState.get().getBIStore() != null) {
    	                    cc = ss.getBIStore().openConnect();
    	                    if (cc != null) {
    	                        ss.getBIStore().insertBadPbFormatLog(cc,
    	                        ss.getTdw_query_info().getQueryId(), rj.getJobID(), badfilecount);
    	                        ss.getBIStore().closeConnect(cc);
    	                    } else {
    	                        LOG.error("insert bad pb format log into BI failed: "
    	                            + ss.getTdw_query_info().getQueryId());
    	                    }
    	                }
    	            } 
    	            catch (Exception e) {
    	                ss.getBIStore().closeConnect(cc);
    	                LOG.error("insert bad pb format log into BI failed: "
    	                    + ss.getTdw_query_info().getQueryId());
    	                LOG.error(e.getMessage());
    	                e.printStackTrace();
    	            }
        	    }
        	}
        }
        
    	if(badfilecount > skipbadlimit){
    		throw new IOException("Too many bad pb file in task " + rj.getJobID());
    	}
    }
    
    return rj;
  }

  public int estimateNumberOfReducers(HiveConf hive, JobConf job,
      mapredWork work) throws IOException {
    if (hive == null) {
      hive = new HiveConf();
    }
    long bytesPerReducer = hive.getLongVar(HiveConf.ConfVars.BYTESPERREDUCER);
    int maxReducers = hive.getIntVar(HiveConf.ConfVars.MAXREDUCERS);
    long totalInputFileSize = getTotalInputFileSize(job, work);

    LOG.info("BytesPerReducer=" + bytesPerReducer + " maxReducers="
        + maxReducers + " totalInputFileSize=" + totalInputFileSize);
    if (SessionState.get() != null)
      SessionState.get().ssLog(
          "BytesPerReducer=" + bytesPerReducer + " maxReducers=" + maxReducers
              + " totalInputFileSize=" + totalInputFileSize);
    totalInputFileSize = totalInputFileSize * work.reduceFactor;

    int reducers = (int) ((totalInputFileSize + bytesPerReducer - 1) / bytesPerReducer);
    reducers = Math.max(1, reducers);
    reducers = Math.min(maxReducers, reducers);
    return reducers;
  }

  protected void setNumberOfReducers() throws IOException {
    Integer numReducersFromWork = work.getNumReduceTasks();

    if (work.getReducer() == null) {
      console
          .printInfo("Number of reduce tasks is set to 0 since there's no reduce operator");
      if (SessionState.get() != null)
        SessionState
            .get()
            .ssLog(
                "Number of reduce tasks is set to 0 since there's no reduce operator");
      work.setNumReduceTasks(Integer.valueOf(0));
    } else {
      if (numReducersFromWork >= 0) {
        console.printInfo("Number of reduce tasks determined at compile time: "
            + work.getNumReduceTasks());
        if (SessionState.get() != null)
          SessionState.get().ssLog(
              "Number of reduce tasks determined at compile time: "
                  + work.getNumReduceTasks());
      } else if (job.getNumReduceTasks() > 0) {
        int reducers = job.getNumReduceTasks();
        work.setNumReduceTasks(reducers);
        console
            .printInfo("Number of reduce tasks not specified. Defaulting to jobconf value of: "
                + reducers);
        if (SessionState.get() != null)
          SessionState.get().ssLog(
              "Number of reduce tasks not specified. Defaulting to jobconf value of: "
                  + reducers);
      } else {
        int reducers = estimateNumberOfReducers(conf, job, work);
        work.setNumReduceTasks(reducers);
        console
            .printInfo("Number of reduce tasks not specified. Estimated from input data size: "
                + reducers);
        if (SessionState.get() != null)
          SessionState.get().ssLog(
              "Number of reduce tasks not specified. Estimated from input data size: "
                  + reducers);
      }
      console
          .printInfo("In order to change the average load for a reducer (in bytes):");
      console.printInfo("  set " + HiveConf.ConfVars.BYTESPERREDUCER.varname
          + "=<number>");
      console.printInfo("In order to limit the maximum number of reducers:");
      console.printInfo("  set " + HiveConf.ConfVars.MAXREDUCERS.varname
          + "=<number>");
      console.printInfo("In order to set a constant number of reducers:");
      console.printInfo("  set " + HiveConf.ConfVars.HADOOPNUMREDUCERS
          + "=<number>");

      if (SessionState.get() != null) {
        SessionState.get().ssLog(
            "In order to change the average load for a reducer (in bytes):");
        SessionState.get().ssLog(
            "  set " + HiveConf.ConfVars.BYTESPERREDUCER.varname + "=<number>");
        SessionState.get().ssLog(
            "In order to limit the maximum number of reducers:");
        SessionState.get().ssLog(
            "  set " + HiveConf.ConfVars.MAXREDUCERS.varname + "=<number>");
        SessionState.get().ssLog(
            "In order to set a constant number of reducers:");
        SessionState.get().ssLog(
            "  set " + HiveConf.ConfVars.HADOOPNUMREDUCERS + "=<number>");
      }
    }
  }

  public long getTotalInputFileSize(JobConf job, mapredWork work)
      throws IOException {
    long r = 0;
    for (String path : work.getPathToAliases().keySet()) {
      try {
        Path p = new Path(path);
        FileSystem fs = p.getFileSystem(job);
        ContentSummary cs = fs.getContentSummary(p);
        r += cs.getLength();
      } catch (IOException e) {
        LOG.info("Cannot get size of " + path + ". Safely ignored.");
        if (SessionState.get() != null)
          SessionState.get().ssLog(
              "Cannot get size of " + path + ". Safely ignored.");
      }
    }
    return r;
  }

  public String getHostfromURL(String url) throws IOException {
    Map<String, String> paramMap = new HashMap<String, String>();

    if (url == null || url.length() == 0) {
      return null;
    } else {
      url = url.substring(url.indexOf('?') + 1);
      String paramaters[] = url.split("&");

      try {
        for (String param : paramaters) {
          String values[] = param.split("=");
          paramMap.put(values[0], values[1]);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
      return paramMap.get("host");
    }
  }

  public int execute() {
    String _cbrSwtich = null;
    if (SessionState.get() == null)
      _cbrSwtich = null;
    else
      _cbrSwtich = SessionState.get().getConf().get(ToolBox.CBR_SWITCH_ATTR);
    if (_cbrSwtich != null /* joeyli add begin */
        && _cbrSwtich.trim().equals("true") /* joeyli add end */) {
      System.out.println("Computation balance is enabled, initiate processing");
      LinkedHashMap<String, Operator<? extends Serializable>> _aliasWork = work
          .getAliasToWork();
      int axSize = _aliasWork.keySet().size();
      if (axSize == 1) {
        String _switch_str_ = SessionState.get().getConf()
            .get(ToolBox.STATISTICS_COLUMNS_ATTR);
        int _sampleWindow_ = SessionState.get().getConf()
            .getInt(SampleOperator.STATISTICS_SAMPLING_WINDOW_ATTR, 100000);

        ArrayList<Integer> _switch_ = new ArrayList<Integer>();
        StringBuilder _sbFieldNamesFromSchema_ = new StringBuilder();
        StringBuilder _sbFieldTypesFromSchema_ = new StringBuilder();
        StringBuilder _sb_ = new StringBuilder();
        ArrayList<String> _allFieldNames_ = new ArrayList<String>();

        Iterator<String> _is = _aliasWork.keySet().iterator();
        while (_is.hasNext()) {
          _sb_.append(_is.next());
        }
        String _key = _sb_.toString();
        _key = _key.substring(_key.indexOf("/") + 1);
        String _cbr_flushFileURI_ = _key + ToolBox.dotDelimiter
            + java.util.UUID.randomUUID().toString();

        job.set(ComputationBalancerReducer.CBR_FLUSHFILEURI_ATTR,
            _cbr_flushFileURI_);

        Operator<? extends Serializable> _tblScanOp_ = _aliasWork.get(_sb_
            .toString());

        try {
          URI _dataStoreURI_ = _hive.getTable(SessionState.get().getDbName(),
              _key).getDataLocation();

          if (_dataStoreURI_ == null) {
          } else {
            job.set(
                org.apache.hadoop.hive.ql.exec.ComputationBalancerReducer.CBR_TABLEDATALOCATION_ATTR,
                _dataStoreURI_.toString());
          }
        } catch (HiveException e) {
          e.printStackTrace();
        }

        try {
          List<FieldSchema> _lfs_ = _hive.getTable(
              SessionState.get().getDbName(), _key).getCols();

          if (_switch_str_ == null || _switch_str_.equals("")) {
            int _fldCounter = _lfs_.size();
            for (int i = 0; i < _fldCounter; i++) {
              _switch_.add(Integer.valueOf(i));
            }
          } else {

            StringTokenizer _st_ = new StringTokenizer(_switch_str_, ",");
            while (_st_.hasMoreTokens()) {

              String _colx = _st_.nextToken();
              int _fldCounter = _lfs_.size();
              int test = -1;

              for (int i = 0; i < _fldCounter; i++) {
                if (_lfs_.get(i).getName().equalsIgnoreCase(_colx)) {
                  test = Integer.valueOf(i);
                  _switch_.add(test);
                }
              }

              if (_colx.startsWith("col") && (test == -1)) {
                String _colx_sub_ = _colx.substring("col".length());
                try {
                  test = Integer.valueOf(_colx_sub_);
                } catch (Exception e) {
                  LOG.error("col " + _colx + " can not be found !");
                  throw new Exception("col " + _colx + " can not be found !");
                }
                _switch_.add(test);
              }

              if (test == -1) {
                LOG.error("col " + _colx + " can not be found !");
                throw new Exception("col " + _colx + " can not be found !");
              }
              LOG.debug("selected col:  " + _colx);
              LOG.debug("col index:  " + test);
            }
          }

          for (FieldSchema fieldSchema : _lfs_) {
            _sbFieldNamesFromSchema_.append(fieldSchema.getName()
                + ToolBox.commaDelimiter);
            _sbFieldTypesFromSchema_.append(fieldSchema.getType()
                + ToolBox.commaDelimiter);
          }

          for (Integer _i : _switch_) {
          }

        } catch (Exception e) {
          SessionState.get().getConf().set(ToolBox.STATISTICS_COLUMNS_ATTR, "");
          SessionState.get().getConf().set(ToolBox.CBR_SWITCH_ATTR, "");
          e.printStackTrace();
          return 1;
        }

        job.set(ToolBox.TABLE_HEADER_NAMES_ATTR,
            _sbFieldNamesFromSchema_.toString());
        job.set(ToolBox.TABLE_HEADER_TYPES_ATTR,
            _sbFieldTypesFromSchema_.toString());
        statsDesc _statsdesc = new statsDesc();
        _statsdesc.setTableName(_key);
        job.set(
            org.apache.hadoop.hive.ql.exec.ComputationBalancerReducer.CBR_TABLENAME_ATTR,
            _statsdesc.getTableName());

        try {

          Vector<StructField> _vs = _hive.getTable(
              SessionState.get().getDbName(), _key).getFields();
          for (StructField _sf : _vs) {
            _allFieldNames_.add(_sf.getFieldName());
          }

          _statsdesc.setSelStar(false);
          _statsdesc.setFieldNames(_allFieldNames_);
          _statsdesc.setFieldStatsSwitch(_switch_);

        } catch (Exception e) {
          e.printStackTrace();
        }

        {
          boolean _return = false;
          for (Integer _i : _switch_) {
            if (_i >= _allFieldNames_.size()) {
              System.out.println("[ERROR]: col" + _i
                  + " specified is out of bounce");
              _return = true;
            }
          }
          if (_return) {
            return -1;
          }
        }

        ArrayList<exprNodeDesc> _colList_ = new ArrayList<exprNodeDesc>();
        try {

          Table _tbl = _hive.getTable(SessionState.get().getDbName(), _key);
          List<FieldSchema> _lfs_ = _tbl.getCols();
          for (Integer _i_ : _switch_) {
            FieldSchema _fs = _lfs_.get(_i_);
            exprNodeColumnDesc _endesc_ = new exprNodeColumnDesc(
                TypeInfoFactory.getPrimitiveTypeInfo(_fs.getType()),
                _fs.getName(), _key, _tbl.isPartitionKey(_fs.getName()));
            _colList_.add(_endesc_);
          }

        } catch (Exception e) {
          e.printStackTrace();
        }

        StatsCollectionOperator _statsOp = (StatsCollectionOperator) (OperatorFactory
            .getAndMakeChild(_statsdesc, _tblScanOp_));

        sampleDesc _sampleDesc = new sampleDesc();
        _sampleDesc.setTableName(_key);
        _statsdesc.setColList(_colList_);
        _sampleDesc.setTableFieldNames(_allFieldNames_);
        _sampleDesc.setSwitch(_switch_);
        _sampleDesc.setNumSampleRecords(_sampleWindow_);
        SampleOperator _sampleOp = (SampleOperator) (OperatorFactory
            .getAndMakeChild(_sampleDesc, _statsOp));

        histogramDesc _histogramDesc = new histogramDesc();
        _histogramDesc.setTableName(_key);
        _histogramDesc.setColList(_colList_);
        _histogramDesc.setFieldNames(_allFieldNames_);
        _histogramDesc.setSwitch(_switch_);
        HistogramOperator _histogramOp = (HistogramOperator) (OperatorFactory
            .getAndMakeChild(_histogramDesc, _sampleOp));

      }
    }

    try {
      setNumberOfReducers();
    } catch (IOException e) {
      String statusMesg = "IOException while accessing HDFS to estimate the number of reducers: "
          + e.getMessage();
      console.printError(statusMesg,
          "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            statusMesg + "\n"
                + org.apache.hadoop.util.StringUtils.stringifyException(e));
      return 1;
    }

    String invalidReason = work.isInvalid();
    if (invalidReason != null) {
      throw new RuntimeException("Plan invalid, Reason: " + invalidReason);
    }

    String hiveScratchDir = HiveConf.getVar(job, HiveConf.ConfVars.SCRATCHDIR);
    String jobScratchDirStr = hiveScratchDir + File.separator
        + HiveConf.getVar(job,HiveConf.ConfVars.HIVEQUERYID) + "_" + Utilities.randGen.nextInt();
    Path jobScratchDir = new Path(jobScratchDirStr);
    String emptyScratchDirStr = null;
    Path emptyScratchDir = null;

    int numTries = 3;
    while (numTries > 0) {
      emptyScratchDirStr = hiveScratchDir + File.separator
          + HiveConf.getVar(job,HiveConf.ConfVars.HIVEQUERYID) + "_" + Utilities.randGen.nextInt();
      emptyScratchDir = new Path(emptyScratchDirStr);

      try {
        FileSystem fs = emptyScratchDir.getFileSystem(job);
        fs.mkdirs(emptyScratchDir);
        break;
      } catch (Exception e) {
        if (numTries > 0)
          numTries--;
        else
          throw new RuntimeException("Failed to make dir "
              + emptyScratchDir.toString() + " : " + e.getMessage());
      }
    }

    FileOutputFormat.setOutputPath(job, jobScratchDir);
    job.setMapperClass(ExecMapper.class);

    job.setMapOutputKeyClass(HiveKey.class);
    job.setMapOutputValueClass(Text.class);

    job.setNumReduceTasks(work.getNumReduceTasks().intValue());
    job.setReducerClass(ExecReducer.class);

    job.setInputFormat(org.apache.hadoop.hive.ql.io.HiveInputFormat.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    String auxJars = HiveConf.getVar(job, HiveConf.ConfVars.HIVEAUXJARS);
    String addedJars = HiveConf.getVar(job, HiveConf.ConfVars.HIVEADDEDJARS);
    if (StringUtils.isNotBlank(auxJars) || StringUtils.isNotBlank(addedJars)) {
      String allJars = StringUtils.isNotBlank(auxJars) ? (StringUtils
          .isNotBlank(addedJars) ? addedJars + "," + auxJars : auxJars)
          : addedJars;
      LOG.info("adding libjars: " + allJars);
      if (SessionState.get() != null)
        SessionState.get().ssLog("adding libjars: " + allJars);
      initializeFiles("tmpjars", allJars);
    }

    String addedFiles = HiveConf.getVar(job, HiveConf.ConfVars.HIVEADDEDFILES);
    if (StringUtils.isNotBlank(addedFiles)) {
      initializeFiles("tmpfiles", addedFiles);
    }

    int returnVal = 0;
    JobClient jc = null;
    RunningJob rj = null, orig_rj = null;
    boolean success = false;
    boolean collectisok = false;

    boolean noName = StringUtils.isEmpty(HiveConf.getVar(job,
        HiveConf.ConfVars.HADOOPJOBNAME));

    if (noName) {
      HiveConf.setVar(job, HiveConf.ConfVars.HADOOPJOBNAME,
          "JOB" + randGen.nextInt());
    }

    try {
      boolean adjustResource = job.getBoolean(HiveConf.ConfVars.HIVE_ADJUST_RESOURCE_ENABLE.varname, 
          HiveConf.ConfVars.HIVE_ADJUST_RESOURCE_ENABLE.defaultBoolVal);
      if(adjustResource){
        
        String javaMapOpts = job.get(HiveConf.ConfVars.HIVE_MAP_CHILD_JAVA_OPTS.varname, 
            HiveConf.ConfVars.HIVE_MAP_CHILD_JAVA_OPTS.defaultVal);
        //String javaReduceOpts = job.get(HiveConf.ConfVars.HIVE_REDUCE_CHILD_JAVA_OPTS.varname, 
        //    HiveConf.ConfVars.HIVE_REDUCE_CHILD_JAVA_OPTS.defaultVal);
        int simpleMapMemoryInMB = job.getInt(HiveConf.ConfVars.HIVE_SIMPLE_MAP_MEMORY_INMB.varname, 
            HiveConf.ConfVars.HIVE_SIMPLE_MAP_MEMORY_INMB.defaultIntVal);
        int adjustMemoryMb = job.getInt(HiveConf.ConfVars.HIVE_ADJUST_MEMORY_VARIATION_INMB.varname,
            HiveConf.ConfVars.HIVE_ADJUST_MEMORY_VARIATION_INMB.defaultIntVal);
        int simpleMapContainerMemoryInMB = job.getInt(HiveConf.ConfVars.HIVE_SIMPLE_MAP_CONTAINER_MEMORY_INMB.varname, 
            HiveConf.ConfVars.HIVE_SIMPLE_MAP_CONTAINER_MEMORY_INMB.defaultIntVal);
  
        LOG.info("XXjavaMapOpts=" + javaMapOpts);
        //SessionState.get().ssLog("XXjavaMapOpts=" + javaMapOpts);
        LOG.info("XXsimpleMapMemoryInMB=" + simpleMapMemoryInMB);
        //SessionState.get().ssLog("XXsimpleMapMemoryInMB=" + simpleMapMemoryInMB);
        LOG.info("XXsimpleMapContainerMemoryInMB=" + simpleMapContainerMemoryInMB);
        //SessionState.get().ssLog("XXsimpleMapContainerMemoryInMB=" + simpleMapContainerMemoryInMB);
        
        boolean isContainComplexOp = containComplexOpInMap(work);
        if(!isContainComplexOp){
          try{
            String newMapOpts = adjustHeapSizeFromOpts(javaMapOpts, simpleMapMemoryInMB);
            if(newMapOpts != null){
              job.set(HiveConf.ConfVars.HIVE_MAP_CHILD_JAVA_OPTS.varname, newMapOpts);
              job.setInt("mapreduce.map.memory.mb", simpleMapContainerMemoryInMB);
            }
          }
          catch(Exception x){
            LOG.warn("XXXadjust memory failed, as hive.simple.map.opts is not right set");
            SessionState.get().ssLog("XXXadjust memory failed, as hive.simple.map.opts is not raightlly set");
          }         
        }
      }

      addInputPaths(job, work, emptyScratchDirStr);
      Utilities.setMapRedWork(job, work);
      if (SessionState.get() != null)
        SessionState.get().ssLog("setMapRedWork success");

      String gname = null;
      String uname = null;
      List<String> roles = null;
      try {
        SessionState ss = SessionState.get();
        if (ss != null && ss.getUserName() != null) {
          uname = ss.getUserName();

          HiveConf conf = ss.getConf();
          if (conf == null || conf.get("tdw.ugi.groupname") == null) {
            roles = db.getPlayRoles(uname);

            if (roles == null || roles.isEmpty()) {
              gname = db.getGroupName(uname);
              if (gname == null || gname.isEmpty()) {
                gname = "default";
              }
            } else if (roles.size() > 1) {
              gname = "default";
            } else {
              gname = roles.get(0);
            }
          } else {
            gname = conf.get("tdw.ugi.groupname");
          }

          String keyPoolName = conf.get("tdw.ugi.groupname.keypool");
          if (keyPoolName != null) {
            gname = keyPoolName;
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        LOG.error("can not get group of user " + uname);
      }

      LOG.info("########tdw.ugi.groupname=" + gname);

      if (gname != null)
        HiveConf.setVar(job, HiveConf.ConfVars.USERGROUPNAME, gname);

      jc = new JobClient(job);

      String bakJobJar = job.getJar();

      Throttle.checkJobTrackerWithHotBackup(job, LOG, jc);

      GregorianCalendar gc = new GregorianCalendar();
      String stime = String.format("%1$4d%2$02d%3$02d%4$02d%5$02d%6$02d",
          gc.get(Calendar.YEAR), gc.get(Calendar.MONTH) + 1,
          gc.get(Calendar.DAY_OF_MONTH), gc.get(Calendar.HOUR_OF_DAY),
          gc.get(Calendar.MINUTE), gc.get(Calendar.SECOND));

      try {
        if (SessionState.get() != null)
          SessionState.get().ssLog("Begin to submit job");
        orig_rj = rj = jc.submitJob(job);
        if (SessionState.get() != null)
          SessionState.get().ssLog("Submit job success");
      } catch (Exception e) {
        LOG.error("submitJob failed " + e.toString());
        if (SessionState.get() != null)
          SessionState.get().ssLog("submitJob failed " + e.toString());
        e.printStackTrace();

        try {
          jc.close();
        } catch (Exception x) {

        }

        int count = 0;
        int wtime = HiveConf.getIntVar(conf,
            HiveConf.ConfVars.JOBCLIENTNPEWAITTIME);
        int rtime = HiveConf.getIntVar(conf,
            HiveConf.ConfVars.JOBCLIENTNPERETRYTIME);
        while (count < rtime) {
          try {
            try {
              Thread.sleep(wtime);
            } catch (InterruptedException e1) {
              LOG.error(e1.getMessage());
            }
            job.setJar(bakJobJar);

            jc = new JobClient(job);

            orig_rj = rj = jc.submitJob(job);
            break;
          } catch (Exception e2) {
            e2.printStackTrace();
            LOG.error("submitJob failed " + count + " "
                + org.apache.hadoop.util.StringUtils.stringifyException(e2));
            if (SessionState.get() != null)
              SessionState.get().ssLog(
                  "submitJob failed "
                      + count
                      + " "
                      + org.apache.hadoop.util.StringUtils
                          .stringifyException(e2));
            count++;

            try {
              jc.close();
            } catch (Exception x) {

            }

            if (count >= rtime) {
              throw new RuntimeException("Failed to submit job "
                  + e.getMessage());
            }
          }
        }
      }

      runJob = rj;

      if (SessionState.get() != null) {
        SessionState ss = SessionState.get();

        try {
          if (confMap != null) {
            confMap.put("JobName", getId());
            confMap.put("JobID", rj.getJobID());
            confMap.put("MapTaskNumber", String.valueOf(job.getNumMapTasks()));
            confMap.put("ReduceTaskNumber",
                String.valueOf(job.getNumReduceTasks()));
            confMap.put("StartTime", stime);
            confMap.put("JobURL", rj.getTrackingURL());
            LOG.info("JobURL: " + rj.getTrackingURL());
            confMap.put("JtIP", getHostfromURL(rj.getTrackingURL()));
          }
        } catch (Exception x) {

        }

        if (/*(!(ss.getiscli())) &&*/ this.getIndex() >= 0
            && ss.getTdw_query_info() != null) {
          boolean test_1 = SessionState.updateRunningJob(ss.getTdw_query_info()
              .getQueryId(), rj);
          boolean test_2 = SessionState.updateUserName(ss.getTdw_query_info()
              .getQueryId(), ss.getTdw_query_info().getUserName());
          tdw_query_stat tmpstat = new tdw_query_stat(ss.getTdw_query_info()
              .getQueryId(), this.getIndex(), rj.getJobID(), stime, "",
              "running", job.getNumMapTasks(), job.getNumReduceTasks(),
              getHostfromURL(rj.getTrackingURL()));
          Connection cc = null;
          try {
            if (SessionState.get().getBIStore() != null) {
              cc = ss.getBIStore().openConnect();
              if (cc != null) {
                ss.getBIStore().insertStat(cc, tmpstat);
                ss.getBIStore().closeConnect(cc);
              } else {
                LOG.error("add_tdw_query_stat into BI failed: "
                    + ss.getCurrnetQueryId());
              }
            }
          } catch (Exception e) {
            ss.getBIStore().closeConnect(cc);
            LOG.error("add_tdw_query_stat into BI failed: "
                + ss.getCurrnetQueryId());
            e.printStackTrace();
          }

        }
      }

      runningJobKillURIs.put(rj.getJobID(), rj.getTrackingURL()
          + "&action=kill");

      jobInfo(rj);
      rj = jobProgress(jc, rj);

      if (rj == null) {
        rj = orig_rj;
        success = false;
      } else {
        try {
          success = rj.isSuccessful();
        } catch (Exception e) {
          int count = 0;
          int wtime = HiveConf.getIntVar(conf,
              HiveConf.ConfVars.JOBCLIENTNPEWAITTIME);
          int rtime = HiveConf.getIntVar(conf,
              HiveConf.ConfVars.JOBCLIENTNPERETRYTIME);
          while (count < rtime) {
            try {
              try {
                Thread.sleep(wtime);
              } catch (InterruptedException e1) {
                LOG.error(e1.getMessage());
              }
              success = rj.isSuccessful();
              break;
            } catch (Exception e2) {
              LOG.error("isSuccessful failed " + count + " "
                  + org.apache.hadoop.util.StringUtils.stringifyException(e2));
              if (SessionState.get() != null)
                SessionState.get().ssLog(
                    "isSuccessful failed "
                        + count
                        + " "
                        + org.apache.hadoop.util.StringUtils
                            .stringifyException(e2));
              count++;
            }
          }
        }
      }

      if (success && _cbrSwtich != null
          && _cbrSwtich.trim().equals("true") ) {
        try {
          SessionState.get().getConf().set(ToolBox.CBR_SWITCH_ATTR, "");
          String cbrDirPath = job
              .get(ComputationBalancerReducer.CBR_FLUSHFILEURI_ATTR);
          if (cbrDirPath != null && !cbrDirPath.equals("")) {
            staticsInfoCollectWork work = new staticsInfoCollectWork("/tmp/"
                + cbrDirPath, false);
            staticsInfoCollectTask task = new staticsInfoCollectTask();
            task.initialize(SessionState.get().getConf(), null);
            task.setWork(work);
            task.execute();

          }

          SessionState.get().getConf().set(ToolBox.STATISTICS_COLUMNS_ATTR, "");
          SessionState.get().getConf().set(ToolBox.CBR_SWITCH_ATTR, "");
        } catch (Exception e) {
          console.printError("Exception:" + e.toString());
          if (SessionState.get() != null)
            SessionState.get().ssLog("Exception:" + e.toString());
        }
      }

      String statusMesg = getJobEndMsg(rj.getJobID());
      if (!success) {
        statusMesg += " with errors";
        returnVal = 2;
        if (!HiveConf.getBoolVar(job, HiveConf.ConfVars.EXECPARALLEL))
          console.printError(statusMesg);
        if (SessionState.get() != null)
          SessionState.get().ssLog(statusMesg);

        if (HiveConf
            .getBoolVar(job, HiveConf.ConfVars.SHOW_JOB_FAIL_DEBUG_INFO)
            && !HiveConf.getBoolVar(job, HiveConf.ConfVars.EXECPARALLEL)) {
          LOG.info("show job fail debug information: ");
          showJobFailDebugInfo(job, rj);
        }
      } else {
        console.printInfo(statusMesg);
        if (SessionState.get() != null)
          SessionState.get().ssLog(statusMesg);
      }
    } catch (Exception e) {
      e.printStackTrace();
      String mesg = " with exception '" + Utilities.getNameMessage(e) + "'";
      if (rj != null) {
        mesg = "Ended Job = " + rj.getJobID() + mesg;
      } else {
        mesg = "Job Submission failed" + mesg;
      }
      if (!HiveConf.getBoolVar(job, HiveConf.ConfVars.EXECPARALLEL))
        console.printError(mesg,
            "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            mesg + "\n"
                + org.apache.hadoop.util.StringUtils.stringifyException(e));

      success = false;
      returnVal = 1;
    } finally {

      if (SessionState.get() != null /*&& (!(SessionState.get().getiscli()))*/) {
        GregorianCalendar gcc = new GregorianCalendar();
        String etime = String.format("%1$4d%2$02d%3$02d%4$02d%5$02d%6$02d",
            gcc.get(Calendar.YEAR), gcc.get(Calendar.MONTH) + 1,
            gcc.get(Calendar.DAY_OF_MONTH), gcc.get(Calendar.HOUR_OF_DAY),
            gcc.get(Calendar.MINUTE), gcc.get(Calendar.SECOND));
        SessionState ss = SessionState.get();
        if (this.getIndex() >= 0 && ss.getTdw_query_info() != null) {
          boolean cre = SessionState.clearQeury(ss.getTdw_query_info()
              .getQueryId());

          Connection cc = null;
          try {
            if (SessionState.get().getBIStore() != null) {
              cc = ss.getBIStore().openConnect();
              if (cc != null) {
                ss.getBIStore().updateStat(cc,
                    ss.getTdw_query_info().getQueryId(), this.getIndex());
                ss.getBIStore().closeConnect(cc);
              } else {
                LOG.error("update_tdw_query_stat into BI failed: "
                    + ss.getTdw_query_info().getQueryId());
              }
            }
          } catch (Exception e) {
            ss.getBIStore().closeConnect(cc);
            LOG.error("update_tdw_query_stat into BI failed: "
                + ss.getTdw_query_info().getQueryId());
            e.printStackTrace();
          }
          LOG.debug("----------------------------------------------------------------");
          LOG.debug("FinishTime:  " + etime);
          LOG.debug("clear session:  " + cre);
          LOG.debug("----------------------------------------------------------------");
        }
      }
      if(rj != null)
    	  LOG.info("start clear work : " + rj.getJobID());
      Utilities.clearMapRedWork(job);
      if(rj != null)
    	  LOG.info("end clear work : " + rj.getJobID());
      try {
        FileSystem fs = jobScratchDir.getFileSystem(job);
        fs.delete(jobScratchDir, true);
        fs.delete(emptyScratchDir, true);
        if(rj != null)
        	LOG.info("clean job scratch dir done : " + rj.getJobID());
        if (returnVal != 0 && rj != null) {
          rj.killJob();
        }
        runningJobKillURIs.remove(rj.getJobID());
      } catch (Exception e) {
      }
    }

    try {
      if (rj != null) {
        if (work.getAliasToWork() != null) {
          for (Operator<? extends Serializable> op : work.getAliasToWork()
              .values()) {      	  
            op.jobClose(job, success);
            LOG.info("job close op done : " + rj.getJobID() + " Operator name: " + op.getName());
          }
        }
        if (work.getReducer() != null) {
          work.getReducer().jobClose(job, success);
        }
      }
    } catch (Exception e) {
      if (success) {
        success = false;
        returnVal = 3;
        String mesg = "Job Commit failed with exception '"
            + Utilities.getNameMessage(e) + "'";
        console.printError(mesg,
            "\n" + org.apache.hadoop.util.StringUtils.stringifyException(e));
        if (SessionState.get() != null)
          SessionState.get().ssLog(
              mesg + "\n"
                  + org.apache.hadoop.util.StringUtils.stringifyException(e));
      }
    }

    try {
      if (jc != null) {
        jc.close();
      }
    } catch (Exception x) {

    }

    return (returnVal);
  }

  private String adjustHeapSizeFromOpts(String mapOpts, int mb) {
    // TODO Auto-generated method stub
    if(mapOpts == null || mb < 0){
      return null;
    }
    return mapOpts.replaceAll("\\-Xmx[0-9]+[G|g|M|m]{1}", "-Xmx" + mb + "M");
  }

  private static void printUsage() {
    System.out
        .println("ExecDriver -plan <plan-file> [-jobconf k1=v1 [-jobconf k2=v2] ...] "
            + "[-files <file1>[,<file2>] ...]");
    System.exit(1);
  }

  public static void main(String[] args) throws IOException, HiveException {

    String planFileName = null;
    ArrayList<String> jobConfArgs = new ArrayList<String>();
    boolean isSilent = false;
    String files = null;

    try {
      for (int i = 0; i < args.length; i++) {
        if (args[i].equals("-plan")) {
          planFileName = args[++i];
          System.out.println("plan = " + planFileName);
        } else if (args[i].equals("-jobconf")) {
          jobConfArgs.add(args[++i]);
        } else if (args[i].equals("-silent")) {
          isSilent = true;
        } else if (args[i].equals("-files")) {
          files = args[++i];
        }
      }
    } catch (IndexOutOfBoundsException e) {
      System.err.println("Missing argument to option");
      printUsage();
    }

    if (isSilent) {
      BasicConfigurator.resetConfiguration();
      BasicConfigurator.configure(new NullAppender());
    }

    if (planFileName == null) {
      System.err.println("Must specify Plan File Name");
      printUsage();
    }

    JobConf conf = new JobConf(ExecDriver.class);
    for (String one : jobConfArgs) {
      int eqIndex = one.indexOf('=');
      if (eqIndex != -1) {
        try {
          conf.set(one.substring(0, eqIndex),
              URLDecoder.decode(one.substring(eqIndex + 1), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
          System.err.println("Unexpected error " + e.getMessage()
              + " while encoding " + one.substring(eqIndex + 1));
          System.exit(3);
        }
      }
    }

    if (files != null) {
      conf.set("tmpfiles", files);
    }

    URI pathURI = (new Path(planFileName)).toUri();
    InputStream pathData;
    if (StringUtils.isEmpty(pathURI.getScheme())) {
      pathData = new FileInputStream(planFileName);
    } else {
      FileSystem fs = FileSystem.get(conf);
      pathData = fs.open(new Path(planFileName));
    }

    boolean localMode = ShimLoader.getHadoopShims().isLocalMode(conf);
    if (localMode) {
      String auxJars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEAUXJARS);
      String addedJars = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEADDEDJARS);
      try {
        ClassLoader loader = conf.getClassLoader();
        if (StringUtils.isNotBlank(auxJars)) {
          loader = Utilities.addToClassPath(loader,
              StringUtils.split(auxJars, ","));
        }
        if (StringUtils.isNotBlank(addedJars)) {
          loader = Utilities.addToClassPath(loader,
              StringUtils.split(addedJars, ","));
        }
        conf.setClassLoader(loader);
        Thread.currentThread().setContextClassLoader(loader);
      } catch (Exception e) {
        throw new HiveException(e.getMessage(), e);
      }
    }

    mapredWork plan = Utilities.deserializeMapRedWork(pathData, conf);
    ExecDriver ed = new ExecDriver(plan, conf, isSilent);
    int ret = ed.execute();
    if (ret != 0) {
      System.out.println("Job Failed");
      System.exit(2);
    }
  }

  public static String generateCmdLine(HiveConf hconf) {
    try {
      StringBuilder sb = new StringBuilder();
      Properties deltaP = hconf.getChangedProperties();
      boolean localMode = ShimLoader.getHadoopShims().isLocalMode(hconf);
      String hadoopSysDir = "mapred.system.dir";
      String hadoopWorkDir = "mapred.local.dir";

      for (Object one : deltaP.keySet()) {
        String oneProp = (String) one;

        if (localMode
            && (oneProp.equals(hadoopSysDir) || oneProp.equals(hadoopWorkDir)))
          continue;

        String oneValue = deltaP.getProperty(oneProp);

        sb.append("-jobconf ");
        sb.append(oneProp);
        sb.append("=");
        sb.append(URLEncoder.encode(oneValue, "UTF-8"));
        sb.append(" ");
      }

      if (localMode) {
        sb.append("-jobconf ");
        sb.append(hadoopSysDir);
        sb.append("=");
        sb.append(URLEncoder.encode(hconf.get(hadoopSysDir) + "/"
            + Utilities.randGen.nextInt(), "UTF-8"));

        sb.append(" ");
        sb.append("-jobconf ");
        sb.append(hadoopWorkDir);
        sb.append("=");
        sb.append(URLEncoder.encode(hconf.get(hadoopWorkDir) + "/"
            + Utilities.randGen.nextInt(), "UTF-8"));
      }

      return sb.toString();
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isMapRedTask() {
    return true;
  }

  @Override
  public boolean hasReduce() {
    mapredWork w = getWork();
    return w.getReducer() != null;
  }

  private boolean isEmptyPath(JobConf job, String path) throws Exception {
    Path dirPath = new Path(path);
    FileSystem inpFs = dirPath.getFileSystem(job);

    if (inpFs.exists(dirPath)) {
      FileStatus[] fStats = inpFs.listStatus(dirPath);
      if (fStats.length > 0)
        return false;
    }
    return true;
  }

  private int addInputPath(String path, JobConf job, mapredWork work,
      String hiveScratchDir, int numEmptyPaths, boolean isEmptyPath,
      String alias) throws Exception {
    assert path == null || isEmptyPath;

    Class<? extends HiveOutputFormat> outFileFormat = null;

    if (isEmptyPath) {
      LOG.info("EmptyPath: " + path);
      if (SessionState.get() != null)
        SessionState.get().ssLog("EmptyPath: " + path);
      outFileFormat = work.getPathToPartitionInfo().get(path).getTableDesc()
          .getOutputFileFormatClass();
    } else
      outFileFormat = (Class<? extends HiveOutputFormat>) (HiveSequenceFileOutputFormat.class);

    String newFile = hiveScratchDir + File.separator + (++numEmptyPaths);
    Path newPath = new Path(newFile);
    LOG.info("Changed input file to " + newPath.toString());
    if (SessionState.get() != null)
      SessionState.get().ssLog("Changed input file to " + newPath.toString());

    LinkedHashMap<String, ArrayList<String>> pathToAliases = work
        .getPathToAliases();
    if (isEmptyPath) {
      assert path != null;
      pathToAliases.put(newPath.toUri().toString(), pathToAliases.get(path));
      pathToAliases.remove(path);
    } else {
      assert path == null;
      ArrayList<String> newList = new ArrayList<String>();
      newList.add(alias);
      pathToAliases.put(newPath.toUri().toString(), newList);
    }

    work.setPathToAliases(pathToAliases);

    LinkedHashMap<String, partitionDesc> pathToPartitionInfo = work
        .getPathToPartitionInfo();
    if (isEmptyPath) {
      pathToPartitionInfo.put(newPath.toUri().toString(),
          pathToPartitionInfo.get(path));
      pathToPartitionInfo.remove(path);
    } else {
      partitionDesc pDesc = work.getAliasToPartnInfo().get(alias).clone();
      Class<? extends InputFormat> inputFormat = SequenceFileInputFormat.class;
      pDesc.getTableDesc().setInputFileFormatClass(inputFormat);
      pathToPartitionInfo.put(newPath.toUri().toString(), pDesc);
    }
    work.setPathToPartitionInfo(pathToPartitionInfo);

    String onefile = newPath.toString();
    job.setBoolean("NeedPostfix", false);
    RecordWriter recWriter = outFileFormat.newInstance().getHiveRecordWriter(
        job, newPath, Text.class, false,
        work.getAliasToPartnInfo().get(alias).getTableDesc().getProperties(),
        null);
    recWriter.close(false);
    job.setBoolean("NeedPostfix", true);
    FileInputFormat.addInputPaths(job, onefile);
    return numEmptyPaths;
  }

  private void addInputPaths(JobConf job, mapredWork work, String hiveScratchDir)
      throws Exception {
    int numEmptyPaths = 0;

    List<String> pathsProcessed = new ArrayList<String>();

    for (String oneAlias : work.getAliasToWork().keySet()) {
      LOG.info("Processing alias " + oneAlias);
      if (SessionState.get() != null)
        SessionState.get().ssLog("Processing alias " + oneAlias);
      List<String> emptyPaths = new ArrayList<String>();

      String path = null;
      for (String onefile : work.getPathToAliases().keySet()) {
        List<String> aliases = work.getPathToAliases().get(onefile);
        if (aliases.contains(oneAlias)) {
          path = onefile;

          if (pathsProcessed.contains(path))
            continue;
          pathsProcessed.add(path);

          if (SessionState.get() != null)
            SessionState.get().ssLog("Adding input file " + path);
          LOG.info("Adding input file " + path);

          if (!isEmptyPath(job, path))
            FileInputFormat.addInputPaths(job, path);
          else
            emptyPaths.add(path);
        }
      }

      for (String emptyPath : emptyPaths)
        numEmptyPaths = addInputPath(emptyPath, job, work, hiveScratchDir,
            numEmptyPaths, true, oneAlias);

      if (path == null)
        numEmptyPaths = addInputPath(null, job, work, hiveScratchDir,
            numEmptyPaths, false, oneAlias);
    }
  }

  public static String getJobStartMsg(String jobId) {
    return "Starting Job = " + jobId;
  }

  public static String getJobEndMsg(String jobId) {
    return "Ended Job = " + jobId;
  }

  @Override
  public List<? extends Node> getChildren() {
    return super.getChildTasks();
  }

  private String getTaskAttemptLogUrl(String taskTrackerHttpAddress,
      String taskAttemptId) {
    LOG.info(taskTrackerHttpAddress + "/tasklog?taskid=" + taskAttemptId
        + "&all=true");
    return taskTrackerHttpAddress + "/tasklog?taskid=" + taskAttemptId
        + "&all=true";
  }

  private static class TaskInfo {
    String jobId;
    HashSet<String> logUrls;

    public TaskInfo(String jobId) {
      this.jobId = jobId;
      logUrls = new HashSet<String>();
    }

    public void addLogUrl(String logUrl) {
      logUrls.add(logUrl);
    }

    public HashSet<String> getLogUrls() {
      return logUrls;
    }

    public String getJobId() {
      return jobId;
    }
  }

  private void showJobFailDebugInfo(JobConf conf, RunningJob rj)
      throws IOException {
    Map<String, Integer> failures = new HashMap<String, Integer>();

    Set<String> successes = new HashSet<String>();

    Map<String, TaskInfo> taskIdToInfo = new HashMap<String, TaskInfo>();

    int startIndex = 0;

    while (true) {
      TaskCompletionEvent[] taskCompletions = rj
          .getTaskCompletionEvents(startIndex);

      if (taskCompletions == null || taskCompletions.length == 0) {
        break;
      }

      for (TaskCompletionEvent t : taskCompletions) {
        String taskId = t.getTaskId();
        String jobId = rj.getJobID();
        LOG.info("taskId = " + taskId);
        LOG.info("jobId = " + jobId);

        TaskInfo ti = taskIdToInfo.get(taskId);
        if (ti == null) {
          ti = new TaskInfo(jobId);
          taskIdToInfo.put(taskId, ti);
        }

        assert (ti.getJobId().equals(jobId));
        ti.getLogUrls().add(
            getTaskAttemptLogUrl(t.getTaskTrackerHttp(), t.getTaskId()));

        if (t.getTaskStatus() != TaskCompletionEvent.Status.SUCCEEDED) {
          Integer failAttempts = failures.get(taskId);
          if (failAttempts == null) {
            failAttempts = Integer.valueOf(0);
          }
          failAttempts = Integer.valueOf(failAttempts.intValue() + 1);
          failures.put(taskId, failAttempts);
        } else {
          successes.add(taskId);
          continue;
        }

        TaskCompletionEvent.Status status = t.getTaskStatus();
        if (status == TaskCompletionEvent.Status.FAILED) {
          LOG.info("EXEC Failed: " + t.toString());

          TaskAttemptID taskAttemptId = t.getTaskAttemptId();
          String[] taskDiagnostics = rj.getTaskDiagnostics(taskAttemptId);
          if (taskDiagnostics != null) {
            for (String diagnostics : taskDiagnostics) {
              console.printError(diagnostics);
              LOG.info("taskDiagnostics :" + diagnostics);
            }
          }
          break;
        } else if (status == TaskCompletionEvent.Status.KILLED) {
          console.printError("Job Killed: " + t.toString());
          LOG.info("EXEC Killed: " + t.toString());
          break;
        } else {
          LOG.info("EXEC Other: " + t.toString());
        }
      }
      startIndex += taskCompletions.length;
    }

    for (String task : successes) {
      failures.remove(task);
    }

    if (failures.keySet().size() == 0) {
      return;
    }

    int maxFailures = 0;
    for (Integer failCount : failures.values()) {
      if (maxFailures < failCount.intValue()) {
        maxFailures = failCount.intValue();
      }
    }
    LOG.info("maxFailures = " + maxFailures);

    return;

  }

  public boolean containComplexOpInMap(Operator op){
    LOG.info("XXXXXXXXXXXXXXXXXXXop type=" + op.getClass().getName());
    if(op instanceof GroupByOperator){
      return true;
    }
    if(op instanceof PartitionerOperator){
      return true;
    }
    if(op instanceof MapJoinOperator){
      return true;
    }
    
    List<Operator> oplist = op.getChildOperators();
    boolean ret = false;
    if(oplist != null){
      for(Operator o:oplist){
        ret = containComplexOpInMap(o);
        if(ret){
          return true;
        }
      }
    }
    
    return false;
  }
  
  public boolean containComplexOpInMap(mapredWork work){
    LinkedHashMap<String, Operator<? extends Serializable>> mapTaskList = work.getAliasToWork();
    for(Entry<String, Operator<? extends Serializable>> e:mapTaskList.entrySet()){
      Operator op = e.getValue();
      boolean ret = containComplexOpInMap(op);
      if(ret){
        return true;
      }
    }

    return false;
  }

  @Override
  public String getName() {
    return "EXEC";
  }
}
