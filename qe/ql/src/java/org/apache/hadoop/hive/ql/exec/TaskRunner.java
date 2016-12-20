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
import java.util.*;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TaskRunner extends Thread {
  protected Task<? extends Serializable> tsk;
  protected TaskResult result;
  protected SessionState ss;
  protected int jobIndex;
  protected ArrayList<Task.ExecResInfo> execinfos;

  public TaskRunner(Task<? extends Serializable> tsk, TaskResult result) {
    this.tsk = tsk;
    this.result = result;
    ss = SessionState.get();
  }

  public TaskRunner(Task<? extends Serializable> tsk, TaskResult result,
      int jobindex, ArrayList<Task.ExecResInfo> execinfos) {
    this.tsk = tsk;
    this.result = result;
    ss = SessionState.get();
    jobIndex = jobindex;
    this.execinfos = execinfos;
  }

  public Task<? extends Serializable> getTask() {
    return tsk;
  }

  public void run() {
    SessionState.start(ss);
    runSequential();
    if (tsk.taskexecinfo != null && tsk.taskexecinfo.taskid != null) {
      synchronized (this.execinfos) {
        this.execinfos.add(tsk.taskexecinfo);
      }
    }
  }

  public void runSequential() {
    try {
      tsk.setIndex(jobIndex);
      int exitVal = tsk.execute();
      result.setExitVal(exitVal);
    } catch (MetaException e) {
      e.printStackTrace();
      result.setExitVal(997);
    } catch (TException e) {
      e.printStackTrace();
      result.setExitVal(998);
    } catch (Exception e) {
      e.printStackTrace();
      result.setExitVal(999);
    }

  }

}
