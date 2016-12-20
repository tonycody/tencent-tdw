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
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class Task<T extends Serializable> implements Serializable,
    Node {

  private static final long serialVersionUID = 1L;
  transient boolean isdone;
  transient protected HiveConf conf;
  transient protected Hive db;
  transient protected Log LOG;
  transient protected LogHelper console;

  public static class ExecResInfo {
    public String taskid = null;
    public String in_success = null;
    public String in_error = null;
    public String out_success = null;
    public String out_error = null;
  }

  public ExecResInfo taskexecinfo = new ExecResInfo();

  public static class ExecResultCounter {
    public long readSuccessNum = 0;
    public long readErrorNum = 0;
    public long fileSinkSuccessNum = 0;
    public long fileSinkErrorNum = 0;
    public long selectSuccessNum = 0;
    public long selectErrorNum = 0;
  }

  public ExecResultCounter execResultCount = new ExecResultCounter();

  transient protected int jobIndex;

  public int getIndex() {
    return jobIndex;
  }

  public void setIndex(int index) {
    jobIndex = index;
  }

  public static class ExecErrorInfo {
    public String taskErrorInfo;
  }

  public ExecErrorInfo execErrorInfo = new ExecErrorInfo();

  protected List<Task<? extends Serializable>> childTasks;
  protected List<Task<? extends Serializable>> parentTasks;
  protected transient boolean queued;
  protected transient boolean initialized;
  protected transient DriverContext driverContext;

  public Task() {
    isdone = false;
    initialized = false;
    queued = false;
    LOG = LogFactory.getLog(this.getClass().getName());
  }

  public void initialize(HiveConf conf, DriverContext driverContext) {
    isdone = false;
    this.conf = conf;
    setInitialized();

    SessionState ss = SessionState.get();
    try {
      if (ss == null) {
        db = Hive.get(conf);
      } else {
        db = ss.getDb();
      }
    } catch (HiveException e) {
      LOG.error(StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
    this.driverContext = driverContext;

    console = new LogHelper(LOG);
  }

  public abstract int execute() throws MetaException, TException;

  public boolean fetch(Vector<String> res) throws IOException {
    assert false;
    return false;
  }

  public void setChildTasks(List<Task<? extends Serializable>> childTasks) {
    this.childTasks = childTasks;
  }

  public List<Task<? extends Serializable>> getChildTasks() {
    return childTasks;
  }

  public void setParentTasks(List<Task<? extends Serializable>> parentTasks) {
    this.parentTasks = parentTasks;
  }

  public List<Task<? extends Serializable>> getParentTasks() {
    return parentTasks;
  }

  public boolean addDependentTask(Task<? extends Serializable> dependent) {
    boolean ret = false;
    if (getChildTasks() == null) {
      setChildTasks(new ArrayList<Task<? extends Serializable>>());
    }
    if (!getChildTasks().contains(dependent)) {
      ret = true;
      getChildTasks().add(dependent);
      if (dependent.getParentTasks() == null) {
        dependent.setParentTasks(new ArrayList<Task<? extends Serializable>>());
      }
      if (!dependent.getParentTasks().contains(this)) {
        dependent.getParentTasks().add(this);
      }
    }
    return ret;
  }

  public void removeDependentTask(Task<? extends Serializable> dependent) {
    if ((getChildTasks() != null) && (getChildTasks().contains(dependent))) {
      getChildTasks().remove(dependent);
      if ((dependent.getParentTasks() != null)
          && (dependent.getParentTasks().contains(this)))
        dependent.getParentTasks().remove(this);
    }
  }

  public boolean done() {
    return isdone;
  }

  public void setDone() {
    isdone = true;
  }

  public boolean isRunnable() {
    boolean isrunnable = true;
    if (parentTasks != null) {
      for (Task<? extends Serializable> parent : parentTasks) {
        if (!parent.done()) {
          isrunnable = false;
          break;
        }
      }
    }
    return isrunnable;
  }

  protected String id;
  protected T work;

  public void setWork(T work) {
    this.work = work;
  }

  public T getWork() {
    return work;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  public boolean isMapRedTask() {
    return false;
  }

  public boolean hasReduce() {
    return false;
  }

  public void setQueued() {
    queued = true;
  }

  public boolean getQueued() {
    return queued;
  }

  public void setInitialized() {
    initialized = true;
  }

  public boolean getInitialized() {
    return initialized;
  }
}
