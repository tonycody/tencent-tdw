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

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.plan.ConditionalResolver;
import org.apache.hadoop.hive.ql.plan.ConditionalWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.thrift.TException;

public class ConditionalTask extends Task<ConditionalWork> implements
    Serializable {

  private static final long serialVersionUID = 1L;
  private List<Task<? extends Serializable>> listTasks;

  private boolean resolved = false;
  private List<Task<? extends Serializable>> resTasks;

  private ConditionalResolver resolver;
  private Object resolverCtx;

  public boolean isMapRedTask() {
    for (Task<? extends Serializable> task : listTasks)
      if (task.isMapRedTask())
        return true;

    return false;
  }

  public boolean hasReduce() {
    for (Task<? extends Serializable> task : listTasks)
      if (task.hasReduce())
        return true;

    return false;
  }

  public void initialize(HiveConf conf, DriverContext driverContext) {
    super.initialize(conf, driverContext);
  }

  @Override
  public int execute() throws MetaException, TException {
    resTasks = resolver.getTasks(conf, resolverCtx);
    resolved = true;
    for (Task<? extends Serializable> tsk : getListTasks()) {
      if (!resTasks.contains(tsk)) {
        this.driverContext.getRunnable().remove(tsk);
        console.printInfo(ExecDriver.getJobEndMsg(""
            + Utilities.randGen.nextInt())
            + ", job is filtered out (removed at runtime).");
        if (tsk.getChildTasks() != null) {
          for (Task<? extends Serializable> child : tsk.getChildTasks()) {
            child.parentTasks.remove(tsk);
            if (DriverContext.isLaunchable(child)) {
              if (this.driverContext.getRunnable().offer(tsk) == false) {
                LOG.error("Could not insert the first task into the queue");
                if (SessionState.get() != null)
                  SessionState.get().ssLog(
                      "Could not insert the first task into the queue");
                return (1);
              }
              this.driverContext.addToRunnable(child);
            }
          }
        }
      } else if (!this.driverContext.getRunnable().contains(tsk)) {
        if (this.driverContext.getRunnable().offer(tsk) == false) {
          LOG.error("Could not insert the first task into the queue");
          if (SessionState.get() != null)
            SessionState.get().ssLog(
                "Could not insert the first task into the queue");
          return (1);
        }
        this.driverContext.addToRunnable(tsk);
      }
    }
    return 0;
  }

  public ConditionalResolver getResolver() {
    return resolver;
  }

  public void setResolver(ConditionalResolver resolver) {
    this.resolver = resolver;
  }

  public Object getResolverCtx() {
    return resolverCtx;
  }

  public void setResolverCtx(Object resolverCtx) {
    this.resolverCtx = resolverCtx;
  }

  public List<Task<? extends Serializable>> getListTasks() {
    return listTasks;
  }

  public void setListTasks(List<Task<? extends Serializable>> listTasks) {
    this.listTasks = listTasks;
  }

  @Override
  public List<? extends Node> getChildren() {
    return super.getChildTasks();
  }

  public boolean done() {
    boolean ret = true;
    List<Task<? extends Serializable>> parentTasks = this.getParentTasks();
    if (parentTasks != null) {
      for (Task<? extends Serializable> par : parentTasks)
        ret = ret && par.done();
    }
    List<Task<? extends Serializable>> retTasks;
    if (resolved)
      retTasks = this.resTasks;
    else
      retTasks = getListTasks();
    if (ret && retTasks != null) {
      for (Task<? extends Serializable> tsk : retTasks)
        ret = ret && tsk.done();
    }
    return ret;
  }

  public boolean addDependentTask(Task<? extends Serializable> dependent) {
    boolean ret = false;
    if (this.getListTasks() != null) {
      for (Task<? extends Serializable> tsk : this.getListTasks()) {
        ret = ret & tsk.addDependentTask(dependent);
      }
    }
    return ret;
  }

  @Override
  public String getName() {
    return "CONDITION";
  }
}
