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

import java.util.*;
import java.io.*;

import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.conf.HiveConf;

public class TaskFactory {

  public final static class taskTuple<T extends Serializable> {
    public Class<T> workClass;
    public Class<? extends Task<T>> taskClass;

    public taskTuple(Class<T> workClass, Class<? extends Task<T>> taskClass) {
      this.workClass = workClass;
      this.taskClass = taskClass;
    }
  }

  public static ArrayList<taskTuple<? extends Serializable>> taskvec;
  static {
    taskvec = new ArrayList<taskTuple<? extends Serializable>>();
    taskvec.add(new taskTuple<moveWork>(moveWork.class, MoveTask.class));
    taskvec.add(new taskTuple<fetchWork>(fetchWork.class, FetchTask.class));
    taskvec.add(new taskTuple<copyWork>(copyWork.class, CopyTask.class));
    taskvec.add(new taskTuple<DDLWork>(DDLWork.class, DDLTask.class));
    taskvec.add(new taskTuple<FunctionWork>(FunctionWork.class,
        FunctionTask.class));
    taskvec
        .add(new taskTuple<explainWork>(explainWork.class, ExplainTask.class));
    taskvec.add(new taskTuple<ConditionalWork>(ConditionalWork.class,
        ConditionalTask.class));
    taskvec.add(new taskTuple<ACLWork>(ACLWork.class, ACLTask.class));
    taskvec.add(new taskTuple<indexWork>(indexWork.class, IndexTask.class));
    taskvec.add(new taskTuple<DeleteWork>(DeleteWork.class, DeleteTask.class));
    taskvec.add(new taskTuple<UpdateWork>(UpdateWork.class, UpdateTask.class));
    taskvec.add(new taskTuple<LocalSelectWork>(LocalSelectWork.class,
        LocalSelectTask.class));
  }

  private static ThreadLocal<Integer> tid = new ThreadLocal<Integer>() {
    protected synchronized Integer initialValue() {
      return new Integer(0);
    }
  };

  public static int getAndIncrementId() {
    int curValue = tid.get().intValue();
    tid.set(new Integer(curValue + 1));
    return curValue;
  }

  public static void resetId() {
    tid.set(new Integer(0));
  }

  @SuppressWarnings("unchecked")
  public static <T extends Serializable> Task<T> get(Class<T> workClass,
      HiveConf conf) {

    for (taskTuple<? extends Serializable> t : taskvec) {
      if (t.workClass == workClass) {
        try {
          Task<T> ret = (Task<T>) t.taskClass.newInstance();
          ret.setId("Stage-" + Integer.toString(getAndIncrementId()));
          return ret;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    if (workClass == mapredWork.class) {

      boolean viachild = conf.getBoolVar(HiveConf.ConfVars.SUBMITVIACHILD);

      try {

        Task<T> ret = null;
        if (ShimLoader.getHadoopShims().isLocalMode(conf) || viachild) {
          ret = (Task<T>) MapRedTask.class.newInstance();
        } else {
          ret = (Task<T>) ExecDriver.class.newInstance();
        }
        ret.setId("Stage-" + Integer.toString(getAndIncrementId()));
        return ret;
      } catch (Exception e) {
        throw new RuntimeException(e.getMessage(), e);
      }

    }

    throw new RuntimeException("No task for work class " + workClass.getName());
  }

  public static <T extends Serializable> Task<T> get(T work, HiveConf conf,
      Task<? extends Serializable>... tasklist) {
    Task<T> ret = get((Class<T>) work.getClass(), conf);
    ret.setWork(work);
    if (tasklist.length == 0)
      return (ret);

    ArrayList<Task<? extends Serializable>> clist = new ArrayList<Task<? extends Serializable>>();
    for (Task<? extends Serializable> tsk : tasklist) {
      clist.add(tsk);
    }
    ret.setChildTasks(clist);
    return (ret);
  }

  public static <T extends Serializable> Task<T> getAndMakeChild(T work,
      HiveConf conf, Task<? extends Serializable>... tasklist) {
    Task<T> ret = get((Class<T>) work.getClass(), conf);
    ret.setWork(work);
    if (tasklist.length == 0)
      return (ret);

    for (Task<? extends Serializable> tsk : tasklist) {
      List<Task<? extends Serializable>> children = tsk.getChildTasks();
      if (children == null) {
        children = new ArrayList<Task<? extends Serializable>>();
      }
      children.add(ret);
      tsk.setChildTasks(children);
    }

    return (ret);
  }

}
