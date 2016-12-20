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

package org.apache.hadoop.hive.ql.plan;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Task;

import StorageEngineClient.ColumnStorageHiveOutputFormat;

public class ConditionalResolverMergeFiles implements ConditionalResolver,
    Serializable {
  public static final Log LOG = LogFactory
      .getLog(ConditionalResolverMergeFiles.class);
  private static final long serialVersionUID = 1L;

  public ConditionalResolverMergeFiles() {
  }

  public static class ConditionalResolverMergeFilesCtx implements Serializable {
    private static final long serialVersionUID = 1L;
    List<Task<? extends Serializable>> listTasks;
    private String dir;

    public ConditionalResolverMergeFilesCtx() {
    }

    public ConditionalResolverMergeFilesCtx(
        List<Task<? extends Serializable>> listTasks, String dir) {
      this.listTasks = listTasks;
      this.dir = dir;
    }

    public String getDir() {
      return dir;
    }

    public void setDir(String dir) {
      this.dir = dir;
    }

    public List<Task<? extends Serializable>> getListTasks() {
      return listTasks;
    }

    public void setListTasks(List<Task<? extends Serializable>> listTasks) {
      this.listTasks = listTasks;
    }
  }

  public List<Task<? extends Serializable>> getTasks(HiveConf conf,
      Object objCtx) {
    ConditionalResolverMergeFilesCtx ctx = (ConditionalResolverMergeFilesCtx) objCtx;
    String dirName = ctx.getDir();

    List<Task<? extends Serializable>> resTsks = new ArrayList<Task<? extends Serializable>>();
    long trgtSize = conf.getLongVar(HiveConf.ConfVars.HIVEMERGEMAPFILESSIZE);

    try {
      Path dirPath = new Path(dirName);
      FileSystem inpFs = dirPath.getFileSystem(conf);
      LOG.info("ct: dirPath:\t" + dirPath.toString());
      if (inpFs.exists(dirPath)) {
        FileStatus[] fStats = inpFs.listStatus(dirPath);
        long totalSz = 0;
        for (FileStatus fStat : fStats)
          totalSz += fStat.getLen();

        long currSz = totalSz / fStats.length;
        LOG.info("ct:fStats.length:" + fStats.length);
        LOG.info("ct:totalSz:" + totalSz);
        LOG.info("ct:currSz:" + currSz);
        LOG.info("ct:trgtSize:" + trgtSize);
        if ((currSz < trgtSize / 2) && (fStats.length > 1)) {
          Task<? extends Serializable> tsk = ctx.getListTasks().get(1);
          mapredWork work = (mapredWork) tsk.getWork();

          int maxReducers = conf.getIntVar(HiveConf.ConfVars.MAXREDUCERS);
          int reducers = (int) (totalSz / (trgtSize * 4 / 5) + 1);
          reducers = Math.max(1, reducers);
          reducers = Math.min(maxReducers, reducers);
          work.setNumReduceTasks(reducers);

          resTsks.add(tsk);
          return resTsks;
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    resTsks.add(ctx.getListTasks().get(0));
    return resTsks;
  }
}
