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

package org.apache.hadoop.hive.ql.parse;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;

import org.antlr.runtime.tree.Tree;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.copyWork;
import org.apache.hadoop.hive.ql.plan.loadTableDesc;
import org.apache.hadoop.hive.ql.plan.moveWork;
import org.apache.hadoop.hive.ql.session.SessionState;

public class LoadSemanticAnalyzer extends BaseSemanticAnalyzer {

  boolean isLocal;
  boolean isOverWrite;

  public LoadSemanticAnalyzer(HiveConf conf) throws SemanticException {
    super(conf);
  }

  public static FileStatus[] matchFilesOrDir(FileSystem fs, Path path)
      throws IOException {
    FileStatus[] srcs = fs.globStatus(path);
    if ((srcs != null) && srcs.length == 1) {
      if (srcs[0].isDir()) {
        srcs = fs.listStatus(srcs[0].getPath());
      }
    }
    return (srcs);
  }

  private URI initializeFromURI(String fromPath) throws IOException,
      URISyntaxException {
    URI fromURI = new Path(fromPath).toUri();

    String fromScheme = fromURI.getScheme();
    String fromAuthority = fromURI.getAuthority();
    String path = fromURI.getPath();

    if (!path.startsWith("/")) {
      if (isLocal) {
        path = new Path(System.getProperty("user.dir"), path).toString();
      } else {
        path = new Path(new Path("/user/" + System.getProperty("user.name")),
            path).toString();
      }
    }

    if (StringUtils.isEmpty(fromScheme)) {
      if (isLocal) {
        fromScheme = "file";
      } else {
        URI defaultURI = FileSystem.get(conf).getUri();
        fromScheme = defaultURI.getScheme();
        fromAuthority = defaultURI.getAuthority();
      }
    }

    if (fromScheme.equals("hdfs") && StringUtils.isEmpty(fromAuthority)) {
      URI defaultURI = FileSystem.get(conf).getUri();
      fromAuthority = defaultURI.getAuthority();
    }

    LOG.debug(fromScheme + "@" + fromAuthority + "@" + path);
    return new URI(fromScheme, fromAuthority, path, null, null);
  }

  private void applyConstraints(URI fromURI, URI toURI, Tree ast,
      boolean isLocal) throws SemanticException {
    if (!fromURI.getScheme().equals("file")
        && !fromURI.getScheme().equals("hdfs")) {
      throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(ast,
          "only \"file\" or \"hdfs\" file systems accepted"));
    }

    if (isLocal && !fromURI.getScheme().equals("file")) {
      throw new SemanticException(ErrorMsg.ILLEGAL_PATH.getMsg(ast,
          "Source file system should be \"file\" if \"local\" is specified"));
    }

    try {
      FileStatus[] srcs = matchFilesOrDir(
          FileSystem.get(fromURI, conf),
          new Path(fromURI.getScheme(), fromURI.getAuthority(), fromURI
              .getPath()));

      if (srcs == null || srcs.length == 0) {
        throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(ast,
            "No files matching path " + fromURI));
      }

      for (FileStatus oneSrc : srcs) {
        if (oneSrc.isDir()) {
          throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(ast,
              "source contains directory: " + oneSrc.getPath().toString()));
        }
      }
    } catch (IOException e) {
      throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(ast), e);
    }

    if (!isLocal
        && (!StringUtils.equals(fromURI.getScheme(), toURI.getScheme()) || !StringUtils
            .equals(fromURI.getAuthority(), toURI.getAuthority()))) {
      String reason = "Move from: " + fromURI.toString() + " to: "
          + toURI.toString() + " is not valid. "
          + "Please check that values for params \"default.fs.name\" and "
          + "\"hive.metastore.warehouse.dir\" do not conflict.";
      throw new SemanticException(ErrorMsg.ILLEGAL_PATH.getMsg(ast, reason));
    }
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    isLocal = isOverWrite = false;
    Tree from_t = ast.getChild(0);
    Tree table_t = ast.getChild(1);

    if (ast.getChildCount() == 4) {
      isOverWrite = isLocal = true;
    }

    if (ast.getChildCount() == 3) {
      if (ast.getChild(2).getText().toLowerCase().equals("local")) {
        isLocal = true;
      } else {
        isOverWrite = true;
      }
    }

    URI fromURI;
    try {
      String fromPath = stripQuotes(from_t.getText());
      fromURI = initializeFromURI(fromPath);
    } catch (IOException e) {
      throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(from_t,
          e.getMessage()), e);
    } catch (URISyntaxException e) {
      throw new SemanticException(ErrorMsg.INVALID_PATH.getMsg(from_t,
          e.getMessage()), e);
    }

    tableSpec ts = new tableSpec();
    ts.init(db, conf, (ASTNode) table_t, null, null, 2);

    if (ts.tableHandle.isView()) {
      throw new SemanticException(ErrorMsg.DML_AGAINST_VIEW.getMsg());
    }

    try {
      if (db.getTable(SessionState.get().getDbName(), ts.tableName)
          .isPartitioned() || ts.partSpec != null) {
        throw new SemanticException(
            "TDW do not support load data to partitioned table directly,use insert ....select method instead!");
      }

      if (!db.hasAuth(SessionState.get().getUserName(),
          Hive.Privilege.INSERT_PRIV, SessionState.get().getDbName(),
          ts.tableName)) {
        throw new SemanticException("user : "
            + SessionState.get().getUserName()
            + " do not have insert privilege on table :"
            + SessionState.get().getDbName() + "." + ts.tableName);
      }

      if (isOverWrite
          && !db.hasAuth(SessionState.get().getUserName(),
              Hive.Privilege.INSERT_PRIV, SessionState.get().getDbName(),
              ts.tableName)) {
        throw new SemanticException("user : "
            + SessionState.get().getUserName()
            + " do not have delete(or overwrite) privilege on table :"
            + SessionState.get().getDbName() + "." + ts.tableName);
      }
    } catch (HiveException e) {
      e.printStackTrace();
      throw new SemanticException(e.toString());

    }

    URI toURI = (ts.partHandle != null) ? ts.partHandle.getDataLocation()
        : ts.tableHandle.getDataLocation();

    List<FieldSchema> parts = null;
    if (isOverWrite && (parts != null && parts.size() > 0)
        && (ts.partSpec == null || ts.partSpec.size() == 0)) {
      throw new SemanticException(ErrorMsg.NEED_PARTITION_ERROR.getMsg());
    }

    applyConstraints(fromURI, toURI, from_t, isLocal);

    Task<? extends Serializable> rTask = null;

    if (isLocal) {
      String copyURIStr = ctx.getExternalTmpFileURI(toURI);
      URI copyURI = URI.create(copyURIStr);
      rTask = TaskFactory.get(new copyWork(fromURI.toString(), copyURIStr),
          this.conf);
      fromURI = copyURI;
    }

    String loadTmpPath = ctx.getExternalTmpFileURI(toURI);
    loadTableDesc loadTableWork = new loadTableDesc(fromURI.toString(),
        loadTmpPath, Utilities.getTableDesc(ts.tableHandle),
        (ts.partSpec != null) ? ts.partSpec : new HashMap<String, String>(),
        isOverWrite);

    if (rTask != null) {
      rTask.addDependentTask(TaskFactory.get(new moveWork(loadTableWork, null,
          true), this.conf));
    } else {
      rTask = TaskFactory.get(new moveWork(loadTableWork, null, true),
          this.conf);
    }

    rootTasks.add(rTask);
  }
}
