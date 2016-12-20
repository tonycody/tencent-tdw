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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.plan.explainWork;
import org.apache.hadoop.hive.ql.plan.fetchWork;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.TextInputFormat;

public class ExplainSemanticAnalyzer extends BaseSemanticAnalyzer {

  private BaseSemanticAnalyzer sem = null;

  public BaseSemanticAnalyzer getSem() {
    return sem;
  }

  public ExplainSemanticAnalyzer(HiveConf conf) throws SemanticException {
    super(conf);
  }

  public void analyzeInternal(ASTNode ast) throws SemanticException {
    ctx.setExplain(true);

    sem = SemanticAnalyzerFactory.get(conf, (ASTNode) ast.getChild(0));
    sem.analyze((ASTNode) ast.getChild(0), ctx);

    boolean extended = false;
    if (ast.getChildCount() > 1) {
      extended = true;
    }

    ctx.setResFile(new Path(ctx.getLocalTmpFileURI()));
    List<Task<? extends Serializable>> tasks = sem.getRootTasks();
    Task<? extends Serializable> fetchTask = sem.getFetchTask();
    if (tasks == null) {
      if (fetchTask != null) {
        tasks = new ArrayList<Task<? extends Serializable>>();
        tasks.add(fetchTask);
      }
    } else if (fetchTask != null) {
      tasks.add(fetchTask);
    }
    Task<? extends Serializable> explain_fetchTask = createFetchTask("query_plan#string");

    rootTasks.add(TaskFactory.get(new explainWork(ctx.getResFile(), tasks,
        ((ASTNode) ast.getChild(0)).toStringTree(), extended), this.conf));

    setFetchTask(explain_fetchTask);

  }

  private Task<? extends Serializable> createFetchTask(String schema) {
    Properties prop = new Properties();

    prop.setProperty(Constants.SERIALIZATION_FORMAT, "9");
    prop.setProperty(Constants.SERIALIZATION_NULL_FORMAT, " ");
    String[] colTypes = schema.split("#");
    prop.setProperty("columns", colTypes[0]);
    prop.setProperty("columns.types", colTypes[1]);
    LOG.debug(ctx.getResFile());
    fetchWork fetch = new fetchWork(ctx.getResFile().toString(), new tableDesc(
        LazySimpleSerDe.class, TextInputFormat.class,
        IgnoreKeyTextOutputFormat.class, prop), -1);
    fetch.setSerializationNullFormat(" ");
    return TaskFactory.get(fetch, this.conf);
  }
}
