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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.IndexQueryInfo;

public class indexWork implements Serializable {
  private static final long serialVersionUID = 1L;

  private Path resFile;
  private List<Task<? extends Serializable>> rootTasks;
  private String astStringTree;
  boolean extended;
  private String serializationNullFormat = "NULL";

  private IndexQueryInfo indexQueryInfo;
  private Properties properties;

  public indexWork() {
  }

  public indexWork(Path resFile, IndexQueryInfo indexQueryInfo,
      Properties properties) {
    this.resFile = resFile;
    this.indexQueryInfo = indexQueryInfo;
    this.properties = properties;
  }

  public Path getResFile() {
    return resFile;
  }

  public void setResFile(Path resFile) {
    this.resFile = resFile;
  }

  public IndexQueryInfo getIndexQueryInfo() {
    return indexQueryInfo;
  }

  public void setIndexQueryInfo(IndexQueryInfo indexQueryInfo) {
    this.indexQueryInfo = indexQueryInfo;
  }

  public Properties getProperties() {
    return properties;
  }

  public void setProperties(Properties properties) {
    this.properties = properties;
  }

  @explain(displayName = "Show IndexFetch Operator")
  public String getSerializationNullFormat() {
    return serializationNullFormat;
  }

  public void setSerializationNullFormat(String format) {
    serializationNullFormat = format;
  }

}
