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
import org.apache.hadoop.fs.Path;

public class showqueryDesc extends ddlDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  private String queryid;
  private Path resFile;
  private final String schema = "query_id,query_string#string:string";

  public showqueryDesc() {
  }

  public showqueryDesc(String qid, Path res) {
    queryid = qid;
    resFile = res;
  }

  public String getQueryId() {
    return queryid;
  }

  public String getSchema() {
    return schema;
  }

  public void setQueryId(String qid) {
    queryid = qid;
  }

  public Path getResFile() {
    return resFile;
  }

  public void setResFile(Path resFile) {
    this.resFile = resFile;
  }
}
