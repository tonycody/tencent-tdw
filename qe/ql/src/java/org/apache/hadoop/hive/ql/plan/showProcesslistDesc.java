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

public class showProcesslistDesc extends ddlDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  Path resFile;
  boolean isLocal = false;
  String unameToShow = null;

  private final String schema = "query_id,user_name,start_time,mr_index,mr_info,mr_progress,ip,taskid,query_string#string,string,string,string,string,string,string,string,string";

  public String getSchema() {
    return schema;
  }

  public showProcesslistDesc() {
    this.resFile = null;
  }

  public showProcesslistDesc(Path resFile) {
    this.resFile = resFile;
  }

  public Path getResFile() {
    return resFile;
  }

  public String getResFileString() {
    return getResFile().getName();
  }

  public void setResFile(Path resFile) {
    this.resFile = resFile;
  }

  public void setIsLocal(boolean isl) {
    this.isLocal = isl;
  }

  public boolean getIsLocal() {
    return this.isLocal;
  }

  public void setUname(String u) {
    this.unameToShow = u;
  }

  public String getUname() {
    return this.unameToShow;
  }
}
