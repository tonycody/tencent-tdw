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

public class showGroupsDesc extends aclDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  String pattern;
  Path resFile;

  private final String schema = "group_name,creator_name,user_num,name_list#string,string,string,string";

  public String getSchema() {
    return schema;
  }

  public showGroupsDesc(Path resFile) {
    this.resFile = resFile;
    pattern = null;
  }

  public showGroupsDesc(Path resFile, String pattern) {
    this.resFile = resFile;
    this.pattern = pattern;
  }

  @explain(displayName = "pattern")
  public String getPattern() {
    return pattern;
  }

  public void setPattern(String pattern) {
    this.pattern = pattern;
  }

  public Path getResFile() {
    return resFile;
  }

  @explain(displayName = "result file", normalExplain = false)
  public String getResFileString() {
    return getResFile().getName();
  }

  public void setResFile(Path resFile) {
    this.resFile = resFile;
  }
}
