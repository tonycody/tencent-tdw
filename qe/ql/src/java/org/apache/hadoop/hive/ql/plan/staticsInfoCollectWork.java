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

@explain(displayName = "staticsInfoCollectWork")
public class staticsInfoCollectWork implements Serializable {
  private static final long serialVersionUID = 1L;

  private String fetchDir = "";
  private boolean rmDir = false;

  public staticsInfoCollectWork() {
  }

  public staticsInfoCollectWork(String fetchDir, boolean rmDir) {
    this.fetchDir = fetchDir;
    this.rmDir = rmDir;
  }

  public boolean getRmDir() {
    return rmDir;
  }

  public void setRmDir(boolean rmDir) {
    this.rmDir = rmDir;
  }

  public String getFetchDir() {
    return fetchDir;
  }

  public void setFetchDir(String fetchDir) {
    this.fetchDir = fetchDir;
  }

  public Path getPath() {
    if (fetchDir == null)
      return null;

    return new Path(fetchDir);
  }

  public String toString() {

    if (fetchDir == null)
      return "null staticsInfoCollectWork";

    String ret = new String("staticsInfoCollectWork" + fetchDir);

    return ret;
  }
}
