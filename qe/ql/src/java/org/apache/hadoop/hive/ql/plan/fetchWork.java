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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.plan.tableDesc;

@explain(displayName = "Fetch Operator")
public class fetchWork implements Serializable {
  private static final long serialVersionUID = 1L;

  private tableDesc tblDesc;

  private List<String> fetchDir;

  private int limit;

  private String serializationNullFormat = "NULL";

  public fetchWork() {
  }

  public fetchWork(String tblDir, tableDesc tblDesc) {
    this(tblDir, tblDesc, -1);
  }

  public fetchWork(String tblDir, tableDesc tblDesc, int limit) {
    this.fetchDir = new ArrayList<String>();
    fetchDir.add(tblDir);
    this.tblDesc = tblDesc;
    this.limit = limit;
  }

  public fetchWork(List<String> fetchDir, tableDesc tbDesc) {
    this(fetchDir, tbDesc, -1);
  }

  public fetchWork(List<String> fetchDir, tableDesc tbDesc, int limit) {
    this.fetchDir = fetchDir;
    this.tblDesc = tbDesc;
    this.limit = limit;
  }

  public String getSerializationNullFormat() {
    return serializationNullFormat;
  }

  public void setSerializationNullFormat(String format) {
    serializationNullFormat = format;
  }

  public tableDesc getTblDesc() {
    return tblDesc;
  }

  public void setTblDesc(tableDesc tblDesc) {
    this.tblDesc = tblDesc;
  }

  public List<String> getFetchDir() {
    return fetchDir;
  }

  public void setFetchDir(List<String> fetchDir) {
    this.fetchDir = fetchDir;
  }

  public static List<String> convertPathToStringArray(List<Path> paths) {
    if (paths == null)
      return null;

    List<String> pathsStr = new ArrayList<String>();
    for (Path path : paths)
      pathsStr.add(path.toString());

    return pathsStr;
  }

  public static List<Path> convertStringToPathArray(List<String> paths) {
    if (paths == null)
      return null;

    List<Path> pathsStr = new ArrayList<Path>();
    for (String path : paths)
      pathsStr.add(new Path(path));

    return pathsStr;
  }

  public void setPartDir(List<String> fetchDir) {
    this.fetchDir = fetchDir;
  }

  @explain(displayName = "limit")
  public int getLimit() {
    return limit;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public String toString() {

    if (fetchDir == null)
      return "null fetchwork";

    String ret = new String("partition = ");
    for (String path : fetchDir)
      ret = ret.concat(path);

    return ret;
  }
}
