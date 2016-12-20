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
package org.apache.hadoop.hive.metastore.model;

public class Mtdw_query_stat {

  private String queryId;
  private int currMRIndex;
  private String currMRId;
  private String currMRStartTime;
  private String currMRFinishTime;
  private String currMRState;
  private int MapNum;
  private int ReduceNum;

  public String getqueryId() {
    return queryId;
  }

  public void setqueryId(String qid) {
    queryId = qid;
  }

  public int getcurrMRIndex() {
    return currMRIndex;
  }

  public void setcurrMRIndex(int index) {
    currMRIndex = index;
  }

  public String getcurrMRId() {
    return currMRId;
  }

  public void setcurrMRId(String cid) {
    currMRId = cid;
  }

  public String getcurrMRStartTime() {
    return currMRStartTime;
  }

  public void setcurrMRStartTime(String stime) {
    currMRStartTime = stime;
  }

  public String getcurrMRFinishTime() {
    return currMRFinishTime;
  }

  public void setcurrMRFinishTime(String etime) {
    currMRFinishTime = etime;
  }

  public String getcurrMRState() {
    return currMRState;
  }

  public void setcurrMRState(String stat) {
    currMRState = stat;
  }

  public int getMapNum() {
    return MapNum;
  }

  public void setMapNum(int m) {
    MapNum = m;
  }

  public int getReduceNum() {
    return ReduceNum;
  }

  public void setReduceNum(int r) {
    ReduceNum = r;
  }

  public Mtdw_query_stat() {
  }

  public Mtdw_query_stat(String queryId, int currMRIndex, String currMRId,
      String currMRStartTime, String currMRFinishTime, String currMRState,
      int MapNum, int ReduceNum) {
    this.queryId = queryId;
    this.currMRIndex = currMRIndex;
    this.currMRId = currMRId;
    this.currMRStartTime = currMRStartTime;
    this.currMRFinishTime = currMRFinishTime;
    this.currMRState = currMRState;
    this.MapNum = MapNum;
    this.ReduceNum = ReduceNum;
  }

  public Mtdw_query_stat(Mtdw_query_stat other) {
    this.queryId = other.getqueryId();
    this.currMRIndex = other.getcurrMRIndex();
    this.currMRId = other.getcurrMRId();
    this.currMRStartTime = other.getcurrMRStartTime();
    this.currMRFinishTime = other.getcurrMRFinishTime();
    this.currMRState = other.getcurrMRState();
    this.MapNum = other.getMapNum();
    this.ReduceNum = other.getReduceNum();
  }
}
