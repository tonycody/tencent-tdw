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

public class Mtdw_query_info {

  private String queryId;
  private String userName;
  private String sessionId;
  private String startTime;
  private String finishTime;
  private String queryString;
  private int MRNum;
  private String queryState;
  private String ip;
  private String taskid;

  public String getIP() {
    return ip;
  }

  public void setIP(String iip) {
    ip = iip;
  }

  public String getTaskId() {
    return taskid;
  }

  public void setTaskId(String tid) {
    taskid = tid;
  }

  public String getqueryId() {
    return queryId;
  }

  public void setqueryId(String qid) {
    queryId = qid;
  }

  public String getuserName() {
    return userName;
  }

  public void setuserName(String uname) {
    userName = uname;
  }

  public String getsessionId() {
    return sessionId;
  }

  public void setsessionId(String sid) {
    sessionId = sid;
  }

  public String getfinishTime() {
    return finishTime;
  }

  public void setfinishTime(String etime) {
    finishTime = etime;
  }

  public String getstartTime() {
    return startTime;
  }

  public void setstartTime(String stime) {
    startTime = stime;
  }

  public String getqueryString() {
    return queryString;
  }

  public void setqueryString(String qstr) {
    queryString = qstr;
  }

  public String getqueryState() {
    return queryState;
  }

  public void setqueryState(String qs) {
    queryState = qs;
  }

  public int getMRNum() {
    return MRNum;
  }

  public void setMRNum(int n) {
    MRNum = n;
  }

  public Mtdw_query_info() {
  }

  public Mtdw_query_info(String queryId, String userName, String sessionId,
      String startTime, String finishTime, String queryString, int MRNum,
      String queryState, String iip, String itaskid) {
    this.queryId = queryId;
    this.userName = userName;
    this.sessionId = sessionId;
    this.startTime = startTime;
    this.finishTime = finishTime;
    this.queryString = queryString;
    this.MRNum = MRNum;
    this.queryState = queryState;
    this.ip = iip;
    this.taskid = itaskid;
  }

  public Mtdw_query_info(Mtdw_query_info other) {
    this.queryId = other.getqueryId();
    this.userName = other.getuserName();
    this.sessionId = other.getsessionId();
    this.startTime = other.getstartTime();
    this.finishTime = other.getfinishTime();
    this.queryString = other.getqueryString();
    this.MRNum = other.getMRNum();
    this.queryState = other.getqueryState();
    this.ip = other.getIP();
    this.taskid = other.getTaskId();
  }

}
