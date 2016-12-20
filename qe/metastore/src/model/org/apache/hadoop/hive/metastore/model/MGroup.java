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

public class MGroup {

  private String GROUP_NAME;
  private String Creator;
  private String USER_LIST;
  private int USER_NUM = 0;

  public String getGroupName() {
    return GROUP_NAME;
  }

  public void setGroupName(String g) {
    GROUP_NAME = g;
  }

  public String getCreator() {
    return Creator;
  }

  public void setCreator(String c) {
    Creator = c;
  }

  public String getUSER_LIST() {
    return USER_LIST;
  }

  public void setUSER_LIST(String li) {
    USER_LIST = li;
  }

  public int getUSER_NUM() {
    return USER_NUM;
  }

  public void setUSER_NUM(int num) {
    USER_NUM = num;
  }

  public MGroup() {
  }

  public MGroup(String gname, String creator, String list, int num) {
    this.GROUP_NAME = gname;
    this.Creator = creator;
    this.USER_LIST = list;
    this.USER_NUM = num;
  }

  public MGroup(MGroup other) {
    this.GROUP_NAME = other.getGroupName();
    this.Creator = other.getCreator();
    this.USER_LIST = other.getUSER_LIST();
    this.USER_NUM = other.getUSER_NUM();
  }

}
