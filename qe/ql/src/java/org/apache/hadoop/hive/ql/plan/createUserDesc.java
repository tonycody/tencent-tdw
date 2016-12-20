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

public class createUserDesc extends aclDesc implements Serializable {

  private static final long serialVersionUID = 1L;
  private String userName;
  private String passwd;
  private boolean isDBA = false;

  public createUserDesc(String userName, String passwd, boolean isDBA,
      String who, String dBconnected) {
    super();
    this.userName = userName;
    this.passwd = passwd;
    this.isDBA = isDBA;
    this.setWho(who);
    this.setDBconnected(DBconnected);
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getPasswd() {
    return passwd;
  }

  public void setPasswd(String passwd) {
    this.passwd = passwd;
  }

  public boolean isDBA() {
    return isDBA;
  }

  public void setDBA(boolean isDBA) {
    this.isDBA = isDBA;
  }

}
