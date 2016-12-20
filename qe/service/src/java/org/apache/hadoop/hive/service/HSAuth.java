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

package org.apache.hadoop.hive.service;

public class HSAuth implements Comparable {
  public enum UserType {
    ROOT, USER,
  }

  UserType userType;

  private String authid;

  private final String user;

  private final String passwd;

  private final String dbName;

  private final int port;

  public HSAuth(String user, String passwd, String dbName, int port) {
    this.user = user;
    this.passwd = passwd;
    this.dbName = dbName;
    this.port = port;
    if (user.equals("root")) {
      this.userType = UserType.ROOT;
    } else {
      this.userType = UserType.USER;
    }
  }

  public UserType getUserType() {
    return this.userType;
  }

  public String getAuthid() {
    return authid;
  }

  public void setAuthid(String authid) {
    this.authid = authid;
  }

  public int compareTo(Object obj) {
    if (obj == null) {
      return -1;
    }
    if (!(obj instanceof HSAuth)) {
      return -1;
    }
    HSAuth o = (HSAuth) obj;
    return o.getAuthid().compareTo(authid);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((authid == null ? 0 : authid.hashCode()));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof HSAuth)) {
      return false;
    }
    HSAuth other = (HSAuth) obj;
    if (authid == null) {
      if (other.authid != null) {
        return false;
      }
    } else if (!authid.equals(other.authid)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return Integer.toString(hashCode());
  }

  public String getUser() {
    return user;
  }

  public String getPasswd() {
    return passwd;
  }

  public String getDbName() {
    return dbName;
  }

  public int getPort() {
    return port;
  }
}
