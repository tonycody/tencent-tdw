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
package org.apache.hadoop.hive.metastore;

public class DBRouterInfo {
  private String dbName = null;
  private String segmentDBUrl = null;
  private String secondarySegmentDBUrl = null;
  private boolean hasTableRouter = false;
  private String desc;

  public DBRouterInfo(String db, String dburl, String secDbUrl, boolean is,
      String detail) {
    dbName = db;
    segmentDBUrl = dburl;
    secondarySegmentDBUrl = secDbUrl;
    hasTableRouter = is;
    desc = detail;
  }

  public DBRouterInfo() {
    dbName = null;
    segmentDBUrl = null;
    secondarySegmentDBUrl = null;
    hasTableRouter = false;
    desc = null;
  }

  public void setDBName(String name) {
    dbName = name;
  }

  public void setSegmentDBUrl(String url) {
    segmentDBUrl = url;
  }

  public void setSecondarySegmentDBUrl(String url) {
    secondarySegmentDBUrl = url;
  }

  public void setHasTableRouter(boolean is) {
    hasTableRouter = is;
  }

  public void setDetail(String detail) {
    desc = detail;
  }

  public String getDBName() {
    return dbName;
  }

  public String getSegmentDBUrl() {
    return segmentDBUrl;
  }

  public String getSecondarySlaveDBUrl() {
    return secondarySegmentDBUrl;
  }

  public String getDetail() {
    return desc;
  }

  public boolean getHasTableRouter() {
    return hasTableRouter;
  }
}
