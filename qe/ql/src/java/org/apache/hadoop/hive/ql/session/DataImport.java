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
package org.apache.hadoop.hive.ql.session;

import org.apache.hadoop.fs.FSDataOutputStream;

public class DataImport {
  private String dbName;
  private String userName;
  private String tableName;
  private String loaderID;
  private String tableDDL;
  private boolean isPart;
  private boolean isFormated;
  private boolean isRCFile;
  private String tempTableName;
  private String tempTableSavePath;
  private FSDataOutputStream tempTableOs;
  private FSDataOutputStream tableOs;
  private boolean isOverWrited;
  public DataImportState state;
  private int initTime;
  private int uploadIdleTime;

  public enum DataImportState {
    INIT, UPLOADING, INSERTING, ERROR, ENDED, COMPLETE
  }

  public DataImport() {
    dbName = null;
    tableName = null;
    loaderID = null;
    isPart = false;
    isFormated = false;
    isRCFile = false;
    tempTableName = null;
    tempTableSavePath = null;
    tempTableOs = null;
    tableOs = null;
    isOverWrited = false;
    state = DataImportState.INIT;
    initTime = 0;
    uploadIdleTime = 0;
  }

  public void serDBName(String name) {
    dbName = name;
  }

  public void setTableName(String name) {
    tableName = name;
  }

  public void setLoaderID(String id) {
    loaderID = id;
  }

  public void setTableDDL(String ddl) {
    tableDDL = ddl;
  }

  public void setIsPart(boolean part) {
    isPart = part;
  }

  public void setIsFormated(boolean format) {
    isFormated = format;
  }

  public void setIsRCFile(boolean rcfile) {
    isRCFile = rcfile;
  }

  public void setTempTableName(String name) {
    tempTableName = name;
  }

  public void setTempTableSavePath(String name) {
    tempTableSavePath = name;
  }

  public void setTempTableOs(FSDataOutputStream os) {
    tempTableOs = os;
  }

  public void setTableOs(FSDataOutputStream os) {
    tableOs = os;
  }

  public void setIsOverWrite(boolean is) {
    isOverWrited = is;
  }

  public void setUserName(String user) {
    userName = user;
  }

  public void setDIState(DataImportState st) {
    state = st;
    if (state == DataImportState.INIT)
      initTime = 0;
    if (state == DataImportState.UPLOADING)
      uploadIdleTime = 0;
  }

  public void setInitTime(int time) {
    initTime = time;
  }

  public void setUploadIdleTime(int time) {
    uploadIdleTime = time;
  }

  public String getDBName() {
    return dbName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getLoaderID() {
    return loaderID;
  }

  public String getTableDDL() {
    return tableDDL;
  }

  public boolean getIsPart() {
    return isPart;
  }

  public boolean getIsFormated() {
    return isFormated;
  }

  public boolean getIsRCFile() {
    return isRCFile;
  }

  public FSDataOutputStream getTableOs() {
    return tableOs;
  }

  public FSDataOutputStream getTempTableOs() {
    return tempTableOs;
  }

  public boolean getIsOverWrite() {
    return isOverWrited;
  }

  public String getTempTableName() {
    return tempTableName;
  }

  public String getTempTableSavePath() {
    return tempTableSavePath;
  }

  public DataImportState getDIState() {
    return state;
  }

  public String getUserName() {
    return userName;
  }

  public int getInitTime() {
    return initTime;
  }

  public int getUploadIdleTime() {
    return uploadIdleTime;
  }

  public void reset() {
    dbName = null;
    userName = null;
    tableName = null;
    loaderID = null;
    isPart = false;
    isFormated = false;
    isRCFile = false;
    tempTableName = null;
    tempTableSavePath = null;
    tempTableOs = null;
    tableOs = null;
    isOverWrited = false;
    state = DataImportState.INIT;
  }
}
