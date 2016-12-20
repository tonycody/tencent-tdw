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

import java.io.ByteArrayOutputStream;

import java.io.PrintStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;

public class JDBCExecuteThread implements Runnable {
  private Driver driver;
  private String user;
  private String cmd;
  private String dbName;
  public static final Log LOG = LogFactory.getLog(JDBCExecuteThread.class
      .getName());

  public void setUser(String username) {
    user = username;
  }

  public void setDriver(Driver dr) {
    driver = dr;
  }

  public void setCmd(String command) {
    cmd = command;
  }

  public void setDBName(String name) {
    dbName = name;
  }

  public void run() {
    SessionState session = new SessionState(new HiveConf(SessionState.class));
    SessionState.start(session);
    session.in = null;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    session.out = new PrintStream(out);
    session.err = new PrintStream(err);

    SessionState ss = SessionState.get();
    if (ss == null || driver == null)
      return;

    ss.setiscli(true);
    ss.setUserName(user);
    ss.setDbName(dbName);

    driver.setProcessState(Driver.SQLProcessState.PROCESSING);
    int exeRet = driver.run(cmd);
    if (exeRet == 0)
      driver.setProcessState(Driver.SQLProcessState.COMPLETE);
    else
      driver.setProcessState(Driver.SQLProcessState.ERROR);

    String errorMessage = null;
    if (ss != null) {
      ss.get().out.flush();
      ss.get().err.flush();
    }
    if (exeRet != 0) {
      errorMessage = err.toString();
    }
    out.reset();
    err.reset();

    if (exeRet != 0) {
      driver.setErrorMsg("Query w/ errno: " + exeRet + " " + errorMessage);
    }
    if (exeRet == 0)
      driver.setProcessState(Driver.SQLProcessState.COMPLETE);
    else
      driver.setProcessState(Driver.SQLProcessState.ERROR);

    LOG.error("SQL execute end");
  }
}
