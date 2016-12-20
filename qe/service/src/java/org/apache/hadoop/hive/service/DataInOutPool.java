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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.DataImport;
import org.apache.hadoop.hive.ql.session.SessionState;

public class DataInOutPool implements Runnable {
  private List<DataImport> diList;
  private int diLimit;
  private SessionState session = null;
  public static final Log LOG = LogFactory
      .getLog(DataInOutPool.class.getName());

  public DataInOutPool(int num) {
    diLimit = num;
    diList = new ArrayList<DataImport>();
  }

  public void init() {

  }

  public DataImport getInstance() {
    synchronized (diList) {
      if (diList == null)
        return null;

      if (diList.size() >= diLimit)
        return null;

      DataImport di = new DataImport();
      diList.add(di);
      return di;
    }
  }

  public void releaseInstance(DataImport di) {
    synchronized (diList) {
      if (diList == null || di == null)
        return;

      diList.remove(di);
    }
  }

  public void cleanResource(DataImport di) {
    if (di == null)
      return;
    if (di.getTableOs() != null) {
      FSDataOutputStream os = di.getTableOs();
      try {
        os.close();
      } catch (Exception x) {
        LOG.error(x.getMessage());
      }
      di.setTableOs(null);
    }
    if (di.getTempTableOs() != null) {
      FSDataOutputStream os = di.getTempTableOs();
      try {
        os.close();
      } catch (Exception x) {
        LOG.error(x.getMessage());
      }
      di.setTempTableOs(null);
    }
    if (di.getTempTableName() != null) {
      session.setiscli(true);
      session.setUserName(di.getUserName());
      session.setDbName(di.getDBName());

      LOG.error(session.getDbName());
      LOG.error(session.getUserName());
      LOG.error("drop temp table  " + di.getTempTableName());

      Driver driver = new Driver();
      driver.run("drop table " + di.getTempTableName());
      di.setTempTableName(null);
    }
  }

  public void run() {
    if (session == null) {
      session = new SessionState(new HiveConf(SessionState.class));
      SessionState.start(session);
      session = SessionState.get();
    }
    while (true) {
      try {
        Thread.sleep(5000);
      } catch (Exception x) {
        x.printStackTrace();
      }

      synchronized (diList) {
        if (diList == null || diList.size() == 0)
          continue;

        for (int i = 0; i < diList.size();) {
          DataImport di = diList.get(i);

          switch (di.getDIState()) {
          case ERROR:
          case COMPLETE:
            cleanResource(di);
            diList.remove(di);
            break;
          case INIT:
            int initTime = di.getInitTime();
            initTime++;
            di.setInitTime(initTime);
            if (initTime > 12) {
              cleanResource(di);
              diList.remove(di);
            } else
              i++;
            break;
          case UPLOADING:
            int uploadIdleTime = di.getUploadIdleTime();
            uploadIdleTime++;
            di.setUploadIdleTime(uploadIdleTime);
            if (uploadIdleTime > 5) {
              cleanResource(di);
              diList.remove(di);
            } else
              i++;
            break;
          case INSERTING:
            i++;
            break;
          default:
            i++;
            break;
          }
        }

      }

    }
  }
}
