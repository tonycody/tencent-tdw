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
package ColumnStorage;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import Comm.SEException;
import FormatStorage.FormatDataFile;
import FormatStorage.Unit.Record;

public class ColumnStorageClient {
  static Log LOG = LogFactory.getLog("ColumnStorageClient");

  FormatDataFile[] fds = null;
  ColumnProject cp = null;
  ArrayList<Short> idx = null;
  ArrayList<String> list = null;
  Configuration conf = null;

  public ColumnStorageClient(Path path, ArrayList<Short> idx, Configuration conf)
      throws Exception {
    if (path == null || idx == null || conf == null) {
      throw new SEException.InvalidParameterException("Null parameter");
    }

    this.idx = idx;
    this.conf = conf;

    cp = new ColumnProject(path, conf);
    list = cp.getFileNameByIndex(idx);
    if (list == null || list.isEmpty()) {
      throw new SEException.InvalidParameterException(
          "Data file no match or field no exist");
    }

    int size = list.size();
    {
    }

    if (fds == null) {
      fds = new FormatDataFile[size];
    }

    for (int i = 0; i < size; i++) {
      fds[i] = new FormatDataFile(conf);
      fds[i].open(list.get(i));
    }
  }

  public Record getRecordByLine(int line) throws Exception {
    if (cp == null || idx == null) {
      throw new SEException.InvalidParameterException(
          "ColumnProject or Field Index Array Not Init");
    }

    if (fds == null || fds.length != list.size()) {
      throw new SEException.InvalidParameterException("FormatDataFile Not Init");
    }

    Record record = null;

    for (int i = 0; i < fds.length; i++) {
      Record tmpRecord = fds[i].getRecordByLine(line);
      if (tmpRecord != null) {
        if (record == null) {
          record = new Record(0);
        }
      } else {
      }
    }

    return record;
  }

  public Record getNextRecord() throws Exception {
    if (cp == null || idx == null) {
      throw new SEException.InvalidParameterException(
          "ColumnProject or Field Index Array Not Init");
    }

    if (fds == null || fds.length != list.size()) {
      throw new SEException.InvalidParameterException("FormatDataFile Not Init");
    }

    Record record = null;

    for (int i = 0; i < fds.length; i++) {
      try {

        Record tmpRecord = fds[i].getNextRecord();
        if (tmpRecord != null) {
          if (record == null) {
            record = new Record(0);
          }
        } else {
        }

      } catch (Exception e) {
        System.out.println("get exception, i:" + i);
        throw e;
      }
    }

    return record;
  }

  public void close() {
    if (fds == null) {
      return;
    }

    for (int i = 0; i < fds.length; i++) {
      try {
        if (fds[i] == null || !fds[i].isOpened()) {
          continue;
        }

        fds[i].close();
      } catch (Exception e) {
        e.printStackTrace();
        LOG.error("close fds fail:" + e.getMessage());
      }
    }
  }
}
