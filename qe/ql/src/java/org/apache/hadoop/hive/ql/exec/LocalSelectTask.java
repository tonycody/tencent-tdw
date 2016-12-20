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
package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.LocalSelectWork;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;

public class LocalSelectTask extends Task<LocalSelectWork> implements
    Serializable {
  @Override
  public List<? extends Node> getChildren() {
    return null;
  }

  @Override
  public String getName() {
    return null;
  }

  public void initialize() {
    Operator<?> topOp = work.getTopOp();
    JobConf jc = new JobConf(conf, LocalSelectTask.class);
    if (topOp != null) {
      try {
        topOp.initialize(jc, null);
      } catch (HiveException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public int execute() throws MetaException, TException {
    LOG.info("######################################in LocalSelectTask, execute, first, init operator");
    initialize();
    LOG.info("######################################in LocalSelectTask, execute, first, end init operator");
    Operator<?> topOp = work.getTopOp();
    if (topOp != null) {
      try {
        topOp.process(null, 0);
      } catch (HiveException e) {
        e.printStackTrace();
      } finally {
        try {
          topOp.jobClose(conf, true);
          topOp.close(true);
        } catch (HiveException e) {
          e.printStackTrace();
        }
      }
    }
    return 0;
  }

}
