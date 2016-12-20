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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.InvalidTableException;
import org.apache.hadoop.hive.ql.plan.DeleteWork;
import org.apache.hadoop.hive.ql.plan.UpdateWork;
import org.apache.hadoop.hive.ql.plan.updateTableDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;

public class UpdateTask extends Task<UpdateWork> implements Serializable {

  private static final long serialVersionUID = 1L;

  @Override
  public int execute() throws MetaException, TException {
    Hive hivedb;
    try {
      hivedb = Hive.get(conf);
      updateTableDesc upDesc = work.getUpTD();
      hivedb.updateTable(upDesc.getDeletepath(), upDesc.getTmpdir_new(),
          upDesc.getTmpdir_old());
    } catch (InvalidTableException e) {
      console.printError("Table " + e.getTableName() + " does not exist");
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "Table " + e.getTableName() + " does not exist");
      LOG.debug(StringUtils.stringifyException(e));
      return 1;
    } catch (HiveException e) {
      console.printError("FAILED: Error in metadata: " + e.getMessage(), "\n"
          + StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "FAILED: Error in metadata: " + e.getMessage() + "\n"
                + StringUtils.stringifyException(e));
      LOG.debug(StringUtils.stringifyException(e));
      return 1;
    } catch (Exception e) {
      console.printError("Failed with exception " + e.getMessage(), "\n"
          + StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "Failed with exception " + e.getMessage() + "\n"
                + StringUtils.stringifyException(e));
      return 1;
    }
    return 0;
  }

  @Override
  public List<? extends Node> getChildren() {
    return super.getChildTasks();
  }

  @Override
  public String getName() {
    return "UPDATE";
  }

}
