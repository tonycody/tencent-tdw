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

package org.apache.hadoop.hive.ql.exec;

import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.joinDesc;
import org.apache.hadoop.hive.ql.exec.persistence.RowContainer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.LongWritable;

public class JoinOperator extends CommonJoinOperator<joinDesc> implements
    Serializable {
  private static final long serialVersionUID = 1L;

  public static enum Counter {
    JOIN_ACCEPT_ROW_COUNTER, JOIN_FORWARD_ROW_COUNT
  }

  private transient SkewJoinHandler skewJoinKeyContext = null;

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    initializeChildren(hconf);
    if (this.handleSkewJoin) {
      skewJoinKeyContext = new SkewJoinHandler(this);
      skewJoinKeyContext.initiliaze(hconf);
    }
  }

  public void process(Object row, int tag) throws HiveException {
    try {
      alias = (byte) tag;

      if ((lastAlias == null) || (!lastAlias.equals(alias)))
        nextSz = joinEmitInterval;

      ArrayList<Object> nr = computeValues(row, joinValues.get(alias),
          joinValuesObjectInspectors.get(alias), joinFilters.get(alias),
          joinFilterObjectInspectors.get(alias), noOuterJoin);

      if (this.handleSkewJoin)
        skewJoinKeyContext.handleSkew(tag);

      int sz = storage.get(alias).size();
      StructObjectInspector soi = (StructObjectInspector) inputObjInspectors[tag];
      StructField sf = soi.getStructFieldRef(Utilities.ReduceField.KEY
          .toString());
      Object keyObject = soi.getStructFieldData(row, sf);

      if (alias == numAliases - 1
          && !(handleSkewJoin && skewJoinKeyContext.currBigKeyTag >= 0)) {
        if (sz == joinEmitInterval) {
          checkAndGenObject();
          storage.get(alias).clear();
        }
      } else {
        if (sz == nextSz) {
          LOG.warn("table " + alias + " has " + sz + " rows for join key "
              + keyObject);
          nextSz = getNextSize(nextSz);
        }
      }

      storage.get(alias).add(nr);
      if (SerDeUtils.isNullObject(keyObject, sf.getFieldObjectInspector(),
          this.hconf)) {
        endGroup();
        startGroup();
      }

      reporter.incrCounter(Counter.JOIN_ACCEPT_ROW_COUNTER, 1);

    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  public void closeOp(boolean abort) throws HiveException {
    if (this.handleSkewJoin) {
      skewJoinKeyContext.close(abort);
    }
    super.closeOp(abort);
  }

  @Override
  public void jobClose(Configuration hconf, boolean success)
      throws HiveException {
    if (conf.getHandleSkewJoin()) {
      int numAliases = conf.getExprs().size();
      try {
        for (int i = 0; i < numAliases; i++) {
          String specPath = this.conf.getBigKeysDirMap().get((byte) i);
          FileSinkOperator.mvFileToFinalPath(specPath, hconf, success, LOG);
          for (int j = 0; j < numAliases; j++) {
            if (j == i)
              continue;
            specPath = getConf().getSmallKeysDirMap().get((byte) i)
                .get((byte) j);
            FileSinkOperator.mvFileToFinalPath(specPath, hconf, success, LOG);
          }
        }

        if (success) {
          for (int i = 0; i < numAliases; i++) {
            String specPath = this.conf.getBigKeysDirMap().get((byte) i);
            moveUpFiles(specPath, hconf, LOG);
            for (int j = 0; j < numAliases; j++) {
              if (j == i)
                continue;
              specPath = getConf().getSmallKeysDirMap().get((byte) i)
                  .get((byte) j);
              moveUpFiles(specPath, hconf, LOG);
            }
          }
        }
      } catch (IOException e) {
        throw new HiveException(e);
      }
    }
    super.jobClose(hconf, success);
  }

  private void moveUpFiles(String specPath, Configuration hconf, Log log)
      throws IOException, HiveException {
    FileSystem fs = (new Path(specPath)).getFileSystem(hconf);
    Path finalPath = new Path(specPath);

    if (fs.exists(finalPath)) {
      FileStatus[] taskOutputDirs = fs.listStatus(finalPath);
      if (taskOutputDirs != null) {
        for (FileStatus dir : taskOutputDirs) {
          Utilities.renameOrMoveFiles(fs, dir.getPath(), finalPath);
          fs.delete(dir.getPath(), true);
        }
      }
    }
  }

  public void endGroup() throws HiveException {
    if (this.handleSkewJoin && skewJoinKeyContext.currBigKeyTag >= 0) {
      try {
        skewJoinKeyContext.endGroup();
      } catch (IOException e) {
        LOG.error(e.getMessage(), e);
        throw new HiveException(e);
      }
      return;
    } else {
      checkAndGenObject();
    }
  }
}
