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
package org.apache.hadoop.hive.ql.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.metadata.CheckResult.PartitionResult;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Table;

import org.apache.thrift.TException;

public class HiveMetaStoreChecker {

  public static final Log LOG = LogFactory.getLog(HiveMetaStoreChecker.class);

  private Hive hive;
  private HiveConf conf;

  public HiveMetaStoreChecker(Hive hive) {
    super();
    this.hive = hive;
    this.conf = hive.getConf();
  }

  public void checkMetastore(String dbName, String tableName, QB.tableRef trf,
      CheckResult result) throws HiveException, IOException {

    if (dbName == null || "".equalsIgnoreCase(dbName)) {
      dbName = SessionState.get().getDbName();
    }

    try {
      if (tableName == null || "".equals(tableName)) {
        List<String> tables = hive.getTablesForDb(dbName, ".*");
        for (String currentTableName : tables) {
          checkTable(dbName, currentTableName, null, result);
        }

        findUnknownTables(dbName, tables, result);
      } else if (trf == null || trf.getPriPart() == null
          && trf.getSubPart() == null) {
        checkTable(dbName, tableName, null, result);
      } else {
        checkTable(dbName, tableName, trf, result);
      }
    } catch (MetaException e) {
      throw new HiveException(e);
    } catch (TException e) {
      throw new HiveException(e);
    }
  }

  void findUnknownTables(String dbName, List<String> tables, CheckResult result)
      throws IOException, MetaException, TException, HiveException {

    Set<Path> dbPaths = new HashSet<Path>();
    Set<String> tableNames = new HashSet<String>(tables);

    for (String tableName : tables) {
      Table table = hive.getTable(dbName, tableName);
      String isExternal = table.getParameters().get("EXTERNAL");
      if (isExternal == null || !"TRUE".equalsIgnoreCase(isExternal)) {
        dbPaths.add(table.getPath().getParent());
      }
    }

    for (Path dbPath : dbPaths) {
      FileSystem fs = dbPath.getFileSystem(conf);
      FileStatus[] statuses = fs.listStatus(dbPath);
      if (statuses == null) {
        throw new HiveException("not find db path: " + dbPath);
      }
      for (FileStatus status : statuses) {

        if (status.isDir() && !tableNames.contains(status.getPath().getName())) {

          result.getTablesNotInMs().add(status.getPath().getName());
        }
      }
    }
  }

  void checkTable(String dbName, String tableName, QB.tableRef trf,
      CheckResult result) throws MetaException, IOException, HiveException {

    Table table = null;

    try {
      table = hive.getTable(dbName, tableName);
    } catch (HiveException e) {
      result.getTablesNotInMs().add(tableName);
      return;
    }

    List<Path> parts = null;
    boolean findUnknownPartitions = true;

    String partName = null;
    boolean sub = false;

    if (table.isPartitioned()) {
      if (trf == null || trf.getPriPart() == null && trf.getSubPart() == null) {

        parts = Warehouse.getPartitionPaths(table.getPath(), table.getTTable()
            .getPriPartition().getParSpaces().keySet(), (table.getTTable()
            .getSubPartition() != null ? table.getTTable().getSubPartition()
            .getParSpaces().keySet() : null));
      } else {
        findUnknownPartitions = false;

        if (trf.getPriPart() != null) {
          if (table.getTTable().getPriPartition().getParSpaces()
              .containsKey(trf.getPriPart())) {
            parts = Warehouse.getPriPartitionPaths(table.getPath(), trf
                .getPriPart(),
                (table.getTTable().getSubPartition() != null ? table
                    .getTTable().getSubPartition().getParSpaces().keySet()
                    : null));
            partName = trf.getPriPart();

          } else {
            PartitionResult pr = new PartitionResult();
            pr.setTableName(tableName);
            pr.setPartitionName(trf.getPriPart());
            pr.setSub(false);
            result.getPartitionsNotInMs().add(pr);
          }
        } else if (trf.getSubPart() != null) {
          if (table.getTTable().getSubPartition() != null
              && table.getTTable().getSubPartition().getParSpaces()
                  .containsKey(trf.getSubPart())) {
            parts = Warehouse.getSubPartitionPaths(table.getPath(), table
                .getTTable().getPriPartition().getParSpaces().keySet(),
                trf.getSubPart());
            partName = trf.getSubPart();
            sub = true;
          } else {
            PartitionResult pr = new PartitionResult();
            pr.setTableName(tableName);
            pr.setPartitionName(trf.getSubPart());
            pr.setSub(true);
            result.getPartitionsNotInMs().add(pr);
          }

        } else
          throw new HiveException("Hive can only check level partitions");

      }
    }

    checkTable(table, parts, findUnknownPartitions, result, partName, sub);

  }

  void checkTable(Table table, List<Path> parts, boolean findUnknownPartitions,
      CheckResult result, String partName, boolean isSub) throws IOException,
      HiveException {

    Path tablePath = table.getPath();
    System.out.print("check table : " + tablePath.toString());
    FileSystem fs = tablePath.getFileSystem(conf);
    if (!fs.exists(tablePath)) {
      result.getTablesNotOnFs().add(table.getName());
      return;
    }

    if (parts == null) {
      return;
    }

    for (Path partPath : parts) {
      fs = partPath.getFileSystem(conf);
      if (!fs.exists(partPath)) {
        PartitionResult pr = new PartitionResult();
        pr.setPartitionName(partName);
        pr.setSub(isSub);
        pr.setTableName(table.getName());
        pr.setPath(partPath.toString());
        result.getPartitionsNotOnFs().add(pr);
      }

    }

    if (findUnknownPartitions) {
      findUnknownPartitions(table, new HashSet<Path>(parts), result);
    }
  }

  void findUnknownPartitions(Table table, Set<Path> partPaths,
      CheckResult result) throws IOException {

    Path tablePath = table.getPath();
    Set<Path> allPartDirs = new HashSet<Path>();
    getAllLeafDirs(tablePath, allPartDirs);
    allPartDirs.remove(tablePath);

    allPartDirs.removeAll(partPaths);

    for (Path partPath : allPartDirs) {
      FileSystem fs = partPath.getFileSystem(conf);
      String partitionName = getPartitionName(fs.makeQualified(tablePath),
          partPath);

      if (partitionName != null) {
        PartitionResult pr = new PartitionResult();

        pr.setPartitionName(partitionName);
        pr.setTableName(table.getName());
        pr.setPath(partPath.toString());

        result.getPartitionsNotInMs().add(pr);
      }
    }
  }

  private String getPartitionName(Path tablePath, Path partitionPath) {
    String result = null;
    Path currPath = partitionPath;
    while (currPath != null && !tablePath.equals(currPath)) {
      if (result == null) {
        result = currPath.getName();
      } else {
        result = currPath.getName() + Path.SEPARATOR + result;
      }

      currPath = currPath.getParent();
    }
    return result;
  }

  private void getAllLeafDirs(Path basePath, Set<Path> allDirs)
      throws IOException {
    getAllLeafDirs(basePath, allDirs, basePath.getFileSystem(conf));
  }

  private void getAllLeafDirs(Path basePath, Set<Path> allDirs, FileSystem fs)
      throws IOException {

    FileStatus[] statuses = fs.listStatus(basePath);

    if (statuses.length == 0) {
      allDirs.add(basePath);
    }

    for (FileStatus status : statuses) {
      if (status.isDir()) {
        getAllLeafDirs(status.getPath(), allDirs, fs);
      }
    }
  }

}
