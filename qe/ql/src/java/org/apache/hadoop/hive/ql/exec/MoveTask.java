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

import java.io.IOException;
import java.io.Serializable;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.dataImport.DBExternalTableUtil;
import org.apache.hadoop.hive.ql.dataToDB.BaseDBExternalDataLoad;
import org.apache.hadoop.hive.ql.dataToDB.LoadConfig;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.QB.PartRefType;
import org.apache.hadoop.hive.ql.plan.loadFileDesc;
import org.apache.hadoop.hive.ql.plan.loadTableDesc;
import org.apache.hadoop.hive.ql.plan.moveWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hive.serde.Constants;

public class MoveTask extends Task<moveWork> implements Serializable {

  private static final long serialVersionUID = 1L;
  static final private Log LOG = LogFactory.getLog("hive.ql.exec.MoveTask");

  public int execute() {

    try {
      loadFileDesc lfd = work.getLoadFileWork();
      //load data from fs to fs
      if (lfd != null) {//use move
        Path targetPath = new Path(lfd.getTargetDir());
        Path sourcePath = new Path(lfd.getSourceDir());
        FileSystem fs = sourcePath.getFileSystem(conf);
        if (lfd.getIsDfsDir()) {
          String mesg = "Moving data to: " + lfd.getTargetDir();
          String mesg_detail = " from " + lfd.getSourceDir();
          console.printInfo(mesg, mesg_detail);
          LOG.info(mesg + mesg_detail);
          if (SessionState.get() != null)
            SessionState.get().ssLog(mesg + mesg_detail);

          fs.delete(targetPath, true);
          if (fs.exists(sourcePath)) {
            if (!fs.rename(sourcePath, targetPath))
              throw new HiveException("Unable to rename: " + sourcePath
                  + " to: " + targetPath);
          } else if (!fs.mkdirs(targetPath))
            throw new HiveException("Unable to make directory: " + targetPath);
          
          LOG.info("end move data from : " + lfd.getSourceDir() + " to :" + lfd.getTargetDir());
        } else {//copy to local from dfs
          String mesg = "Copying data to local directory " + lfd.getTargetDir();
          String mesg_detail = " from dfs" + lfd.getSourceDir();
          LOG.info(mesg + mesg_detail);
          console.printInfo(mesg, mesg_detail);
          if (SessionState.get() != null)
            SessionState.get().ssLog(mesg + mesg_detail);

          LocalFileSystem dstFs = FileSystem.getLocal(conf);

          if (dstFs.delete(targetPath, true) || !dstFs.exists(targetPath)) {
            console.printInfo(mesg, mesg_detail);
            LOG.info(mesg + mesg_detail);
            if (SessionState.get() != null)
              SessionState.get().ssLog(mesg + mesg_detail);
            if (fs.exists(sourcePath)){
            	LOG.info("start copy to Local from :" + lfd.getSourceDir());
            	fs.copyToLocalFile(sourcePath, targetPath);
            	LOG.info("end copy to Local from :" + lfd.getSourceDir());
            }
            else {
              if (!dstFs.mkdirs(targetPath))
                throw new HiveException("Unable to make local directory: "
                    + targetPath);
            }
          } else {
            throw new AccessControlException(
                "Unable to delete the existing destination directory: "
                    + targetPath);
          }
        }
      }

      //load data from fs to table or partition
      int optimizeLevel = 0;
      HiveConf conf = SessionState.get().getConf();
      if (conf == null) {
        optimizeLevel = 2;
      } else {
        optimizeLevel = conf
            .getInt("hive.semantic.analyzer.optimizer.level", 2);
      }

      loadTableDesc tbd = work.getLoadTableWork();

      if (tbd != null) {
        String mydbname = tbd.getTable().getDBName();
        if (mydbname == null) {
          mydbname = SessionState.get().getDbName();
        }

        Table tbl = null;

        switch (optimizeLevel) {
        case 0:
          break;

        case 1:
        case 2:
        default:
          tbl = db.getTable(mydbname, tbd.getTable().getTableName());
          break;
        }

        boolean isCanInsert = false;
        switch (optimizeLevel) {
        case 0:

          isCanInsert = DBExternalTableUtil.isDBExternalCanInsert(db, mydbname,
              tbd.getTable().getTableName());
          break;

        case 1:
        case 2:
        default:

          if (mydbname.equalsIgnoreCase(mydbname)) {
            isCanInsert = DBExternalTableUtil.isDBExternalCanInsert(tbl);
          } else {
            isCanInsert = DBExternalTableUtil.isDBExternalCanInsert(db,
                mydbname, tbd.getTable().getTableName());
          }
          break;
        }

        if (isCanInsert) {
          LoadConfig config = new LoadConfig();
          Table tab = db.getTable(mydbname, tbd.getTable().getTableName());
          String tableServer = tab.getSerdeParam(Constants.TABLE_SERVER);
          String port = tab.getSerdeParam(Constants.TABLE_PORT);
          String db = tab.getSerdeParam(Constants.TBALE_DB);
          String user = tab.getSerdeParam(Constants.DB_URSER);
          String pwd = tab.getSerdeParam(Constants.DB_PWD);
          String type = tab.getSerdeParam(Constants.DB_TYPE);
          String table = tab.getSerdeParam(Constants.TABLE_NAME);
          config.setHost(tableServer);
          config.setPort(port);
          config.setDbName(db);
          config.setUser(user);
          config.setPwd(pwd);
          config.setDbType(type);
          config.setTable(tab);
          config.setDbTable(table);
          config.setDesc(tbd);
          config.setConf(conf);
          config.setFilePath(tbd.getSourceDir());
          BaseDBExternalDataLoad load = new BaseDBExternalDataLoad(config);
          load.loadDataToDBTable();
          return 0;
        }

        String mesg = "Loading data to table "
            + tbd.getTable().getTableName()
            + ((tbd.getPartitionSpec().size() > 0) ? " partition "
                + tbd.getPartitionSpec().toString() : "");
        String mesg_detail = " from " + tbd.getSourceDir();
        console.printInfo(mesg, mesg_detail);
        LOG.info(mesg + mesg_detail);
        SessionState.get().setTbName(tbd.getTable().getTableName());

        if (SessionState.get() != null)
          SessionState.get().ssLog(mesg + mesg_detail);

        if (work.getCheckFileFormat()) {

          FileStatus[] dirs;
          ArrayList<FileStatus> files;
          FileSystem fs;
          try {

            switch (optimizeLevel) {
            case 0:
              fs = FileSystem.get(
                  db.getTable(mydbname, tbd.getTable().getTableName())
                      .getDataLocation(), conf);
              break;

            case 1:
            case 2:
            default:
              fs = FileSystem.get(tbl.getDataLocation(), conf);
              break;
            }

            dirs = fs.globStatus(new Path(tbd.getSourceDir()));
            files = new ArrayList<FileStatus>();
            for (int i = 0; (dirs != null && i < dirs.length); i++) {
              files.addAll(Arrays.asList(fs.listStatus(dirs[i].getPath())));
              if (files.size() > 0)
                break;
            }
          } catch (IOException e) {
            throw new HiveException(
                "addFiles: filesystem error in check phase", e);
          }

          boolean flag = HiveFileFormatUtils.checkInputFormat(fs, conf, tbd
              .getTable().getInputFileFormatClass(), files);
          if (!flag)
            throw new HiveException(
                "Wrong file format. Please check the file's format.");
        }

        String partName = tbd.getPartName();
        String subPartName = tbd.getSubPartName();
        PartRefType prt = tbd.getPartType();

        //load to table without partition
        if (tbd.getPartitionSpec().size() == 0
            && (partName == null && subPartName == null)) {
          LOG.info("Load table : source dir " + tbd.getSourceDir()
              + ", tmp dir " + tbd.getTmpDir() + ".");
          if (SessionState.get() != null)
            SessionState.get().ssLog(
                "Load table : source dir " + tbd.getSourceDir() + ", tmp dir "
                    + tbd.getTmpDir() + ".");

          switch (optimizeLevel) {
          case 0:
            db.loadTable(new Path(tbd.getSourceDir()), mydbname, tbd.getTable()
                .getTableName(), tbd.getReplace(), new Path(tbd.getTmpDir()));
            break;

          case 1:
          case 2:
          default:
            db.loadTable(new Path(tbd.getSourceDir()), tbl, tbd.getReplace(),
                new Path(tbd.getTmpDir()));
            break;
          }
        } else {
          LOG.info("Partition is: " + partName + "  " + subPartName);
          if (SessionState.get() != null)
            SessionState.get().ssLog(
                "Partition is: " + tbd.getPartitionSpec().toString());

          switch (optimizeLevel) {
          case 0:
            db.loadPartition(new Path(tbd.getSourceDir()), mydbname, tbd
                .getTable().getTableName(), prt, partName, subPartName, tbd
                .getReplace(), new Path(tbd.getTmpDir()));
            break;

          case 1:
          case 2:
          default:
            db.loadPartition(new Path(tbd.getSourceDir()), tbl, prt, partName,
                subPartName, tbd.getReplace(), new Path(tbd.getTmpDir()));
            break;
          }
        }

      }

      return 0;
    } catch (Exception e) {
      console.printError("Failed with exception " + e.getMessage(), "\n"
          + StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "Failed with exception " + e.getMessage() + "\n"
                + StringUtils.stringifyException(e));
      return (1);
    }
  }

  public boolean isLocal() {
    loadTableDesc tbd = work.getLoadTableWork();
    if (tbd != null)
      return false;

    loadFileDesc lfd = work.getLoadFileWork();
    if (lfd != null) {
      if (lfd.getIsDfsDir()) {
        return false;
      } else
        return true;
    }

    return false;
  }

  @Override
  public List<? extends Node> getChildren() {
    return super.getChildTasks();
  }

  @Override
  public String getName() {
    return "MOVE";
  }
}
