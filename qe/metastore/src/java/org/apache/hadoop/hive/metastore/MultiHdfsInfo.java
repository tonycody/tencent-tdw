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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;

public class MultiHdfsInfo {
  private String defaultNNSchema = null;
  private boolean multiHdfsEnable = false;
  private String queryid = "";

  private Log LOG = LogFactory.getLog(this.getClass().getName());
  private HashMap<String, String> dbToHdfs = new HashMap<String, String>();
  private ArrayList<String> hdfsList = new ArrayList<String>();
  private boolean allDefault = true;
  private Configuration conf = null;
  private Random rand = new Random();

  private String scratchPath = null;
  private ArrayList<Path> allScratchDirs = new ArrayList<Path>();
  private int pathid = 10000;

  private String currentHdfsScheme = null;

  public MultiHdfsInfo(Configuration conf) {
    this.multiHdfsEnable = HiveConf.getBoolVar(conf,
        HiveConf.ConfVars.MULTIHDFSENABLE);
    String defaultFS = conf.get("fs.defaultFS");
    if(defaultFS == null){
      defaultFS = conf.get("fs.default.name");
    }
    this.defaultNNSchema = makeQualified(defaultFS);
    this.conf = conf;
    Path tmpPath = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.SCRATCHDIR));
    scratchPath = tmpPath.toUri().getPath();
  }

  public void setQueryid(String queryid) {
    this.queryid = queryid;
  }

  public boolean isMultiHdfsEnable() {
    return multiHdfsEnable;
  }
  
  public void setMultiHdfsEnable(boolean is){
	multiHdfsEnable = is;
  }
  
  public String getDefaultNNSchema(){
	return defaultNNSchema;
  }

  public static String makeQualified(String nnscheme) {
    Path nnPath = new Path(nnscheme);
    return new Path(nnPath.toUri().getScheme() + ":" + "//"
        + (nnPath.toUri().getAuthority() != null ? nnPath.toUri().getAuthority() : "")).toString();
  }

  private Path makeExternalScratchDir(URI extURI) throws IOException {
    while (true) {
      String extPath = scratchPath + File.separator
          + HiveConf.getVar(conf,HiveConf.ConfVars.HIVEQUERYID) + "_" + Integer.toString(Math.abs(rand.nextInt()));
      Path extScratchDir = new Path(extURI.getScheme(), extURI.getAuthority(),
          extPath);

      FileSystem fs = extScratchDir.getFileSystem(conf);
      if (fs.mkdirs(extScratchDir)) {
        allScratchDirs.add(extScratchDir);
        return extScratchDir;
      }
    }
  }

  private static boolean strEquals(String str1, String str2) {
    return org.apache.commons.lang.StringUtils.equals(str1, str2);
  }

  private String getExternalScratchDir(URI extURI) {
    try {
      for (Path p : allScratchDirs) {
        URI pURI = p.toUri();
        if (strEquals(pURI.getScheme(), extURI.getScheme())
            && strEquals(pURI.getAuthority(), extURI.getAuthority())) {
          return p.toString();
        }
      }
      return makeExternalScratchDir(extURI).toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public String getExternalTmpFileURI(URI extURI) {
    return nextPath(getExternalScratchDir(extURI));
  }

  private String nextPath(String base) {
    return base + File.separator + Integer.toString(pathid++);
  }

  public void checkMultiHdfsEnable(ArrayList<String> dbset)
      throws MetaException {
    if (!multiHdfsEnable)
      return;

    for (String onealias : dbset) {
      String dbname = onealias.split("/")[0];
      if (!dbToHdfs.containsKey(dbname)) {
        String hdfsScheme = getHdfsPathFromDB(dbname);
        if (!defaultNNSchema.equalsIgnoreCase(hdfsScheme)) {
          allDefault = false;
        }
        dbToHdfs.put(dbname, hdfsScheme);
        if (!hdfsList.contains(hdfsScheme)) {
          hdfsList.add(hdfsScheme);
        }
      }
    }

    if (allDefault) {
      dbToHdfs = null;
      LOG.info(this.queryid + " set the multiHdfsEnable false");
      multiHdfsEnable = false;
    }
  }

  public String getTmpHdfsScheme() {
    if (this.currentHdfsScheme != null) {
      return currentHdfsScheme;
    }

    if (hdfsList == null) {
      LOG.info(this.queryid + " no table info get");
      currentHdfsScheme = defaultNNSchema;
      return defaultNNSchema;
    }

    if (hdfsList.contains(defaultNNSchema)) {
      LOG.info(this.queryid + " no need to change");
      currentHdfsScheme = defaultNNSchema;
      return defaultNNSchema;
    }

    String nnscheme = null;
    if (hdfsList.size() == 1) {
      nnscheme = hdfsList.get(0);
    } else {
      nnscheme = defaultNNSchema;
    }

    currentHdfsScheme = nnscheme;

    return nnscheme;
  }

  private void processDir(String queryTmpdir, String deststr, URI nnURI,
      ArrayList<String> dirList) {
    Path destPath = new Path(deststr);
    try {
      FileSystem fs = destPath.getFileSystem(conf);
      fs.delete(destPath, true);
      fs.delete(new Path(queryTmpdir), true);
    } catch (IOException e) {
      LOG.info(this.queryid + " delete the bad path error");
    }

    deststr = getExternalTmpFileURI(nnURI);
    queryTmpdir = getExternalTmpFileURI(nnURI).toString();
    dirList.add(deststr);
    dirList.add(queryTmpdir);
    LOG.debug(this.queryid + " get new deststr:" + deststr);
    LOG.debug(this.queryid + " get new queryTmpdir:" + queryTmpdir);
  }

  public boolean processCtas(String queryTmpdir, String deststr, String dbname,
      ArrayList<String> dirList) throws MetaException {
    LOG.debug(this.queryid + " process ctas");
    LOG.debug(this.queryid + " queryTmpdir:" + queryTmpdir);
    LOG.debug(this.queryid + " deststr:" + deststr);
    LOG.debug(this.queryid + " dbname:" + dbname);
    String nnscheme = getHdfsPathFromDB(dbname);
    URI destURI = new Path(deststr).toUri();
    URI nnURI = new Path(nnscheme).toUri();

    if (destURI.getScheme().equalsIgnoreCase(nnURI.getScheme())
        && destURI.getAuthority().equalsIgnoreCase(nnURI.getAuthority())) {
      LOG.info(this.queryid + " no need to change");
      return false;
    }

    processDir(queryTmpdir, deststr, nnURI, dirList);
    return true;
  }

  private boolean processSelectDir(String queryTmpdir, String deststr,
      ArrayList<String> dirList) {
    LOG.debug(this.queryid + " process select dir");
    LOG.debug(this.queryid + " queryTmpdir:" + queryTmpdir);
    LOG.debug(this.queryid + " deststr:" + deststr);

    if (hdfsList == null) {
      LOG.info(this.queryid + " no table info get");
      return false;
    }

    if (hdfsList.contains(defaultNNSchema)) {
      LOG.info(this.queryid + " no need to change");
      return false;
    }

    String nnscheme = null;
    if (hdfsList.size() == 1) {
      nnscheme = hdfsList.get(0);
    } else {
      nnscheme = defaultNNSchema;
    }

    processDir(queryTmpdir, deststr, new Path(nnscheme).toUri(), dirList);

    return true;
  }

  public boolean processTmpDir(String queryTmpdir, String deststr,
      String dbname, boolean isctas, ArrayList<String> dirList)
      throws MetaException {
    if (isctas) {
      return processCtas(queryTmpdir, deststr, dbname, dirList);
    } else if (allDefault) {
      return false;
    } else {
      return processSelectDir(queryTmpdir, deststr, dirList);
    }
  }

  public String getDatabase(String name) throws NoSuchObjectException,
      MetaException {
    Database db = null;
    Connection con;
    name = name.toLowerCase();

    try {
      con = JDBCStore.getSegmentConnection(name);
    } catch (MetaStoreConnectException e1) {
      LOG.error("get database error, db=" + name + ", msg=" + e1.getMessage());
      throw new NoSuchObjectException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get database error, db=" + name + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    Statement stmt = null;

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

      stmt = con.createStatement();
      String sql = "SELECT name, hdfs_schema, description FROM DBS WHERE name='"
          + name + "'";

      ResultSet dbSet = stmt.executeQuery(sql);
      boolean isDBFind = false;

      while (dbSet.next()) {
        isDBFind = true;
        db = new Database();
        db.setName(dbSet.getString(1));
        db.setHdfsscheme(dbSet.getString(2));
        db.setDescription(dbSet.getString(3));
        break;
      }

      dbSet.close();

      if (!isDBFind) {
        LOG.error("get database error, db=" + name);
        throw new NoSuchObjectException("database " + name + " does not exist!");
      }
    } catch (SQLException sqlex) {
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      JDBCStore.closeStatement(stmt);
      JDBCStore.closeConnection(con);
    }

    return db.getHdfsscheme();
  }

  public String getHdfsPathFromDB(String db) throws MetaException {
    if (!multiHdfsEnable)
      return defaultNNSchema;

    String nnscheme = null;
    if (dbToHdfs != null) {
      if (dbToHdfs.containsKey(db))
        return dbToHdfs.get(db);
    }

    try {
      nnscheme = getDatabase(db);
    } catch (NoSuchObjectException e) {
      LOG.info(this.queryid
          + " this database is not exists, maybe a create database operation");
    } catch (Exception e) {
      throw new MetaException("get  database : " + db
          + " error, the reason is:" + e.getMessage());
    }

    if (nnscheme == null || nnscheme.isEmpty()) {
      LOG.info(this.queryid + " hdfs scheme is not set for database:" + db);
      return defaultNNSchema;
    }

    if (!(new Path(nnscheme).toUri().getScheme()).equalsIgnoreCase("hdfs")) {
      throw new MetaException("wrong scheme:" + nnscheme);
    }

    nnscheme = makeQualified(nnscheme.trim());

    LOG.info(this.queryid + " get hdfs scheme for " + db + " " + nnscheme);
    return nnscheme;
  }
}
