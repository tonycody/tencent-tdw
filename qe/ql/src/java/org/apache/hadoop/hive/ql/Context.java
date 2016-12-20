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

package org.apache.hadoop.hive.ql;

import java.io.File;
import java.io.DataInput;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Random;
import java.util.ArrayList;

import org.antlr.runtime.TokenRewriteStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.hive.common.FileUtils;

public class Context {
  private Path resFile;
  private Path resDir;
  private FileSystem resFs;
  static final private Log LOG = LogFactory.getLog("hive.ql.Context");
  private Path[] resDirPaths;
  private int resDirFilesNum;
  boolean initialized;
  private String scratchPath;
  private Path MRScratchDir;
  private Path localScratchDir;
  private ArrayList<Path> allScratchDirs = new ArrayList<Path>();
  private HiveConf conf;
  Random rand = new Random();
  protected int randomid = Math.abs(rand.nextInt());
  protected int pathid = 10000;
  protected boolean explain = false;

  private TokenRewriteStream tokenRewriteStream;

  private String userName;
  private String DBname;

  private String ctxId;

  public Context() {
  }

  public Context(HiveConf conf) {
    this.conf = conf;
    Path tmpPath = new Path(conf.getVar(HiveConf.ConfVars.SCRATCHDIR));
    scratchPath = tmpPath.toUri().getPath();

    ctxId = "sql_ctx_" + Math.random();
  }

  public Context(HiveConf conf, String userName, String DBname) {
    this.conf = conf;
    Path tmpPath = new Path(conf.getVar(HiveConf.ConfVars.SCRATCHDIR));
    scratchPath = tmpPath.toUri().getPath();

    this.userName = userName;
    this.DBname = DBname;

    ctxId = "sql_ctx_" + Math.random();

  }

  public void setExplain(boolean value) {
    explain = value;
  }

  public String getUserName() {
    return userName;
  }

  public String getCtxId() {
    return ctxId;
  }

  public void setCtxId(String ctxId) {
    this.ctxId = ctxId;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getDBname() {
    return DBname;
  }

  public void setDBname(String dBname) {
    DBname = dBname;
  }

  public boolean getExplain() {
    return explain;
  }

  private void makeLocalScratchDir() throws IOException {
    while (true) {
      localScratchDir = new Path(System.getProperty("java.io.tmpdir")
          + File.separator + Math.abs(rand.nextInt()));
      FileSystem fs = FileSystem.getLocal(conf);
      if (fs.mkdirs(localScratchDir)) {
        localScratchDir = fs.makeQualified(localScratchDir);
        allScratchDirs.add(localScratchDir);
        break;
      }
    }
  }

  private void makeMRScratchDir() throws IOException {
    while (true) {
      MRScratchDir = FileUtils.makeQualified(
          new Path(conf.getVar(HiveConf.ConfVars.SCRATCHDIR), conf.getVar(HiveConf.ConfVars.HIVEQUERYID) + "_" + Integer
              .toString(Math.abs(rand.nextInt()))), conf);

      if (explain) {
        allScratchDirs.add(MRScratchDir);
        return;
      }

      FileSystem fs = MRScratchDir.getFileSystem(conf);
      if (fs.mkdirs(MRScratchDir)) {
        allScratchDirs.add(MRScratchDir);
        return;
      }
    }
  }

  private Path makeExternalScratchDir(URI extURI) throws IOException {
    while (true) {
      String extPath = scratchPath + File.separator
          + conf.getVar(HiveConf.ConfVars.HIVEQUERYID) + "_" + Integer.toString(Math.abs(rand.nextInt()));
      Path extScratchDir = new Path(extURI.getScheme(), extURI.getAuthority(),
          extPath);

      if (explain) {
        allScratchDirs.add(extScratchDir);
        return extScratchDir;
      }

      FileSystem fs = extScratchDir.getFileSystem(conf);
      if (fs.mkdirs(extScratchDir)) {
        allScratchDirs.add(extScratchDir);
        return extScratchDir;
      }
    }
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

  private String getMRScratchDir() {
    if (MRScratchDir == null) {
      try {
        makeMRScratchDir();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return MRScratchDir.toString();
  }

  private String getLocalScratchDir() {
    if (localScratchDir == null) {
      try {
        makeLocalScratchDir();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return localScratchDir.toString();
  }

  private void removeScratchDir() {
    if (explain) {
      try {
        if (localScratchDir != null)
          FileSystem.getLocal(conf).delete(localScratchDir);
      } catch (Exception e) {
        LOG.warn("Error Removing Scratch: " + StringUtils.stringifyException(e));
      }
    } else {
      for (Path p : allScratchDirs) {
        try {
          p.getFileSystem(conf).delete(p);
        } catch (Exception e) {
          LOG.warn("Error Removing Scratch: "
              + StringUtils.stringifyException(e));
        }
      }
    }
    MRScratchDir = null;
    localScratchDir = null;
  }

  private String nextPath(String base) {
    return base + File.separator + Integer.toString(pathid++);
  }

  public boolean isMRTmpFileURI(String uriStr) {
    return (uriStr.indexOf(scratchPath) != -1);
  }

  public String getMRTmpFileURI() {
    return nextPath(getMRScratchDir());
  }

  public String getLocalTmpFileURI() {
    return nextPath(getLocalScratchDir());
  }

  public String getExternalTmpFileURI(URI extURI) {
    return nextPath(getExternalScratchDir(extURI));
  }

  public Path getResFile() {
    return resFile;
  }

  public void setResFile(Path resFile) {
    this.resFile = resFile;
    resDir = null;
    resDirPaths = null;
    resDirFilesNum = 0;
  }

  public Path getResDir() {
    return resDir;
  }

  public void addScratchDir(Path newpath) {
    allScratchDirs.add(newpath);
  }

  public void setResDir(Path resDir) {
    this.resDir = resDir;
    resFile = null;

    resDirFilesNum = 0;
    resDirPaths = null;
  }

  public void clear() throws IOException {
    if (resDir != null) {
      try {
        FileSystem fs = resDir.getFileSystem(conf);
        fs.delete(resDir, true);
      } catch (IOException e) {
        LOG.info("Context clear error: " + StringUtils.stringifyException(e));
      }
    }

    if (resFile != null) {
      try {
        FileSystem fs = resFile.getFileSystem(conf);
        fs.delete(resFile, false);
      } catch (IOException e) {
        LOG.info("Context clear error: " + StringUtils.stringifyException(e));
      }
    }
    removeScratchDir();
  }

  public DataInput getStream() {
    try {
      if (!initialized) {
        initialized = true;
        if ((resFile == null) && (resDir == null))
          return null;

        if (resFile != null) {
          return (DataInput) resFile.getFileSystem(conf).open(resFile);
        }

        resFs = resDir.getFileSystem(conf);
        FileStatus status = resFs.getFileStatus(resDir);
        assert status.isDir();
        FileStatus[] resDirFS = resFs.globStatus(new Path(resDir + "/*"));
        resDirPaths = new Path[resDirFS.length];
        int pos = 0;
        for (FileStatus resFS : resDirFS)
          if (!resFS.isDir())
            resDirPaths[pos++] = resFS.getPath();
        if (pos == 0)
          return null;

        return (DataInput) resFs.open(resDirPaths[resDirFilesNum++]);
      } else {
        return getNextStream();
      }
    } catch (FileNotFoundException e) {
      LOG.info("getStream error: " + StringUtils.stringifyException(e));
      return null;
    } catch (IOException e) {
      LOG.info("getStream error: " + StringUtils.stringifyException(e));
      return null;
    }
  }

  private DataInput getNextStream() {
    try {
      if (resDir != null && resDirFilesNum < resDirPaths.length
          && (resDirPaths[resDirFilesNum] != null))
        return (DataInput) resFs.open(resDirPaths[resDirFilesNum++]);
    } catch (FileNotFoundException e) {
      LOG.info("getNextStream error: " + StringUtils.stringifyException(e));
      return null;
    } catch (IOException e) {
      LOG.info("getNextStream error: " + StringUtils.stringifyException(e));
      return null;
    }

    return null;
  }

  private static boolean strEquals(String str1, String str2) {
    return org.apache.commons.lang.StringUtils.equals(str1, str2);
  }

  public void setTokenRewriteStream(TokenRewriteStream tokenRewriteStream) {
    this.tokenRewriteStream = tokenRewriteStream;
  }

  public TokenRewriteStream getTokenRewriteStream() {
    return tokenRewriteStream;
  }
}
