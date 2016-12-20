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

package org.apache.hadoop.hive.metastore;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;

/*
 import core.org.apache.hadoop.conf.Configuration;
 import core.org.apache.hadoop.fs.FileStatus;
 import core.org.apache.hadoop.fs.FileSystem;
 import core.org.apache.hadoop.fs.Path;
 import core.org.apache.hadoop.fs.Trash;
 */

public class Warehouse {
  private Path whRoot = null;
  private Configuration conf;
  private String whRootString;
  private MultiHdfsInfo multiHdfsInfo = null;

  public static final Log LOG = LogFactory.getLog("hive.metastore.warehouse");

  public Warehouse(Configuration conf) throws MetaException {
    this.conf = conf;
    whRootString = HiveConf.getVar(conf, HiveConf.ConfVars.METASTOREWAREHOUSE);
    if (StringUtils.isBlank(whRootString)) {
      throw new MetaException(HiveConf.ConfVars.METASTOREWAREHOUSE.varname
          + " is not set in the config or blank");
    }
    multiHdfsInfo = new MultiHdfsInfo(conf);
  }

  public FileSystem getFs(Path f) throws MetaException {
    try {
      return f.getFileSystem(conf);
    } catch (IOException e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return null;
  }

  public Path getDnsPath(Path path) throws MetaException {
    FileSystem fs = getFs(path);
    return (new Path(fs.getUri().getScheme(), fs.getUri().getAuthority(), path
        .toUri().getPath()));
  }

  private Path getWhRoot() throws MetaException {
    if (whRoot != null) {
      return whRoot;
    }
    whRoot = getDnsPath(new Path(whRootString));
    return whRoot;
  }

  private Path getWhRoot(String dbName) throws MetaException {
    if (!multiHdfsInfo.isMultiHdfsEnable())
      return getWhRoot();

    Path WHRoot = null;
    WHRoot = new Path(multiHdfsInfo.getHdfsPathFromDB(dbName), whRootString);

    return WHRoot;
  }

  public Path getDefaultDatabasePath(String dbName) throws MetaException {
    Path WHRoot = getWhRoot(dbName);

    if (dbName.equalsIgnoreCase(MetaStoreUtils.DEFAULT_DATABASE_NAME)) {
      return new Path(WHRoot, MetaStoreUtils.DEFAULT_DATABASE_NAME);
    }
    return new Path(WHRoot, dbName.toLowerCase() + ".db");
  }

  public Path getDefaultDatabasePath(String dbName, String hdfsScheme)
      throws MetaException {
    if (hdfsScheme == null || hdfsScheme.isEmpty())
      return getDefaultDatabasePath(dbName);
    else {
      if (!multiHdfsInfo.isMultiHdfsEnable())
        return getDefaultDatabasePath(dbName);

      if (!(new Path(hdfsScheme).toUri().getScheme()).equalsIgnoreCase("hdfs")) {
        throw new MetaException("wrong scheme:" + hdfsScheme);
      }

      String hdfsSchemeLocal = MultiHdfsInfo.makeQualified(hdfsScheme.trim());

      return new Path(new Path(hdfsSchemeLocal, whRootString),
          dbName.toLowerCase() + ".db");
    }
  }

  public Path getDefaultTablePath(String dbName, String tableName)
      throws MetaException {
    return new Path(getDefaultDatabasePath(dbName), tableName.toLowerCase());
  }

  public Path getDefaultIndexPath(String dbName, String tableName,
      String indexName) throws MetaException {
    Path prepath = null;
    String preString = "";
    if (dbName.equalsIgnoreCase(MetaStoreUtils.DEFAULT_DATABASE_NAME)) {
      prepath = new Path(getWhRoot(), new String("/index/")
          + MetaStoreUtils.DEFAULT_DATABASE_NAME);
      preString = getWhRoot().toString() + "/index/"
          + MetaStoreUtils.DEFAULT_DATABASE_NAME;
    } else {
      prepath = new Path(getWhRoot(), new String("/index/")
          + dbName.toLowerCase() + ".db");
      preString = getWhRoot().toString() + "/index/" + dbName.toLowerCase()
          + ".db";
    }

    String indexString = preString + "/" + tableName.toLowerCase() + "/"
        + indexName.toLowerCase();
    return new Path(indexString);
  }

  public boolean mkdirs(Path f) throws MetaException {
    try {
      FileSystem fs = getFs(f);
      LOG.debug("Creating directory if it doesn't exist: " + f);
      return (fs.mkdirs(f) || fs.getFileStatus(f).isDir());
    } catch (IOException e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return false;
  }

  public boolean deleteDir(Path f, boolean recursive) throws MetaException {
    LOG.info("deleting  " + f);
    try {
      FileSystem fs = getFs(f);
      if (!fs.exists(f)) {
        return false;
      }

      Configuration dupConf = new Configuration(conf);
      FileSystem.setDefaultUri(dupConf, fs.getUri());

      Trash trashTmp = new Trash(dupConf);
      if (trashTmp.moveToTrash(f)) {
        LOG.info("Moved to trash: " + f);
        return true;
      }
      if (fs.delete(f, true)) {
        LOG.info("Deleted the diretory " + f);
        return true;
      }
      if (fs.exists(f)) {
        throw new MetaException("Unable to delete directory: " + f);
      }
    } catch (FileNotFoundException e) {
      return true;
    } catch (IOException e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return false;
  }
  
  public boolean deleteDirThrowExp(Path f, boolean recursive) throws MetaException {
    LOG.info("deleting  " + f);
    try {
      FileSystem fs = getFs(f);
      if (!fs.exists(f)) {
        return false;
      }

      Configuration dupConf = new Configuration(conf);
      FileSystem.setDefaultUri(dupConf, fs.getUri());

      Trash trashTmp = new Trash(dupConf);
      if (trashTmp.moveToTrash(f)) {
        LOG.info("Moved to trash: " + f);
        return true;
      }
      if (fs.delete(f, true)) {
        LOG.info("Deleted the diretory " + f);
        return true;
      }
      if (fs.exists(f)) {
        throw new MetaException("Unable to delete directory: " + f);
      }
    } catch (FileNotFoundException e) {
      return true;
    } catch (Exception e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return false;
  }

  static BitSet charToEscape = new BitSet(128);
  static {
    for (char c = 0; c < ' '; c++) {
      charToEscape.set(c);
    }
    char[] clist = new char[] { '"', '#', '%', '\'', '*', '/', ':', '=', '?',
        '\\', '\u00FF' };
    for (char c : clist) {
      charToEscape.set(c);
    }
  }

  static boolean needsEscaping(char c) {
    return c >= 0 && c < charToEscape.size() && charToEscape.get(c);
  }

  static String escapePathName(String path) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < path.length(); i++) {
      char c = path.charAt(i);
      if (needsEscaping(c)) {
        sb.append('%');
        sb.append(String.format("%1$02X", (int) c));
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  static String unescapePathName(String path) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < path.length(); i++) {
      char c = path.charAt(i);
      if (c == '%' && i + 2 < path.length()) {
        int code = -1;
        try {
          code = Integer.valueOf(path.substring(i + 1, i + 3), 16);
        } catch (Exception e) {
          code = -1;
        }
        if (code >= 0) {
          sb.append((char) code);
          i += 2;
          continue;
        }
      }
      sb.append(c);
    }
    return sb.toString();
  }

  public static String makePartName(Map<String, String> spec)
      throws MetaException {
    StringBuffer suffixBuf = new StringBuffer();
    for (Entry<String, String> e : spec.entrySet()) {
      if (e.getValue() == null || e.getValue().length() == 0) {
        throw new MetaException("Partition spec is incorrect. " + spec);
      }
      suffixBuf.append(escapePathName(e.getKey()));
      suffixBuf.append('=');
      suffixBuf.append(escapePathName(e.getValue()));
      suffixBuf.append(Path.SEPARATOR);
    }
    return suffixBuf.toString();
  }

  static final Pattern pat = Pattern.compile("([^/]+)=([^/]+)");

  public static LinkedHashMap<String, String> makeSpecFromName(String name)
      throws MetaException {
    LinkedHashMap<String, String> partSpec = new LinkedHashMap<String, String>();
    if (name == null || name.isEmpty()) {
      throw new MetaException("Partition name is invalid. " + name);
    }
    List<String[]> kvs = new ArrayList<String[]>();
    Path currPath = new Path(name);
    do {
      String component = currPath.getName();
      Matcher m = pat.matcher(component);
      if (m.matches()) {
        String k = unescapePathName(m.group(1));
        String v = unescapePathName(m.group(2));

        if (partSpec.containsKey(k)) {
          throw new MetaException("Partition name is invalid. Key " + k
              + " defined at two levels");
        }
        String[] kv = new String[2];
        kv[0] = k;
        kv[1] = v;
        kvs.add(kv);
      } else {
        throw new MetaException("Partition name is invalid. " + name);
      }
      currPath = currPath.getParent();
    } while (currPath != null && !currPath.getName().isEmpty());

    for (int i = kvs.size(); i > 0; i--) {
      partSpec.put(kvs.get(i - 1)[0], kvs.get(i - 1)[1]);
    }
    return partSpec;
  }

  public Path getPartitionPath(String dbName, String tableName, String pn)
      throws MetaException {
    return new Path(getDefaultTablePath(dbName, tableName), makePartName(pn));
  }

  public static Path getPartitionPath(Path tblPath, String pn)
      throws MetaException {
    return new Path(tblPath, makePartName(pn));
  }

  public static Path getPartitionPath(Path tblPath, String priPart,
      String subPart) throws MetaException {

    return new Path(getPartitionPath(tblPath, priPart), subPart);
  }

  public List<Path> getPriPartitionPaths(String dbName, String tbleName,
      String priPart, Set<String> subParts) throws MetaException {

    ArrayList<Path> PriPartPaths = new ArrayList<Path>();
    Path tblPath = getDefaultTablePath(dbName, tbleName);
    Path priPath = getPartitionPath(tblPath, priPart);

    if (subParts == null || subParts.isEmpty()) {
      PriPartPaths.add(priPath);
    } else
      for (String part : subParts) {
        PriPartPaths.add(new Path(priPath, part));
      }

    return PriPartPaths;

  }

  public static List<Path> getPriPartitionPaths(Path tblPath, String priPart,
      Set<String> subParts) throws MetaException {

    ArrayList<Path> PriPartPaths = new ArrayList<Path>();
    Path priPath = getPartitionPath(tblPath, priPart);

    if (subParts == null || subParts.isEmpty()) {
      PriPartPaths.add(priPath);
    } else
      for (String part : subParts) {
        PriPartPaths.add(new Path(priPath, part));
      }

    return PriPartPaths;

  }
  
  public static List<Path> getPriPartitionPaths(Path tblPath, String priPart,
      List<String> subParts) throws MetaException {

    ArrayList<Path> PriPartPaths = new ArrayList<Path>();
    Path priPath = getPartitionPath(tblPath, priPart);

    if (subParts == null || subParts.isEmpty()) {
      PriPartPaths.add(priPath);
    } else
      for (String part : subParts) {
        PriPartPaths.add(new Path(priPath, part));
      }

    return PriPartPaths;

  }

  public List<Path> getSubPartitionPaths(String dbName, String tbleName,
      Set<String> priParts, String subPart) throws MetaException {
    ArrayList<Path> subPartPaths = new ArrayList<Path>();

    assert (priParts.size() >= 1);
    for (String part : priParts) {
      subPartPaths.add(new Path(getPartitionPath(dbName, tbleName, part),
          subPart));
    }
    return subPartPaths;
  }

  public static List<Path> getSubPartitionPaths(Path tblPath,
      Set<String> priParts, String subPart) throws MetaException {
    ArrayList<Path> subPartPaths = new ArrayList<Path>();
    assert (priParts.size() >= 1);
    for (String part : priParts) {
      subPartPaths.add(new Path(getPartitionPath(tblPath, part), subPart));
    }
    return subPartPaths;
  }

  public List<Path> getPartitionPaths(String dbName, String tbleName,
      Set<String> priParts, Set<String> subParts) throws MetaException {

    ArrayList<Path> paths = new ArrayList<Path>();

    for (String path : priParts) {
      paths.addAll(getPriPartitionPaths(dbName, tbleName, path, subParts));
    }
    return paths;

  }

  public static List<Path> getPartitionPaths(Path tblPath,
      Set<String> priParts, Set<String> subParts) throws MetaException {

    ArrayList<Path> paths = new ArrayList<Path>();

    for (String path : priParts) {
      paths.addAll(getPriPartitionPaths(tblPath, path, subParts));
    }
    return paths;

  }
  
  public static List<Path> getPartitionPaths(Path tblPath,
      List<String> priParts, List<String> subParts) throws MetaException {

    ArrayList<Path> paths = new ArrayList<Path>();

    for (String path : priParts) {
      paths.addAll(getPriPartitionPaths(tblPath, path, subParts));
    }
    return paths;

  }

  public boolean isDir(Path f) throws MetaException {
    try {
      FileSystem fs = getFs(f);
      FileStatus fstatus = fs.getFileStatus(f);
      if (!fstatus.isDir()) {
        return false;
      }
    } catch (FileNotFoundException e) {
      return false;
    } catch (IOException e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return true;
  }

  public static String makePartName(String partName) {
    return partName;
  }

  public static String makePartName(FieldSchema parKey, String parType,
      int level) throws MetaException {
    StringBuilder name = new StringBuilder();
    name.append(escapePathName(parKey.getName().toLowerCase()));
    name.append('-').append(level).append('-').append(parType);
    return name.toString();
  }

}
