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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.File;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.JDBCStore.Privilege;
import org.apache.hadoop.hive.metastore.api.AddSerdeDesc;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DbPriv;
import org.apache.hadoop.hive.metastore.api.DropPartitionDesc;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.IndexItem;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.RenameColDesc;
import org.apache.hadoop.hive.metastore.api.tdw_query_info;
import org.apache.hadoop.hive.metastore.api.tdw_query_stat;
import org.apache.hadoop.hive.metastore.api.tdw_sys_fields_statistics;
import org.apache.hadoop.hive.metastore.api.tdw_sys_table_statistics;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.TblPriv;
import org.apache.hadoop.hive.metastore.api.User;
import org.apache.hadoop.hive.metastore.api.group;
import org.apache.hadoop.hive.metastore.model.Mtdw_query_info;
import org.apache.hadoop.hive.metastore.model.Mtdw_query_stat;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.ql.PBJarTool;
import org.apache.hadoop.hive.ql.dataToDB.StoreAsPgdata;
import org.apache.hadoop.hive.ql.exec.ExecDriver;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.parse.PartitionDesc;
import org.apache.hadoop.hive.ql.parse.PartitionType;
import org.apache.hadoop.hive.ql.parse.QB.PartRefType;
import org.apache.hadoop.hive.ql.plan.AddPartitionDesc;
import org.apache.hadoop.hive.ql.plan.dropTableDesc;
import org.apache.hadoop.hive.ql.plan.partitionDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import FormatStorage1.IFormatDataFile;

public class Hive {
  private LogHelper console;
  static final private Log LOG = LogFactory.getLog("hive.ql.metadata.Hive");

  static Hive db = null;

  public enum Privilege {
    SELECT_PRIV, INSERT_PRIV, INDEX_PRIV, CREATE_PRIV, DROP_PRIV, DELETE_PRIV, ALTER_PRIV, UPDATE_PRIV, CREATE_VIEW_PRIV, SHOW_VIEW_PRIV, ALL_PRIV, DBA_PRIV
  }

  private static Map<Privilege, Integer> privMap = new ConcurrentHashMap<Privilege, Integer>();
  static {
    privMap.put(Privilege.SELECT_PRIV, 1);
    privMap.put(Privilege.INSERT_PRIV, 2);
    privMap.put(Privilege.INDEX_PRIV, 3);
    privMap.put(Privilege.CREATE_PRIV, 4);
    privMap.put(Privilege.DROP_PRIV, 5);
    privMap.put(Privilege.DELETE_PRIV, 6);
    privMap.put(Privilege.ALTER_PRIV, 7);
    privMap.put(Privilege.UPDATE_PRIV, 8);
    privMap.put(Privilege.CREATE_VIEW_PRIV, 9);
    privMap.put(Privilege.SHOW_VIEW_PRIV, 10);
    privMap.put(Privilege.ALL_PRIV, 11);
    privMap.put(Privilege.DBA_PRIV, 12);
  }

  private HiveConf conf = null;
  private int numOfHashPar;

  private ThreadLocal<IMetaStoreClient> threadLocalMSC = new ThreadLocal() {
    protected synchronized Object initialValue() {
      return null;
    }

    public synchronized void remove() {
      if (this.get() != null) {
        ((IMetaStoreClient) this.get()).close();
      }
      super.remove();
    }
  };

  public static Hive get(HiveConf c) throws HiveException {
    boolean needsRefresh = false;

    if (db != null) {
      for (HiveConf.ConfVars oneVar : HiveConf.metaVars) {
        String oldVar = db.getConf().getVar(oneVar);
        String newVar = c.getVar(oneVar);
        if (oldVar == null || oldVar.compareToIgnoreCase(newVar) != 0) {
          needsRefresh = true;
          break;
        }
      }
    }
    return get(c, needsRefresh);
  }

  public static Hive get(HiveConf c, boolean needsRefresh) throws HiveException {
    if (db == null || needsRefresh) {
      closeCurrent();
      c.set("fs.scheme.class", "dfs");
      db = new Hive(c);
    }

    return db;
  }

  public static Hive get() throws HiveException {
    if (db == null) {
      db = new Hive(new HiveConf(Hive.class));
    }
    return db;
  }

  public static void closeCurrent() {
    if (db != null) {
      db.close();
    }
  }

  private Hive(HiveConf c) throws HiveException {
    this.conf = c;
    numOfHashPar = this.conf.getInt("hive.hashPartition.num", 500);
    if (numOfHashPar <= 0) {
      throw new HiveException(
          "Hash Partition Number should be Positive Integer!");
    }
    console = new LogHelper(LOG, c);
  }

  private void close() {
    LOG.info("Closing current thread's connection to Hive Metastore.");
    db.threadLocalMSC.remove();
  }

  public void createTable(String tableName, List<String> columns,
      PartitionDesc partdesc, Class<? extends InputFormat> fileInputFormat,
      Class<?> fileOutputFormat) throws HiveException {
    this.createTable(tableName, columns, partdesc, fileInputFormat,
        fileOutputFormat, -1, null);
  }

  public void createTable(String tableName, List<String> columns,
      PartitionDesc partdesc, Class<? extends InputFormat> fileInputFormat,
      Class<?> fileOutputFormat, int bucketCount, List<String> bucketCols)
      throws HiveException {
    if (columns == null) {
      throw new HiveException("columns not specified for table " + tableName);
    }

    Table tbl = new Table(tableName);
    tbl.setInputFormatClass(fileInputFormat.getName());
    tbl.setOutputFormatClass(fileOutputFormat.getName());

    for (String col : columns) {
      FieldSchema field = new FieldSchema(col,
          org.apache.hadoop.hive.serde.Constants.STRING_TYPE_NAME, "default");
      tbl.getCols().add(field);
    }

    if (partdesc != null) {

      tbl.setPartitions(partdesc);
    }
    tbl.setSerializationLib(LazySimpleSerDe.class.getName());
    tbl.setNumBuckets(bucketCount);
    tbl.setBucketCols(bucketCols);
    createTable(tbl);
  }

  public void alterTable(String tblName, Table newTbl)
      throws InvalidOperationException, HiveException {
    try {

      getMSC().alter_table(SessionState.get().getDbName(), tblName,
          newTbl.getTTable());
    } catch (MetaException e) {
      throw new HiveException("Unable to alter table.", e);
    } catch (TException e) {
      throw new HiveException("Unable to alter table.", e);
    }
  }

  public void alterTable(String dbName, String tblName, Table newTbl)
      throws InvalidOperationException, HiveException {
    try {

      getMSC().alter_table(dbName, tblName, newTbl.getTTable());
    } catch (MetaException e) {
      throw new HiveException("Unable to alter table.", e);
    } catch (TException e) {
      throw new HiveException("Unable to alter table.", e);
    }
  }

  public void createTable(Table tbl) throws HiveException {
    createTable(tbl, false);
  }

  public void createTable(Table tbl, boolean ifNotExists) throws HiveException {
    try {
      tbl.initSerDe();
      if (tbl.getCols().size() == 0) {
        tbl.setFields(MetaStoreUtils.getFieldsFromDeserializer(tbl.getName(),
            tbl.getDeserializer()));
      }
      tbl.checkValidity();
      
      if(tbl.getTableType() == TableType.EXTERNAL_TABLE){
        HiveConf sessionConf = SessionState.get().getConf();
        if(sessionConf != null){
          boolean createExtDirIfNotExist = sessionConf.getBoolean(HiveConf.ConfVars.HIVE_CREATE_EXTTABLE_DIR_IFNOTEXIST.varname, 
              HiveConf.ConfVars.HIVE_CREATE_EXTTABLE_DIR_IFNOTEXIST.defaultBoolVal);
          LOG.info("XXcreateExtDirIfNotExist=" + createExtDirIfNotExist);
          if(!createExtDirIfNotExist){
            Map<String, String> tblParams = tbl.getTTable().getParameters();
            if(tblParams == null){
              tblParams = new HashMap<String, String>();
              tblParams.put(HiveConf.ConfVars.HIVE_CREATE_EXTTABLE_DIR_IFNOTEXIST.varname, "false");
              tbl.getTTable().setParameters(tblParams);
            }
            else{
              tblParams.put(HiveConf.ConfVars.HIVE_CREATE_EXTTABLE_DIR_IFNOTEXIST.varname, "false");
            }
          } 
        }

        // check whether current user has auth to create external table on the location
        String location = tbl.getTTable().getSd().getLocation();
        boolean isAuthLocation = sessionConf.getBoolean(HiveConf.ConfVars.HIVE_CREATE_EXTTABLE_LOCATION_AUTH.varname, 
            HiveConf.ConfVars.HIVE_CREATE_EXTTABLE_LOCATION_AUTH.defaultBoolVal);
        if (isAuthLocation && location != null && location.trim().startsWith("hdfs://")) {
          if (!getMSC().hasAuthOnLocation(SessionState.get().getUserName(), location.trim())) {
            throw new HiveException("user " + SessionState.get().getUserName() + 
                " has no privilege to create external table on location: " + location);
          }
        }
      }
      
      getMSC().createTable(tbl.getTTable());
    } catch (AlreadyExistsException e) {
      if (!ifNotExists) {
        throw new HiveException(e);
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void dropTable(String dbName, String tableName) throws HiveException {
    dropTable(dbName, tableName, true, true);
  }

  public void dropTable(String dbName, String tableName, boolean deleteData,
      boolean ignoreUnknownTab) throws HiveException {

    try {
      getMSC().dropTable(dbName, tableName, deleteData, ignoreUnknownTab);
    } catch (NoSuchObjectException e) {
      if (!ignoreUnknownTab) {
        throw new HiveException(e);
      }
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public void truncateTable(String dbName, String tblName)
      throws HiveException, MetaException {

    Table tbl = null;
    try {
      tbl = getTable(dbName, tblName);
    } catch (Exception x) {
      return;
    }

    Path tblPath = tbl.getPath();

    if (tbl.getTableStorgeType().equalsIgnoreCase("pgdata")) {
      LOG.info("truncate table stored as pgdata");
      StoreAsPgdata sto = new StoreAsPgdata();
      LOG.info("server: " + tbl.getSerdeParam(Constants.TABLE_SERVER));
      sto.sendTruncateTableToPg(tbl);
    }

    getMSC().getWarehouse().deleteDir(tblPath, true);
    getMSC().getWarehouse().mkdirs(tblPath);

    List<Path> partPathList = null;
    List<String> partNameList = null;
    if (tbl.isPartitioned()) {
      partPathList = new ArrayList<Path>();
      partNameList = new ArrayList<String>();
      partPathList.addAll(Warehouse.getPartitionPaths(tbl.getPath(), tbl
          .getTTable().getPriPartition().getParSpaces().keySet(), tbl
          .getTTable().getSubPartition() != null ? tbl.getTTable()
          .getSubPartition().getParSpaces().keySet() : null));
      for (Path path : partPathList) {
        getMSC().getWarehouse().mkdirs(path);
        LOG.debug("mkdir : " + path.toString());
      }
      LOG.debug("delete and create dir OK!");
      Iterator<String> iter = tbl.getTTable().getPriPartition().getParSpaces()
          .keySet().iterator();
      while (iter.hasNext()) {
        partNameList.add(iter.next());
        LOG.debug("add pri partition names : "
            + partNameList.get(partNameList.size() - 1));
      }
    }

    try {
      List<IndexItem> indexItemList = getMSC().get_all_index_table(dbName,
          tblName);
      if (indexItemList == null || indexItemList.isEmpty()) {
        return;
      }

      for (int i = 0; i < indexItemList.size(); i++) {
        Path indexPath = new Path(indexItemList.get(i).getLocation());
        getMSC().getWarehouse().deleteDir(indexPath, true);
        getMSC().getWarehouse().mkdirs(indexPath);

        if (tbl.isPartitioned()) {
          Set<String> partNameSet = tbl.getTTable().getPriPartition()
              .getParSpaces().keySet();
          List<Path> indexPartPath = getMSC().getWarehouse().getPartitionPaths(
              indexPath, partNameSet, null);
          for (Path partPath : indexPartPath) {
            getMSC().getWarehouse().mkdirs(partPath);
          }
        }
      }
    } catch (Exception e) {
      LOG.info("drop index fail:" + e.getMessage() + ",db:" + dbName
          + ",table:" + tblName + " when trunk table");
      if (SessionState.get() != null) {
        SessionState.get().ssLog(
            "drop index fail:" + e.getMessage() + ",db:" + dbName + ",table:"
                + tblName + " when trunk table");
      }
    }
  }

  public void deleteTable(String deletepath, String tmpdir)
      throws HiveException, MetaException {
    Path delPath = new Path(deletepath);

    try {
      getMSC().getWarehouse().deleteDir(delPath, true);
    } catch (MetaException e) {
      throw new MetaException(
          "Cannot execute delete Operation, may be the table is being used");
    }

    try {
      Path tmpPath = new Path(tmpdir);
      FileSystem fs = tmpPath.getFileSystem(conf);

      if (fs.exists(tmpPath)) {
        if (!fs.rename(tmpPath, delPath))
          throw new HiveException("Unable to rename: " + tmpPath + " to: "
              + delPath);
      } else {
        if (!fs.mkdirs(delPath))
          throw new HiveException("Unable to make directory: " + delPath);
      }
    } catch (Exception e) {
      console.printError("Failed with exception " + e.getMessage(), "\n"
          + StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "Failed with exception " + e.getMessage() + "\n"
                + StringUtils.stringifyException(e));
    }
  }

  public void updateTable(String deletepath, String tmpdir_new,
      String tmpdir_old) throws HiveException, MetaException {
    Path delPath = new Path(deletepath);

    try {
      getMSC().getWarehouse().deleteDir(delPath, true);
    } catch (MetaException e) {
      throw new MetaException(
          "Cannot execute delete Operation, may be the table is being used");
    }

    try {
      Path tmpPath = new Path(tmpdir_new);
      FileSystem fs = tmpPath.getFileSystem(conf);

      if (fs.exists(tmpPath)) {
        if (!fs.rename(tmpPath, delPath))
          throw new HiveException("Unable to rename: " + tmpPath + " to: "
              + delPath);
      } else {
        if (!fs.mkdirs(delPath))
          throw new HiveException("Unable to make directory: " + delPath);
      }
    } catch (Exception e) {
      console.printError("Failed with exception " + e.getMessage(), "\n"
          + StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "Failed with exception " + e.getMessage() + "\n"
                + StringUtils.stringifyException(e));
    }
  }

  public HiveConf getConf() {
    return (conf);
  }

  public Table getTable(final String dbName, final String tableName)
      throws HiveException {

    return this.getTable(dbName, tableName, true);
  }

  public Table cloneTable(org.apache.hadoop.hive.metastore.api.Table oldTTable)
      throws HiveException {
    if (oldTTable == null) {
      throw new HiveException("empty table creation??");
    }

    org.apache.hadoop.hive.metastore.api.Table tTable = oldTTable.deepCopy();
    Table table = new Table();

    assert (tTable != null);

    if(PBJarTool.checkIsPbTable(tTable)){
      String dbName = tTable.getDbName();
      String tableName = tTable.getTableName();
      LOG.debug("dbName: " + dbName);
      LOG.debug("tableName: " + tableName);
      
      String modifyTime = PBJarTool.downloadjar(dbName.toLowerCase(),tableName.toLowerCase(), conf);
      String pbOuterClassName = dbName.toLowerCase() + "_" + tableName.toLowerCase() + "_" + modifyTime;     
      String jarPath = "./auxlib/" + pbOuterClassName + ".jar";
      
      LOG.info("has jarPath,adding..");
      ClassLoader loader = JavaUtils.getpbClassLoader();
      URL[] urls = ((URLClassLoader) loader).getURLs();
      Set<URL> newPath = new HashSet<URL>(Arrays.asList(urls));
      try{
        URL aurl = (new File(jarPath)).toURL();
        if (!newPath.contains(aurl)) {
          String[] jarstrs = new String[1];
          jarstrs[0] = jarPath;
          LOG.debug("has jarPath,add begin " + jarPath);
          loader = Utilities.addToClassPath(loader, jarstrs);
          JavaUtils.setpbClassLoader(loader);
          LOG.debug("has jarPath,add OK");
        } else {
          LOG.info("already has this jar,skip add OK");
        }
        
        tTable.getParameters().put(Constants.PB_OUTER_CLASS_NAME, pbOuterClassName);
        tTable.getParameters().put(Constants.PB_JAR_PATH, jarPath);
      }
      catch(Exception x){
        throw new HiveException(x.getMessage());
      }

    }

    try {
      if (org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe.class
          .getName()
          .equals(tTable.getSd().getSerdeInfo().getSerializationLib())
          && tTable.getSd().getColsSize() > 0
          && tTable.getSd().getCols().get(0).getType().indexOf('<') == -1) {
        tTable
            .getSd()
            .getSerdeInfo()
            .setSerializationLib(
                org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class
                    .getName());
      }

      Properties p = MetaStoreUtils.getSchema(tTable);
      table.setSchema(p);
      table.setColComments(MetaStoreUtils.getColComments(tTable));
      table.setTTable(tTable);

      if (table.isView()) {
        table.checkValidity();
        return table;
      }

      table
          .setInputFormatClass((Class<? extends InputFormat<WritableComparable, Writable>>) Class
              .forName(
                  table
                      .getSchema()
                      .getProperty(
                          org.apache.hadoop.hive.metastore.api.Constants.FILE_INPUT_FORMAT,
                          org.apache.hadoop.mapred.SequenceFileInputFormat.class
                              .getName()), true, JavaUtils.getClassLoader()));

      table
          .setOutputFormatClass((Class<? extends HiveOutputFormat>) Class
              .forName(
                  table
                      .getSchema()
                      .getProperty(
                          org.apache.hadoop.hive.metastore.api.Constants.FILE_OUTPUT_FORMAT,
                          HiveSequenceFileOutputFormat.class.getName()), true,
                  JavaUtils.getClassLoader()));

      table.setDeserializer(MetaStoreUtils.getDeserializer(getConf(), p));
      table.setDataLocation(new URI(tTable.getSd().getLocation()));
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }

    String sf = table
        .getSerdeParam(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT);
    if (sf != null) {
      char[] b = sf.toCharArray();
      if ((b.length == 1) && (b[0] < 10)) {
        table.setSerdeParam(
            org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT,
            Integer.toString(b[0]));

      }
    }

    table.checkValidity();
    if (tTable.getSubPartition() != null) {
      table.setHasSubPartition(true);
    }

    return table;
  }

  public Table getTable(final String dbName, final String tableName,
      boolean throwException) throws HiveException {

    if (tableName == null || tableName.equals("")) {
      throw new HiveException("empty table creation??");
    }

    LOG.debug("getTable: DBname: " + dbName + " ,TBname: " + tableName);
    if (SessionState.get() != null)
      SessionState.get().ssLog(
          "getTable: DBname: " + dbName + " ,TBname: " + tableName);

    Table table = new Table();
    org.apache.hadoop.hive.metastore.api.Table tTable = null;
    try {
      tTable = getMSC().getTable(dbName, tableName);
      org.apache.hadoop.hive.metastore.api.Partition priPart = tTable
          .getPriPartition();
      org.apache.hadoop.hive.metastore.api.Partition subPart = tTable
          .getSubPartition();

      if (priPart != null && priPart.getParType().equalsIgnoreCase("range")) {

        PrimitiveTypeInfo pti = new PrimitiveTypeInfo();
        pti.setTypeName(priPart.getParKey().getType());
        ObjectInspector StringIO = PrimitiveObjectInspectorFactory
            .getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING);
        ObjectInspector ValueIO = PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector(pti.getPrimitiveCategory());
        ObjectInspectorConverters.Converter converter1 = ObjectInspectorConverters
            .getConverter(StringIO, ValueIO);
        ObjectInspectorConverters.Converter converter2 = ObjectInspectorConverters
            .getConverter(StringIO, ValueIO);

        PartitionComparator comp = new PartitionComparator(converter1,
            converter2);

        LinkedHashMap<String, List<String>> priPartSpace = (LinkedHashMap<String, List<String>>) priPart
            .getParSpaces();

        boolean containDefault = priPartSpace.containsKey("default");
        if (containDefault) {
          priPartSpace.remove("default");
        }

        List<RangePartitionItem> ragPartList = new ArrayList<RangePartitionItem>();

        for (Entry<String, List<String>> entry : priPartSpace.entrySet()) {
          RangePartitionItem item = new RangePartitionItem();
          item.name = entry.getKey();
          item.value = entry.getValue().get(0);
          ragPartList.add(item);
        }

        Collections.sort(ragPartList, comp);

        LinkedHashMap<String, List<String>> newPriPartSpace = new LinkedHashMap<String, List<String>>();

        int size = ragPartList.size();
        for (int i = 0; i < size; i++) {
          newPriPartSpace.put(ragPartList.get(i).name,
              priPartSpace.get(ragPartList.get(i).name));
        }

        if (containDefault) {
          newPriPartSpace.put("default", new ArrayList<String>());
        }

        priPart.setParSpaces(newPriPartSpace);
      }

      if (subPart != null && subPart.getParType().equalsIgnoreCase("range")) {

        PrimitiveTypeInfo pti = new PrimitiveTypeInfo();
        pti.setTypeName(subPart.getParKey().getType());
        ObjectInspector StringIO = PrimitiveObjectInspectorFactory
            .getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING);
        ObjectInspector ValueIO = PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector(pti.getPrimitiveCategory());
        ObjectInspectorConverters.Converter converter1 = ObjectInspectorConverters
            .getConverter(StringIO, ValueIO);
        ObjectInspectorConverters.Converter converter2 = ObjectInspectorConverters
            .getConverter(StringIO, ValueIO);

        PartitionComparator comp = new PartitionComparator(converter1,
            converter2);

        LinkedHashMap<String, List<String>> subPartSpace = (LinkedHashMap<String, List<String>>) subPart
            .getParSpaces();

        boolean containDefault = subPartSpace.containsKey("default");
        if (containDefault) {
          subPartSpace.remove("default");
        }

        List<RangePartitionItem> ragPartList = new ArrayList<RangePartitionItem>();

        for (Entry<String, List<String>> entry : subPartSpace.entrySet()) {
          RangePartitionItem item = new RangePartitionItem();
          item.name = entry.getKey();
          item.value = entry.getValue().get(0);
          ragPartList.add(item);
        }

        Collections.sort(ragPartList, comp);

        LinkedHashMap<String, List<String>> newSubPartSpace = new LinkedHashMap<String, List<String>>();

        int size = ragPartList.size();
        for (int i = 0; i < size; i++) {
          newSubPartSpace.put(ragPartList.get(i).name,
              subPartSpace.get(ragPartList.get(i).name));
        }

        if (containDefault) {
          newSubPartSpace.put("default", new ArrayList<String>());
        }

        subPart.setParSpaces(newSubPartSpace);
      }
    } catch (NoSuchObjectException e) {
      if (throwException) {
        LOG.error(StringUtils.stringifyException(e));
        throw new InvalidTableException("Table not found ", tableName);
      }
      return null;
    } catch (Exception e) {
      throw new HiveException("Unable to fetch table " + tableName, e);
    }
    assert (tTable != null);

    if(PBJarTool.checkIsPbTable(tTable)){
      LOG.debug("dbName: " + dbName);
      LOG.debug("tableName: " + tableName);
      
      String modifyTime = PBJarTool.downloadjar(dbName.toLowerCase(),tableName.toLowerCase(), conf);
      String pbOuterClassName = dbName.toLowerCase() + "_" + tableName.toLowerCase() + "_" + modifyTime;     
      String jarPath = "./auxlib/" + pbOuterClassName + ".jar";
      
      LOG.info("has jarPath,adding..");
      ClassLoader loader = JavaUtils.getpbClassLoader();
      URL[] urls = ((URLClassLoader) loader).getURLs();
      Set<URL> newPath = new HashSet<URL>(Arrays.asList(urls));
      
      try
      {
        URL aurl = (new File(jarPath)).toURL();
        if (!newPath.contains(aurl)) {
          String[] jarstrs = new String[1];
          jarstrs[0] = jarPath;
          LOG.debug("has jarPath,add begin " + jarPath);
          loader = Utilities.addToClassPath(loader, jarstrs);
          JavaUtils.setpbClassLoader(loader);
          LOG.debug("has jarPath,add OK");
        } else {
          LOG.info("already has this jar,skip add OK");
        }
        
        tTable.getParameters().put(Constants.PB_OUTER_CLASS_NAME, pbOuterClassName);
        tTable.getParameters().put(Constants.PB_JAR_PATH, jarPath);
      }
      catch(Exception x){
        throw new HiveException(x.getMessage());
      }

    }

    try {

      if (org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe.class
          .getName()
          .equals(tTable.getSd().getSerdeInfo().getSerializationLib())
          && tTable.getSd().getColsSize() > 0
          && tTable.getSd().getCols().get(0).getType().indexOf('<') == -1) {
        tTable
            .getSd()
            .getSerdeInfo()
            .setSerializationLib(
                org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe.class
                    .getName());
      }

      Properties p = MetaStoreUtils.getSchema(tTable);
      table.setSchema(p);
      table.setColComments(MetaStoreUtils.getColComments(tTable));
      table.setTTable(tTable);

      if (table.isView()) {
        table.checkValidity();
        return table;
      }

      table
          .setInputFormatClass((Class<? extends InputFormat<WritableComparable, Writable>>) Class
              .forName(
                  table
                      .getSchema()
                      .getProperty(
                          org.apache.hadoop.hive.metastore.api.Constants.FILE_INPUT_FORMAT,
                          org.apache.hadoop.mapred.SequenceFileInputFormat.class
                              .getName()), true, JavaUtils.getClassLoader()));
      table
          .setOutputFormatClass((Class<? extends HiveOutputFormat>) Class
              .forName(
                  table
                      .getSchema()
                      .getProperty(
                          org.apache.hadoop.hive.metastore.api.Constants.FILE_OUTPUT_FORMAT,
                          HiveSequenceFileOutputFormat.class.getName()), true,
                  JavaUtils.getClassLoader()));
      table.setDeserializer(MetaStoreUtils.getDeserializer(getConf(), p));
      table.setDataLocation(new URI(tTable.getSd().getLocation()));
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    String sf = table
        .getSerdeParam(org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT);
    if (sf != null) {
      char[] b = sf.toCharArray();
      if ((b.length == 1) && (b[0] < 10)) {
        table.setSerdeParam(
            org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT,
            Integer.toString(b[0]));
      }
    }
    table.checkValidity();

    if (tTable.getSubPartition() != null)
      table.setHasSubPartition(true);

    return table;
  }

  public List<String> getAllTables() throws HiveException {
    return getTablesByPattern(".*");
  }

  public List<String> getTablesByPattern(String tablePattern)
      throws HiveException {
    try {
      return getMSC().getTables(SessionState.get().getDbName(), tablePattern);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  protected List<String> getTablesForDb(String database, String tablePattern)
      throws HiveException {
    try {
      return getMSC().getTables(database, tablePattern);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public boolean createDatabase(String name, String locationUri, String tmp)
      throws AlreadyExistsException, MetaException, TException {
    HiveConf sessionConf = SessionState.get().getConf();
    String role = null;
    if (sessionConf == null) {
      role = null;
    } else {
      role = sessionConf.get("tdw.ugi.groupname");
    }
    if (role == null) {
      return getMSC().createDatabase(name, locationUri, null);
    } else {
      Database db = new Database();
      db.setName(name);
      db.setDescription(locationUri);
      db.setHdfsscheme(null);
      db.setMetastore(null);
      db.setOwner(role);
      return getMSC().createDatabase(db);
    }
  }

  public boolean createDatabase(String name, String hdfsscheme)
      throws AlreadyExistsException, MetaException, TException {
    Warehouse wh = new Warehouse(conf);

    HiveConf sessionConf = SessionState.get().getConf();
    String role = null;
    if (sessionConf == null) {
      role = null;
    } else {
      role = sessionConf.get("tdw.ugi.groupname");
    }

    if (role == null) {
      return getMSC().createDatabase(name,
          wh.getDefaultDatabasePath(name).toString(), hdfsscheme);
    } else {
      Database db = new Database();
      db.setName(name);
      db.setDescription(wh.getDefaultDatabasePath(name).toString());
      db.setHdfsscheme(hdfsscheme);
      db.setMetastore(null);
      db.setOwner(role);
      return getMSC().createDatabase(db);
    }

  }

  public boolean createDatabase(Database db) throws AlreadyExistsException,
      MetaException, TException {
    Warehouse wh = new Warehouse(conf);

    HiveConf sessionConf = SessionState.get().getConf();
    String role = null;
    if (sessionConf == null) {
      role = null;
    } else {
      role = sessionConf.get("tdw.ugi.groupname");
    }

    if (role == null) {
      return getMSC().createDatabase(db.getName(),
          wh.getDefaultDatabasePath(db.getName()).toString(),
          db.getHdfsscheme(), db.getMetastore());
    } else {
      db.setOwner(role);
      db.setDescription(wh.getDefaultDatabasePath(db.getName()).toString());
      return getMSC().createDatabase(db);
    }

  }

  public boolean dropDatabase(String name) throws MetaException, TException {
    try {
      return getMSC().dropDatabase(name);
    } catch (NoSuchObjectException e) {
      e.printStackTrace();
      return false;
    }
  }

  public Database getDatabase(String name) throws Exception {
    return getMSC().getDatabase(name);
  }

  public void loadTable(Path loadPath, String tableName, boolean replace,
      Path tmpDirPath) throws HiveException {
    Table tbl = getTable(SessionState.get().getDbName(), tableName);
    if (replace) {
      tbl.replaceFiles(loadPath, tmpDirPath);
    } else {
      tbl.copyFiles(loadPath);
    }
  }

  public void loadTable(Path loadPath, String dbname, String tableName,
      boolean replace, Path tmpDirPath) throws HiveException {
    if (dbname == null) {
      dbname = SessionState.get().getDbName();
    }

    Table tbl = getTable(dbname, tableName);
    if (replace) {
      tbl.replaceFiles(loadPath, tmpDirPath);
    } else {
      tbl.copyFiles(loadPath);
    }
  }

  public void loadTable(Path loadPath, Table tbl, boolean replace,
      Path tmpDirPath) throws HiveException {
    if (replace) {
      tbl.replaceFiles(loadPath, tmpDirPath);
    } else {
      tbl.copyFiles(loadPath);
    }
  }

  public void loadPartition(Path loadPath, Table tbl, PartRefType prt,
      String partName, String subPartName, boolean replace, Path tmpDirPath)
      throws HiveException {
    if (replace) {
      tbl.replaceFiles(loadPath, prt, partName, subPartName, tmpDirPath);

    } else {
      tbl.copyFiles(loadPath);
    }
  }

  public void loadPartition(Path loadPath, String dbName, String tableName,
      PartRefType prt, String partName, String subPartName, boolean replace,
      Path tmpDirPath) throws HiveException {
    if (dbName == null) {
      dbName = SessionState.get().getDbName();
    }
    Table tbl = getTable(dbName, tableName);

    if (replace) {
      tbl.replaceFiles(loadPath, prt, partName, subPartName, tmpDirPath);

    } else {
      tbl.copyFiles(loadPath);
    }
  }

  public void addPartitions(Table tbl, AddPartitionDesc apd)
      throws HiveException, MetaException, TException,
      InvalidOperationException {
    org.apache.hadoop.hive.metastore.api.AddPartitionDesc tAddPartDesc = new org.apache.hadoop.hive.metastore.api.AddPartitionDesc();
    tAddPartDesc.setDbName(apd.getDbName());
    tAddPartDesc.setTableName(apd.getTableName());
    tAddPartDesc.setLevel(apd.getIsSubPartition() ? 1 : 0);
    tAddPartDesc.setPartType(apd.getPartDesc().getPartitionType().name());
    tAddPartDesc.setParSpaces(apd.getPartDesc().getPartitionSpaces());

    getMSC().addPartition(tbl.getDbName(), tbl.getName(), tAddPartDesc);
  }

  public void addPartitions(String db, String tbl, AddPartitionDesc apd)
      throws HiveException, InvalidOperationException, MetaException,
      TException {
    org.apache.hadoop.hive.metastore.api.AddPartitionDesc tAddPartDesc = new org.apache.hadoop.hive.metastore.api.AddPartitionDesc();

    tAddPartDesc.setDbName(apd.getDbName());
    tAddPartDesc.setTableName(apd.getTableName());
    tAddPartDesc.setLevel(apd.getIsSubPartition() ? 1 : 0);
    tAddPartDesc.setPartType(apd.getPartDesc().getPartitionType().name());
    tAddPartDesc.setParSpaces(apd.getPartDesc().getPartitionSpaces());

    getMSC().addPartition(db, tbl, tAddPartDesc);
  }

  public void addPartitions(AddPartitionDesc apd) throws HiveException,
      InvalidOperationException, MetaException, TException {
    org.apache.hadoop.hive.metastore.api.AddPartitionDesc tAddPartDesc = new org.apache.hadoop.hive.metastore.api.AddPartitionDesc();
    tAddPartDesc.setDbName(apd.getDbName());
    tAddPartDesc.setTableName(apd.getTableName());
    tAddPartDesc.setLevel(apd.getIsSubPartition() ? 1 : 0);
    tAddPartDesc.setPartType(apd.getPartDesc().getPartitionType().name());
    tAddPartDesc.setParSpaces(apd.getPartDesc().getPartitionSpaces());

    getMSC().addPartition(tAddPartDesc.getDbName(),
        tAddPartDesc.getTableName(), tAddPartDesc);
  }

  public void addDefalutSubPartition(Table tbl) throws HiveException,
      MetaException, TException, InvalidOperationException {
    getMSC().addDefaultPartition(tbl.getDbName(), tbl.getName(), 1);
  }

  public void addDefalutSubPartition(String db, String tbl, int level)
      throws HiveException, MetaException, TException,
      InvalidOperationException {
    getMSC().addDefaultPartition(db, tbl, 1);
  }

  public void addDefalutPriPartition(Table tbl) throws HiveException,
      MetaException, TException, InvalidOperationException {
    getMSC().addDefaultPartition(tbl.getDbName(), tbl.getName(), 0);
  }

  public void addDefalutPriPartition(String db, String tbl, int level)
      throws HiveException, MetaException, TException,
      InvalidOperationException {
    getMSC().addDefaultPartition(db, tbl, level);
  }

  public void truncatePartition(Table tbl, String pri, String sub)
      throws HiveException, MetaException, TException,
      InvalidOperationException {
    List<Path> partPaths;
    if (pri != null && sub != null) {
      getMSC().getWarehouse().deleteDir(
          Warehouse.getPartitionPath(tbl.getPath(), pri, sub), true);

      getMSC().getWarehouse().mkdirs(
          Warehouse.getPartitionPath(tbl.getPath(), pri, sub));

    } else if (pri != null) {
      Set<String> subPartNames = null;
      if (tbl.getTTable().getSubPartition() != null) {
        if (tbl.getTTable().getSubPartition().getParType().equals("hash")) {
          subPartNames = new TreeSet<String>();
          for (int i = 0; i < numOfHashPar; i++) {
            if (i < 10)
              subPartNames.add("Hash_" + "000" + i);
            else if (i < 100)
              subPartNames.add("Hash_" + "00" + i);
            else if (i < 1000)
              subPartNames.add("Hash_" + "0" + i);
            else
              subPartNames.add("Hash_" + i);
          }
        } else {
          subPartNames = tbl.getTTable().getSubPartition().getParSpaces()
              .keySet();
        }
      }

      partPaths = Warehouse.getPriPartitionPaths(tbl.getPath(), pri,
          subPartNames);

      for (Path p : partPaths) {
        getMSC().getWarehouse().deleteDir(p, true);
      }

      for (Path p : partPaths) {
        getMSC().getWarehouse().mkdirs(p);
      }

      List<Path> indexPath2Delete = new ArrayList<Path>();
      List<IndexItem> indexItemList = getMSC().get_all_index_table(
          tbl.getDbName(), tbl.getName());
      if (indexItemList != null) {
        for (int i = 0; i < indexItemList.size(); i++) {
          String indexName = indexItemList.get(i).getName();
          String indexPathName = getMSC().getWarehouse()
              .getDefaultIndexPath(tbl.getDbName(), tbl.getName(), indexName)
              .toString();

          indexPath2Delete.add(new Path(indexPathName, pri));
        }
      }

      for (Path path : indexPath2Delete) {
        getMSC().getWarehouse().deleteDir(path, true);
      }
      for (Path path : indexPath2Delete) {
        getMSC().getWarehouse().mkdirs(path);
      }
    } else {

      partPaths = Warehouse.getSubPartitionPaths(tbl.getPath(), tbl.getTTable()
          .getPriPartition().getParSpaces().keySet(), sub);

      for (Path p : partPaths) {
        getMSC().getWarehouse().deleteDir(p, true);
      }

      for (Path p : partPaths) {
        getMSC().getWarehouse().mkdirs(p);
      }

    }
  }

  public void truncatePartition(String db, String table, String pri, String sub)
      throws HiveException, MetaException, TException,
      InvalidOperationException {
    return;
  }

  public boolean dropPartition(dropTableDesc dropTbl, boolean deleteData)
      throws HiveException {
    DropPartitionDesc dropPartitionDesc = new DropPartitionDesc();
    dropPartitionDesc.setDbName(dropTbl.getDbName());
    dropPartitionDesc.setLevel(dropTbl.getIsSub() ? 1 : 0);
    dropPartitionDesc.setTableName(dropTbl.getTableName());
    dropPartitionDesc.setPartNames(dropTbl.getPartitionNames());

    try {
      getMSC().dropPartition(dropPartitionDesc.getDbName(),
          dropPartitionDesc.getTableName(), dropPartitionDesc);
      return true;
    } catch (InvalidOperationException e) {
      LOG.error("drop partition error, msg=" + e.getMessage());
      throw new HiveException(e);
    } catch (MetaException e) {
      LOG.error("drop partition error, msg=" + e.getMessage());
      throw new HiveException(e);
    } catch (TException e) {
      LOG.error("drop partition error, msg=" + e.getMessage());
      throw new HiveException(e);
    }
  }

  public ArrayList<ArrayList<String>> getPartitionNames(String dbName,
      Table tbl, short max) throws HiveException {
    ArrayList<ArrayList<String>> rt = null;

    try {
      System.out.println("Get partitions from db " + dbName + ", table "
          + tbl.getName());
      List<org.apache.hadoop.hive.metastore.api.Partition> partitions = getMSC()
          .getPartitions(dbName, tbl.getName());

      if (partitions == null || partitions.get(0) == null) {
        throw new HiveException("table " + tbl.getName()
            + " do not partitioned");
      }

      rt = new ArrayList<ArrayList<String>>();
      rt.add(new ArrayList<String>());

      if (partitions.get(0).getParType().equals("hash")) {
        Set<String> partNames = new TreeSet<String>();
        partNames.add("hash(" + numOfHashPar + ")");
        rt.get(0).addAll(partNames);
      } else {
        rt.get(0).addAll(partitions.get(0).getParSpaces().keySet());
      }

      if (partitions.get(1) != null) {
        rt.add(new ArrayList<String>());
        if (partitions.get(1).getParType().equals("hash")) {
          Set<String> partNames = new TreeSet<String>();
          partNames.add("hash(" + numOfHashPar + ")");
          rt.get(1).addAll(partNames);
        } else {
          rt.get(1).addAll(partitions.get(1).getParSpaces().keySet());
        }
      }

    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    return rt;
  }

  public ArrayList<ArrayList<String>> getPartitionNames(String dbName,
      org.apache.hadoop.hive.metastore.api.Table tbl, short max)
      throws HiveException {
    ArrayList<ArrayList<String>> rt = null;

    if (tbl == null) {
      return null;
    }

    org.apache.hadoop.hive.metastore.api.Partition priPart = tbl
        .getPriPartition();
    org.apache.hadoop.hive.metastore.api.Partition subPart = tbl
        .getSubPartition();

    try {
      rt = new ArrayList<ArrayList<String>>();
      rt.add(new ArrayList<String>());

      if (priPart != null) {
        Map<String, List<String>> partSpace = priPart.getParSpaces();
        if (partSpace != null && !partSpace.isEmpty()
            && partSpace.containsKey("default")) {
          List<String> defSpace = partSpace.remove("default");
          partSpace.put("default", defSpace);
        }

        if (priPart.getParType().equals("hash")) {
          Set<String> partNames = new TreeSet<String>();
          partNames.add("hash(" + numOfHashPar + ")");
          rt.get(0).addAll(partNames);
        } else {
          rt.get(0).addAll(partSpace.keySet());
        }

        if (subPart != null) {
          Map<String, List<String>> subPartSpace = subPart.getParSpaces();
          if (subPartSpace != null && !subPartSpace.isEmpty()
              && subPartSpace.containsKey("default")) {
            List<String> defSpace = subPartSpace.remove("default");
            subPartSpace.put("default", defSpace);
          }

          rt.add(new ArrayList<String>());
          if (subPart.getParType().equals("hash")) {
            Set<String> partNames = new TreeSet<String>();
            partNames.add("hash(" + numOfHashPar + ")");
            rt.get(1).addAll(partNames);
          } else {
            rt.get(1).addAll(subPartSpace.keySet());
          }
        }

      }
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException(e);
    }
    return rt;
  }

  public List<List<String>> getPartitionNames(String dbName, String tblName,
      short max) throws HiveException {
    try {
      return getMSC().getPartitionNames(dbName, tblName, max);
    } catch (InvalidOperationException e) {
      throw new HiveException(e);
    } catch (MetaException e) {
      throw new HiveException(e);
    } catch (TException e) {
      throw new HiveException(e);
    }
  }

  public List<org.apache.hadoop.hive.metastore.api.Partition> getPartitions(
      Table tbl) throws HiveException, MetaException, TException,
      NoSuchObjectException {
    return getMSC().getPartitions(tbl.getDbName(), tbl.getName());
  }

  public List<org.apache.hadoop.hive.metastore.api.Partition> getPartitions(
      String db, String table) throws HiveException, MetaException, TException,
      NoSuchObjectException {
    return getMSC().getPartitions(db, table);
  }

  static private void checkPaths(FileSystem fs, FileStatus[] srcs, Path destf,
      boolean replace) throws HiveException {
    try {
      for (int i = 0; i < srcs.length; i++) {
        FileStatus[] items = fs.listStatus(srcs[i].getPath());
        for (int j = 0; j < items.length; j++) {

          if (Utilities.isTempPath(items[j])) {
            fs.delete(items[j].getPath(), true);
            continue;
          }

          Path tmpDest = new Path(destf, items[j].getPath().getName());
          if (!replace && fs.exists(tmpDest) && fs.isFile(tmpDest)) {
            throw new HiveException("checkPaths: " + tmpDest
                + " already exists");
          }
        }
      }
    } catch (IOException e) {
      throw new HiveException("checkPaths: filesystem error in check phase", e);
    }
  }

  static protected void copyFiles(Path srcf, Path destf, FileSystem fs)
      throws HiveException {
    if (!checkPathDomain(srcf, fs)) {
      throw new HiveException("Invalid path: srcf=" + srcf + ", fs=" + (fs==null ? null:fs.getUri()));
    }
    
    try {
      if (!fs.exists(destf))
        fs.mkdirs(destf);
    } catch (IOException e) {
      throw new HiveException(
          "copyFiles: error while checking/creating destination directory!!!",
          e);
    }

    FileStatus[] srcs;
    try {
      srcs = fs.globStatus(srcf);
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException("addFiles: filesystem error in check phase", e);
    }
    if (srcs == null) {
      LOG.info("No sources specified to move: " + srcf);
      if (SessionState.get() != null)
        SessionState.get().ssLog("No sources specified to move: " + srcf);
      return;
    }
    checkPaths(fs, srcs, destf, false);

    try {
      for (int i = 0; i < srcs.length; i++) {
        copyDir(srcs[i].getPath(), destf, fs);
      }
    } catch (IOException e) {
      throw new HiveException("copyFiles: error while moving files!!!", e);
    }

    Connection cc = null;
    SessionState ss = SessionState.get();
    try {
      if (ss != null && ss.getBIStore() != null
          && ss.getTdw_query_info() != null) {
        cc = ss.getBIStore().openConnect();
        if (cc != null) {
          ss.getBIStore().insertMoveInfo(cc,
              ss.getTdw_query_info().getQueryId(), srcf.toString(),
              destf.toString(), ss.getTbName(), ss.getDbName(),
              ss.getTdw_query_info().getTaskid());
          ss.getBIStore().closeConnect(cc);
        } else {
          LOG.error("record move operation into BI failed: "
              + ss.getTdw_query_info().getQueryId());
        }
      }
    } catch (Exception e) {
      ss.getBIStore().closeConnect(cc);
      LOG.error("record move operation into BI failed: "
          + ss.getCurrnetQueryId());
      e.printStackTrace();
    }
  }

  private static void copyDir(Path srcf, Path destf, FileSystem fs)
      throws IOException {
    FileStatus[] items = fs.listStatus(srcf);
    SessionState ss = SessionState.get();
    
    if (ss != null)
      ss.ssLog("Start copy file to " + destf.getName());
    for (int i = 0; i < items.length; i++) {
      if (items[i].isDir()) {
        Path destPath = new Path(destf, items[i].getPath()
            .getName());
        
        if(!fs.exists(destPath))
        {
          fs.mkdirs(destPath);
        }
        copyDir(items[i].getPath(), destPath, fs);
      } else {
        fs.rename(items[i].getPath(), new Path(destf, items[i].getPath()
            .getName()));
      }
      if ((i + 1) % 1000 == 0) {
        if (ss != null)
          ss.ssLog("Loading.......");
      }
    }
    if (ss != null)
      ss.ssLog("End copy file to  " + destf.getName());
  }

  static protected void replaceFiles(Path srcf, Path destf, FileSystem fs,
      Path tmppath) throws HiveException {
    if (!checkPathDomain(srcf, fs)) {
      throw new HiveException("Invalid path: srcf=" + srcf + ", fs=" + (fs==null ? null:fs.getUri()));
    }
    
    FileStatus[] srcs;
    try {
      srcs = fs.globStatus(srcf);
    } catch (IOException e) {
      throw new HiveException("addFiles: filesystem error in check phase", e);
    }
    if (srcs == null) {
      LOG.info("No sources specified to move: " + srcf);
      if (SessionState.get() != null)
        SessionState.get().ssLog("No sources specified to move: " + srcf);
      return;
    }
    checkPaths(fs, srcs, destf, true);

    try {
      fs.mkdirs(tmppath);
      for (int i = 0; i < srcs.length; i++) {
        FileStatus[] items = fs.listStatus(srcs[i].getPath());
        for (int j = 0; j < items.length; j++) {
          if (!fs.rename(items[j].getPath(), new Path(tmppath, items[j]
              .getPath().getName()))) {
            throw new HiveException("Error moving: " + items[j].getPath()
                + " into: " + tmppath);
          }
        }
      }

      boolean b = fs.delete(destf, true);
      LOG.info("Deleting:" + destf.toString() + ",Status:" + b);
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "Deleting:" + destf.toString() + ",Status:" + b);

      if (!fs.mkdirs(destf.getParent())) {
        throw new HiveException("Unable to create destination directory: "
            + destf.getParent().toString());
      }

      b = fs.rename(tmppath, destf);
      if (!b) {
        throw new HiveException(
            "Unable to move results to destination directory: "
                + destf.getParent().toString());
      }
      LOG.info("Renaming:" + tmppath.toString() + ",Status:" + b);
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            "Renaming:" + tmppath.toString() + ",Status:" + b);

      Connection cc = null;
      SessionState ss = SessionState.get();
      try {
        if (ss != null && ss.getBIStore() != null
            && ss.getTdw_query_info() != null) {
          cc = ss.getBIStore().openConnect();
          if (cc != null) {
            ss.getBIStore().insertMoveInfo(cc,
                ss.getTdw_query_info().getQueryId(), tmppath.toString(),
                destf.toString(), ss.getTbName(), ss.getDbName(),
                ss.getTdw_query_info().getTaskid());
            ss.getBIStore().closeConnect(cc);
          } else {
            LOG.error("record move operation into BI failed: "
                + ss.getTdw_query_info().getQueryId());
          }
        }
      } catch (Exception e) {
        ss.getBIStore().closeConnect(cc);
        LOG.error("record move operation into BI failed: "
            + ss.getCurrnetQueryId());
        e.printStackTrace();
      }

    } catch (IOException e) {
      throw new HiveException("replaceFiles: error while moving files!!!", e);
    } finally {
      try {
        fs.delete(tmppath, true);
      } catch (IOException e) {
        if (SessionState.get() != null)
          SessionState.get().ssLog(
              "Unable delete path " + tmppath + e.toString());
        LOG.warn("Unable delete path " + tmppath, e);
      }
    }
  }

  private IMetaStoreClient createMetaStoreClient() throws MetaException {
    return new HiveMetaStoreClient(this.conf);
  }

  public HiveMetaStoreClient getMSC() throws MetaException {
    IMetaStoreClient msc = threadLocalMSC.get();
    if (msc == null) {
      msc = this.createMetaStoreClient();
      threadLocalMSC.set(msc);
    }
    return (HiveMetaStoreClient) msc;
  }

  public static List<FieldSchema> getFieldsFromDeserializer(String name,
      Deserializer serde) throws HiveException {
    try {
      return MetaStoreUtils.getFieldsFromDeserializer(name, serde);
    } catch (SerDeException e) {
      throw new HiveException("Error in getting fields from serde. "
          + e.getMessage(), e);
    } catch (MetaException e) {
      throw new HiveException("Error in getting fields from serde."
          + e.getMessage(), e);
    }
  }

  public tdw_sys_table_statistics add_table_statistics(
      tdw_sys_table_statistics new_table_statistics)
      throws AlreadyExistsException, MetaException, TException {
    return getMSC().add_table_statistics(new_table_statistics);
  }

  public boolean delete_table_statistics(String table_statistics_name,
      String db_statistics_name) throws NoSuchObjectException, MetaException,
      TException {
    return getMSC().delete_table_statistics(table_statistics_name,
        db_statistics_name);
  }

  public tdw_sys_table_statistics get_table_statistics(
      String table_statistics_name, String db_statistics_name)
      throws MetaException, TException {
    return getMSC().get_table_statistics(table_statistics_name,
        db_statistics_name);
  }

  public List<tdw_sys_table_statistics> get_table_statistics_multi(
      String db_statistics_name, int max_parts) throws NoSuchObjectException,
      MetaException, TException {
    return getMSC().get_table_statistics_multi(db_statistics_name, max_parts);
  }

  public List<String> get_table_statistics_names(String db_statistics_name,
      int max_parts) throws NoSuchObjectException, MetaException, TException {
    return getMSC().get_table_statistics_names(db_statistics_name, max_parts);
  }

  public tdw_sys_fields_statistics add_fields_statistics(
      tdw_sys_fields_statistics new_fields_statistics)
      throws AlreadyExistsException, MetaException, TException {
    return getMSC().add_fields_statistics(new_fields_statistics);
  }

  public boolean delete_fields_statistics(String table_statistics_name,
      String db_statistics_name, String fields_statistics_name)
      throws NoSuchObjectException, MetaException, TException {
    return getMSC().delete_fields_statistics(table_statistics_name,
        db_statistics_name, fields_statistics_name);
  }

  public tdw_sys_fields_statistics get_fields_statistics(
      String table_statistics_name, String db_statistics_name,
      String fields_statistics_name) throws MetaException, TException {
    return getMSC().get_fields_statistics(table_statistics_name,
        db_statistics_name, fields_statistics_name);
  }

  public List<tdw_sys_fields_statistics> get_fields_statistics_multi(
      String table_statistics_name, String db_statistics_name, int max_parts)
      throws NoSuchObjectException, MetaException, TException {
    return getMSC().get_fields_statistics_multi(table_statistics_name,
        db_statistics_name, max_parts);
  }

  public List<String> get_fields_statistics_names(String table_statistics_name,
      String db_statistics_name, int max_parts) throws NoSuchObjectException,
      MetaException, TException {
    return getMSC().get_fields_statistics_names(table_statistics_name,
        db_statistics_name, max_parts);
  }

  public enum skewstat {
    unknow, noskew, skew
  };

  public enum mapjoinstat {
    unknow, canmapjoin, cannotmapjoin
  };

  static final double skewfactor = 0.1;
  static final int MapJoinSizeLimit = 1024 * 1024 * 200;

  public List<String> getTablesByDB(String dbname) throws HiveException {
    try {
      return getMSC().getTables(dbname, ".*");

    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public long getOnePartitionRowCount(Path pp,
      ArrayList<ArrayList<String>> parts, String partname, String subpartname,
      int tbType) throws Exception {
    long result = 0;
    if (!parts.get(0).contains(partname)) {
      return -2;
    }

    if (parts.size() == 2 && parts.get(1) != null) {
      if (!parts.get(1).contains(subpartname)) {
        return -3;
      }
      Path re = new Path(pp, partname);
      result = getRowCount(new Path(re, subpartname), tbType);
      if (result < 0)
        return -4;
      return result;
    } else {
      return -1;
    }
  }

  public long getOnePartitionSize(Path pp, ArrayList<ArrayList<String>> parts,
      String partname, String subpartname) {
    long result = 0;
    if (!parts.get(0).contains(partname)) {
      return -2;
    }

    if (parts.size() == 2 && parts.get(1) != null) {
      if (!parts.get(1).contains(subpartname)) {
        return -3;
      }
      Path re = new Path(pp, partname);
      result = getPathFileSize(new Path(re, subpartname));
      if (result < 0)
        return -4;
      return result;
    } else {
      return -1;
    }
  }

  public long getSubPartitionRowCount(Path pp,
      ArrayList<ArrayList<String>> parts, String spartname, int tbType)
      throws Exception {
    long result = 0;
    if (parts.size() == 2 && parts.get(1) != null) {
      if (!parts.get(1).contains(spartname)) {
        return -3;
      }

      Iterator<String> iterParts_1 = parts.get(0).iterator();
      while (iterParts_1.hasNext()) {
        String ppp = iterParts_1.next();
        Path tmppath = new Path(pp, ppp);
        long subre = getRowCount(new Path(tmppath, spartname), tbType);
        if (subre < 0)
          return -4;
        else {
          result = subre + result;
        }
      }

      return result;
    } else {
      return -1;
    }
  }

  public long getSubPartitionSize(Path pp, ArrayList<ArrayList<String>> parts,
      String spartname) {
    long result = 0;
    if (parts.size() == 2 && parts.get(1) != null) {
      if (!parts.get(1).contains(spartname)) {
        return -3;
      }

      Iterator<String> iterParts_1 = parts.get(0).iterator();
      while (iterParts_1.hasNext()) {
        String ppp = iterParts_1.next();
        Path tmppath = new Path(pp, ppp);
        long subre = getPathFileSize(new Path(tmppath, spartname));
        if (subre < 0)
          return -4;
        else {
          result = subre + result;
        }
      }

      return result;
    } else {
      return -1;
    }
  }

  public long getPartitionRowCount(Path pp, ArrayList<ArrayList<String>> parts,
      String partname, int tbType) throws Exception {
    long result = 0;
    if (parts.size() == 2 && parts.get(1) != null) {
      if (!parts.get(0).contains(partname)) {
        return -2;
      }
      Path re = new Path(pp, partname);
      Iterator<String> iterParts_2 = parts.get(1).iterator();
      while (iterParts_2.hasNext()) {
        String ppppp = iterParts_2.next();
        long tmpre = getRowCount(new Path(re, ppppp), tbType);
        if (tmpre < 0)
          return -4;
        result += tmpre;
      }
    } else {
      if (!parts.get(0).contains(partname)) {
        return -2;
      }
      Path re = new Path(pp, partname);
      result = getRowCount(re, tbType);
      if (result < 0)
        return -4;
    }
    return result;
  }

  public long getPartitionSize(Path pp, ArrayList<ArrayList<String>> parts,
      String partname) {
    long result = 0;
    if (parts.size() == 2 && parts.get(1) != null) {
      if (!parts.get(0).contains(partname)) {
        return -2;
      }
      Path re = new Path(pp, partname);
      Iterator<String> iterParts_2 = parts.get(1).iterator();
      while (iterParts_2.hasNext()) {
        String ppppp = iterParts_2.next();
        long tmpre = getPathFileSize(new Path(re, ppppp));
        if (tmpre < 0)
          return -4;
        result += tmpre;
      }
    } else {
      if (!parts.get(0).contains(partname)) {
        return -2;
      }
      Path re = new Path(pp, partname);
      result = getPathFileSize(re);
      if (result < 0)
        return -4;
    }
    return result;
  }

  public long getTableTotalRowCount(Path pp,
      ArrayList<ArrayList<String>> parts, int tbType) throws Exception {
    if (parts == null || pp == null)
      return -1;
    long totalrow = 0;

    Iterator<String> iterParts = parts.get(0).iterator();
    if (parts.size() == 2 && parts.get(1) != null) {
      Iterator<String> iterParts_2 = null;
      while (iterParts.hasNext()) {
        String ppp = iterParts.next();
        Path tmppath = new Path(pp, ppp);
        iterParts_2 = parts.get(1).iterator();
        while (iterParts_2.hasNext()) {
          String ppppp = iterParts_2.next();
          long subre = getRowCount(new Path(tmppath, ppppp), tbType);
          if (subre > 0)
            totalrow += subre;
        }
      }

    } else {
      while (iterParts.hasNext()) {
        String ppp = iterParts.next();
        Path re = new Path(pp, ppp);
        long subre = getRowCount(re, tbType);
        if (subre > 0)
          totalrow += subre;
      }
    }

    return totalrow;
  }

  public long getTableTotalSize(Path pp, ArrayList<ArrayList<String>> parts)
      throws MetaException {
    if (parts == null || pp == null)
      return -1;
    long totalsize = 0;

    Iterator<String> iterParts = parts.get(0).iterator();
    if (parts.size() == 2 && parts.get(1) != null) {
      Iterator<String> iterParts_2 = null;
      while (iterParts.hasNext()) {
        String ppp = iterParts.next();
        Path tmppath = new Path(pp, ppp);
        iterParts_2 = parts.get(1).iterator();
        while (iterParts_2.hasNext()) {
          String ppppp = iterParts_2.next();
          long subre = getPathFileSize(new Path(tmppath, ppppp));
          if (subre > 0)
            totalsize += subre;
        }
      }

    } else {
      while (iterParts.hasNext()) {
        String ppp = iterParts.next();
        Path re = new Path(pp, ppp);
        long subre = getPathFileSize(re);
        if (subre > 0)
          totalsize += subre;
      }
    }

    return totalsize;
  }

  public Vector<String> getAllPartitionsRowCount(Path pp,
      ArrayList<ArrayList<String>> parts, int tbType) throws Exception {
    if (parts == null || pp == null)
      return null;
    long totalsize = 0;
    Vector<String> result = new Vector<String>();
    result.clear();
    Iterator<String> iterParts = parts.get(0).iterator();
    if (parts.size() == 2 && parts.get(1) != null) {
      Vector<Long> subresult = new Vector<Long>(parts.get(1).size());
      subresult.setSize(parts.get(1).size());
      for (int i = 0; i < parts.get(1).size(); i++) {
        subresult.set(i, 0L);
      }
      Iterator<String> iterParts_2 = null;
      result.add("pri partitions:");
      while (iterParts.hasNext()) {
        String ppp = iterParts.next();
        Path tmppath = new Path(pp, ppp);
        long tmpre = 0;
        int index = 0;
        iterParts_2 = parts.get(1).iterator();
        while (iterParts_2.hasNext()) {
          String ppppp = iterParts_2.next();
          long subre = getRowCount(new Path(tmppath, ppppp), tbType);
          if (subre > 0) {
            totalsize += subre;
            tmpre += subre;
            long pastre = subresult.get(index);
            subresult.set(index, subre + pastre);
            index++;
          } else {
            index++;
          }

        }
        result.add(ppp + "	" + tmpre);
      }

      result.add("sub partitions:");
      iterParts_2 = parts.get(1).iterator();
      int index = 0;
      while (iterParts_2.hasNext()) {
        String ppppp = iterParts_2.next();
        result.add(ppppp + "	" + subresult.get(index));
        index++;
      }
    } else {
      result.add("pri partitions:");
      while (iterParts.hasNext()) {
        String ppp = iterParts.next();
        Path re = new Path(pp, ppp);
        long subre = getRowCount(re, tbType);
        if (subre > 0)
          totalsize += subre;
        if (subre > 0)
          result.add(ppp + "	" + subre);
        else
          result.add(ppp + "	" + 0);
      }
    }

    result.add("row count:" + "	" + totalsize);
    return result;
  }

  public Vector<String> getAllPartitions(Path pp,
      ArrayList<ArrayList<String>> parts) throws MetaException {
    if (parts == null || pp == null)
      return null;
    long totalsize = 0;
    Vector<String> result = new Vector<String>();
    result.clear();
    Iterator<String> iterParts = parts.get(0).iterator();
    if (parts.size() == 2 && parts.get(1) != null) {
      Vector<Long> subresult = new Vector<Long>(parts.get(1).size());
      subresult.setSize(parts.get(1).size());
      for (int i = 0; i < parts.get(1).size(); i++) {
        subresult.set(i, 0L);
      }
      Iterator<String> iterParts_2 = null;
      result.add("pri partitions:");
      while (iterParts.hasNext()) {
        String ppp = iterParts.next();
        Path tmppath = new Path(pp, ppp);
        long tmpre = 0;
        int index = 0;
        iterParts_2 = parts.get(1).iterator();
        while (iterParts_2.hasNext()) {
          String ppppp = iterParts_2.next();
          long subre = getPathFileSize(new Path(tmppath, ppppp));
          if (subre > 0) {
            totalsize += subre;
            tmpre += subre;
            long pastre = subresult.get(index);
            subresult.set(index, subre + pastre);
            index++;
          } else {
            index++;
          }
        }
        result.add(ppp + "	" + tmpre);
      }

      result.add("sub partitions:");
      iterParts_2 = parts.get(1).iterator();
      int index = 0;
      while (iterParts_2.hasNext()) {
        String ppppp = iterParts_2.next();
        result.add(ppppp + "	" + subresult.get(index));
        index++;
      }
    } else {
      result.add("pri partitions:");
      while (iterParts.hasNext()) {
        String ppp = iterParts.next();
        Path re = new Path(pp, ppp);
        long subre = getPathFileSize(re);
        if (subre > 0)
          totalsize += subre;
        if (subre > 0)
          result.add(ppp + "	" + subre);
        else
          result.add(ppp + "	" + 0);
      }
    }

    result.add("total size:" + "	" + totalsize);
    return result;
  }

  public long getRowCountForHash(Path pp, int tbType) {
    FileSystem fs = null;
    long re = 0;
    try {
      LOG.info("PATH : " + pp.toString());
      SessionState.get().ssLog("PATH : " + pp.toString());
      Class<?> clazz = conf.getClass("fs." + pp.toUri().getScheme() + ".impl",
          null);
      if (clazz == null) {
        LOG.error("No FileSystem for scheme: " + pp.toUri().getScheme());
        throw new IOException("No FileSystem for scheme: "
            + pp.toUri().getScheme());
      }
      fs = (FileSystem) ReflectionUtils.newInstance(clazz, conf);
      fs.initialize(pp.toUri(), conf);
    } catch (Exception e) {
      LOG.error(e);
      LOG.error(pp.toString());
      SessionState.get().ssLog(pp.toString());
      e.printStackTrace();
      fs = null;
      return -1;
    }
    if (fs == null) {
      LOG.error("fs==null");
      return -1;
    }
    try {
      FileStatus[] fss = fs.listStatus(pp);
      if (fss == null) {
        LOG.error("fss == null");
        fs = null;
        return -3;
      }
      for (FileStatus fssentry : fss) {
        long tmp = getRowCount(fssentry.getPath(), tbType);
        if (tmp > 0)
          re += tmp;
      }
    } catch (Exception e) {
      LOG.error(e);
      e.printStackTrace();
      fs = null;
      return -2;
    }
    fs = null;
    return re;
  }

  public long getRowCount(Path pp, int tbType) throws Exception {
    if (tbType == 1) {
      return this.getFormatRowCount(pp);
    } else if (tbType == 4) {
      return this.getRCFileRowCount(pp);
    }
    return 0;
  }

  public long getRCFileRowCount(Path pp) throws Exception {
    FileSystem fs = null;
    long re = 0;
    try {
      LOG.debug("PATH : " + pp.toString());
      fs = FileSystem.get(pp.toUri(), conf);
      FileStatus[] fss = fs.listStatus(pp);
      if (fss != null) {
        for (FileStatus fssentry : fss) {
          String fileName = fssentry.getPath().toString();
          if (fileName.endsWith(".rcf")) {
            int index = fileName.lastIndexOf("_");
            String sub = fileName.substring(index + 1, fileName.length() - 4);
            re += Integer.valueOf(sub);
          }

        }
      }
    } catch (Exception e) {
      LOG.error(e);
      LOG.error(pp.toString());
      SessionState.get().ssLog(pp.toString());
      e.printStackTrace();
      fs = null;
      return -1;
    }
    return re;
  }

  public long getFormatRowCount(Path pp) throws Exception {
    FileSystem fs = null;
    IFormatDataFile ifdf = null;
    long re = 0;
    try {
      LOG.debug("PATH : " + pp.toString());
      SessionState.get().ssLog("PATH : " + pp.toString());
      Class<?> clazz = conf.getClass("fs." + pp.toUri().getScheme() + ".impl",
          null);
      if (clazz == null) {
        LOG.error("No FileSystem for scheme: " + pp.toUri().getScheme());
        throw new IOException("No FileSystem for scheme: "
            + pp.toUri().getScheme());
      }
      fs = (FileSystem) ReflectionUtils.newInstance(clazz, conf);
      fs.initialize(pp.toUri(), conf);
      ifdf = new IFormatDataFile(new Configuration());
    } catch (Exception e) {
      LOG.error(e);
      LOG.error(pp.toString());
      SessionState.get().ssLog(pp.toString());
      e.printStackTrace();
      fs = null;
      return -1;
    }
    if (fs == null || ifdf == null) {
      LOG.error("fs==null or ifdf==null");
      fs = null;
      return -1;
    }
    try {
      FileStatus[] fss = fs.listStatus(pp);
      if (fss == null) {
        LOG.error("fss == null");
        fs = null;
        return -3;
      }

      int filecnt = 1;
      for (FileStatus fssentry : fss) {
        String pa = fssentry.getPath().toString();
        if ((filecnt % 100) == 0) {
          if (SessionState.get() != null)
            SessionState.get().ssLog("sr...");

          filecnt = 1;
        }
        filecnt++;

        if (!pa.endsWith(".gz")) {
          try {
            ifdf.open(pa);
          } catch (Exception e) {
            LOG.error(e);
            if (e.getMessage().contains("native-lzo library not available")) {
              throw e;
            }
            e.printStackTrace();
            continue;
          }
          long tmp = ifdf.recnum();
          if (tmp > 0)
            re += tmp;
          ifdf.close();
        } else {
          int index = pa.lastIndexOf("_");
          String sub = pa.substring(index + 1, pa.length() - 3);
          re += Integer.valueOf(sub);
        }
      }

      if (filecnt > 0) {
        if (SessionState.get() != null)
          SessionState.get().ssLog("sr...");
      }
    } catch (Exception e) {
      if (e.getMessage().contains("native-lzo library not available")) {
        throw e;
      }
      LOG.error(e);
      e.printStackTrace();
      fs = null;
      return -2;
    }
    fs = null;
    return re;
  }

  public long getPathFileSizeForHash(Path pp) {
    FileSystem fs = null;
    long re = 0;
    try {
      LOG.debug("PATH : " + pp.toString());
      SessionState.get().ssLog("PATH : " + pp.toString());
      Class<?> clazz = conf.getClass("fs." + pp.toUri().getScheme() + ".impl",
          null);
      if (clazz == null) {
        LOG.error("No FileSystem for scheme: " + pp.toUri().getScheme());
        throw new IOException("No FileSystem for scheme: "
            + pp.toUri().getScheme());
      }
      fs = (FileSystem) ReflectionUtils.newInstance(clazz, conf);
      fs.initialize(pp.toUri(), conf);
    } catch (Exception e) {
      LOG.error(e);
      LOG.error(pp.toString());
      SessionState.get().ssLog(pp.toString());
      e.printStackTrace();
      fs = null;
      return -1;
    }
    if (fs == null) {
      LOG.error("fs==null");
      return -1;
    }
    try {
      FileStatus[] fss = fs.listStatus(pp);
      if (fss == null) {
        LOG.error("fss==null");
        fs = null;
        return -3;
      }
      for (FileStatus fssentry : fss) {
        long tmp = getPathFileSize(fssentry.getPath());
        if (tmp > 0)
          re += tmp;
      }
    } catch (Exception e) {
      LOG.error(e);
      e.printStackTrace();
      fs = null;
      return -2;
    }
    fs = null;
    return re;
  }

  public long getPathFileSize(Path pp) {
    FileSystem fs = null;
    long re = 0;
    try {
      LOG.debug("PATH : " + pp.toString());
      SessionState.get().ssLog("PATH : " + pp.toString());
      // pp = new Path(arr[0]+"://171.25.38.241:"+arr[2]);
      Class<?> clazz = conf.getClass("fs." + pp.toUri().getScheme() + ".impl",
          null);
      if (clazz == null) {
        LOG.error("No FileSystem for scheme: " + pp.toUri().getScheme());
        throw new IOException("No FileSystem for scheme: "
            + pp.toUri().getScheme());
      }
      fs = (FileSystem) ReflectionUtils.newInstance(clazz, conf);
      fs.initialize(pp.toUri(), conf);
    } catch (Exception e) {
      LOG.error(e);
      LOG.error(pp.toString());
      SessionState.get().ssLog(e.toString());
      SessionState.get().ssLog(pp.toString());
      e.printStackTrace();
      fs = null;
      return -1;
    }
    if (fs == null) {
      LOG.error("fs==null");
      return -1;
    }
    try {
      FileStatus[] fss = fs.listStatus(pp);
      if (fss == null) {
        LOG.error("fss==null");
        fs = null;
        return -3;
      }

      int filecnt = 1;
      for (FileStatus fssentry : fss) {
        re += fssentry.getLen();
        if ((filecnt % 100) == 0) {
          if (SessionState.get() != null)
            SessionState.get().ssLog("sf...");

          filecnt = 1;
        }
        filecnt++;
      }
      if (filecnt > 0) {
        if (SessionState.get() != null)
          SessionState.get().ssLog("sf...");
      }
    } catch (Exception e) {
      LOG.error(e);
      e.printStackTrace();
      fs = null;
      return -2;
    }
    fs = null;
    return re;
  }

  public String showinfo(String table_name) {
    if (table_name == null) {
      return null;
    }
    String dbname = SessionState.get().getDbName();
    List<FieldSchema> cols = null;
    tdw_sys_table_statistics ttt = null;
    tdw_sys_fields_statistics t = null;
    int count = 0;
    String result = table_name;
    try {
      cols = this.getTable(dbname, table_name).getCols();
      count = cols.size();
      ttt = get_table_statistics(table_name, dbname);
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (cols == null) {
      return null;
    } else if (ttt == null) {
      return null;
    } else {
      int tablesize = ttt.getStat_total_size();
      result += ("	" + tablesize + "	");
      for (int i = 0; i < count; i++) {
        try {
          t = get_fields_statistics(table_name, dbname, cols.get(i).getName());
        } catch (Exception e) {
          e.printStackTrace();
        }
        if (t != null) {
          String distvalue[] = t.getStat_numbers_1().split("\1");
          double mymax = 0.0;
          for (String mystr : distvalue) {
            double tmpd = 0.0;
            try {
              tmpd = Double.parseDouble(mystr);
            } catch (Exception e) {
              continue;
            }
            if (tmpd > mymax) {
              mymax = tmpd;
            }
          }
          result += (cols.get(i).getName() + ":" + mymax + "  ");
        } else {
          result += (cols.get(i).getName() + ":unkown" + "  ");
        }
      }
    }
    return result;
  }

  public skewstat getSkew(String table_statistics_name,
      String fields_statistics_name) {
    try {

      tdw_sys_fields_statistics t = get_fields_statistics(
          table_statistics_name, SessionState.get().getDbName(),
          fields_statistics_name);
      if (t == null) {
        console.printInfo(table_statistics_name + "." + fields_statistics_name
            + " has no stat info");
        return skewstat.unknow;
      }
      String mcvnums[] = t.getStat_numbers_1().split("\1");
      String mcvvalues[] = t.getStat_values_1().split("\1");
      int index = -1;
      for (String str : mcvnums) {
        index++;
        if (!str.trim().equals("")) {
          if (Double.valueOf(str.trim()).doubleValue() > skewfactor) {
            return skewstat.skew;
          }

        }

      }

    } catch (Exception e) {
      return skewstat.unknow;
    }

    return skewstat.noskew;
  }

  public mapjoinstat canTableMapJoin(String table_statistics_name) {
    try {

      tdw_sys_table_statistics t = get_table_statistics(table_statistics_name,
          SessionState.get().getDbName());
      if (t != null) {
        if (t.getStat_total_size() < MapJoinSizeLimit
            && t.getStat_total_size() > 0) {
          return mapjoinstat.canmapjoin;
        } else {
          return mapjoinstat.cannotmapjoin;
        }
      } else {
        console.printInfo(table_statistics_name + " has no stat info");
        return mapjoinstat.unknow;
      }

    } catch (Exception e) {
      return mapjoinstat.unknow;
    }

  }

  public String getGroupName(String username) throws HiveException {
    try {
      return getMSC().getGroupname(username.trim().toLowerCase());
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  public List<String> getGroups(String pa) throws HiveException {
    List<String> result = new ArrayList<String>();

    try {
      List<group> re = getMSC().getGroups(pa);
      result.clear();
      for (Iterator i = re.iterator(); i.hasNext();) {
        group tmp = (group) i.next();
        String tmpre = tmp.getGroupName() + "	" + tmp.getCreator() + "	"
            + tmp.getUserNum() + "	" + tmp.getUserList();
        result.add(tmpre);
      }

      return result;
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException(e);
    }
  }

  public boolean revoke_user_group(String gname, ArrayList<String> users,
      String revoker) throws HiveException {
    if ((!revoker.equals(HiveMetaStore.ROOT_USER)) && (!isDBAUser(revoker))) {
      throw new HiveException(revoker + " has no right to grant the group!");
    }
    for (int i = 0; i < users.size(); i++) {
      String curuser = users.get(i);
      if (curuser.equalsIgnoreCase(HiveMetaStore.ROOT_USER)
          && (!revoker.equalsIgnoreCase(HiveMetaStore.ROOT_USER))) {
        throw new HiveException(revoker + " has no right to grant root!");
      }
      boolean midre = false;
      int re = 0;
      try {
        re = getMSC().revokeUserGroup(gname, curuser, revoker);
      } catch (Exception e) {
        e.printStackTrace();
        throw new HiveException("Fail to grant the group: " + gname);
      }

      LOG.debug("revoke usergroup result:  " + re);
      if (re == 1) {
        throw new HiveException("Can not find user " + curuser);
      } else if (re == 2) {
        throw new HiveException(curuser + " is not in user group " + gname);
      } else if (re == 3) {
        throw new HiveException("Can not find user group " + gname);
      } else if (re == 5) {
        throw new HiveException(revoker + " has no right to revoke the group "
            + gname);
      } else if (re == 0) {
        midre = true;
      } else {
        throw new HiveException("Fail to grant the group: " + gname);
      }

      if (!midre) {
        return false;
      }
    }
    return true;
  }

  public boolean revokeUserGroup(String gname, ArrayList<String> users,
      String revoker) throws HiveException {
    if ((!revoker.equals(HiveMetaStore.ROOT_USER)) && (!isDBAUser(revoker))) {
      throw new HiveException(revoker + " has no right to grant the group!");
    }

    for (int i = 0; i < users.size(); i++) {
      String curuser = users.get(i);
      if (curuser.equalsIgnoreCase(HiveMetaStore.ROOT_USER)
          && (!revoker.equalsIgnoreCase(HiveMetaStore.ROOT_USER))) {
        throw new HiveException(revoker + " has no right to grant root!");
      }

      boolean midre = false;
      int re = 0;

      try {
        re = getMSC().revokeUserGroup(gname, curuser, revoker);
      } catch (Exception e) {
        e.printStackTrace();
        throw new HiveException("Fail to grant the group: " + gname);
      }

      LOG.debug("revoke usergroup result:  " + re);
      if (re == 1) {
        throw new HiveException("Can not find user " + curuser);
      } else if (re == 2) {
        throw new HiveException(curuser + " is not in user group " + gname);
      } else if (re == 3) {
        throw new HiveException("Can not find user group " + gname);
      } else if (re == 5) {
        throw new HiveException(revoker + " has no right to revoke the group "
            + gname);
      } else if (re == 0) {
        midre = true;
      } else {
        throw new HiveException("Fail to grant the group: " + gname);
      }

      if (!midre) {
        return false;
      }
    }
    return true;
  }

  public boolean grant_user_group(String gname, ArrayList<String> users,
      String granter) throws HiveException {
    if ((!granter.equals(HiveMetaStore.ROOT_USER)) && (!isDBAUser(granter))) {
      throw new HiveException(granter + " has no right to grant the group!");
    }
    for (int i = 0; i < users.size(); i++) {
      String curuser = users.get(i);
      if (curuser.equalsIgnoreCase(HiveMetaStore.ROOT_USER)
          && (!granter.equalsIgnoreCase(HiveMetaStore.ROOT_USER))) {
        throw new HiveException(granter + " has no right to grant root!");
      }
      boolean midre = false;
      int re = 0;
      try {
        re = getMSC().grantUserGroup(gname, curuser, granter);
      } catch (Exception e) {
        e.printStackTrace();
        throw new HiveException("Fail to grant the group: " + gname);
      }

      LOG.debug("grant usergroup result:  " + re);
      if (re == 1) {
        SessionState.get().ssLog("Can not find user " + curuser);
        throw new HiveException("Can not find user " + curuser);
      } else if (re == 2) {
        SessionState.get().ssLog(curuser + " already in user group " + gname);
        throw new HiveException(curuser + " already in user group " + gname);
      } else if (re == 4) {
        SessionState.get().ssLog("Can not find user group " + gname);
        throw new HiveException("Can not find user group " + gname);
      } else if (re == 5) {
        SessionState.get().ssLog(
            granter + " has no right to grant the group " + gname);
        throw new HiveException(granter + " has no right to grant the group "
            + gname);
      } else if (re == 0) {
        midre = true;
      } else {
        SessionState.get().ssLog("Fail to grant the group: " + gname);
        throw new HiveException("Fail to grant the group: " + gname);
      }

      if (!midre) {
        return false;
      }
    }
    return true;
  }

  public boolean grantUserGroup(String gname, ArrayList<String> users,
      String granter) throws HiveException {
    if ((!granter.equals(HiveMetaStore.ROOT_USER)) && (!isDBAUser(granter))) {
      throw new HiveException(granter + " has no right to grant the group!");
    }

    for (int i = 0; i < users.size(); i++) {
      String curuser = users.get(i);
      if (curuser.equalsIgnoreCase(HiveMetaStore.ROOT_USER)
          && (!granter.equalsIgnoreCase(HiveMetaStore.ROOT_USER))) {
        throw new HiveException(granter + " has no right to grant root!");
      }

      boolean midre = false;
      int re = 0;

      try {
        re = getMSC().grantUserGroup(gname, curuser, granter);
      } catch (Exception e) {
        e.printStackTrace();
        throw new HiveException("Fail to grant the group: " + gname);
      }

      LOG.debug("grant usergroup result:  " + re);
      if (re == 1) {
        SessionState.get().ssLog("Can not find user " + curuser);
        throw new HiveException("Can not find user " + curuser);
      } else if (re == 2) {
        SessionState.get().ssLog(curuser + " already in user group " + gname);
        throw new HiveException(curuser + " already in user group " + gname);
      } else if (re == 4) {
        SessionState.get().ssLog("Can not find user group " + gname);
        throw new HiveException("Can not find user group " + gname);
      } else if (re == 5) {
        SessionState.get().ssLog(
            granter + " has no right to grant the group " + gname);
        throw new HiveException(granter + " has no right to grant the group "
            + gname);
      } else if (re == 0) {
        midre = true;
      } else {
        SessionState.get().ssLog("Fail to grant the group: " + gname);
        throw new HiveException("Fail to grant the group: " + gname);
      }

      if (!midre) {
        return false;
      }
    }
    return true;
  }

  public boolean drop_user_group(String gname, String drop, String dbname)
      throws HiveException {
    gname = gname.trim().toLowerCase();
    drop = drop.trim().toLowerCase();

    if ((!drop.equals(HiveMetaStore.ROOT_USER)) && (!isDBAUser(drop))) {
      throw new HiveException(drop + " has no right to drop the group!");
    }

    if (gname.equalsIgnoreCase(HiveMetaStore.DEFAULT)) {
      throw new HiveException("default group can not be dropped!");
    }

    int re = 0;
    try {
      re = getMSC().dropUserGroup(gname, drop);
    } catch (Exception e) {
      throw new HiveException("Fail to drop the group: " + gname);
    }

    if (re == 1)
      throw new HiveException("Can not find the group: " + gname);
    else if (re == 2)
      throw new HiveException("Fail to drop the group: " + gname);
    else if (re == 0)
      return true;
    else if (re == 3)
      throw new HiveException(drop + " has no right to drop the group: "
          + gname);
    else
      throw new HiveException("Fail to drop the group: " + gname);

  }

  public boolean dropUserGroup(String gname, String drop, String dbname)
      throws HiveException {
    gname = gname.trim().toLowerCase();
    drop = drop.trim().toLowerCase();

    if ((!drop.equals(HiveMetaStore.ROOT_USER)) && (!isDBAUser(drop))) {
      throw new HiveException(drop + " has no right to drop the group!");
    }

    if (gname.equalsIgnoreCase(HiveMetaStore.DEFAULT)) {
      throw new HiveException("default group can not be dropped!");
    }

    int re = 0;
    try {
      re = getMSC().dropUserGroup(gname, drop);
    } catch (Exception e) {
      throw new HiveException("Fail to drop the group: " + gname);
    }

    if (re == 1) {
      throw new HiveException("Can not find the group: " + gname);
    } else if (re == 2) {
      throw new HiveException("Fail to drop the group: " + gname);
    } else if (re == 0) {
      return true;
    } else if (re == 3) {
      throw new HiveException(drop + " has no right to drop the group: "
          + gname);
    } else {
      throw new HiveException("Fail to drop the group: " + gname);
    }

  }

  public boolean add_user_group(String gname, String creat, String dbname)
      throws HiveException {
    gname = gname.trim().toLowerCase();
    creat = creat.trim().toLowerCase();

    if ((!creat.equals(HiveMetaStore.ROOT_USER)) && (!isDBAUser(creat))) {
      throw new HiveException(creat + " has no right to create the group!");
    }

    if (gname.length() > 128 || gname.length() < 1)
      throw new HiveException(
          "Length of the new group's name must be between 1 and 128 !");

    if (isARole(gname) || (isAUser(gname))) {
      throw new HiveException(
          "Fail to create the new group, There is a role/user with the same name!");
    }

    if (gname.equalsIgnoreCase(HiveMetaStore.DEFAULT)) {
      throw new HiveException("default group can not be created!");
    }

    if (!MetaStoreUtils.validateName(gname)) {
      throw new HiveException("the name is not a valid group name!");
    }

    if (gname.equalsIgnoreCase(dbname)) {
      throw new HiveException(
          "Fail to create the new group, it is the same with database name!");
    }

    try {
      return getMSC().addUserGroup(new group(gname, creat, null, 0), creat);
    } catch (Exception e) {
      throw new HiveException("Fail to create the group: " + gname);
    }
  }

  public boolean addUserGroup(String gname, String creat, String dbname)
      throws HiveException {
    gname = gname.trim().toLowerCase();
    creat = creat.trim().toLowerCase();

    if ((!creat.equals(HiveMetaStore.ROOT_USER)) && (!isDBAUser(creat))) {
      throw new HiveException(creat + " has no right to create the group!");
    }

    if (gname.length() > 128 || gname.length() < 1) {
      throw new HiveException(
          "Length of the new group's name must be between 1 and 128 !");
    }

    if (isARole(gname) || (isAUser(gname))) {
      throw new HiveException(
          "Fail to create the new group, There is a role/user with the same name!");
    }

    if (gname.equalsIgnoreCase(HiveMetaStore.DEFAULT)) {
      throw new HiveException("default group can not be created!");
    }

    if (!MetaStoreUtils.validateName(gname)) {
      throw new HiveException("the name is not a valid group name!");
    }

    if (gname.equalsIgnoreCase(dbname)) {
      throw new HiveException(
          "Fail to create the new group, it is the same with database name!");
    }

    try {
      return getMSC().addUserGroup(new group(gname, creat, null, 0), creat);
    } catch (Exception e) {
      throw new HiveException("Fail to create the group: " + gname);
    }
  }

  public String get_query(String qid, String username) throws Exception {
    SessionState ss = SessionState.get();
    Connection cc = null;
    tdw_query_info result = null;
    try {
      if (ss.getBIStore() != null) {
        cc = ss.getBIStore().openConnect();
        if (cc != null) {
          result = ss.getBIStore().getQueryInfoItem(cc, qid);
          ss.getBIStore().closeConnect(cc);
        } else {
          LOG.error("get sql  process info from BI failed: queryid=" + qid);
        }
      } else {
        LOG.info("SessionState.get().getBIStore() is null");
      }
    } catch (Exception e) {
      ss.getBIStore().closeConnect(cc);
      LOG.error("get sql process info from BI failed: queryid=" + qid);
      e.printStackTrace();
    }

    if (result == null) {
      return null;
    }
    String uuu = result.getUserName();
    if (username.equalsIgnoreCase(uuu) || username.equalsIgnoreCase("root")) {
      return result.getQueryString();
    } else {
      throw new Exception(username + " can not get this info!");
    }

  }

  public String get_jobid(String qid, String username) throws Exception {
    tdw_query_info result = getMSC().search_tdw_query_info(qid);
    String killed = result.getUserName();
    String killer = username;
    if ((!killer.equalsIgnoreCase(killed))
        && (!killer.equalsIgnoreCase("root"))) {
      throw new Exception(killer + " has no right to kill this query!");
    }

    List<tdw_query_stat> statlist = getMSC().get_tdw_query_stat();
    for (tdw_query_stat tmpstat : statlist) {
      if (tmpstat.getQueryId().equalsIgnoreCase(qid)) {
        return tmpstat.getCurrMRId();
      }
    }
    return null;
  }

  public void kill_jobid(String jid) throws Exception {
    JobConf mysmalljob = new JobConf(conf, Hive.class);
    JobClient jc = new JobClient(mysmalljob);
    RunningJob running = jc.getJob(jid);
    running.killJob();
  }

  public boolean add_tdw_query_info(tdw_query_info query_info)
      throws MetaException, TException {
    return getMSC().add_tdw_query_info(query_info);
  }

  public boolean add_tdw_query_stat(tdw_query_stat query_stat)
      throws MetaException, TException {
    return getMSC().add_tdw_query_stat(query_stat);
  }

  public boolean update_tdw_query_info(String qid, String finishtime,
      String state) throws MetaException, TException {
    return getMSC().update_tdw_query_info(qid, finishtime, state);
  }

  public boolean update_tdw_query_stat(String qid, String finishtime,
      String state) throws MetaException, TException {
    return getMSC().update_tdw_query_stat(qid, finishtime, state);
  }

  public List<String> get_tdw_query(String uname, boolean islocal)
      throws Exception {

    List<ShowProcessListResult> processlist = null;
    SessionState ss = SessionState.get();
    Connection cc = null;
    try {
      if (ss.getBIStore() != null) {
        cc = ss.getBIStore().openConnect();
        if (cc != null) {
          processlist = ss.getBIStore().getQueryInfo(cc, uname);
          ss.getBIStore().closeConnect(cc);
        } else {
          LOG.error("get sql   processing info from BI failed: ");
        }
      } else {
        LOG.info("SessionState.get().getBIStore() is null");
      }
    } catch (Exception e) {
      ss.getBIStore().closeConnect(cc);
      LOG.error("get sql processing info from BI failed: ");
      e.printStackTrace();
    }

    List<String> alist = new ArrayList<String>();

    if (processlist != null && !processlist.isEmpty()) {
      for (ShowProcessListResult ret : processlist) {
        String tre = printprocess(ret.queryInfo, ret.queryStat, islocal);
        if (tre != null) {
          alist.add(tre);
        }
      }
    }

    return alist;
  }

  public boolean clear_tdw_query(int days) throws MetaException, TException {

    boolean r1 = getMSC().clear_tdw_query_info(days);
    if (r1) {
      return getMSC().clear_tdw_query_stat(days);
    } else {
      boolean r2 = getMSC().clear_tdw_query_stat(days);
      return false;
    }
  }

  private String printprocess(tdw_query_info info, tdw_query_stat stat,
      boolean islocal) throws Exception {
    int maxlen = 100;
    double rate = 0.8;

    RunningJob thejob = SessionState.getRunningJob(info.getQueryId());
    if ((thejob == null && (!islocal))
        || (!thejob.getJobID().equalsIgnoreCase(stat.getCurrMRId()))) {
      String jid = stat.getCurrMRId();
      if (jid != null) {
        JobConf mysmalljob = new JobConf(conf, Hive.class);
        JobClient jc = new JobClient(mysmalljob);
        thejob = jc.getJob(jid);
      }
    }
    if ((thejob == null) || (thejob.isComplete())) {
      LOG.info(info.getQueryId() + " lost or completed");
      return null;
    }

    if (info != null && stat != null) {
      String result = "";
      result += info.getQueryId();
      result += "\01";
      result += info.getUserName();
      result += "\01";
      String newdatestr = info.getStartTime();
      result += newdatestr;
      result += "\01";
      result += stat.getCurrMRIndex() + "/" + info.getMRNum();
      result += "\01";
      result += (stat.getCurrMRId() + "," + stat.getMapNum() + "," + stat
          .getReduceNum());
      result += "\01";
      result += Math.round(thejob.mapProgress() * 100) + "%  "
          + Math.round(thejob.reduceProgress() * 100) + "%";
      result += "\01";
      result += info.getIp();
      result += "\01";
      if (info.getTaskid() != null) {
        result += info.getTaskid();
        result += "\01";
      } else {
        result += "null";
        result += "\01";
      }
      int len = info.getQueryString().length();
      if (len > maxlen) {
        result += info.getQueryString().substring(0, (int) (maxlen * rate))
            .replace('\n', ' ');
        result += " ... ";
        result += info.getQueryString()
            .substring(len - (int) (maxlen * (1 - rate)), len)
            .replace('\n', ' ');
      } else {
        result += info.getQueryString().replace('\n', ' ');
      }
      return result;
    } else {
      return null;
    }
  }

  public boolean createUser(String byWho, String newUser, String passwd)
      throws HiveException {
    byWho = byWho.trim().toLowerCase();
    newUser = newUser.trim().toLowerCase();

    if (newUser.equals(HiveMetaStore.ROOT_USER)) {
      throw new HiveException("Can not create the ROOT user!");
    }

    if (newUser.length() > 128 || newUser.length() < 1) {
      throw new HiveException(
          "Length of the new user's name must be between 1 and 128 !");
    }

    if (passwd.length() > 64 || passwd.length() < 1) {
      throw new HiveException("Length of the passwd must be between 1 and 64 !");
    }

    boolean byDBA = isDBAUser(byWho);
    if (!byDBA) {
      throw new HiveException(
          "Only DBAs have the privilege to create a new user!");
    }

    try {
      getMSC().createUer(byWho, newUser, passwd);
      return true;
    } catch (AlreadyExistsException e1) {
      throw new HiveException("The user already exists! Can not recreate it!");
    } catch (TException e1) {
      throw new HiveException("Fail to create the new user: " + newUser
          + ", msg:" + e1.getMessage());
    } catch (MetaException e1) {
      throw new HiveException("Fail to create the new user: " + newUser
          + ", msg:" + e1.getMessage());
    } catch (Exception e1) {
      throw new HiveException("Fail to create the new user: " + newUser
          + ", msg:" + e1.getMessage());
    }
  }

  public boolean dropUsers(String byWho, List<String> users)
      throws HiveException {
    boolean success = true;
    for (String user : users) {
      success &= dropUser(byWho, user);
    }
    return success;
  }

  private boolean dropUser(String byWho, String userName) throws HiveException {
    byWho = byWho.trim().toLowerCase();
    userName = userName.trim().toLowerCase();

    if (userName.equals(HiveMetaStore.ROOT_USER)) {
      throw new HiveException("Can not drop the ROOT user!");
    }

    boolean byRoot = byWho.equals(HiveMetaStore.ROOT_USER);
    boolean byDBA = isDBAUser(byWho);
    boolean dropDBA = isDBAUser(userName);

    if (dropDBA) {
      if (byRoot) {
        try {
          return getMSC().dropUser(byWho, userName);
        } catch (NoSuchObjectException e) {
          throw new HiveException("Fail to drop the user: " + userName
              + "! The user does not exist!");
        } catch (Exception e) {
          throw new HiveException("Fail to drop the user: " + userName + "!"
              + e.toString());
        }
      } else {
        throw new HiveException("Only the ROOT user can drop a DBA user!");
      }
    }

    if (byDBA) {
      try {
        return getMSC().dropUser(byWho, userName);
      } catch (NoSuchObjectException e) {
        throw new HiveException("Fail to drop the user: " + userName
            + "! The user does not exist!");
      } catch (Exception e) {
        throw new HiveException("Fail to drop the user: " + userName + "!"
            + org.apache.hadoop.util.StringUtils.stringifyException(e));
      }
    } else {
      throw new HiveException("Only DBAs can drop users!");
    }
  }

  private User getUser(String byWho, String userName) throws HiveException {
    byWho = byWho.trim().toLowerCase();
    userName = userName.trim().toLowerCase();

    try {
      return getMSC().getUser(byWho, userName);
    } catch (NoSuchObjectException e) {
      throw new HiveException("Fail to get information of the user: "
          + userName + "! The user does not exist!");
    } catch (Exception e) {
      throw new HiveException("Fail to get information of the user: "
          + userName + "!");
    }
  }

  public List<String> showUsers(String byWho) throws HiveException {
    byWho = byWho.trim().toLowerCase();
    try {
      return getMSC().getUsersAll(byWho);
    } catch (Exception e) {
      throw new HiveException(
          "Failed to get all the users' names! Please check the log.");
    }
  }

  public boolean setPasswd(String byWho, String userName, String newPasswd)
      throws HiveException {
    byWho = byWho.trim().toLowerCase();

    if (userName == null) {
      userName = byWho;
    } else {
      userName = userName.trim().toLowerCase();
    }

    if (newPasswd.length() < 1 || newPasswd.length() > 64) {
      throw new HiveException(
          "The length of the new passwd must be between 1 and 41!");
    }

    if (userName.equals(HiveMetaStore.ROOT_USER)) {
      if (byWho.equals(HiveMetaStore.ROOT_USER)) {
        try {
          return getMSC().setPasswd(byWho, userName, newPasswd);
        } catch (Exception e) {
          throw new HiveException(
              "Failed to set passwd for the ROOT user! Please check the log.");
        }
      } else {
        throw new HiveException(
            "You have no privilege to set the passwd for the ROOT user!");
      }
    }

    boolean byDBA = isDBAUser(byWho);
    boolean forDBA = isDBAUser(userName);

    if (forDBA) {
      if (byWho.equals(userName) || byWho.equals(HiveMetaStore.ROOT_USER)) {
        try {
          return getMSC().setPasswd(byWho, userName, newPasswd);
        } catch (Exception e) {
          throw new HiveException("Failed to set passwd for the user: "
              + userName + " ! Please check the log.");
        }
      } else {
        throw new HiveException(
            "Only the DBA user himself or the ROOT user has the privilege to set the passwd for the DBA user: "
                + userName + " !");
      }
    }

    if (byWho.equals(userName) || byDBA) {
      try {
        return getMSC().setPasswd(byWho, userName, newPasswd);
      } catch (Exception e) {
        throw new HiveException("Failed to set passwd for the user: "
            + userName + " ! Please check the log.");
      }
    } else {
      throw new HiveException(
          "Only the user himself or DBAs have the privilege to set passwd for the user: "
              + userName + " !");
    }
  }


  public boolean isAUser(String userName, String passwd) throws HiveException {
    try {
      return getMSC().isAUser(userName, passwd);
    } catch (Exception e) {
      throw new HiveException("eeeeee  " + e.getMessage() + "  " + e.toString()
          + "  " + "Failed to judge the user: " + userName
          + " ! Please check the log.");
    }
  }

  private boolean isAUser(String userName) throws HiveException {
    userName = userName.trim().toLowerCase();

    try {
      return getMSC().isAUser(userName);
    } catch (NoSuchObjectException e) {
      return false;
    } catch (Exception e) {
      throw new HiveException("Failed to judge the user: " + userName
          + " ! Please check the log.");
    }
  }

  public boolean grantAuth(String byWho, String who, List<String> privileges,
      String db, String table) throws HiveException {
    if (db == null || db.equals("*")) {
      if (!(table == null || table.equals("*"))) {
        throw new HiveException("Can not grant privileges in this format!");
      }

      if (isAUser(who)) {
        return grantAuthSys(byWho, who, privileges);
      } else if (isARole(who)) {
        return grantAuthRoleSys(byWho, who, privileges);
      } else {
        throw new HiveException(
            "Can not grant sys privileges to user or role which doesn't exist!");
      }
    } else if (table == null || table.equals("*")) {
      return grantAuthOnDb(byWho, who, privileges, db);
    } else {
      return grantAuthOnTbl(byWho, who, privileges, db, table);
    }
  }

  private boolean grantAuthSys(String byWho, String userName,
      List<String> privileges) throws HiveException {
    byWho = byWho.trim().toLowerCase();
    userName = userName.trim().toLowerCase();

    if (userName.equals(HiveMetaStore.ROOT_USER)) {
      throw new HiveException(
          "You have no privilege to grant sys privileges for the ROOT user!");
    }

    boolean byDBA = isDBAUser(byWho);
    boolean forDBA = isDBAUser(userName);
    List<String> privs = new ArrayList<String>();

    for (String priv : privileges) {
      priv = priv.trim().toUpperCase();
      if (priv.equals("TOK_DBA_PRI"))
        forDBA = true;
      privs.add(priv);
    }

    if (forDBA) {
      if (byWho.equals(HiveMetaStore.ROOT_USER)) {
        try {
          return getMSC().grantAuthSys(byWho, userName, privs);
        } catch (NoSuchObjectException e) {
          throw new HiveException("Fail to grant sys privileges to the user: "
              + userName + " " + e.getMessage());
        } catch (InvalidObjectException e) {
          throw new HiveException("Fail to grant sys privileges to the user: "
              + userName + " " + e.getMessage());
        } catch (Exception e) {
          throw new HiveException("Fail to grant sys privileges to the user: "
              + userName + " " + e.getMessage());
        }
      } else {
        throw new HiveException(
            "Only the ROOT user has the privilege to grant sys privileges for the DBA user or grant the DBA privilege to the user!");
      }
    }

    if (byDBA) {
      try {
        return getMSC().grantAuthSys(byWho, userName, privs);
      } catch (NoSuchObjectException e) {
        throw new HiveException("Fail to grant sys privileges to the user: "
            + userName + " " + e.getMessage());
      } catch (InvalidObjectException e) {
        throw new HiveException("Fail to grant sys privileges to the user: "
            + userName + " " + e.getMessage());
      } catch (Exception e) {
        throw new HiveException("Fail to grant sys privileges to the user: "
            + userName + " " + e.getMessage());
      }
    } else {
      throw new HiveException(
          "Only the DBA users have the privilege to grant sys privileges for the user!");
    }
  }

  private boolean isDBAUser(String userName) throws HiveException {
    User user = getUser(HiveMetaStore.THE_SYSTEM, userName);

    if (user == null) {
      return false;
    }

    boolean isDBA = user.isDbaPriv();
    if (isDBA) {
      return isDBA;
    } else {
      List<String> playRoles = user.getPlayRoles();
      for (String r : playRoles) {
        isDBA = isDBA || isDBARole(r);
      }
      return isDBA;
    }
  }

  private boolean isDBARole(String rolename) throws HiveException {
    Role role = getRole(HiveMetaStore.THE_SYSTEM, rolename);
    if (role == null) {
      return false;
    }

    boolean isDBA = role.isDbaPriv();
    if (isDBA) {
      return isDBA;
    } else {
      List<String> playRoles = role.getPlayRoles();
      if (playRoles != null && !playRoles.isEmpty()) {
        for (String r : playRoles) {
          isDBA = isDBA || isDBARole(r);
        }
      }
      return isDBA;
    }
  }

  public boolean grantRoleToUser(String byWho, List<String> users,
      List<String> roles) throws HiveException {
    boolean success = true;
    byWho = byWho.trim().toLowerCase();

    for (String user : users) {
      user = user.trim().toLowerCase();
      if (isAUser(user)) {
        success &= grantRoleToUser(byWho, user, roles);
      } else if (isARole(user)) {
        throw new HiveException("Can not grant role to a role now");
      } else {
        throw new HiveException(
            "Can not grant role to a user or role not exist!");
      }
    }
    return success;
  }

  private boolean grantRoleToUser(String byWho, String userName,
      List<String> roles) throws HiveException {
    if (userName.equals(HiveMetaStore.ROOT_USER)) {
      throw new HiveException(
          "You have no privilege to grant roles for the ROOT user!");
    }

    boolean byDBA = isDBAUser(byWho);
    boolean forDBA = isDBAUser(userName);
    boolean grantDBA = false;
    List<String> rs = new ArrayList<String>();

    for (String r : roles) {
      r = r.trim().toLowerCase();
      grantDBA = grantDBA || isDBARole(r);
      rs.add(r);
    }

    if (forDBA || grantDBA) {
      if (byWho.equals(HiveMetaStore.ROOT_USER)) {
        try {
          return getMSC().grantRoleToUser(byWho, userName, rs);
        } catch (NoSuchObjectException e) {
          throw new HiveException("Fail to grant roles to the user: "
              + userName + ", msg=" + e.getMessage());
        } catch (InvalidObjectException e) {
          throw new HiveException("Fail to grant roles to the user: "
              + userName + ", msg=" + e.getMessage());
        } catch (Exception e) {
          throw new HiveException("Fail to grant roles to the user: "
              + userName + ", msg=" + e.getMessage());
        }
      } else {
        throw new HiveException(
            "Only the ROOT user has the privilege to grant roles for the DBA user OR grant DBA role to the user!");
      }
    }

    if (byDBA) {
      try {
        return getMSC().grantRoleToUser(byWho, userName, rs);
      } catch (NoSuchObjectException e) {
        throw new HiveException("Fail to grant roles to the user: " + userName
            + "! The user does not exist!");
      } catch (InvalidObjectException e) {
        throw new HiveException("Fail to grant roles to the user: " + userName
            + "! Some role does not exist!");
      } catch (Exception e) {
        throw new HiveException("Fail to grant roles to the user: " + userName
            + "! Please check the log.");
      }
    } else {
      throw new HiveException(
          "Only the DBAs have the privilege to grant roles for the user!");
    }
  }

  public boolean revokeAuth(String byWho, String who, List<String> privileges,
      String db, String table) throws HiveException {
    if (db == null || db.equals("*")) {
      if (!(table == null || table.equals("*"))) {
        throw new HiveException("Can not revoke privileges in this format!");
      }

      if (isAUser(who)) {
        return revokeAuthSys(byWho, who, privileges);
      } else if (isARole(who)) {
        return revokeAuthRoleSys(byWho, who, privileges);
      } else {
        throw new HiveException(
            "Can not revoke sys privileges from user or role which doesn't exist!");
      }
    } else if (table == null || table.equals("*")) {
      return revokeAuthOnDb(byWho, who, privileges, db);
    } else {
      return revokeAuthOnTbl(byWho, who, privileges, db, table);
    }
  }

  private boolean revokeAuthSys(String byWho, String userName,
      List<String> privileges) throws HiveException {
    byWho = byWho.trim().toLowerCase();
    userName = userName.trim().toLowerCase();
    if (userName.equals(HiveMetaStore.ROOT_USER)) {
      throw new HiveException(
          "You have no privilege to revoke sys privileges from the ROOT user!");
    }

    boolean byDBA = isDBAUser(byWho);
    boolean forDBA = isDBAUser(userName);
    List<String> privs = new ArrayList<String>();

    for (String p : privileges) {
      privs.add(p.trim().toUpperCase());
    }

    if (forDBA) {
      if (byWho.equals(HiveMetaStore.ROOT_USER)) {
        try {
          return getMSC().revokeAuthSys(byWho, userName, privs);
        } catch (NoSuchObjectException e) {
          throw new HiveException(
              "Fail to revoke sys privileges from the user: " + userName
                  + "! msg=" + e.getMessage());
        } catch (InvalidObjectException e) {
          throw new HiveException(
              "Fail to revoke sys privileges from the user: " + userName
                  + "! msg=" + e.getMessage());
        } catch (Exception e) {
          throw new HiveException(
              "Fail to revoke sys privileges from the user: " + userName
                  + "! msg=" + e.getMessage());
        }
      } else {
        throw new HiveException(
            "Onlu the ROOT user has the privilege to revoke sys privileges from the DBA user!");
      }
    }

    if (byDBA) {
      try {
        return getMSC().revokeAuthSys(byWho, userName, privs);
      } catch (NoSuchObjectException e) {
        throw new HiveException("Fail to revoke sys privileges from the user: "
            + userName + "! msg=" + e.getMessage());
      } catch (InvalidObjectException e) {
        throw new HiveException("Fail to revoke sys privileges from the user: "
            + userName + "! msg=" + e.getMessage());
      } catch (Exception e) {
        throw new HiveException("Fail to revoke sys privileges from the user: "
            + userName + "! msg=" + e.getMessage());
      }
    } else {
      throw new HiveException(
          "Only DBAs have the privilege to revoke sys privileges from the user!");
    }
  }

  public boolean revokeRoleFromUser(String byWho, List<String> users,
      List<String> roles) throws HiveException {
    boolean success = true;
    byWho = byWho.trim().toLowerCase();

    for (String user : users) {
      user = user.trim().toLowerCase();
      if (isAUser(user)) {
        success &= revokeRoleFromUser(byWho, user, roles);
      } else if (isARole(user)) {
        throw new HiveException("Can not revoke role from a role now");
      } else {
        throw new HiveException(
            "Can not revoke role from a user or role not exist!");
      }
    }
    return success;
  }

  private boolean revokeRoleFromUser(String byWho, String userName,
      List<String> roles) throws HiveException {
    if (userName.equals(HiveMetaStore.ROOT_USER)) {
      throw new HiveException(
          "You have no privilege to revoke roles from the ROOT user!");
    }

    boolean byDBA = isDBAUser(byWho);
    boolean forDBA = isDBAUser(userName);
    List<String> rs = new ArrayList<String>();

    for (String r : roles) {
      r = r.trim().toLowerCase();
      rs.add(r);
    }

    if (forDBA) {
      if (byWho.equals(HiveMetaStore.ROOT_USER)) {
        try {
          return getMSC().revokeRoleFromUser(byWho, userName, rs);
        } catch (NoSuchObjectException e) {
          throw new HiveException("Fail to revoke roles from the user: "
              + userName + "! msg=" + e.getMessage());
        } catch (InvalidObjectException e) {
          throw new HiveException("Fail to revoke roles from the user: "
              + userName + "! msg=" + e.getMessage());
        } catch (Exception e) {
          throw new HiveException("Fail to revoke roles from the user: "
              + userName + "! msg=" + e.getMessage());
        }
      } else {
        throw new HiveException(
            "Only the ROOT user has the privilege to revoke roles from the DBA user!");
      }
    }

    if (byDBA) {
      try {
        return getMSC().revokeRoleFromUser(byWho, userName, rs);
      } catch (NoSuchObjectException e) {
        throw new HiveException("Fail to revoke roles from the user: "
            + userName + "! msg=" + e.getMessage());
      } catch (InvalidObjectException e) {
        throw new HiveException("Fail to revoke roles from the user: "
            + userName + "! msg=" + e.getMessage());
      } catch (Exception e) {
        throw new HiveException("Fail to revoke roles from the user: "
            + userName + "! msg=" + e.getMessage());
      }
    } else {
      throw new HiveException(
          "Only DBAs have the privilege to revoke roles from the user!");
    }
  }

  public List<String> showGrants(String byWho, String who) throws HiveException {
    byWho = byWho.trim().toLowerCase();
    who = who.trim().toLowerCase();
    List<String> grants = new ArrayList<String>();
    String grant = "";
    List<String> roles = null;
    List<String> privs = new ArrayList<String>();
    int num = 0;

    if (isAUser(who)) {
      grant += "User name: " + who;
      grants.add(grant);
      grant = "Play roles: ";
      User user = getUser(byWho, who);
      roles = user.getPlayRoles();

      if (!roles.isEmpty()) {
        for (String role : roles) {
          grant += role + " ";
        }
      }

      grants.add(grant);
      grants.add("Privileges:");
      grant = "GRANT ";

      if (user.isSelectPriv()) {
        privs.add("SELECT");
        num++;
      }

      if (user.isAlterPriv()) {
        privs.add("ALTER");
        num++;
      }

      if (user.isCreatePriv()) {
        privs.add("CREATE");
        num++;
      }

      if (user.isUpdatePriv()) {
        privs.add("UPDATE");
        num++;
      }

      if (user.isDeletePriv()) {
        privs.add("DELETE");
        num++;
      }

      if (user.isDropPriv()) {
        privs.add("DROP");
        num++;
      }

      if (user.isInsertPriv()) {
        privs.add("INSERT");
        num++;
      }

      if (user.isIndexPriv()) {
        privs.add("INDEX");
        num++;
      }

      if (user.isCreateviewPriv()) {
        privs.add("CREATE_VIEW");
        num++;
      }

      if (user.isShowviewPriv()) {
        privs.add("SHOW_VIEW");
        num++;
      }

      if (user.isDbaPriv()) {
        privs.add("DBA");
        num++;
      }

      if (num >= 1) {
        if (num > 1) {
          for (int i = 1; i < num; i++) {
            grant += privs.get(i - 1) + ", ";
          }
        }

        grant += privs.get(num - 1) + " ON *.*";
        grants.add(grant);
      }

      grant = "";
    } else if (isARole(who)) {
      grant += "Role name: " + who;
      grants.add(grant);
      grant = "Play roles: ";
      Role role = getRole(byWho, who);
      roles = role.getPlayRoles();

      if (roles != null && !roles.isEmpty()) {
        for (String r : roles) {
          grant += r + " ";
        }
      }

      grants.add(grant);
      grants.add("Privileges:");
      grant = "GRANT ";

      if (role.isSelectPriv()) {
        privs.add("SELECT");
        num++;
      }

      if (role.isAlterPriv()) {
        privs.add("ALTER");
        num++;
      }

      if (role.isCreatePriv()) {
        privs.add("CREATE");
        num++;
      }

      if (role.isUpdatePriv()) {
        privs.add("UPDATE");
        num++;
      }

      if (role.isDeletePriv()) {
        privs.add("DELETE");
        num++;
      }

      if (role.isDropPriv()) {
        privs.add("DROP");
        num++;
      }

      if (role.isInsertPriv()) {
        privs.add("INSERT");
        num++;
      }

      if (role.isIndexPriv()) {
        privs.add("INDEX");
        num++;
      }

      if (role.isCreateviewPriv()) {
        privs.add("CREATE_VIEW");
        num++;
      }

      if (role.isShowviewPriv()) {
        privs.add("SHOW_VIEW");
        num++;
      }

      if (role.isDbaPriv()) {
        privs.add("DBA");
        num++;
      }

      if (num >= 1) {
        if (num > 1) {
          for (int i = 1; i < num; i++) {
            grant += privs.get(i - 1) + ", ";
          }
        }

        grant += privs.get(num - 1) + " ON *.*";
        grants.add(grant);
      }
      grant = "";
    } else {
      throw new HiveException("The user or role doesn't exist!");
    }

    List<DbPriv> dbps = getAuthOnDbs(byWho, who);
    if (dbps != null) {
      for (DbPriv dbp : dbps) {
        grant = "GRANT ";
        privs.clear();
        num = 0;

        if (dbp.isSelectPriv()) {
          privs.add("SELECT");
          num++;
        }

        if (dbp.isAlterPriv()) {
          privs.add("ALTER");
          num++;
        }

        if (dbp.isCreatePriv()) {
          privs.add("CREATE");
          num++;
        }

        if (dbp.isUpdatePriv()) {
          privs.add("UPDATE");
          num++;
        }

        if (dbp.isDeletePriv()) {
          privs.add("DELETE");
          num++;
        }

        if (dbp.isDropPriv()) {
          privs.add("DROP");
          num++;
        }

        if (dbp.isInsertPriv()) {
          privs.add("INSERT");
          num++;
        }

        if (dbp.isIndexPriv()) {
          privs.add("INDEX");
          num++;
        }

        if (dbp.isCreateviewPriv()) {
          privs.add("CREATE_VIEW");
          num++;
        }

        if (dbp.isShowviewPriv()) {
          privs.add("SHOW_VIEW");
          num++;
        }

        if (num >= 1) {
          for (int i = 1; i < num; i++) {
            grant += privs.get(i - 1) + ", ";
          }

          grant += privs.get(num - 1) + " ON " + dbp.getDb() + ".*";
          grants.add(grant);
        }

        grant = "";
      }
    }
    List<TblPriv> tblps = getAuthOnTbls(byWho, who);
    if (tblps != null) {
      for (TblPriv tblp : tblps) {
        grant = "GRANT ";
        privs.clear();
        num = 0;

        if (tblp.isSelectPriv()) {
          privs.add("SELECT");
          num++;
        }

        if (tblp.isAlterPriv()) {
          privs.add("ALTER");
          num++;
        }

        if (tblp.isCreatePriv()) {
          privs.add("CREATE");
          num++;
        }

        if (tblp.isUpdatePriv()) {
          privs.add("UPDATE");
          num++;
        }

        if (tblp.isDeletePriv()) {
          privs.add("DELETE");
          num++;
        }

        if (tblp.isDropPriv()) {
          privs.add("DROP");
          num++;
        }

        if (tblp.isInsertPriv()) {
          privs.add("INSERT");
          num++;
        }

        if (tblp.isIndexPriv()) {
          privs.add("INDEX");
          num++;
        }

        if (num >= 1) {
          for (int i = 1; i < num; i++) {
            grant += privs.get(i - 1) + ", ";
          }

          grant += privs.get(num - 1) + " ON " + tblp.getDb() + "."
              + tblp.getTbl();
          grants.add(grant);
        }
        grant = "";
      }

    }
    return grants;
  }

  private boolean isARole(String roleName) throws HiveException {
    roleName = roleName.trim().toLowerCase();
    try {
      return getMSC().isARole(roleName);
    } catch (Exception e) {
      throw new HiveException(" Failed to judge the role: " + roleName
          + " Error: " + e.toString());
    }
  }

  public boolean createRoles(String byWho, List<String> roles)
      throws HiveException {
    boolean success = true;
    for (String role : roles) {
      success &= createRole(byWho, role);
    }
    return success;
  }

  private boolean createRole(String byWho, String roleName)
      throws HiveException {
    byWho = byWho.trim().toLowerCase();
    roleName = roleName.trim().toLowerCase();

    if (roleName.length() > 128 || roleName.length() < 1) {
      throw new HiveException(
          "Length of the new role's name must between 1 and 128!");
    }

    if (roleName.equals(HiveMetaStore.ROOT_USER)) {
      throw new HiveException("No one can create the ROOT role!");
    }

    boolean byDBA = isDBAUser(byWho);
    if (byDBA) {
      try {
        return getMSC().createRole(byWho, roleName);
      } catch (AlreadyExistsException e1) {
        throw new HiveException("The role already exists!");
      } catch (TException e1) {
        throw new HiveException("Fail to create the new role: " + roleName
            + ", msg=" + e1.getMessage());
      } catch (MetaException e1) {
        throw new HiveException("Fail to create the new role: " + roleName
            + ", msg=" + e1.getMessage());
      }
    } else {
      throw new HiveException(
          "Only DBAs have the privilege to create a new role!");
    }
  }

  public boolean dropRoles(String byWho, List<String> roles)
      throws HiveException {
    boolean success = true;
    for (String role : roles) {
      success &= dropRole(byWho, role);
    }
    return success;
  }

  private boolean dropRole(String byWho, String roleName) throws HiveException {
    byWho = byWho.trim().toLowerCase();
    roleName = roleName.trim().toLowerCase();

    boolean byRoot = byWho.equals(HiveMetaStore.ROOT_USER);
    boolean byDBA = isDBAUser(byWho);
    boolean dropDBA = isDBARole(roleName);

    if (dropDBA) {
      if (byRoot) {
        try {
          return getMSC().dropRole(byWho, roleName);
        } catch (NoSuchObjectException e) {
          throw new HiveException("Fail to drop the role: " + roleName
              + "! The role does not exist!");
        } catch (Exception e) {
          throw new HiveException("Fail to drop the role: " + roleName
              + ", msg=" + e.getMessage());
        }
      } else {
        throw new HiveException("Only the ROOT user can drop a DBA role!");
      }
    }

    if (byDBA) {
      try {
        return getMSC().dropRole(byWho, roleName);
      } catch (NoSuchObjectException e) {
        throw new HiveException("Fail to drop the role: " + roleName
            + "! The role does not exist!");
      } catch (Exception e) {
        throw new HiveException("Fail to drop the role: " + roleName + ", msg="
            + e.getMessage());
      }
    } else {
      throw new HiveException("Only DBAs can drop roles!");
    }
  }

  private boolean hasSonRole(String rolename) throws HiveException {
    Role role = getRole(HiveMetaStore.THE_SYSTEM, rolename);

    if (role.getPlayRoles().isEmpty()) {
      return false;
    } else {
      return true;
    }
  }

  private boolean grantRoleToRole(String byWho, String roleName,
      List<String> roles) throws HiveException {
    boolean byDBA = isDBAUser(byWho);
    boolean forDBA = isDBARole(roleName);
    boolean grantDBA = false;
    List<String> rs = new ArrayList<String>();

    for (String r : roles) {
      r = r.trim().toLowerCase();
      if (r.equals(roleName)) {
        throw new HiveException("Can not grant a role to himself!");
      }
      if (hasSonRole(r)) {
        throw new HiveException(
            "Can not grant a role, which has son roles, to this role!");
      }
      grantDBA = grantDBA || isDBARole(r);
      rs.add(r);
    }

    if (forDBA || grantDBA) {
      if (byWho.equals(HiveMetaStore.ROOT_USER)) {
        try {
          return getMSC().grantRoleToRole(byWho, roleName, rs);
        } catch (NoSuchObjectException e) {
          throw new HiveException("Fail to grant roles to the role: "
              + roleName + ", msg=" + e.getMessage());
        } catch (InvalidObjectException e) {
          throw new HiveException("Fail to grant roles to the role: "
              + roleName + ", msg=" + e.getMessage());
        } catch (Exception e) {
          throw new HiveException("Fail to grant roles to the role: "
              + roleName + ", msg=" + e.getMessage());
        }
      } else {
        throw new HiveException(
            "Only the ROOT user has the privilege to grant roles for the DBA role OR grant DBA role to the role!");
      }
    }

    if (byDBA) {
      try {
        return getMSC().grantRoleToRole(byWho, roleName, rs);
      } catch (NoSuchObjectException e) {
        throw new HiveException("Fail to grant roles to the role: " + roleName
            + ", msg=" + e.getMessage());
      } catch (InvalidObjectException e) {
        throw new HiveException("Fail to grant roles to the role: " + roleName
            + ", msg=" + e.getMessage());
      } catch (Exception e) {
        throw new HiveException("Fail to grant roles to the role: " + roleName
            + ", msg=" + e.getMessage());
      }
    } else {
      throw new HiveException(
          "Only DBAs have the privilege to grant roles for the role!");
    }
  }

  private boolean revokeRoleFromRole(String byWho, String roleName,
      List<String> roles) throws HiveException {
    boolean byDBA = isDBAUser(byWho);
    boolean forDBA = isDBARole(roleName);
    boolean revokeDBA = false;

    List<String> rs = new ArrayList<String>();

    for (String r : roles) {
      r = r.trim().toLowerCase();
      revokeDBA = revokeDBA || isDBARole(r);
      rs.add(r);
    }

    if (forDBA || revokeDBA) {
      if (byWho.equals(HiveMetaStore.ROOT_USER)) {
        try {
          return getMSC().revokeRoleFromRole(byWho, roleName, rs);
        } catch (NoSuchObjectException e) {
          throw new HiveException("Fail to revoke roles from the role: "
              + roleName + ", msg=" + e.getMessage());
        } catch (InvalidObjectException e) {
          throw new HiveException("Fail to revoke roles from the role: "
              + roleName + ", msg=" + e.getMessage());
        } catch (Exception e) {
          throw new HiveException("Fail to revoke roles from the role: "
              + roleName + ", msg=" + e.getMessage());
        }
      } else {
        throw new HiveException(
            "Only the ROOT user has the privilege to revoke roles from the DBA role or revoke DBA role from the role!");
      }
    }

    if (byDBA) {
      try {
        return getMSC().revokeRoleFromRole(byWho, roleName, rs);
      } catch (NoSuchObjectException e) {
        throw new HiveException("Fail to revoke roles from the role: "
            + roleName + ", msg=" + e.getMessage());
      } catch (InvalidObjectException e) {
        throw new HiveException("Fail to revoke roles from the role: "
            + roleName + ", msg=" + e.getMessage());
      } catch (Exception e) {
        throw new HiveException("Fail to revoke roles from the role: "
            + roleName + ", msg=" + e.getMessage());
      }
    } else {
      throw new HiveException(
          "Only DBAs have the privilege to revoke roles from the role!");
    }
  }

  private Role getRole(String byWho, String roleName) throws HiveException {
    byWho = byWho.trim().toLowerCase();
    roleName = roleName.trim().toLowerCase();
    try {
      return getMSC().getRole(byWho, roleName);
    } catch (NoSuchObjectException e) {
      throw new HiveException("Fail to get the role: " + roleName
          + "! The role does not exist!");
    } catch (Exception e) {
      throw new HiveException("Fail to get the role: " + roleName + ", msg="
          + e.getMessage());
    }
  }

  public List<String> getPlayRoles(String who) throws HiveException {
    try {
      return getMSC().getPlayRoles(who);
    } catch (NoSuchObjectException e) {
      throw new HiveException("Fail to get the user play role: " + who
          + "! The role does not exist!");
    } catch (Exception e) {
      throw new HiveException("Fail to get the user play role: " + who
          + ", msg=" + e.getMessage());
    }
  }

  public List<String> showRoles(String byWho) throws HiveException {
    byWho = byWho.trim().toLowerCase();
    try {
      return getMSC().getRolesAll(byWho);
    } catch (Exception e) {
      throw new HiveException("Failed to get all the roles' names, msg="
          + e.getMessage());
    }
  }

  private boolean grantAuthRoleSys(String byWho, String role,
      List<String> privileges) throws HiveException {
    byWho = byWho.trim().toLowerCase();
    role = role.trim().toLowerCase();

    boolean byDBA = isDBAUser(byWho);
    boolean forDBA = isDBARole(role);
    List<String> privs = new ArrayList<String>();

    for (String priv : privileges) {
      priv = priv.trim().toUpperCase();
      if (priv.equals("TOK_DBA_PRI")) {
        forDBA = true;
      }
      privs.add(priv);
    }

    if (forDBA) {
      if (byWho.equals(HiveMetaStore.ROOT_USER)) {
        try {
          return getMSC().grantAuthRoleSys(byWho, role, privs);
        } catch (NoSuchObjectException e) {
          throw new HiveException("Fail to grant sys privileges to the role: "
              + role + " " + e.getMessage());
        } catch (InvalidObjectException e) {
          throw new HiveException("Fail to grant sys privileges to the role: "
              + role + " " + e.getMessage());
        } catch (Exception e) {
          throw new HiveException("Fail to grant sys privileges to the role: "
              + role + " " + e.getMessage());
        }
      } else {
        throw new HiveException(
            "Only the ROOT user has the privilege to grant sys privileges for the DBA role or grant DBA privilege to the user!");
      }
    }

    if (byDBA) {
      try {
        return getMSC().grantAuthRoleSys(byWho, role, privs);
      } catch (NoSuchObjectException e) {
        throw new HiveException("Fail to grant sys privileges to the role: "
            + role + " " + e.getMessage());
      } catch (InvalidObjectException e) {
        throw new HiveException("Fail to grant sys privileges to the role: "
            + role + " " + e.getMessage());
      } catch (Exception e) {
        throw new HiveException("Fail to grant sys privileges to the role: "
            + role + " " + e.getMessage());
      }
    } else {
      throw new HiveException(
          "Only DBAs have the privilege to grant sys privileges for the role!");
    }
  }

  private boolean revokeAuthRoleSys(String byWho, String role,
      List<String> privileges) throws HiveException {
    byWho = byWho.trim().toLowerCase();
    role = role.trim().toLowerCase();

    boolean byDBA = isDBAUser(byWho);
    boolean forDBA = isDBARole(role);

    List<String> privs = new ArrayList<String>();

    for (String priv : privileges) {
      priv = priv.trim().toUpperCase();
      privs.add(priv);
    }

    if (forDBA) {
      if (byWho.equals(HiveMetaStore.ROOT_USER)) {
        try {
          return getMSC().revokeAuthRoleSys(byWho, role, privs);
        } catch (NoSuchObjectException e) {
          throw new HiveException(
              "Fail to revoke sys privileges from the role: " + role + ", msg="
                  + e.getMessage());
        } catch (InvalidObjectException e) {
          throw new HiveException(
              "Fail to revoke sys privileges from the role: " + role + ", msg="
                  + e.getMessage());
        } catch (Exception e) {
          throw new HiveException(
              "Fail to revoke sys privileges from the role: " + role + ", msg="
                  + e.getMessage());
        }
      } else {
        throw new HiveException(
            "Only the ROOT user has the privilege to revoke sys privileges from the DBA role!");
      }
    }

    if (byDBA) {
      try {
        return getMSC().revokeAuthRoleSys(byWho, role, privs);
      } catch (NoSuchObjectException e) {
        throw new HiveException("Fail to revoke sys privileges from the role: "
            + role + ", msg=" + e.getMessage());
      } catch (InvalidObjectException e) {
        throw new HiveException("Fail to revoke sys privileges from the role: "
            + role + ", msg=" + e.getMessage());
      } catch (Exception e) {
        throw new HiveException("Fail to revoke sys privileges from the role: "
            + role + ", msg=" + e.getMessage());
      }
    } else {
      throw new HiveException(
          "Only DBAs have the privilege to revoke sys privileges from the role!");
    }
  }

  private boolean grantAuthOnDb(String byWho, String forWho,
      List<String> privileges) throws HiveException {
    return grantAuthOnDb(byWho, forWho, privileges,
        MetaStoreUtils.DEFAULT_DATABASE_NAME);
  }

  private boolean grantAuthOnDb(String byWho, String forWho,
      List<String> privileges, String db) throws HiveException {
    byWho = byWho.trim().toLowerCase();
    forWho = forWho.trim().toLowerCase();
    db = db.trim().toLowerCase();

    if (forWho.equals(HiveMetaStore.ROOT_USER)) {
      throw new HiveException(
          "You have no privilege to grant privileges for the ROOT user!");
    }

    boolean byDBA = isDBAUser(byWho);
    boolean forDBA = false;
    if (isAUser(forWho)) {
      forDBA = isDBAUser(forWho);
    } else if (isARole(forWho)) {
      forDBA = isDBARole(forWho);
    } else {
      throw new HiveException("Fail to grant privileges on db: " + db
          + " to the user/role: " + forWho + "! The user/role does not exist!");
    }

    List<String> privs = new ArrayList<String>();
    for (String priv : privileges) {
      priv = priv.trim().toUpperCase();
      privs.add(priv);
    }

    if (forDBA) {
      if (byWho.equals(HiveMetaStore.ROOT_USER)) {
        try {
          return getMSC().grantAuthOnDb(byWho, forWho, privs, db);
        } catch (NoSuchObjectException e) {
          throw new HiveException("Fail to grant privileges on db: " + db
              + " to the user/role: " + forWho + ", msg=" + e.getMessage());
        } catch (InvalidObjectException e) {
          throw new HiveException("Fail to grant privileges on db: " + db
              + " to the user/role: " + forWho + ", msg=" + e.getMessage());
        } catch (Exception e) {
          throw new HiveException("Fail to grant privileges on db: " + db
              + " to the user/role: " + forWho + ", msg=" + e.getMessage());
        }
      } else {
        throw new HiveException(
            "Only the ROOT user has the privilege to grant db privileges for the DBA user/role!");
      }
    }

    if (byDBA) {
      try {
        return getMSC().grantAuthOnDb(byWho, forWho, privs, db);
      } catch (NoSuchObjectException e) {
        throw new HiveException("Fail to grant privileges on db: " + db
            + " to the user/role: " + forWho + ", msg=" + e.getMessage());
      } catch (InvalidObjectException e) {
        throw new HiveException("Fail to grant privileges on db: " + db
            + " to the user/role: " + forWho + ", msg=" + e.getMessage());
      } catch (Exception e) {
        throw new HiveException("Fail to grant privileges on db: " + db
            + " to the user/role: " + forWho + ", msg=" + e.getMessage());
      }
    } else {
      throw new HiveException(
          "Only DBAs have the privilege to grant db privileges for the user/role!");
    }
  }

  private DbPriv getAuthOnDb(String byWho, String forWho) throws HiveException {
    return getAuthOnDb(byWho, forWho, MetaStoreUtils.DEFAULT_DATABASE_NAME);
  }

  private DbPriv getAuthOnDb(String byWho, String forWho, String db)
      throws HiveException {
    byWho = byWho.trim().toLowerCase();
    forWho = forWho.trim().toLowerCase();
    db = db.trim().toLowerCase();
    try {
      return getMSC().getAuthOnDb(byWho, forWho, db);
    } catch (NoSuchObjectException e) {
      throw new HiveException("Fail to get the privileges on db: " + db
          + " for the user/role: " + forWho + "! The user/role does not exist!");
    } catch (Exception e) {
      throw new HiveException("Fail to get the privileges on db: " + db
          + " for the user/role: " + forWho + ", msg="
          + org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
  }

  private List<DbPriv> getAuthOnDbs(String byWho, String forWho)
      throws HiveException {
    byWho = byWho.trim().toLowerCase();
    forWho = forWho.trim().toLowerCase();
    try {
      return getMSC().getAuthOnDbs(byWho, forWho);
    } catch (NoSuchObjectException e) {
      throw new HiveException(
          "Fail to get all the db privileges for the user/role: " + forWho
              + "! The user/role does not exist!");
    } catch (Exception e) {
      throw new HiveException(
          "Fail to get all the db privileges for the user/role: " + forWho
              + ", msg="
              + org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
  }

  private List<DbPriv> getDbAuth(String byWho) throws HiveException {
    return getDbAuth(byWho, MetaStoreUtils.DEFAULT_DATABASE_NAME);
  }

  private List<DbPriv> getDbAuth(String byWho, String db) throws HiveException {
    byWho = byWho.trim().toLowerCase();
    db = db.trim().toLowerCase();

    try {
      return getMSC().getDbAuth(byWho, db);
    } catch (NoSuchObjectException e) {
      throw new HiveException("Fail to get the privileges on db: " + db
          + " ! The db doesn't exist!");
    } catch (Exception e) {
      throw new HiveException("Fail to get the privileges on db: " + db
          + ", msg=" + e.getMessage());
    }
  }

  private List<DbPriv> getDbAuthAll(String byWho) throws HiveException {
    byWho = byWho.trim().toLowerCase();

    try {
      return getMSC().getDbAuthAll(byWho);
    } catch (Exception e) {
      throw new HiveException("Fail to get all the privileges on dbs, msg="
          + e.getMessage());
    }
  }

  private boolean revokeAuthOnDb(String byWho, String forWho,
      List<String> privileges) throws HiveException {
    return revokeAuthOnDb(byWho, forWho, privileges,
        MetaStoreUtils.DEFAULT_DATABASE_NAME);
  }

  private boolean revokeAuthOnDb(String byWho, String forWho,
      List<String> privileges, String db) throws HiveException {
    byWho = byWho.trim().toLowerCase();
    forWho = forWho.trim().toLowerCase();
    db = db.trim().toLowerCase();

    if (forWho.equals(HiveMetaStore.ROOT_USER)) {
      throw new HiveException(
          "You have no privilege to revoke privileges from the ROOT user!");
    }

    boolean byDBA = isDBAUser(byWho);
    boolean forDBA = false;
    if (isAUser(forWho)) {
      forDBA = isDBAUser(forWho);
    } else if (isARole(forWho)) {
      forDBA = isDBARole(forWho);
    } else {
      throw new HiveException("Fail to revoke privileges on db: " + db
          + " from the user/role: " + forWho
          + "! The user/role does not exist!");
    }

    List<String> privs = new ArrayList<String>();
    for (String priv : privileges) {
      priv = priv.trim().toUpperCase();
      privs.add(priv);
    }

    if (forDBA) {
      if (byWho.equals(HiveMetaStore.ROOT_USER)) {
        try {
          return getMSC().revokeAuthOnDb(byWho, forWho, privs, db);
        } catch (NoSuchObjectException e) {
          throw new HiveException("Fail to revoke privileges on db: " + db
              + " from the user/role: " + forWho + ", msg=" + e.getMessage());
        } catch (InvalidObjectException e) {
          throw new HiveException("Fail to revoke privileges on db: " + db
              + " from the user/role: " + forWho + ", msg=" + e.getMessage());
        } catch (Exception e) {
          throw new HiveException("Fail to revoke privileges on db: " + db
              + " from the user/role: " + forWho + ", msg=" + e.getMessage());
        }
      } else {
        throw new HiveException(
            "Only the ROOT user has the privilege to revoke db privileges from the DBA user/role!");
      }
    }

    if (byDBA) {
      try {
        return getMSC().revokeAuthOnDb(byWho, forWho, privs, db);
      } catch (NoSuchObjectException e) {
        throw new HiveException("Fail to revoke privileges on db: " + db
            + " from the user/role: " + forWho + ", msg=" + e.getMessage());
      } catch (InvalidObjectException e) {
        throw new HiveException("Fail to revoke privileges on db: " + db
            + " from the user/role: " + forWho + ", msg=" + e.getMessage());

      } catch (Exception e) {
        throw new HiveException("Fail to revoke privileges on db: " + db
            + " from the user/role: " + forWho + ", msg=" + e.getMessage());
      }
    } else {
      throw new HiveException(
          "Only DBAs have the privilege to revoke db privileges from the user/role!");
    }
  }

  private boolean dropAuthOnDb(String byWho, String forWho)
      throws HiveException {
    return dropAuthOnDb(byWho, forWho, MetaStoreUtils.DEFAULT_DATABASE_NAME);
  }

  private boolean dropAuthOnDb(String byWho, String forWho, String db)
      throws HiveException {
    byWho = byWho.trim().toLowerCase();
    forWho = forWho.trim().toLowerCase();
    db = db.trim().toLowerCase();

    if (forWho.equals(HiveMetaStore.ROOT_USER)) {
      throw new HiveException(
          "You have no privilege to drop privileges for the ROOT user!");
    }

    boolean byDBA = isDBAUser(byWho);
    boolean forDBA = false;
    if (isAUser(forWho)) {
      forDBA = isDBAUser(forWho);
    } else if (isARole(forWho)) {
      forDBA = isDBARole(forWho);
    } else {
      throw new HiveException("Fail to drop privileges on db: " + db
          + " for the user/role: " + forWho + "! The user/role does not exist!");
    }

    if (forDBA) {
      if (byWho.equals(HiveMetaStore.ROOT_USER)) {
        try {

          return getMSC().dropAuthOnDb(byWho, forWho, db);
        } catch (Exception e) {
          throw new HiveException("Fail to drop privileges on db: " + db
              + " for the user/role: " + forWho + ", msg=" + e.getMessage());
        }
      } else {
        throw new HiveException(
            "Only the ROOT user has the privilege to drop db privileges for the DBA user/role!");
      }
    }

    if (byDBA) {
      try {
        return getMSC().dropAuthOnDb(byWho, forWho, db);

      } catch (Exception e) {
        throw new HiveException("Fail to drop privileges on db: " + db
            + " for the user/role: " + forWho + ", msg=" + e.getMessage());
      }
    } else {
      throw new HiveException(
          "Only DBAs have the privilege to drop db privileges for the user/role!");
    }
  }

  private boolean dropAuthInDb(String byWho, String forWho)
      throws HiveException {
    byWho = byWho.trim().toLowerCase();
    forWho = forWho.trim().toLowerCase();

    if (forWho.equals(HiveMetaStore.ROOT_USER)) {
      throw new HiveException(
          "You have no privilege to drop privileges for the ROOT user!");
    }

    boolean byDBA = isDBAUser(byWho);
    boolean forDBA = false;
    if (isAUser(forWho)) {
      forDBA = isDBAUser(forWho);
    } else if (isARole(forWho)) {
      forDBA = isDBARole(forWho);
    } else {
      throw new HiveException("Fail to drop db privileges for the user/role: "
          + forWho + "! The user/role does not exist!");
    }

    if (forDBA) {
      if (byWho.equals(HiveMetaStore.ROOT_USER)) {
        try {
          return getMSC().dropAuthInDb(byWho, forWho);
        } catch (Exception e) {
          throw new HiveException(
              "Fail to drop db privileges for the user/role: " + forWho
                  + ", msg=" + e.getMessage());
        }
      } else {
        throw new HiveException(
            "Only the ROOT user has the privilege to drop db privileges for the DBA user/role!");
      }
    }

    if (byDBA) {
      try {
        return getMSC().dropAuthInDb(byWho, forWho);
      } catch (Exception e) {
        throw new HiveException(
            "Fail to drop db privileges for the user/role: " + forWho
                + ", msg=" + e.getMessage());
      }
    } else {
      throw new HiveException(
          "Only DBAs have the privilege to drop db privileges for the user/role!");
    }
  }

  private boolean grantAuthOnTbl(String byWho, String forWho,
      List<String> privileges, String tbl) throws HiveException {
    return grantAuthOnTbl(byWho, forWho, privileges,
        MetaStoreUtils.DEFAULT_DATABASE_NAME, tbl);
  }

  private boolean grantAuthOnTbl(String byWho, String forWho,
      List<String> privileges, String db, String tbl) throws HiveException {
    byWho = byWho.trim().toLowerCase();
    forWho = forWho.trim().toLowerCase();
    db = db.trim().toLowerCase();
    tbl = tbl.trim().toLowerCase();

    if (forWho.equals(HiveMetaStore.ROOT_USER)) {
      throw new HiveException(
          "You have no privilege to grant privileges for the ROOT user!");
    }

    boolean byDBA = isDBAUser(byWho);
    boolean forDBA = false;
    if (isAUser(forWho)) {
      forDBA = isDBAUser(forWho);
    } else if (isARole(forWho)) {
      forDBA = isDBARole(forWho);
    } else {
      throw new HiveException("Fail to grant privileges on tbl: " + tbl
          + " in db: " + db + " to the user/role: " + forWho
          + "! The user/role does not exist!");
    }

    List<String> privs = new ArrayList<String>();
    for (String priv : privileges) {
      priv = priv.trim().toUpperCase();
      privs.add(priv);
    }

    if (forDBA) {
      if (byWho.equals(HiveMetaStore.ROOT_USER)) {
        try {
          return getMSC().grantAuthOnTbl(byWho, forWho, privs, db, tbl);
        } catch (NoSuchObjectException e) {
          throw new HiveException("Fail to grant privileges on tbl: " + tbl
              + " in db: " + db + " to the user/role: " + forWho + ", msg="
              + e.getMessage());
        } catch (InvalidObjectException e) {
          throw new HiveException("Fail to grant privileges on tbl: " + tbl
              + " in db: " + db + " to the user/role: " + forWho + ", msg="
              + e.getMessage());
        } catch (Exception e) {
          throw new HiveException("Fail to grant privileges on tbl: " + tbl
              + " in db: " + db + " to the user/role: " + forWho + ", msg="
              + e.getMessage());
        }
      } else {
        throw new HiveException(
            "Only the ROOT user has the privilege to grant table privileges for the DBA user/role!");
      }
    }

    if (byDBA) {
      try {
        return getMSC().grantAuthOnTbl(byWho, forWho, privs, db, tbl);
      } catch (NoSuchObjectException e) {
        throw new HiveException("Fail to grant privileges on tbl: " + tbl
            + " in db: " + db + " to the user/role: " + forWho + ", msg="
            + e.getMessage());
      } catch (InvalidObjectException e) {

        throw new HiveException("Fail to grant privileges on tbl: " + tbl
            + " in db: " + db + " to the user/role: " + forWho + ", msg="
            + e.getMessage());

      } catch (Exception e) {
        throw new HiveException("Fail to grant privileges on tbl: " + tbl
            + " in db: " + db + " to the user/role: " + forWho + ", msg="
            + e.getMessage());
      }
    } else {
      throw new HiveException(
          "Only DBAs have the privilege to grant table privileges for the user/role!");
    }
  }

  private TblPriv getAuthOnTbl(String byWho, String forWho, String tbl)
      throws HiveException {
    return getAuthOnTbl(byWho, forWho, MetaStoreUtils.DEFAULT_DATABASE_NAME,
        tbl);
  }

  private TblPriv getAuthOnTbl(String byWho, String forWho, String db,
      String tbl) throws HiveException {
    byWho = byWho.trim().toLowerCase();
    forWho = forWho.trim().toLowerCase();
    db = db.trim().toLowerCase();
    tbl = tbl.trim().toLowerCase();

    try {
      return getMSC().getAuthOnTbl(byWho, forWho, db, tbl);
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException("Fail to get the privileges on tbl: " + tbl
          + " in db: " + db + " for the user/role: " + forWho + ", msg="
          + e.getMessage());
    }
  }

  private List<TblPriv> getAuthOnTbls(String byWho, String forWho)
      throws HiveException {
    byWho = byWho.trim().toLowerCase();
    forWho = forWho.trim().toLowerCase();
    try {
      return getMSC().getAuthOnTbls(byWho, forWho);
    } catch (NoSuchObjectException e) {
      throw new HiveException(
          "Fail to get the privileges on all tbls for the user/role: " + forWho
              + "! The user/role does not exist!");
    } catch (Exception e) {
      throw new HiveException(
          "Fail to get the privileges on all tbls for the user/role: " + forWho
              + "!");
    }
  }

  private List<TblPriv> getTblAuth(String byWho, String tbl)
      throws HiveException {
    return getTblAuth(byWho, MetaStoreUtils.DEFAULT_DATABASE_NAME, tbl);
  }

  private List<TblPriv> getTblAuth(String byWho, String db, String tbl)
      throws HiveException {
    byWho = byWho.trim().toLowerCase();
    db = db.trim().toLowerCase();
    tbl = tbl.trim().toLowerCase();

    try {
      return getMSC().getTblAuth(byWho, db, tbl);
    } catch (Exception e) {
      throw new HiveException("Fail to get the privileges on tbl: " + tbl
          + " in db: " + db + ", msg=" + e.getMessage());
    }
  }

  private List<TblPriv> getTblAuthAll(String byWho) throws HiveException {
    byWho = byWho.trim().toLowerCase();
    try {
      return getMSC().getTblAuthAll(byWho);
    } catch (Exception e) {
      throw new HiveException("Fail to get all the privileges on tables, msg="
          + e.getMessage());
    }
  }

  private boolean revokeAuthOnTbl(String byWho, String forWho,
      List<String> privileges, String tbl) throws HiveException {
    return revokeAuthOnTbl(byWho, forWho, privileges,
        MetaStoreUtils.DEFAULT_DATABASE_NAME, tbl);
  }

  private boolean revokeAuthOnTbl(String byWho, String forWho,
      List<String> privileges, String db, String tbl) throws HiveException {
    byWho = byWho.trim().toLowerCase();
    forWho = forWho.trim().toLowerCase();
    db = db.trim().toLowerCase();
    tbl = tbl.trim().toLowerCase();

    if (forWho.equals(HiveMetaStore.ROOT_USER)) {
      throw new HiveException(
          "You have no privilege to revoke privileges from the ROOT user!");
    }

    boolean byDBA = isDBAUser(byWho);
    boolean forDBA = false;
    if (isAUser(forWho)) {
      forDBA = isDBAUser(forWho);
    } else if (isARole(forWho)) {
      forDBA = isDBARole(forWho);
    } else {
      throw new HiveException("Fail to revoke privileges on tbl: " + tbl
          + " in db: " + db + " from the user/role: " + forWho
          + "! The user/role does not exist!");
    }

    List<String> privs = new ArrayList<String>();
    for (String priv : privileges) {
      priv = priv.trim().toUpperCase();
      privs.add(priv);
    }

    if (forDBA) {
      if (byWho.equals(HiveMetaStore.ROOT_USER)) {
        try {
          return getMSC().revokeAuthOnTbl(byWho, forWho, privs, db, tbl);
        } catch (NoSuchObjectException e) {
          throw new HiveException("Fail to revoke privileges on tbl: " + tbl
              + " in db: " + db + " from the user/role: " + forWho + ", msg="
              + e.getMessage());
        } catch (InvalidObjectException e) {
          throw new HiveException("Fail to revoke privileges on tbl: " + tbl
              + " in db: " + db + " from the user/role: " + forWho + ", msg="
              + e.getMessage());
        } catch (Exception e) {
          throw new HiveException("Fail to revoke privileges on tbl: " + tbl
              + " in db: " + db + " from the user/role: " + forWho + ", msg="
              + e.getMessage());
        }
      } else {
        throw new HiveException(
            "Only the ROOT user has the privilege to revoke table privileges from the DBA user/role!");
      }
    }

    if (byDBA) {
      try {
        return getMSC().revokeAuthOnTbl(byWho, forWho, privs, db, tbl);
      } catch (NoSuchObjectException e) {
        throw new HiveException("Fail to revoke privileges on tbl: " + tbl
            + " in db: " + db + " from the user/role: " + forWho + ", msg="
            + e.getMessage());
      } catch (InvalidObjectException e) {
        throw new HiveException("Fail to revoke privileges on tbl: " + tbl
            + " in db: " + db + " from the user/role: " + forWho + ", msg="
            + e.getMessage());
      } catch (Exception e) {
        throw new HiveException("Fail to revoke privileges on tbl: " + tbl
            + " in db: " + db + " from the user/role: " + forWho + ", msg="
            + e.getMessage());
      }
    } else {
      throw new HiveException(
          "Only DBAs have the privilege to revoke table privileges from the user/role!");
    }
  }

  private boolean dropAuthOnTbl(String byWho, String forWho, String tbl)
      throws HiveException {
    return dropAuthOnTbl(byWho, forWho, MetaStoreUtils.DEFAULT_DATABASE_NAME,
        tbl);
  }

  private boolean dropAuthOnTbl(String byWho, String forWho, String db,
      String tbl) throws HiveException {
    byWho = byWho.trim().toLowerCase();
    forWho = forWho.trim().toLowerCase();
    db = db.trim().toLowerCase();
    tbl = tbl.trim().toLowerCase();

    if (forWho.equals(HiveMetaStore.ROOT_USER)) {
      throw new HiveException(
          "You have no privilege to drop privileges for the ROOT user!");
    }

    boolean byDBA = isDBAUser(byWho);
    boolean forDBA = false;
    if (isAUser(forWho)) {
      forDBA = isDBAUser(forWho);
    } else if (isARole(forWho)) {
      forDBA = isDBARole(forWho);
    } else {
      throw new HiveException("Fail to drop privileges on tbl: " + tbl
          + " in db: " + db + " for the user/role: " + forWho
          + "! The user/role does not exist!");
    }

    if (forDBA) {
      if (byWho.equals(HiveMetaStore.ROOT_USER)) {
        try {

          return getMSC().dropAuthOnTbl(byWho, forWho, db, tbl);
        } catch (Exception e) {
          throw new HiveException("Fail to drop privileges on tbl: " + tbl
              + " in db: " + db + " for the user/role: " + forWho + ", msg="
              + e.getMessage());
        }
      } else {
        throw new HiveException(
            "Only the ROOT user has the privilege to drop table privileges for the DBA user/role!");
      }
    }

    if (byDBA) {
      try {
        return getMSC().dropAuthOnTbl(byWho, forWho, db, tbl);
      } catch (Exception e) {
        throw new HiveException("Fail to drop privileges on tbl: " + tbl
            + " in db: " + db + " for the user/role: " + forWho + ", msg="
            + e.getMessage());
      }
    } else {
      throw new HiveException(
          "Only DBAs have the privilege to drop table privileges for the user/role!");
    }
  }

  private boolean dropAuthInTbl(String byWho, String forWho)
      throws HiveException {
    byWho = byWho.trim().toLowerCase();
    forWho = forWho.trim().toLowerCase();

    if (forWho.equals(HiveMetaStore.ROOT_USER)) {
      throw new HiveException(
          "You have no privilege to drop privileges for the ROOT user!");
    }

    boolean byDBA = isDBAUser(byWho);
    boolean forDBA = false;

    if (isAUser(forWho)) {
      forDBA = isDBAUser(forWho);
    } else if (isARole(forWho)) {
      forDBA = isDBARole(forWho);
    } else {
      throw new HiveException(
          "Fail to drop table privileges for the user/role: " + forWho
              + "! The user/role does not exist!");
    }

    if (forDBA) {
      if (byWho.equals(HiveMetaStore.ROOT_USER)) {
        try {
          return getMSC().dropAuthInTbl(byWho, forWho);
        } catch (Exception e) {
          throw new HiveException(
              "Fail to drop table privileges for the user/role: " + forWho
                  + ", msg=" + e.getMessage());
        }
      } else {
        throw new HiveException(
            "Only the ROOT user has the privilege to drop table privileges for the DBA user/role!");
      }
    }

    if (byDBA) {
      try {
        return getMSC().dropAuthInTbl(byWho, forWho);
      } catch (Exception e) {

        throw new HiveException(
            "Fail to drop table privileges for the user/role: " + forWho
                + ", msg=" + e.getMessage());

      }
    } else {
      throw new HiveException(
          "Only DBAs have the privilege to drop table privileges for the user/role!");
    }
  }

  public boolean hasAuth(String who, Privilege priv, String db, String table)
      throws HiveException {
    if (db == null) {
      return hasAuth(who, priv);
    } else if (table == null) {
      return hasAuthOnDb(who, priv, db);
    } else {
      return hasAuthOnTbl(who, priv, db, table);
    }
  }

  private boolean hasAuth(String who, Privilege priv) throws HiveException {
    who = who.trim().toLowerCase();
    if (who.equals(HiveMetaStore.ROOT_USER))
      return true;

    int privIndex = privMap.get(priv);

    HiveConf sessionConf = SessionState.get().getConf();
    String role = null;
    if (sessionConf == null) {
      role = null;
    } else {
      role = sessionConf.get("tdw.ugi.groupname");
    }

    if (role == null) {
      try {
        return getMSC().hasAuth(who, privIndex);
      } catch (NoSuchObjectException e) {
        throw new HiveException("The user doesn't exist!");
      } catch (MetaException e) {
        throw new HiveException("get priv error:" + e.getMessage());
      } catch (TException e) {
        throw new HiveException("get priv error:" + e.getMessage());
      }
    } else {
      boolean success;
      try {
        success = getMSC().hasAuthWithRole(who, role, privIndex);
      } catch (NoSuchObjectException e) {
        throw new HiveException("The user doesn't exist!");
      } catch (MetaException e) {
        throw new HiveException("get priv error:" + e.getMessage());
      } catch (TException e) {
        throw new HiveException("get priv error:" + e.getMessage());
      }
      if (success)
        return true;
      else{
        try {
          return getMSC().hasAuth(who, privIndex);
        } catch (NoSuchObjectException e) {
          throw new HiveException("The user doesn't exist!");
        } catch (MetaException e) {
          throw new HiveException("get priv error:" + e.getMessage());
        } catch (TException e) {
          throw new HiveException("get priv error:" + e.getMessage());
        }
      }
    }
  }

  private boolean hasAuthOnDb(String who, Privilege priv, String db)
      throws HiveException {
    who = who.trim().toLowerCase();
    if (who.equals(HiveMetaStore.ROOT_USER))
      return true;

    int privIndex = privMap.get(priv);

    HiveConf sessionConf = SessionState.get().getConf();
    String role = null;
    if (sessionConf == null) {
      role = null;
    } else {
      role = sessionConf.get("tdw.ugi.groupname");
    }
    if (role == null) {
      try {
        return getMSC().hasAuthOnDb(who, db, privIndex);
      } catch (NoSuchObjectException e) {
        throw new HiveException("The user doesn't exist!");
      } catch (MetaException e) {
        throw new HiveException("get priv error:" + e.getMessage());
      } catch (TException e) {
        throw new HiveException("get priv error:" + e.getMessage());
      }
    } else {
      boolean success;
      try {
        success = getMSC().hasAuthOnDbWithRole(who, role, db, privIndex);
      } catch (NoSuchObjectException e) {
        throw new HiveException("The user doesn't exist!");
      } catch (MetaException e) {
        throw new HiveException("get priv error:" + e.getMessage());
      } catch (TException e) {
        throw new HiveException("get priv error:" + e.getMessage());
      }
      if (success)
        return true;
      else{
        try {
          return getMSC().hasAuthOnDb(who, db, privIndex);
        } catch (NoSuchObjectException e) {
          throw new HiveException("The user doesn't exist!");
        } catch (MetaException e) {
          throw new HiveException("get priv error:" + e.getMessage());
        } catch (TException e) {
          throw new HiveException("get priv error:" + e.getMessage());
        }
      }
    }
  }

  private boolean hasAuthOnTbl(String who, Privilege priv, String db,
      String table) throws HiveException {

    who = who.trim().toLowerCase();
    if (who.equals(HiveMetaStore.ROOT_USER))
      return true;

    int privIndex = privMap.get(priv);

    HiveConf sessionConf = SessionState.get().getConf();
    String role = null;
    if (sessionConf == null) {
      role = null;
    } else {
      role = sessionConf.get("tdw.ugi.groupname");
    }

    if (role == null) {
      try {
        return getMSC().hasAuthOnTbl(who, db, table, privIndex);
      } catch (NoSuchObjectException e) {
        throw new HiveException("The user doesn't exist!");
      } catch (MetaException e) {
        throw new HiveException("get priv error:" + e.getMessage());
      } catch (TException e) {
        throw new HiveException("get priv error:" + e.getMessage());
      }
    } else {
      boolean success;
      try {
        success = getMSC().hasAuthOnTblWithRole(who, role, db, table, privIndex);
      } catch (NoSuchObjectException e) {
        throw new HiveException("The user doesn't exist!");
      } catch (MetaException e) {
        throw new HiveException("get priv error:" + e.getMessage());
      } catch (TException e) {
        throw new HiveException("get priv error:" + e.getMessage());
      }
      if (success)
        return true;
      else{
        try {
          return getMSC().hasAuthOnTbl(who, db, table, privIndex);
        } catch (NoSuchObjectException e) {
          throw new HiveException("The user doesn't exist!");
        } catch (MetaException e) {
          throw new HiveException("get priv error:" + e.getMessage());
        } catch (TException e) {
          throw new HiveException("get priv error:" + e.getMessage());
        }
      }
    }

  }

  public List<String> getDatabases() throws HiveException {
    try {
      return getMSC().getDatabases();
    } catch (Exception e) {
      throw new HiveException("get databases meta error : " + e.getMessage());
    }
  }

  public List<String> getDatabases(String owner) throws HiveException {
    try {
      return getMSC().getDatabases(owner);
    } catch (Exception e) {
      throw new HiveException("get databases meta error : " + e.getMessage());
    }
  }

  public boolean createIndex(Table table) throws HiveException {

    try {
      IndexItem index = new IndexItem();
      index.setDb(table.getDbName());
      index.setTbl(table.getName());

      int indexNum = 0;
      String indexNumString = table.getParameters().get("indexNum");
      if (indexNumString != null && indexNumString.length() != 0) {
        indexNum = Integer.valueOf(indexNumString);
      }

      if (indexNum != 0) {
        String indexInfoString = table.getParameters().get(
            "index" + (indexNum - 1));
        indexInfoString = (indexInfoString == null ? "" : indexInfoString);

        String[] indexInfoStrings = indexInfoString.split(";");

        if (indexInfoStrings != null && indexInfoStrings.length != 0) {
          index.setName(indexInfoStrings[0]);
          index.setFieldList(indexInfoStrings[1]);
          index.setType(Integer.valueOf(indexInfoStrings[2]));
        }

        return getMSC().create_index(index);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException("Fail to create the index: " + e.getMessage());
    }

    return true;
  }

  public boolean dropIndex(String db, String table, String name)
      throws HiveException {
    db = db.trim().toLowerCase();
    table = table.trim().toLowerCase();
    name = name.trim().toLowerCase();

    try {
      return getMSC().drop_index(db, table, name);
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException("Fail to drop the index: " + e.getMessage());
    }
  }

  public int getIndexNum(String db, String table) throws HiveException {
    db = db.trim().toLowerCase();
    table = table.trim().toLowerCase();

    try {
      return getMSC().get_index_num(db, table);
    } catch (Exception e) {
      throw new HiveException("Fail to get index num: " + e.getMessage());
    }
  }

  public int getIndexType(String db, String table, String name)
      throws HiveException {
    db = db.trim().toLowerCase();
    table = table.trim().toLowerCase();
    name = name.trim().toLowerCase();

    try {
      return getMSC().get_index_type(db, table, name);
    } catch (Exception e) {
      throw new HiveException("Fail to get index type: " + e.getMessage());
    }
  }

  public String getIndexField(String db, String table, String name)
      throws HiveException {
    db = db.trim().toLowerCase();
    table = table.trim().toLowerCase();
    name = name.trim().toLowerCase();

    try {
      return getMSC().get_index_field(db, table, name);
    } catch (Exception e) {
      throw new HiveException("Fail to get index field: " + e.getMessage());
    }
  }

  public String getIndexLocation(String db, String table, String name)
      throws HiveException {
    db = db.trim().toLowerCase();
    table = table.trim().toLowerCase();
    name = name.trim().toLowerCase();

    try {
      return getMSC().get_index_location(db, table, name);
    } catch (Exception e) {
      throw new HiveException("Fail to get index location: " + e.getMessage());
    }
  }

  public boolean setIndexLocation(String db, String table, String name,
      String location) throws HiveException {
    db = db.trim().toLowerCase();
    table = table.trim().toLowerCase();
    name = name.trim().toLowerCase();
    location = location.trim().toLowerCase();

    try {
      return getMSC().set_index_location(db, table, name, location);
    } catch (Exception e) {
      throw new HiveException("Fail to set index location: " + e.getMessage());
    }
  }

  public boolean setIndexStatus(String db, String table, String name, int status)
      throws HiveException {
    db = db.trim().toLowerCase();
    table = table.trim().toLowerCase();
    name = name.trim().toLowerCase();

    try {
      return getMSC().set_index_status(db, table, name, status);
    } catch (Exception e) {
      throw new HiveException("Fail to set index status: " + e.getMessage());
    }
  }

  public List<IndexItem> getAllIndexTable(String db, String table)
      throws HiveException {
    db = db.trim().toLowerCase();
    table = table.trim().toLowerCase();

    try {
      return getMSC().get_all_index_table(db, table);
    } catch (Exception e) {
      e.printStackTrace();
      throw new HiveException("Fail to get all index in table: "
          + e.getMessage());
    }
  }

  public IndexItem getIndexInfo(String db, String table, String name)
      throws HiveException {
    db = db.trim().toLowerCase();
    table = table.trim().toLowerCase();
    name = name.trim().toLowerCase();

    try {
      return getMSC().get_index_info(db, table, name);
    } catch (Exception e) {
      throw new HiveException("Fail to get index info: " + e.getMessage());
    }
  }

  public List<IndexItem> getAllIndexSys() throws HiveException {
    try {
      return getMSC().get_all_index_sys();
    } catch (Exception e) {
      throw new HiveException("Fail to get all index info in sys: "
          + e.getMessage());
    }
  }

  public List<FieldSchema> getFieldsJdbc(String dbName, String tableName)
      throws HiveException {
    try {
      return getMSC().getFiledsJdbc(dbName, tableName);
    } catch (Exception x) {
      throw new HiveException("Fail to get columns in sys: " + x.getMessage());
    }
  }

  public List<FieldSchema> getPartFieldsJdbc(String dbName, String tableName)
      throws HiveException {
    try {
      return getMSC().getPartFiledsJdbc(dbName, tableName);
    } catch (Exception x) {
      throw new HiveException("Fail to get columns in sys: " + x.getMessage());
    }
  }

  public int renameTable(String dbName, String tblName, String modifyUser,
      String newName) throws HiveException, InvalidOperationException {
    try {
      getMSC().renameTable(dbName, tblName, modifyUser, newName);
      return 0;
    } catch (InvalidOperationException e) {
      throw new InvalidOperationException(e);
    } catch (MetaException e) {
      throw new HiveException(e);
    } catch (TException e) {
      throw new HiveException(e);
    }
  }

  public int addCols(String dbName, String tblName, String modifyUser,
      List<FieldSchema> newCols) throws HiveException,
      InvalidOperationException {
    try {
      getMSC().addCols(dbName, tblName, modifyUser, newCols);
      return 0;
    } catch (InvalidOperationException e) {
      throw new InvalidOperationException(e);
    } catch (MetaException e) {
      throw new HiveException(e);
    } catch (TException e) {
      throw new HiveException(e);
    }
  }

  public int renameCol(String dbName, String tblName,
      RenameColDesc renameColDesc) throws HiveException,
      InvalidOperationException {
    try {
      getMSC().renameCols(dbName, tblName, renameColDesc);
      return 0;
    } catch (InvalidOperationException e) {
      throw new InvalidOperationException(e);
    } catch (MetaException e) {
      throw new HiveException(e);
    } catch (TException e) {
      throw new HiveException(e);
    }
  }

  public int replaceCols(String dbName, String tblName, String modifyUser,
      List<FieldSchema> newCols) throws HiveException,
      InvalidOperationException {
    try {
      getMSC().replaceCols(dbName, tblName, modifyUser, newCols);
      return 0;
    } catch (InvalidOperationException e) {
      throw new InvalidOperationException(e);
    } catch (MetaException e) {
      throw new HiveException(e);
    } catch (TException e) {
      throw new HiveException(e);
    }
  }

  public int addTableProps(String dbName, String tblName, String modifyUser,
      Map<String, String> props) throws HiveException,
      InvalidOperationException {
    try {
      getMSC().addTblProps(dbName, tblName, modifyUser, props);
      return 0;
    } catch (InvalidOperationException e) {
      throw new InvalidOperationException(e);
    } catch (MetaException e) {
      throw new HiveException(e);
    } catch (TException e) {
      throw new HiveException(e);
    }
  }

  public int addSerdeProps(String dbName, String tblName, String modifyUser,
      Map<String, String> props) throws HiveException,
      InvalidOperationException {
    try {
      getMSC().addSerdeProps(dbName, tblName, modifyUser, props);
      return 0;
    } catch (InvalidOperationException e) {
      throw new InvalidOperationException(e);
    } catch (MetaException e) {
      throw new HiveException(e);
    } catch (TException e) {
      throw new HiveException(e);
    }
  }

  public int addSerde(String dbName, String tblName, AddSerdeDesc addSerdeDesc)
      throws HiveException, InvalidOperationException {
    try {
      getMSC().addSerde(dbName, tblName, addSerdeDesc);
      return 0;
    } catch (InvalidOperationException e) {
      throw new InvalidOperationException(e);
    } catch (MetaException e) {
      throw new HiveException(e);
    } catch (TException e) {
      throw new HiveException(e);
    }
  }

  public boolean isView(String db, String tbl) throws HiveException,
      NoSuchObjectException {
    try {
      return getMSC().isView(db, tbl);
    } catch (MetaException x) {
      throw new HiveException(x.getMessage());
    } catch (TException x) {
      throw new HiveException(x.getMessage());
    }
  }

  public int modifyTableComment(String dbName, String tblName, String comment)
      throws HiveException, InvalidOperationException {
    try {
      getMSC().modifyTableComment(dbName, tblName, comment);
      return 0;
    } catch (InvalidOperationException e) {
      throw new InvalidOperationException(e);
    } catch (MetaException e) {
      throw new HiveException(e);
    } catch (TException e) {
      throw new HiveException(e);
    }
  }

  public int modifyColumnComment(String dbName, String tblName, String colName,
      String comment) throws HiveException, InvalidOperationException {
    try {
      getMSC().modifyColumnComment(dbName, tblName, colName, comment);
      return 0;
    } catch (InvalidOperationException e) {
      throw new InvalidOperationException(e);
    } catch (MetaException e) {
      throw new HiveException(e);
    } catch (TException e) {
      throw new HiveException(e);
    }
  }

  public boolean updatePBInfo(String dbname, String tablename,
      String modified_time) {
    try {
      getMSC().updatePBInfo(dbname, tablename, modified_time);
    } catch (MetaException e) {
      e.printStackTrace();
    } catch (TException e) {
      e.printStackTrace();
    }
    return true;
  }

  public boolean isPBTable(String dbname, String tablename) {
    try {
      return getMSC().isPBTable(dbname, tablename);
    } catch (MetaException e) {
      e.printStackTrace();
    }
    return false;
  }
  
  public boolean isHdfsExternalTable(String dbname, String tablename) {
    try {
      return getMSC().isHdfsExternalTable(dbname, tablename);
    } catch (MetaException e) {
      e.printStackTrace();
    }
    return false;
  }
  
  public boolean isLhotseImportExport(){
    String lhotse;
    String server;
    if (SessionState.get() != null){
      lhotse = SessionState.get().getConf().get("lz.etl.flag");
      server = SessionState.get().getConf().get("lz.etl.flag_server");
      if(lhotse == null || lhotse.isEmpty() || server == null || server.isEmpty())
        return false;
      else
        return lhotse.equalsIgnoreCase(server);
    }
    return false;
  }
  
  // check if the path is in current FileSystem fs
  private static boolean checkPathDomain(Path path, FileSystem fs) {
    if (path == null || fs == null)
      return false;

    URI pathUri = path.toUri();
    URI fsUri = fs.getUri();
    
    if(pathUri == null || fsUri == null)
      return true;
    
    LOG.debug("scheme " + pathUri.getScheme() + ", " + pathUri.getScheme());
    LOG.debug("authority " + fsUri.getAuthority() + ", " + fsUri.getAuthority());
    
    if (!strEquals(pathUri.getScheme(), "hdfs") || !strEquals(fsUri.getScheme(), "hdfs"))
      return true;
    else if (strEquals(pathUri.getAuthority(), fsUri.getAuthority()))
      return true;
    else
      return false;
  }
  
  private static boolean strEquals(String str1, String str2) {
    return org.apache.commons.lang.StringUtils.equals(str1, str2);
  }
  
};
