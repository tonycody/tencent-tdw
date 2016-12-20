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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AddPartitionDesc;
import org.apache.hadoop.hive.metastore.api.AddSerdeDesc;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DbPriv;
import org.apache.hadoop.hive.metastore.api.DropPartitionDesc;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.IndexAlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.IndexItem;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.RenameColDesc;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableInfo;
import org.apache.hadoop.hive.metastore.api.TblPriv;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.tdw_query_info;
import org.apache.hadoop.hive.metastore.api.tdw_query_stat;
import org.apache.hadoop.hive.metastore.api.tdw_sys_fields_statistics;
import org.apache.hadoop.hive.metastore.api.tdw_sys_table_statistics;
import org.apache.hadoop.hive.metastore.api.User;
import org.apache.hadoop.hive.metastore.api.group;
import org.apache.hadoop.hive.metastore.model.Mtdw_query_info;
import org.apache.hadoop.hive.metastore.model.Mtdw_query_stat;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import com.facebook.fb303.FacebookBase;
import com.facebook.fb303.FacebookService;
import com.facebook.fb303.fb_status;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;
import org.postgresql.jdbc2.optional.PoolingDataSource;

public class HiveMetaStore extends ThriftHiveMetastore {
  public static final String ROOT_USER = "root";
  public static final String THE_SYSTEM = "hive";
  public static final String DEFAULT = "default";

  public static String globalDbURL = null;
  public static String user = null;
  public static String passwd = null;
  public static int poolActiveSize = 5;
  public static boolean poolEnable = true;
  public static long syncTime = 100;
  public static boolean routerBufferEnable = true;
  public static int timeout = 10;

  public static class HMSHandler extends FacebookBase implements
      ThriftHiveMetastore.Iface {

    public static final Log LOG = LogFactory.getLog(HiveMetaStore.class
        .getName());

    public static final Log AUTH_LOG = LogFactory.getLog("Authorization_LOG");

    private static boolean createdDefaultDB = false;

    private static boolean createdRootUser = false;

    private static boolean createDefaultGroup = false;

    private boolean checkForRootUser;

    private static String rootPasswd = "tdwroot";

    private static int nextSerialNum = 0;

    private boolean checkForDefaultDb;

    private String rawStoreClassName;

    private HiveConf hiveConf;

    private Warehouse wh;

    private int numOfHashPar;

    private ThreadLocal<RawStore> threadLocalMS = new ThreadLocal() {
      protected synchronized Object initialValue() {
        return null;
      }
    };

    private static ThreadLocal<Integer> threadLocalId = new ThreadLocal() {
      protected synchronized Object initialValue() {
        return new Integer(nextSerialNum++);
      }
    };

    public static Integer get() {
      return threadLocalId.get();
    }

    public HMSHandler(String name) throws MetaException {
      super(name);
      hiveConf = new HiveConf(this.getClass());
      init();
    }

    public HMSHandler(String name, HiveConf conf) throws MetaException {
      super(name);
      hiveConf = conf;
      init();
    }

    private ClassLoader classLoader;
    private AlterHandler alterHandler;
    {
      classLoader = Thread.currentThread().getContextClassLoader();
      if (classLoader == null) {
        classLoader = Configuration.class.getClassLoader();
      }
    }

    private synchronized static void initSyncThread() {
      if (!JDBCStore.globalDbSyncInit) {
        LOG.debug("#################################################################### init sync thread");

        JDBCStore.globalDbUrl = globalDbURL;
        JDBCStore.user = user;
        JDBCStore.passwd = passwd;
        JDBCStore.poolActiveSize = poolActiveSize;
        JDBCStore.poolEnable = poolEnable;
        JDBCStore.timeout = timeout;

        LOG.debug("JDBCStore.globalDbUrl=" + JDBCStore.globalDbUrl);
        LOG.debug("JDBCStore.user=" + JDBCStore.user);
        LOG.debug("JDBCStore.passwd=" + JDBCStore.passwd);
        LOG.debug("JDBCStore.poolActiveSize=" + JDBCStore.poolActiveSize);
        LOG.debug("JDBCStore.poolEnable=" + JDBCStore.poolEnable);
        LOG.debug("JDBCStore.timeout=" + JDBCStore.timeout);

        if (routerBufferEnable) {
          LOG.debug("routerBufferEnable is true");
          LOG.debug("JDBCStore.masterUrl");
          JDBCStore.syncTime = syncTime;
          JDBCStore.initMetaSyncThread();
        }

        JDBCStore.globalDbSyncInit = true;
        LOG.debug("#################################################################### init sync thread over");
      }
    }

    private boolean init() throws MetaException {
      rawStoreClassName = "org.apache.hadoop.hive.metastore.JDBCStore";
      checkForDefaultDb = hiveConf.getBoolean(
          "hive.metastore.checkForDefaultDb", true);
      checkForRootUser = hiveConf.getBoolean("hive.metastore.checkForRootUser",
          true);

      globalDbURL = hiveConf.get("hive.metastore.global.url",
          "jdbc:postgresql://10.136.130.102:5432/master_metastore_db");
      user = hiveConf.get("hive.metastore.user", "tdw");
      passwd = hiveConf.get("hive.metastore.passwd", "tdw");
      poolActiveSize = hiveConf
          .getInt("hive.metadata.conn.pool.activesize", 10);
      poolEnable = hiveConf.getBoolean("hive.metadata.conn.pool.enable", true);
      routerBufferEnable = hiveConf.getBoolean(
          "hive.metastore.router.buffer.enable", true);
      syncTime = hiveConf.getLong("hive.metastore.router.buffer.sync.time", 10);
      timeout = hiveConf.getInt("hive.pg.timeout", 10);

      JDBCStore.initMasterToSlaveMap(hiveConf);
      initSyncThread();

      wh = new Warehouse(hiveConf);

      numOfHashPar = hiveConf.getInt("hive.hashPartition.num", 500);
      if (numOfHashPar <= 0)
        throw new MetaException(
            "Hash Partition Number should be Positive Integer!");

      createDefaultDB();
      createRootUser();
      createDefaultGroup();
      return true;
    }

    private RawStore getMS() throws MetaException {
      RawStore ms = threadLocalMS.get();

      if (ms == null) {
        LOG.info(threadLocalId.get()
            + ": Opening raw store with implemenation class:"
            + rawStoreClassName);

        ms = (RawStore) ReflectionUtils.newInstance(
            getClass(rawStoreClassName, RawStore.class), hiveConf);
        threadLocalMS.set(ms);
        ms = threadLocalMS.get();
      }
      return ms;
    }

    private void createDefaultDB() throws MetaException {
      if (HMSHandler.createdDefaultDB || !checkForDefaultDb) {
        return;
      }

      try {
        getMS().getDatabase(MetaStoreUtils.DEFAULT_DATABASE_NAME);
      } catch (NoSuchObjectException e) {
        //e.printStackTrace();
    	LOG.info("no default database,will create ...");
        getMS().createDatabase(
            new Database(MetaStoreUtils.DEFAULT_DATABASE_NAME, wh
                .getDefaultDatabasePath(MetaStoreUtils.DEFAULT_DATABASE_NAME)
                .toString(), null, null, null));
      }

      HMSHandler.createdDefaultDB = true;
    }

    private Class<?> getClass(String rawStoreClassName, Class<?> class1)
        throws MetaException {
      try {
        return Class.forName(rawStoreClassName, true, classLoader);
      } catch (ClassNotFoundException e) {
        throw new MetaException(rawStoreClassName + " class not found");
      }
    }

    private void logStartFunction(String m) {
      LOG.info(threadLocalId.get().toString() + ": " + m);

    }

    private void logStartFunction(String f, String db, String tbl) {

      LOG.info(threadLocalId.get().toString() + ": " + f + " : db=" + db
          + " tbl=" + tbl);

    }

    private void logStartFunction(String bywho, String dowhat) {
      AUTH_LOG.info(bywho + " " + dowhat);

    }

    @Override
    public int getStatus() {
      return 2;
    }

    public void shutdown() {
      logStartFunction("Shutting down the object store...");
      try {
        if (threadLocalMS.get() != null) {
          getMS().shutdown();
          threadLocalMS.remove();
        }
      } catch (MetaException e) {
        LOG.error("unable to shutdown metastore", e);
      }

    }

    public boolean create_database(String name, String location_uri,
        String hdfsscheme) throws AlreadyExistsException, MetaException {
      this.incrementCounter("create_database");
      logStartFunction("create_database: " + name);
      boolean success = false;
      try {
        getMS().openTransaction();
        Database db = new Database(name, location_uri, hdfsscheme, null, null);
        Path databasePath = wh.getDefaultDatabasePath(name, hdfsscheme);
        if (getMS().createDatabase(db) && wh.mkdirs(databasePath)) {
          success = getMS().commitTransaction();
        }
      } finally {
        if (!success) {
          getMS().rollbackTransaction();
        }
      }
      return success;
    }

    public boolean createDatabase(String name, String location,
        String hdfsscheme) throws AlreadyExistsException, MetaException {
      this.incrementCounter("create_database");
      logStartFunction("create_database: " + name);
      boolean success = false;

      Database db = new Database(name, location, hdfsscheme, null, null);
      return getMS().createDatabase(db);
    }

    public Database get_database(String name) throws NoSuchObjectException,
        MetaException {
      this.incrementCounter("get_database");
      logStartFunction("get_database: " + name);
      return getMS().getDatabase(name);
    }

    public Database getDatabase(String name) throws NoSuchObjectException,
        MetaException {
      this.incrementCounter("get_database");
      logStartFunction("get_database: " + name);
      return getMS().getDatabase(name);
    }

    public boolean drop_database(String name) throws MetaException {
      this.incrementCounter("drop_database");
      logStartFunction("drop_database: " + name);
      if (name.equalsIgnoreCase(MetaStoreUtils.DEFAULT_DATABASE_NAME)) {
        throw new MetaException("Can't drop default database");
      }
      boolean success = false;
      Path databaseDir = null;
      try {
        getMS().openTransaction();
        databaseDir = wh.getDefaultDatabasePath(name);
        if (getMS().dropDatabase(name)) {
          success = getMS().commitTransaction();

          Path indexPath = wh.getDefaultIndexPath(name, "", "");
          wh.deleteDir(indexPath, true);
        }
      } finally {
        if (!success) {
          getMS().rollbackTransaction();
        } else {
          wh.deleteDir(databaseDir, true);
        }
      }
      return success;
    }

    public boolean dropDatabase(String name) throws MetaException {
      this.incrementCounter("drop_database");
      logStartFunction("drop_database: " + name);

      if (name.equalsIgnoreCase(MetaStoreUtils.DEFAULT_DATABASE_NAME)) {
        throw new MetaException("Can't drop default database");
      }

      boolean ret = false;
      try {
        getMS().dropDatabase(name);
        ret = true;
      } catch (Exception x) {
        ret = false;
      }

      return ret;
    }

    public List<String> get_databases() throws MetaException {
      this.incrementCounter("get_databases");
      logStartFunction("get_databases");
      return getMS().getDatabases();
    }

    public List<String> getDatabases() throws MetaException {
      this.incrementCounter("get_databases");
      logStartFunction("get_databases");
      return getMS().getDatabases();
    }

    public Warehouse getWarehouse() {
      return wh;
    }

    public boolean create_type(Type type) throws AlreadyExistsException,
        MetaException, InvalidObjectException {
      this.incrementCounter("create_type");
      logStartFunction("create_type: " + type.getName());
      if (get_type(type.getName()) != null) {
        throw new AlreadyExistsException("Type " + type.getName()
            + " already exists");
      }

      return getMS().createType(type);
    }

    public boolean createType(Type type) throws AlreadyExistsException,
        MetaException, InvalidObjectException {
      this.incrementCounter("create_type");
      logStartFunction("create_type: " + type.getName());

      return getMS().createType(type);
    }

    public Type get_type(String name) throws MetaException {
      this.incrementCounter("get_type");
      logStartFunction("get_type: " + name);
      return getMS().getType(name);
    }

    public Type getType(String name) throws MetaException {
      this.incrementCounter("get_type");
      logStartFunction("get_type: " + name);
      return getMS().getType(name);
    }

    public boolean drop_type(String name) throws MetaException {
      this.incrementCounter("drop_type");
      logStartFunction("drop_type: " + name);
      return getMS().dropType(name);
    }

    public boolean dropType(String name) throws MetaException {
      this.incrementCounter("drop_type");
      logStartFunction("drop_type: " + name);
      return getMS().dropType(name);
    }

    public Map<String, Type> get_type_all(String name) throws MetaException {
      this.incrementCounter("get_type_all");
      logStartFunction("get_type_all");
      throw new MetaException("Not yet implemented");
    }

    public Map<String, Type> getypeAll(String name) throws MetaException {
      this.incrementCounter("get_type_all");
      logStartFunction("get_type_all");
      throw new MetaException("Not yet implemented");
    }

    public void create_table(Table tbl) throws AlreadyExistsException,
        MetaException, InvalidObjectException {
      this.incrementCounter("create_table");
      logStartFunction("create_table: db=" + tbl.getDbName() + " tbl="
          + tbl.getTableName());

      if (!MetaStoreUtils.validateName(tbl.getTableName())
          || !MetaStoreUtils.validateColNames(tbl.getSd().getCols())
          || (tbl.getPriPartition() != null && !MetaStoreUtils.validateName(tbl
              .getPriPartition().getParKey().getName()))
          || (tbl.getSubPartition() != null && !MetaStoreUtils.validateName(tbl
              .getSubPartition().getParKey().getName()))) {
        throw new InvalidObjectException(tbl.getTableName()
            + " is not a valid object name");
      }

      Path tblPath = null;
      Path indexPath = null;
      boolean madeIndexDir = false;

      boolean success = false, madeDir = false;
      List<Path> partPaths = null;
      try {
        getMS().openTransaction();
        if (!TableType.VIRTUAL_VIEW.toString().equals(tbl.getTableType())) {
          if (tbl.getSd().getLocation() == null
              || tbl.getSd().getLocation().isEmpty()) {
            tblPath = wh.getDefaultTablePath(tbl.getDbName(),
                tbl.getTableName());
          } else {
            if (!isExternal(tbl)) {
              LOG.warn("Location: " + tbl.getSd().getLocation()
                  + "specified for non-external table:" + tbl.getTableName());
            }
            tblPath = wh.getDnsPath(new Path(tbl.getSd().getLocation()));
          }

          tbl.getSd().setLocation(tblPath.toString());
        }

        if (is_table_exists(tbl.getDbName(), tbl.getTableName())) {
          throw new AlreadyExistsException("Table " + tbl.getTableName()
              + " already exists");
        }

        if (tblPath != null) {
          if (!wh.isDir(tblPath)) {
            if (!wh.mkdirs(tblPath)) {
              throw new MetaException(tblPath
                  + " is not a directory or unable to create one");
            }
            madeDir = true;
          }
        }

        if (tbl.getPriPartition() != null) {
          Set<String> priPartNames;

          priPartNames = tbl.getPriPartition().getParSpaces().keySet();

          Set<String> subPartNames = null;
          if (tbl.getSubPartition() != null) {

            subPartNames = tbl.getSubPartition().getParSpaces().keySet();
          }

          partPaths = wh.getPartitionPaths(tblPath, priPartNames, subPartNames);

          for (Path partPath : partPaths) {
            if (!wh.mkdirs(partPath)) {
              throw new MetaException("Partition path " + partPath
                  + " is not a directory or unable to create one.");
            }
          }
        }

        getMS().createTable(tbl);
        success = getMS().commitTransaction();

      } finally {
        if (!success) {
          getMS().rollbackTransaction();
          if (madeDir) {
            wh.deleteDir(tblPath, true);
          }
        }
      }
    }

    public void createTable(Table tbl) throws AlreadyExistsException,
        MetaException, InvalidObjectException {
      this.incrementCounter("createTable");
      logStartFunction("createTable: db=" + tbl.getDbName() + " tbl="
          + tbl.getTableName());

      if (!MetaStoreUtils.validateName(tbl.getTableName())
          || !MetaStoreUtils.validateColNames(tbl.getSd().getCols())
          || (tbl.getPriPartition() != null && !MetaStoreUtils.validateName(tbl
              .getPriPartition().getParKey().getName()))
          || (tbl.getSubPartition() != null && !MetaStoreUtils.validateName(tbl
              .getSubPartition().getParKey().getName()))) {
        throw new InvalidObjectException(tbl.getTableName()
            + " is not a valid object name");
      }

      Path tblPath = null;

      if (!TableType.VIRTUAL_VIEW.toString().equals(tbl.getTableType())) {
        if (tbl.getSd().getLocation() == null
            || tbl.getSd().getLocation().isEmpty()) {
          tblPath = wh.getDefaultTablePath(tbl.getDbName(), tbl.getTableName());
        } else {
          if (!isExternal(tbl)) {
            LOG.warn("Location: " + tbl.getSd().getLocation()
                + "specified for non-external table:" + tbl.getTableName());
          }
          tblPath = wh.getDnsPath(new Path(tbl.getSd().getLocation()));
        }

        tbl.getSd().setLocation(tblPath.toString());
      }

      getMS().createTable(tbl);
    }

    public boolean is_table_exists(String dbname, String name)
        throws MetaException {
      try {
        return (get_table(dbname, name) != null);
      } catch (NoSuchObjectException e) {
        return false;
      }
    }

    public boolean isTableExists(String dbname, String name)
        throws MetaException {
      return getMS().isTableExit(dbname, name);
    }

    public void drop_table(String dbname, String name, boolean deleteData)
        throws NoSuchObjectException, MetaException {
      this.incrementCounter("drop_table");
      logStartFunction("drop_table", dbname, name);
      boolean success = false;
      boolean isExternal = false;
      Path tblPath = null;
      Path indexPath = null;
      Table tbl = null;
      isExternal = false;
      try {
        getMS().openTransaction();
        tbl = get_table(dbname, name);
        if (tbl == null) {
          throw new NoSuchObjectException(name + " doesn't exist");
        }
        if (tbl.getSd() == null) {
          throw new MetaException("Table metadata is corrupted");
        }
        isExternal = isExternal(tbl);
        if (tbl.getSd().getLocation() != null) {
          tblPath = new Path(tbl.getSd().getLocation());
        }

        if (!getMS().dropTable(dbname, name)) {
          throw new MetaException("Unable to drop table");
        }
        tbl = null;
        success = getMS().commitTransaction();

        List<IndexItem> indexItemList = getMS().getAllIndexTable(dbname, name);
        if (indexItemList != null) {
          for (int i = 0; i < indexItemList.size(); i++) {
            try {
              getMS().dropIndex(dbname, name, indexItemList.get(i).getName());
            } catch (Exception e) {
              LOG.info("drop index:" + indexItemList.get(i).getName()
                  + " fail:" + e.getMessage() + ",db:" + dbname + ",table:"
                  + name);

            }
          }
        }
        indexPath = wh.getDefaultIndexPath(dbname, name, "");
        wh.deleteDir(indexPath, true);

      } finally {
        if (!success) {
          getMS().rollbackTransaction();
        } else if (deleteData && (tblPath != null) && !isExternal) {
          wh.deleteDir(tblPath, true);
        }
      }
    }

    public void dropTable(String dbname, String name, boolean deleteData)
        throws NoSuchObjectException, MetaException {
      this.incrementCounter("drop_table");
      logStartFunction("drop_table", dbname, name);

      getMS().dropTable(dbname, name, deleteData);
    }

    private boolean isExternal(Table table) {
      if (table == null) {
        return false;
      }

      Map<String, String> params = table.getParameters();
      if (params == null) {
        return false;
      }
      return "TRUE".equalsIgnoreCase(params.get("EXTERNAL"));
    }

    public Table get_table(String dbname, String name) throws MetaException,
        NoSuchObjectException {
      this.incrementCounter("get_table");
      logStartFunction("get_table", dbname, name);
      Table t = getMS().getTable(dbname, name);
      if (t == null) {
        throw new NoSuchObjectException(dbname + "." + name
            + " table not found");
      }
      return t;
    }

    public Table getTable(String dbname, String name) throws MetaException,
        NoSuchObjectException {
      this.incrementCounter("get_table");
      logStartFunction("get_table", dbname, name);
      Table t = getMS().getTable(dbname, name);

      if (t == null) {
        throw new NoSuchObjectException(dbname + "." + name
            + " table not found");
      }
      return t;
    }

    public boolean set_table_parameters(String dbname, String name,
        Map<String, String> params) throws NoSuchObjectException, MetaException {
      this.incrementCounter("set_table_parameters");
      logStartFunction("set_table_parameters", dbname, name);
      return false;
    }

    public boolean setTableParameters(String dbname, String name,
        Map<String, String> params) throws NoSuchObjectException, MetaException {
      this.incrementCounter("set_table_parameters");
      logStartFunction("set_table_parameters", dbname, name);
      return false;
    }

    public Partition get_partition(String db_name, String tbl_name, int level)
        throws MetaException {
      this.incrementCounter("get_partition");
      logStartFunction("get_partition", db_name, tbl_name);
      return getMS().getPartition(db_name, tbl_name, level);
    }

    public Partition getPartition(String dbName, String tblName, int level)
        throws MetaException {
      this.incrementCounter("getPartition");
      logStartFunction("getPartition", dbName, tblName);
      return getMS().getPartition(dbName, tblName, level);
    }

    public List<Partition> get_partitions(String db_name, String tbl_name)
        throws NoSuchObjectException, MetaException {
      this.incrementCounter("get_partitions");
      logStartFunction("get_partitions", db_name, tbl_name);

      List<Partition> partitions = new ArrayList<Partition>();
      partitions.add(get_partition(db_name, tbl_name, 0));
      partitions.add(get_partition(db_name, tbl_name, 1));
      return partitions;
    }

    public List<Partition> getPartitions(String dbName, String tblName)
        throws NoSuchObjectException, MetaException {
      this.incrementCounter("get_partitions");
      logStartFunction("get_partitions", dbName, tblName);

      List<Partition> partitions = new ArrayList<Partition>();
      partitions.add(getPartition(dbName, tblName, 0));
      partitions.add(getPartition(dbName, tblName, 1));
      return partitions;
    }

    public void alter_partition(String db_name, String tbl_name,
        Partition new_part) throws InvalidOperationException, MetaException,
        TException {
      this.incrementCounter("alter_partition");
      logStartFunction("alter_partition", db_name, tbl_name);
      try {
        getMS().alterPartition(db_name, tbl_name, new_part);
      } catch (InvalidObjectException e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new InvalidOperationException(StringUtils.stringifyException(e));
      }
    }

    public void alterPartition(String dbName, String tblName, Partition newPart)
        throws InvalidOperationException, MetaException, TException {
      this.incrementCounter("alter_partition");
      logStartFunction("alter_partition", dbName, tblName);

      try {
        getMS().alterPartition(dbName, tblName, newPart);
      } catch (InvalidObjectException e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new InvalidOperationException(StringUtils.stringifyException(e));
      }
    }

    public boolean create_index(Index index_def)
        throws IndexAlreadyExistsException, MetaException {
      this.incrementCounter("create_index");
      throw new MetaException("Not yet implemented");
    }

    public boolean createIndex(Index indexDef)
        throws IndexAlreadyExistsException, MetaException {
      this.incrementCounter("create_index");
      throw new MetaException("Not yet implemented");
    }

    public String getVersion() throws TException {
      this.incrementCounter("getVersion");
      logStartFunction("getVersion");
      return "3.0";
    }

    public void alter_table(String dbname, String name, Table newTable)
        throws InvalidOperationException, MetaException {
      this.incrementCounter("alter_table");
      logStartFunction("truncate_table: db=" + dbname + " tbl=" + name
          + " newtbl=" + newTable.getTableName());
      alterHandler.alterTable(getMS(), wh, dbname, name, newTable);
    }

    public void alterTable(String dbname, String name, Table newTable)
        throws InvalidOperationException, MetaException {
      this.incrementCounter("alterTable");
      logStartFunction("alterTable: db=" + dbname + " tbl=" + name + " newtbl="
          + newTable.getTableName());
      alterHandler.alterTable(getMS(), wh, dbname, name, newTable);
    }

    public List<String> get_tables(String dbname, String pattern)
        throws MetaException {
      this.incrementCounter("get_tables");
      logStartFunction("get_tables: db=" + dbname + " pat=" + pattern);
      return getMS().getTables(dbname, pattern);
    }

    public List<String> getTables(String dbname, String pattern)
        throws MetaException {
      this.incrementCounter("get_tables");
      logStartFunction("get_tables: db=" + dbname + " pat=" + pattern);
      return getMS().getTables(dbname, pattern);
    }

    public List<FieldSchema> get_fields(String db, String tableName)
        throws MetaException, UnknownTableException, UnknownDBException {
      this.incrementCounter("get_fields");
      logStartFunction("get_fields: db=" + db + "tbl=" + tableName);
      String[] names = tableName.split("\\.");
      String base_table_name = names[0];

      Table tbl;
      try {
        tbl = this.get_table(db, base_table_name);
      } catch (NoSuchObjectException e) {
        throw new UnknownTableException(e.getMessage());
      }

      boolean getColsFromSerDe = SerDeUtils.shouldGetColsFromSerDe(tbl.getSd()
          .getSerdeInfo().getSerializationLib());
      if (!getColsFromSerDe)
        return tbl.getSd().getCols();
      else {
        try {
          Deserializer s = MetaStoreUtils.getDeserializer(this.hiveConf, tbl);
          return MetaStoreUtils.getFieldsFromDeserializer(tableName, s);
        } catch (SerDeException e) {
          StringUtils.stringifyException(e);
          throw new MetaException(e.getMessage());
        }
      }
    }

    public List<FieldSchema> getFields(String db, String tableName)
        throws MetaException, UnknownTableException, UnknownDBException {
      this.incrementCounter("get_fields");
      logStartFunction("get_fields: db=" + db + "tbl=" + tableName);

      String[] names = tableName.split("\\.");
      String baseTableName = names[0];
      return getMS().getFieldsJdbc(db, baseTableName);
    }

    public List<FieldSchema> get_fields_jdbc(String db, String tableName)
        throws MetaException {
      this.incrementCounter("get_fields");
      logStartFunction("get_fileds", db, tableName);
      List<FieldSchema> coulmnInfo = null;
      try {
        coulmnInfo = getMS().getFieldsJdbc(db, tableName);
      } catch (MetaException x) {
        x.printStackTrace();
      }

      if (coulmnInfo == null) {
        throw new MetaException(db + "." + tableName + " columns not found");
      }
      return coulmnInfo;
    }

    public List<FieldSchema> getFieldsJdbc(String db, String tableName)
        throws MetaException {
      this.incrementCounter("getFieldsJdbc");
      logStartFunction("getFieldsJdbc", db, tableName);

      List<FieldSchema> coulmnInfo = null;

      try {
        coulmnInfo = getMS().getFieldsJdbc(db, tableName);
      } catch (MetaException x) {
        x.printStackTrace();
      }

      if (coulmnInfo == null) {
        throw new MetaException(db + "." + tableName + " columns not found");
      }

      return coulmnInfo;
    }

    public List<FieldSchema> get_part_fields_jdbc(String db, String tableName)
        throws MetaException {
      this.incrementCounter("get_fields");
      logStartFunction("get_fileds", db, tableName);
      List<FieldSchema> coulmnInfo = new ArrayList<FieldSchema>();
      try {
        coulmnInfo = getMS().getPartFieldsJdbc(db, tableName);
      } catch (MetaException x) {
        x.printStackTrace();
      }
      return coulmnInfo;
    }

    public List<FieldSchema> getPartFieldsJdbc(String db, String tableName)
        throws MetaException {
      this.incrementCounter("get_fields");
      logStartFunction("get_fileds", db, tableName);
      List<FieldSchema> coulmnInfo = new ArrayList<FieldSchema>();
      try {
        coulmnInfo = getMS().getPartFieldsJdbc(db, tableName);
      } catch (MetaException x) {
        x.printStackTrace();
      }
      return coulmnInfo;
    }

    public List<FieldSchema> get_schema(String db, String tableName)
        throws MetaException, UnknownTableException, UnknownDBException {
      this.incrementCounter("get_schema");
      logStartFunction("get_schema: db=" + db + "tbl=" + tableName);
      String[] names = tableName.split("\\.");
      String base_table_name = names[0];

      Table tbl;
      try {
        tbl = this.get_table(db, base_table_name);
      } catch (NoSuchObjectException e) {
        throw new UnknownTableException(e.getMessage());
      }
      List<FieldSchema> fieldSchemas = this.get_fields(db, base_table_name);

      if (tbl == null || fieldSchemas == null) {
        throw new UnknownTableException(tableName + " doesn't exist");
      }

      return fieldSchemas;
    }

    public List<FieldSchema> getSchema(String db, String tableName)
        throws MetaException, UnknownTableException, UnknownDBException {
      this.incrementCounter("getSchema");
      logStartFunction("getSchema: db=" + db + "tbl=" + tableName);

      String[] names = tableName.split("\\.");

      String baseTableName = names[0];

      return getMS().getFieldsJdbc(db, baseTableName);
    }

    public String getCpuProfile(int profileDurationInSec) throws TException {
      return "";
    }

    public tdw_sys_table_statistics add_table_statistics(
        tdw_sys_table_statistics new_table_statistics)
        throws AlreadyExistsException, MetaException, TException {
      this.incrementCounter("add_table_statistics");
      logStartFunction("add_table_statistics "
          + new_table_statistics.getStat_table_name());
      boolean success = false;
      try {
        getMS().openTransaction();
        tdw_sys_table_statistics old_table_statistics = this
            .get_table_statistics(new_table_statistics.getStat_table_name(),
                new_table_statistics.getStat_db_name());
        if (old_table_statistics != null) {
          throw new AlreadyExistsException("table_statistics already exists:"
              + new_table_statistics);
        }
        success = getMS().add_table_statistics(new_table_statistics)
            && getMS().commitTransaction();
      } finally {
        if (!success) {
          getMS().rollbackTransaction();
        }
      }
      return new_table_statistics;

    }

    public boolean delete_table_statistics(String table_statistics_name,
        String db_statistics_name) throws NoSuchObjectException, MetaException,
        TException {
      this.incrementCounter("delete_table_statistics");
      logStartFunction("delete_table_statistics: " + table_statistics_name);
      boolean success = false;
      try {
        getMS().openTransaction();
        if (getMS().delete_table_statistics(table_statistics_name,
            db_statistics_name)) {
          success = getMS().commitTransaction();
        }
      } finally {
        if (!success) {
          getMS().rollbackTransaction();
        }
      }
      return success;
    }

    public tdw_sys_table_statistics get_table_statistics(
        String table_statistics_name, String db_statistics_name)
        throws MetaException, TException {
      this.incrementCounter("get_table_statistics");
      logStartFunction("get_table_statistics" + table_statistics_name);
      tdw_sys_table_statistics table_statistics = getMS().get_table_statistics(
          table_statistics_name, db_statistics_name);
      return table_statistics;
    }

    public List<tdw_sys_table_statistics> get_table_statistics_multi(
        String db_statistics_name, int max) throws NoSuchObjectException,
        MetaException, TException {
      this.incrementCounter("get_table_statistics_multi");
      logStartFunction("get_table_statistics_multi");
      return getMS().get_table_statistics_multi(db_statistics_name, max);
    }

    public List<String> get_table_statistics_names(String db_statistics_name,
        int max) throws NoSuchObjectException, MetaException, TException {
      this.incrementCounter("get_table_statistics_names");
      logStartFunction("get_table_statistics_names");
      return getMS().get_table_statistics_names(db_statistics_name, max);
    }

    public tdw_sys_fields_statistics add_fields_statistics(
        tdw_sys_fields_statistics new_fields_statistics)
        throws AlreadyExistsException, MetaException, TException {
      this.incrementCounter("add_fields_statistics");
      logStartFunction("add_fields_statistics "
          + new_fields_statistics.getStat_table_name());
      boolean success = false;
      try {
        getMS().openTransaction();
        tdw_sys_fields_statistics old_fields_statistics = this
            .get_fields_statistics(new_fields_statistics.getStat_table_name(),
                new_fields_statistics.getStat_db_name(),
                new_fields_statistics.getStat_field_name());
        if (old_fields_statistics != null) {
          throw new AlreadyExistsException("table_statistics already exists:"
              + new_fields_statistics);
        }
        success = getMS().add_fields_statistics(new_fields_statistics)
            && getMS().commitTransaction();
      } finally {
        if (!success) {
          getMS().rollbackTransaction();
        }
      }
      return new_fields_statistics;

    }

    public boolean delete_fields_statistics(String stat_table_name,
        String db_statistics_name, String stat_field_name)
        throws NoSuchObjectException, MetaException, TException {
      this.incrementCounter("delete_fields_statistics");
      logStartFunction("delete_fields_statistics: " + stat_table_name + "."
          + stat_field_name);
      boolean success = false;
      try {
        getMS().openTransaction();
        if (getMS().delete_fields_statistics(stat_table_name,
            db_statistics_name, stat_field_name)) {
          success = getMS().commitTransaction();
        }
      } finally {
        if (!success) {
          getMS().rollbackTransaction();
        }
      }
      return success;
    }

    public tdw_sys_fields_statistics get_fields_statistics(
        String stat_table_name, String stat_db_name, String stat_field_name)
        throws MetaException, TException {
      this.incrementCounter("get_fields_statistics");
      logStartFunction("get_fields_statistics" + stat_table_name + "."
          + stat_field_name);
      tdw_sys_fields_statistics fields_statistics = getMS()
          .get_fields_statistics(stat_table_name, stat_db_name, stat_field_name);
      return fields_statistics;
    }

    public List<tdw_sys_fields_statistics> get_fields_statistics_multi(
        String stat_table_name, String db_statistics_name, int max)
        throws NoSuchObjectException, MetaException, TException {
      this.incrementCounter("get_fields_statistics_multi");
      logStartFunction("get_fields_statistics_multi " + stat_table_name);
      return getMS().get_fields_statistics_multi(stat_table_name,
          db_statistics_name, max);
    }

    public List<String> get_fields_statistics_names(String stat_table_name,
        String db_statistics_name, int max) throws NoSuchObjectException,
        MetaException, TException {
      this.incrementCounter("get_fields_statistics_names");
      logStartFunction("get_fields_statistics_names " + stat_table_name);
      return getMS().get_fields_statistics_names(stat_table_name,
          db_statistics_name, max);
    }

    public boolean add_tdw_query_info(tdw_query_info query_info)
        throws MetaException {
      return getMS().add_tdw_query_info(query_info);
    }

    public boolean add_tdw_query_stat(tdw_query_stat query_stat)
        throws MetaException {
      return getMS().add_tdw_query_stat(query_stat);
    }

    public boolean update_tdw_query_info(String qid, String finishtime,
        String state) throws MetaException {
      return getMS().update_tdw_query_info(qid, finishtime, state);
    }

    public boolean update_tdw_query_stat(String qid, String finishtime,
        String state) throws MetaException {
      return getMS().update_tdw_query_stat(qid, finishtime, state);
    }

    public List<tdw_query_info> get_tdw_query_info() throws MetaException {
      return getMS().get_tdw_query_info();
    }

    public List<tdw_query_stat> get_tdw_query_stat() throws MetaException {
      return getMS().get_tdw_query_stat();
    }

    public boolean clear_tdw_query_info(int days) throws MetaException {
      return getMS().clear_tdw_query_info(days);
    }

    public boolean clear_tdw_query_stat(int days) throws MetaException {
      return getMS().clear_tdw_query_stat(days);
    }

    public tdw_query_info search_tdw_query_info(String qid)
        throws MetaException {
      return getMS().search_tdw_query_info(qid);
    }

    public boolean add_user_group(group newgroup, String user)
        throws MetaException {
      return getMS().add_user_group(newgroup, user);
    }

    public int drop_user_group(String groupname, String user)
        throws MetaException {
      return getMS().drop_user_group(groupname, user);
    }

    public String get_groupname(String username) throws MetaException {
      return getMS().get_groupname(username);
    }

    public int revoke_user_group(String groupname, String namelist, String user)
        throws MetaException {
      return getMS().revoke_user_group(groupname, namelist, user);
    }

    public int grant_user_group(String groupname, String namelist, String user)
        throws MetaException {
      return getMS().grant_user_group(groupname, namelist, user);
    }

    public List<group> get_groups(String pattern) throws MetaException {
      return getMS().get_groups(pattern);
    }

    public boolean addUserGroup(group newGroup, String user)
        throws MetaException {
      this.incrementCounter("addUserGroup");
      logStartFunction("addUserGroup: group=" + newGroup.getGroupName()
          + " creator=" + user);
      return getMS().addUserGroup(newGroup, user);
    }

    public int dropUserGroup(String groupName, String user)
        throws MetaException {
      this.incrementCounter("dropUserGroup");
      logStartFunction("dropUserGroup: group=" + groupName + " user=" + user);
      return getMS().dropUserGroup(groupName, user);
    }

    public String getGroupname(String username) throws MetaException {
      this.incrementCounter("getGroupname");
      logStartFunction("getGroupname: username=" + username);
      return getMS().getGroupname(username);
    }

    public int revokeUserGroup(String groupname, String namelist, String user)
        throws MetaException {
      this.incrementCounter("revokeUserGroup");
      logStartFunction("revokeUserGroup: groupname=" + groupname + " namelist="
          + namelist + " user=" + user);
      return getMS().revokeUserGroup(groupname, namelist, user);
    }

    public int grantUserGroup(String groupname, String namelist, String user)
        throws MetaException {
      this.incrementCounter("grantUserGroup");
      logStartFunction("grantUserGroup: groupname=" + groupname + " namelist="
          + namelist + " user=" + user);
      return getMS().grantUserGroup(groupname, namelist, user);
    }

    public List<group> getGroups(String pattern) throws MetaException {
      this.incrementCounter("getGroups");
      logStartFunction("getGroups: groupname=" + pattern);
      return getMS().getGroups(pattern);
    }

    public boolean create_role(String byWho, String roleName)
        throws AlreadyExistsException, TException, MetaException {
      this.incrementCounter("create_role");
      logStartFunction(byWho, "create_role: " + roleName);

      return getMS().createRole(roleName);
    }

    public boolean createRole(String byWho, String roleName)
        throws AlreadyExistsException, TException, MetaException {
      this.incrementCounter("createRole");
      logStartFunction(byWho, "createRole: " + roleName);

      return getMS().createRole(roleName);
    }

    public boolean create_user(String byWho, String newUser, String passwd)
        throws AlreadyExistsException, TException, MetaException {
      this.incrementCounter("create_user");
      logStartFunction(byWho, "create user: " + newUser);

      return getMS().createUser(newUser, passwd);
    }

    public boolean createUser(String byWho, String newUser, String passwd)
        throws AlreadyExistsException, TException, MetaException {
      this.incrementCounter("createUser");
      logStartFunction(byWho, "createUser user: " + newUser);

      return getMS().createUser(newUser, passwd);
    }

    public boolean drop_role(String byWho, String roleName)
        throws NoSuchObjectException, TException, MetaException {
      this.incrementCounter("drop_role");
      logStartFunction(byWho, "drop role: " + roleName);

      return getMS().dropRole(roleName);
    }

    public boolean dropRole(String byWho, String roleName)
        throws NoSuchObjectException, TException, MetaException {
      this.incrementCounter("dropRole");
      logStartFunction(byWho, "dropRole role: " + roleName);

      return getMS().dropRole(roleName);
    }

    public boolean drop_user(String byWho, String userName)
        throws NoSuchObjectException, TException, MetaException {
      this.incrementCounter("drop_user");
      logStartFunction(byWho, "drop user: " + userName);

      return getMS().dropUser(userName);
    }

    public boolean dropUser(String byWho, String userName)
        throws NoSuchObjectException, TException, MetaException {
      this.incrementCounter("dropUser");
      logStartFunction(byWho, "dropUser user: " + userName);

      return getMS().dropUser(userName);
    }

    public DbPriv get_auth_on_db(String byWho, String who) throws TException,
        MetaException {
      return get_auth_on_db(byWho, who, MetaStoreUtils.DEFAULT_DATABASE_NAME);
    }

    public DbPriv getAuthOnDb(String byWho, String who) throws TException,
        MetaException {
      return getAuthOnDb(byWho, who, MetaStoreUtils.DEFAULT_DATABASE_NAME);
    }

    public DbPriv get_auth_on_db(String byWho, String who, String db)
        throws TException, MetaException {

      return getMS().getAuthOnDb(who, db);
    }

    public DbPriv getAuthOnDb(String byWho, String who, String db)
        throws TException, MetaException {
      this.incrementCounter("getAuthOnDb");
      logStartFunction(byWho, "get " + who + "'s auth on db: " + db);

      return getMS().getAuthOnDb(who, db);
    }

    public List<DbPriv> get_auth_on_dbs(String byWho, String who)
        throws TException, MetaException {

      return getMS().getAuthOnDbs(who);
    }

    public List<DbPriv> getAuthOnDbs(String byWho, String who)
        throws TException, MetaException {
      this.incrementCounter("getAuthOnDbs");
      logStartFunction(byWho, "get " + who + "'s auth on all db");

      return getMS().getAuthOnDbs(who);
    }

    public TblPriv get_auth_on_tbl(String byWho, String who, String tbl)
        throws TException, MetaException {
      return get_auth_on_tbl(byWho, who, MetaStoreUtils.DEFAULT_DATABASE_NAME,
          tbl);
    }

    public TblPriv getAuthOnTbl(String byWho, String who, String tbl)
        throws TException, MetaException {
      return getAuthOnTbl(byWho, who, MetaStoreUtils.DEFAULT_DATABASE_NAME, tbl);
    }

    public TblPriv get_auth_on_tbl(String byWho, String who, String db,
        String tbl) throws TException, MetaException {

      return getMS().getAuthOnTbl(who, db, tbl);
    }

    public TblPriv getAuthOnTbl(String byWho, String who, String db, String tbl)
        throws TException, MetaException {
      this.incrementCounter("getAuthOnTbl");
      logStartFunction(byWho, "get " + who + "'s auth on tbl: " + tbl
          + " in db: " + db);

      return getMS().getAuthOnTbl(who, db, tbl);
    }

    public List<TblPriv> get_auth_on_tbls(String byWho, String who)
        throws TException, MetaException {

      return getMS().getAuthOnTbls(who);
    }

    public List<TblPriv> getAuthOnTbls(String byWho, String who)
        throws TException, MetaException {
      this.incrementCounter("getAuthOnTbls");
      logStartFunction(byWho, "get " + who + "'s auth on all tbls");

      return getMS().getAuthOnTbls(who);
    }

    public List<DbPriv> get_db_auth(String byWho) throws TException,
        MetaException {
      return get_db_auth(byWho, MetaStoreUtils.DEFAULT_DATABASE_NAME);
    }

    public List<DbPriv> getDbAuth(String byWho) throws TException,
        MetaException {
      return getDbAuth(byWho, MetaStoreUtils.DEFAULT_DATABASE_NAME);
    }

    public List<DbPriv> get_db_auth(String byWho, String db) throws TException,
        MetaException {

      return getMS().getDbAuth(db);
    }

    public List<DbPriv> getDbAuth(String byWho, String db) throws TException,
        MetaException {
      this.incrementCounter("getDbAuth");
      logStartFunction(byWho, "get all auth on db: " + db);

      return getMS().getDbAuth(db);
    }

    public List<DbPriv> get_db_auth_all(String byWho) throws TException,
        MetaException {

      return getMS().getDbAuthAll();
    }

    public List<DbPriv> getDbAuthAll(String byWho) throws TException,
        MetaException {
      this.incrementCounter("getDbAuthAll");
      logStartFunction(byWho, "get all auth on dbs");

      return getMS().getDbAuthAll();
    }

    public Role get_role(String byWho, String roleName)
        throws NoSuchObjectException, TException, MetaException {

      return getMS().getRole(roleName);
    }

    public Role getRole(String byWho, String roleName)
        throws NoSuchObjectException, TException, MetaException {
      this.incrementCounter("getRole");
      logStartFunction(byWho, "get the role: " + roleName);

      return getMS().getRole(roleName);
    }

    public List<String> get_roles_all(String byWho) throws TException,
        MetaException {

      return getMS().getRolesAll();
    }

    public List<String> getRolesAll(String byWho) throws TException,
        MetaException {
      this.incrementCounter("getRolesAll");
      logStartFunction(byWho, "get all the roles");

      return getMS().getRolesAll();
    }

    public List<TblPriv> get_tbl_auth(String byWho, String tbl)
        throws TException, MetaException {
      return get_tbl_auth(byWho, MetaStoreUtils.DEFAULT_DATABASE_NAME, tbl);
    }

    public List<TblPriv> getTblAuth(String byWho, String tbl)
        throws TException, MetaException {
      return getTblAuth(byWho, MetaStoreUtils.DEFAULT_DATABASE_NAME, tbl);
    }

    public List<TblPriv> get_tbl_auth(String byWho, String db, String tbl)
        throws TException, MetaException {

      return getMS().getTblAuth(db, tbl);
    }

    public List<TblPriv> getTblAuth(String byWho, String db, String tbl)
        throws TException, MetaException {
      this.incrementCounter("getTblAuth");
      logStartFunction(byWho, "get the auths on tbl: " + tbl + " in db: " + db);

      return getMS().getTblAuth(db, tbl);
    }

    public List<TblPriv> get_tbl_auth_all(String byWho) throws TException,
        MetaException {

      return getMS().getTblAuthAll();
    }

    public List<TblPriv> getTblAuthAll(String byWho) throws TException,
        MetaException {
      this.incrementCounter("getTblAuthAll");
      logStartFunction(byWho, "get all the auths on tbls");

      return getMS().getTblAuthAll();
    }

    public User get_user(String byWho, String userName)
        throws NoSuchObjectException, TException, MetaException {

      return getMS().getUser(userName);
    }

    public User getUser(String byWho, String userName)
        throws NoSuchObjectException, TException, MetaException {
      this.incrementCounter("getUser");
      logStartFunction(byWho, "get the sys privileges of the user: " + userName);

      return getMS().getUser(userName);
    }

    public List<String> get_users_all(String byWho) throws TException,
        MetaException {

      return getMS().getUsersAll();
    }

    public List<String> getUsersAll(String byWho) throws TException,
        MetaException {
      this.incrementCounter("getUsersAll");
      logStartFunction(byWho, "get all the users' names");

      return getMS().getUsersAll();
    }

    public boolean grant_auth_on_db(String byWho, String forWho,
        List<String> privileges) throws NoSuchObjectException, TException,
        MetaException, InvalidObjectException {
      return grant_auth_on_db(byWho, forWho, privileges,
          MetaStoreUtils.DEFAULT_DATABASE_NAME);
    }

    public boolean grantAuthOnDb(String byWho, String forWho,
        List<String> privileges) throws NoSuchObjectException, TException,
        MetaException, InvalidObjectException {
      return grantAuthOnDb(byWho, forWho, privileges,
          MetaStoreUtils.DEFAULT_DATABASE_NAME);
    }

    public boolean grant_auth_on_db(String byWho, String forWho,
        List<String> privileges, String db) throws NoSuchObjectException,
        TException, MetaException, InvalidObjectException {
      this.incrementCounter("grant_auth_on_db");
      String privs = "";
      int privNum = privileges.size();
      int num = 0;
      for (String priv : privileges) {
        if (num < privNum - 1)
          privs += priv + ", ";
        else
          privs += priv;
        num++;
      }

      logStartFunction(byWho, "grant privileges: " + privs + " on db: " + db
          + " for: " + forWho);

      return getMS().grantAuthOnDb(forWho, privileges, db);
    }

    public boolean grantAuthOnDb(String byWho, String forWho,
        List<String> privileges, String db) throws NoSuchObjectException,
        TException, MetaException, InvalidObjectException {
      this.incrementCounter("grantAuthOnDb");

      String privs = "";
      int privNum = privileges.size();
      int num = 0;

      for (String priv : privileges) {
        if (num < privNum - 1)
          privs += priv + ", ";
        else
          privs += priv;
        num++;
      }

      logStartFunction(byWho, "grant privileges: " + privs + " on db: " + db
          + " for: " + forWho);

      return getMS().grantAuthOnDb(forWho, privileges, db);
    }

    public boolean grant_auth_on_tbl(String byWho, String forWho,
        List<String> privileges, String tbl) throws NoSuchObjectException,
        TException, InvalidObjectException, MetaException {
      return grant_auth_on_tbl(byWho, forWho, privileges,
          MetaStoreUtils.DEFAULT_DATABASE_NAME, tbl);
    }

    public boolean grantAuthOnTbl(String byWho, String forWho,
        List<String> privileges, String tbl) throws NoSuchObjectException,
        TException, InvalidObjectException, MetaException {
      return grantAuthOnTbl(byWho, forWho, privileges,
          MetaStoreUtils.DEFAULT_DATABASE_NAME, tbl);
    }

    public boolean grant_auth_on_tbl(String byWho, String forWho,
        List<String> privileges, String db, String tbl)
        throws NoSuchObjectException, TException, InvalidObjectException,
        MetaException {
      this.incrementCounter("grant_auth_on_tbl");
      String privs = "";
      int privNum = privileges.size();
      int num = 0;
      for (String priv : privileges) {
        if (num < privNum - 1)
          privs += priv + ", ";
        else
          privs += priv;
        num++;
      }

      logStartFunction(byWho, "grant privileges: " + privs + " on tbl: " + tbl
          + " in db:" + db + " for: " + forWho);

      return getMS().grantAuthOnTbl(forWho, privileges, db, tbl);
    }

    public boolean grantAuthOnTbl(String byWho, String forWho,
        List<String> privileges, String db, String tbl)
        throws NoSuchObjectException, TException, InvalidObjectException,
        MetaException {
      this.incrementCounter("grantAuthOnTbl");
      String privs = "";
      int privNum = privileges.size();
      int num = 0;
      for (String priv : privileges) {
        if (num < privNum - 1)
          privs += priv + ", ";
        else
          privs += priv;
        num++;
      }

      logStartFunction(byWho, "grant privileges: " + privs + " on tbl: " + tbl
          + " in db:" + db + " for: " + forWho);

      return getMS().grantAuthOnTbl(forWho, privileges, db, tbl);
    }

    public boolean grant_auth_role_sys(String byWho, String roleName,
        List<String> privileges) throws NoSuchObjectException, TException,
        InvalidObjectException, MetaException {
      this.incrementCounter("grant_auth_role_sys");
      String privs = "";
      int privNum = privileges.size();
      int num = 0;
      for (String priv : privileges) {
        if (num < privNum - 1)
          privs += priv + ", ";
        else
          privs += priv;
        num++;
      }

      logStartFunction(byWho, "grant sys privileges: " + privs + " for role: "
          + roleName);

      return getMS().grantAuthRoleSys(roleName, privileges);
    }

    public boolean grantAuthRoleSys(String byWho, String roleName,
        List<String> privileges) throws NoSuchObjectException, TException,
        InvalidObjectException, MetaException {
      this.incrementCounter("grant_auth_role_sys");
      String privs = "";
      int privNum = privileges.size();
      int num = 0;
      for (String priv : privileges) {
        if (num < privNum - 1)
          privs += priv + ", ";
        else
          privs += priv;
        num++;
      }

      logStartFunction(byWho, "grant sys privileges: " + privs + " for role: "
          + roleName);

      return getMS().grantAuthRoleSys(roleName, privileges);
    }

    public boolean grant_auth_sys(String byWho, String userName,
        List<String> privileges) throws NoSuchObjectException, TException,
        InvalidObjectException, MetaException {
      this.incrementCounter("grant_auth_sys");
      String privs = "";
      int privNum = privileges.size();
      int num = 0;
      for (String priv : privileges) {
        if (num < privNum - 1)
          privs += priv + ", ";
        else
          privs += priv;
        num++;
      }

      logStartFunction(byWho, "grant sys privileges: " + privs + " for user: "
          + userName);

      return getMS().grantAuthSys(userName, privileges);
    }

    public boolean grantAuthSys(String byWho, String userName,
        List<String> privileges) throws NoSuchObjectException, TException,
        InvalidObjectException, MetaException {
      this.incrementCounter("grantAuthSys");
      String privs = "";
      int privNum = privileges.size();
      int num = 0;
      for (String priv : privileges) {
        if (num < privNum - 1)
          privs += priv + ", ";
        else
          privs += priv;
        num++;
      }

      logStartFunction(byWho, "grant sys privileges: " + privs + " for user: "
          + userName);

      return getMS().grantAuthSys(userName, privileges);
    }

    public boolean grant_role_to_role(String byWho, String roleName,
        List<String> roleNames) throws NoSuchObjectException, TException,
        InvalidObjectException, MetaException {
      this.incrementCounter("grant_role_to_role");
      String roles = "";
      int roleNum = roleNames.size();
      int num = 0;
      for (String role : roleNames) {
        if (num < roleNum - 1)
          roles += role + ", ";
        else
          roles += role;
        num++;
      }

      logStartFunction(byWho, "grant role: " + roles + " to role: " + roleName);

      return getMS().grantRoleToRole(roleName, roleNames);
    }

    public boolean grantRoleToRole(String byWho, String roleName,
        List<String> roleNames) throws NoSuchObjectException, TException,
        InvalidObjectException, MetaException {
      this.incrementCounter("grantRoleToRole");
      String roles = "";
      int roleNum = roleNames.size();
      int num = 0;
      for (String role : roleNames) {
        if (num < roleNum - 1)
          roles += role + ", ";
        else
          roles += role;
        num++;
      }

      logStartFunction(byWho, "grant role: " + roles + " to role: " + roleName);

      return getMS().grantRoleToRole(roleName, roleNames);
    }

    public boolean grant_role_to_user(String byWho, String userName,
        List<String> roleNames) throws NoSuchObjectException, TException,
        InvalidObjectException, MetaException {
      this.incrementCounter("grant_role_to_user");
      String roles = "";
      int roleNum = roleNames.size();
      int num = 0;
      for (String role : roleNames) {
        if (num < roleNum - 1)
          roles += role + ", ";
        else
          roles += role;
        num++;
      }

      logStartFunction(byWho, "grant role: " + roles + " to user: " + userName);

      return getMS().grantRoleToUser(userName, roleNames);
    }

    public boolean grantRoleToUser(String byWho, String userName,
        List<String> roleNames) throws NoSuchObjectException, TException,
        InvalidObjectException, MetaException {
      this.incrementCounter("grant_role_to_user");
      String roles = "";
      int roleNum = roleNames.size();
      int num = 0;
      for (String role : roleNames) {
        if (num < roleNum - 1)
          roles += role + ", ";
        else
          roles += role;
        num++;
      }

      logStartFunction(byWho, "grant role: " + roles + " to user: " + userName);

      return getMS().grantRoleToUser(userName, roleNames);
    }

    public boolean is_a_role(String roleName) throws TException, MetaException {

      return getMS().isARole(roleName);
    }

    public boolean isARole(String roleName) throws TException, MetaException {
      this.incrementCounter("isARole");
      logStartFunction("judge a role role:" + roleName);
      return getMS().isARole(roleName);
    }

    public boolean is_a_user(String userName, String passwd) throws TException,
        MetaException {

      return getMS().isAUser(userName, passwd);
    }

    public boolean isAUser(String userName, String passwd) throws TException,
        MetaException {
      this.incrementCounter("isAUser");
      logStartFunction("judge a  user user:" + userName + "");
      return getMS().isAUser(userName, passwd);
    }

    public boolean revoke_auth_on_db(String byWho, String who,
        List<String> privileges) throws NoSuchObjectException, TException,
        InvalidObjectException, MetaException {
      return revoke_auth_on_db(byWho, who, privileges,
          MetaStoreUtils.DEFAULT_DATABASE_NAME);
    }

    public boolean revokeAuthOnDb(String byWho, String who,
        List<String> privileges) throws NoSuchObjectException, TException,
        InvalidObjectException, MetaException {
      return revokeAuthOnDb(byWho, who, privileges,
          MetaStoreUtils.DEFAULT_DATABASE_NAME);
    }

    public boolean revoke_auth_on_db(String byWho, String who,
        List<String> privileges, String db) throws NoSuchObjectException,
        TException, InvalidObjectException, MetaException {
      this.incrementCounter("revoke_auth_on_db");
      String privs = "";
      int privNum = privileges.size();
      int num = 0;
      for (String priv : privileges) {
        if (num < privNum - 1)
          privs += priv + ", ";
        else
          privs += priv;
        num++;
      }

      logStartFunction(byWho, "revoke privileges: " + privs + " on db: " + db
          + " from user/role: " + who);

      return getMS().revokeAuthOnDb(who, privileges, db);
    }

    public boolean revokeAuthOnDb(String byWho, String who,
        List<String> privileges, String db) throws NoSuchObjectException,
        TException, InvalidObjectException, MetaException {
      this.incrementCounter("revokeAuthOnDb");
      String privs = "";
      int privNum = privileges.size();
      int num = 0;
      for (String priv : privileges) {
        if (num < privNum - 1)
          privs += priv + ", ";
        else
          privs += priv;
        num++;
      }

      logStartFunction(byWho, "revoke privileges: " + privs + " on db: " + db
          + " from user/role: " + who);

      return getMS().revokeAuthOnDb(who, privileges, db);
    }

    public boolean revoke_auth_on_tbl(String byWho, String who,
        List<String> privileges, String tbl) throws NoSuchObjectException,
        TException, InvalidObjectException, MetaException {
      return revoke_auth_on_tbl(byWho, who, privileges,
          MetaStoreUtils.DEFAULT_DATABASE_NAME, tbl);
    }

    public boolean revokeAuthOnTbl(String byWho, String who,
        List<String> privileges, String tbl) throws NoSuchObjectException,
        TException, InvalidObjectException, MetaException {
      return revokeAuthOnTbl(byWho, who, privileges,
          MetaStoreUtils.DEFAULT_DATABASE_NAME, tbl);
    }

    public boolean revoke_auth_on_tbl(String byWho, String who,
        List<String> privileges, String db, String tbl)
        throws NoSuchObjectException, TException, InvalidObjectException,
        MetaException {
      this.incrementCounter("revoke_auth_on_tbl");
      String privs = "";
      int privNum = privileges.size();
      int num = 0;
      for (String priv : privileges) {
        if (num < privNum - 1)
          privs += priv + ", ";
        else
          privs += priv;
        num++;
      }

      logStartFunction(byWho, "revoke privileges: " + privs + " on tbl: " + tbl
          + "in db: " + db + " from user/role: " + who);

      return getMS().revokeAuthOnTbl(who, privileges, db, tbl);
    }

    public boolean revokeAuthOnTbl(String byWho, String who,
        List<String> privileges, String db, String tbl)
        throws NoSuchObjectException, TException, InvalidObjectException,
        MetaException {
      this.incrementCounter("revokeAuthOnTbl");
      String privs = "";
      int privNum = privileges.size();
      int num = 0;
      for (String priv : privileges) {
        if (num < privNum - 1)
          privs += priv + ", ";
        else
          privs += priv;
        num++;
      }

      logStartFunction(byWho, "revoke privileges: " + privs + " on tbl: " + tbl
          + "in db: " + db + " from user/role: " + who);

      return getMS().revokeAuthOnTbl(who, privileges, db, tbl);
    }

    public boolean revoke_auth_role_sys(String byWho, String roleName,
        List<String> privileges) throws NoSuchObjectException, TException,
        InvalidObjectException, MetaException {
      this.incrementCounter("revoke_auth_role_sys");
      String privs = "";
      int privNum = privileges.size();
      int num = 0;
      for (String priv : privileges) {
        if (num < privNum - 1)
          privs += priv + ", ";
        else
          privs += priv;
        num++;
      }
      logStartFunction(byWho, "revoke sys privileges: " + privs
          + " from role: " + roleName);

      return getMS().revokeAuthRoleSys(roleName, privileges);
    }

    public boolean revokeAuthRoleSys(String byWho, String roleName,
        List<String> privileges) throws NoSuchObjectException, TException,
        InvalidObjectException, MetaException {
      this.incrementCounter("revokeAuthRoleSys");
      String privs = "";
      int privNum = privileges.size();
      int num = 0;
      for (String priv : privileges) {
        if (num < privNum - 1)
          privs += priv + ", ";
        else
          privs += priv;
        num++;
      }
      logStartFunction(byWho, "revoke sys privileges: " + privs
          + " from role: " + roleName);

      return getMS().revokeAuthRoleSys(roleName, privileges);
    }

    public boolean revoke_auth_sys(String byWho, String userName,
        List<String> privileges) throws NoSuchObjectException, TException,
        InvalidObjectException, MetaException {
      this.incrementCounter("revoke_auth_sys");
      String privs = "";
      int privNum = privileges.size();
      int num = 0;
      for (String priv : privileges) {
        if (num < privNum - 1)
          privs += priv + ", ";
        else
          privs += priv;
        num++;
      }

      logStartFunction(byWho, "revoke sys privileges: " + privs
          + " from user: " + userName);

      return getMS().revokeAuthSys(userName, privileges);
    }

    public boolean revokeAuthSys(String byWho, String userName,
        List<String> privileges) throws NoSuchObjectException, TException,
        InvalidObjectException, MetaException {
      this.incrementCounter("revokeAuthSys");
      String privs = "";
      int privNum = privileges.size();
      int num = 0;
      for (String priv : privileges) {
        if (num < privNum - 1)
          privs += priv + ", ";
        else
          privs += priv;
        num++;
      }

      logStartFunction(byWho, "revoke sys privileges: " + privs
          + " from user: " + userName);

      return getMS().revokeAuthSys(userName, privileges);
    }

    public boolean revoke_role_from_role(String byWho, String roleName,
        List<String> roleNames) throws NoSuchObjectException, TException,
        InvalidObjectException, MetaException {
      this.incrementCounter("revoke_role_from_role");
      String roles = "";
      int roleNum = roleNames.size();
      int num = 0;
      for (String role : roleNames) {
        if (num < roleNum - 1)
          roles += role + ", ";
        else
          roles += role;
        num++;
      }

      logStartFunction(byWho, "revoke roles: " + roles + " from role: "
          + roleName);

      return getMS().revokeRoleFromRole(roleName, roleNames);
    }

    public boolean revokeRoleFromRole(String byWho, String roleName,
        List<String> roleNames) throws NoSuchObjectException, TException,
        InvalidObjectException, MetaException {
      this.incrementCounter("revokeRoleFromRole");
      String roles = "";
      int roleNum = roleNames.size();
      int num = 0;
      for (String role : roleNames) {
        if (num < roleNum - 1)
          roles += role + ", ";
        else
          roles += role;
        num++;
      }

      logStartFunction(byWho, "revoke roles: " + roles + " from role: "
          + roleName);

      return getMS().revokeRoleFromRole(roleName, roleNames);
    }

    public boolean revoke_role_from_user(String byWho, String userName,
        List<String> roleNames) throws NoSuchObjectException, TException,
        InvalidObjectException, MetaException {
      this.incrementCounter("revoke_role_from_user");
      String roles = "";
      int roleNum = roleNames.size();
      int num = 0;
      for (String role : roleNames) {
        if (num < roleNum - 1)
          roles += role + ", ";
        else
          roles += role;
        num++;
      }

      logStartFunction(byWho, "revoke roles: " + roles + " from user: "
          + userName);

      return getMS().revokeRoleFromUser(userName, roleNames);
    }

    public boolean revokeRoleFromUser(String byWho, String userName,
        List<String> roleNames) throws NoSuchObjectException, TException,
        InvalidObjectException, MetaException {
      this.incrementCounter("revokeRoleFromUser");
      String roles = "";
      int roleNum = roleNames.size();
      int num = 0;
      for (String role : roleNames) {
        if (num < roleNum - 1)
          roles += role + ", ";
        else
          roles += role;
        num++;
      }

      logStartFunction(byWho, "revoke roles: " + roles + " from user: "
          + userName);

      return getMS().revokeRoleFromUser(userName, roleNames);
    }

    public boolean set_passwd(String byWho, String forWho, String newPasswd)
        throws NoSuchObjectException, TException, MetaException {

      logStartFunction(byWho, "set passwd for user: " + forWho);
      return getMS().setPasswd(forWho, newPasswd);
    }

    public boolean setPasswd(String byWho, String forWho, String newPasswd)
        throws NoSuchObjectException, TException, MetaException {
      this.incrementCounter("setPasswd");
      logStartFunction(byWho, "set passwd for user: " + forWho);
      return getMS().setPasswd(forWho, newPasswd);
    }

    public boolean drop_auth_in_db(String byWho, String forWho)
        throws TException, MetaException {
      logStartFunction(byWho, "drop auth in DbPriv table for user: " + forWho);
      return getMS().dropAuthInDb(forWho);
    }

    public boolean dropAuthInDb(String byWho, String forWho) throws TException,
        MetaException {
      this.incrementCounter("dropAuthInDb");
      logStartFunction(byWho, "drop auth in DbPriv table for user: " + forWho);
      return getMS().dropAuthInDb(forWho);
    }

    public boolean drop_auth_in_tbl(String byWho, String forWho)
        throws TException, MetaException {
      logStartFunction(byWho, "drop auth in TblPriv table for user: " + forWho);
      return getMS().dropAuthInTbl(forWho);
    }

    public boolean dropAuthInTbl(String byWho, String forWho)
        throws TException, MetaException {
      this.incrementCounter("dropAuthInTbl");
      logStartFunction(byWho, "drop auth in TblPriv table for user: " + forWho);
      return getMS().dropAuthInTbl(forWho);
    }

    public boolean drop_auth_on_db(String byWho, String forWho, String db)
        throws TException, MetaException {
      logStartFunction(byWho, "drop auth on db: " + db
          + " in DbPriv table for user: " + forWho);
      return getMS().dropAuthOnDb(forWho, db);
    }

    public boolean dropAuthOnDb(String byWho, String forWho, String db)
        throws TException, MetaException {
      this.incrementCounter("dropAuthOnDb");
      logStartFunction(byWho, "drop auth on db: " + db
          + " in DbPriv table for user: " + forWho);
      return getMS().dropAuthOnDb(forWho, db);
    }

    public boolean drop_auth_on_tbl(String byWho, String forWho, String db,
        String tbl) throws TException, MetaException {
      logStartFunction(byWho, "drop auth on table: " + tbl + " in db: " + db
          + " in TblPriv table for user: " + forWho);
      return getMS().dropAuthOnTbl(forWho, db, tbl);
    }

    public boolean dropAuthOnTbl(String byWho, String forWho, String db,
        String tbl) throws TException, MetaException {
      this.incrementCounter("dropAuthOnTbl");
      logStartFunction(byWho, "drop auth on table: " + tbl + " in db: " + db
          + " in TblPriv table for user: " + forWho);
      return getMS().dropAuthOnTbl(forWho, db, tbl);
    }

    private void createDefaultGroup() throws MetaException {
      if (createDefaultGroup) {
        return;
      }
      try {
        if (getMS().findGroup(HiveMetaStore.DEFAULT) == null) {
          getMS().addUserGroup(
              new group(HiveMetaStore.DEFAULT, ROOT_USER, null, 0), ROOT_USER);
        }
      } catch (Exception e) {
        try {
          getMS().addUserGroup(
              new group(HiveMetaStore.DEFAULT, ROOT_USER, null, 0), ROOT_USER);
        } catch (Exception ee) {
          throw new MetaException("Failed to create the default group!");
        }
      }
      createDefaultGroup = true;
    }

    private void createRootUser() throws MetaException {
      if (HMSHandler.createdRootUser || !checkForRootUser) {
        return;
      }

      try {
        getMS().getUser(ROOT_USER);
      } catch (NoSuchObjectException e) {
        try {
          LOG.info("no root user,will create ...");
          getMS().createUser(ROOT_USER, HMSHandler.rootPasswd);
          List<String> privileges = new ArrayList<String>();
          privileges.add("TOK_DBA_PRI");
          getMS().grantAuthSys(ROOT_USER, privileges);

        } catch (Exception ee) {
          ee.printStackTrace();
          throw new MetaException("Failed to create the ROOT user!");
        }
      }
      HMSHandler.createdRootUser = true;
    }

    public boolean create_index(IndexItem index) throws MetaException {
      Path indexPath = null;
      boolean madeIndexDir = false;
      boolean success = false;
      try {
        String indexName = index.getName();
        String fieldList = "";
        int type = 1;

        getMS().openTransaction();

        indexPath = wh.getDefaultIndexPath(index.getDb(), index.getTbl(),
            index.getName());
        if (!wh.mkdirs(indexPath)) {
          throw new MetaException("Index path " + indexPath
              + " is not a directory or unable to create one.");
        }

        index.setLocation(indexPath.toString());

        madeIndexDir = true;

        Set<String> priPartNames = index.getPartPath();
        if (priPartNames != null) {
          List<Path> indexPartPath = wh.getPartitionPaths(indexPath,
              priPartNames, null);
          for (Path partPath : indexPartPath) {
            if (!wh.mkdirs(partPath)) {
              throw new MetaException("Index Partition path " + partPath
                  + " is not a directory or unable to create one.");
            }
          }
        }

        logStartFunction("create index: " + indexName + ",db:" + index.getDb()
            + ",table:" + index.getTbl());

        getMS().createIndex(index);

        success = getMS().commitTransaction();
      } finally {
        if (!success) {
          getMS().rollbackTransaction();

          if (madeIndexDir) {
            wh.deleteDir(indexPath, true);
          }
        }
      }

      return success;
    }

    public boolean createIndex(IndexItem index) throws MetaException {
      this.incrementCounter("dropIndex");
      Path indexPath = null;
      boolean madeIndexDir = false;
      boolean success = false;
      try {
        String indexName = index.getName();
        String fieldList = "";
        int type = 1;

        getMS().openTransaction();

        indexPath = wh.getDefaultIndexPath(index.getDb(), index.getTbl(),
            index.getName());
        if (!wh.mkdirs(indexPath)) {
          throw new MetaException("Index path " + indexPath
              + " is not a directory or unable to create one.");
        }

        index.setLocation(indexPath.toString());

        madeIndexDir = true;

        Set<String> priPartNames = index.getPartPath();
        if (priPartNames != null) {
          List<Path> indexPartPath = wh.getPartitionPaths(indexPath,
              priPartNames, null);
          for (Path partPath : indexPartPath) {
            if (!wh.mkdirs(partPath)) {
              throw new MetaException("Index Partition path " + partPath
                  + " is not a directory or unable to create one.");
            }
          }
        }

        logStartFunction("create index: " + indexName + ",db:" + index.getDb()
            + ",table:" + index.getTbl());

        getMS().createIndex(index);

        success = getMS().commitTransaction();
      } finally {
        if (!success) {
          getMS().rollbackTransaction();

          if (madeIndexDir) {
            wh.deleteDir(indexPath, true);
          }
        }
      }

      return success;
    }

    public boolean drop_index(String db, String table, String name)
        throws MetaException {
      logStartFunction("drop index: " + name + ",db:" + db + ",table:" + table);

      Path indexPath = null;
      boolean madeIndexDir = false;
      boolean success = false;
      try {
        getMS().openTransaction();

        IndexItem item = getMS().getIndexInfo(db, table, name);
        if (item == null) {
          getMS().commitTransaction();
          return true;
        }

        if (item.getLocation() != null) {
          indexPath = new Path(item.getLocation());
        }

        getMS().dropIndex(db, table, name);
        success = getMS().commitTransaction();

      } finally {
        if (!success) {
          getMS().rollbackTransaction();
        } else if (indexPath != null) {
          wh.deleteDir(indexPath, true);
        }
      }

      return success;
    }

    public boolean dropIndex(String db, String table, String name)
        throws MetaException {
      this.incrementCounter("dropIndex");
      logStartFunction("drop index: " + name + ",db:" + db + ",table:" + table);

      Path indexPath = null;
      boolean madeIndexDir = false;
      boolean success = false;
      try {
        getMS().openTransaction();

        IndexItem item = getMS().getIndexInfo(db, table, name);
        if (item == null) {
          getMS().commitTransaction();
          return true;
        }

        if (item.getLocation() != null) {
          indexPath = new Path(item.getLocation());
        }

        getMS().dropIndex(db, table, name);
        success = getMS().commitTransaction();

      } finally {
        if (!success) {
          getMS().rollbackTransaction();
        } else if (indexPath != null) {
          wh.deleteDir(indexPath, true);
        }
      }

      return success;
    }

    public int get_index_num(String db, String table) throws MetaException {
      logStartFunction("get index num, db:" + db + ",table:" + table);
      return getMS().getIndexNum(db, table);
    }

    public int get_index_type(String db, String table, String name)
        throws MetaException {
      logStartFunction("get index type, name:" + name + ",db:" + db + ",table:"
          + table);
      return getMS().getIndexType(db, table, name);
    }

    public String get_index_field(String db, String table, String name)
        throws MetaException {
      logStartFunction("get index field, name:" + name + ",db:" + db
          + ",table:" + table);
      return getMS().getIndexField(db, table, name);
    }

    public String get_index_location(String db, String table, String name)
        throws MetaException {
      logStartFunction("get index location, name:" + name + ",db:" + db
          + ",table:" + table);
      return getMS().getIndexLocation(db, table, name);
    }

    public boolean set_index_location(String db, String table, String name,
        String location) throws MetaException {
      logStartFunction("set index location, name:" + name + ",db:" + db
          + ",table:" + table);
      return getMS().setIndexLocation(db, table, name, location);
    }

    public boolean set_index_status(String db, String table, String name,
        int status) throws MetaException {
      logStartFunction("set index status, name:" + name + ",db:" + db
          + ",table:" + table + ",new status:" + status);
      return getMS().setIndexStatus(db, table, name, status);
    }

    public List<IndexItem> get_all_index_table(String db, String table)
        throws MetaException {
      logStartFunction("get all index in table" + ",db:" + db + ",table:"
          + table);
      return getMS().getAllIndexTable(db, table);
    }

    public IndexItem get_index_info(String db, String table, String name)
        throws MetaException {
      logStartFunction("get index info:" + name + ",db:" + db + ",table:"
          + table);
      return getMS().getIndexInfo(db, table, name);
    }

    public List<IndexItem> get_all_index_sys() throws MetaException {
      logStartFunction("get all index sys");
      return getMS().getAllIndexSys();
    }

    public int getIndexNum(String db, String table) throws MetaException {
      this.incrementCounter("getIndexNum");
      logStartFunction("get index num, db:" + db + ",table:" + table);
      return getMS().getIndexNum(db, table);
    }

    public int getIndexType(String db, String table, String name)
        throws MetaException {
      this.incrementCounter("getIndexType");
      logStartFunction("get index type, name:" + name + ",db:" + db + ",table:"
          + table);
      return getMS().getIndexType(db, table, name);
    }

    public String getIndexField(String db, String table, String name)
        throws MetaException {
      this.incrementCounter("getIndexField");
      logStartFunction("get index field, name:" + name + ",db:" + db
          + ",table:" + table);
      return getMS().getIndexField(db, table, name);
    }

    public String getIndexLocation(String db, String table, String name)
        throws MetaException {
      this.incrementCounter("getIndexLocation");
      logStartFunction("get index location, name:" + name + ",db:" + db
          + ",table:" + table);
      return getMS().getIndexLocation(db, table, name);
    }

    public boolean setIndexLocation(String db, String table, String name,
        String location) throws MetaException {
      this.incrementCounter("setIndexLocation");
      logStartFunction("set index location, name:" + name + ",db:" + db
          + ",table:" + table);
      return getMS().setIndexLocation(db, table, name, location);
    }

    public boolean setIndexStatus(String db, String table, String name,
        int status) throws MetaException {
      this.incrementCounter("setIndexStatus");
      logStartFunction("set index status, name:" + name + ",db:" + db
          + ",table:" + table + ",new status:" + status);
      return getMS().setIndexStatus(db, table, name, status);
    }

    public List<IndexItem> getAllIndexTable(String db, String table)
        throws MetaException {
      this.incrementCounter("getAllIndexTable");
      logStartFunction("get all index in table" + ",db:" + db + ",table:"
          + table);
      return getMS().getAllIndexTable(db, table);
    }

    public IndexItem getIndexInfo(String db, String table, String name)
        throws MetaException {
      this.incrementCounter("getIndexInfo");
      logStartFunction("get index info:" + name + ",db:" + db + ",table:"
          + table);
      return getMS().getIndexInfo(db, table, name);
    }

    public List<IndexItem> getAllIndexSys() throws MetaException {
      this.incrementCounter("getAllIndexSys");
      logStartFunction("get all index sys");
      return getMS().getAllIndexSys();
    }

    @Override
    public List<String> get_tables_jdbc(String db, String pattern)
        throws MetaException, TException {
      this.incrementCounter("get_tables");
      logStartFunction("get_tables: db=" + db + " pat=" + pattern);
      return getMS().getJdbcTables(db, pattern);
    }

    public List<String> getTablesJdbc(String db, String pattern)
        throws MetaException, TException {
      this.incrementCounter("get_tables");
      logStartFunction("get_tables: db=" + db + " pat=" + pattern);
      return getMS().getJdbcTables(db, pattern);
    }

    @Override
    public Map<String, Type> getTypeAll(String name) throws MetaException,
        TException {
      this.incrementCounter("getTypeAll");
      logStartFunction("getTypeAll");
      throw new MetaException("Not yet implemented");
    }

    @Override
    public void addPartition(String dbName, String tblName,
        AddPartitionDesc addPartitionDesc) throws InvalidOperationException,
        MetaException, TException {
      this.incrementCounter("addPartition");
      logStartFunction("addPartition");

      try {
        getMS().addPartition(dbName, tblName, addPartitionDesc);
      } catch (InvalidObjectException e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new InvalidOperationException(StringUtils.stringifyException(e));
      }
    }

    @Override
    public void dropPartition(String dbName, String tblName,
        DropPartitionDesc dropPartitionDesc) throws InvalidOperationException,
        MetaException, TException {
      this.incrementCounter("dropPartition");
      logStartFunction("dropPartition");

      try {
        getMS().dropPartition(dbName, tblName, dropPartitionDesc);
      } catch (InvalidObjectException e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new InvalidOperationException(StringUtils.stringifyException(e));
      }
    }

    @Override
    public void addDefaultPartition(String dbName, String tblName, int level)
        throws InvalidOperationException, MetaException, TException {
      this.incrementCounter("addDefaultPartition");
      logStartFunction("addDefaultPartition");

      try {
        getMS().addDefaultPartition(dbName, tblName, level);
      } catch (InvalidObjectException e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new InvalidOperationException(StringUtils.stringifyException(e));
      }
    }

    @Override
    public void dropDefaultPartition(String dbName, String tblName, int level)
        throws InvalidOperationException, MetaException, TException {
      this.incrementCounter("dropDefaultPartition");
      logStartFunction("dropDefaultPartition");

      try {
        getMS().dropDefaultPartition(dbName, tblName, level);
      } catch (InvalidObjectException e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new InvalidOperationException(StringUtils.stringifyException(e));
      }
    }

    @Override
    public void renameTable(String dbName, String tblName, String modifyUser,
        String newName) throws InvalidOperationException, MetaException,
        TException {
      this.incrementCounter("renameTable");
      logStartFunction("renameTable");

      getMS().renameTable(dbName, tblName, modifyUser, newName);
    }

    @Override
    public void addCols(String dbName, String tblName, String modifyUser,
        List<FieldSchema> newCols) throws InvalidOperationException,
        MetaException, TException {
      this.incrementCounter("addCols");
      logStartFunction("addCols");

      try {
        getMS().addCols(dbName, tblName, modifyUser, newCols);
      } catch (InvalidObjectException e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new InvalidOperationException(StringUtils.stringifyException(e));
      }
    }

    @Override
    public void renameCols(String dbName, String tblName,
        RenameColDesc renameColDesc) throws InvalidOperationException,
        MetaException, TException {
      this.incrementCounter("renameCols");
      logStartFunction("renameCols");

      try {
        getMS().renameCols(dbName, tblName, renameColDesc);
      } catch (InvalidObjectException e) {
        LOG.error(StringUtils.stringifyException(e));
        throw new InvalidOperationException(StringUtils.stringifyException(e));
      }
    }

    @Override
    public void replaceCols(String dbName, String tblName, String modifyUser,
        List<FieldSchema> newCols) throws InvalidOperationException,
        MetaException, TException {
      this.incrementCounter("replaceCols");
      logStartFunction("replaceCols");

      getMS().replaceCols(dbName, tblName, modifyUser, newCols);
    }

    @Override
    public void addTblProps(String dbName, String tblName, String modifyUser,
        Map<String, String> props) throws InvalidOperationException,
        MetaException, TException {
      this.incrementCounter("addTblProps");
      logStartFunction("addTblProps");

      getMS().addTblProps(dbName, tblName, modifyUser, props);
    }

    @Override
    public void addSerdeProps(String dbName, String tblName, String modifyUser,
        Map<String, String> props) throws InvalidOperationException,
        MetaException, TException {
      this.incrementCounter("addSerdeProps");
      logStartFunction("addSerdeProps");

      getMS().addSerdeProps(dbName, tblName, modifyUser, props);
    }

    @Override
    public void addSerde(String dbName, String tblName,
        AddSerdeDesc addSerdeDesc) throws InvalidOperationException,
        MetaException, TException {
      this.incrementCounter("addSerde");
      logStartFunction("addSerde");

      getMS().addSerde(dbName, tblName, addSerdeDesc);
    }

    @Override
    public boolean isTableExist(String dbName, String tblName)
        throws MetaException, TException {
      this.incrementCounter("isTableExist");
      logStartFunction("isTableExist");

      return getMS().isTableExit(dbName, tblName);
    }

    @Override
    public List<List<String>> getPartitionNames(String db, String tbl, int max)
        throws InvalidOperationException, MetaException, TException {
      this.incrementCounter("getPartitionNames");
      logStartFunction("getPartitionNames");

      return getMS().getPartitionNames(db, tbl);
    }

    @Override
    public void modifyTableComment(String dbName, String tblName, String comment)
        throws InvalidOperationException, MetaException, TException {
      this.incrementCounter("modifyTableComment");
      logStartFunction("modifyTableComment");

      getMS().modifyTableComment(dbName, tblName, comment);
    }

    @Override
    public void modifyColumnComment(String dbName, String tblName,
        String colName, String comment) throws InvalidOperationException,
        MetaException, TException {
      this.incrementCounter("modifyColumnComment");
      logStartFunction("modifyColumnComment");

      getMS().modifyColumnComment(dbName, tblName, colName, comment);
    }

    @Override
    public boolean isView(String dbName, String tblName)
        throws NoSuchObjectException, MetaException, TException {
      this.incrementCounter("isView");
      logStartFunction("isView");

      return getMS().isView(dbName, tblName);
    }

    @Override
    public boolean isAUserName(String userName) throws NoSuchObjectException,
        MetaException, TException {
      this.incrementCounter("isAUserName");
      logStartFunction("isAUserName");

      return getMS().isAUserName(userName);
    }

    @Override
    public boolean createDatabase(String name, String description,
        String hdfsscheme, String metastore) throws AlreadyExistsException,
        MetaException, TException {
      this.incrementCounter("createDatabase");
      logStartFunction("createDatabase: " + name);

      Database db = new Database(name, description, hdfsscheme, metastore, null);
      return getMS().createDatabase(db);
    }

    @Override
    public boolean hasAuth(String who, int priv) throws MetaException,
        TException {
      this.incrementCounter("hasAuth");
      logStartFunction("hasAuth: who=" + who + ", priv=" + priv);

      try {
        return getMS().hasAuth(who, priv);
      } catch (NoSuchObjectException e) {
        return false;
      }
    }
    
    @Override
    public boolean hasAuthOnLocation(String who, String location) 
        throws MetaException, TException {
      this.incrementCounter("hasAuthOnLocation");
      logStartFunction("hasAuthOnLocation: who=" + who + ", location=" + location);
      
      try {
        return getMS().hasAuthOnLocation(who, location);
      } catch (NoSuchObjectException e) {
        return false;
      }
    }

    @Override
    public boolean hasAuthOnDb(String who, String db, int priv)
        throws MetaException, TException {
      this.incrementCounter("hasAuth");
      logStartFunction("hasAuth: who=" + who + ", db=" + db + ", priv=" + priv);

      try {
        return getMS().hasAuthOnDb(who, db, priv);
      } catch (NoSuchObjectException e) {
        return false;
      }
    }

    @Override
    public boolean hasAuthOnTbl(String who, String db, String tbl, int priv)
        throws MetaException, TException {
      this.incrementCounter("hasAuth");
      logStartFunction("hasAuth: who=" + who + ", db=" + db + ", tbl=" + tbl
          + ", priv=" + priv);

      try {
        return getMS().hasAuthOnTbl(who, db, tbl, priv);
      } catch (NoSuchObjectException e) {
        return false;
      }
    }

    @Override
    public boolean hasAuthWithRole(String who, String role, int priv)
        throws MetaException, TException {
      this.incrementCounter("hasAuthWithRole");
      logStartFunction("hasAuthWithRole: who=" + who + ", role=" + role
          + ", priv=" + priv);

      try {
        return getMS().hasAuthWithRole(who, role, priv);
      } catch (NoSuchObjectException e) {
        return false;
      }
    }

    @Override
    public boolean hasAuthOnDbWithRole(String who, String role, String db,
        int priv) throws MetaException, TException {
      this.incrementCounter("hasAuthOnDbWithRole");
      logStartFunction("hasAuthOnDbWithRole: who=" + who + ", role=" + role
          + ", db=" + db + ", priv=" + priv);

      try {
        return getMS().hasAuthOnDbWithRole(who, role, db, priv);
      } catch (NoSuchObjectException e) {
        return false;
      }
    }

    @Override
    public boolean hasAuthOnTblWithRole(String who, String role, String db,
        String tbl, int priv) throws MetaException, TException {
      this.incrementCounter("hasAuthOnTblWithRole");
      logStartFunction("hasAuthOnTblWithRole: who=" + who + ", role=" + role
          + ", db=" + db + ", tbl=" + tbl + ", priv=" + priv);

      try {
        return getMS().hasAuthOnTblWithRole(who, role, db, tbl, priv);
      } catch (NoSuchObjectException e) {
        return false;
      }
    }

    @Override
    public boolean createDatabaseDb(Database db) throws AlreadyExistsException,
        MetaException, TException {

      this.incrementCounter("createDatabaseDb");

      logStartFunction("createDatabaseDb: " + db.getName());

      return getMS().createDatabase(db);
    }

    @Override
    public List<String> getPlayRoles(String byWho)
        throws NoSuchObjectException, MetaException, TException {

      this.incrementCounter("getPlayRoles");

      logStartFunction("getPlayRoles: " + byWho);

      return getMS().getPlayRoles(byWho);
    }

    @Override
    public boolean updatePBInfo(String dbName, String tableName,
        String modifiedTime) throws MetaException, TException {

      this.incrementCounter("updatePBInfo");

      logStartFunction("updatePBInfo: dbName=" + dbName + ", tableName="
          + tableName + ", modifiedTime=" + modifiedTime);

      try {
        return getMS().updatePBInfo(dbName, tableName, modifiedTime);
      } catch (InvalidOperationException e) {
        e.printStackTrace();
      }
      return false;
    }

    @Override
    public List<String> getDatabasesWithOwner(String owner)
        throws MetaException, TException {
      this.incrementCounter("get_databases");
      logStartFunction("get_databases");
      return getMS().getDatabasesWithOwner(owner);
    }

    @Override
    public boolean isPBTable(String dbName, String tableName)
        throws MetaException, TException {
      this.incrementCounter("isPBTable");

      logStartFunction("isPBTable: dbName=" + dbName + ", tableName="
          + tableName);
      return getMS().isPBTable(dbName, tableName);
    }

    @Override
    public List<String> getDbsByPriv(String user, String passwd)
        throws MetaException, TException {
      this.incrementCounter("getDbsByPriv");

      logStartFunction("getDbsByPriv: " + user);

      return getMS().getDbsByPriv(user, passwd);
    }

    @Override
    public Map<String, String> getTblsByPriv(String user, String passwd,
        String db) throws MetaException, NoSuchObjectException, TException {
      this.incrementCounter("getTblsByPriv");

      logStartFunction("getTblsByPriv: " + user);

      return getMS().getTblsByPriv(user, passwd, db);
    }

    @Override
    public TableInfo getTblInfo(String user, String passwd, String db,
        String tbl) throws MetaException, NoSuchObjectException, TException {
      this.incrementCounter("getTblInfo");

      logStartFunction("getTblInfo: " + user);

      return getMS().getTblInfo(user, passwd, db, tbl);
    }

    @Override
    public List<TableInfo> getAllTblsByPriv(String user, String passwd)
        throws MetaException, NoSuchObjectException, TException {
      // TODO Auto-generated method stub
      this.incrementCounter("getAllTblsByPriv");

      logStartFunction("getAllTblsByPriv: " + user);

      // boolean success = false;

      return getMS().getAllTblsByPriv(user, passwd);
    }

    @Override
    public Map<String, TableInfo> getTblsByPrivWithParams(String user,
        String passwd, String db) throws MetaException, NoSuchObjectException,
        TException {
      // TODO Auto-generated method stub
      this.incrementCounter("getTblsByPrivWithParams");

      logStartFunction("getTblsByPrivWithParams: " + user);

      // boolean success = false;

      return getMS().getTblsByPrivWithParams(user, passwd, db);
    }

    @Override
    public Map<String, TableInfo> getAllTblsByPrivWithKeyword(String user,
        String passwd, String db, String keyWord) throws MetaException,
        NoSuchObjectException, TException {
      // TODO Auto-generated method stub
      this.incrementCounter("getAllTblsByPrivWithKeyword");

      logStartFunction("getAllTblsByPrivWithKeyword: " + user);

      // boolean success = false;
      return getMS().getAllTblsByPrivWithKeyword(user, passwd, db, keyWord);
    }

    @Override
    public List<String> getDbsByOwner(String user, String passwd)
        throws MetaException, NoSuchObjectException, TException {
      // TODO Auto-generated method stub
      this.incrementCounter("getDbsByOwner");

      logStartFunction("getDbsByOwner: " + user);

      // boolean success = false;
      return getMS().getDbsByOwner(user, passwd);
    }

    @Override
    public boolean isHdfsExternalTable(String dbName, String tableName)
        throws MetaException, TException {
      this.incrementCounter("isHdfsExternalTable");

      logStartFunction("isHdfsExternalTable: dbName=" + dbName + ", tableName="
          + tableName);
      return getMS().isHdfsExternalTable(dbName, tableName);
      // TODO Auto-generated method stub
    }

  }

  public static void main(String[] args) {
    int port = 9083;

    if (args.length > 0) {
      port = Integer.getInteger(args[0]);
    }

    try {
      TServerTransport serverTransport = new TServerSocket(port);
      Iface handler = new HMSHandler("new db based metaserver");
      FacebookService.Processor processor = new ThriftHiveMetastore.Processor(
          handler);

      Args serverArgs = new Args(serverTransport);

      serverArgs.inputTransportFactory(new TTransportFactory());

      serverArgs.outputTransportFactory(new TTransportFactory());

      serverArgs.inputProtocolFactory(new TBinaryProtocol.Factory());

      serverArgs.outputProtocolFactory(new TBinaryProtocol.Factory());

      serverArgs.processor(processor);

      serverArgs.maxWorkerThreads = 200;

      TServer server = new TThreadPoolServer(serverArgs);

      HMSHandler.LOG.info("Started the new metaserver on port [" + port
          + "]...");

      HMSHandler.LOG.info("Options.minWorkerThreads = "
          + serverArgs.minWorkerThreads);

      HMSHandler.LOG.info("Options.maxWorkerThreads = "
          + serverArgs.maxWorkerThreads);

      server.serve();
    } catch (Throwable x) {

      x.printStackTrace();

      HMSHandler.LOG
          .error("Metastore Thrift Server threw an exception. Exiting...");

      HMSHandler.LOG.error(StringUtils.stringifyException(x));

      System.exit(1);
    }
  }
}
