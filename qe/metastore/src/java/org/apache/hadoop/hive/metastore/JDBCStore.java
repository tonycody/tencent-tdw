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

import java.io.IOException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.AddPartitionDesc;
import org.apache.hadoop.hive.metastore.api.AddSerdeDesc;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnInfo;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DbPriv;
import org.apache.hadoop.hive.metastore.api.DropPartitionDesc;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.IndexItem;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.RenameColDesc;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableInfo;
import org.apache.hadoop.hive.metastore.api.TblPriv;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.User;
import org.apache.hadoop.hive.metastore.api.group;
import org.apache.hadoop.hive.metastore.api.tdw_query_info;
import org.apache.hadoop.hive.metastore.api.tdw_query_stat;
import org.apache.hadoop.hive.metastore.api.tdw_sys_fields_statistics;
import org.apache.hadoop.hive.metastore.api.tdw_sys_table_statistics;
import org.apache.hadoop.hive.metastore.model.MGroup;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe;
import org.apache.hadoop.hive.serde2.dynamic_type.DynamicSerDe;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.protobuf.ProtobufSerDe;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.postgresql.xa.PGXADataSource;


class MyXid implements Xid {

  public int formatId;
  public byte gtrid[];
  public byte bqual[];

  public MyXid() {

  }

  public MyXid(int formatId, byte gtrid[], byte bqual[]) {
    this.formatId = formatId;
    this.gtrid = gtrid;
    this.bqual = bqual;
  }

  @Override
  public byte[] getBranchQualifier() {
    return bqual;
  }

  @Override
  public int getFormatId() {
    return formatId;
  }

  @Override
  public byte[] getGlobalTransactionId() {
    return gtrid;
  }
}

public class JDBCStore implements RawStore, Configurable {
  public enum Privilege {
    SELECT_PRIV, INSERT_PRIV, INDEX_PRIV, CREATE_PRIV, DROP_PRIV, DELETE_PRIV, ALTER_PRIV, UPDATE_PRIV, CREATE_VIEW_PRIV, SHOW_VIEW_PRIV, ALL_PRIV, DBA_PRIV
  }

  private static Map<Integer, Privilege> privMap = new ConcurrentHashMap<Integer, Privilege>();
  static {
    privMap.put(1, Privilege.SELECT_PRIV);
    privMap.put(2, Privilege.INSERT_PRIV);
    privMap.put(3, Privilege.INDEX_PRIV);
    privMap.put(4, Privilege.CREATE_PRIV);
    privMap.put(5, Privilege.DROP_PRIV);
    privMap.put(6, Privilege.DELETE_PRIV);
    privMap.put(7, Privilege.ALTER_PRIV);
    privMap.put(8, Privilege.UPDATE_PRIV);
    privMap.put(9, Privilege.CREATE_VIEW_PRIV);
    privMap.put(10, Privilege.SHOW_VIEW_PRIV);
    privMap.put(11, Privilege.ALL_PRIV);
    privMap.put(12, Privilege.DBA_PRIV);
  }

  private Configuration hiveConf;
  private static final Log LOG = LogFactory.getLog(JDBCStore.class.getName());
  private static Map<String, PoolingDataSource> SegmentDbConPool = new ConcurrentHashMap<String, PoolingDataSource>();
  private static PoolingDataSource globalDbConPool = null;
  public static String globalDbUrl = "jdbc:postgresql://172.25.38.244:5432/master_metastore_db";
  public static String user = "tdw";
  public static String passwd = "tdw";
  public static boolean globalDbSyncInit = false;
  public static long syncTime = 10;
  public static int poolActiveSize = 10;
  public static boolean poolEnable = false;
  public static int timeout = 10;
  private static SyncDBRouterThread syncer = null;
  private static boolean masterForRead = false;
  
  private static Map<String, Vector<String>> masterToSlave = new ConcurrentHashMap<String, Vector<String>>();

  public static void initMasterToSlaveMap(Configuration conf){
    masterForRead = conf.getBoolean("hive.metastore.master.read", true);
    String [] masterurls = conf.getStrings("hive.metastore.master.urls");
    if(masterurls == null){
      return;
    }
    
    for(int i = 0; i < masterurls.length; i++){
      String [] slaveurls = conf.getStrings("hive.metastore.slave.urls." + masterurls[i]);
      if(slaveurls == null){
        continue;
      }
      Vector<String> slaves = new Vector<String>();
      for(int j = 0 ; j < slaveurls.length; j++){
        slaves.add(slaveurls[j]);
      }
      
      if(masterForRead){
        slaves.add(masterurls[i]);
      }
      
      masterToSlave.put(masterurls[i], slaves);
    }
  }
  
  public static String getUrlForRead(String url){
    JDBCUrl jdbcUrl = new JDBCUrl(url);
    Vector<String> slaveurls = masterToSlave.get(jdbcUrl.getHost());
    if(slaveurls == null || slaveurls.isEmpty()){
      return url;
    }
    
    Random r = new Random();
    int index = r.nextInt(100000000) % slaveurls.size();
    return "jdbc:" + jdbcUrl.getDriver() + "://" + slaveurls.get(index) 
          + ":" + jdbcUrl.getPort() + "/" + jdbcUrl.getDB();
  }
  
  public static int fnv1a32db(byte[] buf, int offset, int len) {
    long seed = 0x811c9dc5L;
    for (int i = offset; i < offset + len; i++) {
      seed ^= buf[i];
      seed += (seed << 1) + (seed << 4) + (seed << 7) + (seed << 8)
          + (seed << 24);
    }

    return (int) (Math.abs((seed & 0x00000000ffffffffL)) % 10000);
  }

  public static int fnv1a32tbl(byte[] buf, int offset, int len) {
    long seed = 0x811c9dc5L;
    for (int i = offset; i < offset + len; i++) {
      seed ^= buf[i];
      seed += (seed << 1) + (seed << 4) + (seed << 7) + (seed << 8)
          + (seed << 24);
    }

    return (int) (Math.abs((seed & 0x00000000ffffffffL)) % 800000000 + 100000000);
  }

  public int getHashForTbl(String db, String tbl) {
    String times = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date());
    String tblName = times + "/" + db + "/" + tbl;
    return fnv1a32tbl(tblName.getBytes(), 0, tblName.getBytes().length);
  }

  public static int getHashForDb(String db) {
    return fnv1a32db(db.getBytes(), 0, db.getBytes().length);
  }

  public long genTblID(String db, String tblName) {
    Random rand = new Random();
    String tblHashCode = String.format("%d", getHashForTbl(db, tblName));
    String randNum = String.format("%06d",
        Math.abs(rand.nextInt(1000000000)) % 1000000);
    String hashcode = String.format("%04d", getHashForDb(db));
    String baseIDStr = tblHashCode + randNum + hashcode;

    return Long.valueOf(baseIDStr);
  }

  public static String getSegmentUrlByDbNameHashCode(String db)
      throws MetaException {
    if (db == null) {
      return null;
    }

    int hashcode = getHashForDb(db.toLowerCase());
    Connection con = null;
    Statement stmt = null;
    String url = null;

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("get segment url error , db=" + db);
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get segment url error , db=" + db);
      throw new MetaException(e1.getMessage());
    }

    try {
      stmt = con.createStatement();
      String sql = "select seg_addr from seg_split where seg_accept_range @> "
          + hashcode;
      ResultSet segmentSet = stmt.executeQuery(sql);
      while (segmentSet.next()) {
        url = segmentSet.getString(1);
      }
      segmentSet.close();
    } catch (SQLException e1) {
      LOG.error("get segment url error , db=" + db);
      throw new MetaException(e1.getMessage());
    } finally {
      closeStatement(stmt);
      closeConnection(con);
    }

    return url;
  }

  public static void initMetaSyncThread() {
    syncer = new SyncDBRouterThread(globalDbUrl, user, passwd, syncTime);
    syncer.init();
    Thread syncThread = new Thread(syncer);
    syncThread.start();
    globalDbSyncInit = true;
  }

  public static void initMetaSyncThread(String url, String use, String pass,
      long time) {
    globalDbUrl = url;
    user = use;
    passwd = pass;
    syncTime = time;

    syncer = new SyncDBRouterThread(globalDbUrl, user, passwd);
    syncer.init();
    Thread syncThread = new Thread(syncer);
    syncThread.start();
    globalDbSyncInit = true;
  }

  public JDBCStore() {

  }

  public static Set<String> getAllSegments() {
    return SegmentDbConPool.keySet();
  }

  
  public static Connection getGlobalConnection() throws SQLException,
      MetaStoreConnectException {
    if (poolEnable) {
      if (globalDbConPool == null) {
        checkGlobalPoolingDataSource(globalDbUrl, user, passwd);
      }

      return globalDbConPool.getConnection();
    } else {
      return openConnect(globalDbUrl, user, passwd);
    }
  }

  public synchronized static void checkGlobalPoolingDataSource(String url,
      String user, String pass) {
    if (globalDbConPool != null) {
      return;
    } else {
      LOG.error("#################################################################### init master connetion pool");
      ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(
          url, user, pass);
      GenericObjectPool connectionPool = new GenericObjectPool();
      connectionPool.setMaxActive(poolActiveSize);
      connectionPool
          .setWhenExhaustedAction(GenericObjectPool.WHEN_EXHAUSTED_BLOCK);
      connectionPool.setMaxWait(10000);
      connectionPool.setTestOnBorrow(true);
      connectionPool.setTestWhileIdle(true);
      connectionPool.setTimeBetweenEvictionRunsMillis(30000);
      connectionPool.setMinEvictableIdleTimeMillis(300000);
      connectionPool.setLifo(true);

      PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(
          connectionFactory, connectionPool, null, null, false, true);

      globalDbConPool = new PoolingDataSource(connectionPool);
      LOG.error("#################################################################### init global connetion pool over");
    }
  }

  public synchronized static PoolingDataSource getSegPoolingDataSource(
      String url, String user, String pass) {
    url = url.toLowerCase();
    PoolingDataSource pool = SegmentDbConPool.get(url);
    if (pool != null) {
      return pool;
    } else {
      LOG.debug("#################################################################### init global connetion pool:"
          + url);
      ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(
          url, user, pass);
      GenericObjectPool connectionPool = new GenericObjectPool();
      connectionPool.setMaxActive(poolActiveSize);
      connectionPool
          .setWhenExhaustedAction(GenericObjectPool.WHEN_EXHAUSTED_BLOCK);
      connectionPool.setMaxWait(10000);
      connectionPool.setTestOnBorrow(true);
      connectionPool.setTestWhileIdle(true);
      connectionPool.setTimeBetweenEvictionRunsMillis(30000);
      connectionPool.setMinEvictableIdleTimeMillis(300000);
      connectionPool.setLifo(true);

      PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(
          connectionFactory, connectionPool, null, null, false, true);

      pool = new PoolingDataSource(connectionPool);
      SegmentDbConPool.put(url, pool);

      LOG.debug("#################################################################### init global connetion pool:"
          + url + " over");
      return pool;
    }
  }

  public static Connection getConnectionFromPool(String url)
      throws SQLException, MetaStoreConnectException {
    if (poolEnable) {
      PoolingDataSource pool = getSegPoolingDataSource(url, user, passwd);

      return pool.getConnection();
    } else {
      return openConnect(url, user, passwd);
    }
  }

  public static Connection openConnect(String url, String user, String pass)
      throws SQLException, MetaStoreConnectException {
    try {
      Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException e) {
      LOG.error(" get org.postgresql.Driver failed ");
      e.printStackTrace();
      throw new MetaStoreConnectException(e.getMessage());
    }

    Connection conn = null;

    DriverManager.setLoginTimeout(timeout);
//    DriverManager.setLoginTimeout(10);
    conn = DriverManager.getConnection(url, user, pass);
    LOG.debug(" get Connection ok: " + url);

    return conn;
  }
  
  public static String getSegmentDBURL(String db) {
    if (db == null) {
      return null;
    }

    DBRouterInfo route = SyncDBRouterThread.dbRouter.get(db.toLowerCase());

    if (route == null) {
      try {
        route = getDBRouter(db.toLowerCase());
        if (route != null) {
          return route.getSegmentDBUrl();
        } else {
          return null;
        }
      } catch (MetaException e) {
        return null;
      }
    } else {
      return route.getSegmentDBUrl();
    }
  }

  public static DBRouterInfo getDBRouter(String db) throws MetaException {
    Connection con = null;
    ;
    Statement ps = null;

    DBRouterInfo route = null;

    db = db.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("get db router error, user=" + user + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get db router error, user=" + user + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = "select db_name, seg_addr, secondary_seg_addr, is_db_split, "
          + "describe from router where db_name='" + db + "'";


      ResultSet rs = ps.executeQuery(sql);

      while (rs.next()) {
        route = new DBRouterInfo();
        route.setDBName(rs.getString(1));
        route.setSegmentDBUrl(rs.getString(2));
        route.setSecondarySegmentDBUrl(rs.getString(3));
        route.setHasTableRouter(rs.getBoolean(4));
        route.setDetail(rs.getString(5));

        LOG.debug("db name is " + route.getDBName() + "\n" + " segment addr is "
            + route.getSegmentDBUrl() + "\n" + " second segment addr is "
            + route.getSecondarySlaveDBUrl() + "\n" + " is has table route is "
            + route.getHasTableRouter() + "\n" + " detail is "
            + route.getDetail());
      }

      rs.close();
    } catch (SQLException sqlex) {
      LOG.error("get user error, user=" + user + ", msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    return route;
  }

  
  public static Connection getSegmentConnection(String dbName)
      throws MetaStoreConnectException, SQLException {
    String url = getSegmentDBURL(dbName);
    if (url == null) {
      throw new MetaStoreConnectException("Can not find segment db, db=" + dbName);
    } else {
      return getConnectionFromPool(url);
    }
  }
  
  public static Connection getSegmentConnectionForRead(String dbName) 
      throws MetaStoreConnectException, SQLException {
    String url = getSegmentDBURL(dbName);
    String readUrl = getUrlForRead(url);
    LOG.info("XXXXXXXXXXXreadUrl = " + readUrl);
    if (readUrl == null) {  
      throw new MetaStoreConnectException("Can not find segment db, db=" + dbName);
    } else {  
      return getConnectionFromPool(readUrl);
    }
  }

  
  public static void closeStatement(Statement stmt) {
    try {
      if (stmt != null) {
        stmt.close();
      }
    } catch (Exception x) {
      LOG.error("close Statement error, msg=" + x.getMessage());
    }
  }

  
  public static void closeConnection(Connection con) {
    try {
      if (con != null) {
        con.close();
      }
    } catch (Exception x) {
      LOG.error("close connection error, msg=" + x.getMessage());
    }
  }

  
  public static void closeXAConnection(XAConnection con) {
    try {
      if (con != null) {
        con.close();
      }
    } catch (Exception x) {
      LOG.error("close connection error, msg=" + x.getMessage());
    }
  }

  public static PGXADataSource getXADataSource(String url, String user,
      String passwd) {
    String[] urlArray = url.split(":|/");
    if (urlArray.length != 7) {
      return null;
    }

    PGXADataSource ds = new PGXADataSource();
    ds.setServerName(urlArray[4]);
    ds.setPortNumber(Integer.valueOf(urlArray[5]));
    ds.setDatabaseName(urlArray[6]);

    ds.setUser(user);
    ds.setPassword(passwd);

    return ds;
  }

  
  private int genFormatID() {
    Random rand = new Random();
    return rand.nextInt(2100000000);
  }

  @Override
  public Configuration getConf() {
    return hiveConf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.hiveConf = conf;
  }

  @Override
  public void shutdown() {

  }

  @Override
  public boolean openTransaction() {
    return false;
  }

  @Override
  public boolean commitTransaction() {
    return false;
  }

  @Override
  public void rollbackTransaction() {

  }

  @Override
  
  public boolean createDatabase(Database db) throws MetaException {
    boolean useDistributeTran = hiveConf.getBoolean(
        "hive.metadata.usedistributetransaction", false);
    if (!useDistributeTran) {
      return createDatabaseNoDistributeTransaction(db);
    } else {
      return createDatabaseByDistributeTransaction(db);
    }
  }

  public boolean createDatabaseNoDistributeTransaction(Database db)
      throws MetaException {
    boolean success = false;
    Connection con;
    PreparedStatement ps = null;
    db.setName(db.getName().toLowerCase());

    if (db.getMetastore() == null) {
      String slaveURL = getSegmentUrlByDbNameHashCode(db.getName());
      db.setMetastore(slaveURL);
    }
    int hashcode = getHashForDb(db.getName());
    
    try {
      con = getConnectionFromPool(db.getMetastore());
    } catch (MetaStoreConnectException e1) {
      LOG.error("create database error, db=" + db.getName() + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("create database error, db=" + db.getName() + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con
          .prepareStatement("INSERT INTO DBS(name, hdfs_schema, description, owner) VALUES(?,?,?,?)");
      ps.setString(1, db.getName());
      ps.setString(2, db.getHdfsscheme());
      ps.setString(3, db.getDescription());
      ps.setString(4, db.getOwner());
      ps.executeUpdate();

      Warehouse wh = new Warehouse(hiveConf);
      Path databasePath = wh.getDefaultDatabasePath(db.getName(),
          db.getHdfsscheme());
      wh.mkdirs(databasePath);

      con.commit();
      success = true;

    } catch (Exception x) {
      LOG.error("create database error, db=" + db.getName() + ", msg="
          + x.getMessage());
      x.printStackTrace();
      try {
        con.rollback();
      } catch (SQLException e) {
        e.printStackTrace();
      }
      throw new MetaException(x.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("create database error, db=" + db.getName() + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("create database error, db=" + db.getName() + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con
          .prepareStatement("INSERT INTO router(db_name, seg_addr, hashcode, owner) VALUES(?,?,?,?)");
      ps.setString(1, db.getName());
      ps.setString(2, db.getMetastore());
      ps.setInt(3, hashcode);
      ps.setString(4, db.getOwner());
      ps.executeUpdate();

      con.commit();
    } catch (Exception x) {
      LOG.error("create database error, db=" + db.getName() + ", msg="
          + x.getMessage());
      x.printStackTrace();
      try {
        con.rollback();
      } catch (SQLException e) {
        e.printStackTrace();
      }
      throw new MetaException(x.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    return success;
  }

  public boolean createDatabaseByDistributeTransaction(Database db)
      throws MetaException {
    if (db.getMetastore() == null) {
      String slaveURL = getSegmentUrlByDbNameHashCode(db.getName());
      db.setMetastore(slaveURL);
    }
    int hashcode = getHashForDb(db.getName());
    db.setName(db.getName().toLowerCase());

    boolean success = false;

    Connection masterConn = null;
    Connection slaveConn = null;

    PGXADataSource masterDS = null;
    PGXADataSource slaveDS = null;

    masterDS = getXADataSource(globalDbUrl, user, passwd);
    slaveDS = getXADataSource(db.getMetastore(), user, passwd);

    XAConnection masterDSXaConn = null;
    XAConnection slaveDSXaConn = null;

    XAResource masterSaRes = null;
    XAResource slaveSaRes = null;

    int formatID = genFormatID();
    Xid masterXid = new MyXid(formatID, new byte[] { 0x01 },
        new byte[] { 0x02 });
    Xid slaveXid = new MyXid(formatID, new byte[] { 0x11 }, new byte[] { 0x12 });

    PreparedStatement masterStmt = null;
    PreparedStatement slaveStmt = null;

    try {
      masterDSXaConn = masterDS.getXAConnection();
      slaveDSXaConn = slaveDS.getXAConnection();

      masterSaRes = masterDSXaConn.getXAResource();
      slaveSaRes = slaveDSXaConn.getXAResource();

      masterConn = masterDSXaConn.getConnection();
      slaveConn = slaveDSXaConn.getConnection();

      masterStmt = masterConn
          .prepareStatement("insert into router(db_name, seg_addr,hashcode, owner) values(?,?,?,?)");
      slaveStmt = slaveConn
          .prepareStatement("insert into dbs(name, hdfs_schema, description, owner) VALUES(?,?,?,?)");

      try {
        masterSaRes.start(masterXid, XAResource.TMNOFLAGS);

        masterStmt.setString(1, db.getName());
        masterStmt.setString(2, db.getMetastore());
        masterStmt.setInt(3, hashcode);
        masterStmt.setString(4, db.getOwner());
        masterStmt.executeUpdate();

        masterSaRes.end(masterXid, XAResource.TMSUCCESS);

        slaveSaRes.start(slaveXid, XAResource.TMNOFLAGS);

        slaveStmt.setString(1, db.getName());
        slaveStmt.setString(2, db.getHdfsscheme());
        slaveStmt.setString(3, db.getDescription());
        slaveStmt.setString(4, db.getOwner());
        slaveStmt.executeUpdate();

        slaveSaRes.end(slaveXid, XAResource.TMSUCCESS);

        int masterRet = masterSaRes.prepare(masterXid);
        int slaveRet = slaveSaRes.prepare(slaveXid);

        Warehouse wh = new Warehouse(hiveConf);
        Path databasePath = wh.getDefaultDatabasePath(db.getName(),
            db.getHdfsscheme());

        if (masterRet == XAResource.XA_OK && slaveRet == XAResource.XA_OK
            && wh.mkdirs(databasePath)) {
          masterSaRes.commit(masterXid, false);
          slaveSaRes.commit(slaveXid, false);

          success = true;
        }
      } catch (XAException e) {
        LOG.error("XAException create database error, db=" + db.getName()
            + ", msg=" + e.getMessage());
        e.printStackTrace();
        throw new MetaException(e.getMessage());
      }
    } catch (SQLException e) {
      LOG.error("SQLException create database error, db=" + db.getName()
          + ", msg=" + e.getMessage());
      e.printStackTrace();
      throw new MetaException(e.getMessage());
    } finally {
      if (!success) {
        try {
          masterSaRes.rollback(masterXid);
        } catch (Exception x) {

        }

        try {
          slaveSaRes.rollback(slaveXid);
        } catch (Exception x) {

        }
      }

      closeStatement(masterStmt);
      closeStatement(slaveStmt);

      closeConnection(masterConn);
      closeConnection(slaveConn);

      closeXAConnection(masterDSXaConn);
      closeXAConnection(slaveDSXaConn);
    }

    return success;
  }

  public boolean createDatabase(String name) throws MetaException {
    Database db = new Database(name, "default_path", null, null, null);
    return this.createDatabase(db);
  }

  @Override
  public Database getDatabase(String name) throws NoSuchObjectException,
      MetaException {
    Database db = null;
    Connection con;
    name = name.toLowerCase();

    try {
      con = getSegmentConnection(name);
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
      String sql = "SELECT name, hdfs_schema, description, owner FROM DBS WHERE name='"
          + name + "'";

      ResultSet dbSet = stmt.executeQuery(sql);
      boolean isDBFind = false;

      while (dbSet.next()) {
        isDBFind = true;
        db = new Database();
        db.setName(dbSet.getString(1));
        db.setHdfsscheme(dbSet.getString(2));
        db.setDescription(dbSet.getString(3));
        db.setOwner(dbSet.getString(4));
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
      closeStatement(stmt);
      closeConnection(con);
    }

    return db;
  }

  @Override
  public boolean dropDatabase(String dbname) throws MetaException {
    boolean useDistributeTran = hiveConf.getBoolean(
        "hive.metadata.usedistributetransaction", false);
    if (!useDistributeTran) {
      return dropDatabaseNoDistributeTransaction(dbname);
    } else {
      return dropDatabaseByDistributeTransaction(dbname);
    }
  }

  public boolean dropDatabaseNoDistributeTransaction(String name)
      throws MetaException {
    boolean success = false;
    Connection con;
    Statement stmt = null;
    name = name.toLowerCase();

    try {
      con = getSegmentConnection(name.toLowerCase());
    } catch (MetaStoreConnectException e1) {
      LOG.error("drop database error, db=" + name + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("drop database error, db=" + name + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      stmt = con.createStatement();
      String sql = "DELETE FROM dbs WHERE name='" + name + "'";
      stmt.executeUpdate(sql);

      Warehouse wh;
      wh = new Warehouse(hiveConf);
      wh.deleteDir(wh.getDefaultDatabasePath(name), true);

      con.commit();
      success = true;
    } catch (Exception x) {
      LOG.error("drop database error, db=" + name + ", msg=" + x.getMessage());
      throw new MetaException(x.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      closeStatement(stmt);
      closeConnection(con);
    }

    success = false;
    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("drop database error, db=" + name + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("drop database error, db=" + name + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      stmt = con.createStatement();
      String sql = "DELETE FROM router WHERE db_name='" + name + "'";
      stmt.executeUpdate(sql);

      sql = "delete from dbpriv where db_name='" + name + "'";
      stmt.executeUpdate(sql);

      sql = "delete from tblpriv where db_name='" + name + "'";
      stmt.executeUpdate(sql);

      try {
        sql = "delete from dbsensitivity where db_name='" + name + "'";
        stmt.executeUpdate(sql);

        sql = "delete from tblsensitivity where db_name='" + name + "'";
        stmt.executeUpdate(sql);
      } catch (Exception x) {

      }

      con.commit();
      success = true;
    } catch (SQLException x) {
      LOG.error("drop database error, db=" + name + ", msg=" + x.getMessage());
      throw new MetaException(x.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
      closeStatement(stmt);
      closeConnection(con);
    }

    return success;
  }

  public boolean dropDatabaseByDistributeTransaction(String name)
      throws MetaException {
    boolean success = false;
    name = name.toLowerCase();
    Connection masterConn = null;
    Connection slaveConn = null;

    PGXADataSource masterDS = null;
    PGXADataSource slaveDS = null;

    masterDS = getXADataSource(globalDbUrl, user, passwd);

    String slaveURL = getSegmentDBURL(name);
    slaveDS = getXADataSource(slaveURL, user, passwd);

    XAConnection masterDSXaConn = null;
    XAConnection slaveDSXaConn = null;

    int formatID = genFormatID();
    Xid masterXid = new MyXid(formatID, new byte[] { 0x01 },
        new byte[] { 0x02 });
    Xid slaveXid = new MyXid(formatID, new byte[] { 0x11 }, new byte[] { 0x12 });

    XAResource masterSaRes = null;
    XAResource slaveSaRes = null;
    Statement masterStmt = null;
    Statement slaveStmt = null;

    try {
      masterDSXaConn = masterDS.getXAConnection();
      slaveDSXaConn = slaveDS.getXAConnection();

      masterSaRes = masterDSXaConn.getXAResource();
      slaveSaRes = slaveDSXaConn.getXAResource();

      masterConn = masterDSXaConn.getConnection();
      slaveConn = slaveDSXaConn.getConnection();

      masterStmt = masterConn.createStatement();
      slaveStmt = slaveConn.createStatement();

      try {
        masterSaRes.start(masterXid, XAResource.TMNOFLAGS);
        masterStmt.executeUpdate("delete from router where db_name='" + name
            + "'");
        masterStmt.executeUpdate("delete from dbpriv where db_name='" + name
            + "'");
        masterStmt.executeUpdate("delete from tblpriv where db_name='" + name
            + "'");
        masterSaRes.end(masterXid, XAResource.TMSUCCESS);

        slaveSaRes.start(slaveXid, XAResource.TMNOFLAGS);
        slaveStmt.executeUpdate("delete from dbs where name='" + name + "'");
        slaveSaRes.end(slaveXid, XAResource.TMSUCCESS);

        int masterRet = masterSaRes.prepare(masterXid);
        int slaveRet = slaveSaRes.prepare(slaveXid);

        Warehouse wh = new Warehouse(hiveConf);

        if (masterRet == XAResource.XA_OK && slaveRet == XAResource.XA_OK
            && wh.deleteDir(wh.getDefaultDatabasePath(name), true)) {
          masterSaRes.commit(masterXid, false);
          slaveSaRes.commit(slaveXid, false);

          success = true;
        }
      } catch (XAException e) {
        LOG.error("drop database error, db=" + name + ", msg=" + e.getMessage());
        throw new MetaException(e.getMessage());
      }
    } catch (Exception e) {
      LOG.error("create database error, db=" + name + ", msg=" + e.getMessage());
      throw new MetaException(e.getMessage());
    } finally {
      if (!success) {
        try {
          masterSaRes.rollback(masterXid);
        } catch (Exception x) {

        }

        try {
          slaveSaRes.rollback(slaveXid);
        } catch (Exception x) {

        }
      }

      closeStatement(masterStmt);
      closeStatement(slaveStmt);

      closeConnection(masterConn);
      closeConnection(slaveConn);

      closeXAConnection(masterDSXaConn);
      closeXAConnection(slaveDSXaConn);
    }

    if (success) {
      Statement stmt = null;
      Connection con = null;
      try {
        con = getGlobalConnection();
        stmt = con.createStatement();
        String sql = "delete from dbsensitivity where db_name='" + name + "'";
        stmt.executeUpdate(sql);

        sql = "delete from tblsensitivity where db_name='" + name + "'";
        stmt.executeUpdate(sql);
      } catch (Exception e1) {
        LOG.error("update tblsenstivity table error, db=" + name + ", msg="
            + e1.getMessage());
      } finally {
        closeStatement(stmt);
        closeConnection(con);
      }
    }

    return success;
  }

  @Override
  public List<String> getDatabases() throws MetaException {
    Connection con;
    Statement stmt = null;

    List<String> dbNameList = new ArrayList<String>();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("get databases error msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get databases error msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

      stmt = con.createStatement();
      String sql = "select db_name, owner from router";
      ResultSet ret = stmt.executeQuery(sql);

      dbNameList = new ArrayList<String>();

      while (ret.next()) {
        dbNameList.add(ret.getString(1));
      }
      ret.close();
    } catch (SQLException x) {
      x.printStackTrace();
      try {
        con.rollback();
      } catch (SQLException e) {
        e.printStackTrace();
      }
      LOG.error("get databases error msg=" + x.getMessage());
      throw new MetaException(x.getMessage());
    } finally {
      closeStatement(stmt);
      closeConnection(con);
    }

    return dbNameList;
  }

  @Override
  public boolean createType(Type type) {
    return false;
  }

  @Override
  public Type getType(String typeName) {
    return null;
  }

  @Override
  public boolean dropType(String typeName) {
    return false;
  }

  public void jdbcCreateView(Table tbl) throws AlreadyExistsException,
      InvalidObjectException, MetaException {
    LOG.debug("first, check the name is valid or not");
    if (!MetaStoreUtils.validateName(tbl.getTableName())
        || !MetaStoreUtils.validateColNames(tbl.getSd().getCols())
        || (tbl.getPriPartition() != null && !MetaStoreUtils.validateName(tbl
            .getPriPartition().getParKey().getName()))
        || (tbl.getSubPartition() != null && !MetaStoreUtils.validateName(tbl
            .getSubPartition().getParKey().getName()))) {
      LOG.error("create view error db=" + tbl.getDbName() + ", view="
          + tbl.getTableName());
      throw new InvalidObjectException(tbl.getTableName()
          + " is not a valid object name");
    }

    boolean success = false;

    Connection con;
    PreparedStatement ps = null;
    tbl.setDbName(tbl.getDbName().toLowerCase());
    tbl.setTableName(tbl.getTableName().toLowerCase());

    long tblID = genTblID(tbl.getDbName(), tbl.getTableName());

    try {
      con = getSegmentConnection(tbl.getDbName());
    } catch (MetaStoreConnectException e1) {
      LOG.error("create view error db=" + tbl.getDbName() + ", view="
          + tbl.getTableName() + ",msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("create view error db=" + tbl.getDbName() + ", view="
          + tbl.getTableName() + ",msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con
          .prepareStatement("select tbl_id, tbl_type from TBLS where db_name=? and tbl_name=?");
      ps.setString(1, tbl.getDbName());
      ps.setString(2, tbl.getTableName());

      boolean isViewFind = false;
      String tblType = null;

      ResultSet tblSet = ps.executeQuery();

      while (tblSet.next()) {
        isViewFind = true;
        tblID = tblSet.getLong(1);
        tblType = tblSet.getString(2);
        break;
      }

      tblSet.close();
      ps.close();

      if (isViewFind && !tbl.isIsReplaceOnExit()) {
        LOG.error("view " + tbl.getDbName() + ":" + tbl.getTableName()
            + " has exist");
        throw new AlreadyExistsException("view " + tbl.getDbName() + ":"
            + tbl.getTableName() + " has exist");
      }

      if (isViewFind && tbl.isIsReplaceOnExit()) {
        if (tblType != null && !tblType.equalsIgnoreCase("VIRTUAL_VIEW")) {
          LOG.error("name conflict " + tbl.getDbName() + ":"
              + tbl.getTableName() + " already exist, and it is not a view");
          throw new MetaException("name conflict " + tbl.getDbName() + ":"
              + tbl.getTableName() + " already exist, and it is not a view");
        }

        ps = con
            .prepareStatement("update TBLS  set tbl_comment=? where tbl_id=? ");
        ps.setString(1, tbl.getParameters().get("comment"));
        ps.setLong(2, tblID);

        ps.executeUpdate();
        ps.close();

        ps = con
            .prepareStatement("update tdwview  set view_original_text=?, view_expanded_text=?, "
                + "vtables=? where tbl_id=? ");

        ps.setString(1, tbl.getViewOriginalText());
        ps.setString(2, tbl.getViewExpandedText());
        ps.setString(3, tbl.getVtables().toLowerCase());
        ps.setLong(4, tblID);
        ps.executeUpdate();
        ps.close();

        ps = con.prepareStatement("delete from COLUMNS where tbl_id=?");
        ps.setLong(1, tblID);
        ps.executeUpdate();
        ps.close();

        ps = con
            .prepareStatement("insert into COLUMNS(column_index, tbl_id, column_name, "
                + "type_name, comment) values(?,?,?,?,?)");

        List<FieldSchema> fieldSchemas = tbl.getSd().getCols();
        int size = fieldSchemas.size();

        for (int i = 0; i < size; i++) {
          ps.setLong(1, i);
          ps.setLong(2, tblID);
          ps.setString(3, fieldSchemas.get(i).getName().toLowerCase());
          ps.setString(4, fieldSchemas.get(i).getType());
          ps.setString(5, fieldSchemas.get(i).getComment());
          ps.addBatch();
        }
        ps.executeBatch();
        ps.close();

        Map<String, String> tblPram = new HashMap<String, String>();
        ps = con
            .prepareStatement("select param_key, param_value from TABLE_PARAMS where tbl_id=? and param_type='TBL'");
        ps.setLong(1, tblID);

        ResultSet paramSet = ps.executeQuery();
        while (paramSet.next()) {
          tblPram.put(paramSet.getString(1), paramSet.getString(2));
        }
        paramSet.close();
        ps.close();

        ps = con
            .prepareStatement("delete from TABLE_PARAMS where tbl_id=? and param_type='TBL'");
        ps.setLong(1, tblID);
        ps.executeUpdate();
        ps.close();

        tblPram.putAll(tbl.getParameters());

        ps = con
            .prepareStatement("insert into TABLE_PARAMS(tbl_id, param_type, param_key, param_value) values(?,?,?,?)");

        for (Entry<String, String> entry : tblPram.entrySet()) {
          ps.setLong(1, tblID);
          ps.setString(2, "TBL");
          ps.setString(3, entry.getKey());
          ps.setString(4, entry.getValue());
          ps.addBatch();
        }
        ps.executeBatch();
        ps.close();

        con.commit();
        success = true;
      } else {
        ps = con
            .prepareStatement("INSERT INTO TBLS(tbl_id, retention, tbl_type, db_name, "
                + "tbl_name, tbl_owner, tbl_comment)"
                + " values(?,?,?,?,?,?,?)");


        StorageDescriptor sd = tbl.getSd();
        if (sd == null || sd.getSerdeInfo() == null) {
          throw new MetaException("storage descriptor of table "
              + tbl.getTableName() + " is null");
        }
        ps.setLong(1, tblID);
        ps.setLong(2, tbl.getRetention());
        ps.setString(3, tbl.getTableType());
        ps.setString(4, tbl.getDbName());
        ps.setString(5, tbl.getTableName());
        ps.setString(6, tbl.getOwner().toLowerCase());
        ps.setString(7, tbl.getParameters().get("comment"));

        ps.executeUpdate();
        ps.close();

        ps = con
            .prepareStatement("INSERT INTO COLUMNS(column_index, tbl_id, column_name, type_name, comment) "
                + " values(?,?,?,?,?)");

        List<FieldSchema> fieldList = sd.getCols();
        int fieldSize = fieldList.size();

        for (int i = 0; i < fieldSize; i++) {
          FieldSchema field = fieldList.get(i);
          ps.setInt(1, i);
          ps.setLong(2, tblID);
          ps.setString(3, field.getName());
          ps.setString(4, field.getType());
          ps.setString(5, field.getComment());

          ps.addBatch();
        }

        ps.executeBatch();
        ps.close();

        if (tbl.getParametersSize() > 0 || sd.getParametersSize() > 0
            || sd.getSerdeInfo().getParametersSize() > 0) {
          ps = con
              .prepareStatement("insert into table_params(tbl_id, param_type, param_key, param_value) "
                  + " values(?,?,?,?)");
          if (tbl.getParametersSize() > 0) {
            for (Map.Entry<String, String> entry : tbl.getParameters()
                .entrySet()) {
              if (entry.getKey().equalsIgnoreCase("type")
                  || entry.getKey().equalsIgnoreCase("comment"))
                break;
              ps.setLong(1, tblID);
              ps.setString(2, "TBL");
              ps.setString(3, entry.getKey());
              ps.setString(4, entry.getValue());

              ps.addBatch();
            }
          }

          if (sd.getParametersSize() > 0) {
            for (Map.Entry<String, String> entry : sd.getParameters()
                .entrySet()) {
              ps.setLong(1, tblID);
              ps.setString(2, "SD");
              ps.setString(3, entry.getKey());
              ps.setString(4, entry.getValue());

              ps.addBatch();
            }
          }

          if (sd.getSerdeInfo().getParametersSize() > 0) {
            for (Map.Entry<String, String> entry : sd.getSerdeInfo()
                .getParameters().entrySet()) {
              ps.setLong(1, tblID);
              ps.setString(2, "SERDE");
              ps.setString(3, entry.getKey());
              ps.setString(4, entry.getValue());

              ps.addBatch();
            }
          }

          ps.executeBatch();
          ps.close();
        }

        ps = con
            .prepareStatement("insert into tdwview(tbl_id, view_original_text, view_expanded_text, vtables) values(?,?,?,?)");
        ps.setLong(1, tblID);
        ps.setString(2, tbl.getViewOriginalText());
        ps.setString(3, tbl.getViewExpandedText());
        ps.setString(4, tbl.getVtables().toLowerCase());
        ps.executeUpdate();
        ps.close();

        con.commit();
        success = true;
      }
    } catch (SQLException sqlex) {
      sqlex.printStackTrace();
      LOG.error("create view error db=" + tbl.getDbName() + ", view="
          + tbl.getTableName() + ", msg=" + sqlex.getMessage());
      throw new MetaException(sqlex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    return;
  }

  @Override
  public void createTable(Table tbl) throws InvalidObjectException,
      MetaException, AlreadyExistsException {

    if (tbl == null) {
      throw new InvalidObjectException("unvalid parameters, tbl is null");
    }

    if (tbl.getTableType() == null) {
      tbl.setTableType("MANAGED_TABLE");
    }

    if (tbl.getTableType().equalsIgnoreCase("VIRTUAL_VIEW")) {
      jdbcCreateView(tbl);
      return;
    }

    tbl.setDbName(tbl.getDbName().toLowerCase());
    tbl.setTableName(tbl.getTableName().toLowerCase());

    LOG.debug("first, check the name is valid or not");
    if (!MetaStoreUtils.validateName(tbl.getTableName())
        || !MetaStoreUtils.validateColNames(tbl.getSd().getCols())
        || (tbl.getPriPartition() != null && !MetaStoreUtils.validateName(tbl
            .getPriPartition().getParKey().getName()))
        || (tbl.getSubPartition() != null && !MetaStoreUtils.validateName(tbl
            .getSubPartition().getParKey().getName()))) {
      throw new InvalidObjectException(tbl.getTableName()
          + " is not a valid object name");
    }

    long tblID = genTblID(tbl.getDbName(), tbl.getTableName());

    boolean success = false;

    Connection con;
    PreparedStatement ps = null;
    Statement stmt = null;
    Path tblPath = null;
    Warehouse wh = new Warehouse(hiveConf);
    boolean madeDir = false;

    LOG.debug("2, generate table path ");

    if (tbl.getSd().getLocation() == null
        || tbl.getSd().getLocation().isEmpty()) {
      tblPath = wh.getDefaultTablePath(tbl.getDbName(), tbl.getTableName());
    } else {
      if (tbl.getTableType().equalsIgnoreCase("EXTERNAL_TABLE")) {
        LOG.warn("Location: " + tbl.getSd().getLocation()
            + "specified for non-external table:" + tbl.getTableName());
      }

      tblPath = wh.getDnsPath(new Path(tbl.getSd().getLocation()));
    }
    tbl.getSd().setLocation(tblPath.toString());

    try {
      con = getSegmentConnection(tbl.getDbName());
    } catch (MetaStoreConnectException e1) {
      LOG.error("create table error, db=" + tbl.getDbName() + ", table="
          + tbl.getTableName() + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("create table error, db=" + tbl.getDbName() + ", table="
          + tbl.getTableName() + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

      stmt = con.createStatement();

      LOG.debug("1 check the table is exist or not");
      String sql = "select tbl_id from tbls where db_name='"
          + tbl.getDbName().toLowerCase() + "' and tbl_name='"
          + tbl.getTableName().toLowerCase() + "'";

      boolean isTblFind = false;
      ResultSet checkTblSet = stmt.executeQuery(sql);

      while (checkTblSet.next()) {
        isTblFind = true;
        break;
      }
      checkTblSet.close();

      if (isTblFind) {
        throw new AlreadyExistsException("table " + tbl.getDbName() + ":"
            + tbl.getTableName() + " has exist");
      }

      LOG.debug("2 insert into tbls");

      

      ps = con
          .prepareStatement("INSERT INTO TBLS(tbl_id, is_compressed, retention, tbl_type, db_name, "
              + "tbl_name, tbl_owner, tbl_format"
              + ", pri_part_type, sub_part_type, pri_part_key, sub_part_key, input_format, output_format"
              + ", serde_name, serde_lib, tbl_location, tbl_comment)"
              + " values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

      StorageDescriptor sd = tbl.getSd();
      if (sd == null || sd.getSerdeInfo() == null) {
        throw new MetaException("storage descriptor of table "
            + tbl.getTableName() + " is null");
      }

      SerDeInfo sdInfo = sd.getSerdeInfo();

      ps.setLong(1, tblID);
      ps.setBoolean(2, sd.isCompressed());
      ps.setLong(3, tbl.getRetention());
      if (tbl.getParameters() != null
          && tbl.getParameters().get("EXTERNAL") != null
          && tbl.getParameters().get("EXTERNAL").equalsIgnoreCase("TRUE")) {
        ps.setString(4, "EXTERNAL_TABLE");
      } else {
        ps.setString(4, tbl.getTableType());
      }
      ps.setString(5, tbl.getDbName());
      ps.setString(6, tbl.getTableName());
      ps.setString(7, tbl.getOwner());

      if (tbl.getParameters() == null) {
        ps.setString(8, null);
      } else {
        ps.setString(8, tbl.getParameters().get("type"));
      }

      Partition priPart = tbl.getPriPartition();
      Partition subPart = tbl.getSubPartition();
      if (priPart != null) {
        ps.setString(11, priPart.getParKey().getName());
        ps.setString(9, priPart.getParType());
      } else {
        ps.setString(11, null);
        ps.setString(9, null);
      }

      if (subPart != null) {
        ps.setString(12, subPart.getParKey().getName());
        ps.setString(10, subPart.getParType());
      } else {
        ps.setString(12, null);
        ps.setString(10, null);
      }

      ps.setString(13, sd.getInputFormat());
      ps.setString(14, sd.getOutputFormat());
      ps.setString(15, sdInfo.getName());
      ps.setString(16, sdInfo.getSerializationLib());
      ps.setString(17, sd.getLocation());

      if (tbl.getParameters() == null) {
        ps.setString(18, null);
      } else {
        ps.setString(18, tbl.getParameters().get("comment"));
      }


      ps.executeUpdate();

      ps.close();

      

      LOG.debug("3 insert into partitions");
      if (priPart != null) {
        ps = con.prepareStatement("INSERT INTO PARTITIONS(level, tbl_id,"
            + "part_name, part_values) values(?,?,?,?)");

        Map<String, List<String>> partSpaceMap = priPart.getParSpaces();

        for (Map.Entry<String, List<String>> entry : partSpaceMap.entrySet()) {
          ps.setInt(1, 0);
          ps.setLong(2, tblID);
          ps.setString(3, entry.getKey());
          if (entry.getValue() != null) {
            Array spaceArray = con.createArrayOf("varchar", entry.getValue()
                .toArray());
            ps.setArray(4, spaceArray);
          } else {
            ps.setArray(4, null);
          }

          ps.addBatch();
        }
        ps.executeBatch();
        ps.close();
      }

      if (subPart != null) {
        ps = con.prepareStatement("INSERT INTO PARTITIONS(level, tbl_id,"
            + "part_name, part_values) values(?,?,?,?)");

        Map<String, List<String>> partSpaceMap = subPart.getParSpaces();

        for (Map.Entry<String, List<String>> entry : partSpaceMap.entrySet()) {
          ps.setInt(1, 1);
          ps.setLong(2, tblID);
          ps.setString(3, entry.getKey());

          if (entry.getValue() != null) {
            Array spaceArray = con.createArrayOf("varchar", entry.getValue()
                .toArray());
            ps.setArray(4, spaceArray);
          } else {
            ps.setArray(4, null);
          }

          ps.addBatch();
        }
        ps.executeBatch();
        ps.close();
      }

      LOG.debug("4 insert into columns");
      ps = con
          .prepareStatement("INSERT INTO COLUMNS(column_index, tbl_id, column_name, type_name, comment) "
              + " values(?,?,?,?,?)");

      List<FieldSchema> fieldList = sd.getCols();
      int fieldSize = fieldList.size();

      for (int i = 0; i < fieldSize; i++) {
        FieldSchema field = fieldList.get(i);
        ps.setInt(1, i);
        ps.setLong(2, tblID);
        ps.setString(3, field.getName().toLowerCase());
        ps.setString(4, field.getType());
        ps.setString(5, field.getComment());

        ps.addBatch();
      }

      ps.executeBatch();
      ps.close();

      LOG.debug("5  insert into parameters");
      
      boolean createExtDirIfNotExist = true;
      if(tbl.getParametersSize() > 0){
        String createExtDirIfNotExistStr = tbl.getParameters().get("hive.exttable.createdir.ifnotexist");
        LOG.info("XXcreateExtDirIfNotExistStr=" + createExtDirIfNotExistStr);
        if(createExtDirIfNotExistStr != null && createExtDirIfNotExistStr.equalsIgnoreCase("false")){
          createExtDirIfNotExist = false;
        }
        tbl.getParameters().remove("hive.exttable.createdir.ifnotexist");
      }
      
      
      if (tbl.getParametersSize() > 0 || sd.getParametersSize() > 0
          || sd.getSerdeInfo().getParametersSize() > 0
          || sd.getNumBuckets() > -1) {
        ps = con
            .prepareStatement("insert into table_params(tbl_id, param_type, param_key, param_value) "
                + " values(?,?,?,?)");
        if (tbl.getParametersSize() > 0) {

          for (Map.Entry<String, String> entry : tbl.getParameters().entrySet()) {
            if (entry.getKey().equalsIgnoreCase("type")
                || entry.getKey().equalsIgnoreCase("comment"))
              continue;
            ps.setLong(1, tblID);
            ps.setString(2, "TBL");
            ps.setString(3, entry.getKey());
            ps.setString(4, entry.getValue());

            ps.addBatch();
          }
        }

        if (sd.getParametersSize() > 0) {
          for (Map.Entry<String, String> entry : sd.getParameters().entrySet()) {
            ps.setLong(1, tblID);
            ps.setString(2, "SD");
            ps.setString(3, entry.getKey());
            ps.setString(4, entry.getValue());

            ps.addBatch();
          }
        }

        if (sd.getSerdeInfo().getParametersSize() > 0) {
          for (Map.Entry<String, String> entry : sd.getSerdeInfo()
              .getParameters().entrySet()) {
            ps.setLong(1, tblID);
            ps.setString(2, "SERDE");
            ps.setString(3, entry.getKey());
            ps.setString(4, entry.getValue());

            ps.addBatch();
          }
        }

        if (sd.getNumBuckets() > -1) {
          ps.setLong(1, tblID);
          ps.setString(2, "SD");
          ps.setString(3, "NUM_BUCKETS");
          ps.setString(4, String.valueOf(sd.getNumBuckets()));
          ps.addBatch();
        }

        ps.executeBatch();
        ps.close();
      }

      if (tbl.getSd().getBucketCols() != null
          && !tbl.getSd().getBucketCols().isEmpty()) {
        ps = con
            .prepareStatement("insert into bucket_cols(tbl_id, bucket_col_name, col_index) values(?,?,?)");
        int index = 0;
        for (String col : tbl.getSd().getBucketCols()) {
          ps.setLong(1, tblID);
          ps.setString(2, col.toLowerCase());
          ps.setInt(3, index);
          index++;
          ps.addBatch();
        }

        ps.executeBatch();
        ps.close();
      }

      if (tbl.getSd().getSortCols() != null
          && !tbl.getSd().getSortCols().isEmpty()) {
        ps = con
            .prepareStatement("insert into sort_cols(tbl_id, sort_column_name, sort_order, col_index) values(?,?,?,?)");
        int index = 0;
        for (Order o : tbl.getSd().getSortCols()) {
          ps.setLong(1, tblID);
          ps.setString(2, o.getCol());
          ps.setInt(3, o.getOrder());
          ps.setInt(4, index);
          index++;
          ps.addBatch();
        }

        ps.executeBatch();
        ps.close();
      }

      LOG.debug("make hdfs directory for table");
      
      if (createExtDirIfNotExist && tblPath != null) {
        if (!wh.isDir(tblPath)) {
          if (!wh.mkdirs(tblPath)) {
            throw new MetaException(tblPath
                + " is not a directory or unable to create one");
          }
          madeDir = true;
        }

        if (tbl.getPriPartition() != null) {
          Set<String> priPartNames = tbl.getPriPartition().getParSpaces()
              .keySet();

          Set<String> subPartNames = null;
          if (tbl.getSubPartition() != null) {
            subPartNames = tbl.getSubPartition().getParSpaces().keySet();
          }

          List<Path> partPaths = Warehouse.getPartitionPaths(tblPath,
              priPartNames, subPartNames);

          for (Path partPath : partPaths) {
            if (!wh.mkdirs(partPath)) {
              throw new MetaException("Partition path " + partPath
                  + " is not a directory or unable to create one.");
            }
          }
        }
      }

      con.commit();

      success = true;
    } catch (SQLException sqlex) {
      LOG.error("create table error db=" + tbl.getDbName() + ", table="
          + tbl.getTableName() + ",msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }

        if (madeDir) {
          wh.deleteDir(tblPath, true);
        }
      }

      closeStatement(stmt);
      closeStatement(ps);
      closeConnection(con);
    }

    return;
  }

  @Override
  public boolean dropTable(String dbName, String tableName, boolean deleteData)
      throws MetaException, NoSuchObjectException {
    Connection con = null;
    Statement ps = null;

    Warehouse wh = new Warehouse(hiveConf);
    boolean success = false;
    String tblLocation = null;
    String tblType = null;
    String tblFormat = null;

    String host = null;
    String port = null;
    String db = null;
    String user = null;
    String pass = null;

    dbName = dbName.toLowerCase();
    tableName = tableName.toLowerCase();

    try {
      con = getSegmentConnection(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("drop table error, db=" + dbName + ", tbl=" + tableName
          + ", msg=" + e1.getMessage());
      return true;
    } catch (SQLException e1) {
      LOG.error("drop table error, db=" + dbName + ", tbl=" + tableName
          + ", msg=" + e1.getMessage());
      return true;
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      ps = con.createStatement();

      String sql = "select tbl_id, tbl_type, tbl_format, tbl_location from tbls where db_name='"
          + dbName + "' and tbl_name='" + tableName + "'";

      long tblID = 0;
      boolean isTblFind = false;

      ResultSet tblSet = ps.executeQuery(sql);
      while (tblSet.next()) {
        isTblFind = true;
        tblID = tblSet.getLong(1);
        tblType = tblSet.getString(2);
        tblFormat = tblSet.getString(3);
        tblLocation = tblSet.getString(4);
        break;
      }
      tblSet.close();

      if (!isTblFind) {
        con.commit();
        success = true;
        return success;
      }

      if (tblFormat != null && tblFormat.equalsIgnoreCase("pgdata")) {
        sql = "select param_key, param_value from table_params where tbl_id="
            + tblID + " and param_type='SERDE'";
        ResultSet pSet = ps.executeQuery(sql);
        Map<String, String> pMap = new HashMap<String, String>();
        while (pSet.next()) {
          pMap.put(pSet.getString(1), pSet.getString(2));
        }

        pSet.close();

        host = pMap.get("ip");
        port = pMap.get("port");
        db = pMap.get("db_name");
        user = pMap.get("user_name");
        pass = pMap.get("pwd");
      }

      sql = "delete from tbls where tbl_id=" + tblID;
      ps.executeUpdate(sql);

      boolean deleteSuccess = false;
      if (deleteData && (tblLocation != null)
          && !tblType.equalsIgnoreCase("EXTERNAL_TABLE")) {
        Path tblPath = new Path(tblLocation);
        deleteSuccess = wh.deleteDir(tblPath, true);
        
        HiveConf hconf = (HiveConf)hiveConf;
        
        if((!deleteSuccess) && hconf.getBoolVar(HiveConf.ConfVars.HIVE_DELETE_DIR_ERROR_THROW)){
          throw new MetaException("can not delete hdfs path, drop table failed!");
        }
      }
      
      con.commit();
      success = true;
    } catch (Exception sqlex) {
      sqlex.printStackTrace();
      LOG.error("drop table error, db=" + dbName + ", tbl=" + tableName
          + ", msg=" + sqlex.getMessage());
      throw new MetaException(sqlex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      } 
      //else {
      //  if (deleteData && (tblLocation != null)
      //      && !tblType.equalsIgnoreCase("EXTERNAL_TABLE")) {
      //    Path tblPath = new Path(tblLocation);
      //    wh.deleteDir(tblPath, true);
      //  }
      //}

      closeStatement(ps);
      closeConnection(con);
    }

    if (success && tblFormat != null && tblFormat.equalsIgnoreCase("pgdata")
        && host != null && port != null && db != null && user != null
        && pass != null) {
      Connection conn = null;
      try {
        String sql = "drop table " + tableName;
        String url = "jdbc:postgresql://" + host + ":" + port + "/" + db;

        conn = DriverManager.getConnection(url, user, pass);
        conn.setAutoCommit(false);
        Statement s = conn.createStatement();
        s.execute(sql);
        conn.commit();
      } catch (Exception e) {
        success = false;
        throw new MetaException(e.getMessage());
      } finally {
        closeConnection(conn);
      }
    }

    success = false;
    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("drop table error, db=" + dbName + ", tbl=" + tableName
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("drop table error, db=" + dbName + ", tbl=" + tableName
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con.createStatement();
      String sql = "delete from tblpriv where db_name='" + dbName.toLowerCase()
          + "' and tbl_name='" + tableName.toLowerCase() + "'";

      ps.executeUpdate(sql);

      try {
        sql = "delete from tblsensitivity where db_name='"
            + dbName.toLowerCase() + "' and tbl_name='"
            + tableName.toLowerCase() + "'";

        ps.executeUpdate(sql);
      } catch (Exception x) {

      }

      con.commit();
      success = true;
    } catch (SQLException x) {
      LOG.error("drop table error, db=" + dbName + ", tbl=" + tableName
          + ", msg=" + x.getMessage());
      throw new MetaException(x.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    return success;
  }

  @Override
  public Table getTable(String dbName, String tableName) throws MetaException,
      NoSuchObjectException {
    boolean success = false;

    Connection con;
    Statement ps = null;
    Table tbl = new Table();

    dbName = dbName.toLowerCase();
    tableName = tableName.toLowerCase();

    try {
      con = getSegmentConnectionForRead(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("get table error, db=" + dbName + ", tbl=" + tableName
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get table error, db=" + dbName + ", tbl=" + tableName
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = "SELECT tbl_id, create_time"
          + ", is_compressed, retention, tbl_type, db_name, tbl_name, tbl_owner "
          + ", tbl_format, pri_part_type, sub_part_type, pri_part_key, sub_part_key "
          + ", input_format, output_format, serde_name, serde_lib, tbl_location, tbl_comment "
          + " from TBLS where db_name='" + dbName + "' and tbl_name='"
          + tableName + "'";

      ResultSet tblSet = ps.executeQuery(sql);
      boolean isTblFind = false;
      StorageDescriptor sd = null;
      SerDeInfo sdInfo = null;
      String priPartKey = null;
      String subPartKey = null;
      Partition priPart = null;
      Partition subPart = null;
      long tblID = 0;

      String comment = null;
      String format = null;
      Timestamp createTime = null;
      String tblType = null;

      boolean hasPriPart = false;
      boolean hasSubPart = false;

      while (tblSet.next()) {
        isTblFind = true;
        tblID = tblSet.getLong(1);

        createTime = tblSet.getTimestamp(2);

        if (createTime != null) {
          tbl.setCreateTime((int) (createTime.getTime() / 1000));
        }


        sd = new StorageDescriptor();
        sdInfo = new SerDeInfo();
        sd.setCompressed(tblSet.getBoolean(3));

        tbl.setRetention((int) tblSet.getLong(4));
        tblType = tblSet.getString(5);
        tbl.setTableType(tblType);
        tbl.setDbName(tblSet.getString(6));
        tbl.setTableName(tblSet.getString(7));
        tbl.setOwner(tblSet.getString(8));

        format = tblSet.getString(9);

        priPartKey = tblSet.getString(12);
        subPartKey = tblSet.getString(13);

        if (priPartKey != null && !priPartKey.isEmpty()) {
          hasPriPart = true;
          priPart = new Partition();
          priPart.setLevel(0);
          priPart.setDbName(tblSet.getString(6));
          priPart.setTableName(tblSet.getString(7));
          priPart.setParType(tblSet.getString(10));
        }

        if (subPartKey != null && !subPartKey.isEmpty()) {
          hasSubPart = true;
          subPart = new Partition();
          subPart.setLevel(1);
          subPart.setDbName(tblSet.getString(6));
          subPart.setTableName(tblSet.getString(7));
          subPart.setParType(tblSet.getString(11));
        }

        sd.setInputFormat(tblSet.getString(14));
        sd.setOutputFormat(tblSet.getString(15));
        sdInfo.setName(tblSet.getString(16));
        sdInfo.setSerializationLib(tblSet.getString(17));
        sd.setLocation(tblSet.getString(18));
        comment = tblSet.getString(19);

        break;
      }

      tblSet.close();

      

      if (!isTblFind) {
        LOG.error(dbName + "." + tableName + " table not found");
        throw new NoSuchObjectException(dbName + "." + tableName
            + " table not found");
      }

      List<FieldSchema> fieldList = new ArrayList<FieldSchema>();
      Map<String, FieldSchema> fieldMap = new LinkedHashMap<String, FieldSchema>();

      sql = "SELECT column_name, type_name, comment from columns where tbl_id="
          + tblID + " order by column_index asc";
      ResultSet colSet = ps.executeQuery(sql);
      while (colSet.next()) {
        FieldSchema field = new FieldSchema();
        field.setName(colSet.getString(1));
        field.setType(colSet.getString(2));
        field.setComment(colSet.getString(3));

        fieldList.add(field);
        fieldMap.put(colSet.getString(1), field);
      }
      colSet.close();

      sd.setCols(fieldList);

      sql = "SELECT param_type, param_key, param_value  from table_params where tbl_id="
          + tblID;
      ResultSet paramSet = ps.executeQuery(sql);
      Map<String, String> tblParamMap = new HashMap<String, String>();
      Map<String, String> sdParamMap = new HashMap<String, String>();
      Map<String, String> serdeParam = new HashMap<String, String>();

      while (paramSet.next()) {
        String type = paramSet.getString(1);
        if (type == null)
          continue;

        if (type.equalsIgnoreCase("sd")) {
          sdParamMap.put(paramSet.getString(2), paramSet.getString(3));
        } else if (type.equalsIgnoreCase("serde")) {
          serdeParam.put(paramSet.getString(2), paramSet.getString(3));
        } else if (type.equalsIgnoreCase("tbl")) {
          tblParamMap.put(paramSet.getString(2), paramSet.getString(3));
        } else {
          tblParamMap.put(paramSet.getString(2), paramSet.getString(3));
        }
      }
      paramSet.close();

      if (comment != null && !comment.isEmpty()) {
        tblParamMap.put("comment", comment);
      }

      if (format != null && !format.isEmpty()) {
        tblParamMap.put("type", format);
      }



      tbl.setParameters(tblParamMap);
      sd.setParameters(sdParamMap);
      sdInfo.setParameters(serdeParam);

      List<String> bucketCols = new ArrayList<String>();
      sql = "select bucket_col_name from bucket_cols where tbl_id=" + tblID
          + " order by col_index asc";
      ResultSet bucketSet = ps.executeQuery(sql);
      while (bucketSet.next()) {
        bucketCols.add(bucketSet.getString(1));
      }

      bucketSet.close();
      if (bucketCols.size() > 0) {
        sd.setBucketCols(bucketCols);
        String numBucketStr = sd.getParameters().get("NUM_BUCKETS");
        if (numBucketStr == null) {
          sd.setNumBuckets(-1);
        } else {
          sd.setNumBuckets(Integer.valueOf(numBucketStr));
        }
      } else {
        sd.setBucketCols(bucketCols);
        sd.setNumBuckets(-1);
      }

      sd.getParameters().remove("NUM_BUCKETS");

      List<Order> sortCols = new ArrayList<Order>();
      sql = "select sort_column_name, sort_order from sort_cols where tbl_id="
          + tblID + " order by col_index asc";
      ResultSet sortSet = ps.executeQuery(sql);
      while (sortSet.next()) {
        Order o = new Order();
        o.setCol(sortSet.getString(1));
        o.setOrder(sortSet.getInt(2));
        sortCols.add(o);
      }

      sortSet.close();
      sd.setSortCols(sortCols);

      sd.setSerdeInfo(sdInfo);
      tbl.setSd(sd);

      if (hasPriPart) {
        sql = "SELECT level, part_name, part_values from  PARTITIONS where tbl_id="
            + tblID;
        ResultSet partSet = ps.executeQuery(sql);
        Map<String, List<String>> priPartSpace = new LinkedHashMap<String, List<String>>();
        Map<String, List<String>> subPartSpace = new LinkedHashMap<String, List<String>>();

        while (partSet.next()) {
          int level = partSet.getInt(1);
          switch (level) {
          case 0:
            String priName = partSet.getString(2);
            List<String> priValueList = new ArrayList<String>();
            Array priSpaceArray = partSet.getArray(3);

            if (priSpaceArray != null) {
              ResultSet priValueSet = priSpaceArray.getResultSet();

              while (priValueSet.next()) {
                priValueList.add(priValueSet.getString(2));
              }
            }

            priPartSpace.put(priName, priValueList);
            break;

          case 1:
            String subName = partSet.getString(2);
            List<String> subValueList = new ArrayList<String>();
            Array subSpaceArray = partSet.getArray(3);

            if (subSpaceArray != null) {
              ResultSet subValueSet = subSpaceArray.getResultSet();
              while (subValueSet.next()) {
                subValueList.add(subValueSet.getString(2));
              }
            }

            subPartSpace.put(subName, subValueList);
            break;

          default:
            break;
          }
        }

        partSet.close();

        priPart.setParSpaces(priPartSpace);

        priPart.setParKey(fieldMap.get(priPartKey.toLowerCase()));

        if (hasSubPart) {
          subPart.setParSpaces(subPartSpace);
          subPart.setParKey(fieldMap.get(subPartKey.toLowerCase()));
        }
      }

      tbl.setPriPartition(priPart);
      tbl.setSubPartition(subPart);

      if (tblType.equalsIgnoreCase("VIRTUAL_VIEW")) {
        sql = "select view_original_text, view_expanded_text, vtables from "
            + " tdwview where tbl_id=" + tblID;

        ResultSet viewSet = ps.executeQuery(sql);
        while (viewSet.next()) {
          tbl.setViewOriginalText(viewSet.getString(1));
          tbl.setViewExpandedText(viewSet.getString(2));
          tbl.setVtables(viewSet.getString(3));
          break;
        }
      }

      con.commit();
      success = true;
    } catch (SQLException sqlex) {
      sqlex.printStackTrace();
      LOG.error("get table error, db=" + dbName + ", tbl=" + tableName
          + ", msg=" + sqlex.getMessage());
      throw new MetaException(sqlex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    if (success)
      return tbl;
    else
      return null;
  }

  @Override
  public Partition getPartition(String dbName, String tableName, int level)
      throws MetaException {
    boolean success = false;

    Connection con = null;
    Statement ps = null;
    Partition part = null;

    dbName = dbName.toLowerCase();
    tableName = tableName.toLowerCase();

    Map<String, List<String>> partNameMap = new LinkedHashMap<String, List<String>>();

    try {
      con = getSegmentConnection(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("get partition error, db=" + dbName + ", tbl=" + tableName
          + ", level=" + level + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get partition error, db=" + dbName + ", tbl=" + tableName
          + ", level=" + level + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      long tblID = 0;
      boolean isTblFind = false;
      String priPartType = null;
      String subPartType = null;
      boolean hasPriPart = false;
      boolean hasSubPart = false;
      String priPartKey = null;
      String subPartKey = null;
      String partKey = null;

      String sql = "SELECT tbl_id,  pri_part_type, pri_part_key, sub_part_type, sub_part_key from TBLS where db_name='"
          + dbName + "' and tbl_name='" + tableName + "'";

      ResultSet tblSet = ps.executeQuery(sql);
      while (tblSet.next()) {
        isTblFind = true;
        tblID = tblSet.getLong(1);
        priPartType = tblSet.getString(2);
        priPartKey = tblSet.getString(3);
        subPartType = tblSet.getString(4);
        subPartKey = tblSet.getString(5);

        if (priPartType != null && !priPartType.isEmpty()) {
          hasPriPart = true;
        }
        if (subPartType != null && !subPartType.isEmpty()) {
          hasSubPart = true;
        }

        if (hasPriPart && level == 0) {
          part = new Partition();
          part.setParType(priPartType);
          partKey = priPartKey;
          break;
        }

        if (hasSubPart && level == 1) {
          part = new Partition();
          part.setParType(subPartType);
          partKey = subPartKey;
          break;
        }

        con.commit();
        return null;
      }

      tblSet.close();

      if (!isTblFind) {
        throw new MetaException("can not find table " + dbName + ":"
            + tableName);
      }

      FieldSchema field = null;
      sql = "select type_name, comment from columns where tbl_id=" + tblID
          + " and column_name='" + partKey + "'";
      ResultSet colSet = ps.executeQuery(sql);
      while (colSet.next()) {
        field = new FieldSchema();
        field.setType(colSet.getString(1));
        field.setComment(colSet.getString(2));
        field.setName(partKey);

        break;
      }

      colSet.close();

      sql = "select part_name, part_values from partitions where tbl_id="
          + tblID + " and level=" + level;
      ResultSet partSet = ps.executeQuery(sql);

      while (partSet.next()) {
        String partName = partSet.getString(1);
        List<String> valueList = new ArrayList<String>();
        Array spaceArray = partSet.getArray(2);

        ResultSet priValueSet = spaceArray.getResultSet();

        if (priValueSet != null) {
          while (priValueSet.next()) {
            valueList.add(priValueSet.getString(2));
          }
        }

        partNameMap.put(partName, valueList);
      }
      partSet.close();

      part.setParSpaces(partNameMap);
      part.setDbName(dbName);
      part.setTableName(tableName);
      part.setLevel(level);
      part.setParKey(field);

      con.commit();
      success = true;
    } catch (SQLException sqlex) {
      sqlex.printStackTrace();
      LOG.error("get partition error, db=" + dbName + ", tbl=" + tableName
          + ", level=" + level + ", msg=" + sqlex.getMessage());
      throw new MetaException(sqlex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    if (success)
      return part;
    else
      return null;
  }

  @Override
  public void alterPartition(String dbName, String tableName, Partition new_part)
      throws InvalidObjectException, MetaException {

  }

  public void addPartition(String dbName, String tblName,
      AddPartitionDesc addPartitionDesc) throws InvalidObjectException,
      MetaException {
    boolean success = false;

    Connection con = null;
    PreparedStatement ps = null;
    Statement stmt = null;
    dbName = dbName.toLowerCase();
    tblName = tblName.toLowerCase();

    boolean isPathMaked = false;
    ArrayList<Path> pathToMake = new ArrayList<Path>();
    Warehouse wh = new Warehouse(hiveConf);
    
    long tblID = 0;

    try {
      con = getSegmentConnection(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("add partition error, db=" + dbName + ", tbl=" + tblName
          + ", level=" + addPartitionDesc.getLevel() + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("add partition error, db=" + dbName + ", tbl=" + tblName
          + ", level=" + addPartitionDesc.getLevel() + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      stmt = con.createStatement();

      String tblType = null;
      boolean hasPriPart = false;
      boolean hasSubPart = false;
      String priPartKey = null;
      String subPartKey = null;
      String priPartType = null;
      String subPartType = null;
     
      String priKeyType = null;
      String subKeyType = null;
      ResultSet tblSet = null;
      boolean isTblFind = false;
      boolean isColFind = false;
      
      String tblFormat = null;
      String tblLocation = null;

      PrimitiveTypeInfo pti = null;
      ObjectInspector StringIO = null;
      ObjectInspector ValueIO = null;
      ObjectInspectorConverters.Converter converter1 = null;
      ObjectInspectorConverters.Converter converter2 = null;

      ArrayList<String> partToAdd = new ArrayList<String>();
      String sql = null;
      
      HiveConf hconf = (HiveConf)hiveConf;
      boolean externalPartition = hconf.getBoolVar(HiveConf.ConfVars.HIVESUPPORTEXTERNALPARTITION);

      if (addPartitionDesc.getLevel() == 0) {
        sql = "SELECT tbl_id, tbl_type, pri_part_type, pri_part_key, tbl_format, tbl_location"
            + " from TBLS where db_name='" + dbName + "' and tbl_name='"
            + tblName + "'";

        tblSet = stmt.executeQuery(sql);
        isTblFind = false;

        while (tblSet.next()) {
          isTblFind = true;
          tblID = tblSet.getLong(1);
          tblType = tblSet.getString(2);
          priPartKey = tblSet.getString(4);
          priPartType = tblSet.getString(3);
          tblFormat = tblSet.getString(5);
          tblLocation = tblSet.getString(6);

          if (priPartType != null && !priPartType.isEmpty()) {
            hasPriPart = true;
          }
          break;
        }
        tblSet.close();

        if (!isTblFind) {
          LOG.error("add partition error, db=" + dbName + ", tbl=" + tblName
              + ", level=" + addPartitionDesc.getLevel() + ", msg="
              + "can not find table " + dbName + ":" + tblName);

          throw new MetaException("can not find table " + dbName + ":"
              + tblName);
        }

        if (!tblType.equalsIgnoreCase("MANAGED_TABLE")) {
          if(tblType.equalsIgnoreCase("EXTERNAL_TABLE") && tblFormat != null &&
              tblFormat.equalsIgnoreCase("pgdata")){
              LOG.error("add partition error, db=" + dbName + ", tbl=" + tblName
                + ", level=" + addPartitionDesc.getLevel() + ", msg=" + tblType + ":"
                + tblFormat + " can not support alter partition");
            throw new MetaException(tblType + ":" + tblFormat + " can not support alter partition");
          }
          
          if(externalPartition && tblType.equalsIgnoreCase("EXTERNAL_TABLE") && 
            		(tblFormat == null || !tblFormat.equalsIgnoreCase("pgdata"))){
          }
          else{
	          LOG.error("add partition error, db=" + dbName + ", tbl=" + tblName
	              + ", level=" + addPartitionDesc.getLevel() + ", msg=" + tblType
	              + " can not support alter partition");
	
	          throw new MetaException(tblType + " can not support alter partition");
          }
        }

        if (!hasPriPart) {
          LOG.error("add partition error, db=" + dbName + ", tbl=" + tblName
              + ", level=" + addPartitionDesc.getLevel() + ", msg=" + "table "
              + dbName + ":" + tblName + " is not pri-partitioned");

          throw new MetaException("table " + dbName + ":" + tblName
              + " is not pri-partitioned");
        }

        sql = "SELECT type_name from COLUMNS where tbl_id=" + tblID
            + " and column_name='" + priPartKey.toLowerCase() + "'";
        isColFind = false;
        ResultSet colSet = stmt.executeQuery(sql);
        while (colSet.next()) {
          isColFind = true;
          priKeyType = colSet.getString(1);
          break;
        }
        colSet.close();

        if (!isColFind) {
          LOG.error("add partition error, db=" + dbName + ", tbl=" + tblName
              + ", level=" + addPartitionDesc.getLevel() + ", msg=" + "table "
              + "can not find partition key information " + priPartKey);

          throw new MetaException("can not find partition key information "
              + priPartKey);
        }

        pti = new PrimitiveTypeInfo();
        pti.setTypeName(priKeyType);
        StringIO = PrimitiveObjectInspectorFactory
            .getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING);
        ValueIO = PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector(pti.getPrimitiveCategory());
        converter1 = ObjectInspectorConverters.getConverter(StringIO, ValueIO);
        converter2 = ObjectInspectorConverters.getConverter(StringIO, ValueIO);

        if ((addPartitionDesc.getPartType().equalsIgnoreCase("RANGE_PARTITION") && !priPartType
            .equalsIgnoreCase("range"))
            || (addPartitionDesc.getPartType().equalsIgnoreCase(
                "LIST_PARTITION") && !priPartType.equalsIgnoreCase("list"))) {
          LOG.error("add partition error, db=" + dbName + ", tbl=" + tblName
              + ", level=" + addPartitionDesc.getLevel() + ", msg="
              + "can not add  a " + addPartitionDesc.getPartType()
              + " partition, but the pri-partition type is " + priPartType);

          throw new MetaException("can not add  a "
              + addPartitionDesc.getPartType()
              + " partition, but the pri-partition type is " + priPartType);
        }

        LinkedHashMap<String, List<String>> partSpaces = new LinkedHashMap<String, List<String>>();
        Set<String> subPartNameSet = new TreeSet<String>();

        sql = "SELECT level, part_name, part_values from PARTITIONS where"
            + " tbl_id=" + tblID;// + " order by level asc";

        ResultSet partSet = stmt.executeQuery(sql);
        int partLevel = 0;

        while (partSet.next()) {
          partLevel = partSet.getInt(1);

          if (partLevel == 0) {
            String partName = partSet.getString(2);
            List<String> valueList = new ArrayList<String>();
            Array spaceArray = partSet.getArray(3);

            ResultSet priValueSet = spaceArray.getResultSet();

            while (priValueSet.next()) {
              valueList.add(priValueSet.getString(2));
            }

            partSpaces.put(partName, valueList);
          } else if (partLevel == 1) {
            String partName = partSet.getString(2);
            subPartNameSet.add(partName);
          }
        }
        partSet.close();

        partToAdd = new ArrayList<String>();

        LinkedHashMap<String, List<String>> addPartSpaces = (LinkedHashMap<String, List<String>>) addPartitionDesc
            .getParSpaces();

        Iterator<String> itr = addPartSpaces.keySet().iterator();

        while (itr.hasNext()) {
          String key = itr.next().toLowerCase();
          if (partSpaces.containsKey(key)) {
            LOG.error("add partition error, db=" + dbName + ", tbl=" + tblName
                + ", level=" + addPartitionDesc.getLevel() + ", msg="
                + "table : " + tblName
                + " have already contain a pri parititon named: " + key);

            throw new MetaException("table : " + tblName
                + " have already contain a pri parititon named: " + key);
          }
          partToAdd.add(key);
        }

        Iterator<List<String>> listItr = addPartSpaces.values().iterator();

        while (listItr.hasNext()) {
          Iterator<String> valueItr = listItr.next().iterator();
          if (valueItr.hasNext()) {
            String value = valueItr.next();

            if (converter1.convert(value) == null) {
              LOG.error("add partition error, db=" + dbName + ", tbl="
                  + tblName + ", level=" + addPartitionDesc.getLevel()
                  + ", msg=" + "value : " + value + " should be type of "
                  + priKeyType);

              throw new MetaException("value : " + value
                  + " should be type of " + priKeyType);
            }

            Iterator<List<String>> PartValuesItr = partSpaces.values()
                .iterator();
            while (PartValuesItr.hasNext()) {
              if (PartValuesItr.next().contains(value)) {
                LOG.error("add partition error, db=" + dbName + ", tbl="
                    + tblName + ", level=" + addPartitionDesc.getLevel()
                    + ", msg=" + "table : " + tblName
                    + " have already contain a pri partition contain value: "
                    + value);

                throw new MetaException("table : " + tblName
                    + " have already contain a pri partition contain value: "
                    + value);
              }
            }
          }
        }

        ps = con.prepareStatement("INSERT INTO partitions(level, tbl_id, "
            + " part_name, part_values) values(?,?,?,?)");

        for (Map.Entry<String, List<String>> entry : addPartSpaces.entrySet()) {
          ps.setInt(1, 0);
          ps.setLong(2, tblID);

          Array spaceArray = con.createArrayOf("varchar", entry.getValue()
              .toArray());
          ps.setArray(4, spaceArray);
          ps.setString(3, entry.getKey());

          ps.addBatch();
        }
        ps.executeBatch();
        
        if(!tblType.equalsIgnoreCase("EXTERNAL_TABLE")){
          for (String partName : partToAdd) {
            if(tblLocation == null || tblLocation.trim().isEmpty()){
              pathToMake.addAll(wh.getPriPartitionPaths(dbName, tblName, partName,
                  subPartNameSet));              
            }else{
              pathToMake.addAll(Warehouse.getPriPartitionPaths(new Path(tblLocation), partName,
                  subPartNameSet));
            }
          }
        }
        else{
	        for (String partName : partToAdd) {
		          pathToMake.addAll(Warehouse.getPriPartitionPaths(new Path(tblLocation), partName,
		              subPartNameSet));
		    }
        }
      } else if (addPartitionDesc.getLevel() == 1) {
        sql = "SELECT tbl_id, tbl_type, sub_part_type, sub_part_key, tbl_format, tbl_location"
            + " from TBLS where db_name='" + dbName.toLowerCase()
            + "' and tbl_name='" + tblName.toLowerCase() + "'";

        tblSet = stmt.executeQuery(sql);
        isTblFind = false;

        while (tblSet.next()) {
          isTblFind = true;
          tblID = tblSet.getLong(1);
          tblType = tblSet.getString(2);
          subPartKey = tblSet.getString(4);
          subPartType = tblSet.getString(3);
          tblFormat = tblSet.getString(5);
          tblLocation = tblSet.getString(6);

          if (subPartType != null && !subPartType.isEmpty()) {
            hasSubPart = true;
          }

          break;
        }

        tblSet.close();
        if (!isTblFind) {
          LOG.error("add partition error, db=" + dbName + ", tbl=" + tblName
              + ", level=" + addPartitionDesc.getLevel() + ", msg="
              + "can not find table " + dbName + ":" + tblName);

          throw new MetaException("can not find table " + dbName + ":"
              + tblName);
        }

        if (!tblType.equalsIgnoreCase("MANAGED_TABLE")) {
          if(tblType.equalsIgnoreCase("EXTERNAL_TABLE") && tblFormat != null &&
              tblFormat.equalsIgnoreCase("pgdata")){
            LOG.error("add partition error, db=" + dbName + ", tbl=" + tblName
              + ", level=" + addPartitionDesc.getLevel() + ", msg=" + tblType + ":"
              + tblFormat + " can not support alter partition");
            throw new MetaException(tblType + ":" + tblFormat + " can not support alter partition");
          }
          
          if(externalPartition && tblType.equalsIgnoreCase("EXTERNAL_TABLE") && 
            	(tblFormat == null || !tblFormat.equalsIgnoreCase("pgdata"))){
          }
          else{
		      LOG.error("add partition error, db=" + dbName + ", tbl=" + tblName
		          + ", level=" + addPartitionDesc.getLevel() + ", msg=" + tblType
		          + " can not support alter partition");
		
		      throw new MetaException(tblType + " can not support alter partition");
          }
        }

        if (!hasSubPart) {
          LOG.error("add partition error, db=" + dbName + ", tbl=" + tblName
              + ", level=" + addPartitionDesc.getLevel() + ", msg=" + "table "
              + dbName + ":" + tblName + " is not sun-partitioned");

          throw new MetaException("table " + dbName + ":" + tblName
              + " is not sun-partitioned");
        }

        sql = "SELECT type_name from COLUMNS where tbl_id=" + tblID
            + " and column_name='" + subPartKey.toLowerCase() + "'";

        isColFind = false;
        ResultSet colSet = stmt.executeQuery(sql);
        while (colSet.next()) {
          isColFind = true;
          subKeyType = colSet.getString(1);
          break;
        }

        colSet.close();

        if (!isColFind) {
          LOG.error("add partition error, db=" + dbName + ", tbl=" + tblName
              + ", level=" + addPartitionDesc.getLevel() + ", msg="
              + "can not find partition key information " + priPartKey);

          throw new MetaException("can not find partition key information "
              + priPartKey);
        }

        pti = new PrimitiveTypeInfo();
        pti.setTypeName(subKeyType);
        StringIO = PrimitiveObjectInspectorFactory
            .getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING);
        ValueIO = PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector(pti.getPrimitiveCategory());
        converter1 = ObjectInspectorConverters.getConverter(StringIO, ValueIO);
        converter2 = ObjectInspectorConverters.getConverter(StringIO, ValueIO);

        if ((addPartitionDesc.getPartType().equalsIgnoreCase("RANGE_PARTITION") && !subPartType
            .equalsIgnoreCase("range"))
            || (addPartitionDesc.getPartType().equalsIgnoreCase(
                "LIST_PARTITION") && !subPartType.equalsIgnoreCase("list"))) {
          LOG.error("add partition error, db=" + dbName + ", tbl=" + tblName
              + ", level=" + addPartitionDesc.getLevel() + ", msg="
              + "you can not add  a " + addPartitionDesc.getPartType()
              + " partition, but the sub-partition type is " + subPartType);

          throw new MetaException("you can not add  a "
              + addPartitionDesc.getPartType()
              + " partition, but the sub-partition type is " + subPartType);
        }

        LinkedHashMap<String, List<String>> partSpaces = new LinkedHashMap<String, List<String>>();
        Set<String> partNameSet = new TreeSet<String>();

        sql = "SELECT level,  part_name, part_values from PARTITIONS where"
            + " tbl_id=" + tblID;// + " order by level asc";

        ResultSet partSet = stmt.executeQuery(sql);
        int partLevel = 0;

        while (partSet.next()) {
          partLevel = partSet.getInt(1);

          if (partLevel == 1) {
            String partName = partSet.getString(2);
            List<String> valueList = new ArrayList<String>();
            Array spaceArray = partSet.getArray(3);

            ResultSet priValueSet = spaceArray.getResultSet();

            while (priValueSet.next()) {
              valueList.add(priValueSet.getString(2));
            }
            partSpaces.put(partName, valueList);
          } else if (partLevel == 0) {
            String partName = partSet.getString(2);
            partNameSet.add(partName);
          }
        }

        partToAdd = new ArrayList<String>();

        LinkedHashMap<String, List<String>> addPartSpaces = (LinkedHashMap<String, List<String>>) addPartitionDesc
            .getParSpaces();

        Iterator<String> itr = addPartSpaces.keySet().iterator();

        while (itr.hasNext()) {
          String key = itr.next().toLowerCase();
          if (partSpaces.containsKey(key)) {
            LOG.error("add partition error, db=" + dbName + ", tbl=" + tblName
                + ", level=" + addPartitionDesc.getLevel() + ", msg="
                + "table : " + tblName
                + " have already contain a sub parititon named: " + key);

            throw new MetaException("table : " + tblName
                + " have already contain a sub parititon named: " + key);
          }

          if (key.equalsIgnoreCase("default")) {
            LOG.error("add partition error, db="
                + dbName
                + ", tbl="
                + tblName
                + ", level="
                + addPartitionDesc.getLevel()
                + ", msg="
                + "use : 'alter table tblname add default subpartition' to add default subpartition!");

            throw new MetaException(
                "use : 'alter table tblname add default subpartition' to add default subpartition!");
          }
          partToAdd.add(key);
        }

        Iterator<List<String>> listItr = addPartSpaces.values().iterator();

        while (listItr.hasNext()) {
          Iterator<String> valueItr = listItr.next().iterator();
          if (valueItr.hasNext()) {
            String value = valueItr.next();

            if (converter1.convert(value) == null) {
              LOG.error("add partition error, db=" + dbName + ", tbl="
                  + tblName + ", level=" + addPartitionDesc.getLevel()
                  + ", msg=" + "value : " + value + " should be type of "
                  + priKeyType);

              throw new MetaException("value : " + value
                  + " should be type of " + priKeyType);
            }

            Iterator<List<String>> PartValuesItr = partSpaces.values()
                .iterator();
            while (PartValuesItr.hasNext()) {
              if (PartValuesItr.next().contains(value)) {
                LOG.error("add partition error, db=" + dbName + ", tbl="
                    + tblName + ", level=" + addPartitionDesc.getLevel()
                    + ", msg=" + "table : " + tblName
                    + " have already contain a sub partition contain value: "
                    + value);

                throw new MetaException("table : " + tblName
                    + " have already contain a sub partition contain value: "
                    + value);
              }
            }
          }
        }

        ps = con.prepareStatement("INSERT INTO partitions(level, tbl_id, "
            + " part_name, part_values) values(?,?,?,?)");

        for (Map.Entry<String, List<String>> entry : addPartSpaces.entrySet()) {
          ps.setInt(1, 1);
          ps.setLong(2, tblID);

          Array spaceArray = con.createArrayOf("varchar", entry.getValue()
              .toArray());
          ps.setArray(4, spaceArray);
          ps.setString(3, entry.getKey());

          ps.addBatch();
        }
        ps.executeBatch();
        
        if(!tblType.equalsIgnoreCase("EXTERNAL_TABLE")){
          for (String partName : partToAdd) {
            if(tblLocation == null || tblLocation.trim().isEmpty()){
              pathToMake.addAll(wh.getSubPartitionPaths(dbName, tblName,
                  partNameSet, partName));              
            }else{
              pathToMake.addAll(Warehouse.getSubPartitionPaths(new Path(tblLocation),
                  partNameSet, partName));              
            }
          }
        }
        else{
	        for (String partName : partToAdd) {
		          pathToMake.addAll(Warehouse.getSubPartitionPaths(new Path(tblLocation),
		              partNameSet, partName));
		    }
        }
      }

      con.commit();
      success = true;
    } catch (SQLException ex) {
      ex.printStackTrace();
      LOG.error("add partition error, db=" + dbName + ", tbl=" + tblName
          + ", level=" + addPartitionDesc.getLevel() + ", msg="
          + ex.getMessage());

      throw new MetaException(ex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }

        if (isPathMaked) {
          for (Path path : pathToMake) {
            wh.deleteDir(path, false);
          }
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    if (success) {
      boolean mkDirOK = false;
      List<Path> createdPath = new ArrayList<Path>();
      try
      {
        for (Path path : pathToMake) {
          mkDirOK = wh.mkdirs(path);
          if(!mkDirOK){
            break;
          }
          
          createdPath.add(path);
        }
      }
      catch(Exception x)
      {
        mkDirOK = false;
      }

      if(!mkDirOK)
      {
        dropPartitionMeta(dbName, tblID, addPartitionDesc);
        if(!createdPath.isEmpty())
        {
          for (Path path : createdPath) {
            wh.deleteDir(path, true);
          }
        }
        
        throw new MetaException("can not create hdfs path, add partition failed");
      }
      
    }
  }
  
  public void dropPartitionMeta(String dbName, long tblID, String partName, int level)
  {
    Connection con = null;
    Statement ps = null;
    PreparedStatement pss = null;

    dbName = dbName.toLowerCase();
    //partName = partName.toLowerCase();

    try {
      con = getSegmentConnection(dbName);
      
      pss = con.prepareStatement("delete from partitions where tbl_id=? and part_name=? and level=?");

      pss.setLong(1, tblID);
      pss.setString(2, partName.toLowerCase());
      pss.setInt(3, level);
      pss.addBatch();
          
      pss.executeBatch();
      
    } catch (Exception e1) {
      LOG.error("drop partition meta error, db=" + dbName + ", tblID=" + tblID
          + ", level=" + level + ", msg="
          + e1.getMessage());
    }
    finally
    {
      closeStatement(ps);
      closeConnection(con);
    }
  }
  
  public void dropPartitionMeta(String dbName, long tblID, AddPartitionDesc addPartitionDesc)
  {
    Connection con = null;
    Statement ps = null;
    PreparedStatement pss = null;

    dbName = dbName.toLowerCase();
    //partName = partName.toLowerCase();

    try {
      con = getSegmentConnection(dbName);
      
      pss = con.prepareStatement("delete from partitions where tbl_id=? and part_name=? and level=?");
      for(String partName:addPartitionDesc.getParSpaces().keySet())
      {
        pss.setLong(1, tblID);
        pss.setString(2, partName.toLowerCase());
        pss.setInt(3, addPartitionDesc.getLevel());
        pss.addBatch();
      }
      
      pss.executeBatch();
      
    } catch (Exception e1) {
      LOG.error("drop partition meta error, db=" + dbName + ", tblID=" + tblID
          + ", level=" + addPartitionDesc.getLevel() + ", msg="
          + e1.getMessage());
    }
    finally
    {
      closeStatement(ps);
      closeConnection(con);
    }
  }

  public void dropPartition(String dbName, String tblName,
      DropPartitionDesc dropPartitionDesc) throws InvalidObjectException,
      MetaException {
    Connection con = null;
    Statement ps = null;
    PreparedStatement pss = null;
    boolean success = false;

    dbName = dbName.toLowerCase();
    tblName = tblName.toLowerCase();

    ArrayList<Path> pathToDel = null;
    Warehouse wh = new Warehouse(hiveConf);
    List<String> existsParts = new ArrayList<String>();

    try {
      con = getSegmentConnection(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("drop partition error, db=" + dbName + ", tbl=" + tblName
          + ", level=" + dropPartitionDesc.getLevel() + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("drop partition error, db=" + dbName + ", tbl=" + tblName
          + ", level=" + dropPartitionDesc.getLevel() + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      ps = con.createStatement();

      String tblType = null;
      boolean hasPriPart = false;
      boolean hasSubPart = false;
      String priPartType = null;
      String subPartType = null;

      long tblID = 0;
      ResultSet tblSet = null;
      boolean isTblFind = false;
      
      String tblFormat = null;
      String tblLocation = null;

      ResultSet partSet = null;
      String sql = null;
      
      HiveConf hconf = (HiveConf)hiveConf;
      boolean externalPartition = hconf.getBoolVar(HiveConf.ConfVars.HIVESUPPORTEXTERNALPARTITION);
      
      if (dropPartitionDesc.getLevel() == 0) {
        sql = "SELECT tbl_id, tbl_type, pri_part_type, tbl_format, tbl_location"
            + " from TBLS where db_name='" + dbName + "' and tbl_name='"
            + tblName + "'";

        tblSet = ps.executeQuery(sql);
        isTblFind = false;

        while (tblSet.next()) {
          isTblFind = true;
          tblID = tblSet.getLong(1);
          tblType = tblSet.getString(2);
          priPartType = tblSet.getString(3);
          tblFormat = tblSet.getString(4);
          tblLocation = tblSet.getString(5);
          
          if (priPartType != null && !priPartType.isEmpty()) {
            hasPriPart = true;
          }
          break;
        }

        tblSet.close();

        if (!isTblFind) {
          throw new MetaException("can not find table " + dbName + ":"
              + tblName);
        }

        if (!tblType.equalsIgnoreCase("MANAGED_TABLE")) {
          if(tblType.equalsIgnoreCase("EXTERNAL_TABLE") && tblFormat != null &&
            tblFormat.equalsIgnoreCase("pgdata")){
            LOG.error("drop partition error, db=" + dbName + ", tbl=" + tblName
              + ", level=" + dropPartitionDesc.getLevel() + ", msg=" + tblType + ":"
              + tblFormat + " can not support alter partition");
            throw new MetaException(tblType + ":" + tblFormat + " can not support alter partition");
          }
          
          if(externalPartition && tblType.equalsIgnoreCase("EXTERNAL_TABLE") && 
            (tblFormat == null || !tblFormat.equalsIgnoreCase("pgdata"))){
          }
          else{
            throw new MetaException(tblType + " can not support alter partition");
          }
        }

        if (!hasPriPart) {
          throw new MetaException("table " + dbName + ":" + tblName
              + " is not pri-partitioned");
        }

        List<String> delPartName = dropPartitionDesc.getPartNames();
        ResultSet ret = null;
        
        for (String del : delPartName) 
        {
          sql = "delete from partitions where tbl_id=" + tblID + " and part_name='" + del + "' and level=0 returning part_name";
          
          ret = ps.executeQuery(sql);
          while(ret.next())
          {
            existsParts.add(del);
            break;
          }
          ret.close();
        }
        
        

        //pss = con
        //    .prepareStatement("delete from partitions where tbl_id=? and part_name=? and level=0 returing part_name");
        //for (String del : delPartName) {
        //  pss.setLong(1, tblID);
        //  pss.setString(2, del);
        //  pss.addBatch();
        //}
        //pss.executeBatch();

        pathToDel = new ArrayList<Path>();
        
        if(!tblType.equalsIgnoreCase("EXTERNAL_TABLE")){
	        for (String del : existsParts) {
	          if(tblLocation == null || tblLocation.trim().isEmpty()){
	            pathToDel.add(wh.getPartitionPath(dbName, tblName, del));
	          }else{
	            pathToDel.add(Warehouse.getPartitionPath(new Path(tblLocation), del));
	          }
	        }
        }
        else{
	        //for (String del : existsParts) {
		    //    pathToDel.add(Warehouse.getPartitionPath(new Path(tblLocation),  del));
		    //}       	
        }
        
      } else if (dropPartitionDesc.getLevel() == 1) {
        sql = "SELECT tbl_id, tbl_type, sub_part_type, tbl_format, tbl_location"
            + " from TBLS where db_name='" + dbName + "' and tbl_name='"
            + tblName + "'";

        tblSet = ps.executeQuery(sql);
        isTblFind = false;

        while (tblSet.next()) {
          isTblFind = true;
          tblID = tblSet.getLong(1);
          tblType = tblSet.getString(2);
          subPartType = tblSet.getString(3);
          tblFormat = tblSet.getString(4);
          tblLocation = tblSet.getString(5);

          if (subPartType != null && !subPartType.isEmpty()) {
            hasSubPart = true;
          }
          break;
        }

        tblSet.close();

        if (!isTblFind) {
          throw new MetaException("can not find table " + dbName + ":"
              + tblName);
        }

        if (!tblType.equalsIgnoreCase("MANAGED_TABLE")) {
          if(tblType.equalsIgnoreCase("EXTERNAL_TABLE") && tblFormat != null &&
              tblFormat.equalsIgnoreCase("pgdata")){
              LOG.error("drop partition error, db=" + dbName + ", tbl=" + tblName
                + ", level=" + dropPartitionDesc.getLevel() + ", msg=" + tblType + ":"
                + tblFormat + " can not support alter partition");
            throw new MetaException(tblType + ":" + tblFormat + " can not support alter partition");
          }
          
          if(externalPartition && tblType.equalsIgnoreCase("EXTERNAL_TABLE") && 
            (tblFormat == null || !tblFormat.equalsIgnoreCase("pgdata"))){
          }
          else{
            throw new MetaException(tblType + " can not support alter partition");
          }
        }

        if (!hasSubPart) {
          throw new MetaException("table " + dbName + ":" + tblName
              + " is not sub-partitioned");
        }

        Set<String> priPartNameSet = new TreeSet<String>();

        List<String> delSubPartName = dropPartitionDesc.getPartNames();

        sql = "SELECT level, part_name from PARTITIONS where" + " tbl_id="
            + tblID + " order by level asc";

        partSet = ps.executeQuery(sql);
        int partLevel = 0;

        while (partSet.next()) {
          partLevel = partSet.getInt(1);
          if (partLevel == 0) {
            String partName = partSet.getString(2);
            priPartNameSet.add(partName);
          }
        }

        partSet.close();

        //pss = con
        //    .prepareStatement("delete from partitions where tbl_id=? and part_name=? and level=1");
        //for (String del : delSubPartName) {
        //  pss.setLong(1, tblID);
        //  pss.setString(2, del);
        //  pss.addBatch();
       // }
       // pss.executeBatch();
        
        ResultSet ret = null;
        
        for (String del : delSubPartName) 
        {
          sql = "delete from partitions where tbl_id=" + tblID + " and part_name='" + del + "' and level=1 returning part_name";
          
          ret = ps.executeQuery(sql);
          while(ret.next())
          {
            existsParts.add(del);
            break;
          }
          ret.close();
        }

        pathToDel = new ArrayList<Path>();
        
        if(!tblType.equalsIgnoreCase("EXTERNAL_TABLE")){
          for (String str : existsParts) {
            if(tblLocation == null || tblLocation.trim().isEmpty()){
              pathToDel.addAll(wh.getSubPartitionPaths(dbName, tblName,
                  priPartNameSet, str));
            }else{
              pathToDel.addAll(Warehouse.getSubPartitionPaths(new Path(tblLocation),
                  priPartNameSet, str));
            }
          }
        }
        else{
            //for (String str : existsParts) {
            //    pathToDel.addAll(Warehouse.getSubPartitionPaths(new Path(tblLocation),
            //        priPartNameSet, str));
            //}
        }
      }

      if(pathToDel != null && !pathToDel.isEmpty())
      {
        for (Path path : pathToDel) {
          wh.deleteDirThrowExp(path, true);
        }
      }
      
      con.commit();
      success = true;
    } catch (SQLException ex) {
      ex.printStackTrace();
      LOG.error("drop partition error, db=" + dbName + ", tbl=" + tblName
          + ", level=" + dropPartitionDesc.getLevel() + ", msg="
          + ex.getMessage());
      throw new MetaException(ex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeStatement(pss);
      closeConnection(con);
    }

    //if (success) {
    //  for (Path path : pathToDel) {
    //    wh.deleteDir(path, true);
    //  }
    //} else {
    //  return;
    //}
  }

  @Override
  public void addDefaultPartition(String dbName, String tblName, int level)
      throws InvalidObjectException, MetaException {
    boolean success = false;

    Connection con = null;
    Statement ps = null;
    PreparedStatement pss = null;

    dbName = dbName.toLowerCase();
    tblName = tblName.toLowerCase();

    boolean isPathMaked = false;
    ArrayList<Path> pathToMake = new ArrayList<Path>();
    Warehouse wh = new Warehouse(hiveConf);
    long tblID = 0;

    try {
      con = getSegmentConnection(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("add default partition error, db=" + dbName + ", tbl="
          + tblName + ", level=" + level + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("add default partition error, db=" + dbName + ", tbl="
          + tblName + ", level=" + level + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      ps = con.createStatement();

      String tblType = null;
      boolean hasPriPart = false;
      boolean hasSubPart = false;
      String priPartKey = null;
      String subPartKey = null;
      String priPartType = null;
      String subPartType = null;
     
      String tblFormat = null;
      String tblLocation = null;
      
      String priKeyType = null;
      String subKeyType = null;
      ResultSet tblSet = null;
      boolean isTblFind = false;

      ArrayList<String> partToAdd = new ArrayList<String>();
      String sql = null;
      
      HiveConf hconf = (HiveConf)hiveConf;
      boolean externalPartition = hconf.getBoolVar(HiveConf.ConfVars.HIVESUPPORTEXTERNALPARTITION);

      if (level == 0) {
        sql = "SELECT tbl_id, tbl_type, pri_part_type, pri_part_key, tbl_format, tbl_location"
            + " from TBLS where db_name='" + dbName + "' and tbl_name='"
            + tblName + "'";

        tblSet = ps.executeQuery(sql);
        isTblFind = false;

        while (tblSet.next()) {
          isTblFind = true;
          tblID = tblSet.getLong(1);
          tblType = tblSet.getString(2);
          priPartKey = tblSet.getString(4);
          priPartType = tblSet.getString(3);
          tblFormat = tblSet.getString(5);
          tblLocation = tblSet.getString(6);

          if (priPartType != null && !priPartType.isEmpty()) {
            hasPriPart = true;
          }
          break;
        }

        tblSet.close();

        if (!isTblFind) {
          LOG.error("add partition error, db=" + dbName + ", tbl=" + tblName
              + ", level=" + level + ", msg=" + "can not find table " + dbName
              + ":" + tblName);

          throw new MetaException("can not find table " + dbName + ":"
              + tblName);
        }

        if (!tblType.equalsIgnoreCase("MANAGED_TABLE")) {
          if(tblType.equalsIgnoreCase("EXTERNAL_TABLE") && tblFormat != null &&
              tblFormat.equalsIgnoreCase("pgdata")){
              LOG.error("add default partition error, db=" + dbName + ", tbl=" + tblName
                + ", level=" + level + ", msg=" + tblType + ":" + tblFormat + " can not support alter partition");
            throw new MetaException(tblType + ":" + tblFormat + " can not support alter partition");
          }
          
          if(externalPartition && tblType.equalsIgnoreCase("EXTERNAL_TABLE") && 
            (tblFormat == null || !tblFormat.equalsIgnoreCase("pgdata"))){
          }
          else{
		        LOG.error("add default partition error, db=" + dbName + ", tbl=" + tblName
		            + ", level=" + level + ", msg=" + tblType
		            + " can not support alter partition");
		
		        throw new MetaException(tblType + " can not support alter partition");
          }
        }

        if (!hasPriPart) {
          LOG.error("add partition error, db=" + dbName + ", tbl=" + tblName
              + ", level=" + level + ", msg=" + "table " + dbName + ":"
              + tblName + " is not pri-partitioned");

          throw new MetaException("table " + dbName + ":" + tblName
              + " is not pri-partitioned");
        }

        List<String> partNames = new ArrayList<String>();
        Set<String> subPartNameSet = new TreeSet<String>();

        sql = "SELECT level, part_name from PARTITIONS where" + " tbl_id="
            + tblID + " order by level asc";

        ResultSet partSet = ps.executeQuery(sql);
        int partLevel = 0;

        while (partSet.next()) {
          partLevel = partSet.getInt(1);

          if (partLevel == 0) {
            String partName = partSet.getString(2);
            partNames.add(partName);
          } else if (partLevel == 1) {
            String partName = partSet.getString(2);
            subPartNameSet.add(partName);
          }
        }
        partSet.close();


        if (partNames.contains("default")) {
          LOG.error("add partition error, db=" + dbName + ", tbl=" + tblName
              + ", level=" + level + ", msg=" + "table : " + tblName
              + " have already contain a pri parititon named: default");

          throw new MetaException("table : " + tblName
              + " have already contain a pri parititon named: default");
        }

        pss = con.prepareStatement("INSERT INTO partitions(level, tbl_id, "
            + " part_name, part_values) values(?,?,?,?)");

        pss.setInt(1, 0);

        pss.setLong(2, tblID);
        pss.setString(3, "default");

        Array spaceArray = con.createArrayOf("varchar",
            new ArrayList<String>().toArray());
        pss.setArray(4, spaceArray);

        pss.executeUpdate();
        
        if(!tblType.equalsIgnoreCase("EXTERNAL_TABLE")){
          if(tblLocation == null || tblLocation.trim().isEmpty()){
            pathToMake.addAll(wh.getPriPartitionPaths(dbName, tblName, "default",
                subPartNameSet));            
          }else{
            pathToMake.addAll(Warehouse.getPriPartitionPaths(new Path(tblLocation), "default",
                subPartNameSet));
          }
        }
        else{
        	pathToMake.addAll(Warehouse.getPriPartitionPaths(new Path(tblLocation), "default",
    	            subPartNameSet));
        }
      } else if (level == 1) {
        sql = "SELECT tbl_id, tbl_type, sub_part_type, sub_part_key, tbl_format, tbl_location"
            + " from TBLS where db_name='" + dbName.toLowerCase()
            + "' and tbl_name='" + tblName.toLowerCase() + "'";

        tblSet = ps.executeQuery(sql);
        isTblFind = false;

        while (tblSet.next()) {
          isTblFind = true;
          tblID = tblSet.getLong(1);
          tblType = tblSet.getString(2);
          subPartKey = tblSet.getString(4);
          subPartType = tblSet.getString(3);
          tblFormat = tblSet.getString(5);
          tblLocation = tblSet.getString(6);

          if (subPartType != null && !subPartType.isEmpty()) {
            hasSubPart = true;
          }

          break;
        }

        tblSet.close();

        if (!isTblFind) {
          LOG.error("add default partition error, db=" + dbName + ", tbl="
              + tblName + ", level=" + level + ", msg=" + "can not find table "
              + dbName + ":" + tblName);

          throw new MetaException("can not find table " + dbName + ":"
              + tblName);
        }

        if (!tblType.equalsIgnoreCase("MANAGED_TABLE")) {
          if(tblType.equalsIgnoreCase("EXTERNAL_TABLE") && tblFormat != null &&
              tblFormat.equalsIgnoreCase("pgdata")){
              LOG.error("add default partition error, db=" + dbName + ", tbl=" + tblName
                + ", level=" + level + ", msg=" + tblType + ":"
                + tblFormat + " can not support alter partition");
            throw new MetaException(tblType + ":" + tblFormat + " can not support alter partition");
          }
            if(externalPartition && tblType.equalsIgnoreCase("EXTERNAL_TABLE") && 
            		(tblFormat == null || !tblFormat.equalsIgnoreCase("pgdata"))){
            }
            else{
		        LOG.error("add default partition error, db=" + dbName + ", tbl="
		            + tblName + ", level=" + level + ", msg=" + tblType
		            + " can not support alter partition");
		
		        throw new MetaException(tblType + " can not support alter partition");
            }
        }

        if (!hasSubPart) {
          LOG.error("add default partition error, db=" + dbName + ", tbl="
              + tblName + ", level=" + level + ", msg=" + "table " + dbName
              + ":" + tblName + " is not sun-partitioned");

          throw new MetaException("table " + dbName + ":" + tblName
              + " is not sun-partitioned");
        }

        List<String> partNames = new ArrayList<String>();
        Set<String> partNameSet = new TreeSet<String>();

        sql = "SELECT level,  part_name from PARTITIONS where" + " tbl_id="
            + tblID + " order by level asc";
        ResultSet partSet = ps.executeQuery(sql);
        int partLevel = 0;

        while (partSet.next()) {
          partLevel = partSet.getInt(1);

          if (partLevel == 1) {
            String partName = partSet.getString(2);
            partNames.add(partName);
          } else if (partLevel == 0) {
            String partName = partSet.getString(2);
            partNameSet.add(partName);
          }
        }

        partSet.close();

        if (partNames.contains("default")) {
          LOG.error("add default partition error, db=" + dbName + ", tbl="
              + tblName + ", level=" + level + ", msg=" + "table : " + tblName
              + " have already contain a sub parititon named: default");

          throw new MetaException("table : " + tblName
              + " have already contain a sub parititon named: default");
        }

        pss = con.prepareStatement("INSERT INTO partitions(level, tbl_id, "
            + " part_name, part_values) values(?,?,?,?)");

        pss.setInt(1, 1);
        pss.setLong(2, tblID);
        pss.setString(3, "default");

        Array spaceArray = con.createArrayOf("varchar",
            new ArrayList<String>().toArray());
        pss.setArray(4, spaceArray);

        pss.executeUpdate();
        
        if(!tblType.equalsIgnoreCase("EXTERNAL_TABLE")){
          if(tblLocation == null || tblLocation.trim().isEmpty()){
            pathToMake.addAll(wh.getSubPartitionPaths(dbName, tblName, partNameSet,
                "default"));
          }else{
            pathToMake.addAll(Warehouse.getSubPartitionPaths(new Path(tblLocation),
                partNameSet, "default"));
          }
        }
        else{
		    pathToMake.addAll(Warehouse.getSubPartitionPaths(new Path(tblLocation),
		              partNameSet, "default"));
        }
      }

      con.commit();
      success = true;
    } catch (SQLException ex) {
      LOG.error("add partition error, db=" + dbName + ", tbl=" + tblName
          + ", level=" + level + ", msg=" + ex.getMessage());

      throw new MetaException(ex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }

        if (isPathMaked) {
          for (Path path : pathToMake) {
            wh.deleteDir(path, false);
          }
        }
      }

      closeStatement(ps);
      closeStatement(pss);
      closeConnection(con);
    }
    
    if (success) {
      boolean mkDirOK = false;
      List<Path> createdPath = new ArrayList<Path>();
      try
      {
        for (Path path : pathToMake) {
          mkDirOK = wh.mkdirs(path);
          if(!mkDirOK){
            break;
          }
          
          createdPath.add(path);
        }
      }
      catch(Exception x)
      {
        mkDirOK = false;
      }

      if(!mkDirOK)
      {
        dropPartitionMeta(dbName, tblID, "default", level);
        if(!createdPath.isEmpty())
        {
          for (Path path : createdPath) {
            wh.deleteDir(path, true);
          }
        }
      }
      
    }
    
  }

  @Override
  public void dropDefaultPartition(String dbName, String tblName, int level)
      throws InvalidObjectException, MetaException {
    Connection con = null;
    Statement ps = null;
    boolean success = false;

    dbName = dbName.toLowerCase();
    tblName = tblName.toLowerCase();

    ArrayList<Path> pathToDel = null;
    Warehouse wh = new Warehouse(hiveConf);

    try {
      con = getSegmentConnection(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("drop default partition error, db=" + dbName + ", tbl="
          + tblName + ", level=" + level + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("drop default partition error, db=" + dbName + ", tbl="
          + tblName + ", level=" + level + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      ps = con.createStatement();

      String tblType = null;
      boolean hasPriPart = false;
      boolean hasSubPart = false;
      String priPartType = null;
      String subPartType = null;

      long tblID = 0;
      ResultSet tblSet = null;
      boolean isTblFind = false;
      
      String tblFormat = null;
      String tblLocation = null;

      ResultSet partSet = null;

      String sql = null;
      
      HiveConf hconf = (HiveConf)hiveConf;
      boolean externalPartition = hconf.getBoolVar(HiveConf.ConfVars.HIVESUPPORTEXTERNALPARTITION);

      if (level == 0) {
        sql = "SELECT tbl_id, tbl_type, pri_part_type, tbl_format, tbl_location"
            + " from TBLS where db_name='" + dbName + "' and tbl_name='"
            + tblName + "'";

        tblSet = ps.executeQuery(sql);
        isTblFind = false;

        while (tblSet.next()) {
          isTblFind = true;
          tblID = tblSet.getLong(1);
          tblType = tblSet.getString(2);
          priPartType = tblSet.getString(3);
          tblFormat = tblSet.getString(4);
          tblLocation = tblSet.getString(5);
          
          if (priPartType != null && !priPartType.isEmpty()) {
            hasPriPart = true;
          }
          break;
        }

        tblSet.close();

        if (!isTblFind) {
          throw new MetaException("can not find table " + dbName + ":"
              + tblName);
        }

        if (!tblType.equalsIgnoreCase("MANAGED_TABLE")) {
          if(tblType.equalsIgnoreCase("EXTERNAL_TABLE") && tblFormat != null &&
              tblFormat.equalsIgnoreCase("pgdata")){
              LOG.error("drop default partition error, db=" + dbName + ", tbl=" + tblName
                + ", level=" + level + ", msg=" + tblType + ":"
                + tblFormat + " can not support alter partition");
            throw new MetaException(tblType + ":" + tblFormat + " can not support alter partition");
          }
            if(externalPartition && tblType.equalsIgnoreCase("EXTERNAL_TABLE") && 
            		(tblFormat == null || !tblFormat.equalsIgnoreCase("pgdata"))){
            }
            else{
                throw new MetaException(tblType + " can not support alter partition");
            }
        }

        if (!hasPriPart) {
          throw new MetaException("table " + dbName + ":" + tblName
              + " is not pri-partitioned");
        }

        sql = "delete from partitions where tbl_id=" + tblID + " and part_name='default' and level=0 returning 'default'";
        ResultSet ret = ps.executeQuery(sql);
        while(ret.next())
        {
          pathToDel = new ArrayList<Path>();
          if(!tblType.equalsIgnoreCase("EXTERNAL_TABLE")){
            if(tblLocation == null || tblLocation.trim().isEmpty()){
              pathToDel.add(wh.getPartitionPath(dbName, tblName, "default"));
            }else{
              pathToDel.add(Warehouse.getPartitionPath(new Path(tblLocation), "default"));
            }
          }
          else{
        	  //pathToDel.add(Warehouse.getPartitionPath(new Path(tblLocation), "default"));
          }
          break;
        }
        ret.close();
        
        //ps.executeUpdate(sql);
        //pathToDel = new ArrayList<Path>();

        //pathToDel.add(wh.getPartitionPath(dbName, tblName, "default"));
      } else if (level == 1) {
        sql = "SELECT tbl_id, tbl_type, sub_part_type, tbl_format, tbl_location"
            + " from TBLS where db_name='" + dbName + "' and tbl_name='"
            + tblName + "'";

        tblSet = ps.executeQuery(sql);
        isTblFind = false;

        while (tblSet.next()) {
          isTblFind = true;
          tblID = tblSet.getLong(1);
          tblType = tblSet.getString(2);
          subPartType = tblSet.getString(3);
          tblFormat = tblSet.getString(4);
          tblLocation = tblSet.getString(5);

          if (subPartType != null && !subPartType.isEmpty()) {
            hasSubPart = true;
          }
          break;
        }

        tblSet.close();

        if (!isTblFind) {
          throw new MetaException("can not find table " + dbName + ":"
              + tblName);
        }

        if (!tblType.equalsIgnoreCase("MANAGED_TABLE")) {
          if(tblType.equalsIgnoreCase("EXTERNAL_TABLE") && tblFormat != null &&
              tblFormat.equalsIgnoreCase("pgdata")){
              LOG.error("drop default partition error, db=" + dbName + ", tbl=" + tblName
                + ", level=" + level + ", msg=" + tblType + ":"
                + tblFormat + " can not support alter partition");
            throw new MetaException(tblType + ":" + tblFormat + " can not support alter partition");
          }
            if(externalPartition && tblType.equalsIgnoreCase("EXTERNAL_TABLE") && 
            		(tblFormat == null || !tblFormat.equalsIgnoreCase("pgdata"))){
            }
            else{
                throw new MetaException(tblType + "can not support alter partition");
            }
        }

        if (!hasSubPart) {
          throw new MetaException("table " + dbName + ":" + tblName
              + " is not sub-partitioned");
        }

        Set<String> priPartNameSet = new TreeSet<String>();

        sql = "SELECT level, part_name from PARTITIONS where" + " tbl_id="
            + tblID + " and level=0 order by level asc";

        partSet = ps.executeQuery(sql);
        int partLevel = 0;

        while (partSet.next()) {
          partLevel = partSet.getInt(1);
          if (partLevel == 0) {
            String partName = partSet.getString(2);
            priPartNameSet.add(partName);
          }
        }

        partSet.close();

        sql = "delete from partitions where tbl_id=" + tblID
            + " and part_name='default' and level=1 return 'default'";
        
        ResultSet ret = ps.executeQuery(sql);
        while(ret.next())
        {
          pathToDel = new ArrayList<Path>();
          if(!tblType.equalsIgnoreCase("EXTERNAL_TABLE")){
            if(tblLocation == null || tblLocation.trim().isEmpty()){
              pathToDel.addAll(wh.getSubPartitionPaths(dbName, tblName,
                  priPartNameSet, "default"));
            }else{
              pathToDel.addAll(Warehouse.getSubPartitionPaths(new Path(tblLocation),
                  priPartNameSet, "default"));
            }
          }
          else{
	          //pathToDel.addAll(Warehouse.getSubPartitionPaths(new Path(tblLocation),
		      //        priPartNameSet, "default"));
          }
          break;
        }
        ret.close();
        
        //ps.executeUpdate(sql);

        //pathToDel = new ArrayList<Path>();
        //pathToDel.addAll(wh.getSubPartitionPaths(dbName, tblName,
        //    priPartNameSet, "default"));
      }
      
      if(pathToDel != null && !pathToDel.isEmpty())
      {
        for (Path path : pathToDel) {
          wh.deleteDirThrowExp(path, true);
        }
      }

      con.commit();
      success = true;
    } catch (SQLException ex) {
      ex.printStackTrace();
      LOG.error("drop partition error, db=" + dbName + ", tbl=" + tblName
          + ", level=" + level + ", msg=" + ex.getMessage());
      throw new MetaException(ex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    //if (success) {
    //  for (Path path : pathToDel) {
    //    wh.deleteDir(path, true);
    //  }
    //} else {
    //  return;
    //}
  }

  @Override
  public void renameTable(String dbName, String tblName, String modifyUser,
      String newName) throws InvalidOperationException, MetaException {
    boolean useDistributeTran = hiveConf.getBoolean(
        "hive.metadata.usedistributetransaction", false);
    if (!useDistributeTran) {
      renameTableNoDistributeTrans(dbName, tblName, modifyUser, newName);
    } else {
      renameTableByDistributeTrans(dbName, tblName, modifyUser, newName);
    }
  }

  public void renameTableByDistributeTrans(String dbName, String tblName,
      String modifyUser, String newName) throws InvalidOperationException,
      MetaException {
    dbName = dbName.toLowerCase();
    tblName = tblName.toLowerCase();
    modifyUser = modifyUser.toLowerCase();
    newName = newName.toLowerCase();

    boolean success = false;

    Connection masterConn = null;
    Connection slaveConn = null;

    PGXADataSource masterDS = null;
    PGXADataSource slaveDS = null;

    String slaveUrl = getSegmentDBURL(dbName);

    masterDS = getXADataSource(globalDbUrl, user, passwd);
    slaveDS = getXADataSource(slaveUrl, user, passwd);

    XAConnection masterDSXaConn = null;
    XAConnection slaveDSXaConn = null;

    XAResource masterSaRes = null;
    XAResource slaveSaRes = null;

    int formatID = genFormatID();
    Xid masterXid = new MyXid(formatID, new byte[] { 0x01 },
        new byte[] { 0x02 });
    Xid slaveXid = new MyXid(formatID, new byte[] { 0x11 }, new byte[] { 0x12 });

    Statement masterStmt = null;
    Statement slaveStmt = null;

    boolean isMoved = false;
    Path newPath = null;
    Path oldPath = null;
    FileSystem oldFs = null;
    FileSystem newFs = null;
    Warehouse wh = null;
    String newLocation = null;

    try {
      masterDSXaConn = masterDS.getXAConnection();
      slaveDSXaConn = slaveDS.getXAConnection();

      masterSaRes = masterDSXaConn.getXAResource();
      slaveSaRes = slaveDSXaConn.getXAResource();

      masterConn = masterDSXaConn.getConnection();
      slaveConn = slaveDSXaConn.getConnection();

      masterStmt = masterConn.createStatement();
      slaveStmt = slaveConn.createStatement();

      try {
        masterSaRes.start(masterXid, XAResource.TMNOFLAGS);
        String sql = "update tblpriv set tbl_name='" + newName
            + "' where db_name='" + dbName + "' and tbl_name='" + tblName + "'";
        masterStmt.executeUpdate(sql);

        masterSaRes.end(masterXid, XAResource.TMSUCCESS);

        slaveSaRes.start(slaveXid, XAResource.TMNOFLAGS);

        sql = "select tbl_id, tbl_type, tbl_location, serde_lib from tbls "
            + "where db_name='" + dbName + "' and tbl_name='" + tblName + "'";

        boolean isTblFind = false;
        long tblID = 0;
        ResultSet tblSet = slaveStmt.executeQuery(sql);
        String tblType = null;
        String oldLocation = null;
        String serdeLib = null;

        while (tblSet.next()) {
          isTblFind = true;
          tblID = tblSet.getLong(1);
          tblType = tblSet.getString(2);
          oldLocation = tblSet.getString(3);
          serdeLib = tblSet.getString(4);
          break;
        }
        tblSet.close();

        if (!isTblFind) {
          throw new MetaException("can not find table " + dbName + ":"
              + tblName);
        }

        if (!tblType.equalsIgnoreCase("MANAGED_TABLE")) {
          throw new MetaException("only manage table can rename ");
        }

        if (serdeLib.equals(ProtobufSerDe.class.getName())) {
          throw new MetaException(
              "Renaming table is not supported for protobuf table. SerDe may be incompatible");
        }

        Map<String, String> tblParamMap = new HashMap<String, String>();
        sql = "select param_key, param_value from table_params where tbl_id="
            + tblID + " and param_type='TBL'";
        ResultSet paramSet = slaveStmt.executeQuery(sql);
        while (paramSet.next()) {
          tblParamMap.put(paramSet.getString(1), paramSet.getString(2));
        }
        paramSet.close();

        boolean containTime = false;
        boolean contailUser = false;
        if (tblParamMap.containsKey("last_modified_time"))
          containTime = true;
        if (tblParamMap.containsKey("last_modified_by"))
          contailUser = true;

        if (containTime && contailUser) {
          slaveStmt.executeUpdate("update table_params set param_value='"
              + String.valueOf(System.currentTimeMillis() / 1000)
              + "' where tbl_id=" + tblID
              + " and param_type='TBL' and param_key='last_modified_time'");

          slaveStmt.executeUpdate("update table_params set param_value='"
              + modifyUser + "' where tbl_id=" + tblID
              + " and param_type='TBL' and param_key='last_modified_by'");
        } else if (!containTime && !contailUser) {
          slaveStmt
              .executeUpdate("insert into table_params(tbl_id, param_type, param_key, param_value) values("
                  + tblID
                  + ", 'TBL', 'last_modified_time', '"
                  + String.valueOf(System.currentTimeMillis() / 1000) + "')");

          slaveStmt
              .executeUpdate("insert into table_params(tbl_id, param_type, param_key, param_value) values("
                  + tblID
                  + ", 'TBL', 'last_modified_by', '"
                  + modifyUser
                  + "')");
        } else if (containTime && !contailUser) {
          slaveStmt.executeUpdate("update table_params set param_value='"
              + String.valueOf(System.currentTimeMillis() / 1000)
              + "' where tbl_id=" + tblID
              + " and param_type='TBL' and param_key='last_modified_time'");

          slaveStmt
              .executeUpdate("insert into table_params(tbl_id, param_type, param_key, param_value) values("
                  + tblID
                  + ", 'TBL', 'last_modified_by', '"
                  + modifyUser
                  + "')");
        } else {
          slaveStmt
              .executeUpdate("insert into table_params(tbl_id, param_type, param_key, param_value) values("
                  + tblID
                  + ", 'TBL', 'last_modified_time', '"
                  + String.valueOf(System.currentTimeMillis() / 1000) + "')");

          slaveStmt.executeUpdate("update table_params set param_value='"
              + modifyUser + "' where tbl_id=" + tblID
              + " and param_type='TBL' and param_key='last_modified_by'");
        }

        wh = new Warehouse(hiveConf);
//        newLocation = wh.getDefaultTablePath(dbName, newName).toString();
        newLocation = oldLocation.substring(0, oldLocation.length()-tblName.length()) + newName;

        sql = "update tbls set tbl_name='" + newName + "', tbl_location='"
            + newLocation + "'" + " where tbl_id=" + tblID;
        slaveStmt.executeUpdate(sql);

        slaveSaRes.end(slaveXid, XAResource.TMSUCCESS);

        int masterRet = masterSaRes.prepare(masterXid);
        int slaveRet = slaveSaRes.prepare(slaveXid);

        oldPath = new Path(oldLocation);
        oldFs = wh.getFs(oldPath);
        newPath = new Path(newLocation);
        newFs = wh.getFs(newPath);

        if (oldFs != newFs) {
          throw new InvalidOperationException("table new location " + oldFs
              + " is on a different file system than the old location " + newFs
              + ". This operation is not supported");
        }

        try {
          oldFs.exists(oldPath); 
          if (newFs.exists(newPath)) {
            throw new InvalidOperationException("New location for this table "
                + dbName + "." + tblName + " already exists : " + newPath);
          }

        } catch (IOException e) {

          throw new InvalidOperationException("Unable to access new location "
              + newPath + " for table " + dbName + "." + tblName);
        }

        try {
          if (oldFs.exists(oldPath)) {
            oldFs.rename(oldPath, newPath);
          }
          isMoved = true;
        } catch (IOException e) {
          throw new InvalidOperationException("Unable to access old location "
              + oldPath + " for table " + dbName + "." + tblName);

        }

        if (masterRet == XAResource.XA_OK && slaveRet == XAResource.XA_OK) {
          masterSaRes.commit(masterXid, false);
          slaveSaRes.commit(slaveXid, false);

          success = true;
        }
      } catch (XAException e) {
        LOG.error("XAException rename table error, db=" + dbName + ", tbl="
            + tblName + ", new tbl=" + newName + ", msg=" + e.getMessage());
        e.printStackTrace();
        throw new MetaException(e.getMessage());
      }
    } catch (SQLException e) {
      LOG.error("XAException rename table error, db=" + dbName + ", tbl="
          + tblName + ", new tbl=" + newName + ", msg=" + e.getMessage());
      e.printStackTrace();
      throw new MetaException(e.getMessage());
    } finally {
      if (!success) {
        try {
          masterSaRes.rollback(masterXid);
        } catch (Exception x) {

        }

        try {
          slaveSaRes.rollback(slaveXid);
        } catch (Exception x) {

        }
        if (isMoved) {
          try {
            if (oldFs.exists(oldPath)) {
              oldFs.rename(newPath, oldPath);
            }

          } catch (IOException e) {
            throw new InvalidOperationException(
                "Unable to access old location " + oldPath + " for table "
                    + dbName + "." + tblName);

          }
        }
      }

      closeStatement(masterStmt);
      closeStatement(slaveStmt);

      closeConnection(masterConn);
      closeConnection(slaveConn);

      closeXAConnection(masterDSXaConn);
      closeXAConnection(slaveDSXaConn);
    }

    if (success) {
      Statement stmt = null;
      Connection con = null;
      try {
        con = getGlobalConnection();
        stmt = con.createStatement();
        String sql = "update tblsensitivity set tbl_name='" + newName
            + "' where db_name='" + dbName + "' and tbl_name='" + tblName + "'";

        stmt.executeUpdate(sql);
      } catch (Exception e1) {
        LOG.error("update tblsenstivity table error, db=" + dbName + ", tbl="
            + tblName + ", msg=" + e1.getMessage());
      } finally {
        closeStatement(stmt);
        closeConnection(con);
      }
    }
  }

  public void renameTableNoDistributeTrans(String dbName, String tblName,
      String modifyUser, String newName) throws InvalidOperationException,
      MetaException {
    if (!MetaStoreUtils.validateName(newName)) {
      throw new InvalidOperationException(newName
          + " is not a valid object name");
    }

    if (tblName.equals(newName)) {
      return;
    }

    dbName = dbName.toLowerCase();
    tblName = tblName.toLowerCase();
    modifyUser = modifyUser.toLowerCase();
    newName = newName.toLowerCase();

    Connection con = null;
    PreparedStatement ps = null;
    String newLocation = null;
    String oldLocation = null;
    String serdeLib = null;
    String tblType = null;
    boolean isMoved = false;
    Path newPath = null;
    Path oldPath = null;
    FileSystem oldFs = null;
    FileSystem newFs = null;
    boolean success = false;

    try {
      con = getSegmentConnection(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("rename table error, db=" + dbName + ", tbl=" + tblName
          + ", newName=" + newName + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("rename table error, db=" + dbName + ", tbl=" + tblName
          + ", newName=" + newName + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con
          .prepareStatement("select tbl_id, tbl_type, tbl_location, serde_lib from TBLS where db_name=? and tbl_name=?");
      ps.setString(1, dbName);
      ps.setString(2, tblName);

      boolean isTblFind = false;
      long tblID = 0;
      ResultSet tblSet = ps.executeQuery();

      while (tblSet.next()) {
        isTblFind = true;
        tblID = tblSet.getLong(1);
        tblType = tblSet.getString(2);
        oldLocation = tblSet.getString(3);
        serdeLib = tblSet.getString(4);
        break;
      }
      tblSet.close();
      ps.close();

      if (!isTblFind) {
        throw new MetaException("can not find table " + dbName + ":" + tblName);
      }

      if (!tblType.equalsIgnoreCase("MANAGED_TABLE")) {
        throw new MetaException("only manage table can rename ");
      }

      if (serdeLib.equals(ProtobufSerDe.class.getName())) {
        throw new MetaException(
            "Renaming table is not supported for protobuf table. SerDe may be incompatible");
      }

      

      Map<String, String> tblParamMap = new HashMap<String, String>();
      ps = con
          .prepareStatement("select param_key, param_value from table_params where tbl_id=? and param_type='TBL'");
      ps.setLong(1, tblID);
      ResultSet paramSet = ps.executeQuery();
      while (paramSet.next()) {
        tblParamMap.put(paramSet.getString(1), paramSet.getString(2));
      }
      paramSet.close();
      ps.close();
      boolean containTime = false;
      boolean contailUser = false;
      if (tblParamMap.containsKey("last_modified_time"))
        containTime = true;
      if (tblParamMap.containsKey("last_modified_by"))
        contailUser = true;

      if (containTime && contailUser) {
        ps = con
            .prepareStatement("update table_params set param_value=? where tbl_id=? and param_type='TBL' and param_key=?");
        ps.setString(1, String.valueOf(System.currentTimeMillis() / 1000));
        ps.setLong(2, tblID);
        ps.setString(3, "last_modified_time");
        ps.addBatch();
        ps.setString(1, modifyUser);
        ps.setLong(2, tblID);
        ps.setString(3, "last_modified_by");
        ps.addBatch();

        ps.executeBatch();
        ps.close();
      } else if (!containTime && !contailUser) {
        ps = con
            .prepareStatement("insert into table_params(tbl_id, param_type, param_key, param_value) "
                + " values(?,?,?,?)");
        ps.setLong(1, tblID);
        ps.setString(2, "TBL");
        ps.setString(3, "last_modified_time");
        ps.setString(4, String.valueOf(System.currentTimeMillis() / 1000));
        ps.addBatch();

        ps.setLong(1, tblID);
        ps.setString(2, "TBL");
        ps.setString(3, "last_modified_by");
        ps.setString(4, modifyUser);

        ps.addBatch();

        ps.executeBatch();
        ps.close();
      } else if (containTime && !contailUser) {
        ps = con
            .prepareStatement("update table_params set param_value=? where tbl_id=? and param_type='TBL' and param_key=?");
        ps.setString(1, String.valueOf(System.currentTimeMillis() / 1000));
        ps.setLong(2, tblID);
        ps.setString(3, "last_modified_time");
        ps.addBatch();

        ps.executeBatch();
        ps.close();

        ps = con
            .prepareStatement("insert into table_params(tbl_id, param_type, param_key, param_value) "
                + " values(?,?,?,?)");

        ps.setLong(1, tblID);
        ps.setString(2, "TBL");
        ps.setString(3, "last_modified_by");
        ps.setString(4, modifyUser);

        ps.addBatch();

        ps.executeBatch();
        ps.close();
      } else {
        ps = con
            .prepareStatement("update table_params set param_value=? where tbl_id=? and param_type='TBL' and param_key=?");

        ps.setString(1, modifyUser);
        ps.setLong(2, tblID);
        ps.setString(3, "last_modified_by");
        ps.addBatch();

        ps.executeBatch();
        ps.close();

        ps = con
            .prepareStatement("insert into table_params(tbl_id, param_type, param_key, param_value) "
                + " values(?,?,?,?)");
        ps.setLong(1, tblID);
        ps.setString(2, "TBL");
        ps.setString(3, "last_modified_time");
        ps.setString(4, String.valueOf(System.currentTimeMillis() / 1000));
        ps.addBatch();

        ps.executeBatch();
        ps.close();
      }

      Warehouse wh = new Warehouse(hiveConf);
//      newLocation = wh.getDefaultTablePath(dbName, newName).toString();
      newLocation = oldLocation.substring(0, oldLocation.length()-tblName.length()) + newName;

      ps = con
          .prepareStatement("update tbls set tbl_name=?, tbl_location=? where tbl_id=?");
      ps.setString(1, newName.toLowerCase());
      ps.setString(2, newLocation);
      ps.setLong(3, tblID);

      ps.executeUpdate();
      ps.close();

      oldPath = new Path(oldLocation);
      oldFs = wh.getFs(oldPath);
      newPath = new Path(newLocation);
      newFs = wh.getFs(newPath);

      if (oldFs != newFs) {
        throw new InvalidOperationException("table new location " + oldFs
            + " is on a different file system than the old location " + newFs
            + ". This operation is not supported");
      }

      try {
        oldFs.exists(oldPath); 
        if (newFs.exists(newPath)) {
          throw new InvalidOperationException("New location for this table "
              + dbName + "." + tblName + " already exists : " + newPath);
        }

      } catch (IOException e) {

        throw new InvalidOperationException("Unable to access new location "
            + newPath + " for table " + dbName + "." + tblName);
      }

      try {
        if (oldFs.exists(oldPath)) {
          oldFs.rename(oldPath, newPath);
        }
        isMoved = true;
      } catch (IOException e) {
        throw new InvalidOperationException("Unable to access old location "
            + oldPath + " for table " + dbName + "." + tblName);

      }
      
      if (isMoved) {
        try {
          if (oldFs.exists(oldPath)) {
            oldFs.rename(newPath, oldPath);
          }

        } catch (IOException e) {
          throw new InvalidOperationException(
              "Unable to access old location " + oldPath + " for table "
                  + dbName + "." + tblName);

        }
      }

      con.commit();
      success = true;
    } catch (SQLException ex) {
      throw new MetaException(ex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }

        //if (isMoved) {
        //  try {
        //    if (oldFs.exists(oldPath)) {
        //      oldFs.rename(newPath, oldPath);
        //    }

        //  } catch (IOException e) {
        //    throw new InvalidOperationException(
        //        "Unable to access old location " + oldPath + " for table "
        //            + dbName + "." + tblName);

        //  }
        //}
      }

      closeStatement(ps);
      closeConnection(con);
    }

    success = false;
    Statement stmt = null;
    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("rename table error, db=" + dbName + ", tbl=" + tblName
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("rename table error, db=" + dbName + ", tbl=" + tblName
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      stmt = con.createStatement();

      String sql = "update tblpriv set tbl_name='" + newName
          + "' where db_name='" + dbName + "' and tbl_name='" + tblName + "'";

      stmt.executeUpdate(sql);

      try {
        sql = "update tblsensitivity set tbl_name='" + newName
            + "' where db_name='" + dbName + "' and tbl_name='" + tblName + "'";

        stmt.executeUpdate(sql);
      } catch (Exception x) {

      }

      con.commit();
      success = true;
    } catch (SQLException x) {
      LOG.error("rename table error, db=" + dbName + ", tbl=" + tblName
          + ", msg=" + x.getMessage());
      throw new MetaException(x.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(stmt);
      closeConnection(con);
    }

    return;
  }

  @Override
  public void addCols(String dbName, String tblName, String modifyUser,
      List<FieldSchema> newCols) throws InvalidOperationException,
      MetaException, InvalidObjectException {
    if (!MetaStoreUtils.validateColNames(newCols)) {
      throw new InvalidObjectException(
          "new add columns name is not valid object");
    }

    Connection con = null;
    PreparedStatement ps = null;
    boolean success = false;

    dbName = dbName.toLowerCase();
    tblName = tblName.toLowerCase();

    try {
      con = getSegmentConnection(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("add column error, db=" + dbName + ", tbl=" + tblName
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("add column error, db=" + dbName + ", tbl=" + tblName
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con
          .prepareStatement("select tbl_id, tbl_type, tbl_format, serde_lib"
              + "  from tbls where tbls.db_name=? and tbls.tbl_name=? ");

      ps.setString(1, dbName);
      ps.setString(2, tblName);

      String tblType = null;
      String serdeLib = null;
      String tblFormat = null;

      boolean isTblFind = false;
      long tblID = 0;

      ResultSet tblSet = ps.executeQuery();
      while (tblSet.next()) {
        isTblFind = true;
        tblID = tblSet.getLong(1);
        tblType = tblSet.getString(2);
        tblFormat = tblSet.getString(3);
        serdeLib = tblSet.getString(4);
      }

      tblSet.close();
      ps.close();

      if (!isTblFind) {
        LOG.error("add column error, db=" + dbName + ", tbl=" + tblName);
        throw new MetaException("can not find table " + dbName + ":" + tblName);
      }

      if (tblFormat == null || tblFormat.isEmpty()) {
        tblFormat = "text";
      }

      if (tblType.equalsIgnoreCase("VITURAL_VIEW")) {
        LOG.error("add column error, db=" + dbName + ", tbl=" + tblName);
        throw new InvalidOperationException("view can not add cloumns");
      }

      if (tblType.equalsIgnoreCase("EXTERNAL_TABLE")
          && !HiveConf.getBoolVar(hiveConf,
              ConfVars.ALTERSCHEMAACTIVATEEXTTABLE)) {
        LOG.error("add column error, db=" + dbName + ", tbl=" + tblName);
        throw new InvalidOperationException(
            "can not add columns for a extenal table ");
      }

      if (!tblType.equalsIgnoreCase("EXTERNAL_TABLE")
          && tblFormat.equalsIgnoreCase("text")
          && (!HiveConf.getBoolVar(hiveConf,
              ConfVars.ALTERSCHEMAACTIVATETXTTABLE))) {
        LOG.error("add column error, db=" + dbName + ", tbl=" + tblName);
        throw new InvalidOperationException(
            "can not add columns for a text format table ");
      }

      if (serdeLib != null && serdeLib.equals(ProtobufSerDe.class.getName())) {
        LOG.error("add column error, db=" + dbName + ", tbl=" + tblName);
        throw new InvalidOperationException(
            "can not add columns for a pb table ");
      }

      if (tblFormat != null
          && (tblFormat.equalsIgnoreCase("column") || tblFormat
              .equalsIgnoreCase("format"))) {
        for (FieldSchema field : newCols) {
          if (field.getType().equals(Constants.BOOLEAN_TYPE_NAME)) {
            LOG.error("add column error, db=" + dbName + ", tbl=" + tblName);
            throw new InvalidOperationException(
                "format file or column file not support boolean type rightnow");
          }
        }
      }

      Map<String, String> tblParamMap = new HashMap<String, String>();
      ps = con
          .prepareStatement("select param_key, param_value from table_params where tbl_id=? and param_type='TBL'");
      ps.setLong(1, tblID);
      ResultSet paramSet = ps.executeQuery();
      while (paramSet.next()) {
        tblParamMap.put(paramSet.getString(1), paramSet.getString(2));
      }
      paramSet.close();
      ps.close();
      boolean containTime = false;
      boolean contailUser = false;
      if (tblParamMap.containsKey("last_modified_time"))
        containTime = true;
      if (tblParamMap.containsKey("last_modified_by"))
        contailUser = true;

      if (containTime && contailUser) {
        ps = con
            .prepareStatement("update table_params set param_value=? where tbl_id=? and param_type='TBL' and param_key=?");
        ps.setString(1, String.valueOf(System.currentTimeMillis() / 1000));
        ps.setLong(2, tblID);
        ps.setString(3, "last_modified_time");
        ps.addBatch();
        ps.setString(1, modifyUser);
        ps.setLong(2, tblID);
        ps.setString(3, "last_modified_by");
        ps.addBatch();

        ps.executeBatch();
        ps.close();
      } else if (!containTime && !contailUser) {
        ps = con
            .prepareStatement("insert into table_params(tbl_id, param_type, param_key, param_value) "
                + " values(?,?,?,?)");
        ps.setLong(1, tblID);
        ps.setString(2, "TBL");
        ps.setString(3, "last_modified_time");
        ps.setString(4, String.valueOf(System.currentTimeMillis() / 1000));
        ps.addBatch();

        ps.setLong(1, tblID);
        ps.setString(2, "TBL");
        ps.setString(3, "last_modified_by");
        ps.setString(4, modifyUser);

        ps.addBatch();

        ps.executeBatch();
        ps.close();
      } else if (containTime && !contailUser) {
        ps = con
            .prepareStatement("update table_params set param_value=? where tbl_id=? and param_type='TBL' and param_key=?");
        ps.setString(1, String.valueOf(System.currentTimeMillis() / 1000));
        ps.setLong(2, tblID);
        ps.setString(3, "last_modified_time");
        ps.addBatch();

        ps.executeBatch();
        ps.close();

        ps = con
            .prepareStatement("insert into table_params(tbl_id, param_type, param_key, param_value) "
                + " values(?,?,?,?)");

        ps.setLong(1, tblID);
        ps.setString(2, "TBL");
        ps.setString(3, "last_modified_by");
        ps.setString(4, modifyUser);

        ps.addBatch();

        ps.executeBatch();
        ps.close();
      } else {
        ps = con
            .prepareStatement("update table_params set param_value=? where tbl_id=? and param_type='TBL' and param_key=?");

        ps.setString(1, modifyUser);
        ps.setLong(2, tblID);
        ps.setString(3, "last_modified_by");
        ps.addBatch();

        ps.executeBatch();
        ps.close();

        ps = con
            .prepareStatement("insert into table_params(tbl_id, param_type, param_key, param_value) "
                + " values(?,?,?,?)");
        ps.setLong(1, tblID);
        ps.setString(2, "TBL");
        ps.setString(3, "last_modified_time");
        ps.setString(4, String.valueOf(System.currentTimeMillis() / 1000));
        ps.addBatch();

        ps.executeBatch();
        ps.close();
      }

      if (serdeLib != null
          && serdeLib
              .equals("org.apache.hadoop.hive.serde.thrift.columnsetSerDe")) {
        ps = con.prepareStatement("delete from columns where tbl_id=?");
        ps.setLong(1, tblID);
        ps.executeUpdate();

        ps = con
            .prepareStatement("insert into columns(column_index, tbl_id, column_name, type_name, comment)"
                + " values(?,?,?,?,?)");
        long index = 0;

        for (FieldSchema field : newCols) {
          ps.setLong(1, index);
          ps.setLong(2, tblID);
          ps.setString(3, field.getName().toLowerCase());
          ps.setString(4, field.getType());
          ps.setString(5, field.getComment());

          ps.addBatch();
          index++;
        }
        ps.executeBatch();
        ps.close();

        ps = con.prepareStatement("update tbls set serde_lib=? where tbl_id=?");
        ps.setString(1, LazySimpleSerDe.class.getName());
        ps.setLong(2, tblID);
        ps.executeUpdate();
        ps.close();
      } else {
        Map<String, Long> colNameMap = new HashMap<String, Long>();
        long maxColIndex = 0;

        ps = con.prepareStatement("select column_name, column_index from "
            + "columns where tbl_id=? order by column_index asc");
        ps.setLong(1, tblID);

        ResultSet colSet = ps.executeQuery();
        while (colSet.next()) {
          maxColIndex = colSet.getLong(2);
          colNameMap.put(colSet.getString(1), maxColIndex);
        }

        colSet.close();
        ps.close();

        ps = con
            .prepareStatement("insert into columns(column_index, tbl_id, column_name, type_name, comment)"
                + " values(?,?,?,?,?)");

        for (FieldSchema field : newCols) {
          if (colNameMap.containsKey(field.getName())) {
            LOG.error("add column error, db=" + dbName + ", tbl=" + tblName);
            throw new MetaException(
                "column name conflict, conflict column name is "
                    + field.getName());
          }

          ps.setLong(1, maxColIndex + 1);
          ps.setLong(2, tblID);
          ps.setString(3, field.getName().toLowerCase());
          ps.setString(4, field.getType());
          ps.setString(5, field.getComment());

          maxColIndex++;
          ps.addBatch();
        }

        ps.executeBatch();
        ps.close();
      }

      con.commit();
      success = true;
    } catch (SQLException ex) {
      throw new MetaException(ex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }
    return;
  }

  private boolean checktype(FieldSchema col, String type) {
    if (HiveConf.getBoolVar(hiveConf, ConfVars.ALTERSCHEMAACTIVATENOTYPELIMIT)) {
      return true;
    }
    if (type == null || type.equalsIgnoreCase(col.getType()))
      return true;

    if (type.equalsIgnoreCase(Constants.TINYINT_TYPE_NAME)
        || type.equalsIgnoreCase(Constants.SMALLINT_TYPE_NAME)
        || type.equalsIgnoreCase(Constants.INT_TYPE_NAME)
        || type.equalsIgnoreCase(Constants.BOOLEAN_TYPE_NAME)
        || type.equalsIgnoreCase(Constants.FLOAT_TYPE_NAME)) {
      return false;
    }
    if (col.getType().equalsIgnoreCase(Constants.BOOLEAN_TYPE_NAME)) {
      return false;
    }
    if (col.getType().equalsIgnoreCase(Constants.STRING_TYPE_NAME)) {
      return false;
    }
    return true;
  }

  @Override
  public void renameCols(String dbName, String tblName,
      RenameColDesc renameColDesc) throws InvalidOperationException,
      MetaException, InvalidObjectException {
    if (!MetaStoreUtils.validateName(renameColDesc.getNewName())) {
      throw new InvalidObjectException("new column name is not valid object "
          + renameColDesc.getNewName());
    }

    Connection con = null;
    PreparedStatement ps = null;
    boolean success = false;

    dbName = dbName.toLowerCase();
    tblName = tblName.toLowerCase();

    try {
      con = getSegmentConnection(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("rename column error, db=" + dbName + ", tbl=" + tblName
          + ", newname=" + renameColDesc.getNewName() + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("rename column error, db=" + dbName + ", tbl=" + tblName
          + ", newname=" + renameColDesc.getNewName() + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con
          .prepareStatement("select tbl_id, tbl_type, tbl_format ,pri_part_key "
              + ", sub_part_key, serde_lib from tbls where db_name=? and tbl_name=?");

      ps.setString(1, dbName);
      ps.setString(2, tblName);

      String tblType = null;
      String serdeLib = null;
      String tblFormat = null;
      boolean isPriPart = false;
      boolean isSubPart = false;
      String priPartKey = null;
      String subPartKey = null;

      boolean isTblFind = false;
      long tblID = 0;

      ResultSet tblSet = ps.executeQuery();

      while (tblSet.next()) {
        isTblFind = true;
        tblID = tblSet.getLong(1);
        tblType = tblSet.getString(2);
        tblFormat = tblSet.getString(3);
        priPartKey = tblSet.getString(4);
        subPartKey = tblSet.getString(5);
        serdeLib = tblSet.getString(6);

        isPriPart = priPartKey != null;
        isSubPart = subPartKey != null;
      }

      tblSet.close();
      ps.close();

      if (!isTblFind) {
        LOG.error("rename column error, db=" + dbName + ", tbl=" + tblName
            + ", newname=" + renameColDesc.getNewName());
        throw new MetaException("can not find table " + dbName + ":" + tblName);
      }

      if (tblFormat == null || tblFormat.isEmpty()) {
        tblFormat = "text";
      }

      if (tblType.equalsIgnoreCase("VITURAL_VIEW")) {
        LOG.error("rename column error, db=" + dbName + ", tbl=" + tblName
            + ", newname=" + renameColDesc.getNewName());
        throw new MetaException("view can not rename a column");
      }

      if (tblType.equalsIgnoreCase("EXTERNAL_TABLE")
          && !HiveConf.getBoolVar(hiveConf,
              ConfVars.ALTERSCHEMAACTIVATEEXTTABLE)) {
        LOG.error("rename column error, db=" + dbName + ", tbl=" + tblName
            + ", newname=" + renameColDesc.getNewName());
        throw new MetaException("can not rename columns for a extenal table ");
      }

      if (!tblType.equalsIgnoreCase("EXTERNAL_TABLE")
          && tblFormat.equalsIgnoreCase("text")
          && (!HiveConf.getBoolVar(hiveConf,
              ConfVars.ALTERSCHEMAACTIVATETXTTABLE))) {
        LOG.error("rename column error, db=" + dbName + ", tbl=" + tblName
            + ", newname=" + renameColDesc.getNewName());
        throw new MetaException(
            "can not rename columns for a text format table ");
      } else if (serdeLib.equals(ProtobufSerDe.class.getName())) {
        LOG.error("rename column error, db=" + dbName + ", tbl=" + tblName
            + ", newname=" + renameColDesc.getNewName());
        throw new MetaException("can not rename columns for a pb table ");
      } else if ((isPriPart && renameColDesc.getOldName().equalsIgnoreCase(
          priPartKey))
          || (isSubPart && renameColDesc.getOldName().equalsIgnoreCase(
              subPartKey))) {
        LOG.error("rename column error, db=" + dbName + ", tbl=" + tblName
            + ", newname=" + renameColDesc.getNewName());
        throw new MetaException("partition key can not be altered  ");
      }

      

      String modifyUser = renameColDesc.getUser();
      Map<String, String> tblParamMap = new HashMap<String, String>();
      ps = con
          .prepareStatement("select param_key, param_value from table_params where tbl_id=? and param_type='TBL'");
      ps.setLong(1, tblID);
      ResultSet paramSet = ps.executeQuery();
      while (paramSet.next()) {
        tblParamMap.put(paramSet.getString(1), paramSet.getString(2));
      }
      paramSet.close();
      ps.close();
      boolean containTime = false;
      boolean contailUser = false;
      if (tblParamMap.containsKey("last_modified_time"))
        containTime = true;
      if (tblParamMap.containsKey("last_modified_by"))
        contailUser = true;

      if (containTime && contailUser) {
        ps = con
            .prepareStatement("update table_params set param_value=? where tbl_id=? and param_type='TBL' and param_key=?");
        ps.setString(1, String.valueOf(System.currentTimeMillis() / 1000));
        ps.setLong(2, tblID);
        ps.setString(3, "last_modified_time");
        ps.addBatch();
        ps.setString(1, modifyUser);
        ps.setLong(2, tblID);
        ps.setString(3, "last_modified_by");
        ps.addBatch();

        ps.executeBatch();
        ps.close();
      } else if (!containTime && !contailUser) {
        ps = con
            .prepareStatement("insert into table_params(tbl_id, param_type, param_key, param_value) "
                + " values(?,?,?,?)");
        ps.setLong(1, tblID);
        ps.setString(2, "TBL");
        ps.setString(3, "last_modified_time");
        ps.setString(4, String.valueOf(System.currentTimeMillis() / 1000));
        ps.addBatch();

        ps.setLong(1, tblID);
        ps.setString(2, "TBL");
        ps.setString(3, "last_modified_by");
        ps.setString(4, modifyUser);

        ps.addBatch();

        ps.executeBatch();
        ps.close();
      } else if (containTime && !contailUser) {
        ps = con
            .prepareStatement("update table_params set param_value=? where tbl_id=? and param_type='TBL' and param_key=?");
        ps.setString(1, String.valueOf(System.currentTimeMillis() / 1000));
        ps.setLong(2, tblID);
        ps.setString(3, "last_modified_time");
        ps.addBatch();

        ps.executeBatch();
        ps.close();

        ps = con
            .prepareStatement("insert into table_params(tbl_id, param_type, param_key, param_value) "
                + " values(?,?,?,?)");

        ps.setLong(1, tblID);
        ps.setString(2, "TBL");
        ps.setString(3, "last_modified_by");
        ps.setString(4, modifyUser);

        ps.addBatch();

        ps.executeBatch();
        ps.close();
      } else {
        ps = con
            .prepareStatement("update table_params set param_value=? where tbl_id=? and param_type='TBL' and param_key=?");

        ps.setString(1, modifyUser);
        ps.setLong(2, tblID);
        ps.setString(3, "last_modified_by");
        ps.addBatch();

        ps.executeBatch();
        ps.close();

        ps = con
            .prepareStatement("insert into table_params(tbl_id, param_type, param_key, param_value) "
                + " values(?,?,?,?)");
        ps.setLong(1, tblID);
        ps.setString(2, "TBL");
        ps.setString(3, "last_modified_time");
        ps.setString(4, String.valueOf(System.currentTimeMillis() / 1000));
        ps.addBatch();

        ps.executeBatch();
        ps.close();
      }

      List<FieldSchema> oldCols = new ArrayList<FieldSchema>();

      String oldName = renameColDesc.getOldName();
      String newName = renameColDesc.getNewName();
      String type = renameColDesc.getType();
      String comment = renameColDesc.getComment();

      boolean colFind = false;

      Map<String, Long> colNameMap = new HashMap<String, Long>();

      long maxColIndex = 0;

      ps = con
          .prepareStatement("select column_index, column_name, type_name from "
              + "columns where tbl_id=? order by column_index asc");

      ps.setLong(1, tblID);

      ResultSet colSet = ps.executeQuery();

      while (colSet.next()) {
        FieldSchema field = new FieldSchema();
        field.setName(colSet.getString(2));
        field.setType(colSet.getString(3));
        oldCols.add(field);

        maxColIndex = colSet.getLong(1);
        colNameMap.put(colSet.getString(2), maxColIndex);
      }

      colSet.close();
      ps.close();

      Iterator<FieldSchema> iterOldCols = oldCols.iterator();
      while (iterOldCols.hasNext()) {
        FieldSchema col = iterOldCols.next();
        String oldColName = col.getName();

        if (oldColName.equalsIgnoreCase(newName)
            && !oldColName.equalsIgnoreCase(oldName)) {
          LOG.error("rename column error, db=" + dbName + ", tbl=" + tblName
              + ", newname=" + renameColDesc.getNewName());
          throw new MetaException("column " + newName + " is exists");
        }

        if (oldColName.equalsIgnoreCase(oldName)) {
          if (!checktype(col, type)) {
            LOG.error("rename column error, db=" + dbName + ", tbl=" + tblName
                + ", newname=" + renameColDesc.getNewName());
            throw new MetaException("column type " + col.getType()
                + " can not convert to " + type);
          }

          colFind = true;
        }
      }

      if (!colFind) {
        LOG.error("rename column error, db=" + dbName + ", tbl=" + tblName
            + ", newname=" + renameColDesc.getNewName());
        throw new MetaException("can not find column " + oldName);
      }

      if (type == null) {
        if (comment == null) {
          ps = con.prepareStatement("update columns set column_name=? "
              + " where tbl_id=? and column_name=?");
          ps.setString(1, newName.toLowerCase());
          ps.setLong(2, tblID);
          ps.setString(3, oldName);

          ps.executeUpdate();
          ps.close();
        } else {
          ps = con
              .prepareStatement("update columns set column_name=?, comment=? "
                  + " where tbl_id=? and column_name=?");
          ps.setString(1, newName.toLowerCase());
          ps.setString(2, comment);
          ps.setLong(3, tblID);
          ps.setString(4, oldName);

          ps.executeUpdate();
          ps.close();
        }
      } else {
        if (comment == null) {
          ps = con
              .prepareStatement("update columns set column_name=?, type_name=? "
                  + " where tbl_id=? and column_name=?");
          ps.setString(1, newName);
          ps.setString(2, type);
          ps.setLong(3, tblID);
          ps.setString(4, oldName);

          ps.executeUpdate();
          ps.close();
        } else {
          ps = con
              .prepareStatement("update columns set column_name=?, type_name=?, comment=? "
                  + " where tbl_id=? and column_name=?");
          ps.setString(1, newName);
          ps.setString(2, type);
          ps.setString(3, comment);
          ps.setLong(4, tblID);
          ps.setString(5, oldName);

          ps.executeUpdate();
          ps.close();
        }
      }

      con.commit();
      success = true;
    } catch (SQLException ex) {
      LOG.error("rename column error, db=" + dbName + ", tbl=" + tblName
          + ", newname=" + renameColDesc.getNewName() + ", msg="
          + ex.getMessage());
      ex.printStackTrace();
      throw new MetaException(ex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }
    return;
  }

  @Override
  public void replaceCols(String dbName, String tblName, String modifyUser,
      List<FieldSchema> newCols) throws InvalidOperationException,
      MetaException {
    Connection con = null;
    PreparedStatement ps = null;
    boolean success = false;
    dbName = dbName.toLowerCase();
    tblName = tblName.toLowerCase();

    try {
      con = getSegmentConnection(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("replace column error, db=" + dbName + ", tbl=" + tblName
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("replace column error, db=" + dbName + ", tbl=" + tblName
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con
          .prepareStatement("select tbl_id, tbl_type, tbl_format, pri_part_type, serde_lib"
              + " from tbls where db_name=? and tbl_name=?");

      ps.setString(1, dbName);
      ps.setString(2, tblName);

      String tblType = null;
      String serdeLib = null;
      String tblFormat = null;
      String priPartType = null;
      boolean isPriPart = false;
      boolean isTblFind = false;
      long tblID = 0;

      ResultSet tblSet = ps.executeQuery();
      while (tblSet.next()) {
        isTblFind = true;
        tblID = tblSet.getLong(1);
        tblType = tblSet.getString(2);
        tblFormat = tblSet.getString(3);
        priPartType = tblSet.getString(4);
        serdeLib = tblSet.getString(5);

        isPriPart = priPartType != null;
      }

      tblSet.close();
      ps.close();

      if (!isTblFind) {
        throw new MetaException("can not find table " + dbName + ":" + tblName);
      }

      if (tblType.equalsIgnoreCase("VITURAL_VIEW")) {
        throw new MetaException("view can not replace column ");
      }

      if (!HiveConf.getBoolVar(hiveConf, ConfVars.ALTERSCHEMAACTIVATEREPLACE)) {
        throw new MetaException("replace columns is not supported rightnow");
      }

      if (tblType.equalsIgnoreCase("EXTERNAL_TABLE")
          && !HiveConf.getBoolVar(hiveConf,
              ConfVars.ALTERSCHEMAACTIVATEEXTTABLE)) {
        throw new MetaException("can not replace columns for a extenal table ");
      }

      if (!tblType.equalsIgnoreCase("EXTERNAL_TABLE")
          && (tblFormat != null && tblFormat.equalsIgnoreCase("text"))
          && (!HiveConf.getBoolVar(hiveConf,
              ConfVars.ALTERSCHEMAACTIVATETXTTABLE))) {
        throw new MetaException(
            "can not replace columns for a text format table ");
      }


      if (!serdeLib.equals(MetadataTypedColumnsetSerDe.class.getName())
          && !serdeLib.equals(LazySimpleSerDe.class.getName())
          && !serdeLib.equals(ColumnarSerDe.class.getName())
          && !serdeLib.equals(DynamicSerDe.class.getName())
          && !serdeLib.equals(ProtobufSerDe.class.getName())) {
        throw new MetaException(
            "Replace columns is not supported for this table. SerDe may be incompatible.");
      }

      if (serdeLib.equals("org.apache.hadoop.hive.serde.thrift.columnsetSerDe")) {
        ps = con.prepareStatement("update tbls set serde_lib=? where tbl_id=?");
        ps.setString(1, LazySimpleSerDe.class.getName());
        ps.setLong(2, tblID);
        ps.executeUpdate();
      }

      

      Map<String, String> tblParamMap = new HashMap<String, String>();
      ps = con
          .prepareStatement("select param_key, param_value from table_params where tbl_id=? and param_type='TBL'");
      ps.setLong(1, tblID);
      ResultSet paramSet = ps.executeQuery();
      while (paramSet.next()) {
        tblParamMap.put(paramSet.getString(1), paramSet.getString(2));
      }
      paramSet.close();
      ps.close();
      boolean containTime = false;
      boolean contailUser = false;
      if (tblParamMap.containsKey("last_modified_time"))
        containTime = true;
      if (tblParamMap.containsKey("last_modified_by"))
        contailUser = true;

      if (containTime && contailUser) {
        ps = con
            .prepareStatement("update table_params set param_value=? where tbl_id=? and param_type='TBL' and param_key=?");
        ps.setString(1, String.valueOf(System.currentTimeMillis() / 1000));
        ps.setLong(2, tblID);
        ps.setString(3, "last_modified_time");
        ps.addBatch();
        ps.setString(1, modifyUser);
        ps.setLong(2, tblID);
        ps.setString(3, "last_modified_by");
        ps.addBatch();

        ps.executeBatch();
        ps.close();
      } else if (!containTime && !contailUser) {
        ps = con
            .prepareStatement("insert into table_params(tbl_id, param_type, param_key, param_value) "
                + " values(?,?,?,?)");
        ps.setLong(1, tblID);
        ps.setString(2, "TBL");
        ps.setString(3, "last_modified_time");
        ps.setString(4, String.valueOf(System.currentTimeMillis() / 1000));
        ps.addBatch();

        ps.setLong(1, tblID);
        ps.setString(2, "TBL");
        ps.setString(3, "last_modified_by");
        ps.setString(4, modifyUser);

        ps.addBatch();

        ps.executeBatch();
        ps.close();
      } else if (containTime && !contailUser) {
        ps = con
            .prepareStatement("update table_params set param_value=? where tbl_id=? and param_type='TBL' and param_key=?");
        ps.setString(1, String.valueOf(System.currentTimeMillis() / 1000));
        ps.setLong(2, tblID);
        ps.setString(3, "last_modified_time");
        ps.addBatch();

        ps.executeBatch();
        ps.close();

        ps = con
            .prepareStatement("insert into table_params(tbl_id, param_type, param_key, param_value) "
                + " values(?,?,?,?)");

        ps.setLong(1, tblID);
        ps.setString(2, "TBL");
        ps.setString(3, "last_modified_by");
        ps.setString(4, modifyUser);

        ps.addBatch();

        ps.executeBatch();
        ps.close();
      } else {
        ps = con
            .prepareStatement("update table_params set param_value=? where tbl_id=? and param_type='TBL' and param_key=?");

        ps.setString(1, modifyUser);
        ps.setLong(2, tblID);
        ps.setString(3, "last_modified_by");
        ps.addBatch();

        ps.executeBatch();
        ps.close();

        ps = con
            .prepareStatement("insert into table_params(tbl_id, param_type, param_key, param_value) "
                + " values(?,?,?,?)");
        ps.setLong(1, tblID);
        ps.setString(2, "TBL");
        ps.setString(3, "last_modified_time");
        ps.setString(4, String.valueOf(System.currentTimeMillis() / 1000));
        ps.addBatch();

        ps.executeBatch();
        ps.close();
      }

      ps = con.prepareStatement("delete from columns where tbl_id=?");
      ps.setLong(1, tblID);
      ps.executeUpdate();
      ps.close();

      ps = con
          .prepareStatement("insert into columns(column_index, tbl_id, column_name "
              + ",type_name, comment) values(?,?,?,?,?)");
      long index = 0;

      for (FieldSchema field : newCols) {
        ps.setLong(1, index);
        ps.setLong(2, tblID);
        ps.setString(3, field.getName().toLowerCase());
        ps.setString(4, field.getType());
        ps.setString(5, field.getComment());

        ps.addBatch();
        index++;
      }

      ps.executeBatch();
      ps.close();

      con.commit();
      success = true;
    } catch (SQLException ex) {
      ex.printStackTrace();
      LOG.error("replace column error, db=" + dbName + ", tbl=" + tblName
          + ", msg=" + ex.getMessage());
      throw new MetaException(ex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }
    return;
  }

  @Override
  public void addTblProps(String dbName, String tblName, String modifyUser,
      Map<String, String> props) throws InvalidOperationException,
      MetaException {
    Connection con;
    PreparedStatement ps = null;
    boolean success = false;
    dbName = dbName.toLowerCase();
    tblName = tblName.toLowerCase();

    try {
      con = getSegmentConnection(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("add table props error, db=" + dbName + ", tbl=" + tblName
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("add table props error, db=" + dbName + ", tbl=" + tblName
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con.prepareStatement("select tbls.tbl_id from tbls where "
          + "tbls.db_name=? and tbls.tbl_name=?");

      ps.setString(1, dbName);
      ps.setString(2, tblName);

      boolean isTblFind = false;
      long tblID = 0;

      ResultSet tblSet = ps.executeQuery();
      while (tblSet.next()) {
        isTblFind = true;
        tblID = tblSet.getLong(1);
      }

      tblSet.close();
      ps.close();

      if (!isTblFind) {
        throw new MetaException("can not find table " + dbName + ":" + tblName);
      }

      

      ps = con
          .prepareStatement("select param_key, param_value from table_params where tbl_id=? and param_type='TBL'");
      ps.setLong(1, tblID);
      ResultSet paramSet = ps.executeQuery();
      Map<String, String> oldParamMap = new HashMap<String, String>();
      while (paramSet.next()) {
        oldParamMap.put(paramSet.getString(1), paramSet.getString(2));
      }
      paramSet.close();
      ps.close();

      Map<String, String> needUpdateMap = new HashMap<String, String>();
      Map<String, String> needAddMap = new HashMap<String, String>();

      for (Entry<String, String> entry : props.entrySet()) {
        if (oldParamMap.containsKey(entry.getKey())) {
          needUpdateMap.put(entry.getKey(), entry.getValue());
        } else {
          needAddMap.put(entry.getKey(), entry.getValue());
        }
      }

      if (oldParamMap.containsKey("last_modified_time")) {
        needUpdateMap.put("last_modified_time",
            String.valueOf(System.currentTimeMillis() / 1000));
      } else {
        needAddMap.put("last_modified_time",
            String.valueOf(System.currentTimeMillis() / 1000));
      }

      if (oldParamMap.containsKey("last_modified_by")) {
        needUpdateMap.put("last_modified_by", modifyUser);
      } else {
        needAddMap.put("last_modified_by", modifyUser);
      }

      if (!needUpdateMap.isEmpty()) {
        ps = con
            .prepareStatement("update table_params set param_value=? where "
                + " tbl_id=? and param_type='TBL' and param_key=?");
        for (Entry<String, String> entry : needUpdateMap.entrySet()) {
          ps.setString(1, entry.getValue());
          ps.setLong(2, tblID);
          ps.setString(3, entry.getKey());
          ps.addBatch();
        }
        ps.executeBatch();
        ps.close();
      }

      if (!needAddMap.isEmpty()) {
        ps = con
            .prepareStatement("insert into table_params(tbl_id, param_type, "
                + "param_key, param_value) values(?,?,?,?)");

        for (Map.Entry<String, String> entry : needAddMap.entrySet()) {
          ps.setLong(1, tblID);
          ps.setString(2, "TBL");
          ps.setString(3, entry.getKey());
          ps.setString(4, entry.getValue());
          ps.addBatch();
        }
        ps.executeBatch();
        ps.close();
      }

      con.commit();
      success = true;
    } catch (SQLException ex) {
      LOG.error("add table props error, db=" + dbName + ", tbl=" + tblName
          + ", msg=" + ex.getMessage());
      throw new MetaException(ex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    return;
  }

  @Override
  public void addSerdeProps(String dbName, String tblName, String modifyUser,
      Map<String, String> props) throws InvalidOperationException,
      MetaException {
    Connection con;
    PreparedStatement ps = null;
    Statement stmt = null;
    boolean success = false;

    dbName = dbName.toLowerCase();
    tblName = tblName.toLowerCase();

    try {
      con = getSegmentConnection(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("add serde props error, db=" + dbName + ", tbl=" + tblName
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("add serde props error, db=" + dbName + ", tbl=" + tblName
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      stmt = con.createStatement();

      String sql = "select tbls.tbl_id from tbls where " + "tbls.db_name='"
          + dbName + "' and tbls.tbl_name='" + tblName + "'";

      boolean isTblFind = false;
      long tblID = 0;

      ResultSet tblSet = stmt.executeQuery(sql);
      while (tblSet.next()) {
        isTblFind = true;
        tblID = tblSet.getLong(1);
      }

      tblSet.close();

      if (!isTblFind) {
        throw new MetaException("can not find table " + dbName + ":" + tblName);
      }

      

      Map<String, String> tblParamMap = new HashMap<String, String>();
      Map<String, String> serdeParamMap = new HashMap<String, String>();
      sql = "select param_type, param_key, param_value from table_params where tbl_id="
          + tblID + " and (param_type='SERDE' or param_type='TBL')";
      ResultSet paramSet = stmt.executeQuery(sql);
      String type = null;
      while (paramSet.next()) {
        type = paramSet.getString(1);
        if (type.equalsIgnoreCase("TBL")) {
          tblParamMap.put(paramSet.getString(2), paramSet.getString(3));
        } else {
          serdeParamMap.put(paramSet.getString(2), paramSet.getString(3));
        }
      }
      paramSet.close();

      boolean containTime = false;
      boolean contailUser = false;
      if (tblParamMap.containsKey("last_modified_time"))
        containTime = true;
      if (tblParamMap.containsKey("last_modified_by"))
        contailUser = true;

      Map<String, String> needUpdateMap = new HashMap<String, String>();
      Map<String, String> needAddMap = new HashMap<String, String>();

      for (Entry<String, String> entry : props.entrySet()) {
        if (serdeParamMap.containsKey(entry.getKey())) {
          needUpdateMap.put(entry.getKey(), entry.getValue());
        } else {
          needAddMap.put(entry.getKey(), entry.getValue());
        }
      }

      if (!needUpdateMap.isEmpty() || containTime || contailUser) {
        ps = con
            .prepareStatement("update table_params set param_value=? where "
                + " tbl_id=? and param_type=? and param_key=?");
        for (Entry<String, String> entry : needUpdateMap.entrySet()) {
          ps.setString(1, entry.getValue());
          ps.setLong(2, tblID);
          ps.setString(3, "SERDE");
          ps.setString(4, entry.getKey());
          ps.addBatch();
        }

        if (containTime) {
          ps.setString(1, String.valueOf(System.currentTimeMillis() / 1000));
          ps.setLong(2, tblID);
          ps.setString(3, "TBL");
          ps.setString(4, "last_modified_time");
          ps.addBatch();
        }

        if (contailUser) {
          ps.setString(1, modifyUser);
          ps.setLong(2, tblID);
          ps.setString(3, "TBL");
          ps.setString(4, "last_modified_by");
          ps.addBatch();
        }

        ps.executeBatch();
        ps.close();
      }

      if (!needAddMap.isEmpty() || !containTime || !contailUser) {
        ps = con
            .prepareStatement("insert into table_params(tbl_id, param_type, "
                + "param_key, param_value) values(?,?,?,?)");

        for (Map.Entry<String, String> entry : needAddMap.entrySet()) {
          ps.setLong(1, tblID);
          ps.setString(2, "SERDE");
          ps.setString(3, entry.getKey());
          ps.setString(4, entry.getValue());
          ps.addBatch();
        }

        if (!containTime) {
          ps.setLong(1, tblID);
          ps.setString(2, "TBL");
          ps.setString(3, "last_modified_time");
          ps.setString(4, String.valueOf(System.currentTimeMillis() / 1000));
          ps.addBatch();
        }

        if (!contailUser) {
          ps.setLong(1, tblID);
          ps.setString(2, "TBL");
          ps.setString(3, "last_modified_by");
          ps.setString(4, modifyUser);
          ps.addBatch();
        }

        ps.executeBatch();
        ps.close();
      }

      con.commit();
      success = true;
    } catch (SQLException ex) {
      ex.printStackTrace();
      LOG.error("add serde props error, db=" + dbName + ", tbl=" + tblName
          + ", msg=" + ex.getMessage());
      throw new MetaException(ex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeStatement(stmt);
      closeConnection(con);
    }

    return;
  }

  public static String getDDLFromFieldSchema(String structName,
      List<FieldSchema> fieldSchemas) {
    StringBuilder ddl = new StringBuilder();
    ddl.append("struct ");
    ddl.append(structName);
    ddl.append(" { ");

    boolean first = true;
    for (FieldSchema col : fieldSchemas) {
      if (first) {
        first = false;
      } else {
        ddl.append(", ");
      }

      ddl.append(MetaStoreUtils.typeToThriftType(col.getType()));
      ddl.append(' ');
      ddl.append(col.getName());
    }

    ddl.append("}");

    LOG.debug("DDL: " + ddl);
    return ddl.toString();
  }

  @Override
  public void addSerde(String dbName, String tblName, AddSerdeDesc addSerdeDesc)
      throws InvalidOperationException, MetaException {
    Connection con;
    PreparedStatement ps = null;
    boolean success = false;

    dbName = dbName.toLowerCase();
    tblName = tblName.toLowerCase();

    try {
      con = getSegmentConnection(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("add serde error, db=" + dbName + ", tbl=" + tblName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("add serde error, db=" + dbName + ", tbl=" + tblName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con
          .prepareStatement("select tbl_id, is_compressed, input_format, output_format, serde_lib, tbl_location "
              + " from tbls where db_name=? and tbl_name=?");

      ps.setString(1, dbName);
      ps.setString(2, tblName);

      boolean isTblFind = false;
      long tblID = 0;
      String serdeLib = null;
      String inputFormat = null;
      String location = null;
      String outputFormat = null;
      boolean isCompressed = false;
      Properties schema = new Properties();

      ResultSet tblSet = ps.executeQuery();
      while (tblSet.next()) {
        isTblFind = true;
        tblID = tblSet.getLong(1);
        isCompressed = tblSet.getBoolean(2);
        inputFormat = tblSet.getString(3);
        outputFormat = tblSet.getString(4);
        location = tblSet.getString(6);
        break;
      }

      serdeLib = addSerdeDesc.getSerdeName();

      tblSet.close();
      ps.close();

      if (!isTblFind) {
        throw new MetaException("can not find table " + dbName + ":" + tblName);
      }

      if (inputFormat == null || inputFormat.length() == 0) {
        inputFormat = org.apache.hadoop.mapred.SequenceFileInputFormat.class
            .getName();
      }
      schema.setProperty(
          org.apache.hadoop.hive.metastore.api.Constants.FILE_INPUT_FORMAT,
          inputFormat);

      if (outputFormat == null || outputFormat.length() == 0) {
        outputFormat = org.apache.hadoop.mapred.SequenceFileOutputFormat.class
            .getName();
      }
      schema.setProperty(
          org.apache.hadoop.hive.metastore.api.Constants.FILE_OUTPUT_FORMAT,
          outputFormat);

      schema.setProperty(
          org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_NAME,
          tblName);

      if (location != null) {
        schema.setProperty(
            org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_LOCATION,
            location);
      }

      schema.setProperty(
          org.apache.hadoop.hive.metastore.api.Constants.BUCKET_COUNT, "0");

      if (isCompressed) {
        schema.setProperty(
            org.apache.hadoop.hive.metastore.api.Constants.COMPRESS, "true");
      } else {
      }

      if (serdeLib == null) {
        throw new MetaException("serde lib for the table " + dbName + ":"
            + tblName + " is null");
      }

      if (serdeLib != null) {
        schema.setProperty(
            org.apache.hadoop.hive.serde.Constants.SERIALIZATION_LIB, serdeLib);
      }

      

      String modifyUser = addSerdeDesc.getUser();
      Map<String, String> tblParamMap = new HashMap<String, String>();
      ps = con
          .prepareStatement("select param_key, param_value from table_params where tbl_id=? and param_type='TBL'");
      ps.setLong(1, tblID);
      ResultSet paramSet = ps.executeQuery();
      while (paramSet.next()) {
        tblParamMap.put(paramSet.getString(1), paramSet.getString(2));
      }
      paramSet.close();
      ps.close();
      boolean containTime = false;
      boolean contailUser = false;
      if (tblParamMap.containsKey("last_modified_time"))
        containTime = true;
      if (tblParamMap.containsKey("last_modified_by"))
        contailUser = true;

      if (containTime && contailUser) {
        ps = con
            .prepareStatement("update table_params set param_value=? where tbl_id=? and param_type='TBL' and param_key=?");
        ps.setString(1, String.valueOf(System.currentTimeMillis() / 1000));
        ps.setLong(2, tblID);
        ps.setString(3, "last_modified_time");
        ps.addBatch();
        ps.setString(1, modifyUser);
        ps.setLong(2, tblID);
        ps.setString(3, "last_modified_by");
        ps.addBatch();

        ps.executeBatch();
        ps.close();
      } else if (!containTime && !contailUser) {
        ps = con
            .prepareStatement("insert into table_params(tbl_id, param_type, param_key, param_value) "
                + " values(?,?,?,?)");
        ps.setLong(1, tblID);
        ps.setString(2, "TBL");
        ps.setString(3, "last_modified_time");
        ps.setString(4, String.valueOf(System.currentTimeMillis() / 1000));
        ps.addBatch();

        ps.setLong(1, tblID);
        ps.setString(2, "TBL");
        ps.setString(3, "last_modified_by");
        ps.setString(4, modifyUser);

        ps.addBatch();

        ps.executeBatch();
        ps.close();
      } else if (containTime && !contailUser) {
        ps = con
            .prepareStatement("update table_params set param_value=? where tbl_id=? and param_type='TBL' and param_key=?");
        ps.setString(1, String.valueOf(System.currentTimeMillis() / 1000));
        ps.setLong(2, tblID);
        ps.setString(3, "last_modified_time");
        ps.addBatch();

        ps.executeBatch();
        ps.close();

        ps = con
            .prepareStatement("insert into table_params(tbl_id, param_type, param_key, param_value) "
                + " values(?,?,?,?)");

        ps.setLong(1, tblID);
        ps.setString(2, "TBL");
        ps.setString(3, "last_modified_by");
        ps.setString(4, modifyUser);

        ps.addBatch();

        ps.executeBatch();
        ps.close();
      } else {
        ps = con
            .prepareStatement("update table_params set param_value=? where tbl_id=? and param_type='TBL' and param_key=?");

        ps.setString(1, modifyUser);
        ps.setLong(2, tblID);
        ps.setString(3, "last_modified_by");
        ps.addBatch();

        ps.executeBatch();
        ps.close();

        ps = con
            .prepareStatement("insert into table_params(tbl_id, param_type, param_key, param_value) "
                + " values(?,?,?,?)");
        ps.setLong(1, tblID);
        ps.setString(2, "TBL");
        ps.setString(3, "last_modified_time");
        ps.setString(4, String.valueOf(System.currentTimeMillis() / 1000));
        ps.addBatch();

        ps.executeBatch();
        ps.close();
      }

      ps = con
          .prepareStatement("select param_key, param_value from table_params where tbl_id=? and (param_type='SERDE' or param_type='TBL')");
      ps.setLong(1, tblID);

      ResultSet sdParamSet = ps.executeQuery();
      while (sdParamSet.next()) {
        schema.setProperty(sdParamSet.getString(1), sdParamSet.getString(2));
      }
      sdParamSet.close();
      ps.close();

      ps = con
          .prepareStatement("select column_name, type_name, comment from columns where tbl_id=? order by column_index asc");
      ps.setLong(1, tblID);
      StringBuilder colNameBuf = new StringBuilder();
      StringBuilder colTypeBuf = new StringBuilder();
      List<FieldSchema> colList = new ArrayList<FieldSchema>();

      ResultSet colSet = ps.executeQuery();
      boolean first = true;
      while (colSet.next()) {
        String name = colSet.getString(1);
        String type = colSet.getString(2);
        String comment = colSet.getString(3);

        FieldSchema field = new FieldSchema();
        field.setName(name);
        field.setType(type);
        field.setComment(comment);
        colList.add(field);

        if (!first) {
          colNameBuf.append(",");
          colTypeBuf.append(":");
        }
        colNameBuf.append(colSet.getString(1));
        colTypeBuf.append(colSet.getString(2));

        first = false;
      }

      colSet.close();
      ps.close();

      String colNames = colNameBuf.toString();
      String colTypes = colTypeBuf.toString();
      schema.setProperty(
          org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_COLUMNS,
          colNames);
      schema
          .setProperty(
              org.apache.hadoop.hive.metastore.api.Constants.META_TABLE_COLUMN_TYPES,
              colTypes);

      schema.setProperty(
          org.apache.hadoop.hive.serde.Constants.SERIALIZATION_DDL,
          getDDLFromFieldSchema(tblName, colList));

      Deserializer deserializer = SerDeUtils.lookupDeserializer(serdeLib);
      deserializer.initialize(hiveConf, schema);


      List<FieldSchema> newColList = null;

      try {
        newColList = MetaStoreUtils.getFieldsFromDeserializer(tblName,
            deserializer);
      } catch (SerDeException e) {
        throw new MetaException("Error in getting fields from serde. "
            + e.getMessage());
      } catch (MetaException e) {
        throw new MetaException("Error in getting fields from serde."
            + e.getMessage());
      }


      ps = con.prepareStatement("delete from columns where tbl_id=?");
      ps.setLong(1, tblID);
      ps.executeUpdate();
      ps.close();

      ps = con
          .prepareStatement("insert into columns(column_index, tbl_id, column_name "
              + ",type_name, comment) values(?,?,?,?,?)");

      long index = 0;

      for (FieldSchema field : newColList) {
        ps.setLong(1, index);
        ps.setLong(2, tblID);
        ps.setString(3, field.getName());
        ps.setString(4, field.getType());
        ps.setString(5, field.getComment());
        ps.addBatch();
        index++;
      }
      ps.executeBatch();
      ps.close();

      if ((addSerdeDesc.getProps() != null)
          && (addSerdeDesc.getProps().size() > 0)) {
        ps = con
            .prepareStatement("select param_key, param_value from table_params where tbl_id=? and "
                + "param_type='SERDE'");
        ps.setLong(1, tblID);
        ResultSet oldParamSet = ps.executeQuery();

        Map<String, String> needUpdateMap = new HashMap<String, String>();
        Map<String, String> needAddMap = new HashMap<String, String>();
        Map<String, String> oldParamMap = new HashMap<String, String>();

        while (oldParamSet.next()) {
          oldParamMap.put(oldParamSet.getString(1), oldParamSet.getString(2));
        }
        oldParamSet.close();
        ps.close();

        for (Map.Entry<String, String> entry : addSerdeDesc.getProps()
            .entrySet()) {
          if (oldParamMap.containsKey(entry.getKey())) {
            needUpdateMap.put(entry.getKey(), entry.getValue());
          } else {
            needAddMap.put(entry.getKey(), entry.getValue());
          }
        }

        if (!needAddMap.isEmpty()) {
          ps = con
              .prepareStatement("insert into table_params(tbl_id, param_key, param_value, param_type) values(?,?,?,?)");

          for (Map.Entry<String, String> entry : needAddMap.entrySet()) {
            ps.setLong(1, tblID);
            ps.setString(2, entry.getKey());
            ps.setString(3, entry.getValue());
            ps.setString(4, "SERDE");
            ps.addBatch();
          }
          ps.executeBatch();
          ps.close();
        }

        if (!needUpdateMap.isEmpty()) {
          ps = con
              .prepareStatement("update table_params set param_value=? where tbl_id=? and param_type='SERDE' and param_key=?");

          for (Map.Entry<String, String> entry : needUpdateMap.entrySet()) {
            ps.setString(1, entry.getValue());
            ps.setLong(2, tblID);
            ps.setString(3, entry.getKey());
            ps.addBatch();
          }
          ps.executeBatch();
          ps.close();
        }
      }

      ps = con.prepareStatement("update tbls set serde_lib=? where tbl_id=?");
      ps.setString(1, addSerdeDesc.getSerdeName());
      ps.setLong(2, tblID);
      ps.executeUpdate();
      ps.close();

      con.commit();
      success = true;
    } catch (Exception ex) {
      LOG.error("add serde error, db=" + dbName + ", tbl=" + tblName + ", msg="
          + ex.getMessage());
      ex.printStackTrace();
      throw new MetaException(ex.getMessage());
    }

    finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    return;
  }

  @Override
  public void alterTable(String dbname, String name, Table newTable)
      throws InvalidObjectException, MetaException {

  }

  @Override
  public List<String> getTables(String dbName, String pattern)
      throws MetaException {

    Connection con;
    Statement ps = null;
    List<String> tableList = new ArrayList<String>();

    dbName = dbName.toLowerCase();
    pattern = pattern.toLowerCase();

    try {
      con = getSegmentConnection(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("get table error, db=" + dbName + ", pattern=" + pattern
          + ", msg=" + e1.getMessage());

      return tableList;
    } catch (SQLException e1) {
      LOG.error("get table error, db=" + dbName + ", pattern=" + pattern
          + ", msg=" + e1.getMessage());

      return tableList;
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();
      String sql = null;

      if (pattern == null || pattern.isEmpty() || pattern.equals(".*")
          || pattern.equals("*")) {
        sql = "select tbl_name from tbls where db_name='"
            + dbName.toLowerCase() + "'";
      } else {
        pattern = pattern.replace('*', '%');

        sql = "select tbl_name from tbls where db_name='"
            + dbName.toLowerCase() + "'" + " and tbl_name like '" + pattern
            + "'";
      }

      ResultSet tblSet = ps.executeQuery(sql);

      while (tblSet.next()) {
        String item = tblSet.getString(1);
        tableList.add(item);
      }
    } catch (SQLException sqlex) {
      LOG.error("get table error, db=" + dbName + ", pattern=" + pattern
          + ", msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    return tableList;
  }

  @Override
  public boolean add_table_statistics(
      tdw_sys_table_statistics new_table_statistics) throws MetaException {
    return false;
  }

  @Override
  public boolean delete_table_statistics(String table_statistics_name,
      String db_statistics_name) throws MetaException {
    return false;
  }

  @Override
  public tdw_sys_table_statistics get_table_statistics(
      String table_statistics_name, String db_statistics_name)
      throws MetaException {
    return null;
  }

  @Override
  public List<tdw_sys_table_statistics> get_table_statistics_multi(
      String db_statistics_name, int max) throws MetaException {
    return null;
  }

  @Override
  public List<String> get_table_statistics_names(String db_statistics_name,
      int max) throws MetaException {
    return null;
  }

  @Override
  public boolean add_fields_statistics(
      tdw_sys_fields_statistics new_fields_statistics) throws MetaException {
    return false;
  }

  @Override
  public boolean delete_fields_statistics(String table_statistics_name,
      String db_statistics_name, String fields_statistics_name)
      throws MetaException {
    return false;
  }

  @Override
  public tdw_sys_fields_statistics get_fields_statistics(
      String table_statistics_name, String table_db_name,
      String fields_statistics_name) throws MetaException {
    return null;
  }

  @Override
  public List<tdw_sys_fields_statistics> get_fields_statistics_multi(
      String table_statistics_name, String db_statistics_name, int max)
      throws MetaException {
    return null;
  }

  @Override
  public List<String> get_fields_statistics_names(String table_statistics_name,
      String db_statistics_name, int max) throws MetaException {
    return null;
  }

  @Override
  public boolean add_tdw_query_info(tdw_query_info queryInfo)
      throws MetaException {
    return false;
  }

  @Override
  public boolean add_tdw_query_stat(tdw_query_stat query_stat)
      throws MetaException {
    return false;
  }

  @Override
  public boolean update_tdw_query_info(String qid, String finishtime,
      String state) throws MetaException {
    return false;
  }

  @Override
  public boolean update_tdw_query_stat(String qid, String finishtime,
      String state) throws MetaException {
    return false;
  }

  @Override
  public List<tdw_query_info> get_tdw_query_info() throws MetaException {
    return null;
  }

  @Override
  public List<tdw_query_stat> get_tdw_query_stat() throws MetaException {
    return null;
  }

  @Override
  public boolean clear_tdw_query_info(int days) throws MetaException {
    return false;
  }

  @Override
  public boolean clear_tdw_query_stat(int days) throws MetaException {
    return false;
  }

  @Override
  public tdw_query_info search_tdw_query_info(String qid) throws MetaException {
    return null;
  }

  @Override
  public boolean add_user_group(group newgroup, String user)
      throws MetaException {
    return false;
  }

  @Override
  public int drop_user_group(String groupname, String user)
      throws MetaException {
    return 0;
  }

  @Override
  public String get_groupname(String username) throws MetaException {
    return null;
  }

  @Override
  public int revoke_user_group(String groupname, String namelist, String user)
      throws MetaException {
    return 0;
  }

  @Override
  public int grant_user_group(String groupname, String namelist, String user)
      throws MetaException {
    return 0;
  }

  @Override
  public List<group> get_groups(String pattern) throws MetaException {
    return null;
  }

  @Override
  public MGroup findGroup(String gname) throws MetaException {
    Connection con = null;
    ;
    Statement ps = null;
    boolean success = false;
    MGroup group = null;

    gname = gname.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("create user error, user=" + user + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("create user error, user=" + user + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();
      String sql = "select creator from usergroup where group_name='" + gname
          + "'";

      ResultSet groupSet = ps.executeQuery(sql);
      while (groupSet.next()) {
        group = new MGroup();
        group.setCreator(groupSet.getString(1));
        group.setGroupName(gname);
      }

      groupSet.close();

      if (group != null) {
        sql = "select string_agg(tdwuser.user_name, ',') namelist from tdwuser where group_name='"
            + gname + "'";
        ResultSet userSet = ps.executeQuery(sql);
        while (userSet.next()) {
          group.setUSER_LIST(userSet.getString(1));
        }
        userSet.close();
      }
      con.commit();
      success = true;
    } catch (SQLException sqlex) {
      LOG.error("create user error, user=" + user + ", msg="
          + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    return group;
  }

  @Override
  public boolean createUser(String user, String passwd)
      throws AlreadyExistsException, MetaException {
    Connection con = null;
    ;
    PreparedStatement ps = null;
    boolean success = false;

    user = user.toLowerCase();
    passwd = passwd.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("create user error, user=" + user + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("create user error, user=" + user + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con
          .prepareStatement("select group_name from usergroup where group_name=?");
      ps.setString(1, user);
      boolean isGroupFind = false;

      ResultSet gSet = ps.executeQuery();

      while (gSet.next()) {
        isGroupFind = true;
        break;
      }

      gSet.close();

      if (isGroupFind) {
        LOG.error("group name " + user + " already exist");
        throw new MetaException(
            "the user name is already used by an usergroup!");
      }

      ps = con
          .prepareStatement("select role_name from tdwrole where role_name=?");
      ps.setString(1, user);
      boolean isRoleFind = false;

      ResultSet rSet = ps.executeQuery();

      while (rSet.next()) {
        isRoleFind = true;
        break;
      }

      rSet.close();

      if (isRoleFind) {
        LOG.error("role name " + user + " already exist");
        throw new MetaException(
            "Fail to create the new user! There is a role with the same name!");
      }

      ps = con
          .prepareStatement("insert into tdwuser(user_name, passwd) values(?,?)");
      ps.setString(1, user);
      ps.setString(2, passwd);
      ps.executeUpdate();

      con.commit();
      success = true;
    } catch (SQLException sqlex) {
      LOG.error("create user error, user=" + user + ", msg="
          + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new AlreadyExistsException(sqlex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    return success;
  }

  @Override
  public boolean dropUser(String userName) throws MetaException,
      NoSuchObjectException {
    boolean useDistributeTran = false;
    if (!useDistributeTran) {
      return dropUserNoDistributeTransaction(userName);
    } else {
      return dropUserByDistributeTransaction(userName);
    }
  }

  public boolean dropUserNoDistributeTransaction(String userName)
      throws NoSuchObjectException, MetaException {
    Connection con = null;
    ;
    Statement ps = null;
    boolean success = false;
    userName = userName.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("drop user error, user=" + userName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("drop user error, user=" + userName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      ps = con.createStatement();

      String sql = "select user_name from tdwuser where user_name='" + userName
          + "'";

      boolean isUserFind = false;
      ResultSet userSet = ps.executeQuery(sql);
      while (userSet.next()) {
        isUserFind = true;
        break;
      }

      userSet.close();

      if (!isUserFind) {
        throw new NoSuchObjectException("can not find user:" + userName);
      }

      sql = "delete from tdwuser where user_name='" + userName + "'";
      ps.executeUpdate(sql);

      sql = "delete from dbpriv where user_name='" + userName + "'";
      ps.executeUpdate(sql);

      sql = "delete from tblpriv where user_name='" + userName + "'";
      ps.executeUpdate(sql);

      con.commit();
      success = true;
    } catch (SQLException sqlex) {
      LOG.error("drop user error, user=" + userName + ", msg="
          + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    

    return success;
  }

  public boolean dropUserByDistributeTransaction(String userName)
      throws NoSuchObjectException, MetaException {

    boolean success = false;

    Connection masterConn = null;

    PGXADataSource masterDS = null;

    masterDS = getXADataSource(globalDbUrl, user, passwd);

    Set<String> slaveURLSet = getAllSegments();
    int size = slaveURLSet.size();

    PGXADataSource[] slaveDSArray = new PGXADataSource[size];
    XAConnection[] slaveDSXaConnArray = new XAConnection[size];
    XAResource[] slaveSaResArray = new XAResource[size];
    Connection[] slaveConArray = new Connection[size];
    Statement[] slaveStmtArray = new Statement[size];
    Xid[] slaveXidArray = new Xid[size];

    int index = 0;
    for (String slaveURL : slaveURLSet) {
      slaveDSArray[index] = getXADataSource(slaveURL, user, passwd);
      index++;
    }

    XAConnection masterDSXaConn = null;

    int formatID = genFormatID();

    try {
      masterDSXaConn = masterDS.getXAConnection();

      for (int i = 0; i < size; i++) {
        slaveDSXaConnArray[i] = slaveDSArray[i].getXAConnection();
        slaveSaResArray[i] = slaveDSXaConnArray[i].getXAResource();
        slaveConArray[i] = slaveDSXaConnArray[i].getConnection();
        slaveStmtArray[i] = slaveConArray[i].createStatement();

        byte id1 = (byte) ((i + 2) * 2);
        byte id2 = (byte) (id1 + 1);

        slaveXidArray[i] = new MyXid(formatID, new byte[] { id1 },
            new byte[] { id2 });
      }

      XAResource masterSaRes = masterDSXaConn.getXAResource();
      masterConn = masterDSXaConn.getConnection();
      Statement masterStmt = masterConn.createStatement();
      Xid masterXid = new MyXid(formatID, new byte[] { 0x01 },
          new byte[] { 0x02 });

      try {
        masterSaRes.start(masterXid, XAResource.TMNOFLAGS);
        masterStmt.executeUpdate("delete from tdwuser where user_name='"
            + userName.toLowerCase() + "'");
        masterSaRes.end(masterXid, XAResource.TMSUCCESS);

        for (int i = 0; i < size; i++) {
          slaveSaResArray[i].start(slaveXidArray[i], XAResource.TMNOFLAGS);
          slaveStmtArray[i]
              .executeUpdate("delete from dbpriv where user_name='"
                  + userName.toLowerCase() + "'");
          slaveStmtArray[i]
              .executeUpdate("delete from tblpriv where user_name='"
                  + userName.toLowerCase() + "'");
          slaveSaResArray[i].end(slaveXidArray[i], XAResource.TMSUCCESS);
        }

        boolean isAllPred = true;
        int masterRet = masterSaRes.prepare(masterXid);

        if (masterRet == XAResource.XA_OK) {
          int[] slaveRetArray = new int[size];
          for (int i = 0; i < size; i++) {
            slaveRetArray[i] = slaveSaResArray[i].prepare(slaveXidArray[i]);

            if (slaveRetArray[i] == XAResource.XA_OK) {
              continue;
            } else {
              isAllPred = false;
              break;
            }
          }

          if (isAllPred) {
            masterSaRes.commit(masterXid, false);
            for (int i = 0; i < size; i++) {
              slaveSaResArray[i].commit(slaveXidArray[i], false);
            }

            success = true;
          }
        }
      } catch (XAException e) {
        LOG.error("drop user error, user=" + userName + ", msg="
            + e.getMessage());
        throw new MetaException(e.getMessage());
      }
    } catch (SQLException e) {
      LOG.error("drop user error, user=" + userName + ", msg=" + e.getMessage());
      throw new MetaException(e.getMessage());
    } finally {
      closeConnection(masterConn);
      closeXAConnection(masterDSXaConn);

      for (int i = 0; i < size; i++) {
        closeConnection(slaveConArray[i]);
        closeXAConnection(slaveDSXaConnArray[i]);
      }
    }

    return success;
  }

  @Override
  public User getUser(String userName) throws NoSuchObjectException,
      MetaException {
    Connection con = null;
    ;
    Statement ps = null;

    User user = null;

    userName = userName.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("get user error, user=" + user + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get user error, user=" + user + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = "select alter_priv, create_priv, createview_priv, dba_priv, "
          + "delete_priv, drop_priv, index_priv, insert_priv, select_priv, showview_priv, "
          + "update_priv, user_name, group_name from tdwuser where tdwuser.user_name='"
          + userName + "'";

      ResultSet userSet = ps.executeQuery(sql);
      boolean isUserFind = false;

      while (userSet.next()) {
        isUserFind = true;
        user = new User();
        user.setAlterPriv(userSet.getBoolean(1));
        user.setCreatePriv(userSet.getBoolean(2));
        user.setCreateviewPriv(userSet.getBoolean(3));
        user.setDbaPriv(userSet.getBoolean(4));
        user.setDeletePriv(userSet.getBoolean(5));
        user.setDropPriv(userSet.getBoolean(6));
        user.setIndexPriv(userSet.getBoolean(7));
        user.setInsertPriv(userSet.getBoolean(8));
        user.setSelectPriv(userSet.getBoolean(9));
        user.setShowviewPriv(userSet.getBoolean(10));
        user.setUpdatePriv(userSet.getBoolean(11));
        user.setUserName(userSet.getString(12));
        user.setGroupName(userSet.getString(13));
      }

      userSet.close();

      if (!isUserFind) {
        throw new NoSuchObjectException("can not find user:" + userName);
      }

      sql = "select role_name from tdwuserrole where user_name='" + userName
          + "'";
      ResultSet roleSet = ps.executeQuery(sql);

      List<String> roleList = new ArrayList<String>();
      while (roleSet.next()) {
        roleList.add(roleSet.getString(1));
      }

      roleSet.close();

      user.setPlayRoles(roleList);
    } catch (SQLException sqlex) {
      LOG.error("get user error, user=" + user + ", msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    return user;
  }

  @Override
  public List<String> getUsersAll() throws MetaException {
    Connection con = null;
    ;
    Statement ps = null;
    boolean success = false;

    List<String> users = new ArrayList<String>();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("get user all error" + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get user all error" + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = "select user_name, group_name from tdwuser";
      ResultSet userSet = ps.executeQuery(sql);

      while (userSet.next()) {
        users.add(userSet.getString(1) + "	" + userSet.getString(2));
      }
      userSet.close();
      success = true;
    } catch (SQLException sqlex) {
      LOG.error("get user all error" + ", msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    if (success == true)
      return users;
    else
      return null;
  }

  @Override
  public boolean setPasswd(String userName, String newPasswd)
      throws NoSuchObjectException, MetaException {
    Connection con = null;
    ;
    PreparedStatement ps = null;
    boolean success = false;
    userName = userName.toLowerCase();
    newPasswd = newPasswd.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("set passwd error, user=" + userName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("set passwd error, user=" + userName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con
          .prepareStatement("select user_name from tdwuser where user_name=?");
      ps.setString(1, userName);

      boolean isUserFind = false;
      ResultSet userSet = ps.executeQuery();
      while (userSet.next()) {
        isUserFind = true;
        break;
      }

      userSet.close();
      ps.close();

      if (isUserFind == false) {
        throw new NoSuchObjectException("can not find user:" + userName);
      }

      ps = con
          .prepareStatement("update tdwuser set passwd=? where user_name=?");
      ps.setString(1, newPasswd);
      ps.setString(2, userName);

      ps.executeUpdate();

      con.commit();
      success = true;
    } catch (SQLException sqlex) {
      LOG.error("set passwd error, user=" + userName + ", msg="
          + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    return success;
  }

  public boolean isAUser(String userName) throws MetaException {
    Connection con = null;
    ;
    Statement ps = null;
    boolean success = false;
    userName = userName.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("check user exist error, user=" + userName + ", passwd="
          + passwd + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("check user exist error, user=" + userName + ", passwd="
          + passwd + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = "select user_name from tdwuser where user_name='" + userName
          + "'";

      boolean isUserFind = false;

      ResultSet userSet = ps.executeQuery(sql);

      while (userSet.next()) {
        isUserFind = true;
        break;
      }
      userSet.close();

      if (!isUserFind) {
        throw new NoSuchObjectException("can not find user:" + userName);
      }

      success = true;
    } catch (Exception ex) {
      LOG.error("check user exist error, user=" + userName + ", passwd="
          + passwd + ", msg=" + ex.getMessage());
      ex.printStackTrace();
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    return success;
  }

  @Override
  public boolean isAUser(String userName, String passwd) throws MetaException {
    Connection con = null;
    ;
    Statement ps = null;
    boolean success = false;

    userName = userName.toLowerCase();
    passwd = passwd.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("audit error, user=" + userName + ", passwd=" + passwd
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("audit error, user=" + userName + ", passwd=" + passwd
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = "select user_name, passwd from tdwuser where user_name='"
          + userName + "'";

      String actualPass = null;
      boolean isUserFind = false;

      ResultSet userSet = ps.executeQuery(sql);

      while (userSet.next()) {
        isUserFind = true;
        actualPass = userSet.getString(2);
        break;
      }

      userSet.close();

      if (!isUserFind) {
        throw new NoSuchObjectException("can not find user:" + userName);
      }

      if (actualPass == null || !actualPass.equals(passwd)) {
        throw new MetaException("audit failed, password error!");
      }

      success = true;
    } catch (Exception ex) {
      LOG.error("audit error, user=" + userName + ", passwd=" + passwd
          + ", msg=" + ex.getMessage());
      ex.printStackTrace();
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    return success;
  }

  @Override
  public boolean grantAuthSys(String userName, List<String> privileges)
      throws NoSuchObjectException, InvalidObjectException, MetaException {
    Connection con = null;
    ;
    PreparedStatement ps = null;
    boolean success = false;

    userName = userName.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("grant auth sys error , user=" + userName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("grant auth sys error , user=" + userName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con
          .prepareStatement("select alter_priv,create_priv, createview_priv, dba_priv "
              + ",delete_priv, drop_priv, index_priv, insert_priv, select_priv, showview_priv"
              + ",update_priv from tdwuser where user_name=?");
      ps.setString(1, userName);

      boolean isPrivFind = false;

      boolean selPriv = false;
      boolean insertPriv = false;
      boolean createPriv = false;
      boolean dropPriv = false;
      boolean deletePriv = false;
      boolean alterPriv = false;
      boolean updatePriv = false;
      boolean indexPriv = false;
      boolean showViewPriv = false;
      boolean createViewPriv = false;
      boolean dbaPriv = false;

      ResultSet userSet = ps.executeQuery();

      while (userSet.next()) {
        isPrivFind = true;
        alterPriv = userSet.getBoolean(1);
        createPriv = userSet.getBoolean(2);
        createViewPriv = userSet.getBoolean(3);
        dbaPriv = userSet.getBoolean(4);
        deletePriv = userSet.getBoolean(5);
        dropPriv = userSet.getBoolean(6);
        indexPriv = userSet.getBoolean(7);
        insertPriv = userSet.getBoolean(8);
        selPriv = userSet.getBoolean(9);
        showViewPriv = userSet.getBoolean(10);
        updatePriv = userSet.getBoolean(11);
        break;
      }

      userSet.close();
      ps.close();

      if (!isPrivFind) {
        throw new NoSuchObjectException("can not find user:" + userName);
      }

      for (String priv : privileges) {
        if (priv.equals("TOK_SELECT_PRI")) {
          selPriv = true;
        } else if (priv.equals("TOK_INSERT_PRI")) {
          insertPriv = true;
        } else if (priv.equals("TOK_CREATE_PRI")) {
          createPriv = true;
        } else if (priv.equals("TOK_DROP_PRI")) {
          dropPriv = true;
        } else if (priv.equals("TOK_DELETE_PRI")) {
          deletePriv = true;
        } else if (priv.equals("TOK_ALTER_PRI")) {
          alterPriv = true;
        } else if (priv.equals("TOK_UPDATE_PRI")) {
          updatePriv = true;
        } else if (priv.equals("TOK_INDEX_PRI")) {
          indexPriv = true;
        } else if (priv.equals("TOK_CREATEVIEW_PRI")) {
          createViewPriv = true;
        } else if (priv.equals("TOK_SHOWVIEW_PRI")) {
          showViewPriv = true;
        } else if (priv.equals("TOK_DBA_PRI")) {
          dbaPriv = true;
        } else if (priv.equals("TOK_ALL_PRI")) {
          selPriv = true;
          insertPriv = true;
          createPriv = true;
          dropPriv = true;
          deletePriv = true;
          alterPriv = true;
          updatePriv = true;
          indexPriv = true;
          createViewPriv = true;
          showViewPriv = true;
        } else {
          throw new InvalidObjectException("Privilege does not exist: " + priv);
        }
      }

      ps = con
          .prepareStatement("update tdwuser set alter_priv=?, create_priv=?, createview_priv=?, dba_priv=?,"
              + " delete_priv=?, drop_priv=?, index_priv=?, insert_priv=?, select_priv=?, showview_priv=?,"
              + " update_priv=? where user_name=?");

      ps.setBoolean(1, alterPriv);
      ps.setBoolean(2, createPriv);
      ps.setBoolean(3, createViewPriv);
      ps.setBoolean(4, dbaPriv);
      ps.setBoolean(5, deletePriv);
      ps.setBoolean(6, dropPriv);
      ps.setBoolean(7, indexPriv);
      ps.setBoolean(8, insertPriv);
      ps.setBoolean(9, selPriv);
      ps.setBoolean(10, showViewPriv);
      ps.setBoolean(11, updatePriv);
      ps.setString(12, userName);

      ps.executeUpdate();

      con.commit();
      success = true;
    } catch (SQLException ex) {
      LOG.error("grant auth sys error , user=" + userName + ", msg="
          + ex.getMessage());
      throw new MetaException(ex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    return success;
  }

  @Override
  public boolean grantRoleToUser(String userName, List<String> roles)
      throws NoSuchObjectException, InvalidObjectException, MetaException {
    Connection con = null;
    ;
    PreparedStatement ps = null;
    boolean success = false;

    userName = userName.toLowerCase();
    List<String> roleLowerCase = new ArrayList<String>(roles.size());
    for (String role : roles) {
      roleLowerCase.add(role.toLowerCase());
    }

    roles = roleLowerCase;

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("grant role to user error , user=" + userName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("grant role to user error , user=" + userName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con
          .prepareStatement("select user_name from tdwuser where user_name=?");
      ps.setString(1, userName);

      boolean isPrivFind = false;
      ResultSet userSet = ps.executeQuery();

      while (userSet.next()) {
        isPrivFind = true;
        break;
      }

      userSet.close();
      ps.close();

      if (!isPrivFind) {
        throw new NoSuchObjectException("can not find user:" + userName);
      }

      ps = con
          .prepareStatement("select role_name from tdwuserrole where user_name=?");
      ps.setString(1, userName);

      List<String> roleSet = new ArrayList<String>();
      ResultSet roleRetSet = ps.executeQuery();
      while (roleRetSet.next()) {
        roleSet.add(roleRetSet.getString(1));
      }
      roleRetSet.close();
      ps.close();

      roles.removeAll(roleSet);

      if (!roles.isEmpty()) {
        ps = con
            .prepareStatement("insert into tdwuserrole(user_name, role_name) values(?,?)");

        for (String role : roles) {
          ps.setString(1, userName);
          ps.setString(2, role);
          ps.addBatch();
        }

        ps.executeBatch();
      }

      con.commit();
      success = true;
    } catch (SQLException ex) {
      LOG.error("grant auth sys error , user=" + userName + ", msg="
          + ex.getMessage());
      ex.printStackTrace();
      throw new MetaException(ex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    return success;
  }

  @Override
  public boolean revokeAuthSys(String userName, List<String> privileges)
      throws NoSuchObjectException, InvalidObjectException, MetaException {
    Connection con = null;
    ;
    PreparedStatement ps = null;
    boolean success = false;
    userName = userName.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("revoke auth from user error , user=" + userName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("revoke auth from user error , user=" + userName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con
          .prepareStatement("select alter_priv,create_priv, createview_priv, dba_priv "
              + ",delete_priv, drop_priv, index_priv, insert_priv, select_priv, showview_priv"
              + ",update_priv from tdwuser where user_name=?");

      ps.setString(1, userName);

      boolean isPrivFind = false;

      boolean selPriv = false;
      boolean insertPriv = false;
      boolean createPriv = false;
      boolean dropPriv = false;
      boolean deletePriv = false;
      boolean alterPriv = false;
      boolean updatePriv = false;
      boolean indexPriv = false;
      boolean showViewPriv = false;
      boolean createViewPriv = false;
      boolean dbaPriv = false;

      ResultSet userSet = ps.executeQuery();

      while (userSet.next()) {
        isPrivFind = true;
        alterPriv = userSet.getBoolean(1);
        createPriv = userSet.getBoolean(2);
        createViewPriv = userSet.getBoolean(3);
        dbaPriv = userSet.getBoolean(4);
        deletePriv = userSet.getBoolean(5);
        dropPriv = userSet.getBoolean(6);
        indexPriv = userSet.getBoolean(7);
        insertPriv = userSet.getBoolean(8);
        selPriv = userSet.getBoolean(9);
        showViewPriv = userSet.getBoolean(10);
        updatePriv = userSet.getBoolean(11);
        break;
      }

      userSet.close();
      ps.close();

      if (!isPrivFind) {
        throw new NoSuchObjectException("can not find user:" + userName);
      }

      for (String priv : privileges) {
        if (priv.equals("TOK_SELECT_PRI")) {
          selPriv = false;
        } else if (priv.equals("TOK_INSERT_PRI")) {
          insertPriv = false;
        } else if (priv.equals("TOK_CREATE_PRI")) {
          createPriv = false;
        } else if (priv.equals("TOK_DROP_PRI")) {
          dropPriv = false;
        } else if (priv.equals("TOK_DELETE_PRI")) {
          deletePriv = false;
        } else if (priv.equals("TOK_ALTER_PRI")) {
          alterPriv = false;
        } else if (priv.equals("TOK_UPDATE_PRI")) {
          updatePriv = false;
        } else if (priv.equals("TOK_INDEX_PRI")) {
          indexPriv = false;
        } else if (priv.equals("TOK_CREATEVIEW_PRI")) {
          createViewPriv = false;
        } else if (priv.equals("TOK_SHOWVIEW_PRI")) {
          showViewPriv = false;
        } else if (priv.equals("TOK_DBA_PRI")) {
          dbaPriv = false;
        } else if (priv.equals("TOK_ALL_PRI")) {
          selPriv = false;
          insertPriv = false;
          createPriv = false;
          dropPriv = false;
          deletePriv = false;
          alterPriv = false;
          updatePriv = false;
          indexPriv = false;
          createViewPriv = false;
          showViewPriv = false;
        } else {
          throw new InvalidObjectException("Privilege does not exist: " + priv);
        }
      }

      ps = con
          .prepareStatement("update tdwuser set alter_priv=?, create_priv=?, createview_priv=?, dba_priv=?,"
              + " delete_priv=?, drop_priv=?, index_priv=?, insert_priv=?, select_priv=?, showview_priv=?,"
              + " update_priv=? where user_name=?");

      ps.setBoolean(1, alterPriv);
      ps.setBoolean(2, createPriv);
      ps.setBoolean(3, createViewPriv);
      ps.setBoolean(4, dbaPriv);
      ps.setBoolean(5, deletePriv);
      ps.setBoolean(6, dropPriv);
      ps.setBoolean(7, indexPriv);
      ps.setBoolean(8, insertPriv);
      ps.setBoolean(9, selPriv);
      ps.setBoolean(10, showViewPriv);
      ps.setBoolean(11, updatePriv);
      ps.setString(12, userName);

      ps.executeUpdate();

      con.commit();
      success = true;
    } catch (SQLException ex) {
      LOG.error("revoke auth from user error , user=" + userName + ", msg="
          + ex.getMessage());
      ex.printStackTrace();
      throw new MetaException(ex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    return success;
  }

  @Override
  public boolean revokeRoleFromUser(String userName, List<String> roles)
      throws NoSuchObjectException, InvalidObjectException, MetaException {
    Connection con = null;
    ;
    Statement ps = null;
    boolean success = false;

    userName = userName.toLowerCase();
    List<String> roleLowerCase = new ArrayList<String>(roles.size());
    for (String role : roles) {
      roleLowerCase.add(role.toLowerCase());
    }

    roles = roleLowerCase;

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("revoke role to user error , user=" + userName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("revoke role to user error , user=" + userName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      ps = con.createStatement();

      String sql = "select user_name from tdwuser where user_name='" + userName
          + "'";

      boolean isPrivFind = false;
      ResultSet userSet = ps.executeQuery(sql);

      while (userSet.next()) {
        isPrivFind = true;
        break;
      }

      userSet.close();

      if (!isPrivFind) {
        throw new NoSuchObjectException("can not find user:" + userName);
      }

      for (String role : roles) {
        sql = "select role_name from tdwrole where role_name='" + role + "'";
        boolean isRoleFind = false;
        ResultSet roleTempSet = ps.executeQuery(sql);

        while (roleTempSet.next()) {
          isRoleFind = true;
        }

        roleTempSet.close();

        if (!isRoleFind) {
          throw new InvalidObjectException("Role does not exist: " + role);
        }
      }

      for (String role : roles) {
        ps.addBatch("delete from tdwuserrole where user_name='" + userName
            + "' and role_name='" + role + "'");
      }

      ps.executeBatch();
      ps.clearBatch();

      con.commit();
      success = true;
    } catch (SQLException ex) {
      LOG.error("revoke role from user error , user=" + userName + ", msg="
          + ex.getMessage());
      throw new MetaException(ex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    return success;
  }

  @Override
  public boolean isARole(String roleName) throws MetaException {
    Connection con = null;
    ;
    Statement ps = null;
    boolean success = false;
    roleName = roleName.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("check role error, role=" + roleName + ", passwd=" + passwd
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("check role error, role=" + roleName + ", passwd=" + passwd
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = "select role_name from tdwrole where role_name='" + roleName
          + "'";

      boolean isRoleFind = false;

      ResultSet userSet = ps.executeQuery(sql);

      while (userSet.next()) {
        isRoleFind = true;
        break;
      }

      userSet.close();

      if (!isRoleFind) {
        throw new NoSuchObjectException("can not find user:" + roleName);
      }

      success = true;
    } catch (Exception ex) {
      LOG.error("check role error, role=" + roleName + ", passwd=" + passwd
          + ", msg=" + ex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    return success;
  }

  @Override
  public boolean createRole(String roleName) throws AlreadyExistsException,
      MetaException {
    Connection con = null;
    ;
    PreparedStatement ps = null;
    boolean success = false;
    roleName = roleName.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("create role error, role=" + roleName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("create role error, role=" + roleName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con
          .prepareStatement("select count(1) from tdwuser where user_name=?");
      ps.setString(1, roleName);
      ResultSet uSet = ps.executeQuery();
      int count = 0;

      while (uSet.next()) {
        count = uSet.getInt(1);
      }

      uSet.close();
      ps.close();

      if (count != 0) {
        throw new MetaException(
            "Fail to create the new role! There is a user with the same name!");
      }

      ps = con.prepareStatement("insert into tdwrole(role_name) values(?)");
      ps.setString(1, roleName);
      ps.executeUpdate();

      con.commit();
      success = true;
    } catch (SQLException sqlex) {
      LOG.error("create role error, role=" + roleName + ", msg="
          + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new AlreadyExistsException(sqlex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    return success;
  }

  public boolean dropRoleNoDistributeTransaction(String roleName)
      throws NoSuchObjectException, MetaException {
    Connection con = null;
    ;
    Statement ps = null;
    boolean success = false;

    roleName = roleName.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("drop role error, role=" + roleName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("drop role error, role=" + roleName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      ps = con.createStatement();

      String sql = "select role_name from tdwrole where role_name='" + roleName
          + "'";

      boolean isRoleFind = false;
      ResultSet roleSet = ps.executeQuery(sql);
      while (roleSet.next()) {
        isRoleFind = true;
        break;
      }

      roleSet.close();

      if (!isRoleFind) {
        throw new NoSuchObjectException("can not find role:" + roleName);
      }

      sql = "delete from tdwrole where role_name='" + roleName + "'";
      ps.executeUpdate(sql);

      sql = "delete from dbpriv where user_name='" + roleName + "'";
      ps.executeUpdate(sql);

      sql = "delete from tblpriv where user_name='" + roleName + "'";
      ps.executeUpdate(sql);

      con.commit();
      success = true;
    } catch (SQLException sqlex) {
      LOG.error("drop role error, role=" + roleName + ", msg="
          + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    

    return success;
  }

  public boolean dropRoleByDistributeTransaction(String roleName)
      throws MetaException, NoSuchObjectException {

    Connection con = null;
    ;
    PreparedStatement ps = null;
    boolean success = false;
    roleName = roleName.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("drop role error, role=" + roleName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("drop role error, role=" + roleName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con
          .prepareStatement("select role_name from tdwrole where role_name=?");
      ps.setString(1, roleName.toLowerCase());

      boolean isRoleFind = false;
      ResultSet roleSet = ps.executeQuery();
      while (roleSet.next()) {
        isRoleFind = true;
        break;
      }

      if (!isRoleFind) {
        throw new NoSuchObjectException("can not find role:" + roleName);
      }

      con.commit();
      success = true;
    } catch (SQLException sqlex) {
      LOG.error("drop role error, role=" + roleName + ", msg="
          + sqlex.getMessage());
      throw new MetaException(sqlex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    success = false;

    Connection masterConn = null;
    PGXADataSource masterDS = null;
    masterDS = getXADataSource(globalDbUrl, user, passwd);

    Set<String> slaveURLSet = getAllSegments();
    int size = slaveURLSet.size();

    PGXADataSource[] slaveDSArray = new PGXADataSource[size];
    XAConnection[] slaveDSXaConnArray = new XAConnection[size];
    XAResource[] slaveSaResArray = new XAResource[size];
    Connection[] slaveConArray = new Connection[size];
    Statement[] slaveStmtArray = new Statement[size];
    Xid[] slaveXidArray = new Xid[size];

    int index = 0;
    for (String slaveURL : slaveURLSet) {
      slaveDSArray[index] = getXADataSource(slaveURL, user, passwd);
      index++;
    }

    XAConnection masterDSXaConn = null;

    int formatID = genFormatID();

    try {
      masterDSXaConn = masterDS.getXAConnection();

      for (int i = 0; i < size; i++) {
        slaveDSXaConnArray[i] = slaveDSArray[i].getXAConnection();
        slaveSaResArray[i] = slaveDSXaConnArray[i].getXAResource();
        slaveConArray[i] = slaveDSXaConnArray[i].getConnection();
        slaveStmtArray[i] = slaveConArray[i].createStatement();

        byte id1 = (byte) ((i + 2) * 2);
        byte id2 = (byte) (id1 + 1);

        slaveXidArray[i] = new MyXid(formatID, new byte[] { id1 },
            new byte[] { id2 });
      }

      XAResource masterSaRes = masterDSXaConn.getXAResource();
      masterConn = masterDSXaConn.getConnection();
      Statement masterStmt = masterConn.createStatement();
      Xid masterXid = new MyXid(formatID, new byte[] { 0x01 },
          new byte[] { 0x02 });

      try {
        masterSaRes.start(masterXid, XAResource.TMNOFLAGS);
        masterStmt.executeUpdate("delete from tdwrole where role_name='"
            + roleName.toLowerCase() + "'");
        masterSaRes.end(masterXid, XAResource.TMSUCCESS);

        for (int i = 0; i < size; i++) {
          slaveSaResArray[i].start(slaveXidArray[i], XAResource.TMNOFLAGS);
          slaveStmtArray[i]
              .executeUpdate("delete from dbpriv where user_name='"
                  + roleName.toLowerCase() + "'");
          slaveStmtArray[i]
              .executeUpdate("delete from tblpriv where user_name='"
                  + roleName.toLowerCase() + "'");
          slaveSaResArray[i].end(slaveXidArray[i], XAResource.TMSUCCESS);
        }

        boolean isAllPred = true;
        int masterRet = masterSaRes.prepare(masterXid);

        if (masterRet == XAResource.XA_OK) {
          int[] slaveRetArray = new int[size];
          for (int i = 0; i < size; i++) {
            slaveRetArray[i] = slaveSaResArray[i].prepare(slaveXidArray[i]);

            if (slaveRetArray[i] == XAResource.XA_OK) {
              continue;
            } else {
              isAllPred = false;
              break;
            }
          }

          if (isAllPred) {
            masterSaRes.commit(masterXid, false);
            for (int i = 0; i < size; i++) {
              slaveSaResArray[i].commit(slaveXidArray[i], false);
            }

            success = true;
          }
        }
      } catch (XAException e) {
        LOG.error("drop role error, role=" + roleName + ", msg="
            + e.getMessage());
        throw new MetaException(e.getMessage());
      }
    } catch (SQLException e) {
      LOG.error("drop role error, role=" + roleName + ", msg=" + e.getMessage());
      throw new MetaException(e.getMessage());
    } finally {
      closeConnection(masterConn);
      closeXAConnection(masterDSXaConn);

      for (int i = 0; i < size; i++) {
        closeConnection(slaveConArray[i]);
        closeXAConnection(slaveDSXaConnArray[i]);
      }
    }

    return success;
  }

  @Override
  public boolean dropRole(String roleName) throws NoSuchObjectException,
      MetaException {
    boolean useDistributeTran = false;
    if (!useDistributeTran) {
      return dropRoleNoDistributeTransaction(roleName);
    } else {
      return dropRoleByDistributeTransaction(roleName);
    }
  }

  @Override
  public boolean grantRoleToRole(String roleName, List<String> roles)
      throws NoSuchObjectException, InvalidObjectException, MetaException {
    Connection con = null;
    ;
    Statement ps = null;
    boolean success = false;

    roleName = roleName.toLowerCase();
    List<String> roleLowerCase = new ArrayList<String>(roles.size());
    for (String role : roles) {
      roleLowerCase.add(role.toLowerCase());
    }

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("grant role error, role=" + roleName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("grant role error, role=" + roleName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      ps = con.createStatement();

      String sql = "select role_name from tdwrole where role_name='"
          + roleName.toLowerCase() + "'";

      boolean isRoleFind = false;
      ResultSet roleSet = ps.executeQuery(sql);
      while (roleSet.next()) {
        isRoleFind = true;
        break;
      }

      roleSet.close();

      if (!isRoleFind) {
        throw new NoSuchObjectException("can not find role:" + roleName);
      }

      Set<String> sonRoleNameSet = new HashSet<String>();
      sql = "select sonrole_name from tdwsonrole where role_name='"
          + roleName.toLowerCase() + "'";

      ResultSet sonroleSet = ps.executeQuery(sql);
      while (sonroleSet.next()) {
        sonRoleNameSet.add(sonroleSet.getString(1));
      }
      sonroleSet.close();

      List<String> needAddRoles = new ArrayList<String>();
      for (String role : roles) {
        if (!roleName.equalsIgnoreCase(role) && !sonRoleNameSet.contains(role)) {
          needAddRoles.add(role);
        }
      }

      if (!needAddRoles.isEmpty()) {
        for (String role : needAddRoles) {
          ps.addBatch("insert into tdwsonrole(role_name, sonrole_name) values('"
              + roleName.toLowerCase() + "', '" + role.toLowerCase() + "')");
        }
        ps.executeBatch();
      }

      con.commit();
      success = true;
    } catch (SQLException sqlex) {
      sqlex.printStackTrace();
      LOG.error("grant role error, role=" + roleName + ", msg="
          + sqlex.getMessage());
      throw new MetaException(sqlex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    return success;
  }

  @Override
  public boolean revokeRoleFromRole(String roleName, List<String> roles)
      throws NoSuchObjectException, InvalidObjectException, MetaException {
    Connection con = null;
    ;
    PreparedStatement ps = null;
    boolean success = false;

    roleName = roleName.toLowerCase();
    List<String> roleLowerCase = new ArrayList<String>(roles.size());
    for (String role : roles) {
      roleLowerCase.add(role.toLowerCase());
    }

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("revoke role error, role=" + roleName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("revoke role error, role=" + roleName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con
          .prepareStatement("select role_name from tdwrole where role_name=?");
      ps.setString(1, roleName.toLowerCase());

      boolean isRoleFind = false;
      ResultSet roleSet = ps.executeQuery();
      while (roleSet.next()) {
        isRoleFind = true;
        break;
      }

      roleSet.close();
      ps.close();

      if (!isRoleFind) {
        throw new NoSuchObjectException("can not find role:" + roleName);
      }

      ps = con
          .prepareStatement("delete from tdwsonrole where role_name=? and sonrole_name=?");
      for (String role : roles) {
        ps.setString(1, roleName.toLowerCase());
        ps.setString(2, role.toLowerCase());
        ps.addBatch();
      }
      ps.executeBatch();

      con.commit();
      success = true;
    } catch (SQLException sqlex) {
      LOG.error("revoke role error, role=" + roleName + ", msg="
          + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    return success;
  }

  @Override
  public Role getRole(String roleName) throws NoSuchObjectException,
      MetaException {
    Connection con = null;
    ;
    Statement ps = null;
    Role role = null;

    roleName = roleName.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("get role error, role=" + roleName + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get role error, role=" + roleName + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = "select alter_priv, create_priv, createview_priv, dba_priv, "
          + "delete_priv, drop_priv, index_priv, insert_priv, select_priv, showview_priv, "
          + "update_priv, role_name from tdwrole where role_name='"
          + roleName
          + "'";

      boolean isRoleFind = false;
      ResultSet roleSet = ps.executeQuery(sql);

      while (roleSet.next()) {
        isRoleFind = true;

        role = new Role();
        role.setAlterPriv(roleSet.getBoolean(1));
        role.setCreatePriv(roleSet.getBoolean(2));
        role.setCreateviewPriv(roleSet.getBoolean(3));
        role.setDbaPriv(roleSet.getBoolean(4));
        role.setDeletePriv(roleSet.getBoolean(5));
        role.setDropPriv(roleSet.getBoolean(6));
        role.setIndexPriv(roleSet.getBoolean(7));
        role.setInsertPriv(roleSet.getBoolean(8));
        role.setSelectPriv(roleSet.getBoolean(9));
        role.setShowviewPriv(roleSet.getBoolean(10));
        role.setUpdatePriv(roleSet.getBoolean(11));
        role.setRoleName(roleSet.getString(12));

        break;
      }

      roleSet.close();

      if (!isRoleFind) {
        throw new NoSuchObjectException("can not find role:" + roleName);
      }

      List<String> sonRoles = new ArrayList<String>();
      role.setPlayRoles(sonRoles);
    } catch (SQLException sqlex) {
      LOG.error("revoke role error, role=" + roleName + ", msg="
          + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    return role;
  }

  @Override
  public List<String> getRolesAll() throws MetaException {
    Connection con = null;
    ;
    Statement ps = null;
    List<String> roles = new ArrayList<String>();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("get roles error,  msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get roles error,  msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = "select  role_name from tdwrole";

      ResultSet roleSet = ps.executeQuery(sql);

      while (roleSet.next()) {
        roles.add(roleSet.getString(1));
      }
    } catch (SQLException sqlex) {
      LOG.error("get roles error,  msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    return roles;
  }

  @Override
  public boolean grantAuthRoleSys(String role, List<String> privileges)
      throws NoSuchObjectException, InvalidObjectException, MetaException {
    Connection con = null;
    ;
    PreparedStatement ps = null;
    boolean success = false;

    role = role.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("grant role auth sys error , user=" + role + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("grant role auth sys error , user=" + role + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con
          .prepareStatement("select alter_priv,create_priv, createview_priv, dba_priv "
              + ",delete_priv, drop_priv, index_priv, insert_priv, select_priv, showview_priv"
              + ",update_priv from tdwrole where role_name=?");
      ps.setString(1, role);

      boolean isPrivFind = false;

      boolean selPriv = false;
      boolean insertPriv = false;
      boolean createPriv = false;
      boolean dropPriv = false;
      boolean deletePriv = false;
      boolean alterPriv = false;
      boolean updatePriv = false;
      boolean indexPriv = false;
      boolean showViewPriv = false;
      boolean createViewPriv = false;
      boolean dbaPriv = false;

      ResultSet userSet = ps.executeQuery();

      while (userSet.next()) {
        isPrivFind = true;
        alterPriv = userSet.getBoolean(1);
        createPriv = userSet.getBoolean(2);
        createViewPriv = userSet.getBoolean(3);
        dbaPriv = userSet.getBoolean(4);
        deletePriv = userSet.getBoolean(5);
        dropPriv = userSet.getBoolean(6);
        indexPriv = userSet.getBoolean(7);
        insertPriv = userSet.getBoolean(8);
        selPriv = userSet.getBoolean(9);
        showViewPriv = userSet.getBoolean(10);
        updatePriv = userSet.getBoolean(11);
        break;
      }

      userSet.close();
      ps.close();

      if (!isPrivFind) {
        throw new NoSuchObjectException("can not find role:" + role);
      }

      for (String priv : privileges) {
        if (priv.equals("TOK_SELECT_PRI")) {
          selPriv = true;
        } else if (priv.equals("TOK_INSERT_PRI")) {
          insertPriv = true;
        } else if (priv.equals("TOK_CREATE_PRI")) {
          createPriv = true;
        } else if (priv.equals("TOK_DROP_PRI")) {
          dropPriv = true;
        } else if (priv.equals("TOK_DELETE_PRI")) {
          deletePriv = true;
        } else if (priv.equals("TOK_ALTER_PRI")) {
          alterPriv = true;
        } else if (priv.equals("TOK_UPDATE_PRI")) {
          updatePriv = true;
        } else if (priv.equals("TOK_INDEX_PRI")) {
          indexPriv = true;
        } else if (priv.equals("TOK_CREATEVIEW_PRI")) {
          createViewPriv = true;
        } else if (priv.equals("TOK_SHOWVIEW_PRI")) {
          showViewPriv = true;
        } else if (priv.equals("TOK_DBA_PRI")) {
          dbaPriv = true;
        } else if (priv.equals("TOK_ALL_PRI")) {
          selPriv = true;
          insertPriv = true;
          createPriv = true;
          dropPriv = true;
          deletePriv = true;
          alterPriv = true;
          updatePriv = true;
          indexPriv = true;
          createViewPriv = true;
          showViewPriv = true;
        } else {
          throw new InvalidObjectException("Privilege does not exist: " + priv);
        }
      }

      ps = con
          .prepareStatement("update tdwrole set alter_priv=?, create_priv=?, createview_priv=?, dba_priv=?,"
              + " delete_priv=?, drop_priv=?, index_priv=?, insert_priv=?, select_priv=?, showview_priv=?,"
              + " update_priv=? where role_name=?");

      ps.setBoolean(1, alterPriv);
      ps.setBoolean(2, createPriv);
      ps.setBoolean(3, createViewPriv);
      ps.setBoolean(4, dbaPriv);
      ps.setBoolean(5, deletePriv);
      ps.setBoolean(6, dropPriv);
      ps.setBoolean(7, indexPriv);
      ps.setBoolean(8, insertPriv);
      ps.setBoolean(9, selPriv);
      ps.setBoolean(10, showViewPriv);
      ps.setBoolean(11, updatePriv);
      ps.setString(12, role);

      ps.executeUpdate();

      con.commit();
      success = true;
    } catch (SQLException ex) {
      ex.printStackTrace();
      LOG.error("grant role auth sys error , user=" + role + ", msg="
          + ex.getMessage());
      throw new MetaException(ex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    return success;
  }

  @Override
  public boolean revokeAuthRoleSys(String role, List<String> privileges)
      throws NoSuchObjectException, InvalidObjectException, MetaException {
    Connection con = null;
    ;
    PreparedStatement ps = null;
    boolean success = false;

    role = role.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("revoke role auth from user error , user=" + role + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("revoke role auth from user error , user=" + role + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con
          .prepareStatement("select alter_priv,create_priv, createview_priv, dba_priv "
              + ",delete_priv, drop_priv, index_priv, insert_priv, select_priv, showview_priv"
              + ",update_priv from tdwrole where role_name=?");

      ps.setString(1, role);

      boolean isPrivFind = false;

      boolean selPriv = false;
      boolean insertPriv = false;
      boolean createPriv = false;
      boolean dropPriv = false;
      boolean deletePriv = false;
      boolean alterPriv = false;
      boolean updatePriv = false;
      boolean indexPriv = false;
      boolean showViewPriv = false;
      boolean createViewPriv = false;
      boolean dbaPriv = false;

      ResultSet userSet = ps.executeQuery();

      while (userSet.next()) {
        isPrivFind = true;
        alterPriv = userSet.getBoolean(1);
        createPriv = userSet.getBoolean(2);
        createViewPriv = userSet.getBoolean(3);
        dbaPriv = userSet.getBoolean(4);
        deletePriv = userSet.getBoolean(5);
        dropPriv = userSet.getBoolean(6);
        indexPriv = userSet.getBoolean(7);
        insertPriv = userSet.getBoolean(8);
        selPriv = userSet.getBoolean(9);
        showViewPriv = userSet.getBoolean(10);
        updatePriv = userSet.getBoolean(11);
        break;
      }

      userSet.close();
      ps.close();

      if (!isPrivFind) {
        throw new NoSuchObjectException("can not find user:" + role);
      }

      for (String priv : privileges) {
        if (priv.equals("TOK_SELECT_PRI")) {
          selPriv = false;
        } else if (priv.equals("TOK_INSERT_PRI")) {
          insertPriv = false;
        } else if (priv.equals("TOK_CREATE_PRI")) {
          createPriv = false;
        } else if (priv.equals("TOK_DROP_PRI")) {
          dropPriv = false;
        } else if (priv.equals("TOK_DELETE_PRI")) {
          deletePriv = false;
        } else if (priv.equals("TOK_ALTER_PRI")) {
          alterPriv = false;
        } else if (priv.equals("TOK_UPDATE_PRI")) {
          updatePriv = false;
        } else if (priv.equals("TOK_INDEX_PRI")) {
          indexPriv = false;
        } else if (priv.equals("TOK_CREATEVIEW_PRI")) {
          createViewPriv = false;
        } else if (priv.equals("TOK_SHOWVIEW_PRI")) {
          showViewPriv = false;
        } else if (priv.equals("TOK_DBA_PRI")) {
          dbaPriv = false;
        } else if (priv.equals("TOK_ALL_PRI")) {
          selPriv = false;
          insertPriv = false;
          createPriv = false;
          dropPriv = false;
          deletePriv = false;
          alterPriv = false;
          updatePriv = false;
          indexPriv = false;
          createViewPriv = false;
          showViewPriv = false;
        } else {
          throw new InvalidObjectException("Privilege does not exist: " + priv);
        }
      }

      ps = con
          .prepareStatement("update tdwrole set alter_priv=?, create_priv=?, createview_priv=?, dba_priv=?,"
              + " delete_priv=?, drop_priv=?, index_priv=?, insert_priv=?, select_priv=?, showview_priv=?,"
              + " update_priv=? where role_name=?");

      ps.setBoolean(1, alterPriv);
      ps.setBoolean(2, createPriv);
      ps.setBoolean(3, createViewPriv);
      ps.setBoolean(4, dbaPriv);
      ps.setBoolean(5, deletePriv);
      ps.setBoolean(6, dropPriv);
      ps.setBoolean(7, indexPriv);
      ps.setBoolean(8, insertPriv);
      ps.setBoolean(9, selPriv);
      ps.setBoolean(10, showViewPriv);
      ps.setBoolean(11, updatePriv);
      ps.setString(12, role);

      ps.executeUpdate();

      con.commit();
      success = true;
    } catch (SQLException ex) {
      LOG.error("revoke auth from role error , user=" + role + ", msg="
          + ex.getMessage());
      ex.printStackTrace();
      throw new MetaException(ex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    return success;
  }

  @Override
  public boolean grantAuthOnDb(String forWho, List<String> privileges, String db)
      throws NoSuchObjectException, InvalidObjectException, MetaException {
    Connection con = null;
    Statement ps = null;
    boolean success = false;
    PreparedStatement pss = null;
    forWho = forWho.toLowerCase();
    db = db.toLowerCase();

    try {
      con = getSegmentConnection(db);
    } catch (MetaStoreConnectException e1) {
      LOG.error("grant auth on db error, db=" + db + ", forwho=" + forWho
          + ", msg=" + e1.getMessage());
      throw new MetaException("can not find db:" + db);
    } catch (SQLException e1) {
      LOG.error("grant auth on db error, db=" + db + ", forwho=" + forWho
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    if (privileges == null) {
      throw new InvalidObjectException("No privileges are given!");
    }

    try {
      ps = con.createStatement();

      String sql = "select name from dbs where name='" + db + "'";

      boolean isDbfind = false;
      ResultSet dbSet = ps.executeQuery(sql);

      while (dbSet.next()) {
        isDbfind = true;
        break;
      }

      dbSet.close();

      if (!isDbfind) {
        throw new NoSuchObjectException("can not find db:" + db);
      }
    } catch (SQLException ex) {
      LOG.error("grant auth on db error, db=" + db + ", forwho=" + forWho
          + ", msg=" + ex.getMessage());
      ex.printStackTrace();
      throw new MetaException(ex.getMessage());
    } finally {

      closeStatement(ps);
      closeConnection(con);
    }

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("grant auth on db error, db=" + db + ", forwho=" + forWho
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("grant auth on db error, db=" + db + ", forwho=" + forWho
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      ps = con.createStatement();

      String sql = "select alter_priv,create_priv,createview_priv, "
          + " delete_priv, drop_priv, index_priv, insert_priv, select_priv, showview_priv,"
          + " update_priv from dbpriv where user_name='" + forWho
          + "' and db_name='" + db + "'";

      boolean isPrivFind = false;

      boolean selPriv = false;
      boolean insertPriv = false;
      boolean createPriv = false;
      boolean dropPriv = false;
      boolean deletePriv = false;
      boolean alterPriv = false;
      boolean updatePriv = false;
      boolean indexPriv = false;
      boolean showViewPriv = false;
      boolean createViewPriv = false;

      ResultSet privSet = ps.executeQuery(sql);

      while (privSet.next()) {
        isPrivFind = true;
        alterPriv = privSet.getBoolean(1);
        createPriv = privSet.getBoolean(2);
        createViewPriv = privSet.getBoolean(3);
        deletePriv = privSet.getBoolean(4);
        dropPriv = privSet.getBoolean(5);
        indexPriv = privSet.getBoolean(6);
        insertPriv = privSet.getBoolean(7);
        selPriv = privSet.getBoolean(8);
        showViewPriv = privSet.getBoolean(9);
        updatePriv = privSet.getBoolean(10);

        break;
      }

      privSet.close();

      for (String priv : privileges) {
        if (priv.equals("TOK_SELECT_PRI")) {
          selPriv = true;
        } else if (priv.equals("TOK_INSERT_PRI")) {
          insertPriv = true;
        } else if (priv.equals("TOK_CREATE_PRI")) {
          createPriv = true;
        } else if (priv.equals("TOK_DROP_PRI")) {
          dropPriv = true;
        } else if (priv.equals("TOK_DELETE_PRI")) {
          deletePriv = true;
        } else if (priv.equals("TOK_ALTER_PRI")) {
          alterPriv = true;
        } else if (priv.equals("TOK_UPDATE_PRI")) {
          updatePriv = true;
        } else if (priv.equals("TOK_INDEX_PRI")) {
          indexPriv = true;
        } else if (priv.equals("TOK_CREATEVIEW_PRI")) {
          createViewPriv = true;
        } else if (priv.equals("TOK_SHOWVIEW_PRI")) {
          showViewPriv = true;
        } else if (priv.equals("TOK_ALL_PRI")) {
          selPriv = true;
          insertPriv = true;
          createPriv = true;
          dropPriv = true;
          deletePriv = true;
          alterPriv = true;
          updatePriv = true;
          indexPriv = true;
          createViewPriv = true;
          showViewPriv = true;
        } else
          throw new InvalidObjectException("Privilege does not exist: " + priv);
      }

      if (!isPrivFind) {
        pss = con
            .prepareStatement("insert into dbpriv(alter_priv, create_priv, createview_priv,"
                + "delete_priv, drop_priv, index_priv, insert_priv, select_priv"
                + ", showview_priv, update_priv, user_name, db_name) values(?,?,?,?,?,?,?,?,?,?,?,?)");

        pss.setBoolean(1, alterPriv);
        pss.setBoolean(2, createPriv);
        pss.setBoolean(3, createViewPriv);
        pss.setBoolean(4, deletePriv);
        pss.setBoolean(5, dropPriv);
        pss.setBoolean(6, indexPriv);
        pss.setBoolean(7, insertPriv);
        pss.setBoolean(8, selPriv);
        pss.setBoolean(9, showViewPriv);
        pss.setBoolean(10, updatePriv);
        pss.setString(11, forWho);
        pss.setString(12, db);

        pss.executeUpdate();
        pss.close();
      } else {
        pss = con
            .prepareStatement("update dbpriv set alter_priv=?, create_priv=?, createview_priv=?, "
                + " delete_priv=?, drop_priv=?, index_priv=?, insert_priv=?, select_priv=?, showview_priv=?,"
                + " update_priv=? where user_name=? and db_name=?");

        pss.setBoolean(1, alterPriv);
        pss.setBoolean(2, createPriv);
        pss.setBoolean(3, createViewPriv);
        pss.setBoolean(4, deletePriv);
        pss.setBoolean(5, dropPriv);
        pss.setBoolean(6, indexPriv);
        pss.setBoolean(7, insertPriv);
        pss.setBoolean(8, selPriv);
        pss.setBoolean(9, showViewPriv);
        pss.setBoolean(10, updatePriv);
        pss.setString(11, forWho);
        pss.setString(12, db);

        pss.executeUpdate();
        pss.close();
      }

      con.commit();
      success = true;
    } catch (SQLException ex) {
      LOG.error("grant auth on db error, db=" + db + ", forwho=" + forWho
          + ", msg=" + ex.getMessage());
      ex.printStackTrace();
      throw new MetaException(ex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeStatement(pss);
      closeConnection(con);
    }

    

    return success;
  }

  @Override
  public DbPriv getAuthOnDb(String who, String db) throws MetaException {
    Connection con = null;
    Statement ps = null;
    DbPriv dbPriv = null;
    who = who.toLowerCase();
    db = db.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("get auth on db error, db=" + db + ", forwho=" + who + ", msg="
          + e1.getMessage());
      throw new MetaException("can not find db:" + db);
    } catch (SQLException e1) {
      LOG.error("get auth on db error, db=" + db + ", forwho=" + who + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = "select alter_priv, create_priv, createview_priv, "
          + "delete_priv, drop_priv, index_priv, insert_priv, select_priv, "
          + "showview_priv, update_priv from dbpriv where user_name='" + who
          + "' and db_name='" + db + "'";

      ResultSet dbPrivSet = ps.executeQuery(sql);
      while (dbPrivSet.next()) {
        dbPriv = new DbPriv();
        dbPriv.setAlterPriv(dbPrivSet.getBoolean(1));
        dbPriv.setCreatePriv(dbPrivSet.getBoolean(2));
        dbPriv.setCreateviewPriv(dbPrivSet.getBoolean(3));
        dbPriv.setDeletePriv(dbPrivSet.getBoolean(4));
        dbPriv.setDropPriv(dbPrivSet.getBoolean(5));
        dbPriv.setIndexPriv(dbPrivSet.getBoolean(6));
        dbPriv.setInsertPriv(dbPrivSet.getBoolean(7));
        dbPriv.setSelectPriv(dbPrivSet.getBoolean(8));
        dbPriv.setShowviewPriv(dbPrivSet.getBoolean(9));
        dbPriv.setUpdatePriv(dbPrivSet.getBoolean(10));
        dbPriv.setDb(db);
        dbPriv.setUser(who);
        break;
      }
    } catch (SQLException ex) {
      LOG.error("get auth on db error, db=" + db + ", forwho=" + who + ", msg="
          + ex.getMessage());
      ex.printStackTrace();
      throw new MetaException(ex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    return dbPriv;
  }

  @Override
  public List<DbPriv> getDbAuth(String db) throws MetaException {
    Connection con = null;
    Statement ps = null;

    List<DbPriv> dbPrivs = new ArrayList<DbPriv>();
    db = db.toLowerCase();

    try {
      con = getSegmentConnection(db);
    } catch (MetaStoreConnectException e1) {
      LOG.error("get auth on db error, db=" + db + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get auth on db error, db=" + db + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = "select alter_priv, create_priv, createview_priv, "
          + "delete_priv, drop_priv, index_priv, insert_priv, select_priv, "
          + "showview_priv, update_priv, user_name from dbpriv where db_name='"
          + db + "'";

      ResultSet dbPrivSet = ps.executeQuery(sql);

      while (dbPrivSet.next()) {
        DbPriv dbPriv = new DbPriv();

        dbPriv.setAlterPriv(dbPrivSet.getBoolean(1));
        dbPriv.setCreatePriv(dbPrivSet.getBoolean(2));
        dbPriv.setCreateviewPriv(dbPrivSet.getBoolean(3));
        dbPriv.setDeletePriv(dbPrivSet.getBoolean(4));
        dbPriv.setDropPriv(dbPrivSet.getBoolean(5));
        dbPriv.setIndexPriv(dbPrivSet.getBoolean(6));
        dbPriv.setInsertPriv(dbPrivSet.getBoolean(7));
        dbPriv.setSelectPriv(dbPrivSet.getBoolean(8));
        dbPriv.setShowviewPriv(dbPrivSet.getBoolean(9));
        dbPriv.setUpdatePriv(dbPrivSet.getBoolean(10));
        dbPriv.setUser(dbPrivSet.getString(11));
        dbPriv.setDb(db);

        dbPrivs.add(dbPriv);
      }

    } catch (SQLException ex) {
      LOG.error("get auth on db error, db=" + db + ", msg=" + ex.getMessage());
      ex.printStackTrace();
      throw new MetaException(ex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    return dbPrivs;
  }

  public List<DbPriv> getAuthOnDbsNoDistributeTransaction(String who)
      throws MetaException {
    Connection con = null;
    Statement ps = null;
    boolean success = false;
    List<DbPriv> dbPrivs = new ArrayList<DbPriv>();

    who = who.toLowerCase();

    success = false;
    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("get user auth on dbs error, user=" + who + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get user auth on dbs error, user=" + who + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = "select alter_priv, create_priv, createview_priv, "
          + "delete_priv, drop_priv, index_priv, insert_priv, select_priv, "
          + "showview_priv, update_priv, db_name from dbpriv where user_name='"
          + who + "' order by db_name asc";

      ResultSet dbPrivSet = ps.executeQuery(sql);
      while (dbPrivSet.next()) {
        DbPriv dbPriv = new DbPriv();
        dbPriv.setAlterPriv(dbPrivSet.getBoolean(1));
        dbPriv.setCreatePriv(dbPrivSet.getBoolean(2));
        dbPriv.setCreateviewPriv(dbPrivSet.getBoolean(3));
        dbPriv.setDeletePriv(dbPrivSet.getBoolean(4));
        dbPriv.setDropPriv(dbPrivSet.getBoolean(5));
        dbPriv.setIndexPriv(dbPrivSet.getBoolean(6));
        dbPriv.setInsertPriv(dbPrivSet.getBoolean(7));
        dbPriv.setSelectPriv(dbPrivSet.getBoolean(8));
        dbPriv.setShowviewPriv(dbPrivSet.getBoolean(9));
        dbPriv.setUpdatePriv(dbPrivSet.getBoolean(10));
        dbPriv.setDb(dbPrivSet.getString(11));
        dbPriv.setUser(who);
        dbPrivs.add(dbPriv);
      }

      success = true;
    } catch (SQLException sqlex) {
      LOG.error("get user auth on dbs error, user=" + who + ", msg="
          + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    if (success) {
      return dbPrivs;
    } else {
      return null;
    }
  }

  public List<DbPriv> getAuthOnDbsByDistributeTransaction(String who)
      throws MetaException {
    boolean success = false;

    List<DbPriv> dbPrivs = new ArrayList<DbPriv>();

    Set<String> slaveURLSet = getAllSegments();
    int size = slaveURLSet.size();

    who = who.toLowerCase();

    PGXADataSource[] slaveDSArray = new PGXADataSource[size];
    XAConnection[] slaveDSXaConnArray = new XAConnection[size];
    XAResource[] slaveSaResArray = new XAResource[size];
    Connection[] slaveConArray = new Connection[size];
    Statement[] slaveStmtArray = new Statement[size];
    Xid[] slaveXidArray = new Xid[size];

    int index = 0;
    for (String slaveURL : slaveURLSet) {
      slaveDSArray[index] = getXADataSource(slaveURL, user, passwd);
      index++;
    }

    int formatID = genFormatID();

    try {
      for (int i = 0; i < size; i++) {
        slaveDSXaConnArray[i] = slaveDSArray[i].getXAConnection();
        slaveSaResArray[i] = slaveDSXaConnArray[i].getXAResource();
        slaveConArray[i] = slaveDSXaConnArray[i].getConnection();
        slaveStmtArray[i] = slaveConArray[i].createStatement();

        byte id1 = (byte) ((i + 2) * 2);
        byte id2 = (byte) (id1 + 1);

        slaveXidArray[i] = new MyXid(formatID, new byte[] { id1 },
            new byte[] { id2 });
      }

      try {
        for (int i = 0; i < size; i++) {
          slaveSaResArray[i].start(slaveXidArray[i], XAResource.TMNOFLAGS);

          ResultSet dbPrivSet = slaveStmtArray[i]
              .executeQuery("select alter_priv, create_priv, createview_priv, "
                  + "delete_priv, drop_priv, index_priv, insert_priv, select_priv, "
                  + "showview_priv, update_priv, db_name from dbpriv where user_name='"
                  + who.toLowerCase() + "'");

          while (dbPrivSet.next()) {
            DbPriv dbPriv = new DbPriv();
            dbPriv.setAlterPriv(dbPrivSet.getBoolean(1));
            dbPriv.setCreatePriv(dbPrivSet.getBoolean(2));
            dbPriv.setCreateviewPriv(dbPrivSet.getBoolean(3));
            dbPriv.setDeletePriv(dbPrivSet.getBoolean(4));
            dbPriv.setDropPriv(dbPrivSet.getBoolean(5));
            dbPriv.setIndexPriv(dbPrivSet.getBoolean(6));
            dbPriv.setInsertPriv(dbPrivSet.getBoolean(7));
            dbPriv.setSelectPriv(dbPrivSet.getBoolean(8));
            dbPriv.setShowviewPriv(dbPrivSet.getBoolean(9));
            dbPriv.setUpdatePriv(dbPrivSet.getBoolean(10));
            dbPriv.setDb(dbPrivSet.getString(11));
            dbPriv.setUser(who);
            dbPrivs.add(dbPriv);
          }
          slaveSaResArray[i].end(slaveXidArray[i], XAResource.TMSUCCESS);
        }

        boolean isAllPred = true;

        int[] slaveRetArray = new int[size];
        for (int i = 0; i < size; i++) {
          slaveRetArray[i] = slaveSaResArray[i].prepare(slaveXidArray[i]);

          if (slaveRetArray[i] == XAResource.XA_OK) {
            continue;
          } else {
            isAllPred = false;
            break;
          }
        }

        if (isAllPred) {
          for (int i = 0; i < size; i++) {
            slaveSaResArray[i].commit(slaveXidArray[i], false);
          }

          success = true;
        }

      } catch (XAException e) {
        LOG.error("get user auth on dbs error, user=" + who + ", msg="
            + e.getMessage());
        throw new MetaException(e.getMessage());
      }
    } catch (SQLException e) {
      LOG.error("get user auth on dbs error, user=" + who + ", msg="
          + e.getMessage());
      throw new MetaException(e.getMessage());
    } finally {
      for (int i = 0; i < size; i++) {
        closeConnection(slaveConArray[i]);
        closeXAConnection(slaveDSXaConnArray[i]);
      }
    }

    if (success) {
      return dbPrivs;
    } else {
      return null;
    }
  }

  @Override
  public List<DbPriv> getAuthOnDbs(String who) throws MetaException {
    boolean useDistributeTran = false;
    if (!useDistributeTran) {
      return getAuthOnDbsNoDistributeTransaction(who);
    } else {
      return getAuthOnDbsByDistributeTransaction(who);
    }
  }

  public List<DbPriv> getDbAuthAllNoDistributeTransaction()
      throws MetaException {
    Connection con = null;
    Statement ps = null;
    boolean success = false;
    List<DbPriv> dbPrivs = new ArrayList<DbPriv>();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("get all user auth on dbs error, msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get all user auth on dbs error, msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = "select alter_priv, create_priv, createview_priv, "
          + "delete_priv, drop_priv, index_priv, insert_priv, select_priv, "
          + "showview_priv, update_priv, db_name, user_name from dbpriv ";

      ResultSet dbPrivSet = ps.executeQuery(sql);
      while (dbPrivSet.next()) {
        DbPriv dbPriv = new DbPriv();
        dbPriv.setAlterPriv(dbPrivSet.getBoolean(1));
        dbPriv.setCreatePriv(dbPrivSet.getBoolean(2));
        dbPriv.setCreateviewPriv(dbPrivSet.getBoolean(3));
        dbPriv.setDeletePriv(dbPrivSet.getBoolean(4));
        dbPriv.setDropPriv(dbPrivSet.getBoolean(5));
        dbPriv.setIndexPriv(dbPrivSet.getBoolean(6));
        dbPriv.setInsertPriv(dbPrivSet.getBoolean(7));
        dbPriv.setSelectPriv(dbPrivSet.getBoolean(8));
        dbPriv.setShowviewPriv(dbPrivSet.getBoolean(9));
        dbPriv.setUpdatePriv(dbPrivSet.getBoolean(10));
        dbPriv.setDb(dbPrivSet.getString(11));
        dbPriv.setUser(dbPrivSet.getString(12));
        dbPrivs.add(dbPriv);
      }

      success = true;
    } catch (SQLException sqlex) {
      LOG.error("get all user auth on dbs error, msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    if (success) {
      return dbPrivs;
    } else {
      return null;
    }

    
  }

  public List<DbPriv> getDbAuthAllByDistributeTransaction()
      throws MetaException {
    boolean success = false;
    List<DbPriv> dbPrivs = new ArrayList<DbPriv>();

    Set<String> slaveURLSet = getAllSegments();
    int size = slaveURLSet.size();

    PGXADataSource[] slaveDSArray = new PGXADataSource[size];
    XAConnection[] slaveDSXaConnArray = new XAConnection[size];
    XAResource[] slaveSaResArray = new XAResource[size];
    Connection[] slaveConArray = new Connection[size];
    Statement[] slaveStmtArray = new Statement[size];
    Xid[] slaveXidArray = new Xid[size];

    int index = 0;
    for (String slaveURL : slaveURLSet) {
      slaveDSArray[index] = getXADataSource(slaveURL, user, passwd);
      index++;
    }

    int formatID = genFormatID();

    try {
      for (int i = 0; i < size; i++) {
        slaveDSXaConnArray[i] = slaveDSArray[i].getXAConnection();
        slaveSaResArray[i] = slaveDSXaConnArray[i].getXAResource();
        slaveConArray[i] = slaveDSXaConnArray[i].getConnection();
        slaveStmtArray[i] = slaveConArray[i].createStatement();

        byte id1 = (byte) ((i + 2) * 2);
        byte id2 = (byte) (id1 + 1);

        slaveXidArray[i] = new MyXid(formatID, new byte[] { id1 },
            new byte[] { id2 });
      }

      try {
        for (int i = 0; i < size; i++) {
          slaveSaResArray[i].start(slaveXidArray[i], XAResource.TMNOFLAGS);

          ResultSet dbPrivSet = slaveStmtArray[i]
              .executeQuery("select alter_priv, create_priv, createview_priv, "
                  + "delete_priv, drop_priv, index_priv, insert_priv, select_priv, "
                  + "showview_priv, update_priv, db_name, user_name from dbpriv ");

          while (dbPrivSet.next()) {
            DbPriv dbPriv = new DbPriv();
            dbPriv.setAlterPriv(dbPrivSet.getBoolean(1));
            dbPriv.setCreatePriv(dbPrivSet.getBoolean(2));
            dbPriv.setCreateviewPriv(dbPrivSet.getBoolean(3));
            dbPriv.setDeletePriv(dbPrivSet.getBoolean(4));
            dbPriv.setDropPriv(dbPrivSet.getBoolean(5));
            dbPriv.setIndexPriv(dbPrivSet.getBoolean(6));
            dbPriv.setInsertPriv(dbPrivSet.getBoolean(7));
            dbPriv.setSelectPriv(dbPrivSet.getBoolean(8));
            dbPriv.setShowviewPriv(dbPrivSet.getBoolean(9));
            dbPriv.setUpdatePriv(dbPrivSet.getBoolean(10));
            dbPriv.setDb(dbPrivSet.getString(11));
            dbPriv.setUser(dbPrivSet.getString(12));
            dbPrivs.add(dbPriv);
          }
          slaveSaResArray[i].end(slaveXidArray[i], XAResource.TMSUCCESS);
        }

        boolean isAllPred = true;

        int[] slaveRetArray = new int[size];
        for (int i = 0; i < size; i++) {
          slaveRetArray[i] = slaveSaResArray[i].prepare(slaveXidArray[i]);

          if (slaveRetArray[i] == XAResource.XA_OK) {
            continue;
          } else {
            isAllPred = false;
            break;
          }
        }

        if (isAllPred) {
          for (int i = 0; i < size; i++) {
            slaveSaResArray[i].commit(slaveXidArray[i], false);
          }

          success = true;
        }

      } catch (XAException e) {
        LOG.error("get all user auth on dbs error, msg=" + e.getMessage());
        throw new MetaException(e.getMessage());
      }
    } catch (SQLException e) {
      LOG.error("get all user auth on dbs error, msg=" + e.getMessage());
      e.printStackTrace();
      throw new MetaException(e.getMessage());
    } finally {
      for (int i = 0; i < size; i++) {
        closeConnection(slaveConArray[i]);
        closeXAConnection(slaveDSXaConnArray[i]);
      }
    }

    if (success) {
      return dbPrivs;
    } else {
      return null;
    }
  }

  @Override
  public List<DbPriv> getDbAuthAll() throws MetaException {
    boolean useDistributeTran = false;
    if (!useDistributeTran) {
      return getDbAuthAllNoDistributeTransaction();
    } else {
      return getDbAuthAllByDistributeTransaction();
    }
  }

  @Override
  public boolean revokeAuthOnDb(String who, List<String> privileges, String db)
      throws NoSuchObjectException, InvalidObjectException, MetaException {
    Connection con = null;
    ;
    PreparedStatement ps = null;
    boolean success = false;

    success = false;
    who = who.toLowerCase();
    db = db.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("revoke auth on db error, who=" + who + ", db=" + db + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("revoke auth on db error, who=" + who + ", db=" + db + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con.prepareStatement("select db_name from router where db_name=?");
      ps.setString(1, db.toLowerCase());
      boolean isDbfind = false;

      ResultSet dbSet = ps.executeQuery();

      while (dbSet.next()) {
        isDbfind = true;
        break;
      }

      dbSet.close();

      if (!isDbfind) {
        LOG.error("revoke auth on db error, who=" + who + ", db=" + db);
        throw new NoSuchObjectException("can not find db:" + db);
      }

      ps = con
          .prepareStatement("select alter_priv, create_priv, createview_priv, "
              + " delete_priv, drop_priv, index_priv, insert_priv, select_priv, showview_priv,"
              + " update_priv from dbpriv where user_name=? and db_name=?");

      ps.setString(1, who);
      ps.setString(2, db);
      boolean isPrivFind = false;

      boolean selPriv = false;
      boolean insertPriv = false;
      boolean createPriv = false;
      boolean dropPriv = false;
      boolean deletePriv = false;
      boolean alterPriv = false;
      boolean updatePriv = false;
      boolean indexPriv = false;
      boolean showViewPriv = false;
      boolean createViewPriv = false;

      ResultSet privSet = ps.executeQuery();

      while (privSet.next()) {
        isPrivFind = true;
        alterPriv = privSet.getBoolean(1);
        createPriv = privSet.getBoolean(2);
        createViewPriv = privSet.getBoolean(3);
        deletePriv = privSet.getBoolean(4);
        dropPriv = privSet.getBoolean(5);
        indexPriv = privSet.getBoolean(6);
        insertPriv = privSet.getBoolean(7);
        selPriv = privSet.getBoolean(8);
        showViewPriv = privSet.getBoolean(9);
        updatePriv = privSet.getBoolean(10);

        break;
      }

      privSet.close();
      ps.close();

      if (!isPrivFind) {
        LOG.error("revoke auth on db error, who=" + who + ", db=" + db);
        throw new NoSuchObjectException("User " + who
            + " does not have privileges on db: " + db);
      }

      if (privileges == null) {
        LOG.error("revoke auth on db error, who=" + who + ", db=" + db);
        throw new InvalidObjectException("No privileges are given!");
      }

      for (String priv : privileges) {
        if (priv.equals("TOK_SELECT_PRI")) {
          selPriv = false;
        } else if (priv.equals("TOK_INSERT_PRI")) {
          insertPriv = false;
        } else if (priv.equals("TOK_CREATE_PRI")) {
          createPriv = false;
        } else if (priv.equals("TOK_DROP_PRI")) {
          dropPriv = false;
        } else if (priv.equals("TOK_DELETE_PRI")) {
          deletePriv = false;
        } else if (priv.equals("TOK_ALTER_PRI")) {
          alterPriv = false;
        } else if (priv.equals("TOK_UPDATE_PRI")) {
          updatePriv = false;
        } else if (priv.equals("TOK_INDEX_PRI")) {
          indexPriv = false;
        } else if (priv.equals("TOK_CREATEVIEW_PRI")) {
          createViewPriv = false;
        } else if (priv.equals("TOK_SHOWVIEW_PRI")) {
          showViewPriv = false;
        } else if (priv.equals("TOK_ALL_PRI")) {
          selPriv = false;
          insertPriv = false;
          createPriv = false;
          dropPriv = false;
          deletePriv = false;
          alterPriv = false;
          updatePriv = false;
          indexPriv = false;
          createViewPriv = false;
          showViewPriv = false;
        } else {
          LOG.error("revoke auth on db error, who=" + who + ", db=" + db);
          throw new InvalidObjectException("Privilege does not exist: " + priv);
        }

      }

      ps = con
          .prepareStatement("update dbpriv set alter_priv=?, create_priv=?, createview_priv=?, "
              + " delete_priv=?, drop_priv=?, index_priv=?, insert_priv=?, select_priv=?, showview_priv=?,"
              + " update_priv=? where user_name=? and db_name=? ");

      ps.setBoolean(1, alterPriv);
      ps.setBoolean(2, createPriv);
      ps.setBoolean(3, createViewPriv);
      ps.setBoolean(4, deletePriv);
      ps.setBoolean(5, dropPriv);
      ps.setBoolean(6, indexPriv);
      ps.setBoolean(7, insertPriv);
      ps.setBoolean(8, selPriv);
      ps.setBoolean(9, showViewPriv);
      ps.setBoolean(10, updatePriv);

      ps.setString(11, who);
      ps.setString(12, db);

      ps.executeUpdate();

      con.commit();
      success = true;
    } catch (SQLException ex) {
      LOG.error("revoke auth on db error, who=" + who + ", db=" + db);
      ex.printStackTrace();
      throw new MetaException(ex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    

    return success;
  }

  @Override
  public boolean dropAuthOnDb(String who, String db) throws MetaException {
    Connection con = null;
    PreparedStatement ps = null;

    boolean success = false;
    who = who.toLowerCase();
    db = db.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("drop auth on db error, who=" + who + ", db=" + db + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("drop auth on db error, who=" + who + ", db=" + db + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con
          .prepareStatement("delete from dbpriv where user_name=? and db_name=?");
      ps.setString(1, who);
      ps.setString(2, db);
      ps.executeUpdate();

      con.commit();
      success = true;
    } catch (SQLException ex) {
      LOG.error("drop auth on db error, who=" + who + ", db=" + db + ", msg="
          + ex.getMessage());
      ex.printStackTrace();
      throw new MetaException(ex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    return success;
  }

  public boolean dropAuthInDbNoDistributeTransaction(String who)
      throws MetaException {
    Connection con = null;
    PreparedStatement ps = null;
    boolean success = false;

    who = who.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("drop auth in db error, who=" + who + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("drop auth in db error, who=" + who + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con.prepareStatement("delete from dbpriv where user_name=?");
      ps.setString(1, who);
      ps.executeUpdate();

      con.commit();
      success = true;
    } catch (SQLException sqlex) {
      LOG.error("drop auth in db error, who=" + who + ", msg="
          + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    

    return success;
  }

  public boolean dropAuthInDbByDistributeTransaction(String who)
      throws MetaException {
    boolean success = false;

    Set<String> slaveURLSet = getAllSegments();
    int size = slaveURLSet.size();

    PGXADataSource[] slaveDSArray = new PGXADataSource[size];
    XAConnection[] slaveDSXaConnArray = new XAConnection[size];
    XAResource[] slaveSaResArray = new XAResource[size];
    Connection[] slaveConArray = new Connection[size];
    Statement[] slaveStmtArray = new Statement[size];
    Xid[] slaveXidArray = new Xid[size];

    who = who.toLowerCase();

    int index = 0;
    for (String slaveURL : slaveURLSet) {
      slaveDSArray[index] = getXADataSource(slaveURL, user, passwd);
      index++;
    }

    int formatID = genFormatID();

    try {
      for (int i = 0; i < size; i++) {
        slaveDSXaConnArray[i] = slaveDSArray[i].getXAConnection();
        slaveSaResArray[i] = slaveDSXaConnArray[i].getXAResource();
        slaveConArray[i] = slaveDSXaConnArray[i].getConnection();
        slaveStmtArray[i] = slaveConArray[i].createStatement();

        byte id1 = (byte) ((i + 2) * 2);
        byte id2 = (byte) (id1 + 1);

        slaveXidArray[i] = new MyXid(formatID, new byte[] { id1 },
            new byte[] { id2 });
      }

      try {
        for (int i = 0; i < size; i++) {
          slaveSaResArray[i].start(slaveXidArray[i], XAResource.TMNOFLAGS);
          slaveStmtArray[i]
              .executeUpdate("delete from dbpriv where user_name='"
                  + who.toLowerCase() + "'");
          slaveSaResArray[i].end(slaveXidArray[i], XAResource.TMSUCCESS);
        }

        boolean isAllPred = true;

        int[] slaveRetArray = new int[size];
        for (int i = 0; i < size; i++) {
          slaveRetArray[i] = slaveSaResArray[i].prepare(slaveXidArray[i]);

          if (slaveRetArray[i] == XAResource.XA_OK) {
            continue;
          } else {
            isAllPred = false;
            break;
          }
        }

        if (isAllPred) {
          for (int i = 0; i < size; i++) {
            slaveSaResArray[i].commit(slaveXidArray[i], false);
          }

          success = true;
        }

      } catch (XAException e) {
        LOG.error("drop auth in db error, who=" + who + ", msg="
            + e.getMessage());
      }
    } catch (SQLException e) {
      LOG.error("drop auth in db error, who=" + who + ", msg=" + e.getMessage());
      throw new MetaException(e.getMessage());
    } finally {
      for (int i = 0; i < size; i++) {
        closeConnection(slaveConArray[i]);
        closeXAConnection(slaveDSXaConnArray[i]);
      }
    }

    return success;
  }

  @Override
  public boolean dropAuthInDb(String who) throws MetaException {
    boolean useDistributeTran = false;
    if (!useDistributeTran) {
      return dropAuthInDbNoDistributeTransaction(who);
    } else {
      return dropAuthInDbByDistributeTransaction(who);
    }
  }

  public class TblPrivDesc {
    public boolean selPriv = false;
    public boolean insertPriv = false;
    public boolean createPriv = false;
    public boolean dropPriv = false;
    public boolean deletePriv = false;
    public boolean alterPriv = false;
    public boolean updatePriv = false;
    public boolean indexPriv = false;
  }

  @Override
  public boolean grantAuthOnTbl(String forWho, List<String> privileges,
      String db, String tbl) throws NoSuchObjectException,
      InvalidObjectException, MetaException {
    Connection con = null;
    ;
    Statement ps = null;
    boolean success = false;
    PreparedStatement pss = null;

    success = false;
    forWho = forWho.toLowerCase();
    db = db.toLowerCase();
    tbl = tbl.toLowerCase();

    if (privileges == null) {
      throw new InvalidObjectException("No privileges are given!");
    }

    try {
      con = getSegmentConnection(db);
    } catch (MetaStoreConnectException e1) {
      LOG.error("grant auth on db error, forWho=" + forWho + ", db=" + db
          + ", tbl=" + tbl + ", msg=" + e1.getMessage());
      throw new MetaException("can not find db:" + db);
    } catch (SQLException e1) {
      LOG.error("grant auth on db error, forWho=" + forWho + ", db=" + db
          + ", tbl=" + tbl + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    Set<String> tblNames = new HashSet<String>();

    try {
      ps = con.createStatement();
      String sql = null;
      StringBuilder sb = new StringBuilder();
      int size = tbl.length();
      for (int i = 0; i < size; i++) {
        if (tbl.charAt(i) != '\'') {
          sb.append(tbl.charAt(i));
        }
      }

      tbl = sb.toString();

      if (tbl == null || tbl.isEmpty() || tbl.equals(".*") || tbl.equals("*")) {
        sql = "select tbl_name from tbls" + " where  tbls.db_name='"
            + db.toLowerCase() + "'";
      } else {
        tbl = tbl.replace('*', '%');

        sql = "select tbl_name from tbls" + " where  tbls.db_name='" + db
            + "' and tbls.tbl_name like '" + tbl + "'";
      }

      LOG.debug("SQL is " + sql);

      ResultSet tblSet = ps.executeQuery(sql);
      while (tblSet.next()) {
        String tblName = tblSet.getString(1);
        tblNames.add(tblName);
      }

      tblSet.close();

      con.commit();
      success = true;
    } catch (SQLException ex) {
      ex.printStackTrace();
      LOG.error("grant auth on db error, forWho=" + forWho + ", db=" + db
          + ", tbl=" + tbl + ", msg=" + ex.getMessage());
      LOG.error(ex.getMessage());
    } finally {
      
      closeStatement(ps);
      closeConnection(con);
    }

    if (tblNames.isEmpty()) {
      throw new NoSuchObjectException("Table does not exist: " + tbl
          + " in db: " + db);
    }

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("grant auth on db error, forWho=" + forWho + ", db=" + db
          + ", tbl=" + tbl + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("grant auth on db error, forWho=" + forWho + ", db=" + db
          + ", tbl=" + tbl + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      ps = con.createStatement();
      String sql = null;
      StringBuilder sb = new StringBuilder();
      int size = tbl.length();
      for (int i = 0; i < size; i++) {
        if (tbl.charAt(i) != '\'') {
          sb.append(tbl.charAt(i));
        }
      }

      tbl = sb.toString();

      if (tbl == null || tbl.isEmpty() || tbl.equals(".*") || tbl.equals("*")) {
        sql = "select tbl_name, alter_priv, create_priv, delete_priv "
            + ",drop_priv, index_priv, insert_priv, select_priv, "
            + " update_priv from tblpriv" + " where db_name='"
            + db.toLowerCase() + "' and user_name='" + forWho + "'";
      } else {
        tbl = tbl.replace('*', '%');

        sql = "select tbl_name, alter_priv, create_priv, delete_priv "
            + ",drop_priv, index_priv, insert_priv, select_priv, "
            + " update_priv from tblpriv" + " where db_name='" + db
            + "' and tbl_name like '" + tbl + "'" + " and user_name='" + forWho
            + "'";
      }

      LOG.debug("SQL is " + sql);

      ResultSet tblSet = ps.executeQuery(sql);

      Map<String, TblPrivDesc> tblPrivMap = new HashMap<String, TblPrivDesc>();

      while (tblSet.next()) {
        String tblName = tblSet.getString(1);

        TblPrivDesc privDesc = new TblPrivDesc();
        privDesc.alterPriv = tblSet.getBoolean(2);
        privDesc.createPriv = tblSet.getBoolean(3);
        privDesc.deletePriv = tblSet.getBoolean(4);
        privDesc.dropPriv = tblSet.getBoolean(5);
        privDesc.indexPriv = tblSet.getBoolean(6);
        privDesc.insertPriv = tblSet.getBoolean(7);
        privDesc.selPriv = tblSet.getBoolean(8);
        privDesc.updatePriv = tblSet.getBoolean(9);

        tblPrivMap.put(tblName, privDesc);
      }

      tblSet.close();

      boolean selPriv = false;
      boolean insertPriv = false;
      boolean createPriv = false;
      boolean dropPriv = false;
      boolean deletePriv = false;
      boolean alterPriv = false;
      boolean updatePriv = false;
      boolean indexPriv = false;

      for (String priv : privileges) {
        if (priv.equals("TOK_SELECT_PRI")) {
          selPriv = true;
        } else if (priv.equals("TOK_INSERT_PRI")) {
          insertPriv = true;
        } else if (priv.equals("TOK_CREATE_PRI")) {
          createPriv = true;
        } else if (priv.equals("TOK_DROP_PRI")) {
          dropPriv = true;
        } else if (priv.equals("TOK_DELETE_PRI")) {
          deletePriv = true;
        } else if (priv.equals("TOK_ALTER_PRI")) {
          alterPriv = true;
        } else if (priv.equals("TOK_UPDATE_PRI")) {
          updatePriv = true;
        } else if (priv.equals("TOK_INDEX_PRI")) {
          indexPriv = true;
        } else if (priv.equals("TOK_ALL_PRI")) {
          selPriv = true;
          insertPriv = true;
          createPriv = true;
          dropPriv = true;
          deletePriv = true;
          alterPriv = true;
          updatePriv = true;
          indexPriv = true;
        } else {
          throw new InvalidObjectException("Privilege does not exist: " + priv);
        }
      }

      if (!tblPrivMap.isEmpty()) {
        Collection<TblPrivDesc> tblPrivColl = tblPrivMap.values();
        if (alterPriv) {
          for (TblPrivDesc entry : tblPrivColl) {
            entry.alterPriv = true;
          }
        }

        if (createPriv) {
          for (TblPrivDesc entry : tblPrivColl) {
            entry.createPriv = true;
          }
        }

        if (deletePriv) {
          for (TblPrivDesc entry : tblPrivColl) {
            entry.deletePriv = true;
          }
        }

        if (dropPriv) {
          for (TblPrivDesc entry : tblPrivColl) {
            entry.dropPriv = true;
          }
        }

        if (indexPriv) {
          for (TblPrivDesc entry : tblPrivColl) {
            entry.indexPriv = true;
          }
        }

        if (insertPriv) {
          for (TblPrivDesc entry : tblPrivColl) {
            entry.insertPriv = true;
          }
        }

        if (selPriv) {
          for (TblPrivDesc entry : tblPrivColl) {
            entry.selPriv = true;
          }
        }

        if (updatePriv) {
          for (TblPrivDesc entry : tblPrivColl) {
            entry.updatePriv = true;
          }
        }

        pss = con
            .prepareStatement("update tblpriv set alter_priv=?, create_priv=?,  "
                + " delete_priv=?, drop_priv=?, index_priv=?, insert_priv=?, select_priv=?,"
                + " update_priv=? where user_name=? and db_name=? and tbl_name=? ");

        for (Entry<String, TblPrivDesc> entry : tblPrivMap.entrySet()) {

          pss.setBoolean(1, entry.getValue().alterPriv);
          pss.setBoolean(2, entry.getValue().createPriv);

          pss.setBoolean(3, entry.getValue().deletePriv);
          pss.setBoolean(4, entry.getValue().dropPriv);
          pss.setBoolean(5, entry.getValue().indexPriv);
          pss.setBoolean(6, entry.getValue().insertPriv);
          pss.setBoolean(7, entry.getValue().selPriv);

          pss.setBoolean(8, entry.getValue().updatePriv);

          pss.setString(9, forWho);
          pss.setString(10, db);
          pss.setString(11, entry.getKey());

          pss.addBatch();
        }

        pss.executeBatch();
      }

      pss = con
          .prepareStatement("insert into tblpriv(alter_priv, create_priv,"
              + "delete_priv, drop_priv, index_priv, insert_priv, select_priv,"
              + " update_priv, user_name, db_name, tbl_name) values(?,?,?,?,?,?,?,?,?,?,?)");
      int needInsertCount = 0;

      for (String tblName : tblNames) {
        if (!tblPrivMap.containsKey(tblName)) {
          pss.setBoolean(1, alterPriv);
          pss.setBoolean(2, createPriv);
          pss.setBoolean(3, deletePriv);
          pss.setBoolean(4, dropPriv);
          pss.setBoolean(5, indexPriv);
          pss.setBoolean(6, insertPriv);
          pss.setBoolean(7, selPriv);

          pss.setBoolean(8, updatePriv);
          pss.setString(9, forWho);
          pss.setString(10, db);
          pss.setString(11, tblName);

          pss.addBatch();

          needInsertCount++;
        }
      }

      if (needInsertCount > 0) {
        pss.executeBatch();
      }

      con.commit();
      success = true;
    } catch (SQLException ex) {
      ex.printStackTrace();
      LOG.error("grant auth on db error, forWho=" + forWho + ", db=" + db
          + ", tbl=" + tbl + ", msg=" + ex.getMessage());
      LOG.error(ex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }
      closeStatement(ps);
      closeStatement(pss);
      closeConnection(con);
    }

    

    return success;
  }

  @Override
  public TblPriv getAuthOnTbl(String who, String db, String tbl)
      throws MetaException {
    boolean success = false;

    Connection con;
    Statement ps = null;
    TblPriv tblPriv = null;

    who = who.toLowerCase();
    db = db.toLowerCase();
    tbl = tbl.toLowerCase();

    try {
      con = getSegmentConnection(db);
    } catch (MetaStoreConnectException e1) {
      LOG.error("get user table auth error, db=" + db + ", tbl=" + tbl
          + ", who=" + who + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get user table auth error, db=" + db + ", tbl=" + tbl
          + ", who=" + who + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = "select alter_priv, create_priv, delete_priv "
          + ",drop_priv, index_priv, insert_priv, select_priv, update_priv"
          + " from tblpriv where user_name='" + who + "' and db_name='" + db
          + "' and tbl_name='" + tbl + "'";

      ResultSet tblSet = ps.executeQuery(sql);

      while (tblSet.next()) {
        tblPriv = new TblPriv();
        tblPriv.setAlterPriv(tblSet.getBoolean(1));
        tblPriv.setCreatePriv(tblSet.getBoolean(2));
        tblPriv.setDeletePriv(tblSet.getBoolean(3));
        tblPriv.setDropPriv(tblSet.getBoolean(4));
        tblPriv.setIndexPriv(tblSet.getBoolean(5));
        tblPriv.setInsertPriv(tblSet.getBoolean(6));
        tblPriv.setSelectPriv(tblSet.getBoolean(7));
        tblPriv.setUpdatePriv(tblSet.getBoolean(8));
        tblPriv.setDb(db);
        tblPriv.setTbl(tbl);
        tblPriv.setUser(who);

        break;
      }

      con.commit();
      success = true;
    } catch (SQLException sqlex) {
      LOG.error("get user table auth error, db=" + db + ", tbl=" + tbl
          + ", who=" + who + ", msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    return tblPriv;
  }

  @Override
  public List<TblPriv> getTblAuth(String db, String tbl) throws MetaException {
    boolean success = false;

    Connection con;
    Statement ps = null;
    List<TblPriv> tblPrivs = new ArrayList<TblPriv>();
    db = db.toLowerCase();
    tbl = tbl.toLowerCase();

    try {
      con = getSegmentConnection(db);
    } catch (MetaStoreConnectException e1) {
      LOG.error("get table auth error, db=" + db + ", tbl=" + tbl + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get table auth error, db=" + db + ", tbl=" + tbl + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = "select alter_priv, create_priv, delete_priv "
          + ",drop_priv, index_priv, insert_priv, select_priv, update_priv, user_name"
          + " from tblpriv where db_name='" + db + "' and tbl_name='" + tbl
          + "'";

      ResultSet tblSet = ps.executeQuery(sql);

      while (tblSet.next()) {
        TblPriv tblPriv = new TblPriv();

        tblPriv.setAlterPriv(tblSet.getBoolean(1));
        tblPriv.setCreatePriv(tblSet.getBoolean(2));
        tblPriv.setDeletePriv(tblSet.getBoolean(3));
        tblPriv.setDropPriv(tblSet.getBoolean(4));
        tblPriv.setIndexPriv(tblSet.getBoolean(5));
        tblPriv.setInsertPriv(tblSet.getBoolean(6));
        tblPriv.setSelectPriv(tblSet.getBoolean(7));
        tblPriv.setUpdatePriv(tblSet.getBoolean(8));
        tblPriv.setDb(db);
        tblPriv.setTbl(tbl);
        tblPriv.setUser(tblSet.getString(9));

        tblPrivs.add(tblPriv);
      }

      success = true;
    } catch (SQLException sqlex) {
      LOG.error("get table auth error, db=" + db + ", tbl=" + tbl + ", msg="
          + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    if (success) {
      return tblPrivs;
    } else {
      return null;
    }
  }

  public List<TblPriv> getAuthOnTblsNoDistributeTransaction(String who)
      throws MetaException {
    Connection con = null;
    Statement ps = null;
    boolean success = false;
    List<TblPriv> tblPrivs = new ArrayList<TblPriv>();

    who = who.toLowerCase();

    success = false;
    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("get user auth on tbls error, user=" + who + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get user auth on tbls error, user=" + who + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = "select alter_priv, create_priv, delete_priv "
          + ",drop_priv, index_priv, insert_priv, select_priv, update_priv, db_name, tbl_name"
          + " from tblpriv where user_name='" + who
          + "' order by db_name asc, tbl_name asc";

      ResultSet tblPrivSet = ps.executeQuery(sql);
      while (tblPrivSet.next()) {
        TblPriv tblPriv = new TblPriv();
        tblPriv.setAlterPriv(tblPrivSet.getBoolean(1));
        tblPriv.setCreatePriv(tblPrivSet.getBoolean(2));
        tblPriv.setDeletePriv(tblPrivSet.getBoolean(3));
        tblPriv.setDropPriv(tblPrivSet.getBoolean(4));
        tblPriv.setIndexPriv(tblPrivSet.getBoolean(5));
        tblPriv.setInsertPriv(tblPrivSet.getBoolean(6));
        tblPriv.setSelectPriv(tblPrivSet.getBoolean(7));
        tblPriv.setUpdatePriv(tblPrivSet.getBoolean(8));
        tblPriv.setDb(tblPrivSet.getString(9));
        tblPriv.setTbl(tblPrivSet.getString(10));
        tblPriv.setUser(who);


        tblPrivs.add(tblPriv);
      }

      success = true;
    } catch (SQLException sqlex) {
      LOG.error("get user auth on tbls error, user=" + who + ", msg="
          + sqlex.getMessage());
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    if (success) {
      return tblPrivs;
    } else {
      return null;
    }
  }

  public List<TblPriv> getAuthOnTblsByDistributeTransaction(String who)
      throws MetaException {
    boolean success = false;

    List<TblPriv> tblPrivs = new ArrayList<TblPriv>();

    Set<String> slaveURLSet = getAllSegments();
    int size = slaveURLSet.size();

    PGXADataSource[] slaveDSArray = new PGXADataSource[size];
    XAConnection[] slaveDSXaConnArray = new XAConnection[size];
    XAResource[] slaveSaResArray = new XAResource[size];
    Connection[] slaveConArray = new Connection[size];
    Statement[] slaveStmtArray = new Statement[size];
    Xid[] slaveXidArray = new Xid[size];

    who = who.toLowerCase();

    int index = 0;
    for (String slaveURL : slaveURLSet) {
      slaveDSArray[index] = getXADataSource(slaveURL, user, passwd);
      index++;
    }

    int formatID = genFormatID();

    try {
      for (int i = 0; i < size; i++) {
        slaveDSXaConnArray[i] = slaveDSArray[i].getXAConnection();
        slaveSaResArray[i] = slaveDSXaConnArray[i].getXAResource();
        slaveConArray[i] = slaveDSXaConnArray[i].getConnection();
        slaveStmtArray[i] = slaveConArray[i].createStatement();

        byte id1 = (byte) ((i + 2) * 2);
        byte id2 = (byte) (id1 + 1);

        slaveXidArray[i] = new MyXid(formatID, new byte[] { id1 },
            new byte[] { id2 });
      }

      try {
        for (int i = 0; i < size; i++) {
          slaveSaResArray[i].start(slaveXidArray[i], XAResource.TMNOFLAGS);

          ResultSet tblPrivSet = slaveStmtArray[i]
              .executeQuery("select alter_priv, create_priv, delete_priv "
                  + ",drop_priv, index_priv, insert_priv, select_priv, update_priv, db_name, tbl_name"
                  + " from tblpriv where user_name='" + who.toLowerCase() + "'");

          while (tblPrivSet.next()) {
            TblPriv tblPriv = new TblPriv();

            tblPriv.setAlterPriv(tblPrivSet.getBoolean(1));
            tblPriv.setCreatePriv(tblPrivSet.getBoolean(2));
            tblPriv.setDeletePriv(tblPrivSet.getBoolean(3));
            tblPriv.setDropPriv(tblPrivSet.getBoolean(4));
            tblPriv.setIndexPriv(tblPrivSet.getBoolean(5));
            tblPriv.setInsertPriv(tblPrivSet.getBoolean(6));
            tblPriv.setSelectPriv(tblPrivSet.getBoolean(7));
            tblPriv.setUpdatePriv(tblPrivSet.getBoolean(8));
            tblPriv.setDb(tblPrivSet.getString(9));
            tblPriv.setTbl(tblPrivSet.getString(10));
            tblPriv.setUser(who);


            tblPrivs.add(tblPriv);
          }
          slaveSaResArray[i].end(slaveXidArray[i], XAResource.TMSUCCESS);
        }

        boolean isAllPred = true;

        int[] slaveRetArray = new int[size];
        for (int i = 0; i < size; i++) {
          slaveRetArray[i] = slaveSaResArray[i].prepare(slaveXidArray[i]);

          if (slaveRetArray[i] == XAResource.XA_OK) {
            continue;
          } else {
            isAllPred = false;
            break;
          }
        }

        if (isAllPred) {
          for (int i = 0; i < size; i++) {
            slaveSaResArray[i].commit(slaveXidArray[i], false);
          }

          success = true;
        }

      } catch (XAException e) {
        LOG.error("get user auth on tbls error, user=" + who + ", msg="
            + e.getMessage());
        throw new MetaException(e.getMessage());
      }
    } catch (SQLException e) {
      LOG.error("get user auth on tbls error, user=" + who + ", msg="
          + e.getMessage());
      throw new MetaException(e.getMessage());
    } finally {
      for (int i = 0; i < size; i++) {
        closeConnection(slaveConArray[i]);
        closeXAConnection(slaveDSXaConnArray[i]);
      }
    }

    if (success) {
      return tblPrivs;
    } else {
      return null;
    }
  }

  @Override
  public List<TblPriv> getAuthOnTbls(String who) throws MetaException {
    boolean useDistributeTran = false;
    if (!useDistributeTran) {
      return getAuthOnTblsNoDistributeTransaction(who);
    } else {
      return getAuthOnTblsByDistributeTransaction(who);
    }
  }

  public List<TblPriv> getAuthOnAllTblsNoDistributeTransaction()
      throws MetaException {
    Connection con = null;
    Statement ps = null;
    boolean success = false;
    List<TblPriv> tblPrivs = new ArrayList<TblPriv>();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("get user auth on all tbls error, msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get user auth on all tbls error, msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = "select alter_priv, create_priv, delete_priv "
          + ",drop_priv, index_priv, insert_priv, select_priv, update_priv, user_name, db_name, tbl_name"
          + " from tblpriv";

      ResultSet tblPrivSet = ps.executeQuery(sql);
      while (tblPrivSet.next()) {
        TblPriv tblPriv = new TblPriv();

        tblPriv.setAlterPriv(tblPrivSet.getBoolean(1));
        tblPriv.setCreatePriv(tblPrivSet.getBoolean(2));
        tblPriv.setDeletePriv(tblPrivSet.getBoolean(3));
        tblPriv.setDropPriv(tblPrivSet.getBoolean(4));
        tblPriv.setIndexPriv(tblPrivSet.getBoolean(5));
        tblPriv.setInsertPriv(tblPrivSet.getBoolean(6));
        tblPriv.setSelectPriv(tblPrivSet.getBoolean(7));
        tblPriv.setUpdatePriv(tblPrivSet.getBoolean(8));
        tblPriv.setUser(tblPrivSet.getString(9));
        tblPriv.setDb(tblPrivSet.getString(10));
        tblPriv.setTbl(tblPrivSet.getString(11));



        tblPrivs.add(tblPriv);
      }

      success = true;
    } catch (SQLException sqlex) {
      LOG.error("get user auth on all tbls error, msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    if (success) {
      return tblPrivs;
    } else {
      return null;
    }

    
  }

  public List<TblPriv> getAuthOnAllTblsByDistributeTransaction()
      throws MetaException {
    boolean success = false;

    List<TblPriv> tblPrivs = new ArrayList<TblPriv>();

    Set<String> slaveURLSet = getAllSegments();
    int size = slaveURLSet.size();

    PGXADataSource[] slaveDSArray = new PGXADataSource[size];
    XAConnection[] slaveDSXaConnArray = new XAConnection[size];
    XAResource[] slaveSaResArray = new XAResource[size];
    Connection[] slaveConArray = new Connection[size];
    Statement[] slaveStmtArray = new Statement[size];
    Xid[] slaveXidArray = new Xid[size];

    int index = 0;
    for (String slaveURL : slaveURLSet) {
      slaveDSArray[index] = getXADataSource(slaveURL, user, passwd);
      index++;
    }

    int formatID = genFormatID();

    try {
      for (int i = 0; i < size; i++) {
        slaveDSXaConnArray[i] = slaveDSArray[i].getXAConnection();
        slaveSaResArray[i] = slaveDSXaConnArray[i].getXAResource();
        slaveConArray[i] = slaveDSXaConnArray[i].getConnection();
        slaveStmtArray[i] = slaveConArray[i].createStatement();

        byte id1 = (byte) ((i + 2) * 2);
        byte id2 = (byte) (id1 + 1);

        slaveXidArray[i] = new MyXid(formatID, new byte[] { id1 },
            new byte[] { id2 });
      }

      try {
        for (int i = 0; i < size; i++) {
          slaveSaResArray[i].start(slaveXidArray[i], XAResource.TMNOFLAGS);

          ResultSet tblPrivSet = slaveStmtArray[i]
              .executeQuery("select alter_priv, create_priv, delete_priv "
                  + ",drop_priv, index_priv, insert_priv, select_priv, update_priv, user_name, db_name, tbl_name"
                  + " from tblpriv");

          while (tblPrivSet.next()) {
            TblPriv tblPriv = new TblPriv();

            tblPriv.setAlterPriv(tblPrivSet.getBoolean(1));
            tblPriv.setCreatePriv(tblPrivSet.getBoolean(2));
            tblPriv.setDeletePriv(tblPrivSet.getBoolean(3));
            tblPriv.setDropPriv(tblPrivSet.getBoolean(4));
            tblPriv.setIndexPriv(tblPrivSet.getBoolean(5));
            tblPriv.setInsertPriv(tblPrivSet.getBoolean(6));
            tblPriv.setSelectPriv(tblPrivSet.getBoolean(7));
            tblPriv.setUpdatePriv(tblPrivSet.getBoolean(8));
            tblPriv.setUser(tblPrivSet.getString(9));
            tblPriv.setDb(tblPrivSet.getString(10));
            tblPriv.setTbl(tblPrivSet.getString(11));


            tblPrivs.add(tblPriv);
          }
          slaveSaResArray[i].end(slaveXidArray[i], XAResource.TMSUCCESS);
        }

        boolean isAllPred = true;

        int[] slaveRetArray = new int[size];
        for (int i = 0; i < size; i++) {
          slaveRetArray[i] = slaveSaResArray[i].prepare(slaveXidArray[i]);

          if (slaveRetArray[i] == XAResource.XA_OK) {
            continue;
          } else {
            isAllPred = false;
            break;
          }
        }

        if (isAllPred) {
          for (int i = 0; i < size; i++) {
            slaveSaResArray[i].commit(slaveXidArray[i], false);
          }

          success = true;
        }

      } catch (XAException e) {
        LOG.error("get user auth on all tbls error, msg=" + e.getMessage());
        throw new MetaException(e.getMessage());
      }
    } catch (SQLException e) {
      LOG.error("get user auth on all tbls error, msg=" + e.getMessage());
      throw new MetaException(e.getMessage());
    } finally {
      for (int i = 0; i < size; i++) {
        closeConnection(slaveConArray[i]);
        closeXAConnection(slaveDSXaConnArray[i]);
      }
    }

    if (success) {
      return tblPrivs;
    } else {
      return null;
    }
  }

  @Override
  public List<TblPriv> getTblAuthAll() throws MetaException {
    boolean useDistributeTran = false;
    if (!useDistributeTran) {
      return getAuthOnAllTblsNoDistributeTransaction();
    } else {
      return getAuthOnAllTblsByDistributeTransaction();
    }
  }

  @Override
  public boolean revokeAuthOnTbl(String who, List<String> privileges,
      String db, String tbl) throws NoSuchObjectException,
      InvalidObjectException, MetaException {
    Connection con = null;
    ;
    Statement ps = null;
    boolean success = false;
    PreparedStatement pss = null;

    who = who.toLowerCase();
    db = db.toLowerCase();
    tbl = tbl.toLowerCase();

    if (privileges == null) {
      throw new InvalidObjectException("No privileges are given!");
    }

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("revoke auth on tbl error, who=" + who + ", db=" + db
          + ", tbl=" + tbl + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("revoke auth on tbl error, who=" + who + ", db=" + db
          + ", tbl=" + tbl + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      Map<String, TblPrivDesc> tblPrivMap = new HashMap<String, TblPrivDesc>();
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con.createStatement();
      String sql = null;
      StringBuilder sb = new StringBuilder();
      int size = tbl.length();
      for (int i = 0; i < size; i++) {
        if (tbl.charAt(i) != '\'') {
          sb.append(tbl.charAt(i));
        }
      }

      tbl = sb.toString();

      tbl = tbl.replace('*', '%');

      if (tbl == null || tbl.isEmpty() || tbl.equals(".*") || tbl.equals("*")) {
        sql = "select alter_priv, create_priv, delete_priv "
            + ",drop_priv, index_priv, insert_priv, select_priv, update_priv, tbl_name"
            + " from tblpriv where user_name='" + who + "' and db_name='" + db
            + "'";
      } else {
        sql = "select alter_priv, create_priv, delete_priv "
            + ",drop_priv, index_priv, insert_priv, select_priv, update_priv, tbl_name"
            + " from tblpriv where user_name='" + who + "' and db_name='" + db
            + "' and tbl_name like '" + tbl + "'";
      }

      ResultSet privSet = ps.executeQuery(sql);

      while (privSet.next()) {
        TblPrivDesc privDesc = new TblPrivDesc();

        privDesc.alterPriv = privSet.getBoolean(1);
        privDesc.createPriv = privSet.getBoolean(2);
        privDesc.deletePriv = privSet.getBoolean(3);
        privDesc.dropPriv = privSet.getBoolean(4);
        privDesc.indexPriv = privSet.getBoolean(5);
        privDesc.insertPriv = privSet.getBoolean(6);
        privDesc.selPriv = privSet.getBoolean(7);
        privDesc.updatePriv = privSet.getBoolean(8);
        String tblName = privSet.getString(9);

        tblPrivMap.put(tblName, privDesc);
      }

      privSet.close();

      if (tblPrivMap.isEmpty()) {
        LOG.error("revoke auth on tbl error, who=" + who + ", db=" + db
            + ", tbl=" + tbl);
        throw new NoSuchObjectException("User " + who
            + " does not have privileges on table: " + tbl + " in db: " + db);
      }

      boolean selPriv = true;
      boolean insertPriv = true;
      boolean createPriv = true;
      boolean dropPriv = true;
      boolean deletePriv = true;
      boolean alterPriv = true;
      boolean updatePriv = true;
      boolean indexPriv = true;

      for (String priv : privileges) {
        if (priv.equals("TOK_SELECT_PRI")) {
          selPriv = false;
        } else if (priv.equals("TOK_INSERT_PRI")) {
          insertPriv = false;
        } else if (priv.equals("TOK_CREATE_PRI")) {
          createPriv = false;
        } else if (priv.equals("TOK_DROP_PRI")) {
          dropPriv = false;
        } else if (priv.equals("TOK_DELETE_PRI")) {
          deletePriv = false;
        } else if (priv.equals("TOK_ALTER_PRI")) {
          alterPriv = false;
        } else if (priv.equals("TOK_UPDATE_PRI")) {
          updatePriv = false;
        } else if (priv.equals("TOK_INDEX_PRI")) {
          indexPriv = false;
        }

        else if (priv.equals("TOK_ALL_PRI")) {
          selPriv = false;
          insertPriv = false;
          createPriv = false;
          dropPriv = false;
          deletePriv = false;
          alterPriv = false;
          updatePriv = false;
          indexPriv = false;
        } else {
          throw new InvalidObjectException("Privilege does not exist: " + priv);
        }
      }

      Collection<TblPrivDesc> tblPrivColl = tblPrivMap.values();
      if (!alterPriv) {
        for (TblPrivDesc entry : tblPrivColl) {
          entry.alterPriv = false;
        }
      }

      if (!createPriv) {
        for (TblPrivDesc entry : tblPrivColl) {
          entry.createPriv = false;
        }
      }

      if (!deletePriv) {
        for (TblPrivDesc entry : tblPrivColl) {
          entry.deletePriv = false;
        }
      }

      if (!dropPriv) {
        for (TblPrivDesc entry : tblPrivColl) {
          entry.dropPriv = false;
        }
      }

      if (!indexPriv) {
        for (TblPrivDesc entry : tblPrivColl) {
          entry.indexPriv = false;
        }
      }

      if (!insertPriv) {
        for (TblPrivDesc entry : tblPrivColl) {
          entry.insertPriv = false;
        }
      }

      if (!selPriv) {
        for (TblPrivDesc entry : tblPrivColl) {
          entry.selPriv = false;
        }
      }

      if (!updatePriv) {
        for (TblPrivDesc entry : tblPrivColl) {
          entry.updatePriv = false;
        }
      }

      pss = con
          .prepareStatement("update tblpriv set alter_priv=?, create_priv=?,  "
              + " delete_priv=?, drop_priv=?, index_priv=?, insert_priv=?, select_priv=?,"
              + " update_priv=? where user_name=? and db_name=? and tbl_name=?");

      for (Entry<String, TblPrivDesc> entry : tblPrivMap.entrySet()) {

        pss.setBoolean(1, entry.getValue().alterPriv);
        pss.setBoolean(2, entry.getValue().createPriv);

        pss.setBoolean(3, entry.getValue().deletePriv);
        pss.setBoolean(4, entry.getValue().dropPriv);
        pss.setBoolean(5, entry.getValue().indexPriv);
        pss.setBoolean(6, entry.getValue().insertPriv);
        pss.setBoolean(7, entry.getValue().selPriv);

        pss.setBoolean(8, entry.getValue().updatePriv);
        pss.setString(9, who);
        pss.setString(10, db);
        pss.setString(11, entry.getKey());

        pss.addBatch();
      }

      pss.executeBatch();

      con.commit();
      success = true;
    } catch (SQLException ex) {
      LOG.error("revoke auth on tbl error, who=" + who + ", db=" + db
          + ", tbl=" + tbl + ", msg=" + ex.getMessage());
      ex.printStackTrace();
      throw new MetaException(ex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeStatement(pss);
      closeConnection(con);
    }

    

    return success;
  }

  @Override
  public boolean dropAuthOnTbl(String who, String db, String tbl)
      throws MetaException {
    Connection con = null;
    ;
    Statement ps = null;
    boolean success = false;

    who = who.toLowerCase();
    db = db.toLowerCase();
    tbl = tbl.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("drop auth on tbl error, who=" + who + ", db=" + db + ", tbl="
          + tbl + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("drop auth on tbl error, who=" + who + ", db=" + db + ", tbl="
          + tbl + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      String sql = "delete from tblpriv where user_name='" + who
          + "' and db_name='" + db + "' and tbl_name='" + tbl + "'";

      ps = con.createStatement();

      ps.executeUpdate(sql);

      con.commit();
      success = true;
    } catch (SQLException sqlex) {
      LOG.error("drop auth on tbl error, who=" + who + ", db=" + db + ", tbl="
          + tbl + ", msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    return success;
  }

  public boolean dropAuthInTblNoDistributeTransaction(String who)
      throws MetaException {
    Connection con = null;
    Statement ps = null;
    boolean success = false;
    who = who.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("drop auth in tbl error, who=" + who + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("drop auth in tbl error, who=" + who + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      String sql = "delete from tblpriv where user_name='" + who + "'";
      ps = con.createStatement();
      ps.executeUpdate(sql);

      con.commit();
      success = true;
    } catch (SQLException sqlex) {
      LOG.error("drop auth in tbl error, who=" + who + ", msg="
          + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }
    

    return success;
  }

  public boolean dropAuthInTblByDistributeTransaction(String who)
      throws MetaException {
    boolean success = false;

    Set<String> slaveURLSet = getAllSegments();
    int size = slaveURLSet.size();

    PGXADataSource[] slaveDSArray = new PGXADataSource[size];
    XAConnection[] slaveDSXaConnArray = new XAConnection[size];
    XAResource[] slaveSaResArray = new XAResource[size];
    Connection[] slaveConArray = new Connection[size];
    Statement[] slaveStmtArray = new Statement[size];
    Xid[] slaveXidArray = new Xid[size];

    who = who.toLowerCase();

    int index = 0;
    for (String slaveURL : slaveURLSet) {
      slaveDSArray[index] = getXADataSource(slaveURL, user, passwd);
      index++;
    }

    int formatID = genFormatID();

    try {
      for (int i = 0; i < size; i++) {
        slaveDSXaConnArray[i] = slaveDSArray[i].getXAConnection();
        slaveSaResArray[i] = slaveDSXaConnArray[i].getXAResource();
        slaveConArray[i] = slaveDSXaConnArray[i].getConnection();
        slaveStmtArray[i] = slaveConArray[i].createStatement();

        byte id1 = (byte) ((i + 2) * 2);
        byte id2 = (byte) (id1 + 1);

        slaveXidArray[i] = new MyXid(formatID, new byte[] { id1 },
            new byte[] { id2 });
      }

      try {
        for (int i = 0; i < size; i++) {
          slaveSaResArray[i].start(slaveXidArray[i], XAResource.TMNOFLAGS);
          slaveStmtArray[i]
              .executeUpdate("delete from tblpriv where user_name='"
                  + who.toLowerCase() + "'");
          slaveSaResArray[i].end(slaveXidArray[i], XAResource.TMSUCCESS);
        }

        boolean isAllPred = true;

        int[] slaveRetArray = new int[size];
        for (int i = 0; i < size; i++) {
          slaveRetArray[i] = slaveSaResArray[i].prepare(slaveXidArray[i]);

          if (slaveRetArray[i] == XAResource.XA_OK) {
            continue;
          } else {
            isAllPred = false;
            break;
          }
        }

        if (isAllPred) {
          for (int i = 0; i < size; i++) {
            slaveSaResArray[i].commit(slaveXidArray[i], false);
          }

          success = true;
        }

      } catch (XAException e) {
        LOG.error("drop auth in tbl error, who=" + who + ", msg="
            + e.getMessage());
      }
    } catch (SQLException e) {
      LOG.error("drop auth in tbl error, who=" + who + ", msg="
          + e.getMessage());
      throw new MetaException(e.getMessage());
    } finally {
      for (int i = 0; i < size; i++) {
        closeConnection(slaveConArray[i]);
        closeXAConnection(slaveDSXaConnArray[i]);
      }
    }

    return success;
  }

  @Override
  public boolean dropAuthInTbl(String who) throws MetaException {
    boolean useDistributeTran = false;
    if (!useDistributeTran) {
      return dropAuthInTblNoDistributeTransaction(who);
    } else {
      return dropAuthInTblByDistributeTransaction(who);
    }
  }

  @Override
  public boolean createIndex(IndexItem index) throws MetaException {
    return false;
  }

  @Override
  public boolean dropIndex(String db, String table, String name)
      throws MetaException {
    return false;
  }

  @Override
  public int getIndexNum(String db, String table) throws MetaException {
    return 0;
  }

  @Override
  public int getIndexType(String db, String table, String name)
      throws MetaException {
    return 0;
  }

  @Override
  public String getIndexField(String db, String table, String name)
      throws MetaException {
    return null;
  }

  @Override
  public String getIndexLocation(String db, String table, String name)
      throws MetaException {
    return null;
  }

  @Override
  public boolean setIndexLocation(String db, String table, String name,
      String location) throws MetaException {
    return false;
  }

  @Override
  public boolean setIndexStatus(String db, String table, String name, int status)
      throws MetaException {
    return false;
  }

  @Override
  public List<IndexItem> getAllIndexTable(String db, String table)
      throws MetaException {
    return null;
  }

  @Override
  public IndexItem getIndexInfo(String db, String table, String name)
      throws MetaException {
    return null;
  }

  @Override
  public List<IndexItem> getAllIndexSys() throws MetaException {
    return null;
  }

  @Override
  public List<FieldSchema> getPartFieldsJdbc(String dbName, String tableName)
      throws MetaException {
    Connection con = null;
    ;
    Statement ps = null;
    boolean success = false;
    List<FieldSchema> columnInfo = new ArrayList<FieldSchema>();

    dbName = dbName.toLowerCase();
    tableName = tableName.toLowerCase();

    try {
      con = getSegmentConnection(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("get partition field error, db=" + dbName + ", tbl="
          + tableName + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get partition field error, db=" + dbName + ", tbl="
          + tableName + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = "select tbl_id, pri_part_key, sub_part_key from tbls where db_name='"
          + dbName + "' " + " and tbl_name='" + tableName + "'";

      ResultSet tblSet = ps.executeQuery(sql);
      String priPartKey = null;
      String subPartKey = null;
      long tblID = 0;
      boolean isTblFind = false;

      while (tblSet.next()) {
        isTblFind = true;
        tblID = tblSet.getLong(1);
        priPartKey = tblSet.getString(2);
        subPartKey = tblSet.getString(3);
      }

      tblSet.close();

      if (isTblFind) {
        sql = "select column_name, type_name, comment, column_len from "
            + " columns where tbl_id=" + tblID + " and ( column_name='"
            + priPartKey + "' or column_name='" + subPartKey + "')";

        ResultSet colSet = ps.executeQuery(sql);

        while (colSet.next()) {
          FieldSchema field = new FieldSchema();
          field.setName(colSet.getString(1));
          field.setType(colSet.getString(2));
          field.setComment(colSet.getString(3));

          columnInfo.add(field);
        }
      }

      con.commit();
      success = true;
    } catch (SQLException sqlex) {
      LOG.error("get partition field error, db=" + dbName + ", tbl="
          + tableName + ", msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    return columnInfo;
  }

  @Override
  public List<FieldSchema> getFieldsJdbc(String dbName, String tableName)
      throws MetaException {
    Connection con = null;
    ;
    Statement ps = null;
    List<FieldSchema> columnInfo = new ArrayList<FieldSchema>();

    dbName = dbName.toLowerCase();
    tableName = tableName.toLowerCase();

    try {
      con = getSegmentConnection(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("get field error, db=" + dbName + ", tbl=" + tableName
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get field error, db=" + dbName + ", tbl=" + tableName
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = "select columns.column_name, columns.type_name, columns.comment, columns.column_len from "
          + " tbls, columns where tbls.tbl_id=columns.tbl_id and tbls.db_name='"
          + dbName
          + "' "
          + " and tbls.tbl_name='"
          + tableName
          + "' order by column_index asc";

      ResultSet colSet = ps.executeQuery(sql);

      while (colSet.next()) {
        FieldSchema field = new FieldSchema();
        field.setName(colSet.getString(1));
        field.setType(colSet.getString(2));
        field.setComment(colSet.getString(3));

        columnInfo.add(field);
      }
    } catch (SQLException sqlex) {
      LOG.error("get field error, db=" + dbName + ", tbl=" + tableName
          + ", msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    return columnInfo;
  }

  @Override
  public List<String> getJdbcTables(String dbName, String pattern)
      throws MetaException {
    Connection con;
    Statement ps = null;
    List<String> tableList = new ArrayList<String>();

    dbName = dbName.toLowerCase();
    pattern = pattern.toLowerCase();

    try {
      con = getSegmentConnection(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("get table error, db=" + dbName + ", pattern=" + pattern
          + ", msg=" + e1.getMessage());

      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get table error, db=" + dbName + ", pattern=" + pattern
          + ", msg=" + e1.getMessage());

      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = null;

      if (pattern == null || pattern.isEmpty() || pattern.equals(".*")
          || pattern.equals("*")) {
        sql = "select tbl_name, tbl_type from tbls where db_name='" + dbName
            + "'";
      } else {
        pattern = pattern.replace('*', '%');

        sql = "select tbl_name, tbl_type from tbls where db_name='" + dbName
            + "'" + " and tbl_name like '" + pattern + "'";
      }

      ResultSet tblSet = ps.executeQuery(sql);

      while (tblSet.next()) {
        String item = tblSet.getString(1) + ":" + tblSet.getString(2);
        tableList.add(item);
      }
    } catch (SQLException sqlex) {
      LOG.error("get table error, db=" + dbName + ", pattern=" + pattern
          + ", msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    return tableList;
  }

  @Override
  public boolean isTableExit(String dbName, String tblName)
      throws MetaException {

    Connection con = null;
    ;
    Statement ps = null;
    boolean ret = false;

    dbName = dbName.toLowerCase();
    tblName = tblName.toLowerCase();

    try {
      con = getSegmentConnection(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("check table exist error, db=" + dbName + ", tbl=" + tblName
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("check table exist error, db=" + dbName + ", tbl=" + tblName
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = "select tbl_id tbls where db_name='" + dbName + "' "
          + " and tbl_name='" + tblName + "'";

      ResultSet tblSet = ps.executeQuery(sql);

      while (tblSet.next()) {
        ret = true;
      }
    } catch (SQLException sqlex) {
      LOG.error("check table exist error, db=" + dbName + ", tbl=" + tblName
          + ", msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    return ret;
  }

  @Override
  public boolean dropTable(String dbName, String tableName)
      throws MetaException, NoSuchObjectException {
    return dropTable(dbName, tableName, true);
  }

  @Override
  public boolean addUserGroup(group newGroup, String user) throws MetaException {
    Connection con = null;
    ;
    PreparedStatement ps = null;
    boolean success = false;
    newGroup.setGroupName(newGroup.getGroupName().toLowerCase());
    user = user.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("add user group error, creator=" + newGroup.getCreator()
          + ", group=" + newGroup.getGroupName() + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("add user group error, creator=" + newGroup.getCreator()
          + ", group=" + newGroup.getGroupName() + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con.prepareStatement("insert into usergroup(creator, group_name)"
          + " values(?,?)");
      ps.setString(1, newGroup.getCreator().toLowerCase());
      ps.setString(2, newGroup.getGroupName());
      ps.executeUpdate();
      ps.close();

      String addList = newGroup.getUserList();
      if (addList != null && !addList.isEmpty()) {
        ps = con
            .prepareStatement("update tdwuser set group_name=? where user_name=?");

        String[] addArray = addList.split(",");
        for (int i = 0; i < addArray.length; i++) {
          ps.setString(1, newGroup.getGroupName());
          ps.setString(2, addArray[i].toLowerCase());
          ps.addBatch();
        }

        ps.executeBatch();
      }

      con.commit();
      success = true;
    } catch (SQLException sqlex) {
      LOG.error("add user group error, creator=" + newGroup.getCreator()
          + ", group=" + newGroup.getGroupName() + ", msg="
          + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    return success;
  }

  @Override
  public int dropUserGroup(String groupName, String user) throws MetaException {
    Connection con = null;
    Statement ps = null;

    boolean success = false;

    groupName = groupName.toLowerCase();
    user = user.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("drop user group error,  group=" + groupName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("drop user group error,  group=" + groupName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      ps = con.createStatement();

      String sql = "select creator from usergroup where group_name='"
          + groupName + "'";
      ResultSet groupSet = ps.executeQuery(sql);
      String creator = null;

      while (groupSet.next()) {
        creator = groupSet.getString(1);
        break;
      }

      groupSet.close();

      if (creator == null) {
        return 1;
      }

      if ((!user.equalsIgnoreCase(creator))
          && (!user.equalsIgnoreCase(HiveMetaStore.ROOT_USER))) {
        return 3;
      }

      sql = "delete from usergroup where group_name='" + groupName + "'";
      ps.executeUpdate(sql);

      sql = "update tdwuser set group_name='" + HiveMetaStore.DEFAULT
          + "' where group_name='" + groupName + "'";

      ps.executeUpdate(sql);

      con.commit();
      success = true;
    } catch (SQLException sqlex) {
      LOG.error("drop user group error,  group=" + groupName + ", msg="
          + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    if (success) {
      return 0;
    } else {
      return 2;
    }
  }

  @Override
  public String getGroupname(String userName) throws MetaException {
    Connection con = null;
    Statement ps = null;
    String groupName = null;

    userName = userName.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("get user group error,  user=" + userName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get user group error,  user=" + userName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = "select group_name from tdwuser where user_name='"
          + userName + "'";

      ResultSet groupNameSet = ps.executeQuery(sql);
      while (groupNameSet.next()) {
        groupName = groupNameSet.getString(1);
        break;
      }
    } catch (SQLException sqlex) {
      LOG.error("get user group error,  user=" + userName + ", msg="
          + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    return groupName;
  }

  @Override
  public int revokeUserGroup(String groupName, String namelist, String user)
      throws MetaException {
    Connection con = null;
    Statement ps = null;
    boolean success = false;

    groupName = groupName.toLowerCase();
    namelist = namelist.toLowerCase();
    user = user.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("revoke user group error,  groupName=" + groupName
          + ", namelist=" + namelist + ", user=" + user + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("revoke user group error,  groupName=" + groupName
          + ", namelist=" + namelist + ", user=" + user + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      ps = con.createStatement();

      String sql = "select group_name from tdwuser where user_name='"
          + namelist + "'";

      boolean isUserFind = false;
      String oldGroupName = null;

      ResultSet groupNameSet = ps.executeQuery(sql);
      while (groupNameSet.next()) {
        isUserFind = true;
        oldGroupName = groupNameSet.getString(1);
        break;
      }

      groupNameSet.close();

      if (!isUserFind) {
        LOG.error("revoke user group error,  groupName=" + groupName
            + ", namelist=" + namelist + ", user=" + user);
        return 1;
      }

      if (!groupName.equalsIgnoreCase(oldGroupName)) {
        LOG.error("revoke user group error,  groupName=" + groupName
            + ", namelist=" + namelist + ", user=" + user);
        return 2;
      }

      sql = "select creator from usergroup where group_name='" + groupName
          + "'";
      boolean isNewGroupFind = false;
      String newGroupCreator = null;

      ResultSet groupCreatorSet = ps.executeQuery(sql);
      while (groupCreatorSet.next()) {
        isNewGroupFind = true;
        newGroupCreator = groupCreatorSet.getString(1);
        break;
      }

      groupCreatorSet.close();

      if (!isNewGroupFind) {
        LOG.error("revoke user group error,  groupName=" + groupName
            + ", namelist=" + namelist + ", user=" + user);
        return 3;
      }

      if (!newGroupCreator.equalsIgnoreCase(user)
          && !user.equalsIgnoreCase("root")) {
        LOG.error("revoke user group error,  groupName=" + groupName
            + ", namelist=" + namelist + ", user=" + user);
        return 5;
      }

      sql = "update tdwuser set group_name='" + HiveMetaStore.DEFAULT
          + "' where user_name='" + namelist.toLowerCase() + "'";

      ps.executeUpdate(sql);

      con.commit();
      success = true;
    } catch (SQLException sqlex) {
      LOG.error("revoke user group error,  groupName=" + groupName
          + ", namelist=" + namelist + ", user=" + user + ", msg="
          + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    if (success) {
      return 0;
    } else {
      return 6;
    }
  }

  @Override
  public int grantUserGroup(String groupName, String namelist, String user)
      throws MetaException {
    Connection con = null;
    Statement ps = null;
    boolean success = false;

    groupName = groupName.toLowerCase();
    namelist = namelist.toLowerCase();
    user = user.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("grant user group error,  groupName=" + groupName
          + ", namelist=" + namelist + ", user=" + user + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("grant user group error,  groupName=" + groupName
          + ", namelist=" + namelist + ", user=" + user + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      ps = con.createStatement();

      String sql = "select group_name from tdwuser where user_name='"
          + namelist + "'";

      boolean isUserFind = false;
      String oldGroupName = null;

      ResultSet groupNameSet = ps.executeQuery(sql);
      while (groupNameSet.next()) {
        isUserFind = true;
        oldGroupName = groupNameSet.getString(1);
        break;
      }

      groupNameSet.close();

      if (!isUserFind) {
        LOG.error("Can not find user group:" + groupName);
        return 1;
      }

      if (groupName.equalsIgnoreCase(oldGroupName)) {
        LOG.error("grant user group error,  groupName=" + groupName
            + ", namelist=" + namelist + ", user=" + user
            + " group name is same to old group name");
        return 2;
      }

      sql = "select creator from usergroup where group_name='" + groupName
          + "'";
      boolean isNewGroupFind = false;
      String newGroupCreator = null;

      ResultSet groupCreatorSet = ps.executeQuery(sql);
      while (groupCreatorSet.next()) {
        isNewGroupFind = true;
        newGroupCreator = groupCreatorSet.getString(1);
        break;
      }

      groupCreatorSet.close();

      if (!isNewGroupFind) {
        LOG.error("revoke user group error,  groupName=" + groupName
            + ", namelist=" + namelist + ", user=" + user);
        return 4;
      }

      if (!newGroupCreator.equalsIgnoreCase(user)
          && !user.equalsIgnoreCase("root")) {
        LOG.error("revoke user group error,  groupName=" + groupName
            + ", namelist=" + namelist + ", user=" + user);
        return 5;
      }

      sql = "update tdwuser set group_name='" + groupName.toLowerCase()
          + "' where user_name='" + namelist.toLowerCase() + "'";

      ps.executeUpdate(sql);

      con.commit();
      success = true;
    } catch (SQLException sqlex) {
      LOG.error("grant user group error,  groupName=" + groupName
          + ", namelist=" + namelist + ", user=" + user + ", msg="
          + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    if (success) {
      return 0;
    } else {
      return 6;
    }
  }

  @Override
  public List<group> getGroups(String pattern) throws MetaException {
    Connection con = null;
    Statement ps = null;
    List<group> groups = new ArrayList<group>();

    pattern = pattern.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("get user group error,  groupName=" + pattern + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get user group error,  groupName=" + pattern + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = null;


      if (pattern == null || pattern.isEmpty() || pattern.equals(".*")
          || pattern.equals("*")) {
        sql = "select usergroup.group_name, usergroup.creator, string_agg(tdwuser.user_name, ',') namelist, count(*) usercount "
            + " from usergroup left join (select user_name, group_name from tdwuser order by user_name asc) tdwuser on(tdwuser.group_name=usergroup.group_name)  "
            + " group by usergroup.group_name, usergroup.creator";
      } else {
        pattern = pattern.replace('*', '%');
        sql = "select usergroup.group_name, usergroup.creator, string_agg(tdwuser.user_name, ',') namelist, count(*) usercount "
            + " from  usergroup left join (select user_name, group_name from tdwuser order by user_name asc) tdwuser on(tdwuser.group_name=usergroup.group_name)  "
            + " where usergroup.group_name like '"
            + pattern
            + "'"
            + " group by usergroup.group_name, usergroup.creator";
      }

      ResultSet groupSet = ps.executeQuery(sql);

      while (groupSet.next()) {
        group g = new group();
        g.setGroupName(groupSet.getString(1));
        g.setCreator(groupSet.getString(2));
        String userList = groupSet.getString(3);
        if (userList == null) {
          g.setUserList("");
          g.setUserNum(0);
        } else {
          g.setUserList(groupSet.getString(3));
          g.setUserNum((int) groupSet.getLong(4));
        }

        groups.add(g);
      }

      groupSet.close();

    } catch (SQLException sqlex) {
      LOG.error("get user group error,  groupName=" + pattern + ", msg="
          + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    return groups;
  }

  @Override
  public List<List<String>> getPartitionNames(String dbName, String tableName)
      throws MetaException {
    boolean success = false;
    Connection con = null;
    Statement ps = null;

    List<List<String>> ret = new ArrayList<List<String>>();
    dbName = dbName.toLowerCase();
    tableName = tableName.toLowerCase();

    List<String> priPartNames = new ArrayList<String>();
    List<String> subPartNames = new ArrayList<String>();

    try {
      con = getSegmentConnection(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("get partition names error, db=" + dbName + ", tbl="
          + tableName + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get partition names error, db=" + dbName + ", tbl="
          + tableName + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      long tblID = 0;
      boolean isTblFind = false;
      String priPartType = null;
      String subPartType = null;
      boolean hasPriPart = false;
      boolean hasSubPart = false;

      String sql = "SELECT tbl_id, pri_part_type, sub_part_type from TBLS where db_name='"
          + dbName + "' and  tbl_name='" + tableName + "'";

      ResultSet tblSet = ps.executeQuery(sql);
      while (tblSet.next()) {
        isTblFind = true;
        tblID = tblSet.getLong(1);
        priPartType = tblSet.getString(2);
        subPartType = tblSet.getString(3);

        if (priPartType != null && !priPartType.isEmpty()) {
          hasPriPart = true;
        }
        if (subPartType != null && !subPartType.isEmpty()) {
          hasSubPart = true;
        }

        if (!hasPriPart) {
          throw new MetaException("get partition names error, db=" + dbName
              + ", tbl=" + tableName + ", msg=table is not a partition table");
        }
      }

      tblSet.close();

      if (!isTblFind) {
        LOG.error("get partition names error, db=" + dbName + ", tbl="
            + tableName);
        throw new MetaException("can not find table " + dbName + ":"
            + tableName);
      }

      sql = "select part_name, level from PARTITIONS where tbl_id=" + tblID;

      ResultSet partSet = ps.executeQuery(sql);

      while (partSet.next()) {
        String partName = partSet.getString(1);
        int level = partSet.getInt(2);

        if (level == 0) {
          priPartNames.add(partName);
        } else if (level == 1) {
          subPartNames.add(partName);
        }
      }

      if (hasPriPart) {
        if (priPartType.equalsIgnoreCase("hash")) {
          int numOfHashPar = hiveConf.getInt("hive.hashPartition.num", 500);
          ret.add(new ArrayList());
          ret.get(0).add("hash(" + numOfHashPar + ")");
        } else {
          if (priPartNames.contains("default")) {
            priPartNames.remove("default");
            priPartNames.add("default");
          }
          ret.add(priPartNames);
        }

      }

      if (hasSubPart) {
        if (subPartType.equalsIgnoreCase("hash")) {
          int numOfHashPar = hiveConf.getInt("hive.hashPartition.num", 500);
          ret.add(new ArrayList());
          ret.get(1).add("hash(" + numOfHashPar + ")");
        } else {
          if (subPartNames.contains("default")) {
            subPartNames.remove("default");
            subPartNames.add("default");
          }
          ret.add(subPartNames);
        }
      } else {
        ret.add(subPartNames);
      }

      con.commit();
      success = true;
    } catch (SQLException sqlex) {
      LOG.error("get partition names error, db=" + dbName + ", tbl="
          + tableName + ", msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    if (success)
      return ret;
    else
      return null;
  }

  @Override
  public List<String> getPartitionNames(String dbName, String tableName,
      int level) throws MetaException {
    boolean success = false;

    Connection con = null;
    Statement ps = null;
    Partition part = null;

    dbName = dbName.toLowerCase();
    tableName = tableName.toLowerCase();

    List<String> ret = new ArrayList<String>();

    Map<String, List<String>> partNameMap = new LinkedHashMap<String, List<String>>();

    try {
      con = getSegmentConnection(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("get partition name error, db=" + dbName + ", tbl=" + tableName
          + ", level=" + level + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get partition name error, db=" + dbName + ", tbl=" + tableName
          + ", level=" + level + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      long tblID = 0;
      boolean isTblFind = false;
      String priPartType = null;
      String subPartType = null;
      boolean hasPriPart = false;
      boolean hasSubPart = false;

      String sql = "SELECT tbl_id, pri_part_type, sub_part_type from TBLS where db_name='"
          + dbName + "' and  tbl_name='" + tableName + "'";

      ResultSet tblSet = ps.executeQuery(sql);
      while (tblSet.next()) {
        isTblFind = true;
        tblID = tblSet.getLong(1);
        priPartType = tblSet.getString(2);
        subPartType = tblSet.getString(3);

        if (priPartType != null && !priPartType.isEmpty()) {
          hasPriPart = true;
        }
        if (subPartType != null && !subPartType.isEmpty()) {
          hasSubPart = true;
        }

        if (hasPriPart && level == 0) {
          part = new Partition();
          part.setParType(tblSet.getString(4));
          break;
        }

        if (hasSubPart && level == 1) {
          part = new Partition();
          part.setParType(tblSet.getString(5));
          break;
        }

        throw new MetaException("can not find partition of level " + level
            + " for table " + dbName + ":" + tableName);
      }

      tblSet.close();

      if (!isTblFind) {
        throw new MetaException("can not find table " + dbName + ":"
            + tableName);
      }

      sql = "select part_name  from PARTITIONS where tbl_id=" + tblID
          + " and level=" + level;

      ResultSet partSet = ps.executeQuery(sql);

      while (partSet.next()) {
        String partName = partSet.getString(1);
        ret.add(partName);
      }

      part.setParSpaces(partNameMap);
      part.setDbName(dbName);
      part.setTableName(tableName);
      part.setLevel(level);

      con.commit();
      success = true;
    } catch (SQLException sqlex) {
      LOG.error("get partition error, db=" + dbName + ", tbl=" + tableName
          + ", level=" + level + ", msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }

    if (success)
      return ret;
    else
      return null;
  }

  @Override
  public void modifyTableComment(String dbName, String tblName, String comment)
      throws InvalidOperationException, MetaException {
    Connection con = null;
    PreparedStatement ps = null;
    boolean success = false;
    dbName = dbName.toLowerCase();
    tblName = tblName.toLowerCase();

    try {
      con = getSegmentConnection(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("alter table comment error, db=" + dbName + ", tbl=" + tblName
          + ", comment=" + comment + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("alter table comment error, db=" + dbName + ", tbl=" + tblName
          + ", comment=" + comment + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con
          .prepareStatement("select tbl_id from tbls where db_name=? and tbl_name=?");
      long tblID = 0;
      boolean isTblFind = false;
      ps.setString(1, dbName);
      ps.setString(2, tblName);

      ResultSet tSet = ps.executeQuery();
      while (tSet.next()) {
        tblID = tSet.getLong(1);
        isTblFind = true;
        break;
      }

      tSet.close();
      ps.close();

      if (!isTblFind) {
        LOG.error("alter table comment error, db=" + dbName + ", tbl="
            + tblName + ", comment=" + comment);
        throw new MetaException("can not find table " + dbName + ":" + tblName);
      }

      ps = con.prepareStatement("update tbls set tbl_comment=? where tbl_id=?");
      ps.setString(1, comment);
      ps.setLong(2, tblID);
      ps.executeUpdate();

      con.commit();
      success = true;
    } catch (SQLException sqlex) {
      LOG.error("alter table comment error, db=" + dbName + ", tbl=" + tblName
          + ", comment=" + comment + ", msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }
  }

  @Override
  public void modifyColumnComment(String dbName, String tblName,
      String colName, String comment) throws InvalidOperationException,
      MetaException {
    Connection con = null;
    PreparedStatement ps = null;
    boolean success = false;

    dbName = dbName.toLowerCase();
    tblName = tblName.toLowerCase();

    try {
      con = getSegmentConnection(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("alter column comment error, db=" + dbName + ", tbl=" + tblName
          + ", column=" + colName + ", comment=" + comment + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("alter column comment error, db=" + dbName + ", tbl=" + tblName
          + ", column=" + colName + ", comment=" + comment + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con
          .prepareStatement("select tbls.tbl_id from tbls, columns where tbls.tbl_id=columns.tbl_id "
              + " and tbls.db_name=? and tbls.tbl_name=? and columns.column_name=?");
      long tblID = 0;
      boolean isColFind = false;
      ps.setString(1, dbName);
      ps.setString(2, tblName);
      ps.setString(3, colName.toLowerCase());

      ResultSet tSet = ps.executeQuery();
      while (tSet.next()) {
        tblID = tSet.getLong(1);
        isColFind = true;
        break;
      }

      tSet.close();
      ps.close();

      if (!isColFind) {
        LOG.error("alter table comment error, db=" + dbName + ", tbl="
            + tblName + ", comment=" + comment);
        throw new MetaException("can not find table/column " + dbName + ":"
            + tblName + "/" + colName);
      }

      ps = con
          .prepareStatement("update columns set comment=? where tbl_id=? and column_name=?");
      ps.setString(1, comment);
      ps.setLong(2, tblID);
      ps.setString(3, colName.toLowerCase());
      ps.executeUpdate();

      con.commit();
      success = true;
    } catch (SQLException sqlex) {
      LOG.error("alter column comment error, db=" + dbName + ", tbl=" + tblName
          + ", column=" + colName + ", comment=" + comment + ", msg="
          + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }
  }

  @Override
  public boolean isView(String dbName, String tblName)
      throws NoSuchObjectException, MetaException {
    Connection con = null;
    Statement ps = null;
    boolean success = false;
    boolean is = false;

    dbName = dbName.toLowerCase();
    tblName = tblName.toLowerCase();

    try {
      con = getSegmentConnection(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("check is a view error error, db=" + dbName + ", tbl="
          + tblName + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("check is a view error error, db=" + dbName + ", tbl="
          + tblName + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = "select tbl_type from tbls where db_name='" + dbName
          + "' and tbl_name='" + tblName + "'";
      String type = null;
      boolean isTblFind = false;

      ResultSet tSet = ps.executeQuery(sql);
      while (tSet.next()) {
        type = tSet.getString(1);
        isTblFind = true;
        break;
      }

      if (!isTblFind) {
        LOG.error("check is a view error error, db=" + dbName + ", tbl="
            + tblName);
        throw new NoSuchObjectException("can not find table/view:" + dbName
            + ", tbl=" + tblName);
      }

      if (type != null && type.equalsIgnoreCase("VIRTUAL_VIEW")) {
        is = true;
      }

      success = true;
    } catch (SQLException sqlex) {
      LOG.error("check is a view error error, db=" + dbName + ", tbl="
          + tblName + ", msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    return is;
  }

  @Override
  public boolean isAUserName(String userName) throws NoSuchObjectException,
      MetaException {
    Connection con = null;
    Statement ps = null;
    boolean is = false;
    userName = userName.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("check user exist error, user=" + userName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("check user exist error, user=" + userName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = "select count(1) from tdwuser where user_name='" + userName
          + "'";
      int count = 0;

      ResultSet uSet = ps.executeQuery(sql);
      while (uSet.next()) {
        count = uSet.getInt(1);
        break;
      }

      if (count == 1) {
        is = true;
      }

    } catch (SQLException sqlex) {
      LOG.error("check user exist error, user=" + userName + ", msg="
          + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    return is;
  }

  @Override
  public boolean createDatabase(Database db, String slaveURL)
      throws MetaException {
    return false;
  }
  
  @Override
  public boolean hasAuthOnLocation(String who, String location)
      throws NoSuchObjectException, MetaException {
    Connection con = null;
    Statement ps = null;
    who = who.toLowerCase();
    
    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("audit error, user=" + user + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("audit error, user=" + user + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }
    
    try {
      ps = con.createStatement();

      String sql = "select group_name from tdwuser where tdwuser.user_name='"
          + who.toLowerCase() + "'";

      ResultSet re = ps.executeQuery(sql);
      boolean isUserFind = false;
      List<String> groups = new LinkedList<String>();

      while (re.next()) {
        isUserFind = true;
        groups.add(re.getString(1));
      }
      
      re.close();

      if (!isUserFind) {
        throw new NoSuchObjectException("can not find user:" + who);
      }
      
      if (!location.endsWith("/"))
        location += "/";

      String local = "";
      for (String group : groups) {
        if (group == null || group.equals(""))
          continue;
        sql = "select location from tdw_group_location where group_name='" + group.trim() + "'";
        re = ps.executeQuery(sql);
        while (re.next()) {
          local = re.getString(1);
          if (local != null && !local.equals("")) {
            local = local.trim();
    		    if (location.startsWith(local)) {
      			  re.close();
      			  return true;
    		    }
          }
        }
        re.close();
      }
    } catch (SQLException sqlex) {
      LOG.error("get user error, user=" + user + ", msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }
    
    return false;
  }

  @Override
  public boolean hasAuth(String who, int privIndex)
      throws NoSuchObjectException, MetaException {
    Connection con = null;
    ;
    Statement ps = null;
    boolean success = false;
    who = who.toLowerCase();

    Privilege priv = privMap.get(privIndex);

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("audit error, user=" + user + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("audit error, user=" + user + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    long nowtime = (new Date()).getTime();
    Map<String, String> outofdataInfoMap = new HashMap<String, String>();

    try {
      ps = con.createStatement();

      String sql = "select alter_priv, create_priv, createview_priv, dba_priv, "
          + "delete_priv, drop_priv, index_priv, insert_priv, select_priv, showview_priv, "
          + "update_priv, user_name, group_name, out_of_date_time from tdwuser where tdwuser.user_name='"
          + who.toLowerCase() + "'";

      ResultSet userSet = ps.executeQuery(sql);
      boolean isUserFind = false;

      Timestamp ts = null;
      long outofdateTime = 0;
      boolean isOutofdate = false;

      while (userSet.next()) {
        isUserFind = true;

        ts = userSet.getTimestamp(14);

        if (ts != null) {
          outofdateTime = ts.getTime();
        } else {
          outofdateTime = Long.MAX_VALUE;
        }

        if (nowtime > outofdateTime) {
          isOutofdate = true;
        } else {
          isOutofdate = false;
        }

        boolean isDBA = userSet.getBoolean(4);
        if (isDBA) {
          return true;
        }

        switch (priv) {
        case SELECT_PRIV:
          if (userSet.getBoolean(9)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " SELECT_PRIV",
                  ts.toString());
            }
          }
          break;

        case ALTER_PRIV:
          if (userSet.getBoolean(1)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap
                  .put("user " + who + " ALTER_PRIV", ts.toString());
            }
          }
          break;

        case INSERT_PRIV:
          if (userSet.getBoolean(8)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " INSERT_PRIV",
                  ts.toString());
            }
          }
          break;

        case INDEX_PRIV:
          if (userSet.getBoolean(7)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap
                  .put("user " + who + " INDEX_PRIV", ts.toString());
            }
          }
          break;

        case CREATE_PRIV:
          if (userSet.getBoolean(2)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " CREATE_PRIV",
                  ts.toString());
            }
          }
          break;

        case DROP_PRIV:
          if (userSet.getBoolean(6)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " DROP_PRIV", ts.toString());
            }
          }
          break;

        case DELETE_PRIV:
          if (userSet.getBoolean(5)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " DELETE_PRIV",
                  ts.toString());
            }
          }
          break;

        case UPDATE_PRIV:
          if (userSet.getBoolean(11)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " UPDATE_PRIV",
                  ts.toString());
            }
          }
          break;

        case CREATE_VIEW_PRIV:
          if (userSet.getBoolean(3)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " CREATE_VIEW_PRIV",
                  ts.toString());
            }
          }
          break;

        case SHOW_VIEW_PRIV:
          if (userSet.getBoolean(10)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " SHOW_VIEW_PRIV",
                  ts.toString());
            }
          }
          break;

        case DBA_PRIV:
          if (userSet.getBoolean(4)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " DBA_PRIV", ts.toString());
            }
          }
          break;

        }
        break;
      }

      userSet.close();

      if (!isUserFind) {
        throw new NoSuchObjectException("can not find user:" + who);
      }

      sql = "select tdwrole.alter_priv, tdwrole.create_priv, tdwrole.createview_priv, tdwrole.dba_priv, "
          + "tdwrole.delete_priv, tdwrole.drop_priv, tdwrole.index_priv, tdwrole.insert_priv, tdwrole.select_priv, tdwrole.showview_priv, "
          + "tdwrole.update_priv, tdwrole.role_name, tdwrole.out_of_date_time from tdwrole, tdwuserrole where tdwuserrole.user_name='"
          + who.toLowerCase()
          + "'"
          + " and tdwrole.role_name=tdwuserrole.role_name";

      ResultSet roleSet = ps.executeQuery(sql);
      String roleName = null;
      while (roleSet.next()) {
        roleName = roleSet.getString(12);
        ts = roleSet.getTimestamp(13);

        if (ts != null) {
          outofdateTime = ts.getTime();
        } else {
          outofdateTime = Long.MAX_VALUE;
        }

        if (nowtime > outofdateTime) {
          isOutofdate = true;
        } else {
          isOutofdate = false;
        }

        boolean isDBA = roleSet.getBoolean(4);
        if (isDBA) {
          return true;
        }
        switch (priv) {
        case SELECT_PRIV:
          if (roleSet.getBoolean(9)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " SELECT_PRIV",
                  ts.toString());
            }
          }
          break;

        case ALTER_PRIV:
          if (roleSet.getBoolean(1)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " ALTER_PRIV",
                  ts.toString());
            }
          }
          break;

        case INSERT_PRIV:
          if (roleSet.getBoolean(8)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " INSERT_PRIV",
                  ts.toString());
            }
          }
          break;

        case INDEX_PRIV:
          if (roleSet.getBoolean(7)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " INDEX_PRIV",
                  ts.toString());
            }
          }
          break;

        case CREATE_PRIV:
          if (roleSet.getBoolean(2)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " CREATE_PRIV",
                  ts.toString());
            }
          }
          break;

        case DROP_PRIV:
          if (roleSet.getBoolean(6)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " DROP_PRIV",
                  ts.toString());
            }
          }
          break;

        case DELETE_PRIV:
          if (roleSet.getBoolean(5)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " DELETE_PRIV",
                  ts.toString());
            }
          }
          break;

        case UPDATE_PRIV:
          if (roleSet.getBoolean(11)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " UPDATE_PRIV",
                  ts.toString());
            }
          }
          break;

        case CREATE_VIEW_PRIV:
          if (roleSet.getBoolean(3)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " CREATE_VIEW_PRIV",
                  ts.toString());
            }
          }
          break;

        case SHOW_VIEW_PRIV:
          if (roleSet.getBoolean(10)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " SHOW_VIEW_PRIV",
                  ts.toString());
            }
          }
          break;

        case DBA_PRIV:
          if (roleSet.getBoolean(4)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " DBA_PRIV",
                  ts.toString());
            }
          }
          break;

        }
      }

      roleSet.close();

      success = true;
    } catch (SQLException sqlex) {
      LOG.error("get user error, user=" + user + ", msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    if (!outofdataInfoMap.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append("priv out of date, you should renewal you privlage; the detail information is ");
      sb.append("\n");
      for (Entry<String, String> e : outofdataInfoMap.entrySet()) {
        sb.append("priv:" + e.getKey() + ",");
        sb.append("out of date:" + e.getValue());
        sb.append("\n");
      }
      throw new MetaException(sb.toString());
    }

    return false;
  }

  @Override
  public boolean hasAuthOnDb(String who, String db, int privIndex)
      throws NoSuchObjectException, MetaException {
    Connection con = null;
    ;
    Statement ps = null;
    Privilege priv = privMap.get(privIndex);
    who = who.toLowerCase();
    db = db.toLowerCase();

    long nowtime = (new Date()).getTime();
    Map<String, String> outofdataInfoMap = new HashMap<String, String>();
    Timestamp ts = null;
    long outofdateTime = 0;
    boolean isOutofdate = false;

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("audit error, user=" + user + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("audit error, user=" + user + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      ps = con.createStatement();

      String sql = "select alter_priv, create_priv, createview_priv, dba_priv, "
          + "delete_priv, drop_priv, index_priv, insert_priv, select_priv, showview_priv, "
          + "update_priv, user_name, group_name, out_of_date_time from tdwuser where tdwuser.user_name='"
          + who.toLowerCase() + "'";

      ResultSet userSet = ps.executeQuery(sql);
      boolean isUserFind = false;

      while (userSet.next()) {
        isUserFind = true;

        ts = userSet.getTimestamp(14);

        if (ts != null) {
          outofdateTime = ts.getTime();
        } else {
          outofdateTime = Long.MAX_VALUE;
        }

        if (nowtime > outofdateTime) {
          isOutofdate = true;
        } else {
          isOutofdate = false;
        }

        boolean isDBA = userSet.getBoolean(4);
        if (isDBA) {
          return true;
        }

        switch (priv) {
        case SELECT_PRIV:
          if (userSet.getBoolean(9)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " SELECT_PRIV",
                  ts.toString());
            }
          }
          break;

        case ALTER_PRIV:
          if (userSet.getBoolean(1)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap
                  .put("user " + who + " ALTER_PRIV", ts.toString());
            }
          }
          break;

        case INSERT_PRIV:
          if (userSet.getBoolean(8)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " INSERT_PRIV",
                  ts.toString());
            }
          }
          break;

        case INDEX_PRIV:
          if (userSet.getBoolean(7)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap
                  .put("user " + who + " INDEX_PRIV", ts.toString());
            }
          }
          break;

        case CREATE_PRIV:
          if (userSet.getBoolean(2)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " CREATE_PRIV",
                  ts.toString());
            }
          }
          break;

        case DROP_PRIV:
          if (userSet.getBoolean(6)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " DROP_PRIV", ts.toString());
            }
          }
          break;

        case DELETE_PRIV:
          if (userSet.getBoolean(5)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " DELETE_PRIV",
                  ts.toString());
            }
          }
          break;

        case UPDATE_PRIV:
          if (userSet.getBoolean(11)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " UPDATE_PRIV",
                  ts.toString());
            }
          }
          break;

        case CREATE_VIEW_PRIV:
          if (userSet.getBoolean(3)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " CREATE_VIEW_PRIV",
                  ts.toString());
            }
          }
          break;

        case SHOW_VIEW_PRIV:
          if (userSet.getBoolean(10)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " SHOW_VIEW_PRIV",
                  ts.toString());
            }
          }
          break;


        }
      }

      userSet.close();

      if (!isUserFind) {
        throw new NoSuchObjectException("can not find user:" + who);
      }

      sql = "select alter_priv, create_priv, createview_priv, "
          + "delete_priv, drop_priv, index_priv, insert_priv, select_priv, "
          + "showview_priv, update_priv, out_of_date_time from dbpriv where user_name='"
          + who.toLowerCase() + "' and db_name='" + db.toLowerCase() + "'";

      ResultSet dbPrivSet = ps.executeQuery(sql);

      while (dbPrivSet.next()) {
        ts = dbPrivSet.getTimestamp(11);

        if (ts != null) {
          outofdateTime = ts.getTime();
        } else {
          outofdateTime = Long.MAX_VALUE;
        }

        if (nowtime > outofdateTime) {
          isOutofdate = true;
        } else {
          isOutofdate = false;
        }

        switch (priv) {
        case SELECT_PRIV:
          if (dbPrivSet.getBoolean(8)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " SELECT_PRIV", ts.toString());
            }
          }
          break;

        case ALTER_PRIV:
          if (dbPrivSet.getBoolean(1)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " ALTER_PRIV", ts.toString());
            }
          }
          break;

        case INSERT_PRIV:
          if (dbPrivSet.getBoolean(7)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " INSERT_PRIV", ts.toString());
            }
          }
          break;

        case INDEX_PRIV:
          if (dbPrivSet.getBoolean(6)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " INDEX_PRIV", ts.toString());
            }
          }
          break;

        case CREATE_PRIV:
          if (dbPrivSet.getBoolean(2)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " CREATE_PRIV", ts.toString());
            }
          }
          break;

        case DROP_PRIV:
          if (dbPrivSet.getBoolean(5)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " DROP_PRIV", ts.toString());
            }
          }
          break;

        case DELETE_PRIV:
          if (dbPrivSet.getBoolean(4)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " DELETE_PRIV", ts.toString());
            }
          }
          break;

        case UPDATE_PRIV:
          if (dbPrivSet.getBoolean(10)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " UPDATE_PRIV", ts.toString());
            }
          }
          break;

        case CREATE_VIEW_PRIV:
          if (dbPrivSet.getBoolean(3)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " CREATE_VIEW_PRIV", ts.toString());
            }
          }
          break;

        case SHOW_VIEW_PRIV:
          if (dbPrivSet.getBoolean(9)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " SHOW_VIEW_PRIV", ts.toString());
            }
          }
          break;


        }
      }

      sql = "select tdwrole.alter_priv, tdwrole.create_priv, tdwrole.createview_priv, tdwrole.dba_priv, "
          + "tdwrole.delete_priv, tdwrole.drop_priv, tdwrole.index_priv, tdwrole.insert_priv, tdwrole.select_priv, tdwrole.showview_priv, "
          + "tdwrole.update_priv, tdwrole.role_name, tdwrole.out_of_date_time from tdwrole, tdwuserrole where tdwuserrole.user_name='"
          + who.toLowerCase()
          + "'"
          + " and tdwrole.role_name=tdwuserrole.role_name";

      ResultSet roleSet = ps.executeQuery(sql);

      String roleName = null;
      List<String> roleList = new ArrayList<String>();
      while (roleSet.next()) {
        roleName = roleSet.getString(12);
        ts = roleSet.getTimestamp(13);

        if (ts != null) {
          outofdateTime = ts.getTime();
        } else {
          outofdateTime = Long.MAX_VALUE;
        }

        if (nowtime > outofdateTime) {
          isOutofdate = true;
        } else {
          isOutofdate = false;
        }

        boolean isDBA = roleSet.getBoolean(4);
        if (isDBA) {
          return true;
        }

        switch (priv) {
        case SELECT_PRIV:
          if (roleSet.getBoolean(9)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " SELECT_PRIV",
                  ts.toString());
            }
          }
          break;

        case ALTER_PRIV:
          if (roleSet.getBoolean(1)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " ALTER_PRIV",
                  ts.toString());
            }
          }
          break;

        case INSERT_PRIV:
          if (roleSet.getBoolean(8)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " INSERT_PRIV",
                  ts.toString());
            }
          }
          break;

        case INDEX_PRIV:
          if (roleSet.getBoolean(7)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " INDEX_PRIV",
                  ts.toString());
            }
          }
          break;

        case CREATE_PRIV:
          if (roleSet.getBoolean(2)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " CREATE_PRIV",
                  ts.toString());
            }
          }
          break;

        case DROP_PRIV:
          if (roleSet.getBoolean(6)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " DROP_PRIV",
                  ts.toString());
            }
          }
          break;

        case DELETE_PRIV:
          if (roleSet.getBoolean(5)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " DELETE_PRIV",
                  ts.toString());
            }
          }
          break;

        case UPDATE_PRIV:
          if (roleSet.getBoolean(11)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " UPDATE_PRIV",
                  ts.toString());
            }
          }
          break;

        case CREATE_VIEW_PRIV:
          if (roleSet.getBoolean(3)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " CREATE_VIEW_PRIV",
                  ts.toString());
            }
          }
          break;

        case SHOW_VIEW_PRIV:
          if (roleSet.getBoolean(10)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " SHOW_VIEW_PRIV",
                  ts.toString());
            }
          }
          break;

        case DBA_PRIV:
          if (roleSet.getBoolean(4)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " DBA_PRIV",
                  ts.toString());
            }
          }
          break;

        }

        roleList.add(roleSet.getString(12));
      }

      roleSet.close();

      for (String r : roleList) {
        sql = "select alter_priv, create_priv, createview_priv, "
            + "delete_priv, drop_priv, index_priv, insert_priv, select_priv, "
            + "showview_priv, update_priv, out_of_date_time from dbpriv where user_name='"
            + r.toLowerCase() + "' and db_name='" + db.toLowerCase() + "'";

        dbPrivSet = ps.executeQuery(sql);

        while (dbPrivSet.next()) {
          ts = dbPrivSet.getTimestamp(11);

          if (ts != null) {
            outofdateTime = ts.getTime();
          } else {
            outofdateTime = Long.MAX_VALUE;
          }

          if (nowtime > outofdateTime) {
            isOutofdate = true;
          } else {
            isOutofdate = false;
          }

          switch (priv) {
          case SELECT_PRIV:
            if (dbPrivSet.getBoolean(8)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + r + " on db:" + db
                    + " SELECT_PRIV", ts.toString());
              }
            }
            break;

          case ALTER_PRIV:
            if (dbPrivSet.getBoolean(1)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + r + " on db:" + db
                    + " ALTER_PRIV", ts.toString());
              }
            }
            break;

          case INSERT_PRIV:
            if (dbPrivSet.getBoolean(7)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + r + " on db:" + db
                    + " INSERT_PRIV", ts.toString());
              }
            }
            break;

          case INDEX_PRIV:
            if (dbPrivSet.getBoolean(6)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + r + " on db:" + db
                    + " INDEX_PRIV", ts.toString());
              }
            }
            break;

          case CREATE_PRIV:
            if (dbPrivSet.getBoolean(2)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + r + " on db:" + db
                    + " CREATE_PRIV", ts.toString());
              }
            }
            break;

          case DROP_PRIV:
            if (dbPrivSet.getBoolean(5)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + r + " on db:" + db
                    + " DROP_PRIV", ts.toString());
              }
            }
            break;

          case DELETE_PRIV:
            if (dbPrivSet.getBoolean(4)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + r + " on db:" + db
                    + " DELETE_PRIV", ts.toString());
              }
            }
            break;

          case UPDATE_PRIV:
            if (dbPrivSet.getBoolean(10)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + r + " on db:" + db
                    + " UPDATE_PRIV", ts.toString());
              }
            }
            break;

          case CREATE_VIEW_PRIV:
            if (dbPrivSet.getBoolean(3)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + r + " on db:" + db
                    + " CREATE_VIEW_PRIV", ts.toString());
              }
            }
            break;

          case SHOW_VIEW_PRIV:
            if (dbPrivSet.getBoolean(9)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + r + " on db:" + db
                    + " SHOW_VIEW_PRIV", ts.toString());
              }
            }
            break;


          }
        }
      }

    } catch (SQLException sqlex) {
      LOG.error("get user error, user=" + user + ", msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    if (!outofdataInfoMap.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append("priv out of date, you should renewal you privlage; the detail information is ");
      sb.append("\n");
      for (Entry<String, String> e : outofdataInfoMap.entrySet()) {
        sb.append("priv:" + e.getKey() + ",");
        sb.append("out of date:" + e.getValue());
        sb.append("\n");
      }
      throw new MetaException(sb.toString());
    }

    return false;
  }

  @Override
  public boolean hasAuthOnTbl(String who, String db, String table, int privIndex)
      throws NoSuchObjectException, MetaException {
    Connection con = null;
    ;
    Statement ps = null;
    Privilege priv = privMap.get(privIndex);
    who = who.toLowerCase();
    db = db.toLowerCase();
    table = table.toLowerCase();

    long nowtime = (new Date()).getTime();
    Map<String, String> outofdataInfoMap = new HashMap<String, String>();
    Timestamp ts = null;
    long outofdateTime = 0;
    boolean isOutofdate = false;

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("audit error, user=" + user + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("audit error, user=" + user + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      ps = con.createStatement();

      String sql = "select alter_priv, create_priv, createview_priv, dba_priv, "
          + "delete_priv, drop_priv, index_priv, insert_priv, select_priv, showview_priv, "
          + "update_priv, user_name, group_name, out_of_date_time from tdwuser where tdwuser.user_name='"
          + who.toLowerCase() + "'";

      ResultSet userSet = ps.executeQuery(sql);
      boolean isUserFind = false;

      while (userSet.next()) {
        isUserFind = true;

        ts = userSet.getTimestamp(14);

        if (ts != null) {
          outofdateTime = ts.getTime();
        } else {
          outofdateTime = Long.MAX_VALUE;
        }

        if (nowtime > outofdateTime) {
          isOutofdate = true;
        } else {
          isOutofdate = false;
        }

        boolean isDBA = userSet.getBoolean(4);
        if (isDBA) {
          return true;
        }

        switch (priv) {
        case SELECT_PRIV:
          if (userSet.getBoolean(9)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " SELECT_PRIV",
                  ts.toString());
            }
          }
          break;

        case ALTER_PRIV:
          if (userSet.getBoolean(1)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap
                  .put("user " + who + " ALTER_PRIV", ts.toString());
            }
          }
          break;

        case INSERT_PRIV:
          if (userSet.getBoolean(8)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " INSERT_PRIV",
                  ts.toString());
            }
          }
          break;

        case INDEX_PRIV:
          if (userSet.getBoolean(7)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap
                  .put("user " + who + " INDEX_PRIV", ts.toString());
            }
          }
          break;

        case CREATE_PRIV:
          if (userSet.getBoolean(2)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " CREATE_PRIV",
                  ts.toString());
            }
          }
          break;

        case DROP_PRIV:
          if (userSet.getBoolean(6)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " DROP_PRIV", ts.toString());
            }
          }
          break;

        case DELETE_PRIV:
          if (userSet.getBoolean(5)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " DELETE_PRIV",
                  ts.toString());
            }
          }
          break;

        case UPDATE_PRIV:
          if (userSet.getBoolean(11)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " UPDATE_PRIV",
                  ts.toString());
            }
          }
          break;




        }
      }

      userSet.close();

      if (!isUserFind) {
        throw new NoSuchObjectException("can not find user:" + who);
      }

      sql = "select alter_priv, create_priv, createview_priv, "
          + "delete_priv, drop_priv, index_priv, insert_priv, select_priv, "
          + "showview_priv, update_priv, out_of_date_time from dbpriv where user_name='"
          + who.toLowerCase() + "' and db_name='" + db.toLowerCase() + "'";

      ResultSet dbPrivSet = ps.executeQuery(sql);

      while (dbPrivSet.next()) {
        ts = dbPrivSet.getTimestamp(11);

        if (ts != null) {
          outofdateTime = ts.getTime();
        } else {
          outofdateTime = Long.MAX_VALUE;
        }

        if (nowtime > outofdateTime) {
          isOutofdate = true;
        } else {
          isOutofdate = false;
        }

        switch (priv) {
        case SELECT_PRIV:
          if (dbPrivSet.getBoolean(8)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " SELECT_PRIV", ts.toString());
            }
          }
          break;

        case ALTER_PRIV:
          if (dbPrivSet.getBoolean(1)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " ALTER_PRIV", ts.toString());
            }
          }
          break;

        case INSERT_PRIV:
          if (dbPrivSet.getBoolean(7)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " INSERT_PRIV", ts.toString());
            }
          }
          break;

        case INDEX_PRIV:
          if (dbPrivSet.getBoolean(6)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " INDEX_PRIV", ts.toString());
            }
          }
          break;

        case CREATE_PRIV:
          if (dbPrivSet.getBoolean(2)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " CREATE_PRIV", ts.toString());
            }
          }
          break;

        case DROP_PRIV:
          if (dbPrivSet.getBoolean(5)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " DROP_PRIV", ts.toString());
            }
          }
          break;

        case DELETE_PRIV:
          if (dbPrivSet.getBoolean(4)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " DELETE_PRIV", ts.toString());
            }
          }
          break;

        case UPDATE_PRIV:
          if (dbPrivSet.getBoolean(10)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " UPDATE_PRIV", ts.toString());
            }
          }
          break;




        }
      }

      dbPrivSet.close();

      sql = "select alter_priv, create_priv, delete_priv "
          + ",drop_priv, index_priv, insert_priv, select_priv, update_priv, out_of_date_time"
          + " from tblpriv where user_name='" + who.toLowerCase()
          + "' and db_name='" + db.toLowerCase() + "' and tbl_name='"
          + table.toLowerCase() + "'";

      ResultSet tblSet = ps.executeQuery(sql);

      while (tblSet.next()) {
        ts = tblSet.getTimestamp(9);

        if (ts != null) {
          outofdateTime = ts.getTime();
        } else {
          outofdateTime = Long.MAX_VALUE;
        }

        if (nowtime > outofdateTime) {
          isOutofdate = true;
        } else {
          isOutofdate = false;
        }

        switch (priv) {
        case SELECT_PRIV:
          if (tblSet.getBoolean(7)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on tbl:" + db + "/"
                  + table + " SELECT_PRIV", ts.toString());
            }
          }
          break;

        case ALTER_PRIV:
          if (tblSet.getBoolean(1)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on tbl:" + db + "/"
                  + table + " ALTER_PRIV", ts.toString());
            }
          }
          break;

        case INSERT_PRIV:
          if (tblSet.getBoolean(6)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on tbl:" + db + "/"
                  + table + " INSERT_PRIV", ts.toString());
            }
          }
          break;

        case INDEX_PRIV:
          if (tblSet.getBoolean(5)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on tbl:" + db + "/"
                  + table + " INDEX_PRIV", ts.toString());
            }
          }
          break;

        case CREATE_PRIV:
          if (tblSet.getBoolean(2)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on tbl:" + db + "/"
                  + table + " CREATE_PRIV", ts.toString());
            }
          }
          break;

        case DROP_PRIV:
          if (tblSet.getBoolean(4)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on tbl:" + db + "/"
                  + table + " DROP_PRIV", ts.toString());
            }
          }
          break;

        case DELETE_PRIV:
          if (tblSet.getBoolean(3)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on tbl:" + db + "/"
                  + table + " DELETE_PRIV", ts.toString());
            }
          }
          break;

        case UPDATE_PRIV:
          if (tblSet.getBoolean(8)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on tbl:" + db + "/"
                  + table + " UPDATE_PRIV", ts.toString());
            }
          }
          break;
        }
        break;
      }

      tblSet.close();

      sql = "select tdwrole.alter_priv, tdwrole.create_priv, tdwrole.createview_priv, tdwrole.dba_priv, "
          + "tdwrole.delete_priv, tdwrole.drop_priv, tdwrole.index_priv, tdwrole.insert_priv, tdwrole.select_priv, tdwrole.showview_priv, "
          + "tdwrole.update_priv, tdwrole.role_name, tdwrole.out_of_date_time from tdwrole, tdwuserrole where tdwuserrole.user_name='"
          + who.toLowerCase()
          + "'"
          + " and tdwrole.role_name=tdwuserrole.role_name";

      ResultSet roleSet = ps.executeQuery(sql);

      String rName = null;
      List<String> roleList = new ArrayList<String>();
      while (roleSet.next()) {
        rName = roleSet.getString(12);
        ts = roleSet.getTimestamp(13);

        if (ts != null) {
          outofdateTime = ts.getTime();
        } else {
          outofdateTime = Long.MAX_VALUE;
        }

        if (nowtime > outofdateTime) {
          isOutofdate = true;
        } else {
          isOutofdate = false;
        }
        boolean isDBA = roleSet.getBoolean(4);
        if (isDBA) {
          return true;
        }

        switch (priv) {
        case SELECT_PRIV:
          if (roleSet.getBoolean(9)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + rName + " SELECT_PRIV",
                  ts.toString());
            }
          }
          break;

        case ALTER_PRIV:
          if (roleSet.getBoolean(1)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + rName + " ALTER_PRIV",
                  ts.toString());
            }
          }
          break;

        case INSERT_PRIV:
          if (roleSet.getBoolean(8)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + rName + " INSERT_PRIV",
                  ts.toString());
            }
          }
          break;

        case INDEX_PRIV:
          if (roleSet.getBoolean(7)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + rName + " INDEX_PRIV",
                  ts.toString());
            }
          }
          break;

        case CREATE_PRIV:
          if (roleSet.getBoolean(2)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + rName + " CREATE_PRIV",
                  ts.toString());
            }
          }
          break;

        case DROP_PRIV:
          if (roleSet.getBoolean(6)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + rName + " DROP_PRIV",
                  ts.toString());
            }
          }
          break;

        case DELETE_PRIV:
          if (roleSet.getBoolean(5)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + rName + " DELETE_PRIV",
                  ts.toString());
            }
          }
          break;

        case UPDATE_PRIV:
          if (roleSet.getBoolean(11)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + rName + " UPDATE_PRIV",
                  ts.toString());
            }
          }
          break;




        }

        roleList.add(roleSet.getString(12));
      }
      roleSet.close();

      for (String roleName : roleList) {
        sql = "select alter_priv, create_priv, createview_priv, "
            + "delete_priv, drop_priv, index_priv, insert_priv, select_priv, "
            + "showview_priv, update_priv, out_of_date_time from dbpriv where user_name='"
            + roleName.toLowerCase() + "' and db_name='" + db.toLowerCase()
            + "'";

        dbPrivSet = ps.executeQuery(sql);

        while (dbPrivSet.next()) {
          ts = dbPrivSet.getTimestamp(11);

          if (ts != null) {
            outofdateTime = ts.getTime();
          } else {
            outofdateTime = Long.MAX_VALUE;
          }

          if (nowtime > outofdateTime) {
            isOutofdate = true;
          } else {
            isOutofdate = false;
          }

          switch (priv) {
          case SELECT_PRIV:
            if (dbPrivSet.getBoolean(8)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + roleName + " on db:" + db
                    + " SELECT_PRIV", ts.toString());
              }
            }
            break;

          case ALTER_PRIV:
            if (dbPrivSet.getBoolean(1)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + roleName + " on db:" + db
                    + " ALTER_PRIV", ts.toString());
              }
            }
            break;

          case INSERT_PRIV:
            if (dbPrivSet.getBoolean(7)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + roleName + " on db:" + db
                    + " INSERT_PRIV", ts.toString());
              }
            }
            break;

          case INDEX_PRIV:
            if (dbPrivSet.getBoolean(6)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + roleName + " on db:" + db
                    + " INDEX_PRIV", ts.toString());
              }
            }
            break;

          case CREATE_PRIV:
            if (dbPrivSet.getBoolean(2)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + roleName + " on db:" + db
                    + " CREATE_PRIV", ts.toString());
              }
            }
            break;

          case DROP_PRIV:
            if (dbPrivSet.getBoolean(5)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + roleName + " on db:" + db
                    + " DROP_PRIV", ts.toString());
              }
            }
            break;

          case DELETE_PRIV:
            if (dbPrivSet.getBoolean(4)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + roleName + " on db:" + db
                    + " DELETE_PRIV", ts.toString());
              }
            }
            break;

          case UPDATE_PRIV:
            if (dbPrivSet.getBoolean(10)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + roleName + " on db:" + db
                    + " UPDATE_PRIV", ts.toString());
              }
            }
            break;

          case CREATE_VIEW_PRIV:
            if (dbPrivSet.getBoolean(3)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + roleName + " on db:" + db
                    + " CREATE_VIEW_PRIV", ts.toString());
              }
            }
            break;

          case SHOW_VIEW_PRIV:
            if (dbPrivSet.getBoolean(9)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + roleName + " on db:" + db
                    + " CREATE_VIEW_PRIV", ts.toString());
              }
            }
            break;


          }
        }

        dbPrivSet.close();

        sql = "select alter_priv, create_priv, delete_priv "
            + ",drop_priv, index_priv, insert_priv, select_priv, update_priv, out_of_date_time "
            + " from tblpriv where user_name='" + roleName.toLowerCase()
            + "' and db_name='" + db.toLowerCase() + "' and tbl_name='"
            + table.toLowerCase() + "'";

        tblSet = ps.executeQuery(sql);

        while (tblSet.next()) {
          ts = tblSet.getTimestamp(9);

          if (ts != null) {
            outofdateTime = ts.getTime();
          } else {
            outofdateTime = Long.MAX_VALUE;
          }

          if (nowtime > outofdateTime) {
            isOutofdate = true;
          } else {
            isOutofdate = false;
          }

          switch (priv) {
          case SELECT_PRIV:
            if (tblSet.getBoolean(7)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + roleName + " on tbl:" + db + "/"
                    + table + " SELECT_PRIV", ts.toString());
              }
            }
            break;

          case ALTER_PRIV:
            if (tblSet.getBoolean(1)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + roleName + " on tbl:" + db + "/"
                    + table + " ALTER_PRIV", ts.toString());
              }
            }
            break;

          case INSERT_PRIV:
            if (tblSet.getBoolean(6)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + roleName + " on tbl:" + db + "/"
                    + table + " INSERT_PRIV", ts.toString());
              }
            }
            break;

          case INDEX_PRIV:
            if (tblSet.getBoolean(5)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + roleName + " on tbl:" + db + "/"
                    + table + " INDEX_PRIV", ts.toString());
              }
            }
            break;

          case CREATE_PRIV:
            if (tblSet.getBoolean(2)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + roleName + " on tbl:" + db + "/"
                    + table + " CREATE_PRIV", ts.toString());
              }
            }
            break;

          case DROP_PRIV:
            if (tblSet.getBoolean(4)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + roleName + " on tbl:" + db + "/"
                    + table + " DROP_PRIV", ts.toString());
              }
            }
            break;

          case DELETE_PRIV:
            if (tblSet.getBoolean(3)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + roleName + " on tbl:" + db + "/"
                    + table + " DELETE_PRIV", ts.toString());
              }
            }
            break;

          case UPDATE_PRIV:
            if (tblSet.getBoolean(8)) {
              if (!isOutofdate) {
                return true;
              } else {
                outofdataInfoMap.put("role " + roleName + " on tbl:" + db + "/"
                    + table + " UPDATE_PRIV", ts.toString());
              }
            }
            break;
          }
          break;
        }

        tblSet.close();
      }

    } catch (SQLException sqlex) {
      LOG.error("get user error, user=" + user + ", msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    if (!outofdataInfoMap.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append("priv out of date, you should renewal you privlage; the detail information is ");
      sb.append("\n");
      for (Entry<String, String> e : outofdataInfoMap.entrySet()) {
        sb.append("priv:" + e.getKey() + ",");
        sb.append("out of date:" + e.getValue());
        sb.append("\n");
      }
      throw new MetaException(sb.toString());
    }

    return false;
  }

  @Override
  public boolean hasAuthWithRole(String who, String role, int privIndex)
      throws NoSuchObjectException, MetaException {
    Connection con = null;
    ;
    Statement ps = null;
    boolean success = false;
    who = who.toLowerCase();
    role = role.toLowerCase();

    Privilege priv = privMap.get(privIndex);

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("audit error, user=" + user + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("audit error, user=" + user + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    long nowtime = (new Date()).getTime();
    Map<String, String> outofdataInfoMap = new HashMap<String, String>();

    try {
      ps = con.createStatement();

      Timestamp ts = null;
      long outofdateTime = 0;
      boolean isOutofdate = false;

      String sql = "select tdwrole.alter_priv, tdwrole.create_priv, tdwrole.createview_priv, tdwrole.dba_priv, "
          + "tdwrole.delete_priv, tdwrole.drop_priv, tdwrole.index_priv, tdwrole.insert_priv, tdwrole.select_priv, tdwrole.showview_priv, "
          + "tdwrole.update_priv, tdwrole.role_name, tdwrole.out_of_date_time from tdwrole, tdwuserrole where tdwuserrole.user_name='"
          + who.toLowerCase()
          + "' and tdwrole.role_name='"
          + role
          + "' "
          + " and tdwrole.role_name=tdwuserrole.role_name";

      ResultSet roleSet = ps.executeQuery(sql);
      String roleName = null;
      boolean isRoleFind = false;
      while (roleSet.next()) {
        isRoleFind = true;
        roleName = roleSet.getString(12);
        ts = roleSet.getTimestamp(13);

        if (ts != null) {
          outofdateTime = ts.getTime();
        } else {
          outofdateTime = Long.MAX_VALUE;
        }

        if (nowtime > outofdateTime) {
          isOutofdate = true;
        } else {
          isOutofdate = false;
        }

        boolean isDBA = roleSet.getBoolean(4);
        if (isDBA) {
          return true;
        }
        switch (priv) {
        case SELECT_PRIV:
          if (roleSet.getBoolean(9)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " SELECT_PRIV",
                  ts.toString());
            }
          }
          break;

        case ALTER_PRIV:
          if (roleSet.getBoolean(1)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " ALTER_PRIV",
                  ts.toString());
            }
          }
          break;

        case INSERT_PRIV:
          if (roleSet.getBoolean(8)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " INSERT_PRIV",
                  ts.toString());
            }
          }
          break;

        case INDEX_PRIV:
          if (roleSet.getBoolean(7)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " INDEX_PRIV",
                  ts.toString());
            }
          }
          break;

        case CREATE_PRIV:
          if (roleSet.getBoolean(2)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " CREATE_PRIV",
                  ts.toString());
            }
          }
          break;

        case DROP_PRIV:
          if (roleSet.getBoolean(6)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " DROP_PRIV",
                  ts.toString());
            }
          }
          break;

        case DELETE_PRIV:
          if (roleSet.getBoolean(5)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " DELETE_PRIV",
                  ts.toString());
            }
          }
          break;

        case UPDATE_PRIV:
          if (roleSet.getBoolean(11)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " UPDATE_PRIV",
                  ts.toString());
            }
          }
          break;

        case CREATE_VIEW_PRIV:
          if (roleSet.getBoolean(3)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " CREATE_VIEW_PRIV",
                  ts.toString());
            }
          }
          break;

        case SHOW_VIEW_PRIV:
          if (roleSet.getBoolean(10)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " SHOW_VIEW_PRIV",
                  ts.toString());
            }
          }
          break;

        case DBA_PRIV:
          if (roleSet.getBoolean(4)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + roleName + " DBA_PRIV",
                  ts.toString());
            }
          }
          break;

        }
        break;
      }

      roleSet.close();

      if (!isRoleFind) {
        LOG.error("user/role does not exist or user does not play this role:"
            + who + "/" + role);
        throw new MetaException(
            "user/role does not exist or user does not play this role:" + who
                + "/" + role + ", you shoule check it");
      }

      sql = "select alter_priv, create_priv, createview_priv, dba_priv, "
          + "delete_priv, drop_priv, index_priv, insert_priv, select_priv, showview_priv, "
          + "update_priv, user_name, group_name, out_of_date_time from tdwuser where tdwuser.user_name='"
          + who.toLowerCase() + "'";

      ResultSet userSet = ps.executeQuery(sql);
      boolean isUserFind = false;

      while (userSet.next()) {
        isUserFind = true;

        ts = userSet.getTimestamp(14);

        if (ts != null) {
          outofdateTime = ts.getTime();
        } else {
          outofdateTime = Long.MAX_VALUE;
        }

        if (nowtime > outofdateTime) {
          isOutofdate = true;
        } else {
          isOutofdate = false;
        }

        boolean isDBA = userSet.getBoolean(4);
        if (isDBA) {
          return true;
        }

        switch (priv) {
        case SELECT_PRIV:
          if (userSet.getBoolean(9)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " SELECT_PRIV",
                  ts.toString());
            }
          }
          break;

        case ALTER_PRIV:
          if (userSet.getBoolean(1)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap
                  .put("user " + who + " ALTER_PRIV", ts.toString());
            }
          }
          break;

        case INSERT_PRIV:
          if (userSet.getBoolean(8)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " INSERT_PRIV",
                  ts.toString());
            }
          }
          break;

        case INDEX_PRIV:
          if (userSet.getBoolean(7)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap
                  .put("user " + who + " INDEX_PRIV", ts.toString());
            }
          }
          break;

        case CREATE_PRIV:
          if (userSet.getBoolean(2)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " CREATE_PRIV",
                  ts.toString());
            }
          }
          break;

        case DROP_PRIV:
          if (userSet.getBoolean(6)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " DROP_PRIV", ts.toString());
            }
          }
          break;

        case DELETE_PRIV:
          if (userSet.getBoolean(5)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " DELETE_PRIV",
                  ts.toString());
            }
          }
          break;

        case UPDATE_PRIV:
          if (userSet.getBoolean(11)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " UPDATE_PRIV",
                  ts.toString());
            }
          }
          break;

        case CREATE_VIEW_PRIV:
          if (userSet.getBoolean(3)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " CREATE_VIEW_PRIV",
                  ts.toString());
            }
          }
          break;

        case SHOW_VIEW_PRIV:
          if (userSet.getBoolean(10)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " SHOW_VIEW_PRIV",
                  ts.toString());
            }
          }
          break;

        case DBA_PRIV:
          if (userSet.getBoolean(4)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " DBA_PRIV", ts.toString());
            }
          }
          break;

        }
        break;
      }

      userSet.close();

      if (!isUserFind) {
        throw new NoSuchObjectException("can not find user:" + who);
      }

      success = true;
    } catch (SQLException sqlex) {
      LOG.error("get user error, user=" + user + ", msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    if (!outofdataInfoMap.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append("priv out of date, you should renewal you privlage; the detail information is ");
      sb.append("\n");
      for (Entry<String, String> e : outofdataInfoMap.entrySet()) {
        sb.append("priv:" + e.getKey() + ",");
        sb.append("out of date:" + e.getValue());
        sb.append("\n");
      }
      throw new MetaException(sb.toString());
    }

    return false;
  }

  @Override
  public boolean hasAuthOnDbWithRole(String who, String role, String db,
      int privIndex) throws NoSuchObjectException, MetaException {
    Connection con = null;
    ;
    Statement ps = null;
    Privilege priv = privMap.get(privIndex);
    who = who.toLowerCase();
    db = db.toLowerCase();
    role = role.toLowerCase();

    long nowtime = (new Date()).getTime();
    Map<String, String> outofdataInfoMap = new HashMap<String, String>();
    Timestamp ts = null;
    long outofdateTime = 0;
    boolean isOutofdate = false;

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("audit error, user=" + user + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("audit error, user=" + user + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      ps = con.createStatement();

      String sql = "select tdwrole.alter_priv, tdwrole.create_priv, tdwrole.createview_priv, tdwrole.dba_priv, "
          + "tdwrole.delete_priv, tdwrole.drop_priv, tdwrole.index_priv, tdwrole.insert_priv, tdwrole.select_priv, tdwrole.showview_priv, "
          + "tdwrole.update_priv, tdwrole.role_name, tdwrole.out_of_date_time from tdwrole, tdwuserrole where tdwuserrole.user_name='"
          + who.toLowerCase()
          + "' and tdwrole.role_name='"
          + role
          + "' "
          + " and tdwrole.role_name=tdwuserrole.role_name";

      ResultSet roleSet = ps.executeQuery(sql);

      boolean isRoleFind = false;
      while (roleSet.next()) {
        isRoleFind = true;
        ts = roleSet.getTimestamp(13);

        if (ts != null) {
          outofdateTime = ts.getTime();
        } else {
          outofdateTime = Long.MAX_VALUE;
        }

        if (nowtime > outofdateTime) {
          isOutofdate = true;
        } else {
          isOutofdate = false;
        }

        boolean isDBA = roleSet.getBoolean(4);
        if (isDBA) {
          return true;
        }

        switch (priv) {
        case SELECT_PRIV:
          if (roleSet.getBoolean(9)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " SELECT_PRIV",
                  ts.toString());
            }
          }
          break;

        case ALTER_PRIV:
          if (roleSet.getBoolean(1)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " ALTER_PRIV",
                  ts.toString());
            }
          }
          break;

        case INSERT_PRIV:
          if (roleSet.getBoolean(8)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " INSERT_PRIV",
                  ts.toString());
            }
          }
          break;

        case INDEX_PRIV:
          if (roleSet.getBoolean(7)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " INDEX_PRIV",
                  ts.toString());
            }
          }
          break;

        case CREATE_PRIV:
          if (roleSet.getBoolean(2)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " CREATE_PRIV",
                  ts.toString());
            }
          }
          break;

        case DROP_PRIV:
          if (roleSet.getBoolean(6)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap
                  .put("role " + role + " DROP_PRIV", ts.toString());
            }
          }
          break;

        case DELETE_PRIV:
          if (roleSet.getBoolean(5)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " DELETE_PRIV",
                  ts.toString());
            }
          }
          break;

        case UPDATE_PRIV:
          if (roleSet.getBoolean(11)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " UPDATE_PRIV",
                  ts.toString());
            }
          }
          break;

        case CREATE_VIEW_PRIV:
          if (roleSet.getBoolean(3)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " CREATE_VIEW_PRIV",
                  ts.toString());
            }
          }
          break;

        case SHOW_VIEW_PRIV:
          if (roleSet.getBoolean(10)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " SHOW_VIEW_PRIV",
                  ts.toString());
            }
          }
          break;

        case DBA_PRIV:
          if (roleSet.getBoolean(4)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " DBA_PRIV", ts.toString());
            }
          }
          break;

        }
        break;
      }

      roleSet.close();
      if (!isRoleFind) {
        LOG.error("user/role does not exist or user does not play this role:"
            + who + "/" + role);
        throw new MetaException(
            "user/role does not exist or user does not play this role:" + who
                + "/" + role + ", you shoule check it");
      }

      sql = "select alter_priv, create_priv, createview_priv, "
          + "delete_priv, drop_priv, index_priv, insert_priv, select_priv, "
          + "showview_priv, update_priv, out_of_date_time from dbpriv where user_name='"
          + role + "' and db_name='" + db.toLowerCase() + "'";

      ResultSet dbPrivSet = ps.executeQuery(sql);

      while (dbPrivSet.next()) {
        ts = dbPrivSet.getTimestamp(11);

        if (ts != null) {
          outofdateTime = ts.getTime();
        } else {
          outofdateTime = Long.MAX_VALUE;
        }

        if (nowtime > outofdateTime) {
          isOutofdate = true;
        } else {
          isOutofdate = false;
        }

        switch (priv) {
        case SELECT_PRIV:
          if (dbPrivSet.getBoolean(8)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on db:" + db
                  + " SELECT_PRIV", ts.toString());
            }
          }
          break;

        case ALTER_PRIV:
          if (dbPrivSet.getBoolean(1)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on db:" + db
                  + " ALTER_PRIV", ts.toString());
            }
          }
          break;

        case INSERT_PRIV:
          if (dbPrivSet.getBoolean(7)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on db:" + db
                  + " INSERT_PRIV", ts.toString());
            }
          }
          break;

        case INDEX_PRIV:
          if (dbPrivSet.getBoolean(6)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on db:" + db
                  + " INDEX_PRIV", ts.toString());
            }
          }
          break;

        case CREATE_PRIV:
          if (dbPrivSet.getBoolean(2)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on db:" + db
                  + " CREATE_PRIV", ts.toString());
            }
          }
          break;

        case DROP_PRIV:
          if (dbPrivSet.getBoolean(5)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on db:" + db
                  + " DROP_PRIV", ts.toString());
            }
          }
          break;

        case DELETE_PRIV:
          if (dbPrivSet.getBoolean(4)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on db:" + db
                  + " DELETE_PRIV", ts.toString());
            }
          }
          break;

        case UPDATE_PRIV:
          if (dbPrivSet.getBoolean(10)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on db:" + db
                  + " UPDATE_PRIV", ts.toString());
            }
          }
          break;

        case CREATE_VIEW_PRIV:
          if (dbPrivSet.getBoolean(3)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on db:" + db
                  + " CREATE_VIEW_PRIV", ts.toString());
            }
          }
          break;

        case SHOW_VIEW_PRIV:
          if (dbPrivSet.getBoolean(9)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on db:" + db
                  + " SHOW_VIEW_PRIV", ts.toString());
            }
          }
          break;


        }
        break;
      }

      sql = "select alter_priv, create_priv, createview_priv, dba_priv, "
          + "delete_priv, drop_priv, index_priv, insert_priv, select_priv, showview_priv, "
          + "update_priv, user_name, group_name, out_of_date_time from tdwuser where tdwuser.user_name='"
          + who.toLowerCase() + "'";

      ResultSet userSet = ps.executeQuery(sql);
      boolean isUserFind = false;

      while (userSet.next()) {
        isUserFind = true;

        ts = userSet.getTimestamp(14);

        if (ts != null) {
          outofdateTime = ts.getTime();
        } else {
          outofdateTime = Long.MAX_VALUE;
        }

        if (nowtime > outofdateTime) {
          isOutofdate = true;
        } else {
          isOutofdate = false;
        }

        boolean isDBA = userSet.getBoolean(4);
        if (isDBA) {
          return true;
        }

        switch (priv) {
        case SELECT_PRIV:
          if (userSet.getBoolean(9)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " SELECT_PRIV",
                  ts.toString());
            }
          }
          break;

        case ALTER_PRIV:
          if (userSet.getBoolean(1)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap
                  .put("user " + who + " ALTER_PRIV", ts.toString());
            }
          }
          break;

        case INSERT_PRIV:
          if (userSet.getBoolean(8)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " INSERT_PRIV",
                  ts.toString());
            }
          }
          break;

        case INDEX_PRIV:
          if (userSet.getBoolean(7)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap
                  .put("user " + who + " INDEX_PRIV", ts.toString());
            }
          }
          break;

        case CREATE_PRIV:
          if (userSet.getBoolean(2)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " CREATE_PRIV",
                  ts.toString());
            }
          }
          break;

        case DROP_PRIV:
          if (userSet.getBoolean(6)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " DROP_PRIV", ts.toString());
            }
          }
          break;

        case DELETE_PRIV:
          if (userSet.getBoolean(5)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " DELETE_PRIV",
                  ts.toString());
            }
          }
          break;

        case UPDATE_PRIV:
          if (userSet.getBoolean(11)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " UPDATE_PRIV",
                  ts.toString());
            }
          }
          break;

        case CREATE_VIEW_PRIV:
          if (userSet.getBoolean(3)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " CREATE_VIEW_PRIV",
                  ts.toString());
            }
          }
          break;

        case SHOW_VIEW_PRIV:
          if (userSet.getBoolean(10)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " SHOW_VIEW_PRIV",
                  ts.toString());
            }
          }
          break;


        }
        break;
      }

      userSet.close();

      if (!isUserFind) {
        throw new NoSuchObjectException("can not find user:" + who);
      }

      sql = "select alter_priv, create_priv, createview_priv, "
          + "delete_priv, drop_priv, index_priv, insert_priv, select_priv, "
          + "showview_priv, update_priv, out_of_date_time from dbpriv where user_name='"
          + who.toLowerCase() + "' and db_name='" + db.toLowerCase() + "'";

      dbPrivSet = ps.executeQuery(sql);

      while (dbPrivSet.next()) {
        ts = dbPrivSet.getTimestamp(11);

        if (ts != null) {
          outofdateTime = ts.getTime();
        } else {
          outofdateTime = Long.MAX_VALUE;
        }

        if (nowtime > outofdateTime) {
          isOutofdate = true;
        } else {
          isOutofdate = false;
        }

        switch (priv) {
        case SELECT_PRIV:
          if (dbPrivSet.getBoolean(8)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " SELECT_PRIV", ts.toString());
            }
          }
          break;

        case ALTER_PRIV:
          if (dbPrivSet.getBoolean(1)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " ALTER_PRIV", ts.toString());
            }
          }
          break;

        case INSERT_PRIV:
          if (dbPrivSet.getBoolean(7)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " INSERT_PRIV", ts.toString());
            }
          }
          break;

        case INDEX_PRIV:
          if (dbPrivSet.getBoolean(6)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " INDEX_PRIV", ts.toString());
            }
          }
          break;

        case CREATE_PRIV:
          if (dbPrivSet.getBoolean(2)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " CREATE_PRIV", ts.toString());
            }
          }
          break;

        case DROP_PRIV:
          if (dbPrivSet.getBoolean(5)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " DROP_PRIV", ts.toString());
            }
          }
          break;

        case DELETE_PRIV:
          if (dbPrivSet.getBoolean(4)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " DELETE_PRIV", ts.toString());
            }
          }
          break;

        case UPDATE_PRIV:
          if (dbPrivSet.getBoolean(10)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " UPDATE_PRIV", ts.toString());
            }
          }
          break;

        case CREATE_VIEW_PRIV:
          if (dbPrivSet.getBoolean(3)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " CREATE_VIEW_PRIV", ts.toString());
            }
          }
          break;

        case SHOW_VIEW_PRIV:
          if (dbPrivSet.getBoolean(9)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " SHOW_VIEW_PRIV", ts.toString());
            }
          }
          break;


        }
        break;
      }

    } catch (SQLException sqlex) {
      LOG.error("get user error, user=" + user + ", msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    if (!outofdataInfoMap.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append("priv out of date, you should renewal you privlage; the detail information is ");
      sb.append("\n");
      for (Entry<String, String> e : outofdataInfoMap.entrySet()) {
        sb.append("priv:" + e.getKey() + ",");
        sb.append("out of date:" + e.getValue());
        sb.append("\n");
      }
      throw new MetaException(sb.toString());
    }

    return false;
  }

  @Override
  public boolean hasAuthOnTblWithRole(String who, String role, String db,
      String table, int privIndex) throws NoSuchObjectException, MetaException {
    Connection con = null;
    ;
    Statement ps = null;
    Privilege priv = privMap.get(privIndex);
    who = who.toLowerCase();
    db = db.toLowerCase();
    table = table.toLowerCase();
    role = role.toLowerCase();

    long nowtime = (new Date()).getTime();
    Map<String, String> outofdataInfoMap = new HashMap<String, String>();
    Timestamp ts = null;
    long outofdateTime = 0;
    boolean isOutofdate = false;

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("audit error, user=" + user + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("audit error, user=" + user + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      ps = con.createStatement();

      String sql = "select tdwrole.alter_priv, tdwrole.create_priv, tdwrole.createview_priv, tdwrole.dba_priv, "
          + "tdwrole.delete_priv, tdwrole.drop_priv, tdwrole.index_priv, tdwrole.insert_priv, tdwrole.select_priv, tdwrole.showview_priv, "
          + "tdwrole.update_priv, tdwrole.role_name, tdwrole.out_of_date_time from tdwrole, tdwuserrole where tdwuserrole.user_name='"
          + who.toLowerCase()
          + "' and tdwrole.role_name='"
          + role
          + "' and tdwuserrole.role_name=tdwrole.role_name";

      System.out.println(sql);

      ResultSet roleSet = ps.executeQuery(sql);
      boolean isRoleFind = false;

      while (roleSet.next()) {
        isRoleFind = true;
        ts = roleSet.getTimestamp(13);

        if (ts != null) {
          outofdateTime = ts.getTime();
        } else {
          outofdateTime = Long.MAX_VALUE;
        }

        if (nowtime > outofdateTime) {
          isOutofdate = true;
        } else {
          isOutofdate = false;
        }
        boolean isDBA = roleSet.getBoolean(4);
        if (isDBA) {
          return true;
        }

        switch (priv) {
        case SELECT_PRIV:
          if (roleSet.getBoolean(9)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " SELECT_PRIV",
                  ts.toString());
            }
          }
          break;

        case ALTER_PRIV:
          if (roleSet.getBoolean(1)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " ALTER_PRIV",
                  ts.toString());
            }
          }
          break;

        case INSERT_PRIV:
          if (roleSet.getBoolean(8)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " INSERT_PRIV",
                  ts.toString());
            }
          }
          break;

        case INDEX_PRIV:
          if (roleSet.getBoolean(7)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " INDEX_PRIV",
                  ts.toString());
            }
          }
          break;

        case CREATE_PRIV:
          if (roleSet.getBoolean(2)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " CREATE_PRIV",
                  ts.toString());
            }
          }
          break;

        case DROP_PRIV:
          if (roleSet.getBoolean(6)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap
                  .put("role " + role + " DROP_PRIV", ts.toString());
            }
          }
          break;

        case DELETE_PRIV:
          if (roleSet.getBoolean(5)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " DELETE_PRIV",
                  ts.toString());
            }
          }
          break;

        case UPDATE_PRIV:
          if (roleSet.getBoolean(11)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " UPDATE_PRIV",
                  ts.toString());
            }
          }
          break;




        }
        break;
      }
      roleSet.close();

      if (!isRoleFind) {
        LOG.error("user/role does not exist or user does not play this role:"
            + who + "/" + role);
        throw new MetaException(
            "user/role does not exist or user does not play this role:" + who
                + "/" + role + ", you shoule check it");
      }

      sql = "select alter_priv, create_priv, createview_priv, "
          + "delete_priv, drop_priv, index_priv, insert_priv, select_priv, "
          + "showview_priv, update_priv, out_of_date_time from dbpriv where user_name='"
          + role + "' and db_name='" + db.toLowerCase() + "'";

      System.out.println(sql);

      ResultSet dbPrivSet = ps.executeQuery(sql);

      while (dbPrivSet.next()) {
        ts = dbPrivSet.getTimestamp(11);

        if (ts != null) {
          outofdateTime = ts.getTime();
        } else {
          outofdateTime = Long.MAX_VALUE;
        }

        if (nowtime > outofdateTime) {
          isOutofdate = true;
        } else {
          isOutofdate = false;
        }

        switch (priv) {
        case SELECT_PRIV:
          if (dbPrivSet.getBoolean(8)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on db:" + db
                  + " SELECT_PRIV", ts.toString());
            }
          }
          break;

        case ALTER_PRIV:
          if (dbPrivSet.getBoolean(1)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on db:" + db
                  + " ALTER_PRIV", ts.toString());
            }
          }
          break;

        case INSERT_PRIV:
          if (dbPrivSet.getBoolean(7)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on db:" + db
                  + " INSERT_PRIV", ts.toString());
            }
          }
          break;

        case INDEX_PRIV:
          if (dbPrivSet.getBoolean(6)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on db:" + db
                  + " INDEX_PRIV", ts.toString());
            }
          }
          break;

        case CREATE_PRIV:
          if (dbPrivSet.getBoolean(2)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on db:" + db
                  + " CREATE_PRIV", ts.toString());
            }
          }
          break;

        case DROP_PRIV:
          if (dbPrivSet.getBoolean(5)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on db:" + db
                  + " DROP_PRIV", ts.toString());
            }
          }
          break;

        case DELETE_PRIV:
          if (dbPrivSet.getBoolean(4)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on db:" + db
                  + " DELETE_PRIV", ts.toString());
            }
          }
          break;

        case UPDATE_PRIV:
          if (dbPrivSet.getBoolean(10)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on db:" + db
                  + " UPDATE_PRIV", ts.toString());
            }
          }
          break;

        case CREATE_VIEW_PRIV:
          if (dbPrivSet.getBoolean(3)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on db:" + db
                  + " CREATE_VIEW_PRIV", ts.toString());
            }
          }
          break;

        case SHOW_VIEW_PRIV:
          if (dbPrivSet.getBoolean(9)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on db:" + db
                  + " CREATE_VIEW_PRIV", ts.toString());
            }
          }
          break;


        }
        break;
      }

      dbPrivSet.close();

      sql = "select alter_priv, create_priv, delete_priv "
          + ",drop_priv, index_priv, insert_priv, select_priv, update_priv, out_of_date_time "
          + " from tblpriv where user_name='" + role + "' and db_name='"
          + db.toLowerCase() + "' and tbl_name='" + table.toLowerCase() + "'";

      System.out.println(sql);

      ResultSet tblSet = ps.executeQuery(sql);

      while (tblSet.next()) {
        ts = tblSet.getTimestamp(9);

        if (ts != null) {
          outofdateTime = ts.getTime();
        } else {
          outofdateTime = Long.MAX_VALUE;
        }

        if (nowtime > outofdateTime) {
          isOutofdate = true;
        } else {
          isOutofdate = false;
        }

        switch (priv) {
        case SELECT_PRIV:
          if (tblSet.getBoolean(7)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on tbl:" + db + "/"
                  + table + " SELECT_PRIV", ts.toString());
            }
          }
          break;

        case ALTER_PRIV:
          if (tblSet.getBoolean(1)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on tbl:" + db + "/"
                  + table + " ALTER_PRIV", ts.toString());
            }
          }
          break;

        case INSERT_PRIV:
          if (tblSet.getBoolean(6)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on tbl:" + db + "/"
                  + table + " INSERT_PRIV", ts.toString());
            }
          }
          break;

        case INDEX_PRIV:
          if (tblSet.getBoolean(5)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on tbl:" + db + "/"
                  + table + " INDEX_PRIV", ts.toString());
            }
          }
          break;

        case CREATE_PRIV:
          if (tblSet.getBoolean(2)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on tbl:" + db + "/"
                  + table + " CREATE_PRIV", ts.toString());
            }
          }
          break;

        case DROP_PRIV:
          if (tblSet.getBoolean(4)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on tbl:" + db + "/"
                  + table + " DROP_PRIV", ts.toString());
            }
          }
          break;

        case DELETE_PRIV:
          if (tblSet.getBoolean(3)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on tbl:" + db + "/"
                  + table + " DELETE_PRIV", ts.toString());
            }
          }
          break;

        case UPDATE_PRIV:
          if (tblSet.getBoolean(8)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("role " + role + " on tbl:" + db + "/"
                  + table + " UPDATE_PRIV", ts.toString());
            }
          }
          break;
        }
        break;
      }

      tblSet.close();

      sql = "select alter_priv, create_priv, createview_priv, dba_priv, "
          + "delete_priv, drop_priv, index_priv, insert_priv, select_priv, showview_priv, "
          + "update_priv, user_name, group_name, out_of_date_time from tdwuser where tdwuser.user_name='"
          + who.toLowerCase() + "'";

      ResultSet userSet = ps.executeQuery(sql);
      boolean isUserFind = false;

      while (userSet.next()) {
        isUserFind = true;

        ts = userSet.getTimestamp(14);

        if (ts != null) {
          outofdateTime = ts.getTime();
        } else {
          outofdateTime = Long.MAX_VALUE;
        }

        if (nowtime > outofdateTime) {
          isOutofdate = true;
        } else {
          isOutofdate = false;
        }

        boolean isDBA = userSet.getBoolean(4);
        if (isDBA) {
          return true;
        }

        switch (priv) {
        case SELECT_PRIV:
          if (userSet.getBoolean(9)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " SELECT_PRIV",
                  ts.toString());
            }
          }
          break;

        case ALTER_PRIV:
          if (userSet.getBoolean(1)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap
                  .put("user " + who + " ALTER_PRIV", ts.toString());
            }
          }
          break;

        case INSERT_PRIV:
          if (userSet.getBoolean(8)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " INSERT_PRIV",
                  ts.toString());
            }
          }
          break;

        case INDEX_PRIV:
          if (userSet.getBoolean(7)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap
                  .put("user " + who + " INDEX_PRIV", ts.toString());
            }
          }
          break;

        case CREATE_PRIV:
          if (userSet.getBoolean(2)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " CREATE_PRIV",
                  ts.toString());
            }
          }
          break;

        case DROP_PRIV:
          if (userSet.getBoolean(6)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " DROP_PRIV", ts.toString());
            }
          }
          break;

        case DELETE_PRIV:
          if (userSet.getBoolean(5)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " DELETE_PRIV",
                  ts.toString());
            }
          }
          break;

        case UPDATE_PRIV:
          if (userSet.getBoolean(11)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " UPDATE_PRIV",
                  ts.toString());
            }
          }
          break;




        }
        break;
      }

      userSet.close();

      if (!isUserFind) {
        throw new NoSuchObjectException("can not find user:" + who);
      }

      sql = "select alter_priv, create_priv, createview_priv, "
          + "delete_priv, drop_priv, index_priv, insert_priv, select_priv, "
          + "showview_priv, update_priv, out_of_date_time from dbpriv where user_name='"
          + who.toLowerCase() + "' and db_name='" + db.toLowerCase() + "'";

      dbPrivSet = ps.executeQuery(sql);

      while (dbPrivSet.next()) {
        ts = dbPrivSet.getTimestamp(11);

        if (ts != null) {
          outofdateTime = ts.getTime();
        } else {
          outofdateTime = Long.MAX_VALUE;
        }

        if (nowtime > outofdateTime) {
          isOutofdate = true;
        } else {
          isOutofdate = false;
        }

        switch (priv) {
        case SELECT_PRIV:
          if (dbPrivSet.getBoolean(8)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " SELECT_PRIV", ts.toString());
            }
          }
          break;

        case ALTER_PRIV:
          if (dbPrivSet.getBoolean(1)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " ALTER_PRIV", ts.toString());
            }
          }
          break;

        case INSERT_PRIV:
          if (dbPrivSet.getBoolean(7)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " INSERT_PRIV", ts.toString());
            }
          }
          break;

        case INDEX_PRIV:
          if (dbPrivSet.getBoolean(6)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " INDEX_PRIV", ts.toString());
            }
          }
          break;

        case CREATE_PRIV:
          if (dbPrivSet.getBoolean(2)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " CREATE_PRIV", ts.toString());
            }
          }
          break;

        case DROP_PRIV:
          if (dbPrivSet.getBoolean(5)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " DROP_PRIV", ts.toString());
            }
          }
          break;

        case DELETE_PRIV:
          if (dbPrivSet.getBoolean(4)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " DELETE_PRIV", ts.toString());
            }
          }
          break;

        case UPDATE_PRIV:
          if (dbPrivSet.getBoolean(10)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on db:" + db
                  + " UPDATE_PRIV", ts.toString());
            }
          }
          break;




        }
        break;
      }

      dbPrivSet.close();

      sql = "select alter_priv, create_priv, delete_priv "
          + ",drop_priv, index_priv, insert_priv, select_priv, update_priv, out_of_date_time"
          + " from tblpriv where user_name='" + who.toLowerCase()
          + "' and db_name='" + db.toLowerCase() + "' and tbl_name='"
          + table.toLowerCase() + "'";

      LOG.error(sql);

      tblSet = ps.executeQuery(sql);

      while (tblSet.next()) {
        ts = tblSet.getTimestamp(9);

        if (ts != null) {
          outofdateTime = ts.getTime();
        } else {
          outofdateTime = Long.MAX_VALUE;
        }

        if (nowtime > outofdateTime) {
          isOutofdate = true;
        } else {
          isOutofdate = false;
        }

        switch (priv) {
        case SELECT_PRIV:
          if (tblSet.getBoolean(7)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on tbl:" + db + "/"
                  + table + " SELECT_PRIV", ts.toString());
            }
          }
          break;

        case ALTER_PRIV:
          if (tblSet.getBoolean(1)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on tbl:" + db + "/"
                  + table + " ALTER_PRIV", ts.toString());
            }
          }
          break;

        case INSERT_PRIV:
          if (tblSet.getBoolean(6)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on tbl:" + db + "/"
                  + table + " INSERT_PRIV", ts.toString());
            }
          }
          break;

        case INDEX_PRIV:
          if (tblSet.getBoolean(5)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on tbl:" + db + "/"
                  + table + " INDEX_PRIV", ts.toString());
            }
          }
          break;

        case CREATE_PRIV:
          if (tblSet.getBoolean(2)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on tbl:" + db + "/"
                  + table + " CREATE_PRIV", ts.toString());
            }
          }
          break;

        case DROP_PRIV:
          if (tblSet.getBoolean(4)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on tbl:" + db + "/"
                  + table + " DROP_PRIV", ts.toString());
            }
          }
          break;

        case DELETE_PRIV:
          if (tblSet.getBoolean(3)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on tbl:" + db + "/"
                  + table + " DELETE_PRIV", ts.toString());
            }
          }
          break;

        case UPDATE_PRIV:
          if (tblSet.getBoolean(8)) {
            if (!isOutofdate) {
              return true;
            } else {
              outofdataInfoMap.put("user " + who + " on tbl:" + db + "/"
                  + table + " UPDATE_PRIV", ts.toString());
            }
          }
          break;
        }
        break;
      }

      tblSet.close();

    } catch (SQLException sqlex) {
      LOG.error("get user error, user=" + user + ", msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    if (!outofdataInfoMap.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append("priv out of date, you should renewal you privlage; the detail information is ");
      sb.append("\n");
      for (Entry<String, String> e : outofdataInfoMap.entrySet()) {
        sb.append("priv:" + e.getKey() + ",");
        sb.append("out of date:" + e.getValue());
        sb.append("\n");
      }
      throw new MetaException(sb.toString());
    }

    return false;
  }

  @Override
  public List<String> getPlayRoles(String userName)
      throws NoSuchObjectException, MetaException {
    Connection con = null;
    ;
    Statement ps = null;
    List<String> roles = new ArrayList<String>();
    userName = userName.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("get roles error,  msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get roles error,  msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = "select  role_name from tdwuserrole where user_name='"
          + userName + "'";

      ResultSet roleSet = ps.executeQuery(sql);

      while (roleSet.next()) {
        roles.add(roleSet.getString(1));
      }
    } catch (SQLException sqlex) {
      LOG.error("get roles error,  msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    return roles;
  }

  @Override
  public boolean updatePBInfo(String dbName, String tableName,
      String modifiedTime) throws InvalidOperationException, MetaException {
    Connection con = null;
    PreparedStatement ps = null;
    boolean success = false;
    dbName = dbName.toLowerCase();
    tableName = tableName.toLowerCase();
    String jarName = "./auxlib/" + dbName + "_" + tableName + "_"
        + modifiedTime + ".jar";
    String className = dbName + "_" + tableName + "_" + modifiedTime;
    try {
      con = getSegmentConnection(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("updatePBInfo, db=" + dbName + ", table=" + tableName
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("replace column error, db=" + dbName + ", table=" + tableName
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con.prepareStatement("select tbl_id, serde_lib"
          + " from tbls where db_name=? and tbl_name=?");

      ps.setString(1, dbName);
      ps.setString(2, tableName);

      String serdeLib = null;
      boolean isTblFind = false;
      long tblID = 0;

      ResultSet tblSet = ps.executeQuery();
      while (tblSet.next()) {
        isTblFind = true;
        tblID = tblSet.getLong(1);
        serdeLib = tblSet.getString(2);
      }

      tblSet.close();
      ps.close();

      if (!isTblFind) {
        throw new MetaException("can not find table " + dbName + ":"
            + tableName);
      }

      if (!serdeLib.equals(ProtobufSerDe.class.getName())) {
        throw new MetaException(
            "sorry, can only update jar info for a pb table ");
      }

      Map<String, String> tblParamMap = new HashMap<String, String>();
      ps = con
          .prepareStatement("select param_key, param_value from table_params where tbl_id=? and param_type='TBL'");
      ps.setLong(1, tblID);
      ResultSet paramSet = ps.executeQuery();
      while (paramSet.next()) {
        tblParamMap.put(paramSet.getString(1), paramSet.getString(2));
      }
      paramSet.close();
      ps.close();
      boolean containJar = false;
      boolean containClass = false;
      if (tblParamMap.containsKey("pb.jar"))
        containJar = true;
      if (tblParamMap.containsKey("pb.outer.class.name"))
        containClass = true;

      if (containJar && containClass) {
        ps = con
            .prepareStatement("update table_params set param_value=? where tbl_id=? and param_type='TBL' and param_key=?");
        ps.setString(1, jarName);
        ps.setLong(2, tblID);
        ps.setString(3, "pb.jar");
        ps.addBatch();
        ps.setString(1, className);
        ps.setLong(2, tblID);
        ps.setString(3, "pb.outer.class.name");
        ps.addBatch();

        ps.executeBatch();
        ps.close();
      }

      con.commit();
      success = true;
    } catch (SQLException ex) {
      ex.printStackTrace();
      LOG.error("updatePBInfo, db=" + dbName + ", tbl=" + tableName + ", msg="
          + ex.getMessage());
      throw new MetaException(ex.getMessage());
    } finally {
      if (!success) {
        try {
          con.rollback();
        } catch (SQLException e) {
        }
      }

      closeStatement(ps);
      closeConnection(con);
    }
    return true;
  }


  @Override
  public List<String> getDatabasesWithOwner(String owner) throws MetaException {
    Connection con;
    Statement stmt = null;
    owner = owner.toLowerCase();

    List<String> dbNameList = new ArrayList<String>();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("get databases error msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get databases error msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

      stmt = con.createStatement();
      String sql = "select db_name, owner from router where owner='" + owner
          + "'";
      ResultSet ret = stmt.executeQuery(sql);

      dbNameList = new ArrayList<String>();

      while (ret.next()) {
        dbNameList.add(ret.getString(1));
      }
      ret.close();
    } catch (SQLException x) {
      x.printStackTrace();
      try {
        con.rollback();
      } catch (SQLException e) {
        e.printStackTrace();
      }
      LOG.error("get databases error msg=" + x.getMessage());
      throw new MetaException(x.getMessage());
    } finally {
      closeStatement(stmt);
      closeConnection(con);
    }

    return dbNameList;
  }

  @Override
  public boolean isPBTable(String dbName, String tableName)
      throws MetaException {
    Connection con = null;
    PreparedStatement ps = null;
    dbName = dbName.toLowerCase();
    tableName = tableName.toLowerCase();
    try {
      con = getSegmentConnection(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("isPBTable, db=" + dbName + ", table=" + tableName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("isPBTable error, db=" + dbName + ", table=" + tableName
          + ", msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      ps = con.prepareStatement("select serde_lib"
          + " from tbls where db_name=? and tbl_name=?");

      ps.setString(1, dbName);
      ps.setString(2, tableName);

      String serdeLib = null;
      boolean isTblFind = false;

      ResultSet tblSet = ps.executeQuery();
      while (tblSet.next()) {
        isTblFind = true;
        serdeLib = tblSet.getString(1);
      }

      tblSet.close();
      ps.close();

      if (!isTblFind) {
        throw new MetaException("can not find table " + dbName + ":"
            + tableName);
      }
      closeStatement(ps);
      closeConnection(con);
      if (serdeLib.equals(ProtobufSerDe.class.getName()))
        return true;
      else
        return false;
    } catch (SQLException ex) {
      ex.printStackTrace();
      closeStatement(ps);
      closeConnection(con);
      LOG.error("updatePBInfo, db=" + dbName + ", tbl=" + tableName + ", msg="
          + ex.getMessage());
      throw new MetaException(ex.getMessage());
    }
  }


  public Map<String, String> getTableWithType(String dbName, String pattern)
      throws MetaException {
    Connection con;
    Statement ps = null;
    Map<String, String> tableMap = new TreeMap<String, String>();

    dbName = dbName.toLowerCase();
    pattern = pattern.toLowerCase();

    try {
      con = getSegmentConnection(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("get table error, db=" + dbName + ", pattern=" + pattern
          + ", msg=" + e1.getMessage());

      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get table error, db=" + dbName + ", pattern=" + pattern
          + ", msg=" + e1.getMessage());

      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = null;

      if (pattern == null || pattern.isEmpty() || pattern.equals(".*")
          || pattern.equals("*")) {
        sql = "select tbl_name, tbl_type from tbls where db_name='" + dbName
            + "'";
      } else {
        pattern = pattern.replace('*', '%');

        sql = "select tbl_name, tbl_type from tbls where db_name='" + dbName
            + "'" + " and tbl_name like '" + pattern + "'";
      }

      ResultSet tblSet = ps.executeQuery(sql);

      while (tblSet.next()) {
        tableMap.put(tblSet.getString(1), tblSet.getString(2));
      }
    } catch (SQLException sqlex) {
      LOG.error("get table error, db=" + dbName + ", pattern=" + pattern
          + ", msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    return tableMap;
  }
	
	public Map<String, TableInfo> getTableWithTypeWithParams(String dbName, String pattern) throws MetaException 
	{
		// TODO Auto-generated method stub
		Connection con;
		Statement ps = null;
		
		Map<String, TableInfo> tblInfoMap = new TreeMap<String, TableInfo>();
		
		dbName = dbName.toLowerCase();
		pattern = pattern.toLowerCase();
		
		try
		{
			con = getSegmentConnection(dbName);
		} 
		catch (MetaStoreConnectException e1) 
		{
			// TODO Auto-generated catch block
			LOG.error("get table error, db=" + dbName + ", pattern="
					+ pattern + ", msg=" + e1.getMessage());
			
			throw new MetaException(e1.getMessage());
		}
		catch (SQLException e1)
		{
			// TODO Auto-generated catch block
			LOG.error("get table error, db=" + dbName + ", pattern="
					+ pattern + ", msg=" + e1.getMessage());
			
			throw new MetaException(e1.getMessage());
		}
		
		try
		{
			con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
			ps = con.createStatement();
			
			String sql = null;
			
			if(pattern == null || pattern.isEmpty() || pattern.equals(".*") || pattern.equals("*"))
			{
				sql = "select create_time, tbl_type, tbl_name, tbl_owner, pri_part_type, tbl_comment from tbls where db_name='" + dbName + "'";
			}
			else
			{
				pattern = pattern.replace('*', '%');
				
				sql = "select create_time, tbl_type, tbl_name, tbl_owner, pri_part_type, tbl_comment from tbls where db_name='" + dbName + "'"
						+ " and tbl_name like '" + pattern + "'";
			}
			
			ResultSet tblSet = ps.executeQuery(sql);	
			TableInfo info = null;
			Map<String, String> params = null;
			String priPartType = null;
			
			while(tblSet.next())
			{	
				//System.out.println("########################table name is " + tblSet.getString(3));
				info = new TableInfo();
				params = new HashMap<String, String>();
				
				info.setCreateTime(tblSet.getString(1));
				params.put("tbl_type", tblSet.getString(2));
				info.setOwner(tblSet.getString(4));
				
				priPartType = tblSet.getString(5);
				if(priPartType == null)
				{
					params.put("is_part_table", "no");
				}
				else
				{
					params.put("is_part_table", "yes");
				}
				info.setComment(tblSet.getString(6));
				info.setTblParams(params);
				tblInfoMap.put(tblSet.getString(3), info);
			}
		}
		catch(SQLException sqlex)
		{
			LOG.error("get table error, db=" + dbName + ", pattern="
					+ pattern + ", msg=" + sqlex.getMessage());
			sqlex.printStackTrace();
			throw new MetaException(sqlex.getMessage());
		}
		finally
		{
			closeStatement(ps);
			closeConnection(con);	
		}	
		
		return tblInfoMap;
	}
	
	public Map<String, TableInfo> getTableWithTypeWithKeyWord(String dbName, String keyWord) throws MetaException 
	{
		// TODO Auto-generated method stub
		Connection con;
		Statement ps = null;
		
		Map<String, TableInfo> tblInfoMap = new TreeMap<String, TableInfo>();
		
		dbName = dbName.toLowerCase();
		//pattern = pattern.toLowerCase();
		
		try
		{
			con = getSegmentConnection(dbName);
		} 
		catch (MetaStoreConnectException e1) 
		{
			// TODO Auto-generated catch block
			LOG.error("get table error, db=" + dbName + ", key word="
					+ keyWord + ", msg=" + e1.getMessage());
			
			throw new MetaException(e1.getMessage());
		}
		catch (SQLException e1)
		{
			// TODO Auto-generated catch block
			LOG.error("get table error, db=" + dbName + ", key word="
					+ keyWord + ", msg=" + e1.getMessage());
			
			throw new MetaException(e1.getMessage());
		}
		
		try
		{
			con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
			ps = con.createStatement();
			
			String sql = null;
			
			sql = "select create_time, tbl_type, tbl_name, tbl_owner, pri_part_type, tbl_comment from tbls where db_name='" + dbName + "'";
			
			if(keyWord != null)
			{
				sql += " and (tbl_name ilike '%" + keyWord + "%' or tbl_comment ilike '%" + keyWord + "%')";
			}
			
			ResultSet tblSet = ps.executeQuery(sql);	
			TableInfo info = null;
			Map<String, String> params = null;
			String priPartType = null;
			
			while(tblSet.next())
			{	
				//System.out.println("########################table name is " + tblSet.getString(3));
				info = new TableInfo();
				params = new HashMap<String, String>();
				
				info.setCreateTime(tblSet.getString(1));
				params.put("tbl_type", tblSet.getString(2));
				info.setOwner(tblSet.getString(4));
				
				priPartType = tblSet.getString(5);
				if(priPartType == null)
				{
					params.put("is_part_table", "no");
				}
				else
				{
					params.put("is_part_table", "yes");
				}
				info.setComment(tblSet.getString(6));
				info.setTblParams(params);
				tblInfoMap.put(tblSet.getString(3), info);
			}
		}
		catch(SQLException sqlex)
		{
			LOG.error("get table error, db=" + dbName + ", key word="
					+ keyWord + ", msg=" + sqlex.getMessage());
			sqlex.printStackTrace();
			throw new MetaException(sqlex.getMessage());
		}
		finally
		{
			closeStatement(ps);
			closeConnection(con);	
		}	
		
		return tblInfoMap;
	}
	
  @Override
  public List<String> getDbsByPriv(String user, String passwd)
      throws MetaException {
    Connection con = null;
    ;
    Statement ps = null;
    Set<String> tempSet = new HashSet<String>();
    List<String> dbs = new ArrayList<String>();
    user = user.toLowerCase();

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("get db by priv error,  msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get db by priv error,  msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = "select db_name from dbpriv where user_name='" + user
          + "' and select_priv=true";

      ResultSet dbSet = ps.executeQuery(sql);

      while (dbSet.next()) {
        tempSet.add(dbSet.getString(1));
      }
      dbSet.close();

      sql = "select db_name from tdwuserrole, dbpriv where tdwuserrole.user_name='"
          + user
          + "' and tdwuserrole.role_name=dbpriv.user_name and dbpriv.select_priv=true";

      dbSet = ps.executeQuery(sql);

      while (dbSet.next()) {
        tempSet.add(dbSet.getString(1));
      }
      dbSet.close();

      sql = "select db_name from tblpriv where user_name='" + user
          + "' and select_priv=true";

      dbSet = ps.executeQuery(sql);

      while (dbSet.next()) {
        tempSet.add(dbSet.getString(1));
      }
      dbSet.close();

      sql = "select tblpriv.db_name from tdwuserrole, tblpriv where tdwuserrole.user_name='"
          + user
          + "' and tdwuserrole.role_name=tblpriv.user_name and tblpriv.select_priv=true";

      dbSet = ps.executeQuery(sql);

      while (dbSet.next()) {
        tempSet.add(dbSet.getString(1));
      }
      dbSet.close();
    } catch (SQLException sqlex) {
      LOG.error("get db by priv error,  msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    for (String i : tempSet) {
      dbs.add(i);
    }

    return dbs;
  }

  @Override
  public Map<String, String> getTblsByPriv(String user, String passwd, String db)
      throws NoSuchObjectException, MetaException {
    Connection con = null;
    ;
    Statement ps = null;
    Set<String> tempSet = new HashSet<String>();
    List<String> dbs = new ArrayList<String>();
    user = user.toLowerCase();
    db = db.toLowerCase();
    boolean isDbPriv = false;

    Map<String, String> tblMap = getTableWithType(db, "*");
    Map<String, String> tblMapPriv = new TreeMap<String, String>();
    if (tblMap.isEmpty()) {
      return new TreeMap<String, String>();
    }

    try {
      con = getGlobalConnection();
    } catch (MetaStoreConnectException e1) {
      LOG.error("get tbls by priv error,  msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get tbls by priv error,  msg=" + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();

      String sql = "select db_name from dbpriv where user_name='" + user
          + "' and select_priv=true and db_name='" + db + "'";

      ResultSet dbSet = ps.executeQuery(sql);

      while (dbSet.next()) {
        isDbPriv = true;
      }
      dbSet.close();

      if (!isDbPriv) {
        sql = "select db_name from tdwuserrole, dbpriv where tdwuserrole.user_name='"
            + user
            + "' and tdwuserrole.role_name=dbpriv.user_name and dbpriv.select_priv=true and db_name='"
            + db + "'";

        dbSet = ps.executeQuery(sql);

        while (dbSet.next()) {
          isDbPriv = true;
        }
        dbSet.close();
      }

      if (isDbPriv) {
        return tblMap;
      } else {
        sql = "select tbl_name from tblpriv where user_name='" + user
            + "' and select_priv=true and db_name='" + db + "'";

        ResultSet tblSet = ps.executeQuery(sql);
        String tblName = null;
        String type = null;
        while (tblSet.next()) {
          tblName = tblSet.getString(1);
          type = tblMap.get(tblName);
          if (type != null) {
            tblMapPriv.put(tblName, type);
          }
        }
        tblSet.close();
				
				sql = "select a.tbl_name from tblpriv a, tdwuserrole b "
					+ "where a.user_name=b.role_name and b.user_name='" 
					+ user + "' and a.select_priv=true and a.db_name='" + db + "'";
				
				tblSet = ps.executeQuery(sql);
				tblName = null;
				type = null;
				while(tblSet.next())
				{
					tblName = tblSet.getString(1);
					type = tblMap.get(tblName);
					if(type != null)
					{
						tblMapPriv.put(tblName, type);
					}
				}
				tblSet.close();
        return tblMapPriv;
      }
    } catch (SQLException sqlex) {
      LOG.error("get db by priv error,  msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }
  }

  @Override
  public TableInfo getTblInfo(String user, String passwd, String dbName,
      String tblName) throws NoSuchObjectException, MetaException {
    boolean hasAuth = hasAuthOnTbl(user, dbName, tblName, 1);
    if (!hasAuth) {
      throw new MetaException("user " + user
          + " does not have select privlege on table:" + dbName + "/" + tblName);
    }

    boolean success = false;

    Connection con;
    Statement ps = null;

    dbName = dbName.toLowerCase();
    tblName = tblName.toLowerCase();

    TableInfo tblInfo = new TableInfo();

    try {
      con = getSegmentConnection(dbName);
    } catch (MetaStoreConnectException e1) {
      LOG.error("get table error, db=" + dbName + ", tbl=" + tblName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    } catch (SQLException e1) {
      LOG.error("get table error, db=" + dbName + ", tbl=" + tblName + ", msg="
          + e1.getMessage());
      throw new MetaException(e1.getMessage());
    }

    try {
      con.setAutoCommit(false);
      con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      ps = con.createStatement();
	  Map<String, String> paramMap = new HashMap<String, String>();
      boolean isTblFind = false;
      String sql = "select tbl_id, create_time, tbl_type, tbl_owner, pri_part_key, sub_part_key, tbl_comment from tbls "
          + " where db_name='" + dbName + "' and tbl_name='" + tblName + "'";
      ResultSet tblSet = ps.executeQuery(sql);
      String priPartKey = null;
      String subPartKey = null;
      long tblID = 0;
      while (tblSet.next()) {
        isTblFind = true;
        tblID = tblSet.getLong(1);
        tblInfo.setCreateTime(tblSet.getString(2));
        paramMap.put("tbl_type", tblSet.getString(3));
				tblInfo.setOwner(tblSet.getString(4));
				priPartKey = tblSet.getString(5);
				subPartKey = tblSet.getString(6);
				tblInfo.setComment(tblSet.getString(7));
        break;
      }
      tblSet.close();

      if (!isTblFind) {
        LOG.error(dbName + "." + tblName + " table not found");
        throw new NoSuchObjectException(dbName + "." + tblName
            + " table not found");
      }

      sql = "select param_key, param_value from table_params where tbl_id="
          + tblID + " and param_type='TBL'";
      ResultSet paramSet = ps.executeQuery(sql);
      while (paramSet.next()) {
        paramMap.put(paramSet.getString(1), paramSet.getString(2));
      }
      paramSet.close();

      tblInfo.setTblParams(paramMap);

      sql = "select column_name, type_name, comment from columns where tbl_id="
          + tblID + " order by column_index asc";

      ResultSet colSet = ps.executeQuery(sql);
      while (colSet.next()) {
        ColumnInfo colInfo = new ColumnInfo();
        colInfo.setName(colSet.getString(1));
        colInfo.setType(colSet.getString(2));
        colInfo.setComment(colSet.getString(3));
        colInfo.setIsPart(0);

        if (priPartKey != null
            && priPartKey.equalsIgnoreCase(colInfo.getName())) {
          colInfo.setIsPart(1);
        }
        if (subPartKey != null
            && subPartKey.equalsIgnoreCase(colInfo.getName())) {
          colInfo.setIsPart(2);
        }

        tblInfo.addToCols(colInfo);
      }

    } catch (SQLException sqlex) {
      LOG.error("get db by priv error,  msg=" + sqlex.getMessage());
      sqlex.printStackTrace();
      throw new MetaException(sqlex.getMessage());
    } finally {
      closeStatement(ps);
      closeConnection(con);
    }

    return tblInfo;
  }

	@Override
	public List<TableInfo> getAllTblsByPriv(String user, String passwd)
			throws MetaException 
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String,TableInfo> getTblsByPrivWithParams(String user, String passwd,
			String db) throws NoSuchObjectException, MetaException 
	{
		// TODO Auto-generated method stub
		Connection con = null;;
		Statement ps = null;
		Set<String> tempSet = new HashSet<String>();
		List<String> dbs = new ArrayList<String>();
		user = user.toLowerCase();
		db = db.toLowerCase();
		boolean isDbPriv = false;
		
		//List<TableInfo> tblList = new ArrayList<TableInfo>();
		
		
		Map<String, TableInfo> tblMap = getTableWithTypeWithParams(db, "*");
		Map<String, TableInfo> tblMapPriv = new TreeMap<String, TableInfo>();
		if(tblMap.isEmpty())
		{
			return new TreeMap<String, TableInfo>();
		}
		
		try
		{
			con = getGlobalConnection();
		} 
		catch (MetaStoreConnectException e1) 
		{
			// TODO Auto-generated catch block
			LOG.error("get tbls by priv error,  msg=" + e1.getMessage());
			throw new MetaException(e1.getMessage());
		}
		catch (SQLException e1)
		{
			// TODO Auto-generated catch block
			LOG.error("get tbls by priv error,  msg=" + e1.getMessage());
			throw new MetaException(e1.getMessage());
		}
		
		try
		{
			con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
			ps = con.createStatement();
			
			String sql = "select db_name from dbpriv where user_name='"
				+ user + "' and select_priv=true and db_name='" + db + "'";

			ResultSet dbSet = ps.executeQuery(sql);
			
			while(dbSet.next())
			{
				isDbPriv = true;
			}
			dbSet.close();
			
			if(!isDbPriv)
			{
				sql = "select db_name from tdwuserrole, dbpriv where tdwuserrole.user_name='" + user
					+ "' and  tdwuserrole.role_name=dbpriv.user_name and dbpriv.select_priv=true and db_name='" + db + "'";
		
				dbSet = ps.executeQuery(sql);
		
				while(dbSet.next())
				{
					isDbPriv = true;
				}
				dbSet.close();
			}
			
			if(isDbPriv)
			{
				return tblMap;
			}
			else
			{
				sql = "select tbl_name from tblpriv where user_name='"
					+ user + "' and select_priv=true and db_name='" + db + "'";

				ResultSet tblSet = ps.executeQuery(sql);
				String tblName = null;

				TableInfo info = null;
				while(tblSet.next())
				{
					tblName = tblSet.getString(1);
					info = tblMap.get(tblName);
					if(info != null)
					{
						tblMapPriv.put(tblName, info);
					}
				}
				tblSet.close();
				
				sql = "select a.tbl_name from tblpriv a, tdwuserrole b "
					+ "where a.user_name=b.role_name and b.user_name='" 
					+ user + "' and a.select_priv=true and a.db_name='" + db + "'";
				

				tblSet = ps.executeQuery(sql);
				tblName = null;
				//type = null;
				//info = null;
				while(tblSet.next())
				{
					tblName = tblSet.getString(1);
					info = tblMap.get(tblName);
					if(info != null)
					{
						tblMapPriv.put(tblName, info);
					}
				}
				tblSet.close();				
				return tblMapPriv;
			}
		}
		catch(SQLException sqlex)
		{		
			LOG.error("get db by priv error,  msg=" + sqlex.getMessage());
			sqlex.printStackTrace();
			throw new MetaException(sqlex.getMessage());
		}
		finally
		{
			closeStatement(ps);
			closeConnection(con);	
		}
	}

	@Override
	public Map<String, TableInfo> getAllTblsByPrivWithKeyword(String user,
			String passwd, String db, String keyWord) throws MetaException 
	{
		// TODO Auto-generated method stub
		Connection con = null;;
		Statement ps = null;
		Set<String> tempSet = new HashSet<String>();
		List<String> dbs = new ArrayList<String>();
		user = user.toLowerCase();
		db = db.toLowerCase();
		boolean isDbPriv = false;
		
		//List<TableInfo> tblList = new ArrayList<TableInfo>();
		
		
		Map<String, TableInfo> tblMap = getTableWithTypeWithKeyWord(db, keyWord);
		Map<String, TableInfo> tblMapPriv = new TreeMap<String, TableInfo>();
		if(tblMap.isEmpty())
		{
			return new TreeMap<String, TableInfo>();
		}
		
		try
		{
			con = getGlobalConnection();
		} 
		catch (MetaStoreConnectException e1) 
		{
			// TODO Auto-generated catch block
			LOG.error("get tbls by priv error,  msg=" + e1.getMessage());
			throw new MetaException(e1.getMessage());
		}
		catch (SQLException e1)
		{
			// TODO Auto-generated catch block
			LOG.error("get tbls by priv error,  msg=" + e1.getMessage());
			throw new MetaException(e1.getMessage());
		}
		
		try
		{
			con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
			ps = con.createStatement();
			
			String sql = "select db_name from dbpriv where user_name='"
				+ user + "' and select_priv=true and db_name='" + db + "'";

			ResultSet dbSet = ps.executeQuery(sql);
			
			while(dbSet.next())
			{
				isDbPriv = true;
			}
			dbSet.close();
			
			if(!isDbPriv)
			{
				sql = "select db_name from tdwuserrole, dbpriv where tdwuserrole.user_name='" + user
					+ "' and  tdwuserrole.role_name=dbpriv.user_name and dbpriv.select_priv=true and db_name='" + db + "'";
		
				dbSet = ps.executeQuery(sql);
		
				while(dbSet.next())
				{
					isDbPriv = true;
				}
				dbSet.close();
			}
			
			if(isDbPriv)
			{
				return tblMap;
			}
			else
			{
				sql = "select tbl_name from tblpriv where user_name='"
					+ user + "' and select_priv=true and db_name='" + db + "'";
				
				if(keyWord != null)
				{
					sql += " and tbl_name ilike '%" + keyWord + "%'";
				}

				ResultSet tblSet = ps.executeQuery(sql);
				String tblName = null;

				TableInfo info = null;
				while(tblSet.next())
				{
					tblName = tblSet.getString(1);
					info = tblMap.get(tblName);
					if(info != null)
					{
						tblMapPriv.put(tblName, info);
					}
				}
				tblSet.close();
				
				sql = "select a.tbl_name from tblpriv a, tdwuserrole b "
					+ "where a.user_name=b.role_name and b.user_name='" 
					+ user + "' and a.select_priv=true and a.db_name='" + db + "'";
				

				tblSet = ps.executeQuery(sql);
				tblName = null;
				//type = null;
				//info = null;
				while(tblSet.next())
				{
					tblName = tblSet.getString(1);
					info = tblMap.get(tblName);
					if(info != null)
					{
						tblMapPriv.put(tblName, info);
					}
				}
				tblSet.close();				
				return tblMapPriv;
			}
		}
		catch(SQLException sqlex)
		{		
			LOG.error("get db by priv error,  msg=" + sqlex.getMessage());
			sqlex.printStackTrace();
			throw new MetaException(sqlex.getMessage());
		}
		finally
		{
			closeStatement(ps);
			closeConnection(con);	
		}
	}

	@Override
	public List<String> getDbsByOwner(String user, String passwd)
			throws NoSuchObjectException, MetaException 
	{
		// TODO Auto-generated method stub
		Connection con = null;;
		Statement ps = null;
		List<String> dbs = new ArrayList<String>();
		user = user.toLowerCase();

		try
		{
			con = getGlobalConnection();
		} 
		catch (MetaStoreConnectException e1) 
		{
			// TODO Auto-generated catch block
			LOG.error("get tbls by owner error,  msg=" + e1.getMessage());
			throw new MetaException(e1.getMessage());
		}
		catch (SQLException e1)
		{
			// TODO Auto-generated catch block
			LOG.error("get tbls by owner error,  msg=" + e1.getMessage());
			throw new MetaException(e1.getMessage());
		}
		
		try
		{
			ps = con.createStatement();
			
			String sql = "select db_name from router where owner='" + user + "'";

			ResultSet dbSet = ps.executeQuery(sql);
			while(dbSet.next())
			{
				dbs.add(dbSet.getString(1));
			}
			dbSet.close();
			return dbs;
		}
		catch(SQLException sqlex)
		{		
			LOG.error("get db by owner error,  msg=" + sqlex.getMessage());
			sqlex.printStackTrace();
			throw new MetaException(sqlex.getMessage());
		}
		finally
		{
			closeStatement(ps);
			closeConnection(con);	
		}
	}	
	
	 @Override
	  public boolean isHdfsExternalTable(String dbName, String tableName)
	      throws MetaException {
	    Connection con = null;
	    PreparedStatement ps = null;
	    dbName = dbName.toLowerCase();
	    tableName = tableName.toLowerCase();
	    try {
	      con = getSegmentConnection(dbName);
	    } catch (MetaStoreConnectException e1) {
	      LOG.error("isHdfsExternalTable error, db=" + dbName + ", table=" + tableName
	          + ", msg=" + e1.getMessage());
	      throw new MetaException(e1.getMessage());
	    } catch (SQLException e1) {
	      LOG.error("isHdfsExternalTable error, db=" + dbName + ", table=" + tableName
	          + ", msg=" + e1.getMessage());
	      throw new MetaException(e1.getMessage());
	    }

	    try {
	      con.setAutoCommit(false);
	      con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

	      ps = con.prepareStatement("select tbl_type, tbl_format"
	          + " from tbls where db_name=? and tbl_name=?");

	      ps.setString(1, dbName);
	      ps.setString(2, tableName);

	      String tblType = null;
	      String tblFormat = null;
	      boolean isTblFind = false;

	      ResultSet tblSet = ps.executeQuery();
	      while (tblSet.next()) {
	        isTblFind = true;
	        tblType = tblSet.getString(1);
	        tblFormat = tblSet.getString(2);
	      }

	      tblSet.close();
	      ps.close();

	      if (!isTblFind) {
	        throw new MetaException("can not find table " + dbName + ":"
	            + tableName);
	      }
	      closeStatement(ps);
	      closeConnection(con);
	      boolean isPgTable = tblType!=null && tblType.equalsIgnoreCase("EXTERNAL_TABLE")
	          && tblFormat!=null && tblFormat.equalsIgnoreCase("pgdata");
	      if (tblType!=null && tblType.equalsIgnoreCase("EXTERNAL_TABLE") && !isPgTable)
	        return true;
	      else
	        return false;
	    } catch (SQLException ex) {
	      ex.printStackTrace();
	      closeStatement(ps);
	      closeConnection(con);
	      LOG.error("isHdfsExternalTable error, db=" + dbName + ", tbl=" + tableName + ", msg="
	          + ex.getMessage());
	      throw new MetaException(ex.getMessage());
	    }
	  }
}
