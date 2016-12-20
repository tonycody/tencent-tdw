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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AddPartitionDesc;
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
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.RenameColDesc;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;
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
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;

public class HiveMetaStoreClient implements IMetaStoreClient {

  ThriftHiveMetastore.Iface client = null;

  private TTransport transport = null;

  static final private Log LOG = LogFactory.getLog("hive.metastore");

  public HiveMetaStoreClient(HiveConf conf) throws MetaException {
    if (conf == null) {
      conf = new HiveConf(HiveMetaStoreClient.class);
    }
    client = new HiveMetaStore.HMSHandler("hive client", conf);
    return;
  }

  public void alter_table(String dbname, String tbl_name, Table new_tbl)
      throws InvalidOperationException, MetaException, TException {
    client.alter_table(dbname, tbl_name, new_tbl);
  }

  public void close() {

    if ((transport != null) && transport.isOpen()) {
      transport.close();
    }

    try {
      if (null != client) {
        client.shutdown();
      }
    } catch (TException e) {
      LOG.error("Unable to shutdown local metastore client");
      LOG.error(e.getStackTrace());
    }
  }

  public void dropTable(String tableName, boolean deleteData)
      throws MetaException, NoSuchObjectException {
    try {
      this.dropTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName,
          deleteData, false);
    } catch (NoSuchObjectException e) {
      throw e;
    } catch (Exception e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
  }

  public void createTable(Table tbl) throws AlreadyExistsException,
      InvalidObjectException, MetaException, NoSuchObjectException, TException {
    client.createTable(tbl);
  }

  public boolean createType(Type type) throws AlreadyExistsException,
      InvalidObjectException, MetaException, TException {
    return client.createType(type);
  }

  public boolean dropDatabase(String name) throws MetaException, TException,
      NoSuchObjectException {
    return client.dropDatabase(name);
  }

  public void dropTable(String dbname, String name)
      throws NoSuchObjectException, MetaException, TException {
    dropTable(dbname, name, true, true);
  }

  public void dropTable(String dbname, String name, boolean deleteData,
      boolean ignoreUknownTab) throws MetaException, TException,
      NoSuchObjectException {
    try {
      client.dropTable(dbname, name, deleteData);
    } catch (NoSuchObjectException e) {
      if (!ignoreUknownTab) {
        throw e;
      }
    }
  }

  public boolean dropType(String type) throws MetaException, TException {
    return client.dropType(type);
  }

  public Map<String, Type> getTypeAll(String name) throws MetaException,
      TException {
    Map<String, Type> result = null;
    Map<String, Type> fromClient = client.getTypeAll(name);

    if (fromClient != null) {
      result = new LinkedHashMap<String, Type>();
      for (String key : fromClient.keySet()) {
        result.put(key, deepCopy(fromClient.get(key)));
      }
    }

    return result;
  }

  public List<String> getDatabases() throws MetaException, TException {
    return client.getDatabases();
  }

  public Database getDatabase(String name) throws NoSuchObjectException,
      MetaException, TException {
    return deepCopy(client.getDatabase(name));
  }

  public Table getTable(String dbname, String name) throws MetaException,
      TException, NoSuchObjectException {
    return deepCopy(client.getTable(dbname, name));
  }

  public Type getType(String name) throws MetaException, TException {
    return deepCopy(client.getType(name));
  }

  public List<String> getTables(String dbname, String tablePattern)
      throws MetaException {
    try {
      return client.getTables(dbname, tablePattern);
    } catch (Exception e) {
      MetaStoreUtils.logAndThrowMetaException(e);
    }
    return null;
  }

  public List<String> getTables(String tablePattern) throws MetaException {
    String dbname = MetaStoreUtils.DEFAULT_DATABASE_NAME;
    return this.getTables(dbname, tablePattern);
  }

  public boolean tableExists(String tableName) throws MetaException,
      TException, UnknownDBException {
    return client.isTableExist(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
  }

  public Table getTable(String tableName) throws MetaException, TException,
      NoSuchObjectException {
    return getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME, tableName);
  }

  public List<FieldSchema> getFields(String db, String tableName)
      throws MetaException, TException, UnknownTableException,
      UnknownDBException {
    return deepCopyFieldSchemas(client.getFields(db, tableName));
  }

  public List<Partition> getPartitions(String dbName, String tblName)
      throws MetaException, TException, NoSuchObjectException {
    return client.getPartitions(dbName, tblName);
  }

  @Override
  public Partition getPriPartition(String dbName, String tblName)
      throws MetaException, TException {
    return client.getPartition(dbName, tblName, 0);
  }

  @Override
  public Partition getSubPartition(String dbName, String tblName)
      throws MetaException, TException {
    return client.getPartition(dbName, tblName, 1);
  }

  @Override
  public void setParititions(String dbName, String tblName,
      List<Partition> partitions) throws MetaException, TException,
      InvalidOperationException {
    for (Partition part : partitions) {
      client.alterPartition(dbName, tblName, part);
    }
  }

  @Override
  public void setPriPartition(String dbName, String tblName, Partition priPart)
      throws MetaException, TException, InvalidOperationException {
    client.alterPartition(dbName, tblName, priPart);
  }

  @Override
  public void setSubPartition(String dbName, String tblName, Partition subPart)
      throws MetaException, TException, InvalidOperationException {
    client.alterPartition(dbName, tblName, subPart);
  }

  public List<FieldSchema> getSchema(String db, String tableName)
      throws MetaException, TException, UnknownTableException,
      UnknownDBException {
    return deepCopyFieldSchemas(client.getFields(db, tableName));
  }

  public Warehouse getWarehouse() {
    return ((HiveMetaStore.HMSHandler) client).getWarehouse();
  }

  public tdw_sys_table_statistics add_table_statistics(
      tdw_sys_table_statistics new_table_statistics)
      throws AlreadyExistsException, MetaException, TException {
    return client.add_table_statistics(new_table_statistics);
  }

  public boolean delete_table_statistics(String table_statistics_name,
      String db_statistics_name) throws NoSuchObjectException, MetaException,
      TException {
    return client.delete_table_statistics(table_statistics_name,
        db_statistics_name);
  }

  public tdw_sys_table_statistics get_table_statistics(
      String table_statistics_name, String db_statistics_name)
      throws MetaException, TException {
    return client.get_table_statistics(table_statistics_name,
        db_statistics_name);
  }

  public List<tdw_sys_table_statistics> get_table_statistics_multi(
      String db_statistics_name, int max_parts) throws NoSuchObjectException,
      MetaException, TException {
    return client.get_table_statistics_multi(db_statistics_name, max_parts);
  }

  public List<String> get_table_statistics_names(String db_statistics_name,
      int max_parts) throws NoSuchObjectException, MetaException, TException {
    return client.get_table_statistics_names(db_statistics_name, max_parts);
  }

  public tdw_sys_fields_statistics add_fields_statistics(
      tdw_sys_fields_statistics new_fields_statistics)
      throws AlreadyExistsException, MetaException, TException {
    return client.add_fields_statistics(new_fields_statistics);
  }

  public boolean delete_fields_statistics(String table_name,
      String db_statistics_name, String field_name)
      throws NoSuchObjectException, MetaException, TException {
    return client.delete_fields_statistics(table_name, db_statistics_name,
        field_name);
  }

  public tdw_sys_fields_statistics get_fields_statistics(String table_name,
      String db_statistics_name, String field_name) throws MetaException,
      TException {
    return client.get_fields_statistics(table_name, db_statistics_name,
        field_name);
  }

  public List<tdw_sys_fields_statistics> get_fields_statistics_multi(
      String table_name, String db_statistics_name, int max_parts)
      throws NoSuchObjectException, MetaException, TException {
    return client.get_fields_statistics_multi(table_name, db_statistics_name,
        max_parts);
  }

  public List<String> get_fields_statistics_names(String table_name,
      String db_statistics_name, int max_parts) throws NoSuchObjectException,
      MetaException, TException {
    return client.get_fields_statistics_names(table_name, db_statistics_name,
        max_parts);
  }

  public boolean add_tdw_query_info(tdw_query_info query_info)
      throws MetaException, TException {
    return client.add_tdw_query_info(query_info);
  }

  public boolean add_tdw_query_stat(tdw_query_stat query_stat)
      throws MetaException, TException {
    return client.add_tdw_query_stat(query_stat);
  }

  public boolean update_tdw_query_info(String qid, String finishtime,
      String state) throws MetaException, TException {
    return client.update_tdw_query_info(qid, finishtime, state);
  }

  public boolean update_tdw_query_stat(String qid, String finishtime,
      String state) throws MetaException, TException {
    return client.update_tdw_query_stat(qid, finishtime, state);
  }

  public List<tdw_query_info> get_tdw_query_info() throws MetaException,
      TException {
    return client.get_tdw_query_info();
  }

  public List<tdw_query_stat> get_tdw_query_stat() throws MetaException,
      TException {
    return client.get_tdw_query_stat();
  }

  public boolean clear_tdw_query_info(int days) throws MetaException,
      TException {
    return client.clear_tdw_query_info(days);
  }

  public boolean clear_tdw_query_stat(int days) throws MetaException,
      TException {
    return client.clear_tdw_query_stat(days);
  }

  public tdw_query_info search_tdw_query_info(String in) throws MetaException,
      TException {
    return client.search_tdw_query_info(in);
  }

  public boolean add_user_group(group newgroup, String user)
      throws MetaException, TException {
    return client.add_user_group(newgroup, user);
  }

  public int drop_user_group(String groupname, String user)
      throws MetaException, TException {
    return client.drop_user_group(groupname, user);
  }

  public String get_groupname(String username) throws MetaException, TException {
    return client.get_groupname(username);
  }

  public int revoke_user_group(String groupname, String namelist, String user)
      throws MetaException, TException {
    return client.revoke_user_group(groupname, namelist, user);
  }

  public int grant_user_group(String groupname, String namelist, String user)
      throws MetaException, TException {
    return client.grant_user_group(groupname, namelist, user);
  }

  public List<group> get_groups(String pattern) throws MetaException,
      TException {
    return client.get_groups(pattern);
  }

  public boolean create_role(String byWho, String roleName)
      throws AlreadyExistsException, TException, MetaException {
    return client.create_role(byWho, roleName);
  }

  public boolean create_user(String byWho, String newUser, String passwd)
      throws AlreadyExistsException, TException, MetaException {
    return client.create_user(byWho, newUser, passwd);
  }

  public boolean drop_role(String byWho, String roleName)
      throws NoSuchObjectException, TException, MetaException {
    return client.drop_role(byWho, roleName);
  }

  public boolean drop_user(String byWho, String userName)
      throws NoSuchObjectException, TException, MetaException {
    return client.drop_user(byWho, userName);
  }

  public DbPriv get_auth_on_db(String byWho, String who, String db)
      throws NoSuchObjectException, TException, MetaException {
    return client.get_auth_on_db(byWho, who, db);
  }

  public List<DbPriv> get_auth_on_dbs(String byWho, String who)
      throws NoSuchObjectException, TException, MetaException {
    return client.get_auth_on_dbs(byWho, who);
  }

  public TblPriv get_auth_on_tbl(String byWho, String who, String db, String tbl)
      throws NoSuchObjectException, TException, MetaException {
    return client.get_auth_on_tbl(byWho, who, db, tbl);
  }

  public List<TblPriv> get_auth_on_tbls(String byWho, String who)
      throws NoSuchObjectException, TException, MetaException {
    return client.get_auth_on_tbls(byWho, who);
  }

  public List<DbPriv> get_db_auth(String byWho, String db)
      throws NoSuchObjectException, TException, MetaException {
    return client.get_db_auth(byWho, db);
  }

  public List<DbPriv> get_db_auth_all(String byWho)
      throws NoSuchObjectException, TException, MetaException {
    return client.get_db_auth_all(byWho);
  }

  public Role get_role(String byWho, String roleName)
      throws NoSuchObjectException, TException, MetaException {
    return client.get_role(byWho, roleName);
  }

  public List<String> get_roles_all(String byWho) throws TException,
      MetaException {
    return client.get_roles_all(byWho);
  }

  public List<TblPriv> get_tbl_auth(String byWho, String db, String tbl)
      throws NoSuchObjectException, TException, MetaException {
    return client.get_tbl_auth(byWho, db, tbl);
  }

  public List<TblPriv> get_tbl_auth_all(String byWho)
      throws NoSuchObjectException, TException, MetaException {
    return client.get_tbl_auth_all(byWho);
  }

  public User get_user(String byWho, String userName)
      throws NoSuchObjectException, TException, MetaException {
    return client.get_user(byWho, userName);
  }

  public List<String> get_users_all(String byWho) throws TException,
      MetaException {
    return client.get_users_all(byWho);
  }

  public boolean grant_auth_on_db(String byWho, String forWho,
      List<String> privileges, String db) throws NoSuchObjectException,
      TException, MetaException, InvalidObjectException {
    return client.grant_auth_on_db(byWho, forWho, privileges, db);
  }

  public boolean grant_auth_on_tbl(String byWho, String forWho,
      List<String> privileges, String db, String tbl)
      throws NoSuchObjectException, TException, InvalidObjectException,
      MetaException {
    return client.grant_auth_on_tbl(byWho, forWho, privileges, db, tbl);
  }

  public boolean grant_auth_role_sys(String byWho, String roleName,
      List<String> privileges) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException {
    return client.grant_auth_role_sys(byWho, roleName, privileges);
  }

  public boolean grant_auth_sys(String byWho, String userName,
      List<String> privileges) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException {
    return client.grant_auth_sys(byWho, userName, privileges);
  }

  public boolean grant_role_to_role(String byWho, String roleName,
      List<String> roleNames) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException {
    return client.grant_role_to_role(byWho, roleName, roleNames);
  }

  public boolean grant_role_to_user(String byWho, String userName,
      List<String> roleNames) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException {
    return client.grant_role_to_user(byWho, userName, roleNames);
  }

  public boolean is_a_role(String roleName) throws TException, MetaException {
    return client.is_a_role(roleName);
  }

  public boolean is_a_user(String userName, String passwd) throws TException,
      MetaException {
    return client.is_a_user(userName, passwd);
  }

  public boolean revoke_auth_on_db(String byWho, String who,
      List<String> privileges, String db) throws NoSuchObjectException,
      TException, InvalidObjectException, MetaException {
    return client.revoke_auth_on_db(byWho, who, privileges, db);
  }

  public boolean revoke_auth_on_tbl(String byWho, String who,
      List<String> privileges, String db, String tbl)
      throws NoSuchObjectException, TException, InvalidObjectException,
      MetaException {
    return client.revoke_auth_on_tbl(byWho, who, privileges, db, tbl);
  }

  public boolean revoke_auth_role_sys(String byWho, String roleName,
      List<String> privileges) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException {
    return client.revoke_auth_role_sys(byWho, roleName, privileges);
  }

  public boolean revoke_auth_sys(String byWho, String userName,
      List<String> privileges) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException {
    return client.revoke_auth_sys(byWho, userName, privileges);
  }

  public boolean revoke_role_from_role(String byWho, String roleName,
      List<String> roleNames) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException {
    return client.revoke_role_from_role(byWho, roleName, roleNames);
  }

  public boolean revoke_role_from_user(String byWho, String userName,
      List<String> roleNames) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException {
    return client.revoke_role_from_user(byWho, userName, roleNames);
  }

  public boolean set_passwd(String byWho, String forWho, String newPasswd)
      throws NoSuchObjectException, TException, MetaException {
    return client.set_passwd(byWho, forWho, newPasswd);
  }

  @Override
  public boolean drop_auth_in_db(String byWho, String forWho)
      throws TException, MetaException {
    return client.drop_auth_in_db(byWho, forWho);
  }

  @Override
  public boolean drop_auth_in_tbl(String byWho, String forWho)
      throws TException, MetaException {
    return client.drop_auth_in_tbl(byWho, forWho);
  }

  @Override
  public boolean drop_auth_on_db(String byWho, String forWho, String db)
      throws TException, MetaException {
    return client.drop_auth_on_db(byWho, forWho, db);
  }

  @Override
  public boolean drop_auth_on_tbl(String byWho, String forWho, String db,
      String tbl) throws TException, MetaException {
    return client.drop_auth_on_tbl(byWho, forWho, db, tbl);
  }

  public boolean create_index(IndexItem index) throws MetaException, TException {
    return client.create_index(index);
  }

  public boolean drop_index(String db, String table, String name)
      throws MetaException, TException {
    return client.drop_index(db, table, name);
  }

  public int get_index_num(String db, String table) throws MetaException,
      TException {
    return client.get_index_num(db, table);
  }

  public int get_index_type(String db, String table, String name)
      throws MetaException, TException {
    return client.get_index_type(db, table, name);
  }

  public String get_index_field(String db, String table, String name)
      throws MetaException, TException {
    return client.get_index_field(db, table, name);
  }

  public String get_index_location(String db, String table, String name)
      throws MetaException, TException {
    return client.get_index_location(db, table, name);
  }

  public boolean set_index_location(String db, String table, String name,
      String location) throws MetaException, TException {
    return client.set_index_location(db, table, name, location);
  }

  public boolean set_index_status(String db, String table, String name,
      int status) throws MetaException, TException {
    return client.set_index_status(db, table, name, status);
  }

  public List<IndexItem> get_all_index_table(String db, String table)
      throws MetaException, TException {
    return client.get_all_index_table(db, table);
  }

  public IndexItem get_index_info(String db, String table, String name)
      throws MetaException, TException {
    return client.get_index_info(db, table, name);
  }

  public List<IndexItem> get_all_index_sys() throws MetaException, TException {
    return client.get_all_index_sys();
  }

  public List<FieldSchema> getFiledsJdbc(String dbName, String tableName)
      throws MetaException, TException {
    return client.get_fields_jdbc(dbName, tableName);
  }

  public List<FieldSchema> getPartFiledsJdbc(String dbName, String tableName)
      throws MetaException, TException {
    return client.get_part_fields_jdbc(dbName, tableName);
  }

  private Database deepCopy(Database database) {
    Database copy = null;
    if (database != null) {
      copy = new Database(database);
    }

    return copy;
  }

  private Table deepCopy(Table table) {
    Table copy = null;

    if (table != null) {
      copy = new Table(table);
    }

    return copy;
  }

  private Type deepCopy(Type type) {
    Type copy = null;

    if (type != null) {
      copy = new Type(type);
    }

    return copy;
  }

  private FieldSchema deepCopy(FieldSchema schema) {
    FieldSchema copy = null;

    if (schema != null) {
      copy = new FieldSchema(schema);
    }
    return copy;
  }

  private List<FieldSchema> deepCopyFieldSchemas(List<FieldSchema> schemas) {

    List<FieldSchema> copy = null;

    if (schemas != null) {
      copy = new ArrayList<FieldSchema>();
      for (FieldSchema schema : schemas) {
        copy.add(deepCopy(schema));
      }
    }
    return copy;
  }

  @Override
  public tdw_sys_table_statistics addTableStatistics(
      tdw_sys_table_statistics newTableStatistics)
      throws InvalidObjectException, AlreadyExistsException, MetaException,
      TException {
    return null;
  }

  @Override
  public boolean deleteTableStatistics(String table_statistics_name,
      String dbStatisticsName) throws NoSuchObjectException, MetaException,
      TException {
    return false;
  }

  @Override
  public tdw_sys_table_statistics getTableStatistics(
      String tableStatisticsName, String dbStatisticsName)
      throws MetaException, TException {
    return null;
  }

  @Override
  public List<tdw_sys_table_statistics> getTableStatisticsMulti(
      String dbStatisticsName, int maxParts) throws NoSuchObjectException,
      MetaException, TException {
    return null;
  }

  @Override
  public List<String> getTableStatisticsNames(String dbStatisticsName,
      int maxParts) throws NoSuchObjectException, MetaException, TException {
    return null;
  }

  @Override
  public tdw_sys_fields_statistics addFieldsStatistics(
      tdw_sys_fields_statistics newFieldsStatistics)
      throws InvalidObjectException, AlreadyExistsException, MetaException,
      TException {
    return null;
  }

  @Override
  public boolean deleteFieldsStatistics(String tableStatisticsName,
      String dbStatisticsName, String fieldsName) throws NoSuchObjectException,
      MetaException, TException {
    return false;
  }

  @Override
  public tdw_sys_fields_statistics getFieldsStatistics(
      String tableStatisticsName, String dbStatisticsName, String fieldsName)
      throws MetaException, TException {
    return null;
  }

  @Override
  public List<tdw_sys_fields_statistics> getFieldsStatisticsMulti(
      String tableStatisticsName, String dbStatisticsName, int maxParts)
      throws NoSuchObjectException, MetaException, TException {
    return null;
  }

  @Override
  public List<String> getFieldsStatistics_names(String tableStatisticsName,
      String dbStatisticsName, int maxParts) throws NoSuchObjectException,
      MetaException, TException {
    return null;
  }

  @Override
  public boolean addTdwQueryInfo(tdw_query_info queryInfo)
      throws MetaException, TException {
    return false;
  }

  @Override
  public boolean addTdwQueryStat(tdw_query_stat queryStat)
      throws MetaException, TException {
    return false;
  }

  @Override
  public boolean updateTdwQueryInfo(String qid, String finishtime, String state)
      throws MetaException, TException {
    return false;
  }

  @Override
  public boolean updateTdwQueryStat(String qid, String finishtime, String state)
      throws MetaException, TException {
    return false;
  }

  @Override
  public List<tdw_query_info> getTdwQueryInfo() throws MetaException,
      TException {
    return null;
  }

  @Override
  public List<tdw_query_stat> getTdwQueryStat() throws MetaException,
      TException {
    return null;
  }

  @Override
  public boolean clearTdwQueryInfo(int days) throws MetaException, TException {
    return false;
  }

  @Override
  public boolean clearTdwQueryStat(int days) throws MetaException, TException {
    return false;
  }

  @Override
  public boolean addUserGroup(group newGroup, String user)
      throws MetaException, TException {
    return client.addUserGroup(newGroup, user);
  }

  @Override
  public int dropUserGroup(String groupname, String user) throws MetaException,
      TException {
    return client.dropUserGroup(groupname, user);
  }

  @Override
  public String getGroupname(String username) throws MetaException, TException {
    return client.getGroupname(username);
  }

  @Override
  public int revokeUserGroup(String groupName, String namelist, String user)
      throws MetaException, TException {
    return client.revokeUserGroup(groupName, namelist, user);
  }

  @Override
  public int grantUserGroup(String groupName, String namelist, String user)
      throws MetaException, TException {
    return client.grantUserGroup(groupName, namelist, user);
  }

  @Override
  public List<group> getGroups(String pattern) throws MetaException, TException {
    return client.getGroups(pattern);
  }

  @Override
  public boolean createUer(String byWho, String newUser, String passwd)
      throws AlreadyExistsException, TException, MetaException {
    return client.createUser(byWho, newUser, passwd);
  }

  @Override
  public boolean dropUser(String byWho, String userName)
      throws NoSuchObjectException, TException, MetaException {
    return client.dropUser(byWho, userName);
  }

  @Override
  public User getUser(String byWho, String userName)
      throws NoSuchObjectException, TException, MetaException {
    return client.getUser(byWho, userName);
  }

  @Override
  public List<String> getUsersAll(String byWho) throws TException,
      MetaException {
    return client.getUsersAll(byWho);
  }

  @Override
  public boolean setPasswd(String byWho, String forWho, String newPasswd)
      throws NoSuchObjectException, TException, MetaException {
    return client.setPasswd(byWho, forWho, newPasswd);
  }

  @Override
  public boolean isAUser(String userName, String passwd) throws TException,
      MetaException {
    return client.isAUser(userName, passwd);
  }

  @Override
  public boolean isARole(String roleName) throws TException, MetaException {
    return client.isARole(roleName);
  }

  @Override
  public boolean createRole(String byWho, String roleName)
      throws AlreadyExistsException, TException, MetaException {
    return client.createRole(byWho, roleName);
  }

  @Override
  public boolean dropRole(String byWho, String roleName)
      throws NoSuchObjectException, TException, MetaException {
    return client.dropRole(byWho, roleName);
  }

  @Override
  public Role getRole(String byWho, String roleName)
      throws NoSuchObjectException, TException, MetaException {
    return client.getRole(byWho, roleName);
  }

  @Override
  public List<String> getRolesAll(String byWho) throws TException,
      MetaException {
    return client.getRolesAll(byWho);
  }

  @Override
  public List<String> getPlayRoles(String byWho) throws NoSuchObjectException,
      TException, MetaException {
    return client.getPlayRoles(byWho);
  }

  @Override
  public boolean grantAuthSys(String byWho, String userName,
      List<String> privileges) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException {
    return client.grantAuthSys(byWho, userName, privileges);
  }

  @Override
  public boolean grantAuthRoleSys(String byWho, String roleName,
      List<String> privileges) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException {
    return client.grantAuthRoleSys(byWho, roleName, privileges);
  }

  @Override
  public boolean grantRoleToUser(String byWho, String userName,
      List<String> roleNames) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException {
    return client.grantRoleToUser(byWho, userName, roleNames);
  }

  @Override
  public boolean grantRoleToRole(String byWho, String roleName,
      List<String> roleNames) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException {
    return client.grantRoleToRole(byWho, roleName, roleNames);
  }

  @Override
  public boolean grantAuthOnDb(String byWho, String forWho,
      List<String> privileges, String db) throws NoSuchObjectException,
      TException, MetaException, InvalidObjectException {
    return client.grantAuthOnDb(byWho, forWho, privileges, db);
  }

  @Override
  public boolean grantAuthOnTbl(String byWho, String forWho,
      List<String> privileges, String db, String tbl)
      throws NoSuchObjectException, TException, InvalidObjectException,
      MetaException {
    return client.grantAuthOnTbl(byWho, forWho, privileges, db, tbl);
  }

  @Override
  public DbPriv getAuthOnDb(String byWho, String who, String db)
      throws NoSuchObjectException, TException, MetaException {
    return client.getAuthOnDb(byWho, who, db);
  }

  @Override
  public List<DbPriv> getDbAuth(String byWho, String db)
      throws NoSuchObjectException, TException, MetaException {
    return client.getDbAuth(byWho, db);
  }

  @Override
  public List<DbPriv> getDbAuthAll(String byWho) throws NoSuchObjectException,
      TException, MetaException {
    return client.getDbAuthAll(byWho);
  }

  @Override
  public TblPriv getAuthOnTbl(String byWho, String who, String db, String tbl)
      throws NoSuchObjectException, TException, MetaException {
    return client.getAuthOnTbl(byWho, who, db, tbl);
  }

  @Override
  public List<TblPriv> getTblAuth(String byWho, String db, String tbl)
      throws NoSuchObjectException, TException, MetaException {
    return client.getTblAuth(byWho, db, tbl);
  }

  @Override
  public List<TblPriv> getTblAuthAll(String byWho)
      throws NoSuchObjectException, TException, MetaException {
    return client.getTblAuthAll(byWho);
  }

  @Override
  public boolean revokeAuthSys(String byWho, String userName,
      List<String> privileges) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException {
    return client.revokeAuthSys(byWho, userName, privileges);
  }

  @Override
  public boolean revokeAuthRoleSys(String byWho, String roleName,
      List<String> privileges) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException {
    return client.revokeAuthRoleSys(byWho, roleName, privileges);
  }

  @Override
  public boolean revokeRoleFromUser(String byWho, String userName,
      List<String> roleNames) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException {
    return client.revokeRoleFromUser(byWho, userName, roleNames);
  }

  @Override
  public boolean revokeRoleFromRole(String byWho, String roleName,
      List<String> roleNames) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException {
    return client.revokeRoleFromRole(byWho, roleName, roleNames);
  }

  @Override
  public boolean revokeAuthOnDb(String byWho, String who,
      List<String> privileges, String db) throws NoSuchObjectException,
      TException, InvalidObjectException, MetaException {
    return client.revokeAuthOnDb(byWho, who, privileges, db);
  }

  @Override
  public boolean revokeAuthOnTbl(String byWho, String who,
      List<String> privileges, String db, String tbl)
      throws NoSuchObjectException, TException, InvalidObjectException,
      MetaException {
    return client.revokeAuthOnTbl(byWho, who, privileges, db, tbl);
  }

  @Override
  public boolean dropAuthOnDb(String byWho, String forWho, String db)
      throws TException, MetaException {
    return client.dropAuthOnDb(byWho, forWho, db);
  }

  @Override
  public boolean dropAuthInDb(String byWho, String forWho) throws TException,
      MetaException {
    return client.dropAuthInDb(byWho, forWho);
  }

  @Override
  public boolean dropAuthOnTbl(String byWho, String forWho, String db,
      String tbl) throws TException, MetaException {
    return client.dropAuthOnTbl(byWho, forWho, db, tbl);
  }

  @Override
  public boolean dropAuthInTbl(String byWho, String forWho) throws TException,
      MetaException {
    return client.dropAuthInTbl(byWho, forWho);
  }

  @Override
  public boolean createIndex(IndexItem index) throws MetaException, TException {
    return false;
  }

  @Override
  public boolean dropUndex(String db, String table, String name)
      throws MetaException, TException {
    return false;
  }

  @Override
  public int getUndexNum(String db, String table) throws MetaException,
      TException {
    return 0;
  }

  @Override
  public int getIndexType(String db, String table, String name)
      throws MetaException, TException {
    return 0;
  }

  @Override
  public String getIndexField(String db, String table, String name)
      throws MetaException, TException {
    return null;
  }

  @Override
  public String getIndexLocation(String db, String table, String name)
      throws MetaException, TException {
    return null;
  }

  @Override
  public boolean setIndexLocation(String db, String table, String name,
      String location) throws MetaException, TException {
    return false;
  }

  @Override
  public boolean setIndexStatus(String db, String table, String name, int status)
      throws MetaException, TException {
    return false;
  }

  @Override
  public List<IndexItem> getAllIndexTable(String db, String table)
      throws MetaException, TException {
    return null;
  }

  @Override
  public IndexItem getIndexInfo(String db, String table, String name)
      throws MetaException, TException {
    return null;
  }

  @Override
  public List<IndexItem> getAllIndexSys() throws MetaException, TException {
    return null;
  }

  @Override
  public boolean isTableExist(String dbName, String tableName)
      throws MetaException, TException {
    return client.isTableExist(dbName, tableName);
  }

  @Override
  public void addPartition(String dbName, String tblName,
      AddPartitionDesc addPartitionDesc) throws InvalidOperationException,
      MetaException, TException {
    client.addPartition(dbName, tblName, addPartitionDesc);
  }

  @Override
  public void dropPartition(String dbName, String tblName,
      DropPartitionDesc dropPartitionDesc) throws InvalidOperationException,
      MetaException, TException {
    client.dropPartition(dbName, tblName, dropPartitionDesc);
  }

  @Override
  public void addDefaultPartition(String dbName, String tblName, int level)
      throws InvalidOperationException, MetaException, TException {
    client.addDefaultPartition(dbName, tblName, level);
  }

  @Override
  public void dropDefaultPartition(String dbName, String tblName, int level)
      throws InvalidOperationException, MetaException, TException {
    client.dropDefaultPartition(dbName, tblName, level);
  }

  @Override
  public void renameTable(String dbName, String tblName, String modifyUser,
      String newName) throws InvalidOperationException, MetaException,
      TException {
    client.renameTable(dbName, tblName, modifyUser, newName);
  }

  @Override
  public void addCols(String dbName, String tblName, String modifyUser,
      List<FieldSchema> newCols) throws InvalidOperationException,
      MetaException, TException {
    client.addCols(dbName, tblName, modifyUser, newCols);
  }

  @Override
  public void renameCols(String dbName, String tblName,
      RenameColDesc renameColDesc) throws InvalidOperationException,
      MetaException, TException {
    client.renameCols(dbName, tblName, renameColDesc);
  }

  @Override
  public void replaceCols(String dbName, String tblName, String modifyUser,
      List<FieldSchema> newCols) throws InvalidOperationException,
      MetaException, TException {
    client.replaceCols(dbName, tblName, modifyUser, newCols);
  }

  @Override
  public void addTblProps(String dbName, String tblName, String modifyUser,
      Map<String, String> props) throws InvalidOperationException,
      MetaException, TException {
    client.addTblProps(dbName, tblName, modifyUser, props);
  }

  @Override
  public void addSerdeProps(String dbName, String tblName, String modifyUser,
      Map<String, String> props) throws InvalidOperationException,
      MetaException, TException {
    client.addSerdeProps(dbName, tblName, modifyUser, props);
  }

  @Override
  public void addSerde(String dbName, String tblName, AddSerdeDesc addSerdeDesc)
      throws InvalidOperationException, MetaException, TException {
    client.addSerde(dbName, tblName, addSerdeDesc);
  }

  @Override
  public List<List<String>> getPartitionNames(String dbName, String tblName,
      short max) throws InvalidOperationException, MetaException, TException {
    return client.getPartitionNames(dbName, tblName, max);
  }

  @Override
  public boolean isAUser(String userName) throws NoSuchObjectException,
      MetaException, TException {
    return client.isAUserName(userName);
  }

  @Override
  public List<DbPriv> getAuthOnDbs(String byWho, String forWho)
      throws NoSuchObjectException, MetaException, TException {
    return client.getAuthOnDbs(byWho, forWho);
  }

  @Override
  public List<TblPriv> getAuthOnTbls(String byWho, String forWho)
      throws NoSuchObjectException, MetaException, TException {
    return client.getAuthOnTbls(byWho, forWho);
  }

  @Override
  public void modifyTableComment(String dbName, String tblName, String comment)
      throws InvalidOperationException, MetaException, TException {
    client.modifyTableComment(dbName, tblName, comment);
  }

  @Override
  public void modifyColumnComment(String dbName, String tblName,
      String colName, String comment) throws InvalidOperationException,
      MetaException, TException {
    client.modifyColumnComment(dbName, tblName, colName, comment);
  }

  @Override
  public boolean isView(String dbName, String tblName) throws MetaException,
      TException, NoSuchObjectException {
    return client.isView(dbName, tblName);
  }

  @Override
  public boolean createDatabase(String name, String location_uri,
      String hdfsscheme) throws AlreadyExistsException, MetaException,
      TException {
    return client.createDatabase(name, location_uri, hdfsscheme, null);
  }

  public boolean createDatabase(String name, String location_uri,
      String hdfsscheme, String metastore) throws AlreadyExistsException,
      MetaException, TException {
    return client.createDatabase(name, location_uri, hdfsscheme, metastore);
  }
  
  public boolean hasAuthOnLocation(String who, String location)
      throws MetaException, TException {
    return client.hasAuthOnLocation(who, location);
  }

  public boolean hasAuth(String who, int privIndex)
      throws NoSuchObjectException, MetaException, TException {
    return client.hasAuth(who, privIndex);
  }

  public boolean hasAuthOnDb(String who, String db, int privIndex)
      throws NoSuchObjectException, MetaException, TException {
    return client.hasAuthOnDb(who, db, privIndex);
  }

  public boolean hasAuthOnTbl(String who, String db, String table, int privIndex)
      throws NoSuchObjectException, MetaException, TException {
    return client.hasAuthOnTbl(who, db, table, privIndex);
  }

  public boolean hasAuthWithRole(String who, String role, int privIndex)
      throws NoSuchObjectException, MetaException, TException {
    return client.hasAuthWithRole(who, role, privIndex);
  }

  public boolean hasAuthOnDbWithRole(String who, String role, String db,
      int privIndex) throws NoSuchObjectException, MetaException, TException {
    return client.hasAuthOnDbWithRole(who, role, db, privIndex);
  }

  public boolean hasAuthOnTblWithRole(String who, String role, String db,
      String table, int privIndex) throws NoSuchObjectException, MetaException,
      TException {
    return client.hasAuthOnTblWithRole(who, role, db, table, privIndex);
  }

  public boolean createDatabase(Database db) throws AlreadyExistsException,
      MetaException, TException {
    return client.createDatabaseDb(db);
  }

  public boolean updatePBInfo(String dbname, String tablename,
      String modified_time) throws MetaException, TException {
    try {
      return client.updatePBInfo(dbname, tablename, modified_time);
    } catch (MetaException e) {
      e.printStackTrace();
    } catch (TException e) {
      e.printStackTrace();
    }
    return false;
  }

  public List<String> getDatabases(String owner) throws MetaException,
      TException {
    return client.getDatabasesWithOwner(owner);
  }

  public boolean isPBTable(String dbname, String tablename) {
    try {
      return client.isPBTable(dbname, tablename);
    } catch (MetaException e) {
      e.printStackTrace();
    } catch (TException e) {
      e.printStackTrace();
    }
    return false;
  }
  
  public boolean isHdfsExternalTable(String dbname, String tablename) {
    try {
      return client.isHdfsExternalTable(dbname, tablename);
    } catch (MetaException e) {
      e.printStackTrace();
    } catch (TException e) {
      e.printStackTrace();
    }
    return false;
  }
}
