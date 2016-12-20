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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hive.metastore.api.AddPartitionDesc;
import org.apache.hadoop.hive.metastore.api.AddSerdeDesc;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DbPriv;
import org.apache.hadoop.hive.metastore.api.DropPartitionDesc;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.RenameColDesc;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TblPriv;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.tdw_query_info;
import org.apache.hadoop.hive.metastore.api.tdw_query_stat;
import org.apache.hadoop.hive.metastore.api.tdw_sys_fields_statistics;
import org.apache.hadoop.hive.metastore.api.tdw_sys_table_statistics;
import org.apache.hadoop.hive.metastore.api.User;
import org.apache.hadoop.hive.metastore.api.group;
import org.apache.hadoop.hive.metastore.api.IndexItem;
import org.apache.hadoop.hive.metastore.model.MGroup;
import org.apache.hadoop.hive.metastore.model.Mtdw_query_info;
import org.apache.hadoop.hive.metastore.model.Mtdw_query_stat;
import org.apache.hadoop.hive.metastore.api.TableInfo;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.thrift.TException;

public interface RawStore extends Configurable {

  public abstract void shutdown();

  public abstract boolean openTransaction();

  public abstract boolean commitTransaction();

  public abstract void rollbackTransaction();

  public abstract boolean createDatabase(Database db) throws MetaException;

  public abstract boolean createDatabase(String name) throws MetaException;

  public abstract Database getDatabase(String name)
      throws NoSuchObjectException, MetaException;

  public abstract boolean dropDatabase(String dbname) throws MetaException;

  public abstract List<String> getDatabases() throws MetaException;

  public abstract boolean createType(Type type);

  public abstract Type getType(String typeName);

  public abstract boolean dropType(String typeName);

  public abstract void createTable(Table tbl) throws InvalidObjectException,
      MetaException, AlreadyExistsException;

  public abstract boolean dropTable(String dbName, String tableName)
      throws MetaException, NoSuchObjectException;

  public abstract Table getTable(String dbName, String tableName)
      throws MetaException, NoSuchObjectException;

  public abstract Partition getPartition(String dbName, String tableName,
      int level) throws MetaException;

  public abstract List<List<String>> getPartitionNames(String dbName,
      String tableName) throws MetaException;

  public abstract List<String> getPartitionNames(String dbName,
      String tableName, int level) throws MetaException;

  public abstract void alterPartition(String dbName, String tableName,
      Partition new_part) throws InvalidObjectException, MetaException;

  public abstract void alterTable(String dbname, String name, Table newTable)
      throws InvalidObjectException, MetaException;

  public List<String> getTables(String dbName, String pattern)
      throws MetaException;

  public abstract boolean add_table_statistics(
      tdw_sys_table_statistics new_table_statistics) throws MetaException;

  public abstract boolean delete_table_statistics(String table_statistics_name,
      String db_statistics_name) throws MetaException;

  public abstract tdw_sys_table_statistics get_table_statistics(
      String table_statistics_name, String db_statistics_name)
      throws MetaException;

  public abstract List<tdw_sys_table_statistics> get_table_statistics_multi(
      String db_statistics_name, int max) throws MetaException;

  public abstract List<String> get_table_statistics_names(
      String db_statistics_name, int max) throws MetaException;

  public abstract boolean add_fields_statistics(
      tdw_sys_fields_statistics new_fields_statistics) throws MetaException;

  public abstract boolean delete_fields_statistics(
      String table_statistics_name, String db_statistics_name,
      String fields_statistics_name) throws MetaException;

  public abstract tdw_sys_fields_statistics get_fields_statistics(
      String table_statistics_name, String table_db_name,
      String fields_statistics_name) throws MetaException;

  public abstract List<tdw_sys_fields_statistics> get_fields_statistics_multi(
      String table_statistics_name, String db_statistics_name, int max)
      throws MetaException;

  public abstract List<String> get_fields_statistics_names(
      String table_statistics_name, String db_statistics_name, int max)
      throws MetaException;

  public abstract boolean add_tdw_query_info(tdw_query_info queryInfo)
      throws MetaException;

  public abstract boolean add_tdw_query_stat(tdw_query_stat query_stat)
      throws MetaException;

  public abstract boolean update_tdw_query_info(String qid, String finishtime,
      String state) throws MetaException;

  public abstract boolean update_tdw_query_stat(String qid, String finishtime,
      String state) throws MetaException;

  public abstract List<tdw_query_info> get_tdw_query_info()
      throws MetaException;

  public abstract List<tdw_query_stat> get_tdw_query_stat()
      throws MetaException;

  public abstract boolean clear_tdw_query_info(int days) throws MetaException;

  public abstract boolean clear_tdw_query_stat(int days) throws MetaException;

  public abstract tdw_query_info search_tdw_query_info(String qid)
      throws MetaException;

  public abstract boolean add_user_group(group newgroup, String user)
      throws MetaException;

  public abstract int drop_user_group(String groupname, String user)
      throws MetaException;

  public abstract String get_groupname(String username) throws MetaException;

  public abstract int revoke_user_group(String groupname, String namelist,
      String user) throws MetaException;

  public abstract int grant_user_group(String groupname, String namelist,
      String user) throws MetaException;

  public abstract List<group> get_groups(String pattern) throws MetaException;

  public abstract MGroup findGroup(String gname) throws MetaException;

  public abstract boolean createUser(String user, String passwd)
      throws AlreadyExistsException, MetaException;

  public abstract boolean dropUser(String userName)
      throws NoSuchObjectException, MetaException;

  public abstract User getUser(String userName) throws NoSuchObjectException,
      MetaException;

  public abstract List<String> getUsersAll() throws MetaException;

  public abstract boolean setPasswd(String userName, String newPasswd)
      throws NoSuchObjectException, MetaException;

  public abstract boolean isAUser(String userName, String passwd)
      throws MetaException;

  public abstract boolean grantAuthSys(String userName, List<String> privileges)
      throws NoSuchObjectException, InvalidObjectException, MetaException;

  public abstract boolean grantRoleToUser(String userName, List<String> roles)
      throws NoSuchObjectException, InvalidObjectException, MetaException;

  public abstract boolean revokeAuthSys(String userName, List<String> privileges)
      throws NoSuchObjectException, InvalidObjectException, MetaException;

  public abstract boolean revokeRoleFromUser(String userName, List<String> roles)
      throws NoSuchObjectException, InvalidObjectException, MetaException;

  public abstract boolean isARole(String roleName) throws MetaException;

  public abstract boolean createRole(String roleName)
      throws AlreadyExistsException, MetaException;

  public abstract boolean dropRole(String roleName)
      throws NoSuchObjectException, MetaException;

  public abstract boolean grantRoleToRole(String roleName, List<String> roles)
      throws NoSuchObjectException, InvalidObjectException, MetaException;

  public abstract boolean revokeRoleFromRole(String roleName, List<String> roles)
      throws NoSuchObjectException, InvalidObjectException, MetaException;

  public abstract Role getRole(String roleName) throws NoSuchObjectException,
      MetaException;

  public abstract List<String> getPlayRoles(String userName)
      throws NoSuchObjectException, MetaException;

  public abstract List<String> getRolesAll() throws MetaException;

  public abstract boolean grantAuthRoleSys(String role, List<String> privileges)
      throws NoSuchObjectException, InvalidObjectException, MetaException;

  public abstract boolean revokeAuthRoleSys(String role, List<String> privileges)
      throws NoSuchObjectException, InvalidObjectException, MetaException;

  public abstract boolean grantAuthOnDb(String forWho, List<String> privileges,
      String db) throws NoSuchObjectException, InvalidObjectException,
      MetaException;

  public abstract DbPriv getAuthOnDb(String who, String db)
      throws MetaException;

  public abstract List<DbPriv> getDbAuth(String db) throws MetaException;

  public abstract List<DbPriv> getAuthOnDbs(String who) throws MetaException;

  public abstract List<DbPriv> getDbAuthAll() throws MetaException;

  public abstract boolean revokeAuthOnDb(String who, List<String> privileges,
      String db) throws NoSuchObjectException, InvalidObjectException,
      MetaException;

  public abstract boolean dropAuthOnDb(String who, String db)
      throws MetaException;

  public abstract boolean dropAuthInDb(String who) throws MetaException;

  public abstract boolean grantAuthOnTbl(String forWho,
      List<String> privileges, String db, String tbl)
      throws NoSuchObjectException, InvalidObjectException, MetaException;

  public abstract TblPriv getAuthOnTbl(String who, String db, String tbl)
      throws MetaException;

  public abstract List<TblPriv> getTblAuth(String db, String tbl)
      throws MetaException;

  public abstract List<TblPriv> getAuthOnTbls(String who) throws MetaException;

  public abstract List<TblPriv> getTblAuthAll() throws MetaException;

  public abstract boolean revokeAuthOnTbl(String who, List<String> privileges,
      String db, String tbl) throws NoSuchObjectException,
      InvalidObjectException, MetaException;

  public abstract boolean dropAuthOnTbl(String who, String db, String tbl)
      throws MetaException;

  public abstract boolean dropAuthInTbl(String who) throws MetaException;

  public abstract boolean createIndex(IndexItem index) throws MetaException;

  public abstract boolean dropIndex(String db, String table, String name)
      throws MetaException;

  public abstract int getIndexNum(String db, String table) throws MetaException;

  public abstract int getIndexType(String db, String table, String name)
      throws MetaException;

  public abstract String getIndexField(String db, String table, String name)
      throws MetaException;

  public abstract String getIndexLocation(String db, String table, String name)
      throws MetaException;

  public abstract boolean setIndexLocation(String db, String table,
      String name, String location) throws MetaException;

  public abstract boolean setIndexStatus(String db, String table, String name,
      int status) throws MetaException;

  public abstract List<IndexItem> getAllIndexTable(String db, String table)
      throws MetaException;

  public abstract IndexItem getIndexInfo(String db, String table, String name)
      throws MetaException;

  public abstract List<IndexItem> getAllIndexSys() throws MetaException;

  public abstract List<FieldSchema> getPartFieldsJdbc(String dbName,
      String tableName) throws MetaException;

  public abstract List<FieldSchema> getFieldsJdbc(String dbName,
      String tableName) throws MetaException;

  public abstract List<String> getJdbcTables(String dbName, String pattern)
      throws MetaException;

  public abstract boolean isTableExit(String dbName, String tblName)
      throws MetaException;

  public abstract boolean dropTable(String dbName, String tblName,
      boolean isDeleteData) throws MetaException, NoSuchObjectException;

  public abstract boolean addUserGroup(group newGroup, String user)
      throws MetaException;

  public abstract int dropUserGroup(String groupName, String user)
      throws MetaException;

  public abstract String getGroupname(String userName) throws MetaException;

  public abstract int revokeUserGroup(String groupName, String namelist,
      String user) throws MetaException;

  public abstract int grantUserGroup(String groupName, String namelist,
      String user) throws MetaException;

  public abstract List<group> getGroups(String pattern) throws MetaException;

  public abstract boolean createDatabase(Database db, String slaveURL)
      throws MetaException;

  public abstract void addPartition(String dbName, String tblName,
      AddPartitionDesc addPartitionDesc) throws InvalidObjectException,
      MetaException;

  public abstract void dropPartition(String dbName, String tblName,
      DropPartitionDesc dropPartitionDesc) throws InvalidObjectException,
      MetaException;

  public abstract void addDefaultPartition(String dbName, String tblName,
      int level) throws InvalidObjectException, MetaException;

  public abstract void dropDefaultPartition(String dbName, String tblName,
      int level) throws InvalidObjectException, MetaException;

  public abstract void renameTable(String dbName, String tblName,
      String modifyUser, String newName) throws InvalidOperationException,
      MetaException;

  public abstract void addCols(String dbName, String tblName,
      String modifyUser, List<FieldSchema> newCols)
      throws InvalidOperationException, MetaException, InvalidObjectException;

  public abstract void renameCols(String dbName, String tblName,
      RenameColDesc renameColDesc) throws InvalidOperationException,
      MetaException, InvalidObjectException;

  public abstract void replaceCols(String dbName, String tblName,
      String modifyUser, List<FieldSchema> newCols)
      throws InvalidOperationException, MetaException;

  public abstract void addTblProps(String dbName, String tblName,
      String modifyUser, Map<String, String> props)
      throws InvalidOperationException, MetaException;

  public abstract void addSerdeProps(String dbName, String tblName,
      String modifyUser, Map<String, String> props)
      throws InvalidOperationException, MetaException;

  public abstract void addSerde(String dbName, String tblName,
      AddSerdeDesc addSerdeDesc) throws InvalidOperationException,
      MetaException;

  public abstract void modifyTableComment(String dbName, String tblName,
      String comment) throws InvalidOperationException, MetaException;

  public abstract void modifyColumnComment(String dbName, String tblName,
      String colName, String comment) throws InvalidOperationException,
      MetaException;

  public abstract boolean isView(String dbName, String tblName)
      throws NoSuchObjectException, MetaException;

  public abstract boolean isAUserName(String userName)
      throws NoSuchObjectException, MetaException;

  public abstract boolean hasAuthOnLocation(String who, String location)
      throws NoSuchObjectException, MetaException;

  public abstract boolean hasAuth(String who, int privIndex)
      throws NoSuchObjectException, MetaException;

  public abstract boolean hasAuthOnDb(String who, String db, int privIndex)
      throws NoSuchObjectException, MetaException;

  public abstract boolean hasAuthOnTbl(String who, String db, String table,
      int privIndex) throws NoSuchObjectException, MetaException;

  public abstract boolean hasAuthWithRole(String who, String role, int privIndex)
      throws NoSuchObjectException, MetaException;

  public abstract boolean hasAuthOnDbWithRole(String who, String role,
      String db, int privIndex) throws NoSuchObjectException, MetaException;

  public abstract boolean hasAuthOnTblWithRole(String who, String role,
      String db, String table, int privIndex) throws NoSuchObjectException,
      MetaException;

  public abstract boolean updatePBInfo(String dbName, String tableName,
      String modifiedTime) throws InvalidOperationException, MetaException;

  public abstract List<String> getDatabasesWithOwner(String owner)
      throws MetaException;

  public abstract boolean isPBTable(String dbName, String tableName)
      throws MetaException;

  public abstract List<String> getDbsByPriv(String user, String passwd)
      throws MetaException;

  public abstract Map<String, String> getTblsByPriv(String user, String passwd,
      String db) throws NoSuchObjectException, MetaException;

  public abstract TableInfo getTblInfo(String user, String passwd, String db,
      String tbl) throws NoSuchObjectException, MetaException;

  public abstract List<TableInfo> getAllTblsByPriv(String user, String passwd)
      throws MetaException;

  public abstract Map<String, TableInfo> getAllTblsByPrivWithKeyword(
      String user, String passwd, String db, String keyWord)
      throws MetaException;

  public abstract Map<String, TableInfo> getTblsByPrivWithParams(String user,
      String passwd, String db) throws NoSuchObjectException, MetaException;

  public abstract List<String> getDbsByOwner(String user, String passwd)
      throws NoSuchObjectException, MetaException;
  
  public abstract boolean isHdfsExternalTable(String dbName, String tableName)
      throws MetaException;

}
