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

import org.apache.hadoop.hive.metastore.api.AddPartitionDesc;
import org.apache.hadoop.hive.metastore.api.AddSerdeDesc;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
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

import org.apache.thrift.TException;

public interface IMetaStoreClient {

  public void close();

  public List<String> getTables(String dbName, String tablePattern)
      throws MetaException, UnknownTableException, TException,
      UnknownDBException;

  public void dropTable(String tableName, boolean deleteData)
      throws MetaException, UnknownTableException, TException,
      NoSuchObjectException;

  public void dropTable(String dbname, String tableName, boolean deleteData,
      boolean ignoreUknownTab) throws MetaException, TException,
      NoSuchObjectException;

  public boolean tableExists(String tableName) throws MetaException,
      TException, UnknownDBException;

  public Table getTable(String tableName) throws MetaException, TException,
      NoSuchObjectException;

  public Table getTable(String dbName, String tableName) throws MetaException,
      TException, NoSuchObjectException;

  public Partition getPriPartition(String dbName, String tblName)
      throws MetaException, TException;

  public void setPriPartition(String dbName, String tblName, Partition priPart)
      throws MetaException, TException, InvalidOperationException;

  public Partition getSubPartition(String dbName, String tblName)
      throws MetaException, TException;

  public void setSubPartition(String dbName, String tblName, Partition subPart)
      throws MetaException, TException, InvalidOperationException;

  public List<Partition> getPartitions(String dbName, String tblName)
      throws MetaException, TException, NoSuchObjectException;

  public void setParititions(String dbName, String tblName,
      List<Partition> partitions) throws MetaException, TException,
      InvalidOperationException;

  public void createTable(Table tbl) throws AlreadyExistsException,
      InvalidObjectException, MetaException, NoSuchObjectException, TException;

  public void alter_table(String defaultDatabaseName, String tblName,
      Table table) throws InvalidOperationException, MetaException, TException;

  public boolean createDatabase(String name, String location_uri,
      String hdfsscheme) throws AlreadyExistsException, MetaException,
      TException;

  public boolean dropDatabase(String name) throws MetaException, TException,
      NoSuchObjectException;

  public List<FieldSchema> getFields(String db, String tableName)
      throws MetaException, TException, UnknownTableException,
      UnknownDBException;

  public List<FieldSchema> getSchema(String db, String tableName)
      throws MetaException, TException, UnknownTableException,
      UnknownDBException;

  public tdw_sys_table_statistics add_table_statistics(
      tdw_sys_table_statistics new_table_statistics)
      throws InvalidObjectException, AlreadyExistsException, MetaException,
      TException;

  public boolean delete_table_statistics(String table_statistics_name,
      String db_statistics_name) throws NoSuchObjectException, MetaException,
      TException;

  public tdw_sys_table_statistics get_table_statistics(
      String table_statistics_name, String db_statistics_name)
      throws MetaException, TException;

  public List<tdw_sys_table_statistics> get_table_statistics_multi(
      String db_statistics_name, int max_parts) throws NoSuchObjectException,
      MetaException, TException;

  public List<String> get_table_statistics_names(String db_statistics_name,
      int max_parts) throws NoSuchObjectException, MetaException, TException;

  public tdw_sys_fields_statistics add_fields_statistics(
      tdw_sys_fields_statistics new_fields_statistics)
      throws InvalidObjectException, AlreadyExistsException, MetaException,
      TException;

  public boolean delete_fields_statistics(String table_statistics_name,
      String db_statistics_name, String fields_name)
      throws NoSuchObjectException, MetaException, TException;

  public tdw_sys_fields_statistics get_fields_statistics(
      String table_statistics_name, String db_statistics_name,
      String fields_name) throws MetaException, TException;

  public List<tdw_sys_fields_statistics> get_fields_statistics_multi(
      String table_statistics_name, String db_statistics_name, int max_parts)
      throws NoSuchObjectException, MetaException, TException;

  public List<String> get_fields_statistics_names(String table_statistics_name,
      String db_statistics_name, int max_parts) throws NoSuchObjectException,
      MetaException, TException;

  public boolean add_tdw_query_info(tdw_query_info query_info)
      throws MetaException, TException;

  public boolean add_tdw_query_stat(tdw_query_stat query_stat)
      throws MetaException, TException;

  public boolean update_tdw_query_info(String qid, String finishtime,
      String state) throws MetaException, TException;

  public boolean update_tdw_query_stat(String qid, String finishtime,
      String state) throws MetaException, TException;

  public List<tdw_query_info> get_tdw_query_info() throws MetaException,
      TException;

  public List<tdw_query_stat> get_tdw_query_stat() throws MetaException,
      TException;

  public boolean clear_tdw_query_info(int days) throws MetaException,
      TException;

  public boolean clear_tdw_query_stat(int days) throws MetaException,
      TException;

  boolean add_user_group(group newgroup, String user) throws MetaException,
      TException;

  int drop_user_group(String groupname, String user) throws MetaException,
      TException;

  String get_groupname(String username) throws MetaException, TException;

  int revoke_user_group(String groupname, String namelist, String user)
      throws MetaException, TException;

  int grant_user_group(String groupname, String namelist, String user)
      throws MetaException, TException;

  List<group> get_groups(String pattern) throws MetaException, TException;

  public boolean create_user(String byWho, String newUser, String passwd)
      throws AlreadyExistsException, TException, MetaException;

  public boolean drop_user(String byWho, String userName)
      throws NoSuchObjectException, TException, MetaException;

  public User get_user(String byWho, String userName)
      throws NoSuchObjectException, TException, MetaException;

  public List<String> get_users_all(String byWho) throws TException,
      MetaException;

  public boolean set_passwd(String byWho, String forWho, String newPasswd)
      throws NoSuchObjectException, TException, MetaException;

  public boolean is_a_user(String userName, String passwd) throws TException,
      MetaException;

  public boolean is_a_role(String roleName) throws TException, MetaException;

  public boolean create_role(String byWho, String roleName)
      throws AlreadyExistsException, TException, MetaException;

  public boolean drop_role(String byWho, String roleName)
      throws NoSuchObjectException, TException, MetaException;

  public Role get_role(String byWho, String roleName)
      throws NoSuchObjectException, TException, MetaException;

  public List<String> get_roles_all(String byWho) throws TException,
      MetaException;

  public boolean grant_auth_sys(String byWho, String userName,
      List<String> privileges) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException;

  public boolean grant_auth_role_sys(String byWho, String roleName,
      List<String> privileges) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException;

  public boolean grant_role_to_user(String byWho, String userName,
      List<String> roleNames) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException;

  public boolean grant_role_to_role(String byWho, String roleName,
      List<String> roleNames) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException;

  public boolean grant_auth_on_db(String byWho, String forWho,
      List<String> privileges, String db) throws NoSuchObjectException,
      TException, MetaException, InvalidObjectException;

  public boolean grant_auth_on_tbl(String byWho, String forWho,
      List<String> privileges, String db, String tbl)
      throws NoSuchObjectException, TException, InvalidObjectException,
      MetaException;

  public DbPriv get_auth_on_db(String byWho, String who, String db)
      throws NoSuchObjectException, TException, MetaException;

  public List<DbPriv> get_db_auth(String byWho, String db)
      throws NoSuchObjectException, TException, MetaException;

  public List<DbPriv> get_db_auth_all(String byWho)
      throws NoSuchObjectException, TException, MetaException;

  public TblPriv get_auth_on_tbl(String byWho, String who, String db, String tbl)
      throws NoSuchObjectException, TException, MetaException;

  public List<TblPriv> get_tbl_auth(String byWho, String db, String tbl)
      throws NoSuchObjectException, TException, MetaException;

  public List<TblPriv> get_tbl_auth_all(String byWho)
      throws NoSuchObjectException, TException, MetaException;

  public boolean revoke_auth_sys(String byWho, String userName,
      List<String> privileges) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException;

  public boolean revoke_auth_role_sys(String byWho, String roleName,
      List<String> privileges) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException;

  public boolean revoke_role_from_user(String byWho, String userName,
      List<String> roleNames) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException;

  public boolean revoke_role_from_role(String byWho, String roleName,
      List<String> roleNames) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException;

  public boolean revoke_auth_on_db(String byWho, String who,
      List<String> privileges, String db) throws NoSuchObjectException,
      TException, InvalidObjectException, MetaException;

  public boolean revoke_auth_on_tbl(String byWho, String who,
      List<String> privileges, String db, String tbl)
      throws NoSuchObjectException, TException, InvalidObjectException,
      MetaException;

  public boolean drop_auth_on_db(String byWho, String forWho, String db)
      throws TException, MetaException;

  public boolean drop_auth_in_db(String byWho, String forWho)
      throws TException, MetaException;

  public boolean drop_auth_on_tbl(String byWho, String forWho, String db,
      String tbl) throws TException, MetaException;

  public boolean drop_auth_in_tbl(String byWho, String forWho)
      throws TException, MetaException;

  public boolean create_index(IndexItem index) throws MetaException, TException;

  public boolean drop_index(String db, String table, String name)
      throws MetaException, TException;

  public int get_index_num(String db, String table) throws MetaException,
      TException;

  public int get_index_type(String db, String table, String name)
      throws MetaException, TException;

  public String get_index_field(String db, String table, String name)
      throws MetaException, TException;

  public String get_index_location(String db, String table, String name)
      throws MetaException, TException;

  public boolean set_index_location(String db, String table, String name,
      String location) throws MetaException, TException;

  public boolean set_index_status(String db, String table, String name,
      int status) throws MetaException, TException;

  public List<IndexItem> get_all_index_table(String db, String table)
      throws MetaException, TException;

  public IndexItem get_index_info(String db, String table, String name)
      throws MetaException, TException;

  public List<IndexItem> get_all_index_sys() throws MetaException, TException;

  public tdw_sys_table_statistics addTableStatistics(
      tdw_sys_table_statistics newTableStatistics)
      throws InvalidObjectException, AlreadyExistsException, MetaException,
      TException;

  public boolean deleteTableStatistics(String table_statistics_name,
      String dbStatisticsName) throws NoSuchObjectException, MetaException,
      TException;

  public tdw_sys_table_statistics getTableStatistics(
      String tableStatisticsName, String dbStatisticsName)
      throws MetaException, TException;

  public List<tdw_sys_table_statistics> getTableStatisticsMulti(
      String dbStatisticsName, int maxParts) throws NoSuchObjectException,
      MetaException, TException;

  public List<String> getTableStatisticsNames(String dbStatisticsName,
      int maxParts) throws NoSuchObjectException, MetaException, TException;

  public tdw_sys_fields_statistics addFieldsStatistics(
      tdw_sys_fields_statistics newFieldsStatistics)
      throws InvalidObjectException, AlreadyExistsException, MetaException,
      TException;

  public boolean deleteFieldsStatistics(String tableStatisticsName,
      String dbStatisticsName, String fieldsName) throws NoSuchObjectException,
      MetaException, TException;

  public tdw_sys_fields_statistics getFieldsStatistics(
      String tableStatisticsName, String dbStatisticsName, String fieldsName)
      throws MetaException, TException;

  public List<tdw_sys_fields_statistics> getFieldsStatisticsMulti(
      String tableStatisticsName, String dbStatisticsName, int maxParts)
      throws NoSuchObjectException, MetaException, TException;

  public List<String> getFieldsStatistics_names(String tableStatisticsName,
      String dbStatisticsName, int maxParts) throws NoSuchObjectException,
      MetaException, TException;

  public boolean addTdwQueryInfo(tdw_query_info queryInfo)
      throws MetaException, TException;

  public boolean addTdwQueryStat(tdw_query_stat queryStat)
      throws MetaException, TException;

  public boolean updateTdwQueryInfo(String qid, String finishtime, String state)
      throws MetaException, TException;

  public boolean updateTdwQueryStat(String qid, String finishtime, String state)
      throws MetaException, TException;

  public List<tdw_query_info> getTdwQueryInfo() throws MetaException,
      TException;

  public List<tdw_query_stat> getTdwQueryStat() throws MetaException,
      TException;

  public boolean clearTdwQueryInfo(int days) throws MetaException, TException;

  public boolean clearTdwQueryStat(int days) throws MetaException, TException;

  boolean addUserGroup(group newGroup, String user) throws MetaException,
      TException;

  int dropUserGroup(String groupname, String user) throws MetaException,
      TException;

  String getGroupname(String username) throws MetaException, TException;

  int revokeUserGroup(String groupName, String namelist, String user)
      throws MetaException, TException;

  int grantUserGroup(String groupName, String namelist, String user)
      throws MetaException, TException;

  List<group> getGroups(String pattern) throws MetaException, TException;

  public boolean createUer(String byWho, String newUser, String passwd)
      throws AlreadyExistsException, TException, MetaException;

  public boolean dropUser(String byWho, String userName)
      throws NoSuchObjectException, TException, MetaException;

  public User getUser(String byWho, String userName)
      throws NoSuchObjectException, TException, MetaException;

  public List<String> getUsersAll(String byWho) throws TException,
      MetaException;

  public boolean setPasswd(String byWho, String forWho, String newPasswd)
      throws NoSuchObjectException, TException, MetaException;

  public boolean isAUser(String userName, String passwd) throws TException,
      MetaException;

  public boolean isARole(String roleName) throws TException, MetaException;

  public boolean createRole(String byWho, String roleName)
      throws AlreadyExistsException, TException, MetaException;

  public boolean dropRole(String byWho, String roleName)
      throws NoSuchObjectException, TException, MetaException;

  public Role getRole(String byWho, String roleName)
      throws NoSuchObjectException, TException, MetaException;

  public List<String> getRolesAll(String byWho) throws TException,
      MetaException;

  public boolean grantAuthSys(String byWho, String userName,
      List<String> privileges) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException;

  public boolean grantAuthRoleSys(String byWho, String roleName,
      List<String> privileges) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException;

  public boolean grantRoleToUser(String byWho, String userName,
      List<String> roleNames) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException;

  public boolean grantRoleToRole(String byWho, String roleName,
      List<String> roleNames) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException;

  public boolean grantAuthOnDb(String byWho, String forWho,
      List<String> privileges, String db) throws NoSuchObjectException,
      TException, MetaException, InvalidObjectException;

  public boolean grantAuthOnTbl(String byWho, String forWho,
      List<String> privileges, String db, String tbl)
      throws NoSuchObjectException, TException, InvalidObjectException,
      MetaException;

  public DbPriv getAuthOnDb(String byWho, String who, String db)
      throws NoSuchObjectException, TException, MetaException;

  public List<DbPriv> getDbAuth(String byWho, String db)
      throws NoSuchObjectException, TException, MetaException;

  public List<DbPriv> getDbAuthAll(String byWho) throws NoSuchObjectException,
      TException, MetaException;

  public TblPriv getAuthOnTbl(String byWho, String who, String db, String tbl)
      throws NoSuchObjectException, TException, MetaException;

  public List<TblPriv> getTblAuth(String byWho, String db, String tbl)
      throws NoSuchObjectException, TException, MetaException;

  public List<TblPriv> getTblAuthAll(String byWho)
      throws NoSuchObjectException, TException, MetaException;

  public boolean revokeAuthSys(String byWho, String userName,
      List<String> privileges) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException;

  public boolean revokeAuthRoleSys(String byWho, String roleName,
      List<String> privileges) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException;

  public boolean revokeRoleFromUser(String byWho, String userName,
      List<String> roleNames) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException;

  public boolean revokeRoleFromRole(String byWho, String roleName,
      List<String> roleNames) throws NoSuchObjectException, TException,
      InvalidObjectException, MetaException;

  public boolean revokeAuthOnDb(String byWho, String who,
      List<String> privileges, String db) throws NoSuchObjectException,
      TException, InvalidObjectException, MetaException;

  public boolean revokeAuthOnTbl(String byWho, String who,
      List<String> privileges, String db, String tbl)
      throws NoSuchObjectException, TException, InvalidObjectException,
      MetaException;

  public boolean dropAuthOnDb(String byWho, String forWho, String db)
      throws TException, MetaException;

  public boolean dropAuthInDb(String byWho, String forWho) throws TException,
      MetaException;

  public boolean dropAuthOnTbl(String byWho, String forWho, String db,
      String tbl) throws TException, MetaException;

  public boolean dropAuthInTbl(String byWho, String forWho) throws TException,
      MetaException;

  public boolean createIndex(IndexItem index) throws MetaException, TException;

  public boolean dropUndex(String db, String table, String name)
      throws MetaException, TException;

  public int getUndexNum(String db, String table) throws MetaException,
      TException;

  public int getIndexType(String db, String table, String name)
      throws MetaException, TException;

  public String getIndexField(String db, String table, String name)
      throws MetaException, TException;

  public String getIndexLocation(String db, String table, String name)
      throws MetaException, TException;

  public boolean setIndexLocation(String db, String table, String name,
      String location) throws MetaException, TException;

  public boolean setIndexStatus(String db, String table, String name, int status)
      throws MetaException, TException;

  public List<IndexItem> getAllIndexTable(String db, String table)
      throws MetaException, TException;

  public IndexItem getIndexInfo(String db, String table, String name)
      throws MetaException, TException;

  public List<IndexItem> getAllIndexSys() throws MetaException, TException;

  public boolean isTableExist(String dbName, String tableName)
      throws MetaException, TException;

  public void addPartition(String dbName, String tblName,
      AddPartitionDesc addPartitionDesc) throws InvalidOperationException,
      MetaException, TException;

  public void dropPartition(String dbName, String tblName,
      DropPartitionDesc dropPartitionDesc) throws InvalidOperationException,
      MetaException, TException;

  public void addDefaultPartition(String dbName, String tblName, int level)
      throws InvalidOperationException, MetaException, TException;

  public void dropDefaultPartition(String dbName, String tblName, int level)
      throws InvalidOperationException, MetaException, TException;

  public void renameTable(String dbName, String tblName, String modifyUser,
      String newName) throws InvalidOperationException, MetaException,
      TException;

  public void addCols(String dbName, String tblName, String modifyUser,
      List<FieldSchema> newCols) throws InvalidOperationException,
      MetaException, TException;

  public void renameCols(String dbName, String tblName,
      RenameColDesc renameColDesc) throws InvalidOperationException,
      MetaException, TException;

  public void replaceCols(String dbName, String tblName, String modifyUser,
      List<FieldSchema> newCols) throws InvalidOperationException,
      MetaException, TException;

  public void addTblProps(String dbName, String tblName, String modifyUser,
      Map<String, String> props) throws InvalidOperationException,
      MetaException, TException;

  public void addSerdeProps(String dbName, String tblName, String modifyUser,
      Map<String, String> props) throws InvalidOperationException,
      MetaException, TException;

  public void addSerde(String dbName, String tblName, AddSerdeDesc addSerdeDesc)
      throws InvalidOperationException, MetaException, TException;

  public List<List<String>> getPartitionNames(String dbName, String tblName,
      short max) throws InvalidOperationException, MetaException, TException;

  public boolean isAUser(String userName) throws NoSuchObjectException,
      MetaException, TException;

  public List<DbPriv> getAuthOnDbs(String byWho, String forWho)
      throws NoSuchObjectException, MetaException, TException;

  public List<TblPriv> getAuthOnTbls(String byWho, String forWho)
      throws NoSuchObjectException, MetaException, TException;

  public void modifyTableComment(String dbName, String tblName, String comment)
      throws InvalidOperationException, MetaException, TException;

  public void modifyColumnComment(String dbName, String tblName,
      String colName, String comment) throws InvalidOperationException,
      MetaException, TException;

  public boolean isView(String dbName, String tblName) throws MetaException,
      TException, NoSuchObjectException;

  public List<String> getPlayRoles(String byWho) throws NoSuchObjectException,
      TException, MetaException;

}
