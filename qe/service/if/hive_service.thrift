#!/usr/local/bin/thrift -java

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# Thrift Service that the hive service is built on
#

#
# TODO: include/thrift is shared among different components. It
# should not be under metastore.

include "thrift/fb303/if/fb303.thrift"
include "metastore/if/hive_metastore.thrift"
#include "ql/if/queryplan.thrift"

namespace java org.apache.hadoop.hive.service
namespace cpp Apache.Hadoop.Hive

// Enumeration of JobTracker.State                                                                      
enum JobTrackerState {                                                                   
  INITIALIZING   = 1,           
  RUNNING        = 2,                                                      
}  

// Map-Reduce cluster status information
struct HiveClusterStatus {
  1: i32              taskTrackers,
  2: i32              mapTasks,
  3: i32              reduceTasks,
  4: i32              maxMapTasks,
  5: i32              maxReduceTasks,
  6: JobTrackerState  state,
}

exception HiveServerException {
  1: string message
  2: optional i32 errorCode
  3: optional string SQLState
}

# Interface for Thrift Hive Server
service ThriftHive extends hive_metastore.ThriftHiveMetastore {
  # Execute a query. Takes a HiveQL string
  string execute(1:string query) throws(1:HiveServerException ex)
##modify by payniexiao for query process
  string jdbc_execute(1:string query, 2:string queryID) throws(1:HiveServerException ex)

  # Fetch one row. This row is the serialized form
  # of the result of the query
  string fetchOne() throws(1:HiveServerException ex)

  # Fetch a given number of rows or remaining number of
  # rows whichever is smaller.
  list<string> fetchN(1:i32 numRows) throws(1:HiveServerException ex)

  # Fetch all rows of the query result
  list<string> fetchAll() throws(1:HiveServerException ex)

  # Get a schema object with fields represented with native Hive types
  hive_metastore.Schema getSchema() throws(1:HiveServerException ex)

  # Get a schema object with fields represented with Thrift DDL types
  hive_metastore.Schema getThriftSchema() throws(1:HiveServerException ex)
  
  # Get the status information about the Map-Reduce cluster
  HiveClusterStatus getClusterStatus() throws(1:HiveServerException ex)

  # Create a session
  list<string> createSession(1:string name) throws(1:HiveServerException ex)

  # Require a session
  string requireSession(1:string sid, 2:string svid) throws(1:HiveServerException ex)
  
  # Detach a session
  i32 detachSession(1:string sid, 2:string svid) throws(1:HiveServerException ex)
  
  # Drop a session
  i32 dropSession(1:string sid, 2:string svid) throws(1:HiveServerException ex)
  
  # Show sessions
  list<string> showSessions() throws(1:HiveServerException ex)
  
  # Upload and Run a job
  i32 uploadJob(1:string job) throws(1:HiveServerException ex)
  
  # Kill a running job
  i32 killJob() throws(1:HiveServerException ex)
  
  # Config job
  i32 configJob(1:string config) throws(1:HiveServerException ex)
  
  # Get the job status
  list<string> getJobStatus(1:i32 jobid) throws(1:HiveServerException ex)
  
  # Get the Environment Variables
  list<string> getEnv() throws(1:HiveServerException ex)
  
  # Get the queryplan annotated with counter information
  #queryplan.QueryPlan getQueryPlan() throws(1:HiveServerException ex)


  # Audit a user/passed
  i32 audit(1:string user, 2:string passwd, 3:string dbname) throws(1:HiveServerException ex)
  
  # Set the hive history file to info file
  void setHistory(1:string sid, 2:i32 jobid) throws(1:HiveServerException ex)
  
  # Get the hive history file of the job
  string getHistory(1:i32 jobid) throws(1:HiveServerException ex)
  
  # Compile the SQL statement
  string compile(1:string query) throws (1:HiveServerException ex)

  # Upload a file to the server
  string upload(1:string rtype, 2:string user, 3:string fileName, 4:string data) throws (1:HiveServerException ex)
  
  # Preprocess proto file & create jar package for create table.
  string makejar(1:string dbname, 2:string tblname, 3:string filename, 4:string username) throws(1:HiveServerException ex)

  # clean up last Hive query (releasing locks etc.)
  void clean()
  # Preprocess proto file for create table.
  string preproto(1:string dbname, 2:string tblname, 3:string filename, 4:string username) throws(1:HiveServerException ex)

  # create jar package from proto file 
  string genjar(1:string dbname, 2:string tblname, 3:string filename) throws(1:HiveServerException ex)

  # Upload a module to the server
  string uploadModule(1:string user, 2:string moduleName, 3:binary module1) throws (1:HiveServerException ex)
  
  # download a module from the server
  binary downloadModule(1:string user, 2:string moduleName) throws (1:HiveServerException ex)
  
  # list a user's modules
  list<string> listModule(1:string user) throws (1:HiveServerException ex)
  
  # get the %rowcount of the last SQL execution
  string getRowCount() throws (1:HiveServerException ex)

  #get sql execute process
  i32 getSQLProcess(1:string sqlID) throws(1:HiveServerException ex)
  
  #kill sql execute process
  i32 killSQLQuery(1:string sqlID) throws(1:HiveServerException ex)
  
  #data sync
  i32 dataSync(1:string createSQL, 2:string tableName, 3:string data, 4:string partName, 5:string subPartName, 6:i32 mode) throws(1:HiveServerException ex)
  
  #data import
  string getLoader(1:string destDB, 2:string destTable, 3:i32 mode) throws(1:HiveServerException ex)
  i32 releaseLoader(1:string loaderID) throws(1:HiveServerException ex)
  i32 uploadData(1:string loaderID, 2:string data) throws(1:HiveServerException ex)
  i32 completeUploadData(1:string loaderID) throws(1:HiveServerException ex)
  
  ##resolve 99% problem
  i32 querySQLState(1:string sqlID) throws(1:HiveServerException ex)
  
  ##open jobtracker url
  string getJobID(1:string sqlID) throws(1:HiveServerException ex)
  
  ##add get running mapreduce infomation
  list<string> getJobCounts(1:string sqlID) throws(1:HiveServerException ex)
  
  ##get load of qe
  list<string> getHiveLoad() throws(1:HiveServerException ex)
  
  ##get scheam for explain
  list<string> getSchemaForExplain(1:string sql) throws(1:HiveServerException ex)
  
  ##replace jar of pb table
  i32 replaceJar(1:string dbname, 2:string tablename, 3:string user) throws (1:HiveServerException ex)
    
  ##Upload a proto file to the server
  string uploadProto(1:string user, 2:string fileName, 3:binary data) throws (1:HiveServerException ex)
}