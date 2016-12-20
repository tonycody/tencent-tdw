#!/usr/local/bin/thrift -java
#
# Thrift Service that the MetaStore is built on
#

include "thrift/fb303/if/fb303.thrift"

namespace java org.apache.hadoop.hive.metastore.api
namespace php metastore
namespace cpp Apache.Hadoop.Hive

struct Version {
  1: string version,
  2: string comments
}

struct FieldSchema {
  1: string name, // name of the field
  2: string type, // type of the field. primitive types defined above, specify list<TYPE_NAME>, map<TYPE_NAME, TYPE_NAME> for lists & maps
  3: string comment
}

struct Type {
  1: string          name,             // one of the types in PrimitiveTypes or CollectionTypes or User defined types
  2: optional string type1,            // object type if the name is 'list' (LIST_TYPE), key type if the name is 'map' (MAP_TYPE)
  3: optional string type2,            // val type if the name is 'map' (MAP_TYPE)
  4: optional list<FieldSchema> fields // if the name is one of the user defined types
}

// namespace for tables
struct Database {
  1: string name,
  2: string description,
  3: string hdfsscheme,
  4: string metastore,
  5: string owner
}

// This object holds the information needed by SerDes
struct SerDeInfo {
  1: string name,                   // name of the serde, table name by default
  2: string serializationLib,       // usually the class that implements the extractor & loader
  3: map<string, string> parameters // initialization parameters
}

// sort order of a column (column name along with asc(1)/desc(0))
struct Order {
  1: string col,  // sort column name
  2: i32    order // asc(1) or desc(0)
}

// this object holds all the information about physical storage of the data belonging to a table
struct StorageDescriptor {
  1: list<FieldSchema> cols,  // required (refer to types defined above)
  2: string location,         // defaults to <warehouse loc>/<db loc>/tablename
  3: string inputFormat,      // SequenceFileInputFormat (binary) or TextInputFormat`  or custom format
  4: string outputFormat,     // SequenceFileOutputFormat (binary) or IgnoreKeyTextOutputFormat or custom format
  5: bool   compressed,       // compressed or not
  6: i32    numBuckets,       // this must be specified if there are any dimension columns
  7: SerDeInfo    serdeInfo,  // serialization and deserialization information
  8: list<string> bucketCols, // reducer grouping columns and clustering columns and bucketing columns`
  9: list<Order>  sortCols,   // sort order of the data in each bucket
  10: map<string, string> parameters // any user supplied key value hash
}

// Modified By : guosijie
// Modified Date : 2010-02-05
//   modify the partition metadata structure to support range & list partitions
// struct Partition {
//   1: list<string> values // string value is converted to appropriate partition key type
//   2: string       dbName,
//   3: string       tableName,
//   4: i32          createTime,
//   5: i32          lastAccessTime,
//   6: StorageDescriptor   sd,
//   7: map<string, string> parameters
// }
struct Partition {
  1: string			dbName,		// the database name
  2: string			tableName,	// the table name
  3: i32			level,		// which level is the partition belongs to?
  4: string 		parType,	// which type is the partition? LIST or RANGE?
  5: FieldSchema	parKey,		// the partition key
  6: map<string, list<string> >	parSpaces,	// the partition spaces
}

// table information
// Modified By : guosijie
// Modified Date : 2010-02-05
//   change the table metadata to support range & list partitions.
//   remove the partitionKeys field, and add the priPartition and subPartition fields.
struct Table {
  1: string tableName,                // name of the table
  2: string dbName,                   // database name ('default')
  3: string owner,                    // owner of this table
  4: i32    createTime,               // creation time of the table
  5: i32    lastAccessTime,           // last access time (usually this will be filled from HDFS and shouldn't be relied on)
  6: i32    retention,                // retention time
  7: StorageDescriptor sd,            // storage descriptor of the table
  8: Partition priPartition,		  // the top-level partition
  9: Partition subPartition,		  // the sub-level partition
  // 8: list<FieldSchema> partitionKeys, // partition keys of the table. only primitive types are supported
  10: map<string, string> parameters,   // to store comments or any other user level parameters
  11: string viewOriginalText,         // original view text, null for non-view Added by Brantzhang for HIVE-972
  12: string viewExpandedText,          // expanded view text, null for non-view Added by Brantzhang for HIVE-972
  13: string tableType,                 // table type enum, e.g. EXTERNAL_TABLE
  14: string vtables,                    // vtables null for non-view Added by seayi for view tables
  15: bool isReplaceOnExit
}

// index on a hive table is also another table whose columns are the subset of the base table columns along with the offset
// this will automatically generate table (table_name_index_name)
struct Index {
  1: string       indexName, // unique with in the whole database namespace
  2: i32          indexType, // reserved
  3: string       tableName,
  4: string       dbName,
  5: list<string> colNames,  // for now columns will be sorted in the ascending order
  6: string       partName   // partition name
}

// schema of the table/query results etc.
struct Schema {
 // column names, types, comments
 1: list<FieldSchema> fieldSchemas,  // delimiters etc
 2: map<string, string> properties
}


exception MetaException {
  1: string message
}

exception UnknownTableException {
  1: string message
}

exception UnknownDBException {
  1: string message
}

exception AlreadyExistsException {
  1: string message
}

exception InvalidObjectException {
  1: string message
}

exception NoSuchObjectException {
  1: string message
}

exception IndexAlreadyExistsException {
  1: string message
}

exception InvalidOperationException {
  1: string message
}

// joeyli added for statics information collect begin 

struct  tdw_sys_table_statistics { 
  1: string stat_table_name,
  2: string stat_db_name,
  3: i32 stat_num_records,
  4: i32 stat_num_units,
  5: i32 stat_total_size,
  6: i32 stat_num_files,
  7: i32 stat_num_blocks
}

struct  tdw_sys_fields_statistics { 
  1: string stat_table_name,
  2: string stat_db_name,
  3: string stat_field_name,
  4: double stat_nullfac,
  5: i32 stat_avg_field_width,
  6: double stat_distinct_values,
  7: string stat_values_1,
  8: string stat_numbers_1,
  9: string stat_values_2,
  10: string stat_numbers_2,
  11: string stat_values_3,
  12: string stat_numbers_3,
  13: i32 stat_number_1_type,
  14: i32 stat_number_2_type,
  15: i32 stat_number_3_type  
}

// joeyli added for statics information collect end 

//added by michealxu
struct  tdw_query_info{ 
  1: string queryId,
  2: string userName,
  3: string sessionId,
  4: string startTime,
  5: string finishTime,
  6: string queryString,
  7: i32 MRNum,
  8: string ip,
  9: string taskid,
  10: string queryState,
  11: string bIQueryString,
  12: string port,
  13: string clientIp,
  14: string dbName
}

struct tdw_query_stat{ 
  1: string queryId,
  2: i32 currMRIndex,
  3: string currMRId,
  4: string currMRStartTime,
  5: string currMRFinishTime,
  6: string currMRState,
  7: i32 MapNum,
  8: i32 ReduceNum,
  9: string jtIP
}

struct tdw_query_error_info{
  1: string queryId,
  2: string taskId,
  3: string errorTime,
  4: string ip,
  5: string port,
  6: string clientIp,
  7: string errorString,
  8: string errorId 
}

struct group{
  1: string groupName,
  2: string creator,
  3: string userList,
  4: i32 userNum
}
//add end michealxu

// added by BrantZhang for authorization begin

//privilege type

struct User {
  1:  string       userName, // unique with in the whole user namespace
  2:  list<string>    playRoles, 
  3:  bool       selectPriv,
  4:  bool       insertPriv,
  5:  bool       indexPriv, 
  6:  bool       createPriv,
  7:  bool       dropPriv,
  8:  bool       deletePriv,
  9:  bool       alterPriv,
  10: bool       updatePriv,
  11: bool       createviewPriv,
  12: bool       showviewPriv,
  13: bool       dbaPriv,
  14: string     groupName
}

struct Role {
  1:  string       roleName, // unique with in the whole role namespace
  2:  list<string>    playRoles, 
  3:  bool       selectPriv,
  4:  bool       insertPriv,
  5:  bool       indexPriv, 
  6:  bool       createPriv,
  7:  bool       dropPriv,
  8:  bool       deletePriv,
  9:  bool       alterPriv,
  10: bool       updatePriv,
  11: bool       createviewPriv,
  12: bool       showviewPriv,
  13: bool       dbaPriv
}

struct DbPriv {
  1:  string       db, 
  2:  string      user, 
  3:  bool       selectPriv,
  4:  bool       insertPriv,
  5:  bool       indexPriv, 
  6:  bool       createPriv,
  7:  bool       dropPriv,
  8:  bool       deletePriv,
  9:  bool       alterPriv,
  10: bool       updatePriv,
  11: bool       createviewPriv,
  12: bool       showviewPriv
}

struct TblPriv {
  1:  string       db, 
  2:  string       tbl,
  3:  string      user, 
  4:  bool       selectPriv,
  5:  bool       insertPriv,
  6:  bool       indexPriv, 
  7:  bool       createPriv,
  8:  bool       dropPriv,
  9:  bool       deletePriv,
  10: bool       alterPriv,
  11: bool       updatePriv
}

// added by BrantZhang for authorization end

// add by konten for index begin
struct IndexItem
{
  1:  string       db, 
  2:  string       tbl,
  3:  string       name, 
  4:  string       fieldList,
  5:  string       location, 
  6:  string       indexPath,
  7:  set<string> partPath,
  8:  i32          type,         // 0, primary; 1, second; 2, union  
  9:  i32          status   // 0, init; 1,building; 2,done;
} 
// add by konten for index end

    struct AddPartitionDesc 
    {
  		1: string			dbName,		// the database name
  		2: string			tableName,	// the table name
  		3: string			user,
  		4: i32			level,		// which level is the partition belongs to?
  		5: string 		partType,	// which type is the partition? LIST or RANGE?
  		6: map<string, list<string> >	parSpaces	// the partition spaces
	}
	
	struct DropPartitionDesc 
    {
  		1: string			dbName,		// the database name
  		2: string			tableName,	// the table name
  		3: string			user,
  		4: i32			level,		// which level is the partition belongs to?
  		5: list<string>			partNames   //part names
	} 	
	
	struct RenameColDesc 
    {
  		1: string			dbName,		// the database name
  		2: string			tableName,	// the table name
  		3: string			user,
  		4: string			oldName,
  		5: string			newName,
  		6: string			type,
  		7: string			comment,
  		8: bool				isFirst,
  		9: string			afterCol
	} 
	
	struct AddSerdeDesc 
    {
  		1: string			dbName,		// the database name
  		2: string			tableName,	// the table name
  		3: string			user,
  		4: string			serdeName,
  		5: map<string, string>  props
	} 
	
	struct ColumnInfo
	{
		1:string name,
		2:string type,
		3:i32 isPart,
		4:string comment,
	}

	struct TableInfo
	{
		1:string comment,
		2:string owner,
		3:string createTime,
		4:list<ColumnInfo> cols,
		5:map<string, string> tblParams
	}

/**
* This interface is live.
*/
service ThriftHiveMetastore extends fb303.FacebookService
{
  bool create_database(1:string name, 2:string description, 3:string hdfsscheme)
                                       throws(1:AlreadyExistsException o1, 2:MetaException o2)
  Database get_database(1:string name) throws(1:NoSuchObjectException o1, 2:MetaException o2)
  bool drop_database(1:string name)    throws(2:MetaException o2)
  list<string> get_databases()         throws(1:MetaException o1)

  // returns the type with given name (make seperate calls for the dependent types if needed)
  Type get_type(1:string name)  throws(1:MetaException o2)
  bool create_type(1:Type type) throws(1:AlreadyExistsException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  bool drop_type(1:string type) throws(1:MetaException o2)
  map<string, Type> get_type_all(1:string name)
                                throws(1:MetaException o2)

  // Gets a list of FieldSchemas describing the columns of a particular table
  list<FieldSchema> get_fields(1: string db_name, 2: string table_name) throws (1: MetaException o1, 2: UnknownTableException o2, 3: UnknownDBException o3),

  // Gets a list of FieldSchemas describing both the columns and the partition keys of a particular table
  list<FieldSchema> get_schema(1: string db_name, 2: string table_name) throws (1: MetaException o1, 2: UnknownTableException o2, 3: UnknownDBException o3)

  // create a Hive table. Following fields must be set
  // tableName
  // database        (only 'default' for now until Hive QL supports databases)
  // owner           (not needed, but good to have for tracking purposes)
  // sd.cols         (list of field schemas)
  // sd.inputFormat  (SequenceFileInputFormat (binary like falcon tables or u_full) or TextInputFormat)
  // sd.outputFormat (SequenceFileInputFormat (binary) or TextInputFormat)
  // sd.serdeInfo.serializationLib (SerDe class name eg org.apache.hadoop.hive.serde.simple_meta.MetadataTypedColumnsetSerDe
  void create_table(1:Table tbl) throws(1:AlreadyExistsException o1, 2:InvalidObjectException o2, 3:MetaException o3, 4:NoSuchObjectException o4)
  // drops the table and all the partitions associated with it if the table has partitions
  // delete data (including partitions) if deleteData is set to true
  void drop_table(1:string dbname, 2:string name, 3:bool deleteData)
                       throws(1:NoSuchObjectException o1, 2:MetaException o3)
  list<string> get_tables(1: string db_name, 2: string pattern)
                       throws (1: MetaException o1)

  Table get_table(1:string dbname, 2:string tbl_name)
                       throws (1:MetaException o1, 2:NoSuchObjectException o2)
  // alter table applies to only future partitions not for existing partitions
  void alter_table(1:string dbname, 2:string tbl_name, 3:Table new_tbl)
                       throws (1:InvalidOperationException o1, 2:MetaException o2)

  // Modification By : guosijie
  // Modification Date : 2010-03-10
  //   change the partition operation interface

  // the following applies to only tables that have partitions
  
  // Partition add_partition(1:Partition new_part)
  //                      throws(1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  // Partition append_partition(1:string db_name, 2:string tbl_name, 3:list<string> part_vals)
  //                      throws (1:InvalidObjectException o1, 2:AlreadyExistsException o2, 3:MetaException o3)
  // bool drop_partition(1:string db_name, 2:string tbl_name, 3:list<string> part_vals, 4:bool deleteData)
  //                      throws(1:NoSuchObjectException o1, 2:MetaException o2)
  Partition get_partition(1:string db_name, 2:string tbl_name, 3:i32 level)
                       throws(1:MetaException o1)
  // returns all the partitions for this table in reverse chronological order.
  // if max parts is given then it will return only that many
  list<Partition> get_partitions(1:string db_name, 2:string tbl_name)
                       throws(1:NoSuchObjectException o1, 2:MetaException o2)
  // list<string> get_partition_names(1:string db_name, 2:string tbl_name, 3:i16 max_parts=-1)
  //                      throws(1:MetaException o2)

  // changes the partition to the new partition object. partition is identified from the part values
  // in the new_part
  void alter_partition(1:string db_name, 2:string tbl_name, 3:Partition new_part)
                       throws(1:InvalidOperationException o1, 2:MetaException o2)
                       
  // Modification end
  
  // joeyli added for statics information collect begin 

  tdw_sys_table_statistics add_table_statistics(1:tdw_sys_table_statistics new_table_statistics)
                       throws(1:AlreadyExistsException o1, 2:MetaException o2)

  bool delete_table_statistics(1:string table_statistics_name, 2:string db_statistics_name)
                       throws(1:NoSuchObjectException o1, 2:MetaException o2)

  tdw_sys_table_statistics get_table_statistics(1:string table_statistics_name, 2:string db_statistics_name)
                       throws(1:MetaException o1)
                       
  list<tdw_sys_table_statistics> get_table_statistics_multi(1:string db_statistics_name, 2:i32 max_parts=-1)
                        throws(1:NoSuchObjectException o1, 2:MetaException o2)

  list<string> get_table_statistics_names(1:string db_statistics_name, 2:i32 max_parts=-1)
                        throws(1:NoSuchObjectException o1, 2:MetaException o2)
                                                                 
  tdw_sys_fields_statistics add_fields_statistics(1:tdw_sys_fields_statistics new_fields_statistics)
                       throws(1:AlreadyExistsException o1, 2:MetaException o2)

  bool delete_fields_statistics(1:string stat_table_name, 2:string db_statistics_name, 3:string stat_field_name)
                       throws(1:NoSuchObjectException o1, 2:MetaException o2)

  tdw_sys_fields_statistics get_fields_statistics(1:string stat_table_name, 2:string db_statistics_name, 3:string stat_field_name)
                       throws(1:MetaException o1)
                       
  list<tdw_sys_fields_statistics> get_fields_statistics_multi(1:string stat_table_name, 2:string db_statistics_name, 3:i32 max_parts=-1)
                        throws(1:NoSuchObjectException o1, 2:MetaException o2)

  list<string> get_fields_statistics_names(1:string stat_table_name, 2:string db_statistics_name, 3:i32 max_parts=-1)
                        throws(1:NoSuchObjectException o1, 2:MetaException o2)
                                                                                         
// joeyli added for statics information collect end              

//added by michealxu

 bool add_tdw_query_info(1:tdw_query_info query_info) throws (1:MetaException o1)
 bool add_tdw_query_stat(1:tdw_query_stat query_stat) throws (1:MetaException o1)
 bool update_tdw_query_info(1:string qid, 2:string finishtime, 3:string state) throws (1:MetaException o1)
 bool update_tdw_query_stat(1:string qid, 2:string finishtime, 3:string state) throws (1:MetaException o1)
 list<tdw_query_info> get_tdw_query_info() throws (1:MetaException o1)
 list<tdw_query_stat> get_tdw_query_stat() throws (1:MetaException o1)
 bool clear_tdw_query_info(1:i32 days) throws (1:MetaException o1)
 bool clear_tdw_query_stat(1:i32 days) throws (1:MetaException o1)
 tdw_query_info search_tdw_query_info(1:string serin) throws (1:MetaException o1) 
  
 bool add_user_group(1:group newgroup, 2: string user) throws (1:MetaException o1)
 i32 drop_user_group(1:string groupname, 2: string user) throws (1:MetaException o1)
 string get_groupname(1:string username) throws (1:MetaException o1)
 i32 revoke_user_group(1:string groupname, 2:string namelist, 3: string user) throws (1:MetaException o1)
 i32 grant_user_group(1:string groupname, 2:string namelist, 3: string user) throws (1:MetaException o1)
 list<group> get_groups(1:string pattern) throws (1:MetaException o1)
//add end  

  // added by BrantZhang for authorization begin
  
  bool          create_user(1:string byWho, 2:string newUser, 3:string passwd)
  									   throws(1:AlreadyExistsException o1, 2:MetaException o2)
  bool          drop_user(1:string byWho, 2:string userName)
  									   throws(1:NoSuchObjectException o1, 2:MetaException o2)
  User          get_user(1:string byWho, 2:string userName)
  			 						   throws(1:NoSuchObjectException o1, 2:MetaException o2)
  list<string>  get_users_all(1:string byWho)
  									   throws(1:MetaException o1)
  bool          set_passwd(1:string byWho, 2:string forWho, 3:string newPasswd)
  									   throws(1:NoSuchObjectException o1, 2:MetaException o2)
  bool          is_a_user(1:string userName, 2:string passwd)
  									   throws(1:MetaException o1)
  
  bool          is_a_role(1:string roleName)
  									   throws(1:MetaException o1)
  bool          create_role(1:string byWho, 2:string roleName)
  									   throws(1:AlreadyExistsException o1, 2:MetaException o2)
  bool          drop_role(1:string byWho, 2:string roleName)
  									   throws(1:NoSuchObjectException o1, 2:MetaException o2)
  Role          get_role(1:string byWho, 2:string roleName)
  									   throws(1:NoSuchObjectException o1, 2:MetaException o2)
  list<string>  get_roles_all(1:string byWho)
  									   throws(1:MetaException o1)
  
  bool          grant_auth_sys(1:string byWho, 2:string userName, 3:list<string> privileges)
  									   throws(1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  bool          grant_auth_role_sys(1:string byWho, 2:string roleName, 3:list<string> privileges)
  									   throws(1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  bool          grant_role_to_user(1:string byWho, 2:string userName, 3:list<string> roleNames)
  									   throws(1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  bool          grant_role_to_role(1:string byWho, 2:string roleName, 3:list<string> roleNames)
  									   throws(1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  bool          grant_auth_on_db(1:string byWho, 2:string forWho, 3:list<string> privileges, 4:string db)
  									   throws(1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  bool          grant_auth_on_tbl(1:string byWho, 2:string forWho, 3:list<string> privileges, 4:string db, 5:string tbl)
  									   throws(1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  									   
  DbPriv        get_auth_on_db(1:string byWho, 2:string who, 3:string db)
  									   throws(1:MetaException o1)
  list<DbPriv>  get_auth_on_dbs(1:string byWho, 2:string who)
  									   throws(1:MetaException o1)
  list<DbPriv>  get_db_auth(1:string byWho, 2:string db)
  									   throws(1:MetaException o1)
  list<DbPriv>  get_db_auth_all(1:string byWho)
  									   throws(1:MetaException o1)
  TblPriv       get_auth_on_tbl(1:string byWho, 2:string who, 3:string db, 4:string tbl)
  									   throws(1:MetaException o1)
  list<TblPriv> get_auth_on_tbls(1:string byWho, 2:string who)
  									   throws(1:MetaException o1)
  list<TblPriv> get_tbl_auth(1:string byWho, 2:string db, 3:string tbl)
  									   throws(1:MetaException o1)
  list<TblPriv> get_tbl_auth_all(1:string byWho)
  									   throws(1:MetaException o1)
  									   
  bool          revoke_auth_sys(1:string byWho, 2:string userName, 3:list<string> privileges)
  									   throws(1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  bool          revoke_auth_role_sys(1:string byWho, 2:string roleName, 3:list<string> privileges)
  									   throws(1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  bool          revoke_role_from_user(1:string byWho, 2:string userName, 3:list<string> roleNames)
  									   throws(1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  bool          revoke_role_from_role(1:string byWho, 2:string roleName, 3:list<string> roleNames)
  									   throws(1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  bool          revoke_auth_on_db(1:string byWho, 2:string who, 3:list<string> privileges, 4:string db)
  									   throws(1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  bool          revoke_auth_on_tbl(1:string byWho, 2:string who, 3:list<string> privileges, 4:string db, 5:string tbl)
  									   throws(1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3) 
  									   
  bool          drop_auth_on_db(1:string byWho, 2:string forWho, 3:string db)
    									   throws(1:MetaException o1)
  bool          drop_auth_in_db(1:string byWho, 2:string forWho)
    									   throws(1:MetaException o1)
  bool          drop_auth_on_tbl(1:string byWho, 2:string forWho, 3:string db, 4:string tbl)
    									   throws(1:MetaException o1)
  bool          drop_auth_in_tbl(1:string byWho, 2:string forWho)
    									   throws(1:MetaException o1)
  				   
  // added by BrantZhang for authorization end
  
  // add by konten for index begin
    bool          create_index(1:IndexItem index)throws(1:MetaException o1)
    bool          drop_index(1:string db, 2:string table, 3:string name)throws(1:MetaException o1)
    i32          get_index_num(1:string db, 2:string table)throws(1:MetaException o1)
    i32          get_index_type(1:string db, 2:string table, 3:string name)throws(1:MetaException o1)
    string get_index_field(1:string db, 2:string table, 3:string name)throws(1:MetaException o1)
    string       get_index_location(1:string db, 2:string table, 3:string name)throws(1:MetaException o1)
    bool         set_index_location(1:string db, 2:string table, 3:string name, 4:string location)throws(1:MetaException o1)
    bool         set_index_status(1:string db, 2:string table, 3:string name, 4:i32 status)throws(1:MetaException o1)
    
    list<IndexItem>   get_all_index_table(1:string db, 2:string table)throws(1:MetaException o1)
    IndexItem        get_index_info(1:string db, 2:string table, 3:string name)throws(1:MetaException o1)
    
    list<IndexItem>   get_all_index_sys()throws(1:MetaException o1)
    
  // add by konten for index end
    //add by paynie for IDE optimize start
    list<FieldSchema> get_fields_jdbc(1:string db, 2:string tableName)throws(1:MetaException x)
    list<FieldSchema> get_part_fields_jdbc(1:string db, 2:string tableName)throws(1:MetaException x)
    //add by paynie for IDE optimize end
    
    //add by paynie for jdbc gettables optimize
    list<string> get_tables_jdbc(1:string db, 2:string pattern)throws(1:MetaException o1)
    //add end
  
  
 	//new 
    bool createDatabase(1:string name, 2:string description, 3:string hdfsscheme, 4:string metastore)
                                       throws(1:AlreadyExistsException o1, 2:MetaException o2)
                                       
  	Database getDatabase(1:string name) throws(1:NoSuchObjectException o1, 2:MetaException o2)
  	
  	bool dropDatabase(1:string name)    throws(1:NoSuchObjectException o1, 2:MetaException o2)
  	
  	list<string> getDatabases()         throws(1:MetaException o1)

  	// returns the type with given name (make seperate calls for the dependent types if needed)
  	Type getType(1:string name)  throws(1:MetaException o2)
  	
  	bool createType(1:Type type) throws(1:AlreadyExistsException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  	
  	bool dropType(1:string type) throws(1:MetaException o2)
  	
  	map<string, Type> getTypeAll(1:string name) throws(1:MetaException o2)

  	// Gets a list of FieldSchemas describing the columns of a particular table
  	list<FieldSchema> getFields(1: string dbName, 2: string tableName) throws (1: MetaException o1, 2: UnknownTableException o2, 3: UnknownDBException o3),

  	// Gets a list of FieldSchemas describing both the columns and the partition keys of a particular table
  	//list<FieldSchema> getSchema(1: string dbName, 2: string tableName) throws (1: MetaException o1, 2: UnknownTableException o2, 3: UnknownDBException o3)

  	// create a Hive table. Following fields must be set
  	// tableName
  	// database        (only 'default' for now until Hive QL supports databases)
  	// owner           (not needed, but good to have for tracking purposes)
  	// sd.cols         (list of field schemas)
  	// sd.inputFormat  (SequenceFileInputFormat (binary like falcon tables or u_full) or TextInputFormat)
  	// sd.outputFormat (SequenceFileInputFormat (binary) or TextInputFormat)
 	// sd.serdeInfo.serializationLib (SerDe class name eg org.apache.hadoop.hive.serde.simple_meta.MetadataTypedColumnsetSerDe
  	void createTable(1:Table tbl) throws(1:AlreadyExistsException o1, 2:InvalidObjectException o2, 3:MetaException o3, 4:NoSuchObjectException o4)
  
  	// drops the table and all the partitions associated with it if the table has partitions
 	// delete data (including partitions) if deleteData is set to true
 	void dropTable(1:string dbname, 2:string name, 3:bool deleteData)throws(1:NoSuchObjectException o1, 2:MetaException o3)
  
  	list<string> getTables(1: string dbName, 2: string pattern) throws (1: MetaException o1)

  	Table getTable(1:string dbName, 2:string tblName) throws (1:MetaException o1, 2:NoSuchObjectException o2)
  
  	void alterTable(1:string dbName, 2:string tblName, 3:Table newTbl)throws (1:InvalidOperationException o1, 2:MetaException o2)

  	Partition getPartition(1:string db_name, 2:string tbl_name, 3:i32 level) throws(1:MetaException o1)
  	
  	list<Partition> getPartitions(1:string db_name, 2:string tbl_name)
                       throws(1:NoSuchObjectException o1, 2:MetaException o2)
                       
  	void alterPartition(1:string dbName, 2:string tblName, 3:Partition newPart)
                       throws(1:InvalidOperationException o1, 2:MetaException o2)
                                
 	bool addUserGroup(1:group newGroup, 2: string user) throws (1:MetaException o1)
 	
 	i32 dropUserGroup(1:string groupName, 2: string user) throws (1:MetaException o1)
 	
 	string getGroupname(1:string userName) throws (1:MetaException o1)
 	
 	i32 revokeUserGroup(1:string groupname, 2:string namelist, 3: string user) throws (1:MetaException o1)
 	
 	i32 grantUserGroup(1:string groupName, 2:string namelist, 3: string user) throws (1:MetaException o1)
 	
 	list<group> getGroups(1:string pattern) throws (1:MetaException o1)
 
  
  	bool          createUser(1:string byWho, 2:string newUser, 3:string passwd)
  									   throws(1:AlreadyExistsException o1, 2:MetaException o2)
  	bool          dropUser(1:string byWho, 2:string userName)
  									   throws(1:NoSuchObjectException o1, 2:MetaException o2)
  	User          getUser(1:string byWho, 2:string userName)
  			 						   throws(1:NoSuchObjectException o1, 2:MetaException o2)
  	list<string>  getUsersAll(1:string byWho)
  									   throws(1:MetaException o1)
  	bool          setPasswd(1:string byWho, 2:string forWho, 3:string newPasswd)
  									   throws(1:NoSuchObjectException o1, 2:MetaException o2)
  	bool          isAUser(1:string userName, 2:string passwd)
  									   throws(1:MetaException o1)
  
  	bool          isARole(1:string roleName)
  									   throws(1:MetaException o1)
  	bool          createRole(1:string byWho, 2:string roleName)
  									   throws(1:AlreadyExistsException o1, 2:MetaException o2)
  	bool          dropRole(1:string byWho, 2:string roleName)
  									   throws(1:NoSuchObjectException o1, 2:MetaException o2)
  	Role          getRole(1:string byWho, 2:string roleName)
  									   throws(1:NoSuchObjectException o1, 2:MetaException o2)
  	list<string>  getRolesAll(1:string byWho)
  									   throws(1:MetaException o1)
  
  	bool          grantAuthSys(1:string byWho, 2:string userName, 3:list<string> privileges)
  									   throws(1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  	bool          grantAuthRoleSys(1:string byWho, 2:string roleName, 3:list<string> privileges)
  									   throws(1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  	bool          grantRoleToUser(1:string byWho, 2:string userName, 3:list<string> roleNames)
  									   throws(1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  	bool          grantRoleToRole(1:string byWho, 2:string roleName, 3:list<string> roleNames)
  									   throws(1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  	bool          grantAuthOnDb(1:string byWho, 2:string forWho, 3:list<string> privileges, 4:string db)
  									   throws(1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  	bool          grantAuthOnTbl(1:string byWho, 2:string forWho, 3:list<string> privileges, 4:string db, 5:string tbl)
  									   throws(1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  									   
  	DbPriv        getAuthOnDb(1:string byWho, 2:string who, 3:string db)
  									   throws(1:MetaException o1)
  	list<DbPriv>  getAuthOnDbs(1:string byWho, 2:string who)
  									   throws(1:MetaException o1)
  	list<DbPriv>  getDbAuth(1:string byWho, 2:string db)
  									   throws(1:MetaException o1)
  	list<DbPriv>  getDbAuthAll(1:string byWho)
  									   throws(1:MetaException o1)
  	TblPriv       getAuthOnTbl(1:string byWho, 2:string who, 3:string db, 4:string tbl)
  									   throws(1:MetaException o1)
  	list<TblPriv> getAuthOnTbls(1:string byWho, 2:string who)
  									   throws(1:MetaException o1)
  	list<TblPriv> getTblAuth(1:string byWho, 2:string db, 3:string tbl)
  									   throws(1:MetaException o1)
  	list<TblPriv> getTblAuthAll(1:string byWho)
  									   throws(1:MetaException o1)
  									   
  	bool          revokeAuthSys(1:string byWho, 2:string userName, 3:list<string> privileges)
  									   throws(1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  	
  	bool          revokeAuthRoleSys(1:string byWho, 2:string roleName, 3:list<string> privileges)
  									   throws(1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  
  	bool          revokeRoleFromUser(1:string byWho, 2:string userName, 3:list<string> roleNames)
  									   throws(1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  
  	bool          revokeRoleFromRole(1:string byWho, 2:string roleName, 3:list<string> roleNames)
  									   throws(1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  	
  	bool          revokeAuthOnDb(1:string byWho, 2:string who, 3:list<string> privileges, 4:string db)
  									   throws(1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3)
  	
  	bool          revokeAuthOnTbl(1:string byWho, 2:string who, 3:list<string> privileges, 4:string db, 5:string tbl)
  									   throws(1:NoSuchObjectException o1, 2:InvalidObjectException o2, 3:MetaException o3) 
  									   
  	bool          dropAuthOnDb(1:string byWho, 2:string forWho, 3:string db)
    									   throws(1:MetaException o1)
  	
  	bool          dropAuthInDb(1:string byWho, 2:string forWho)
    									   throws(1:MetaException o1)
  	
  	bool          dropAuthOnTbl(1:string byWho, 2:string forWho, 3:string db, 4:string tbl)
    									   throws(1:MetaException o1)
  	
  	bool          dropAuthInTbl(1:string byWho, 2:string forWho)
    									   throws(1:MetaException o1)
    		
								  
    
    void addPartition(1:string dbName, 2:string tblName, 3:AddPartitionDesc addPartitionDesc)
                       throws(1:InvalidOperationException o1, 2:MetaException o2)		
    
    void dropPartition(1:string dbName, 2:string tblName, 3:DropPartitionDesc dropPartitionDesc)
                       throws(1:InvalidOperationException o1, 2:MetaException o2)									   									 
  				   
    void addDefaultPartition(1:string dbName, 2:string tblName, 3:i32 level)
                       throws(1:InvalidOperationException o1, 2:MetaException o2)		
    
    void dropDefaultPartition(1:string dbName, 2:string tblName, 3:i32 level)
                       throws(1:InvalidOperationException o1, 2:MetaException o2)	
                       
	void renameTable(1:string dbName, 2:string tblName, 3:string modifyUser, 4:string newName)
                       throws (1:InvalidOperationException o1, 2:MetaException o2)                 
      
    void addCols(1:string dbName, 2:string tblName, 3:string modifyUser, 4:list<FieldSchema> newCols)
                       throws (1:InvalidOperationException o1, 2:MetaException o2) 
		
    void renameCols(1:string dbName, 2:string tblName, 3:RenameColDesc renameColDesc)
                       throws (1:InvalidOperationException o1, 2:MetaException o2)  
    
    void replaceCols(1:string dbName, 2:string tblName, 3:string modifyUser, 4:list<FieldSchema> newCols)
                       throws (1:InvalidOperationException o1, 2:MetaException o2)     
    
    void addTblProps(1:string dbName, 2:string tblName, 3:string modifyUser, 4:map<string, string> props)
                       throws (1:InvalidOperationException o1, 2:MetaException o2)                                   

    void addSerdeProps(1:string dbName, 2:string tblName, 3:string modifyUser, 4:map<string, string> props)
                       throws (1:InvalidOperationException o1, 2:MetaException o2)  
   

    void addSerde(1:string dbName, 2:string tblName, 3:AddSerdeDesc addSerdeDesc)
                       throws (1:InvalidOperationException o1, 2:MetaException o2)  
                       
    bool isTableExist(1:string dbName, 2:string tblName)
                       throws (1:MetaException o2)  
                       
    list<list<string>> getPartitionNames(1:string db, 2:string tbl, 3:i32 max)
					   throws(1:InvalidOperationException o1, 2:MetaException o2)
					   
	bool isAUserName(1:string userName) throws(1:NoSuchObjectException o1, 2:MetaException o2)
	
	void modifyTableComment(1:string dbName, 2:string tblName, 3:string comment) throws(1:InvalidOperationException o1, 2:MetaException o2)
	
	void modifyColumnComment(1:string dbName, 2:string tblName, 3:string colName, 4:string comment) throws(1:InvalidOperationException o1, 2:MetaException o2)
	
	bool isView(1:string dbName, 2:string tblName) throws(1:NoSuchObjectException o1, 2:MetaException o2)
	
	bool hasAuthOnLocation(1:string who, 2:string location) throws (1:MetaException o1)
	
	bool hasAuth(1:string who, 2:i32 priv) throws(1:MetaException o1)
	
	bool hasAuthOnDb(1:string who, 2:string db, 3:i32 priv) throws(1:MetaException o1)

	bool hasAuthOnTbl(1:string who, 2:string db, 3:string tbl, 4:i32 priv) throws(1:MetaException o1)
	
	bool hasAuthWithRole(1:string who, 2:string role, 3:i32 priv) throws(1:MetaException o1)
	
	bool hasAuthOnDbWithRole(1:string who, 2:string role, 3:string db, 4:i32 priv) throws(1:MetaException o1)

	bool hasAuthOnTblWithRole(1:string who, 2:string role, 3:string db, 4:string tbl, 5:i32 priv) throws(1:MetaException o1)
	
	bool createDatabaseDb(1:Database name) throws(1:AlreadyExistsException o1, 2:MetaException o2)
	
	list<string>  getPlayRoles(1:string byWho) throws(1:NoSuchObjectException o1, 2:MetaException o2)
	
	bool updatePBInfo(1:string dbName, 2:string tableName, 3:string modifiedTime) throws(1:MetaException o1)
	
	list<string> getDatabasesWithOwner(1:string owner)         throws(1:MetaException o1)
	
	bool isPBTable(1:string dbName, 2:string tableName) throws(1:MetaException o1)
	
	##get db by priv
  	list<string> getDbsByPriv(1:string user, 2:string passwd) throws(1:MetaException ex) 
  
  	##get table by priv
  	map<string, string> getTblsByPriv(1:string user, 2:string passwd, 3:string db) throws(1:MetaException ex, 2:NoSuchObjectException o1)
  
  	##get table info
  	TableInfo getTblInfo(1:string user, 2:string passwd, 3:string db, 4:string tbl) throws(1:MetaException ex, 2:NoSuchObjectException o1)
	  	
  	##get table by priv
  	list<TableInfo> getAllTblsByPriv(1:string user, 2:string passwd) throws(1:MetaException ex, 2:NoSuchObjectException o1)
  	
  	##get table by priv with key word
  	map<string, TableInfo> getAllTblsByPrivWithKeyword(1:string user, 2:string passwd, 3:string db, 4:string keyWord) throws(1:MetaException ex, 2:NoSuchObjectException o1)
  	
  	##get table by priv
  	map<string, TableInfo> getTblsByPrivWithParams(1:string user, 2:string passwd, 3:string db) throws(1:MetaException ex, 2:NoSuchObjectException o1)
  	
  	##get dbs of user
  	list<string> getDbsByOwner(1:string user, 2:string passwd) throws(1:MetaException ex, 2:NoSuchObjectException o1)
	
	bool isHdfsExternalTable (1:string dbName, 2:string tableName) throws(1:MetaException o1)
}

// these should be needed only for backward compatibility with filestore
const string META_TABLE_COLUMNS   = "columns",
const string META_TABLE_COLUMN_TYPES   = "columns.types",
const string BUCKET_FIELD_NAME    = "bucket_field_name",
const string BUCKET_COUNT         = "bucket_count",
const string FIELD_TO_DIMENSION   = "field_to_dimension",
const string META_TABLE_NAME      = "name",
const string META_TABLE_DB        = "db",
const string META_TABLE_LOCATION  = "location",
const string META_TABLE_SERDE     = "serde",
const string META_TABLE_PARTITION_COLUMNS = "partition_columns",
const string FILE_INPUT_FORMAT    = "file.inputformat",
const string FILE_OUTPUT_FORMAT   = "file.outputformat",
const string PROJECTION = "projection",   
const string TABLE_TYPE = "type",        //
const string COMPRESS = "compress",        //


