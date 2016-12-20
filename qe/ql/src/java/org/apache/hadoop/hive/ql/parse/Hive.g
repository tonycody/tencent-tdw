grammar Hive;

options
{
output=AST;
ASTLabelType=CommonTree;
backtrack=false;
k=4;
}
 
tokens {
TOK_INSERT;
TOK_QUERY;
TOK_SELECT;
TOK_SELECTDI;
TOK_SELEXPR;
TOK_FROM;
TOK_TAB;
//TOK_PARTSPEC;
TOK_PARTVAL;
TOK_DIR;

TOK_NNUMBER;

TOK_TABDEST;
TOK_LOCAL_DIR;
TOK_TABREF;
TOK_SUBQUERY;
TOK_DESTINATION;
TOK_APPENDDESTINATION;
TOK_ALLCOLREF;
TOK_TABLE_OR_COL;
TOK_FUNCTION;
TOK_FUNCTIONDI;
TOK_FUNCTIONSTAR;
TOK_WHERE;
TOK_OP_EQ;
TOK_OP_NE;
TOK_OP_LE;
TOK_OP_LT;
TOK_OP_GE;
TOK_OP_GT;
TOK_OP_DIV;
TOK_OP_ADD;
TOK_OP_SUB;
TOK_OP_MUL;
TOK_OP_MOD;
TOK_OP_BITAND;
TOK_OP_BITNOT;
TOK_OP_BITOR;
TOK_OP_BITXOR;
TOK_OP_BITSHIFTLEFT;
TOK_OP_BITSHIFTRIGHT;
TOK_OP_AND;
TOK_OP_OR;
TOK_OP_NOT;
TOK_OP_LIKE;
TOK_OP_IN;         
TOK_TRUE;
TOK_FALSE;
TOK_TRANSFORM;
TOK_SERDE;
TOK_SERDENAME;
TOK_SERDEPROPS;
TOK_EXPLIST;
TOK_ALIASLIST;
TOK_GROUPBY;
TOK_ROLLUP;    
TOK_CUBE;      
TOK_GROUP;     
TOK_GROUPINGSETS;     
TOK_HAVING;
TOK_ORDERBY;
TOK_CLUSTERBY;
TOK_DISTRIBUTEBY;
TOK_SORTBY;
TOK_UNION;
TOK_UNIQUE_UNION;
TOK_JOIN;
TOK_LEFTOUTERJOIN;
TOK_RIGHTOUTERJOIN;
TOK_FULLOUTERJOIN;
TOK_UNIQUEJOIN;      
TOK_LOAD;
TOK_NULL;
TOK_ISNULL;
TOK_ISNOTNULL;
TOK_TINYINT;
TOK_SMALLINT;
TOK_INT;
TOK_BIGINT;
TOK_BOOLEAN;
TOK_FLOAT;
TOK_DOUBLE;
TOK_DATE;
TOK_DATETIME;
TOK_TIMESTAMP;
TOK_STRING;
TOK_LIST;
TOK_STRUCT;
TOK_MAP;

TOK_UNIONTYPE;
TOK_COLTYPELIST;

TOK_CREATETABLE;
TOK_LIKETABLE;
TOK_DESCTABLE;
TOK_DESCTABLE_COLUMN;
TOK_DESCFUNCTION;
TOK_ALTERTABLE_RENAME;
TOK_ALTERTABLE_ADDCOLS;

TOK_ALTERTABLE_RENAMECOL;

TOK_ALTERTABLE_REPLACECOLS;
TOK_ALTERTABLE_ADDPARTS;
TOK_ALTERTABLE_DROPPARTS;
TOK_ALTERTABLE_SERDEPROPERTIES;
TOK_ALTERTABLE_SERIALIZER;
TOK_ALTERTABLE_PROPERTIES;

TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION;
TOK_MSCK;
TOK_SHOWTABLES;

TOK_WITH;
TOK_SHOWPROCESSLIST;  
TOK_KILLQUERY;  
TOK_SHOWQUERY;  
TOK_CLEARQUERY;   
TOK_SHOWSTATINFO;   
TOK_CREATE_USERGROUP;
TOK_DROP_USERGROUP;
TOK_REVOKE_USERGROUP;
TOK_GRANT_USERGROUP;
TOK_SHOWUSERGROUPS;
TOK_SHOWTABLESIZE;  
TOK_SHOWDATABASESIZE;       
TOK_SHOWROWCOUNT;
TOK_DBNAME;
TOK_UPDATE;

TOK_SHOWFUNCTIONS;
TOK_SHOWPARTITIONS;
TOK_DROPTABLE;
TOK_TRUNCATETABLE;
TOK_TABCOLLIST;
TOK_TABCOL;
TOK_TABLECOMMENT;
TOK_TABLEPARTCOLS;
TOK_TABLEBUCKETS;
TOK_TABLEROWFORMAT;
TOK_TABLEROWFORMATFIELD;
TOK_TABLEROWFORMATCOLLITEMS;
TOK_TABLEROWFORMATMAPKEYS;
TOK_TABLEROWFORMATLINES;
TOK_TBLSEQUENCEFILE;
TOK_TBLTEXTFILE;
TOK_TBLRCFILE;
TOK_PGDATA; 
TOK_TABLEFILEFORMAT;
TOK_TABCOLNAME;
TOK_TABLELOCATION;
TOK_PARTITIONLOCATION;
TOK_TABLESAMPLE;
TOK_TABLESAMPLE_LINE;
TOK_TMP_FILE;
TOK_TABSORTCOLNAMEASC;
TOK_TABSORTCOLNAMEDESC;
TOK_CHARSETLITERAL;
TOK_CREATEFUNCTION;
TOK_DROPFUNCTION;
TOK_CREATEVIEW;  
TOK_DROPVIEW;    
TOK_VIEW;        
TOK_SHOWVIEWTABLES;
TOK_ALTERVIEW_PROPERTIES; 
TOK_EXPLAIN;
TOK_TABLESERIALIZER;
TOK_TABLEPROPERTIES;
TOK_TABLEPROPLIST;
TOK_TABTYPE;
TOK_LIMIT;
TOK_TABLEPROPERTY;
TOK_IFNOTEXISTS;
TOK_ORREPLACE; 
TOK_HINTLIST;
TOK_HINT;
TOK_MAPJOIN;
TOK_STREAMTABLE;  
TOK_HINTARGLIST;
TOK_USERSCRIPTCOLNAMES;
TOK_USERSCRIPTCOLSCHEMA;
TOK_RECORDREADER;
TOK_LEFTSEMIJOIN; 
TOK_FUNCTIONOVER; 
TOK_FUNCTIONOVERDI; 
TOK_FUNCPARAMETER;  
TOK_PARTITIONBY;    

TOK_PARTITIONHEAD;
TOK_PARTITIONBODY;
TOK_SUBPARTITION;
TOK_TABLEPARTITION;
TOK_PARTITION;
TOK_SUBPARTITIONHEAD;
TOK_SUBPARTITIONBODY;
TOK_SUBPARTITIONSPACE;

TOK_PARTITIONSPACE;

TOK_RANGEPARTITIONDEFINE;
TOK_LISTPARTITIONDEFINE;
TOK_PARTITIONREF;
TOK_SUBPARTITIONREF;
TOK_COMPPARTITIONREF;
TOK_ALTERTABLE_MERGEPARTS;
TOK_ALTERTABLE_ADDLISTPARTVALUES;
TOK_ALTERTABLE_REMOVELISTPARTVALUES;
TOK_ALTERTABLE_RENAMEPARTS;
TOK_ALTERTABLE_EXCHANGEPART;
TOK_ALTERTABLE_EXCHANGEPARTS;
TOK_ALTERTABLE_ADDSUBPARTS;
TOK_DEFAULTPARTITION;

TOK_ALTERTABLE_ADDDEFAULTPARTITION;
TOK_DEFAULTSUBPARTITION;
TOK_ALTERTABLE_TRUNCATE_PARTITION;

TOK_CREATE_USER;
TOK_DBA;
TOK_DROP_USER;
TOK_SHOW_USERS;
TOK_SET_PWD;
TOK_GRANT_PRIS;
TOK_PRI_LIST;
TOK_SELECT_PRI;
TOK_CREATE_PRI;
TOK_ALTER_PRI;
TOK_DROP_PRI;
TOK_INSERT_PRI;
TOK_DELETE_PRI;
TOK_UPDATE_PRI;
TOK_INDEX_PRI;
TOK_ALL_PRI;
TOK_SHOWVIEW_PRI;
TOK_CREATEVIEW_PRI;
TOK_DBA_PRI;
TOK_REVOKE_PRI;
TOK_GRANT_ROLE;
TOK_SHOW_ROLES;
TOK_REVOKE_ROLE;
TOK_SHOW_GRANTS;
TOK_DROP_ROLE;
TOK_ID_LIST;
TOK_CREATE_ROLE;
TOK_CREATE_DATABASE;
TOK_DROP_DATABASE;
TOK_SHOW_DATABASES;
TOK_USE_DATABASE;

TOK_CHANGE_USER;

TOK_TBLFORMATFILE;
TOK_TBLCOLUMNFILE;
TOK_COMPRESS;
TOK_SUBPROJECTION;
TOK_PROJECTION;
TOK_ALTERTABLE_ADDINDEX;
TOK_ALTERTABLE_DROPINDEX;
TOK_INDEX;
TOK_INDEXFIELD;
TOK_INDEXNAME;
TOK_SHOWTABLEINDEXS;
TOK_SHOWALLINDEXS;

TOK_PB_FILE;

TOK_VALUE;
TOK_VALUES;

TOK_RESERVE_DATA;

TOK_LATERAL_VIEW;
TOK_TABALIAS;

TOK_SHOW_CREATE_TABLE;

TOK_CHARSET;
TOK_PROPERTIESWITH;
TOK_DELETE; 

TOK_SHOWVERSION;
TOK_EXECEXTSQL;
TOK_NEWCOMMENT;
TOK_PB;
}

@header {
package org.apache.hadoop.hive.ql.parse;
}
@lexer::header {package org.apache.hadoop.hive.ql.parse;}
@members { 
  Stack msgs = new Stack<String>();
}

@lexer::members {
	boolean isCheck = false;
	
	public void setIsCheck(boolean ck){
		isCheck = ck;
	}
	public boolean getIsCheck(){
		return isCheck;
	}
}

@rulecatch {
catch (RecognitionException e) {
 reportError(e);
  throw e;
}
}

statement
	: explainStatement EOF
	| execStatement EOF
	;

explainStatement
@init { msgs.push("explain statement"); }
@after { msgs.pop(); }
	: KW_EXPLAIN (isExtended=KW_EXTENDED)? execStatement -> ^(TOK_EXPLAIN execStatement $isExtended?)
	;
		
execStatement
@init { msgs.push("statement"); }
@after { msgs.pop(); }
    
    : queryStatement
    | loadStatement
    | ddlStatement
    | aclStatement
    | dmlStatement
	| uplStatement
    ;

dmlStatement
@init { msgs.push("dml statement"); }
@after { msgs.pop(); }
    : deleteStatement
    ;
    
deleteStatement
@init { msgs.push("delete statement"); 
int whereclause = 0;
}
@after { msgs.pop(); 
whereclause = 0;
}
    : KW_DELETE KW_FROM updateTableSource (whereClause {whereclause = 1;})?
    ->{whereclause == 1}? ^(TOK_DELETE  ^(TOK_QUERY ^(TOK_FROM updateTableSource) ^(TOK_INSERT ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE)) ^(TOK_SELECT ^(TOK_SELEXPR TOK_ALLCOLREF)) whereClause )))
    -> ^(TOK_DELETE updateTableSource)
    ;

updateTableSource
@init { msgs.push("update table source"); }
@after { msgs.pop(); }
    :
    tabName (alias=Identifier)? -> ^(TOK_TABREF tabName $alias?)
    ;
	
uplStatement
@init { msgs.push("upl statement"); }
@after { msgs.pop(); }
    : updateStatement
    ;
	
updateStatement
@init { msgs.push("update statement"); 
int whereclause = 0;
}
@after { msgs.pop(); 
whereclause = 0;
}
    : KW_UPDATE updateTableSource KW_SET identifier EQUAL expression (COMMA identifier EQUAL expression)* (whereClause {whereclause = 1;})?
    ->{whereclause == 1}? ^(TOK_UPDATE  ^(TOK_QUERY ^(TOK_FROM updateTableSource) ^(TOK_INSERT ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE)) ^(TOK_SELECT ^(TOK_SELEXPR identifier expression)+) whereClause )))
    ->^(TOK_UPDATE  ^(TOK_QUERY ^(TOK_FROM updateTableSource) ^(TOK_INSERT ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE)) ^(TOK_SELECT ^(TOK_SELEXPR identifier  expression )+) )))
    ;


loadStatement
@init { msgs.push("load statement"); }
@after { msgs.pop(); }
    : KW_LOAD KW_DATA (islocal=KW_LOCAL)? KW_INPATH (path=StringLiteral) (isoverwrite=KW_OVERWRITE)? KW_INTO KW_TABLE (tab=tabName) 
    -> ^(TOK_LOAD $path $tab $islocal? $isoverwrite?)
    ;

ddlStatement
@init { msgs.push("ddl statement"); }
@after { msgs.pop(); }
    : createTableStatement  
    | dropTableStatement    
    | truncateStatement
    | alterStatement
    | descStatement
    | showStatement
    | metastoreCheck
    | createViewStatement  
    | dropViewStatement  
    | createFunctionStatement
    | dropFunctionStatement
    | createDatabase
    | dropDatabase
    | showDatabases
    | showViewTables
    | useDatabase
    | execExtSQL
    | commentStatement  
    ;

execExtSQL
	:	KW_EXEC_EXT_SQL sql=StringLiteral KW_WITH LPAREN propertiesList RPAREN
		-> ^(TOK_EXECEXTSQL $sql propertiesList)
	;
ifNotExists
@init { msgs.push("if not exists clause"); }
@after { msgs.pop(); }
    : KW_IF KW_NOT KW_EXISTS
    -> ^(TOK_IFNOTEXISTS)
    ;
    
orReplace
@init { msgs.push("or replace clause"); }
@after { msgs.pop(); }
    : KW_OR KW_REPLACE
    -> ^(TOK_ORREPLACE)
    ;

reserveData
@init { msgs.push("reserve data clause"); }
@after { msgs.pop(); }
    : KW_RESERVE KW_DATA
    -> ^(TOK_RESERVE_DATA)
    ;
    
    //-----------------------TDW ACL Statement-------------------------------
aclStatement
	:createUser
	|dropUser
	|setPwd
	|showUsers
	|grantPris
	|revokePri
	|showGrants
	|createRole
	|dropRole
	|showRoles
	|grantRole
	|revokeRole
	|changeToUser
	|createGroup
	|dropGroup
	|showGroups
	|grantGroup
	|revokeGroup
	;
	
showGroups
    :
    KW_SHOW KW_USERGROUPS showStmtIdentifier?  -> ^(TOK_SHOWUSERGROUPS showStmtIdentifier?)
	;

createGroup
	:
	KW_CREATE KW_USERGROUP Identifier -> ^(TOK_CREATE_USERGROUP Identifier)
	;
	
dropGroup
	:
	KW_DROP KW_USERGROUP Identifier -> ^(TOK_DROP_USERGROUP Identifier)
	;
	
grantGroup
    :
	KW_GRANT KW_USERGROUP Identifier KW_TO user=identifierList -> ^(TOK_GRANT_USERGROUP Identifier $user)
	;

revokeGroup
	:
	KW_REVOKE KW_USERGROUP Identifier KW_FROM user=identifierList -> ^(TOK_REVOKE_USERGROUP Identifier $user)
	;
	
createUser
	:KW_CREATE KW_USER Identifier KW_IDENTIFIED KW_BY StringLiteral (KW_AS dba=KW_DBA)?
	-> ^(TOK_CREATE_USER Identifier StringLiteral $dba?)
	;

dropUser
	:
	KW_DROP KW_USER Identifier (COMMA Identifier)*
	-> ^(TOK_DROP_USER Identifier+)
	;
setPwd	
	:
	KW_SET_PASSWD (KW_FOR user=Identifier)? KW_TO StringLiteral
	-> ^(TOK_SET_PWD StringLiteral $user?)	
	;
showUsers 
	:
	KW_SHOW KW_USERS
	-> ^(TOK_SHOW_USERS)
	;
	
grantPris
	:
	KW_GRANT privilegeList (KW_ON (db=Identifier|db=STAR) DOT (tbl=STAR|tbl=StringLiteral))? KW_TO user=Identifier
	-> ^(TOK_GRANT_PRIS privilegeList $user $db? $tbl?)
	;
	
privilegeList
	: privilege (COMMA privilege)*
	-> ^(TOK_PRI_LIST privilege+)
	;
privilege
	:	
	 KW_SELECT
	 -> ^(TOK_SELECT_PRI)
	|KW_CREATE
	-> ^(TOK_CREATE_PRI)
	|KW_ALTER
	-> ^(TOK_ALTER_PRI)
	|KW_DROP
	-> ^(TOK_DROP_PRI)
	|KW_TRUNCATE
	-> ^(TOK_DELETE_PRI)
	|KW_DELETE
	-> ^(TOK_DELETE_PRI)
	|KW_INDEX
	-> ^(TOK_INDEX_PRI)
	|KW_INSERT
	-> ^(TOK_INSERT_PRI)
	|KW_UPDATE
	-> ^(TOK_UPDATE_PRI)
	|KW_DBA
	-> ^(TOK_DBA_PRI)
	|KW_CREATEVIEW
	-> ^(TOK_CREATEVIEW_PRI)
	|KW_SHOWVIEW
	-> ^(TOK_SHOWVIEW_PRI)
	|KW_ALL
	-> ^(TOK_ALL_PRI)
	;
revokePri
	:KW_REVOKE privilegeList (KW_ON (db=Identifier|db=STAR) DOT (tbl=STAR|tbl=StringLiteral))? KW_FROM user=Identifier
	-> ^(TOK_REVOKE_PRI privilegeList $user $db? $tbl?)
	;

showGrants
	:KW_SHOW KW_GRANTS (KW_FOR Identifier)?
	-> ^(TOK_SHOW_GRANTS Identifier?)
	;
createRole
	:KW_CREATE KW_ROLE Identifier (COMMA Identifier)* (KW_AS dba=KW_DBA)?
	-> ^(TOK_CREATE_ROLE Identifier+ $dba?)
	;
dropRole:	
	KW_DROP KW_ROLE Identifier (COMMA Identifier)*
	-> ^(TOK_DROP_ROLE Identifier+)
	;
showRoles
	:KW_SHOW KW_ROLES (KW_FOR Identifier)?
	-> ^(TOK_SHOW_ROLES Identifier?)
	;
grantRole:
	KW_GRANT KW_ROLE role=identifierList KW_TO user=identifierList
	-> ^(TOK_GRANT_ROLE $role $user)
	;
identifierList:	
	Identifier (COMMA Identifier)*
	-> ^(TOK_ID_LIST Identifier+)
	;
revokeRole
	:KW_REVOKE KW_ROLE role=identifierList  KW_FROM user=identifierList
	-> ^(TOK_REVOKE_ROLE $role $user)
	;

changeToUser
  : KW_CHANGE KW_TO KW_USER Identifier (KW_IDENTIFIED KW_BY pwd=StringLiteral)?
   -> ^(TOK_CHANGE_USER Identifier $pwd?)
  ;
   
createTableStatement  
@init { msgs.push("create table statement"); }  
@after { msgs.pop(); }
    : KW_CREATE (ext=KW_EXTERNAL)? KW_TABLE ifNotExists? name=Identifier
      ( like=KW_LIKE likeName=Identifier | 
      (LPAREN columnNameTypeList RPAREN)? 
      tableComment?
      tablePartition? 
      tableBuckets? 
      tableRowFormat? 
      tableIndex? 
      tableFileFormat? 
      tablePropertiesPrefixed?
      (KW_AS selectStatement)?
      ) 
      tableLocation?
    -> ^(TOK_CREATETABLE $name $ext? ifNotExists? 
    ^(TOK_LIKETABLE $likeName?)
      columnNameTypeList? 
      tableComment? 
      tableFileFormat? 
      tablePartition? 
      tableBuckets? 
      tableRowFormat? 
      tableIndex? 
      tableLocation? 
      tablePropertiesPrefixed?
      selectStatement?
     )
    ;
    
commentStatement
@init { msgs.push(" comment statement"); }
@after { msgs.pop(); }
    :commentTable
    |commentView
    |commentColumn
    ;	

comments
    :	
    comment=StringLiteral->$comment 
    |
    KW_NULL->TOK_NULL	
    ;

commentView
@init { msgs.push(" comment view"); }
@after { msgs.pop(); }
    : 
    KW_COMMENT KW_ON KW_VIEW tableOrColumn KW_IS comments
    ->^(TOK_NEWCOMMENT TOK_VIEW tableOrColumn comments)
    ;
            
commentTable
@init { msgs.push(" comment table"); }
@after { msgs.pop(); }
    : 
    KW_COMMENT KW_ON KW_TABLE tableOrColumn KW_IS comments
    ->^(TOK_NEWCOMMENT TOK_TAB tableOrColumn comments)  
    ;
    
commentColumn
@init { msgs.push(" comment column"); }
@after { msgs.pop(); }
    :
    KW_COMMENT KW_ON KW_COLUMN precedenceFieldExpression KW_IS comments
    ->^(TOK_NEWCOMMENT TOK_TABCOL precedenceFieldExpression comments)
    ;

dropTableStatement    
@init { msgs.push("drop statement"); }
@after { msgs.pop(); }
    : KW_DROP KW_TABLE Identifier reserveData?  -> ^(TOK_DROPTABLE Identifier reserveData?)
    ;

truncateStatement
@init { msgs.push("truncate statement"); }
@after { msgs.pop(); }
    : KW_TRUNCATE KW_TABLE Identifier  -> ^(TOK_TRUNCATETABLE Identifier)
    ;

createDatabase
    : KW_CREATE KW_DATABASE Identifier
	  tablePropertiesPrefixed?
    -> ^(TOK_CREATE_DATABASE Identifier
	  tablePropertiesPrefixed?)
    ;
dropDatabase
    : KW_DROP KW_DATABASE Identifier
    -> ^(TOK_DROP_DATABASE Identifier)
    ;
showDatabases
    : KW_SHOW KW_DATABASES  tablePropertiesPrefixed?
    -> ^(TOK_SHOW_DATABASES tablePropertiesPrefixed?)
    ;

useDatabase
    : KW_USE Identifier
    -> ^(TOK_USE_DATABASE Identifier)
    ;

alterStatement
@init { msgs.push("alter statement"); }
@after { msgs.pop(); }
    
    
    : KW_ALTER!
        (
            KW_TABLE! alterTableStatementSuffix
        |
            KW_VIEW! alterViewStatementSuffix
        )
    
    ;

alterTableStatementSuffix    
@init { msgs.push("alter table statement"); }  
@after { msgs.pop(); }
    : alterStatementSuffixRename
    | alterStatementSuffixAddCol
    | alterStatementSuffixRenameCol  
    | alterStatementSuffixAddPartitions
    | alterStatementSuffixAddSubPartitions
    | alterStatementSuffixDropPartitions
    | alterStatementSuffixMergePartitions
    | alterStatementSuffixAddListPartitionValues
    | alterStatementSuffixRemoveListPartitionValues
    | alterStatementSuffixRenamePartition
    | alterStatementSuffixExchangePartition
    | alterStatementSuffixProperties
    | alterStatementSuffixSerdeProperties
    | alterStatementSuffixAddDefaultPartition
    | alterStatementSuffixTruncatePartition
    | alterStatementSuffixAddIndex
    | alterStatementSuffixDropIndex
    ;

alterViewStatementSuffix
@init { msgs.push("alter view statement"); }
@after { msgs.pop(); }
    : alterViewSuffixProperties
    ;

alterStatementSuffixTruncatePartition
	:
	Identifier KW_TRUNCATE 	partitionRef
	-> ^(TOK_ALTERTABLE_TRUNCATE_PARTITION Identifier partitionRef)
	;

alterStatementSuffixRename
@init { msgs.push("rename statement"); }
@after { msgs.pop(); }
    : oldName=Identifier KW_RENAME KW_TO newName=Identifier 
    -> ^(TOK_ALTERTABLE_RENAME $oldName $newName)
    ;

alterStatementSuffixAddCol
@init { msgs.push("add column statement"); }
@after { msgs.pop(); }
    : Identifier (add=KW_ADD | replace=KW_REPLACE) KW_COLUMNS LPAREN columnNameTypeList RPAREN
    -> {$add != null}? ^(TOK_ALTERTABLE_ADDCOLS Identifier columnNameTypeList)
    ->                 ^(TOK_ALTERTABLE_REPLACECOLS Identifier columnNameTypeList)
    ;

alterStatementSuffixRenameCol
@init { msgs.push("rename column name"); }
@after { msgs.pop(); }
	
    : Identifier KW_CHANGE KW_COLUMN? oldName=identifier newName=identifier colType? (KW_COMMENT comment=StringLiteral)? alterStatementChangeColPosition?
    ->^(TOK_ALTERTABLE_RENAMECOL Identifier $oldName $newName colType? $comment? alterStatementChangeColPosition?)
    ;    

alterStatementChangeColPosition
    : first=KW_FIRST|KW_AFTER afterCol=Identifier
    ->{$first != null}? ^(TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION )
    -> ^(TOK_ALTERTABLE_CHANGECOL_AFTER_POSITION $afterCol)
    ;    

alterStatementSuffixAddIndex
@init { msgs.push("add index statement"); }
@after { msgs.pop(); }
    : Identifier KW_ADD KW_INDEX indexName? LPAREN indexFieldNameList RPAREN
    -> ^(TOK_ALTERTABLE_ADDINDEX Identifier indexName? indexFieldNameList)
    ;

alterStatementSuffixDropIndex
@init { msgs.push("drop index statement"); }
@after { msgs.pop(); }
    : Identifier KW_DROP KW_INDEX indexName
    -> ^(TOK_ALTERTABLE_DROPINDEX Identifier indexName)    
    ;
    
alterStatementSuffixAddPartitions
	: Identifier KW_ADD partitionSpace (COMMA partitionSpace)*
	-> ^(TOK_ALTERTABLE_ADDPARTS Identifier partitionSpace+)	
	;

alterStatementSuffixAddSubPartitions
	: Identifier KW_ADD subPartitionSpace (COMMA subPartitionSpace)*
	-> ^(TOK_ALTERTABLE_ADDSUBPARTS Identifier subPartitionSpace+)	
	;
	
	 alterStatementSuffixAddDefaultPartition
	 : Identifier KW_ADD KW_DEFAULT KW_PARTITION
	 -> ^(TOK_ALTERTABLE_ADDDEFAULTPARTITION Identifier TOK_DEFAULTPARTITION)
	 | Identifier KW_ADD KW_DEFAULT KW_SUBPARTITION
	 -> ^(TOK_ALTERTABLE_ADDDEFAULTPARTITION Identifier TOK_DEFAULTSUBPARTITION)
	 ;

alterStatementSuffixMergePartitions
	: Identifier KW_MERGE src=partitionLevelRef KW_INTO dist=partitionLevelRef
	-> ^(TOK_ALTERTABLE_MERGEPARTS $src $dist)	
	;


alterStatementSuffixAddListPartitionValues
	: Identifier KW_ADD KW_VALUES LPAREN constant (COMMA constant)* RPAREN KW_TO partitionLevelRef
	-> ^(TOK_ALTERTABLE_ADDLISTPARTVALUES partitionLevelRef constant+)
	;

alterStatementSuffixRemoveListPartitionValues
	: Identifier KW_REMOVE KW_VALUES LPAREN constant (COMMA constant)* RPAREN KW_TO partitionLevelRef
	-> ^(TOK_ALTERTABLE_REMOVELISTPARTVALUES partitionLevelRef constant+)	
	;

alterStatementSuffixDropPartitions
@init { msgs.push("drop partition statement"); }
@after { msgs.pop(); }
    : Identifier KW_DROP partitionLevelRef (COMMA partitionLevelRef)*
    -> ^(TOK_ALTERTABLE_DROPPARTS Identifier partitionLevelRef+)
    ;

alterStatementSuffixRenamePartition
	:	Identifier KW_RENAME src=partitionLevelRef KW_TO dist=partitionLevelRef
	-> ^(TOK_ALTERTABLE_RENAMEPARTS $src $dist)
	;

alterStatementSuffixExchangePartition
	: Identifier KW_EXCHANGE partitionRef KW_WITH KW_TABLE Identifier
	-> ^(TOK_ALTERTABLE_EXCHANGEPARTS partitionRef Identifier)	
	;

alterStatementSuffixProperties
@init { msgs.push("alter properties statement"); }
@after { msgs.pop(); }
    
    : name=Identifier KW_SET KW_TBLPROPERTIES tableProperties  
    -> ^(TOK_ALTERTABLE_PROPERTIES $name tableProperties)
    ;

alterViewSuffixProperties
@init { msgs.push("alter view properties statement"); }
@after { msgs.pop(); }
    : name=Identifier KW_SET KW_TBLPROPERTIES tableProperties
    -> ^(TOK_ALTERVIEW_PROPERTIES $name tableProperties)
    ;

alterStatementSuffixSerdeProperties
@init { msgs.push("alter serdes statement"); }
@after { msgs.pop(); }
    : name=Identifier KW_SET KW_SERDE serdeName=StringLiteral (KW_WITH KW_SERDEPROPERTIES tableProperties)?
    -> ^(TOK_ALTERTABLE_SERIALIZER $name $serdeName tableProperties?)
    | name=Identifier KW_SET KW_SERDEPROPERTIES tableProperties
    -> ^(TOK_ALTERTABLE_SERDEPROPERTIES $name tableProperties)
    ;

tabTypeExpr
@init { msgs.push("specifying table types"); }
@after { msgs.pop(); }

   : Identifier (DOT^ (Identifier | KW_ELEM_TYPE | KW_KEY_TYPE | KW_VALUE_TYPE))*
   ;
   
partTypeExpr
@init { msgs.push("specifying table partitions"); }
@after { msgs.pop(); }
    :  (db=Identifier COLON COLON)? tabTypeExpr -> ^(TOK_TABTYPE tabTypeExpr $db?)
    ;

descStatement
@init { msgs.push("describe statement"); }
@after { msgs.pop(); }
    : (KW_DESCRIBE|KW_DESC) (isExtended=KW_EXTENDED)? (parttype=partTypeExpr) (KW_WHERE KW_COLUMNNAME KW_CONTAINS pattern=StringLiteral)? -> ^(TOK_DESCTABLE $parttype $isExtended? $pattern? )
    | (KW_DESCRIBE|KW_DESC) KW_FUNCTION KW_EXTENDED? (name=descFuncNames) -> ^(TOK_DESCFUNCTION $name KW_EXTENDED?) 
    ;   
sysFuncNames
    :
      KW_AND
    | KW_OR
    | KW_NOT
    | KW_LIKE
    | KW_IF
    | KW_CASE
    | KW_WHEN
    | KW_TINYINT
    | KW_SMALLINT
    | KW_INT
    | KW_BIGINT
    | KW_FLOAT
    | KW_DOUBLE
    | KW_BOOLEAN
    | KW_STRING
    | KW_ARRAY
    | KW_MAP
    | KW_STRUCT
    
    | EQUAL
    | NOTEQUAL
    | LESSTHANOREQUALTO
    | LESSTHAN
    | GREATERTHANOREQUALTO
    | GREATERTHAN
    | DIVIDE
    | PLUS
    | MINUS
    | STAR
    | MOD
    | DIV
    | AMPERSAND
    | TILDE
    | BITWISEOR
    | BITWISEXOR
    | BITWISESHIFTLEFT
    | BITWISESHIFTRIGHT
    | KW_RLIKE
    | KW_REGEXP
    | KW_IN
	| KW_BETWEEN
    ;

descFuncNames
    :
      sysFuncNames
    | StringLiteral
    | Identifier
    ;

showStatement
@init { msgs.push("show statement"); }
@after { msgs.pop(); }
    : KW_SHOW KW_TABLES showStmtIdentifier?  -> ^(TOK_SHOWTABLES showStmtIdentifier?)
    | KW_SHOW KW_FUNCTIONS showStmtIdentifier?  -> ^(TOK_SHOWFUNCTIONS showStmtIdentifier?)
    | KW_SHOW KW_PARTITIONS Identifier -> ^(TOK_SHOWPARTITIONS Identifier)
    | KW_SHOW KW_INDEX KW_FROM Identifier -> ^(TOK_SHOWTABLEINDEXS Identifier)
    | KW_SHOW KW_ALL KW_INDEX -> ^(TOK_SHOWALLINDEXS)
    | KW_SHOW KW_CREATE KW_TABLE Identifier -> ^(TOK_SHOW_CREATE_TABLE Identifier)
    | KW_SHOW KW_PROCESSLIST (islocal=KW_LOCAL)? Identifier? -> ^(TOK_SHOWPROCESSLIST $islocal? Identifier?)
	| KW_SHOW KW_STATINFO Identifier -> ^(TOK_SHOWSTATINFO Identifier)
	| KW_SHOW KW_VERSION -> ^(TOK_SHOWVERSION) 
	| KW_SHOW KW_TABLESIZE (isExtended=KW_EXTENDED)? (db=Identifier COLON COLON)? tb=Identifier  partitionRef? (COMMA partitionRef)* -> ^(TOK_SHOWTABLESIZE $isExtended? (TOK_DBNAME $db)? $tb partitionRef*) 
	| KW_SHOW KW_ROWCOUNT (isExtended=KW_EXTENDED)? (db=Identifier COLON COLON)? tb=Identifier  partitionRef? (COMMA partitionRef)* -> ^(TOK_SHOWROWCOUNT $isExtended? (TOK_DBNAME $db)? $tb partitionRef*) 
	| KW_SHOW KW_DATABASESIZE (isExtended=KW_EXTENDED)? Identifier -> ^(TOK_SHOWDATABASESIZE $isExtended? Identifier)
	| KW_KILLQUERY Identifier -> ^(TOK_KILLQUERY  Identifier)
	| KW_SHOWQUERY Identifier -> ^(TOK_SHOWQUERY  Identifier)
	| KW_CLEARQUERY num=Number? -> ^(TOK_CLEARQUERY  $num?)
    ;

metastoreCheck
@init { msgs.push("metastore check statement"); }
@after { msgs.pop(); }
    : KW_MSCK (KW_TABLE table=Identifier partitionLevelRef?)?
    -> ^(TOK_MSCK ($table partitionLevelRef?)?)
    ;     
    
createFunctionStatement
@init { msgs.push("create function statement"); }
@after { msgs.pop(); }
    : KW_CREATE KW_TEMPORARY KW_FUNCTION Identifier KW_AS StringLiteral
    -> ^(TOK_CREATEFUNCTION Identifier StringLiteral)
    ;

dropFunctionStatement
@init { msgs.push("drop temporary function statement"); }
@after { msgs.pop(); }
    : KW_DROP KW_TEMPORARY KW_FUNCTION Identifier
    -> ^(TOK_DROPFUNCTION Identifier)
    ;

createViewStatement
@init { 
    msgs.push("create view statement");
}
@after { msgs.pop(); }
    
    : KW_CREATE (orReplace)? KW_VIEW (ifNotExists)? name=Identifier 
        (LPAREN columnNameCommentList RPAREN)? tableComment?
        tablePropertiesPrefixed?   
        KW_AS 
        selectStatement 
    
    -> ^(TOK_CREATEVIEW $name orReplace?   
         ifNotExists?    
         columnNameCommentList?
         tableComment?
         tablePropertiesPrefixed?   
         selectStatement
        )
    ;

dropViewStatement
@init { msgs.push("drop view statement"); }
@after { msgs.pop(); }
    : KW_DROP KW_VIEW Identifier
    -> ^(TOK_DROPVIEW Identifier)
    ;

showViewTables
@init { msgs.push("show view tables");}
@after { msgs.pop();}
    : KW_SHOW KW_VIEWTABLES Identifier
    ->^(TOK_SHOWVIEWTABLES Identifier)
    ;

showStmtIdentifier
@init { msgs.push("identifier for show statement"); }
@after { msgs.pop(); }
    : StringLiteral
    ;

tableComment
@init { msgs.push("table's comment"); }
@after { msgs.pop(); }
    :
      KW_COMMENT comment=StringLiteral  -> ^(TOK_TABLECOMMENT $comment)
    ;

tablePartition
@init { msgs.push("table partition specification"); }
@after { msgs.pop(); }
	: partitionHead subPartition? partitionBody? 
	-> ^(TOK_TABLEPARTITION ^(TOK_PARTITION partitionHead partitionBody?) subPartition?)
	;

partitionHead
@init { msgs.push("table partition head"); }
@after { msgs.pop(); }
	: KW_PARTITION KW_BY partitionType LPAREN columnName RPAREN
	-> ^(TOK_PARTITIONHEAD partitionType columnName)
	;
partitionType
@init { msgs.push("table partition type"); }
@after { msgs.pop(); }
	: KW_LIST 
	| KW_HASH 
	| KW_RANGE 
	;
subPartition
@init { msgs.push("table partition sub partition"); }
@after { msgs.pop(); }
	: subPartitionHead subPartitionBody? 
	-> ^(TOK_SUBPARTITION subPartitionHead subPartitionBody?) 
	;
subPartitionHead
@init { msgs.push("table partition sub partition Head"); }
@after { msgs.pop(); }
	: KW_SUBPARTITION KW_BY partitionType LPAREN columnName RPAREN
	-> ^(TOK_SUBPARTITIONHEAD partitionType columnName)
	;	
partitionBody
@init { msgs.push("table partition body"); }
@after { msgs.pop(); }
	: LPAREN partitionSpace (COMMA partitionSpace)* RPAREN
	-> ^(TOK_PARTITIONBODY partitionSpace+)
	;

subPartitionBody
@init { msgs.push("table sub partition body"); }
@after { msgs.pop(); }
	:  LPAREN subPartitionSpace (COMMA subPartitionSpace)* RPAREN
	-> ^(TOK_SUBPARTITIONBODY subPartitionSpace+)
	;
subPartitionSpace
@init { msgs.push("table sub partition space"); }
@after { msgs.pop(); }
	: KW_SUBPARTITION Identifier KW_VALUES partitionDefine
	-> ^(TOK_SUBPARTITIONSPACE Identifier partitionDefine)
	| KW_SUBPARTITION KW_DEFAULT
	-> ^(TOK_SUBPARTITIONSPACE TOK_DEFAULTPARTITION)
	;
	
partitionSpace
@init { msgs.push("table partition space"); }
@after { msgs.pop(); }
	: KW_PARTITION Identifier KW_VALUES partitionDefine
	-> ^(TOK_PARTITIONSPACE Identifier partitionDefine)
	| KW_PARTITION KW_DEFAULT
	-> ^(TOK_PARTITIONSPACE TOK_DEFAULTPARTITION)
	;
	
partitionDefine
@init { msgs.push("table partition define"); }
@after { msgs.pop(); }
	: KW_IN LPAREN constant (COMMA constant)* RPAREN -> ^(TOK_LISTPARTITIONDEFINE constant+)
	| KW_LESS KW_THAN LPAREN constant RPAREN -> ^(TOK_RANGEPARTITIONDEFINE constant)
	;

tableBuckets
@init { msgs.push("table buckets specification"); }
@after { msgs.pop(); }
    :
      KW_CLUSTERED KW_BY LPAREN bucketCols=columnNameList RPAREN (KW_SORTED KW_BY LPAREN sortCols=columnNameOrderList RPAREN)? KW_INTO num=Number KW_BUCKETS 
    -> ^(TOK_TABLEBUCKETS $bucketCols $sortCols? $num)
    ;

serde
@init { msgs.push("serde specification"); }
@after { msgs.pop(); }
    : serdeFormat -> ^(TOK_SERDE serdeFormat)
    | serdePropertiesFormat -> ^(TOK_SERDE serdePropertiesFormat)
    |   -> ^(TOK_SERDE)
    ;

recordReader
@init { msgs.push("record reader specification"); }
@after { msgs.pop(); }
    : KW_RECORDREADER StringLiteral -> ^(TOK_RECORDREADER StringLiteral)
    |   -> ^(TOK_RECORDREADER)
    ;

serdeFormat
@init { msgs.push("serde format specification"); }
@after { msgs.pop(); }
    : KW_ROW KW_FORMAT KW_SERDE name=StringLiteral (KW_WITH KW_SERDEPROPERTIES serdeprops=tableProperties)?
    -> ^(TOK_SERDENAME $name $serdeprops?)
    ;

serdePropertiesFormat
@init { msgs.push("serde properties specification"); }
@after { msgs.pop(); }
    :
      KW_ROW KW_FORMAT KW_DELIMITED tableRowFormatFieldIdentifier? tableRowFormatCollItemsIdentifier? tableRowFormatMapKeysIdentifier? tableRowFormatLinesIdentifier? 
    -> ^(TOK_SERDEPROPS tableRowFormatFieldIdentifier? tableRowFormatCollItemsIdentifier? tableRowFormatMapKeysIdentifier? tableRowFormatLinesIdentifier?)
    ;

tableRowFormat
@init { msgs.push("table row format specification"); }
@after { msgs.pop(); }
    :
      serdePropertiesFormat
    -> ^(TOK_TABLEROWFORMAT serdePropertiesFormat)
    | serdeFormat
    -> ^(TOK_TABLESERIALIZER serdeFormat)
    ;

tablePropertiesPrefixed
@init { msgs.push("table properties with prefix"); }
@after { msgs.pop(); }
    :
        KW_WITH LPAREN withProperty (COMMA withProperty)* RPAREN-> ^(TOK_PROPERTIESWITH withProperty+)
    ;

withProperty
@init { msgs.push("table withProperties"); }
@after { msgs.pop(); }
	:
	charsetProperty|keyValueProperty	
	;

charsetProperty
@init { msgs.push("table charsetPropertie"); }
@after { msgs.pop(); }
    :
    	KW_CHARSET EQUAL value=StringLiteral -> ^(TOK_CHARSET  $value)
    ;

tableProperties
@init { msgs.push("table properties"); }
@after { msgs.pop(); }
    :
      LPAREN propertiesList RPAREN -> ^(TOK_TABLEPROPERTIES propertiesList)
    ;

propertiesList
@init { msgs.push("properties list"); }
@after { msgs.pop(); }
    :
      keyValueProperty (COMMA keyValueProperty)* -> ^(TOK_TABLEPROPLIST keyValueProperty+)
    ;

keyValueProperty
@init { msgs.push("specifying key/value property"); }
@after { msgs.pop(); }
    :
      (key=StringLiteral| key = Identifier) EQUAL (value=StringLiteral | value = Identifier) -> ^(TOK_TABLEPROPERTY $key $value)
    ;

tableRowFormatFieldIdentifier
@init { msgs.push("table row format's field separator"); }
@after { msgs.pop(); }
    :
      KW_FIELDS KW_TERMINATED KW_BY fldIdnt=StringLiteral (KW_ESCAPED KW_BY fldEscape=StringLiteral)?
    -> ^(TOK_TABLEROWFORMATFIELD $fldIdnt $fldEscape?)
    ;

tableRowFormatCollItemsIdentifier
@init { msgs.push("table row format's column separator"); }
@after { msgs.pop(); }
    :
      KW_COLLECTION KW_ITEMS KW_TERMINATED KW_BY collIdnt=StringLiteral
    -> ^(TOK_TABLEROWFORMATCOLLITEMS $collIdnt)
    ;

tableRowFormatMapKeysIdentifier
@init { msgs.push("table row format's map key separator"); }
@after { msgs.pop(); }
    :
      KW_MAP KW_KEYS KW_TERMINATED KW_BY mapKeysIdnt=StringLiteral
    -> ^(TOK_TABLEROWFORMATMAPKEYS $mapKeysIdnt)
    ;

tableRowFormatLinesIdentifier
@init { msgs.push("table row format's line separator"); }
@after { msgs.pop(); }
    :
      KW_LINES KW_TERMINATED KW_BY linesIdnt=StringLiteral
    -> ^(TOK_TABLEROWFORMATLINES $linesIdnt)
    ;

projectionField
@init { msgs.push("cluster filed name"); }
@after { msgs.pop(); }
    :
      identifier
    ;   
    
projectionFieldList
@init { msgs.push("cluster filed name list"); }
@after { msgs.pop(); }
    : projectionField (COMMA projectionField)* -> ^(TOK_SUBPROJECTION projectionField+)
    ;
	
projection
@init { msgs.push("one cluster specification "); }
@after { msgs.pop(); }
    : LPAREN projectionFieldList RPAREN -> ^( projectionFieldList)
    ;
    
projectionList
@init { msgs.push("cluster filed name list"); }
@after { msgs.pop(); }
    : projection (COMMA projection)* -> ^(TOK_PROJECTION projection+)
    ;
    
compress
@init { msgs.push("if compress clause"); }
@after { msgs.pop(); }
    : KW_COMPRESS
    -> ^(TOK_COMPRESS)
    ;
    
tableFileFormat
@init { msgs.push("table file format specification"); }
@after { msgs.pop(); }
    :
      KW_STORED KW_AS KW_SEQUENCEFILE  -> TOK_TBLSEQUENCEFILE
      | KW_STORED KW_AS KW_TEXTFILE  -> TOK_TBLTEXTFILE      
      | KW_STORED KW_AS KW_RCFILE  (isCompress=compress)? -> ^(TOK_TBLRCFILE $isCompress ?)
      | KW_STORED KW_AS KW_PGDATA  -> TOK_PGDATA
      | KW_STORED KW_AS KW_FORMATFILE (isCompress=compress)? -> ^(TOK_TBLFORMATFILE $isCompress ?)
      | KW_STORED KW_AS KW_COLUMNFILE (KW_PROJECTION projectionList)? (isCompress=compress)? -> ^(TOK_TBLCOLUMNFILE $isCompress ? ^(projectionList)?)
      | KW_STORED KW_AS KW_INPUTFORMAT inFmt=StringLiteral KW_OUTPUTFORMAT outFmt=StringLiteral
      -> ^(TOK_TABLEFILEFORMAT $inFmt $outFmt)
      | KW_STORED KW_AS KW_PBFILE  -> TOK_PB_FILE
      | KW_STORED KW_AS KW_PB  -> TOK_PB
      ;
indexFieldNameList
@init { msgs.push("column name list"); }
@after { msgs.pop(); }
    : indexFieldName (COMMA indexFieldName)* -> ^(TOK_INDEXFIELD indexFieldName+)
    ;

indexFieldName
@init { msgs.push("column name"); }
@after { msgs.pop(); }
    :
      Identifier
    ;

indexFieldList
@init { msgs.push("index filed name list"); }
@after { msgs.pop(); }
    :
      LPAREN indexFieldNameList RPAREN -> ^(indexFieldNameList)
    ;

indexName
@init { msgs.push("index name specification"); }
@after { msgs.pop(); }
    :
     Identifier -> ^(TOK_INDEXNAME Identifier)
    ;
    
tableIndex
@init { msgs.push("table index specification"); }
@after { msgs.pop(); }
    :
      KW_INDEX indexName? indexFieldList -> ^(TOK_INDEX indexName? indexFieldList)     
    ;
    

tableLocation
@init { msgs.push("table location specification"); }
@after { msgs.pop(); }
    :
      KW_LOCATION locn=StringLiteral -> ^(TOK_TABLELOCATION $locn)
    ;
  
columnNameTypeList
@init { msgs.push("column name type list"); }
@after { msgs.pop(); }
    : columnNameType (COMMA columnNameType)* -> ^(TOK_TABCOLLIST columnNameType+)
    ;

columnNameColonTypeList
@init { msgs.push("column name type list"); }
@after { msgs.pop(); }
    : columnNameColonType (COMMA columnNameColonType)* -> ^(TOK_TABCOLLIST columnNameColonType+)
    ;
   
columnNameList
@init { msgs.push("column name list"); }
@after { msgs.pop(); }
    : columnName (COMMA columnName)* -> ^(TOK_TABCOLNAME columnName+)
    ;

columnName
@init { msgs.push("column name"); }
@after { msgs.pop(); }
    :
      identifier
    ;

columnNameOrderList
@init { msgs.push("column name order list"); }
@after { msgs.pop(); }
    : columnNameOrder (COMMA columnNameOrder)* -> ^(TOK_TABCOLNAME columnNameOrder+)
    ;

columnNameOrder
@init { msgs.push("column name order"); }
@after { msgs.pop(); }
    : identifier (asc=KW_ASC | desc=KW_DESC)? 
    -> {$desc == null}? ^(TOK_TABSORTCOLNAMEASC identifier)
    ->                  ^(TOK_TABSORTCOLNAMEDESC identifier)
    ;

columnRefOrder
@init { msgs.push("column order"); }
@after { msgs.pop(); }
    : expression (asc=KW_ASC | desc=KW_DESC)? 
    -> {$desc == null}? ^(TOK_TABSORTCOLNAMEASC expression)
    ->                  ^(TOK_TABSORTCOLNAMEDESC expression)
    ;

columnNameCommentList
@init { msgs.push("column name comment list"); }
@after { msgs.pop(); }
    : columnNameComment (COMMA columnNameComment)* -> ^(TOK_TABCOLNAME columnNameComment+)
    ;

columnNameComment
@init { msgs.push("column name comment"); }
@after { msgs.pop(); }
    : colName=identifier (KW_COMMENT comment=StringLiteral)? 
    -> ^(TOK_TABCOL $colName TOK_NULL $comment?)
    ;

columnNameType
@init { msgs.push("column specification"); }
@after { msgs.pop(); }
    : colName=identifier colType (KW_COMMENT comment=StringLiteral)?    
    -> {$comment == null}? ^(TOK_TABCOL $colName colType)
    ->                     ^(TOK_TABCOL $colName colType $comment)
    ;

columnNameColonType
@init { msgs.push("column specification"); }
@after { msgs.pop(); }
    : colName=identifier COLON colType (KW_COMMENT comment=StringLiteral)?    
    -> {$comment == null}? ^(TOK_TABCOL $colName colType)
    ->                     ^(TOK_TABCOL $colName colType $comment)
    ;    

colType
@init { msgs.push("column type"); }
@after { msgs.pop(); }
    : type
    ;

type
    : primitiveType
    | listType
    | structType
    | mapType;

primitiveType
@init { msgs.push("primitive type specification"); }
@after { msgs.pop(); }
    : KW_TINYINT       ->    TOK_TINYINT
    | KW_SMALLINT      ->    TOK_SMALLINT
    | KW_INT           ->    TOK_INT
    | KW_BIGINT        ->    TOK_BIGINT
    | KW_BOOLEAN       ->    TOK_BOOLEAN
    | KW_FLOAT         ->    TOK_FLOAT
    | KW_DOUBLE        ->    TOK_DOUBLE
    | KW_DATE          ->    TOK_DATE
    | KW_DATETIME      ->    TOK_DATETIME
    | KW_TIMESTAMP     ->    TOK_TIMESTAMP
    | KW_STRING        ->    TOK_STRING
    ;

listType
@init { msgs.push("list type"); }
@after { msgs.pop(); }
    : KW_ARRAY LESSTHAN type GREATERTHAN   -> ^(TOK_LIST type)
    ;

structType
@init { msgs.push("struct type"); }
@after { msgs.pop(); }
    : KW_STRUCT LESSTHAN columnNameColonTypeList GREATERTHAN -> ^(TOK_STRUCT columnNameColonTypeList)
    ;

mapType
@init { msgs.push("map type"); }
@after { msgs.pop(); }
    : KW_MAP LESSTHAN left=primitiveType COMMA right=type GREATERTHAN
    -> ^(TOK_MAP $left $right)
    ;

queryOperator
@init { msgs.push("query operator"); }
@after { msgs.pop(); }
    : KW_UNION KW_ALL -> ^(TOK_UNION)
    | KW_UNION -> ^(TOK_UNIQUE_UNION)
    ;


queryStatementExpression
    
    : queryStatement1 (queryOperator^ queryStatement1)*
    ;

queryStatement
    :
    fromClause
    ( b+=body )+ -> ^(TOK_QUERY fromClause body+)
    | regular_body
    ;

queryStatement1
    :
    fromClause
    ( b+=body )+ -> ^(TOK_QUERY fromClause body+)
    | selectStatement
    ;
    
regular_body
   :
   
   insertIntoClause valuesClause-> ^(TOK_QUERY ^(TOK_INSERT insertIntoClause valuesClause))
   |
   insertClause queryStatementExpression -> ^(TOK_QUERY ^(TOK_FROM ^(TOK_SUBQUERY queryStatementExpression)) 
                                            ^(TOK_INSERT insertClause ^(TOK_SELECT ^(TOK_SELEXPR TOK_ALLCOLREF))))

   |
   selectStatement
   |
   withSource  
   ;
   
selectStatement
   :
   selectClause
   fromClause?
   whereClause?
   groupByClause?
   havingClause?
   orderByClause?
   clusterByClause?
   distributeByClause?
   sortByClause?
   limitClause? -> ^(TOK_QUERY fromClause? ^(TOK_INSERT ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE))
                     selectClause whereClause? groupByClause? havingClause? orderByClause? clusterByClause?
                     distributeByClause? sortByClause? limitClause?))
   ;

body
   :
   insertClause
   selectClause
   whereClause?
   groupByClause?
   havingClause?
   orderByClause?
   clusterByClause?
   distributeByClause?
   sortByClause?
   limitClause? -> ^(TOK_INSERT insertClause?
                     selectClause whereClause? groupByClause? havingClause? orderByClause? clusterByClause?
                     distributeByClause? sortByClause? limitClause?)
   |
   selectClause
   whereClause?
   groupByClause?
   havingClause?
   orderByClause?
   clusterByClause?
   distributeByClause?
   sortByClause?
   limitClause? -> ^(TOK_INSERT ^(TOK_DESTINATION ^(TOK_DIR TOK_TMP_FILE))
                     selectClause whereClause? groupByClause? havingClause? orderByClause? clusterByClause?
                     distributeByClause? sortByClause? limitClause?)
   ;

insertClause
@init { msgs.push("insert clause"); }
@after { msgs.pop(); }
   :
     KW_INSERT KW_OVERWRITE destination -> ^(TOK_DESTINATION  destination)
   | KW_INSERT destination -> ^(TOK_APPENDDESTINATION destination)
   ;


insertIntoClause
@init { msgs.push("insertIntoClause"); }
@after { msgs.pop(); }
   :
     KW_INSERT KW_OVERWRITE intodestination -> ^(TOK_DESTINATION  intodestination)
   | KW_INSERT intodestination -> ^(TOK_APPENDDESTINATION intodestination)
   ;

valuesClause
@init { msgs.push("valuesClause"); }
@after { msgs.pop(); }
	:
	KW_VALUES insertValues ( COMMA  insertValues )* -> ^(TOK_VALUES insertValues+)
	;

insertValues
@init { msgs.push("insertValues"); }
@after { msgs.pop(); }
	:
	LPAREN insertValue ( COMMA  insertValue )* RPAREN -> ^(TOK_VALUE insertValue+)
	;

insertValue
@init { msgs.push("insertValue"); }
@after { msgs.pop(); }
	:
	KW_NULL -> TOK_NULL
	| constant
	;

destination
@init { msgs.push("destination specification"); }
@after { msgs.pop(); }
   :
    
   KW_DIRECTORY StringLiteral -> ^(TOK_DIR StringLiteral)
    
	
   | KW_TABLE tabName insertColumns? -> ^(TOK_TABDEST tabName insertColumns?)
   ;

intodestination
@init { msgs.push("intodestination specification"); }
@after { msgs.pop(); }
   :
   KW_INTO tabName insertColumns? -> ^(TOK_TABDEST tabName insertColumns?)
   ;

insertColumns
@init { msgs.push("insertColumns specification"); }
@after { msgs.pop(); }
   :
   LPAREN tableOrColumn ( COMMA  tableOrColumn )* RPAREN -> tableOrColumn+
   ;
   
limitClause
@init { msgs.push("limit clause"); }
@after { msgs.pop(); }
   :
   KW_LIMIT num=Number -> ^(TOK_LIMIT $num)
   ;

selectClause
@init { msgs.push("select clause"); }
@after { msgs.pop(); }
    :
    KW_SELECT hintClause? (KW_ALL | dist=KW_DISTINCT)?
    selectList -> {$dist == null}? ^(TOK_SELECT hintClause? selectList)
               ->                  ^(TOK_SELECTDI hintClause? selectList)
    |
    trfmClause  ->^(TOK_SELECT ^(TOK_SELEXPR trfmClause) )
    ;

selectList
@init { msgs.push("select list"); }
@after { msgs.pop(); }
    :
     selectItem ( COMMA  selectItem )* -> selectItem+
    ;

hintClause
@init { msgs.push("hint clause"); }
@after { msgs.pop(); }
    :
    DIVIDE STAR PLUS hintList STAR DIVIDE -> ^(TOK_HINTLIST hintList)
    ;

hintList
@init { msgs.push("hint list"); }
@after { msgs.pop(); }
    :
    hintItem (COMMA hintItem)* -> hintItem+
    ;

hintItem
@init { msgs.push("hint item"); }
@after { msgs.pop(); }
    :
    hintName (LPAREN hintArgs RPAREN)? -> ^(TOK_HINT hintName hintArgs)
    ;

hintName
@init { msgs.push("hint name"); }
@after { msgs.pop(); }
    :
    KW_MAPJOIN -> TOK_MAPJOIN
    | KW_STREAMTABLE -> TOK_STREAMTABLE    
    ;

hintArgs
@init { msgs.push("hint arguments"); }
@after { msgs.pop(); }
    :
    hintArgName (COMMA hintArgName)* -> ^(TOK_HINTARGLIST hintArgName+)
    ;

hintArgName
@init { msgs.push("hint argument name"); }
@after { msgs.pop(); }
    :
    tableOrColumn   
    ;

selectItem
@init { msgs.push("selection target"); }
@after { msgs.pop(); }
    :
    
    ( selectExpression  ((KW_AS? identifier) | (KW_AS LPAREN identifier (COMMA identifier)* RPAREN))?) -> ^(TOK_SELEXPR selectExpression identifier*) 
    ;

trfmClause
@init { msgs.push("transform clause"); }
@after { msgs.pop(); }
    :
    ( KW_SELECT KW_TRANSFORM LPAREN selectExpressionList RPAREN
      | KW_MAP    selectExpressionList
      | KW_REDUCE selectExpressionList )
    inSerde=serde 
    KW_USING StringLiteral 
    ( KW_AS ((LPAREN (aliasList | columnNameTypeList) RPAREN) | (aliasList | columnNameTypeList)))? 
    outSerde=serde outRec=recordReader
    -> ^(TOK_TRANSFORM selectExpressionList $inSerde StringLiteral $outSerde $outRec aliasList? columnNameTypeList?)
    ;
    
selectExpression
@init { msgs.push("select expression"); }
@after { msgs.pop(); }
    :
    expression | tableAllColumns
    ;

selectExpressionList
@init { msgs.push("select expression list"); }
@after { msgs.pop(); }
    :
    selectExpression (COMMA selectExpression)* -> ^(TOK_EXPLIST selectExpression+)
    ;

tableAllColumns
    :
    STAR -> ^(TOK_ALLCOLREF)
    | /*(db=Identifier COLON COLON)? */tab=Identifier DOT STAR -> ^(TOK_ALLCOLREF $tab /*$db?*/)
    ;
    
tableOrColumn
@init { msgs.push("table or column identifier"); }
@after { msgs.pop(); }
    :
    (db=Identifier COLON COLON)? tb_or_col=identifier -> ^(TOK_TABLE_OR_COL $tb_or_col $db?)
    ;

expressionList
@init { msgs.push("expression list"); }
@after { msgs.pop(); }
    :
    expression (COMMA expression)* -> ^(TOK_EXPLIST expression+)
    ;

aliasList
@init { msgs.push("alias list"); }
@after { msgs.pop(); }
    :
    Identifier (COMMA Identifier)* -> ^(TOK_ALIASLIST Identifier+)
    ;
   
fromClause
@init { msgs.push("from clause"); }
@after { msgs.pop(); }
    :
    KW_FROM joinSource -> ^(TOK_FROM joinSource)
    ;

joinSource
@init { msgs.push("join source"); }
@after { msgs.pop(); }  
    : fromSource ( joinToken^ fromSource (KW_ON! expression)? )*  
    | uniqueJoinToken^ uniqueJoinSource (COMMA! uniqueJoinSource)+  
    ;

uniqueJoinSource
@init { msgs.push("join source"); }
@after { msgs.pop(); }
    : KW_PRESERVE? fromSource uniqueJoinExpr 
    ;

uniqueJoinExpr
@init { msgs.push("unique join expression list"); }
@after { msgs.pop(); }
    : LPAREN e1+=expression (COMMA e1+=expression)* RPAREN 
      -> ^(TOK_EXPLIST $e1*)
    ;

uniqueJoinToken
@init { msgs.push("unique join"); }
@after { msgs.pop(); }
    : KW_UNIQUEJOIN -> TOK_UNIQUEJOIN;


joinToken
@init { msgs.push("join type specifier"); }
@after { msgs.pop(); }
    :
      COMMA                       -> TOK_JOIN
    | KW_JOIN                     -> TOK_JOIN
    | KW_LEFT KW_OUTER? KW_JOIN    -> TOK_LEFTOUTERJOIN
    | KW_RIGHT KW_OUTER? KW_JOIN   -> TOK_RIGHTOUTERJOIN
    | KW_FULL KW_OUTER? KW_JOIN    -> TOK_FULLOUTERJOIN
    | KW_LEFT  KW_SEMI  KW_JOIN   -> TOK_LEFTSEMIJOIN      
    ;

lateralView
@init {msgs.push("lateral view"); }
@after {msgs.pop(); }
	:
	KW_LATERAL KW_VIEW function tableAlias KW_AS Identifier (COMMA Identifier)* -> ^(TOK_LATERAL_VIEW ^(TOK_SELECT ^(TOK_SELEXPR function Identifier+ tableAlias)))
	;

tableAlias
@init {msgs.push("table alias"); }
@after {msgs.pop(); }
    :
    Identifier -> ^(TOK_TABALIAS Identifier)
    ;
    
fromSource
@init { msgs.push("from source"); }
@after { msgs.pop(); }
    :
    (tableSource | subQuerySource)(lateralView^)*
    ;
    
tableSample
@init { msgs.push("table sample specification"); }
@after { msgs.pop(); }
    :
    KW_TABLESAMPLE LPAREN KW_BUCKET (numerator=Number) KW_OUT KW_OF (denominator=Number) (KW_ON expr+=expression (COMMA expr+=expression)*)? RPAREN -> ^(TOK_TABLESAMPLE $numerator $denominator $expr*)
    |KW_TABLESAMPLE LPAREN KW_LINE (numerator=Number) KW_OUT KW_OF (denominator=Number) (KW_LIMIT num=Number)? RPAREN -> ^(TOK_TABLESAMPLE_LINE $numerator $denominator $num?)
    ;

tableSource
@init { msgs.push("table source"); }
@after { msgs.pop(); }
    :
    tabName (ts=tableSample)? (alias=Identifier)? -> ^(TOK_TABREF tabName $ts? $alias?)
 
    ;
	
subQuerySource
@init { msgs.push("subquery source"); }
@after { msgs.pop(); }
    :
    LPAREN queryStatementExpression RPAREN Identifier? -> ^(TOK_SUBQUERY queryStatementExpression Identifier?)
    ;

newdestination
@init { msgs.push("newdestination specification"); }
@after { msgs.pop(); }
   :
   KW_INTO KW_TABLE tabName insertColumns? -> ^(TOK_TABDEST tabName insertColumns?)
   ;
withSource
@init { msgs.push("with  Source"); }
@after { msgs.pop(); }
	:
	KW_WITH subQueryeOfWith (COMMA subQueryeOfWith)*  selectStatement -> ^(TOK_QUERY ^(TOK_WITH subQueryeOfWith+ selectStatement))
	|
	KW_INSERT KW_OVERWRITE newdestination KW_WITH subQueryeOfWith (COMMA subQueryeOfWith)*  selectStatement 
	-> ^(TOK_QUERY ^(TOK_WITH subQueryeOfWith+ selectStatement) ^(TOK_INSERT ^(TOK_DESTINATION  newdestination) ^(TOK_SELECT ^(TOK_SELEXPR TOK_ALLCOLREF))) )
	|
	KW_INSERT newdestination KW_WITH subQueryeOfWith (COMMA subQueryeOfWith)*  selectStatement
	-> ^(TOK_QUERY ^(TOK_WITH subQueryeOfWith+ selectStatement) ^(TOK_INSERT ^(TOK_APPENDDESTINATION  newdestination) ^(TOK_SELECT ^(TOK_SELEXPR TOK_ALLCOLREF))) )
	;
	
subQueryeOfWith
@init { msgs.push("subquery with"); }
@after { msgs.pop(); }
    :
    Identifier KW_AS LPAREN queryStatementExpression RPAREN -> ^(TOK_SUBQUERY queryStatementExpression Identifier)
    ;

whereClause
@init { msgs.push("where clause"); }
@after { msgs.pop(); }
    :
    KW_WHERE searchCondition -> ^(TOK_WHERE searchCondition)
    ;

searchCondition
@init { msgs.push("search condition"); }
@after { msgs.pop(); }
    :
    expression
    ;

groupByClause
@init { msgs.push("group by clause"); }
@after { msgs.pop(); }
    :
    KW_GROUP KW_BY
    groupByExpression
    ( COMMA groupByExpression )*
    ((rollup=KW_WITH KW_ROLLUP) | (cube=KW_WITH KW_CUBE)) ?
    -> {rollup != null}? ^(TOK_GROUPBY ^(TOK_ROLLUP groupByExpression+))
    -> {cube != null}? ^(TOK_GROUPBY ^(TOK_CUBE groupByExpression+))
    -> ^(TOK_GROUPBY groupByExpression+)
    ;

groupByExpression
@init { msgs.push("group by expression"); }
@after { msgs.pop(); }
    :
    expression   
    |
    rollupExpression 
    |
    cubeExpression 
    |
    groupingsetsExpression 
    ;


groupExpression
    :
    KW_GROUP LPAREN e1+=expression? (COMMA e1+=expression)* RPAREN
    ->{$e1 != null}? ^(TOK_GROUP $e1*)  
    
    ->^(TOK_GROUP TOK_NULL)
    ;
    
multiDimensions
    :
    groupExpression
    |
    expression    
    ;
    	
rollupExpression
    :
    KW_ROLLUP LPAREN multiDimensions (COMMA multiDimensions)* RPAREN
    ->^(TOK_ROLLUP multiDimensions+)	
    ;
 	
cubeExpression
    : 
    KW_CUBE LPAREN multiDimensions (COMMA multiDimensions)* RPAREN
    ->^(TOK_CUBE multiDimensions+)
    ;

groupingsetsExpression
    : 
    KW_GROUPINGSETS LPAREN multiDimensions (COMMA multiDimensions)* RPAREN
    ->^(TOK_GROUPINGSETS multiDimensions+)
    ;
 
havingClause
@init { msgs.push("having clause"); }
@after { msgs.pop(); }
    :
    KW_HAVING havingCondition -> ^(TOK_HAVING havingCondition)
    ;
 
havingCondition
@init { msgs.push("having condition"); }
@after { msgs.pop(); }
    :
    expression
    ;

orderByClause
@init { msgs.push("order by clause"); }
@after { msgs.pop(); }
    :
    KW_ORDER KW_BY
    columnRefOrder
    ( COMMA columnRefOrder)* -> ^(TOK_ORDERBY columnRefOrder+)
    ;

clusterByClause
@init { msgs.push("cluster by clause"); }
@after { msgs.pop(); }
    :
    KW_CLUSTER KW_BY
    expression
    ( COMMA expression )* -> ^(TOK_CLUSTERBY expression+)
    ;

distributeByClause
@init { msgs.push("distribute by clause"); }
@after { msgs.pop(); }
    :
    KW_DISTRIBUTE KW_BY
    expression (COMMA expression)* -> ^(TOK_DISTRIBUTEBY expression+)
    ;

sortByClause
@init { msgs.push("sort by clause"); }
@after { msgs.pop(); }
    :
    KW_SORT KW_BY
    columnRefOrder
    ( COMMA columnRefOrder)* -> ^(TOK_SORTBY columnRefOrder+)
    ;
    
function
@init { msgs.push("function specification"); }
@after { msgs.pop(); }
    :
    functionName
    LPAREN
      (
        (star=STAR)
        | (dist=KW_DISTINCT)? (expression (COMMA expression)*)?
        
      )
    RPAREN
    (over=KW_OVER)?
    ( 
       LPAREN
         (
            partitionByClause?
            orderByClause?
          )
       RPAREN 
     )? -> {$star != null}? ^(TOK_FUNCTIONSTAR functionName)
           ->{$over != null }? ^(TOK_FUNCTIONOVER  functionName ($dist)? (^(TOK_FUNCPARAMETER expression+))? partitionByClause? orderByClause?)
           -> {$dist == null}? ^(TOK_FUNCTION functionName (expression+)?)
           -> ^(TOK_FUNCTIONDI functionName (expression+)?)
    ;

funcParameter
@init {  msgs.push("function over parameter"); }
@after { msgs.pop(); }
    	:
    	expression (COMMA expression)*
    	->	
    	^(TOK_FUNCPARAMETER expression+)
    	;
    	
partitionByClause
@init {  msgs.push("partition by clause in function over"); }
@after { msgs.pop(); }
	:
	KW_PARTITION KW_BY 
	expression 
	(COMMA expression)*
	->
	^(TOK_PARTITIONBY expression+)
	;

functionName
@init { msgs.push("function name"); }
@after { msgs.pop(); }
    : 
    Identifier | KW_IF | KW_ARRAY | KW_STRUCT
    ;

castExpression
@init { msgs.push("cast expression"); }
@after { msgs.pop(); }
    :
    KW_CAST
    LPAREN 
          expression
          KW_AS
          primitiveType
    RPAREN -> ^(TOK_FUNCTION primitiveType expression)
    ;
    
caseExpression
@init { msgs.push("case expression"); }
@after { msgs.pop(); }
    :
    KW_CASE expression
    (KW_WHEN expression KW_THEN expression)+
    (KW_ELSE expression)?
    KW_END -> ^(TOK_FUNCTION KW_CASE expression*)
    ;
    
whenExpression
@init { msgs.push("case expression"); }
@after { msgs.pop(); }
    :
    KW_CASE
     ( KW_WHEN expression KW_THEN expression)+
    (KW_ELSE expression)?
    KW_END -> ^(TOK_FUNCTION KW_WHEN expression*)
    ;
    
constant
@init { msgs.push("constant"); }
@after { msgs.pop(); }
    :
    Number
    | StringLiteral
    | charSetStringLiteral
    | booleanValue 
    | negativeNumber->^(TOK_NNUMBER negativeNumber)
    ;

charSetStringLiteral
@init { msgs.push("character string literal"); }
@after { msgs.pop(); }
    :
    csName=CharSetName csLiteral=CharSetLiteral -> ^(TOK_CHARSETLITERAL $csName $csLiteral)
    ;

expression
@init { msgs.push("expression specification"); }
@after { msgs.pop(); }
    :
    precedenceOrExpression
    ;

atomExpression
    :
    KW_NULL -> TOK_NULL
    | constant
    | function
    | castExpression
    | caseExpression
    | whenExpression
    | tableOrColumn
    | LPAREN! expression RPAREN!
    ;

precedenceFieldExpression
    :
    atomExpression ((LSQUARE^ expression RSQUARE!) | (DOT^ identifier))*
    ;

precedenceUnaryOperator
    :
    PLUS | MINUS | TILDE
    ;

nullCondition
    :
    KW_NULL -> ^(TOK_ISNULL)
    | KW_NOT KW_NULL -> ^(TOK_ISNOTNULL)
    ;

precedenceUnaryPrefixExpression
    :
    (precedenceUnaryOperator^)* precedenceFieldExpression
    ;

precedenceUnarySuffixExpression
    : precedenceUnaryPrefixExpression (a=KW_IS nullCondition)?
    -> {$a != null}? ^(TOK_FUNCTION nullCondition precedenceUnaryPrefixExpression)
    -> precedenceUnaryPrefixExpression
    ;

precedenceBitwiseShiftLeftOperator
    :
    BITWISESHIFTLEFT
    ;

precedenceBitwiseShiftLeftExpression
    :
    precedenceUnarySuffixExpression (precedenceBitwiseShiftLeftOperator^ precedenceUnarySuffixExpression)*
    ;

precedenceBitwiseShiftRightOperator
    :
    BITWISESHIFTRIGHT
    ;

precedenceBitwiseShiftRightExpression
    :
    precedenceBitwiseShiftLeftExpression (precedenceBitwiseShiftRightOperator^ precedenceBitwiseShiftLeftExpression)*
    ;
    
precedenceBitwiseXorOperator
    :
    BITWISEXOR
    ;

precedenceBitwiseXorExpression
    :
    precedenceBitwiseShiftRightExpression (precedenceBitwiseXorOperator^ precedenceBitwiseShiftRightExpression)*
    ;

	
precedenceStarOperator
    :
    STAR | DIVIDE | MOD | DIV | DIVIDEZERONULL
    ;

precedenceStarExpression
    :
    precedenceBitwiseXorExpression (precedenceStarOperator^ precedenceBitwiseXorExpression)*
    ;

precedencePlusOperator
    :
    PLUS | MINUS 
    ;

precedencePlusExpression
    :
    precedenceStarExpression (precedencePlusOperator^ precedenceStarExpression)*
    ;

precedenceConcatOperator
    :
    CONCAT 
    ;

precedenceConcatExpression
    :
    precedencePlusExpression (precedenceConcatOperator^ precedencePlusExpression)*
    ;

precedenceAmpersandOperator
    :
    AMPERSAND
    ;

precedenceAmpersandExpression
    :
    precedenceConcatExpression (precedenceAmpersandOperator^ precedenceConcatExpression)*
    ;


precedenceBitwiseOrOperator
    :
    BITWISEOR
    ;

precedenceBitwiseOrExpression
    :
    precedenceAmpersandExpression (precedenceBitwiseOrOperator^ precedenceAmpersandExpression)*
    ;

precedenceEqualOperator
    :
    EQUAL | NOTEQUAL | LESSTHANOREQUALTO | LESSTHAN | GREATERTHANOREQUALTO | GREATERTHAN
    | KW_LIKE | KW_RLIKE | KW_REGEXP 
    ;

precedenceInOperator
     : 
     KW_IN
     ;

precedenceBetweenOperator
     : 
     KW_BETWEEN
     ;	 

precedenceEqualExpression
    :
    precedenceBitwiseOrExpression ((precedenceNotOperator^)* (precedenceEqualOperator^ precedenceBitwiseOrExpression))* ((precedenceNotOperator^)* (precedenceInOperator^ inParameterList))*  (precedenceBetweenOperator^ betweenParameterList)*
    ;

precedenceNotOperator
    :
    KW_NOT
    ;
    
betweenParameterList
	:
	precedenceBitwiseOrExpression precedenceAndOperator precedenceBitwiseOrExpression
	-> precedenceBitwiseOrExpression+
	;
	

inParameterList
    :
    LPAREN ( precedenceBitwiseOrExpression (COMMA precedenceBitwiseOrExpression)* ) RPAREN
    -> precedenceBitwiseOrExpression+
    
    ;

existsSource
@init { msgs.push("exists source"); }
@after { msgs.pop(); }
    :
    LPAREN queryStatementExpression RPAREN -> ^(TOK_SUBQUERY queryStatementExpression)
    ;
    
precedenceExistsExpression
    :
    precedenceExistsOperator^ (existsSource)
    ;

precedenceExistsOperator
    :
    KW_EXISTS
    ;	

precedenceEqualOrExistsExpression
    :
     precedenceEqualExpression | precedenceExistsExpression
    ;
 
precedenceNotExpression
    :
    
    (precedenceNotOperator^)* precedenceEqualOrExistsExpression
    ;

precedenceAndOperator
    :
    KW_AND
    ;

precedenceAndExpression
    :
    precedenceNotExpression (precedenceAndOperator^ precedenceNotExpression)*   
    ;


precedenceOrOperator
    :
    KW_OR
    ;

precedenceOrExpression
    :
    precedenceAndExpression (precedenceOrOperator^ precedenceAndExpression)*
    ;


booleanValue
    :
    KW_TRUE^ | KW_FALSE^
    ;

tabName
   :
   (db=Identifier COLON COLON)? tab=Identifier partitionRef? -> ^(TOK_TAB $tab partitionRef? $db?)
   ;
   
partitionRef
	: partitionLevelRef
	| partitionCompRef	
	
	;

   partitionLevelRef
   	: KW_PARTITION LPAREN ( id = KW_DEFAULT | id = Identifier ) RPAREN -> ^(TOK_PARTITIONREF $id)
   	| KW_SUBPARTITION LPAREN (id = KW_DEFAULT | id = Identifier) RPAREN -> ^(TOK_SUBPARTITIONREF $id)
   	|  KW_PARTITION LPAREN  partitionVal  RPAREN -> ^(TOK_PARTITIONREF partitionVal)
   	;
   partitionCompRef
   	:  KW_PARTITION LPAREN (pri = KW_DEFAULT | pri= Identifier) COMMA (sub = KW_DEFAULT | sub=Identifier) RPAREN -> ^(TOK_COMPPARTITIONREF $pri $sub)  	
   	;

partitionVal
    :
    tableOrColumn EQUAL constant -> ^(TOK_PARTVAL tableOrColumn constant)
    ; 
 
// Keywords
KW_TRUE : 'TRUE';
KW_FALSE : 'FALSE';
KW_ALL : 'ALL';
KW_AND : 'AND';
KW_OR : 'OR';
KW_NOT : 'NOT' | '!';
KW_LIKE : 'LIKE';

KW_IF : 'IF';
KW_EXISTS : 'EXISTS';

KW_ASC : 'ASC';
KW_DESC : 'DESC';
KW_ORDER : 'ORDER';
KW_BY : 'BY';
KW_GROUP : 'GROUP';
KW_ROLLUP : 'ROLLUP'; 
KW_CUBE	: 'CUBE';     
KW_GROUPINGSETS : 'GROUPINGSETS';  
KW_HAVING : 'HAVING';
KW_WHERE : 'WHERE';
KW_FROM : 'FROM';
KW_AS : 'AS';
KW_SELECT : 'SELECT';
KW_DISTINCT : 'DISTINCT';
KW_INSERT : 'INSERT';
KW_OVERWRITE : 'OVERWRITE';
KW_OUTER : 'OUTER';
KW_UNIQUEJOIN : 'UNIQUEJOIN';  
KW_PRESERVE : 'PRESERVE';    
KW_JOIN : 'JOIN';
KW_LEFT : 'LEFT';
KW_RIGHT : 'RIGHT';
KW_FULL : 'FULL';
KW_ON : 'ON';
KW_PARTITION : 'PARTITION';
KW_PARTITIONS : 'PARTITIONS';
KW_TABLE: 'TABLE';
KW_TABLES: 'TABLES';
KW_FUNCTIONS: 'FUNCTIONS';
KW_SHOW: 'SHOW';

KW_PROCESSLIST: 'PROCESSLIST';  
KW_KILLQUERY: 'KILLQUERY';      
KW_SHOWQUERY: 'SHOWQUERY';       
KW_CLEARQUERY: 'CLEARQUERY';      
KW_STATINFO: 'STATINFO';   
KW_USERGROUP: 'USERGROUP';
KW_USERGROUPS: 'USERGROUPS';	
KW_TABLESIZE: 'TABLESIZE';
KW_DATABASESIZE: 'DATABASESIZE';
KW_ROWCOUNT: 'ROWCOUNT';

KW_MSCK: 'MSCK';
KW_DIRECTORY: 'DIRECTORY';
KW_LOCAL: 'LOCAL';
KW_TRANSFORM : 'TRANSFORM';
KW_USING: 'USING';
KW_CLUSTER: 'CLUSTER';
KW_DISTRIBUTE: 'DISTRIBUTE';
KW_SORT: 'SORT';
KW_UNION: 'UNION';
KW_LOAD: 'LOAD';
KW_DATA: 'DATA';
KW_INPATH: 'INPATH';
KW_IS: 'IS';
KW_NULL: 'NULL';
KW_CREATE: 'CREATE';
KW_EXTERNAL: 'EXTERNAL';
KW_ALTER: 'ALTER';

KW_CHANGE: 'CHANGE';
KW_COLUMN: 'COLUMN';
KW_FIRST: 'FIRST';
KW_AFTER: 'AFTER';

KW_DESCRIBE: 'DESCRIBE';
KW_DROP: 'DROP';
KW_RENAME: 'RENAME';
KW_TO: 'TO';
KW_COMMENT: 'COMMENT';
KW_BOOLEAN: 'BOOLEAN';
KW_TINYINT: 'TINYINT';
KW_SMALLINT: 'SMALLINT';
KW_INT: 'INT';
KW_BIGINT: 'BIGINT';
KW_FLOAT: 'FLOAT';
KW_DOUBLE: 'DOUBLE';
KW_DATE: 'DATE';
KW_DATETIME: 'DATETIME';
KW_TIMESTAMP: 'TIMESTAMP';
KW_STRING: 'STRING';
KW_ARRAY: 'ARRAY';
KW_STRUCT: 'STRUCT';
KW_MAP: 'MAP';
KW_REDUCE: 'REDUCE';
KW_PARTITIONED: 'PARTITIONED';
KW_CLUSTERED: 'CLUSTERED';
KW_SORTED: 'SORTED';
KW_INTO: 'INTO';
KW_BUCKETS: 'BUCKETS';
KW_ROW: 'ROW';
KW_FORMAT: 'FORMAT';
KW_DELIMITED: 'DELIMITED';
KW_FIELDS: 'FIELDS';
KW_TERMINATED: 'TERMINATED';
KW_ESCAPED: 'ESCAPED';
KW_COLLECTION: 'COLLECTION';
KW_ITEMS: 'ITEMS';
KW_KEYS: 'KEYS';
KW_KEY_TYPE: '$KEY$';
KW_LINES: 'LINES';
KW_STORED: 'STORED';
KW_SEQUENCEFILE: 'SEQUENCEFILE';
KW_TEXTFILE: 'TEXTFILE';
KW_RCFILE: 'RCFILE';
KW_PGDATA: 'PGDATA'; 
KW_INPUTFORMAT: 'INPUTFORMAT';
KW_OUTPUTFORMAT: 'OUTPUTFORMAT';
KW_LOCATION: 'LOCATION';
KW_TABLESAMPLE: 'TABLESAMPLE';
KW_BUCKET: 'BUCKET';
KW_LINE: 'LINE';
KW_OUT: 'OUT';
KW_OF: 'OF';
KW_CAST: 'CAST';
KW_ADD: 'ADD';
KW_REPLACE: 'REPLACE';
KW_COLUMNS: 'COLUMNS';
KW_RLIKE: 'RLIKE';
KW_REGEXP: 'REGEXP';
KW_TEMPORARY: 'TEMPORARY';
KW_FUNCTION: 'FUNCTION';
KW_EXPLAIN: 'EXPLAIN';
KW_EXTENDED: 'EXTENDED';
KW_SERDE: 'SERDE';
KW_WITH: 'WITH';
KW_SERDEPROPERTIES: 'SERDEPROPERTIES';
KW_LIMIT: 'LIMIT';
KW_SET: 'SET';

KW_TBLPROPERTIES: 'TBLPROPERTIES';  
KW_VALUE_TYPE: '$VALUE$';
KW_ELEM_TYPE: '$ELEM$';
KW_CASE: 'CASE';
KW_WHEN: 'WHEN';
KW_THEN: 'THEN';
KW_ELSE: 'ELSE';
KW_END: 'END';
KW_MAPJOIN: 'MAPJOIN';
KW_STREAMTABLE: 'STREAMTABLE';        
KW_CLUSTERSTATUS: 'CLUSTERSTATUS';
KW_UTC: 'UTC';
KW_UTCTIMESTAMP: 'UTC_TMESTAMP';
KW_LONG: 'LONG';
KW_DELETE: 'DELETE';
KW_PLUS: 'PLUS';
KW_MINUS: 'MINUS';
KW_FETCH: 'FETCH';
KW_INTERSECT: 'INTERSECT';
KW_VIEW: 'VIEW';

KW_VIEWTABLES
  : 'VIEWTABLES';

KW_IN: 'IN';
KW_DATABASE: 'DATABASE';
KW_MATERIALIZED: 'MATERIALIZED';
KW_SCHEMA: 'SCHEMA';
KW_SCHEMAS: 'SCHEMAS';
KW_GRANT: 'GRANT';
KW_REVOKE: 'REVOKE';
KW_SSL: 'SSL';
KW_UNDO: 'UNDO';
KW_LOCK: 'LOCK';
KW_UNLOCK: 'UNLOCK';
KW_PROCEDURE: 'PROCEDURE';
KW_UNSIGNED: 'UNSIGNED';
KW_WHILE: 'WHILE';
KW_READ: 'READ';
KW_READS: 'READS';
KW_PURGE: 'PURGE';
KW_RANGE: 'RANGE';
KW_ANALYZE: 'ANALYZE';
KW_BEFORE: 'BEFORE';
KW_BETWEEN: 'BETWEEN';
KW_BOTH: 'BOTH';
KW_BINARY: 'BINARY';
KW_CROSS: 'CROSS';
KW_CONTINUE: 'CONTINUE';
KW_CURSOR: 'CURSOR';
KW_TRIGGER: 'TRIGGER';
KW_RECORDREADER: 'RECORDREADER';
KW_SEMI: 'SEMI';                 
KW_OVER: 'OVER';                 
KW_LATERAL: 'LATERAL';

KW_LIST:  'LIST';
KW_HASH:  'HASHKEY'; 
KW_LESS: 'LESS';
KW_THAN: 'THAN';
KW_VALUES :'VALUES';
KW_SUBPARTITION: 'SUBPARTITION';	
KW_MODIFY
	:	'MODIFY';
KW_REMOVE
	:	'REMOVE';
KW_MERGE:	'MERGE';
KW_EXCHANGE
	:	'EXCHANGE';

KW_DEFAULT
	:	'DEFAULT';

KW_TRUNCATE
	:	'TRUNCATE';
	
KW_USER	:	'USER';

KW_DBA	:	'DBA';
KW_IDENTIFIED
	:	'IDENTIFIED';

KW_USERS:'USERS';
KW_GRANTS:'GRANTS';
KW_FOR	:	'FOR';
KW_ROLE:'ROLE';

KW_ROLES:	'ROLES';
KW_SET_PASSWD
	:	'SETPASSWD';
KW_UPDATE
	:	'UPDATE';
KW_INDEX:'INDEX';
KW_CREATEVIEW:'CREATEVIEW';
KW_SHOWVIEW:'SHOWVIEW';
KW_DATABASES
	:	 'DATABASES';
KW_USE :'USE';


KW_FORMATFILE:	'FORMATFILE';
KW_COLUMNFILE: 'COLUMNFILE';
KW_COMPRESS: 'COMPRESS';	
KW_PROJECTION: 'PROJECTION';

KW_PBFILE 
	:	'PBFILE';
KW_PB 
	:	'PB';
	
KW_CONTAINS: 'CONTAINS';
KW_COLUMNNAME: 'COLUMN_NAME';

KW_RESERVE
	:	'RESERVE';
KW_CHARSET
	:	'CHARSET';
KW_VERSION
  : 'VERSION';
  
KW_EXEC_EXT_SQL:	'EXECEXTSQL';

DOT : '.'; 
COLON : ':' ;
COMMA : ',' ;
SEMICOLON : ';' ;

LPAREN : '(' ;
RPAREN : ')' ;
LSQUARE : '[' ;
RSQUARE : ']' ;
LCURLY : '{';
RCURLY : '}';

EQUAL : '=' | '==';
NOTEQUAL : '<>' | '!=';
LESSTHANOREQUALTO : '<=';
LESSTHAN : '<';
GREATERTHANOREQUALTO : '>=';
GREATERTHAN : '>';

DIVIDE : '/';
DIVIDEZERONULL : '//';
PLUS : '+';
MINUS : '-';
STAR : '*';
MOD : '%';
DIV : 'DIV';

CONCAT : '||';

AMPERSAND : '&';
TILDE : '~';
BITWISEOR : '|';
BITWISEXOR : '^';
BITWISESHIFTLEFT : '<<';
BITWISESHIFTRIGHT : '>>';
QUESTION : '?';
DOLLAR : '$';

fragment
Letter
    : 'a'..'z' | 'A'..'Z'
    ;
fragment
HexDigit
    : 'a'..'f' | 'A'..'F' 
    ;
fragment
Digit
    :
    '0'..'9'
    ;

fragment
Exponent
    :
    'e' ( PLUS|MINUS )? (Digit)+
    ;

fragment
RegexComponent
    : 'a'..'z' | 'A'..'Z' | '0'..'9' | '_'
    | PLUS | STAR | QUESTION | MINUS | DOT
    | LPAREN | RPAREN | LSQUARE | RSQUARE | LCURLY | RCURLY
    | BITWISEXOR | BITWISEOR | DOLLAR
    ;

StringLiteral
    :
    ( '\'' ( ~('\''|'\\') | ('\\' .) )* '\'' 
    | '\"' ( ~('\"'|'\\') | ('\\' .) )* '\"' 
    )+
    ;

CharSetLiteral
    :    
    StringLiteral 
    | '0' 'X' (HexDigit|Digit)+
    ;

Number
    :
    (Digit)+ ( DOT (Digit)* (Exponent)? | Exponent)?
    ;


negativeNumber
    :
    '-' Number
    ;

Identifier
    :
    (Letter | Digit | '_') (Letter | Digit | '_')*
    | '`' RegexComponent+ '`'
    
    ;

identifier
    :
    Identifier
    | nonReserved -> Identifier[$nonReserved.text]
    ;

nonReserved
    :
     KW_USER | KW_ROLE | KW_VERSION
     ;
     
CharSetName
    :
    '_' (Letter | Digit | '_' | '-' | '.' | ':' )+
    ;

WS  :  (' '|'\r'|'\t'|'\n') {$channel=HIDDEN;}
    ;

COMMENT
  : '--' (~('\n'|'\r'))*
    { $channel=HIDDEN; }
  ;