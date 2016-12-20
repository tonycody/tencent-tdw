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

package org.apache.hadoop.hive.ql.parse;

import org.antlr.runtime.tree.*;

import org.apache.hadoop.hive.ql.metadata.HiveUtils;

public enum ErrorMsg {
  GENERIC_ERROR("Exception while processing"), INVALID_TABLE("Table not found"), INVALID_COLUMN(
      "Invalid Column Reference"), INVALID_TABLE_OR_COLUMN(
      "Invalid Table Alias or Column Reference"), AMBIGUOUS_TABLE_OR_COLUMN(
      "Ambiguous Table Alias or Column Reference"), INVALID_PARTITION(
      "Partition not found"), AMBIGUOUS_COLUMN("Ambiguous Column Reference"), AMBIGUOUS_TABLE_ALIAS(
      "Ambiguous Table Alias"), INVALID_TABLE_ALIAS("Invalid Table Alias"), NO_TABLE_ALIAS(
      "No Table Alias"), INVALID_FUNCTION("Invalid Function"), INVALID_FUNCTION_SIGNATURE(
      "Function Argument Type Mismatch"), INVALID_OPERATOR_SIGNATURE(
      "Operator Argument Type Mismatch"), INVALID_ARGUMENT("Wrong Arguments"), INVALID_ARGUMENT_LENGTH(
      "Arguments Length Mismatch"), INVALID_ARGUMENT_TYPE(
      "Argument Type Mismatch"), INVALID_JOIN_CONDITION_1(
      "Both Left and Right Aliases Encountered in Join"), INVALID_JOIN_CONDITION_2(
      "Neither Left nor Right Aliases Encountered in Join"), INVALID_JOIN_CONDITION_3(
      "OR not supported in Join currently"), INVALID_JOIN_CONDITION_4(
      "Ambiguous Table or Column: "), INVALID_JOIN_CONDITION_5(
      "Unknown Table or Column: "), INVALID_JOIN_CONDITION_6(
      "Error join table name: "), INVALID_TRANSFORM(
      "TRANSFORM with Other Select Columns not Supported"), DUPLICATE_GROUPBY_KEY(
      "Repeated Key in Group By"), UNSUPPORTED_MULTIPLE_DISTINCTS(
      "DISTINCT on Different Columns not Supported with skew in data"), NO_SUBQUERY_ALIAS(
      "No Alias For Subquery"), NO_INSERT_INSUBQUERY(
      "Cannot insert in a Subquery. Inserting to table "), NON_KEY_EXPR_IN_GROUPBY(
      "Expression Not In Group By Key"), INVALID_XPATH(
      "General . and [] Operators are Not Supported"), INVALID_PATH(
      "Invalid Path"), ILLEGAL_PATH("Path is not legal"), INVALID_NUMERICAL_CONSTANT(
      "Invalid Numerical Constant"), INVALID_ARRAYINDEX_CONSTANT(
      "Non Constant Expressions for Array Indexes not Supported"), INVALID_MAPINDEX_CONSTANT(
      "Non Constant Expression for Map Indexes not Supported"), INVALID_MAPINDEX_TYPE(
      "Map Key Type does not Match Index Expression Type"), NON_COLLECTION_TYPE(
      "[] not Valid on Non Collection Types"), SELECT_DISTINCT_WITH_GROUPBY(
      "SELECT DISTINCT and GROUP BY can not be in the same query"), PARTITIONING_COLS_SHOULD_IN_COLUMNS(
      "partitioning columns should in table columns"), DUPLICATE_COLUMN_NAMES(
      "Duplicate column name:"), COLUMN_REPEATED_IN_CLUSTER_SORT(
      "Same column cannot appear in cluster and sort by"), SAMPLE_RESTRICTION(
      "Cannot Sample on More Than Two Columns"), SAMPLE_COLUMN_NOT_FOUND(
      "Sample Column Not Found"), NO_PARTITION_PREDICATE(
      "No Partition Predicate Found"), INVALID_DOT(
      ". operator is only supported on struct or list of struct types"), INVALID_TBL_DDL_SERDE(
      "Either list of columns or a custom serializer should be specified"), TARGET_TABLE_COLUMN_MISMATCH(
      "Cannot insert into target table because column number/types are different"), TABLE_ALIAS_NOT_ALLOWED(
      "Table Alias not Allowed in Sampling Clause"), CLUSTERBY_DISTRIBUTEBY_CONFLICT(
      "Cannot have both Cluster By and Distribute By Clauses"), ORDERBY_DISTRIBUTEBY_CONFLICT(
      "Cannot have both Order By and Distribute By Clauses"), CLUSTERBY_SORTBY_CONFLICT(
      "Cannot have both Cluster By and Sort By Clauses"), ORDERBY_SORTBY_CONFLICT(
      "Cannot have both Order By and Sort By Clauses"), CLUSTERBY_ORDERBY_CONFLICT(
      "Cannot have both Cluster By and Order By Clauses"), NO_LIMIT_WITH_ORDERBY(
      "In strict mode, limit must be specified if ORDER BY is present"), NO_LIMIT_WITH_ORDERBY_FORTOP(
      "Limit must be specified if ORDER BY is present"), INVALID_LIMIT_COUNT_FOR_ORDERBY(
      "Limit value is Too BIG for ORDER BY(Default Max Limit:1024)"), NO_CARTESIAN_PRODUCT(
      "In strict mode, cartesian product is not allowed. If you really want to perform the operation, set hive.mapred.mode=nonstrict"), UNION_NOTIN_SUBQ(
      "Top level Union is not supported currently; use a subquery for the union"), INVALID_INPUT_FORMAT_TYPE(
      "Input Format must implement InputFormat"), INVALID_OUTPUT_FORMAT_TYPE(
      "Output Format must implement HiveOutputFormat, otherwise it should be either IgnoreKeyTextOutputFormat or SequenceFileOutputFormat"), NO_VALID_PARTN(
      "The query does not reference any valid partition. To run this query, set hive.mapred.mode=nonstrict"), NO_FULL_OUTER_MAPJOIN(
      "Map Join cannot be performed with Full Outer join"), LEFT_OUTER_MAPJOIN(
      "Left Map Join should place the hint table on the right of join operator"), RIGHT_OUTER_MAPJOIN(
      "Right Map Join should place the hint table on the left of join operator"), INVALID_MAPJOIN_HINT(
      "neither table specified as map-table"), INVALID_MAPJOIN_TABLE(
      "result of a union cannot be a map table"), NON_BUCKETED_TABLE(
      "Sampling Expression Needed for Non-Bucketed Table"), NEED_PARTITION_ERROR(
      "need to specify partition columns because the destination table is partitioned."),

  /* add by roachxiang for ctas begin */
  CTAS_CTLT_COEXISTENCE(
      "Create table command does not allow LIKE and AS-SELECT in the same command"), CTAS_COLLST_COEXISTENCE(
      "Create table as select command cannot specify the list of columns for the target table."), CTLT_COLLST_COEXISTENCE(
      "Create table like command cannot specify the list of columns for the target table."), INVALID_SELECT_SCHEMA(
      "Cannot derive schema from the select-clause."), CTAS_PARCOL_COEXISTENCE(
      "CREATE-TABLE-AS-SELECT does not support partitioning in the target table."), CTAS_MULTI_LOADFILE(
      "CREATE-TABLE-AS-SELECT results in multiple file load."), CTAS_EXTTBL_COEXISTENCE(
      "CREATE-TABLE-AS-SELECT cannot create external table."), CTAS_PB_COEXISTENCE(
      "CREATE-TABLE-AS-SELECT cannot create protobuf table."), CTAS_INDEX_COEXISTENCE(
      "CREATE-TABLE-AS-SELECT cannot create index table."), TABLE_ALREADY_EXISTS(
      "Table already exists:"),
  /* add by roachxiang for ctas end */

  STORE_AS_PGDATA_WITHOUT_EXTERNAL("Table Stored as pgdata should be external"), STORE_AS_PGDATA_NOT_SUPPORT(
      "Table Stored as pgdata NOT support"),

  /* add by roachxiang for delete begin */
  DELETE_FROM_OTHERSOURCE("can only delete from table"), DELETE_PARTITION_TABLE(
      "can not delete from partition table"), DELETE_EXTERNAL_TABLE(
      "can not delete from external table"),
  /* add by roachxiang for delete end */

  UPDATE_HASH_TABLE("hash table can not update or delete"), UPDATE_PARTITION2_TABLE(
      "wrong partition in table"), UPDATE_PARTITION1_TABLE(
      "this is a partition table"), UPDATE_EXTERNAL_TABLE(
      "can not update external table"), UPDATE_VIEW(
      "can not update or delete view"), UPDATE_COL_NOTFIND("can not find col"), UPDATE_COLTYPE_NOTFIND(
      "can not find col type"), UPDATE_EXIST_ERROR(
      "exist can not be used in where of update"), UPDATE_PARTITION_NOT_EXIST_ERROR(
      "partition not exist"), UPDATE_PARTITIONCOL_ERROR(
      "partition column can not update "), WITH_CHECK_ERROR(
      "with is not correctly used "), TIMESTAMP_ERROR(
      "timestamp should not be used in insert values "),

  PROJECTION_TOO_MANY("projection must no exceed 20"), PROJECTION_FIELD_DUP(
      "projection field duplicate"), PROJECTION_FIELD_NOT_EXIST(
      "projection field not exist"), PROJECTION_NOT_DEFINE(
      "field exceed 20, must define projection"),

  INDEX_FIELD_DUP("index field duplicate"), INDEX_FIELD_NOT_EXIST(
      "index field not exist"),

  PARAMETER_NUMBER_NOT_MATCH("too much parmeters for the Analysis Function"), PARTITION_BY_PARAMETER_NOT_MATCH(
      "parameters in partition by clauses are not equal in the Analysis Functions"), PARTITION_BY_CLAUSE_NOT_EXIST(
      "partition by clauses do not exist in the Analysis Functions"), ORDER_BY_CLAUSE_NOT_EXIST(
      "order by clauses do not exist in the Analysis Functions"), ORDER_BY_PARAMETER_NOT_MATCH(
      "parameters do not equal in the order by clauses"), ORDER_BY_CONFLICT_WITH_DISTINCT(
      "order by clauses conflict with distinct"), ORDER_BY_WITH_WRONG_PARAMETERS(
      "something wrong with parameters in the order by clauses"), DUPLICATE_PARTITIONBY_KEY(
      "parameters in partition by clauses repeat in the Analysis Function"),

  UDTF_MULTIPLE_EXPR(
      "Only a single expression in the SELECT clause is supported with UDTF's"), UDTF_REQUIRE_AS(
      "UDTF's require an AS clause"), UDTF_NO_GROUP_BY(
      "GROUP BY is not supported with a UDTF in the SELECT clause"), UDTF_NO_SORT_BY(
      "SORT BY is not supported with a UDTF in the SELECT clause"), UDTF_NO_CLUSTER_BY(
      "CLUSTER BY is not supported with a UDTF in the SELECT clause"), UDTF_NO_DISTRIBUTE_BY(
      "DISTRUBTE BY is not supported with a UDTF in the SELECT clause"), UDTF_INVALID_LOCATION(
      "UDTF's are not supported outside the SELECT clause, nor nested in expressions"), UDTF_LATERAL_VIEW(
      "UDTF's cannot be in a select expression when there is a lateral view"), UDTF_ALIAS_MISMATCH(
      "The number of aliases supplied in the AS clause does not match the number of columns output by the UDTF"), LATERAL_VIEW_WITH_JOIN(
      "Join with a lateral view is not supported"), LATERAL_VIEW_INVALID_CHILD(
      "Lateral view AST with invalid child"), OUTPUT_SPECIFIED_MULTIPLE_TIMES(
      "The same output cannot be present multiple times: "),

  LOCKMGR_NOT_SPECIFIED(
      "lock manager not specified correctly, set hive.lock.manager"), LOCKMGR_NOT_INITIALIZED(
      "lock manager could not be initialized, check hive.lock.manager "), LOCK_CANNOT_BE_ACQUIRED(
      "locks on the underlying objects cannot be acquired. retry after some time"), ZOOKEEPER_CLIENT_COULD_NOT_BE_INITIALIZED(
      "Check hive.zookeeper.quorum and hive.zookeeper.client.port"), INVALID_AS(
      "AS clause has an invalid number of aliases"), VIEW_COL_MISMATCH(
      "The number of columns produced by the SELECT clause does not match the number of column names specified by CREATE VIEW"), DML_AGAINST_VIEW(
      "A view cannot be used as target table for LOAD or INSERT"), OUTERJOIN_USES_FILTERS(
      "The query results could be wrong. "
          + "Turn on hive.outerjoin.supports.filters"), INVALID_FILEFORMAT(
      "db external table insert can not support the file format!"),

  SELECT_WITHNOFROM_INSUBQ("sub query must have a from clause!!"),

  PBFILE_PARTITION_NOTSUPPORT("pbfile cannot support partition now"), ;
  private String mesg;

  ErrorMsg(String mesg) {
    this.mesg = mesg;
  }

  private static int getLine(ASTNode tree) {
    if (tree.getChildCount() == 0) {
      return tree.getToken().getLine();
    }

    return getLine((ASTNode) tree.getChild(0));
  }

  private static int getCharPositionInLine(ASTNode tree) {
    if (tree.getChildCount() == 0) {
      return tree.getToken().getCharPositionInLine();
    }

    return getCharPositionInLine((ASTNode) tree.getChild(0));
  }

  private String getText(ASTNode tree) {
    if (tree.getChildCount() == 0) {
      return tree.getText();
    }

    return getText((ASTNode) tree.getChild(tree.getChildCount() - 1));
  }

  public String getMsg(ASTNode tree) {
    StringBuilder sb = new StringBuilder();
    renderPosition(sb, tree);
    sb.append(" ");
    sb.append(mesg);
    sb.append(" ");
    sb.append(getText(tree));
    renderOrigin(sb, tree.getOrigin());
    return sb.toString();
  }

  public static void renderOrigin(StringBuilder sb, ASTNodeOrigin origin) {
    while (origin != null) {
      sb.append(" in definition of ");
      sb.append(origin.getObjectType());
      sb.append(" ");
      sb.append(origin.getObjectName());
      sb.append(" [");
      sb.append(HiveUtils.LINE_SEP);
      sb.append(origin.getObjectDefinition());
      sb.append(HiveUtils.LINE_SEP);
      sb.append("] used as ");
      sb.append(origin.getUsageAlias());
      sb.append(" at ");
      ASTNode usageNode = origin.getUsageNode();
      renderPosition(sb, usageNode);
      origin = usageNode.getOrigin();
    }
  }

  private static void renderPosition(StringBuilder sb, ASTNode tree) {
    sb.append("line ");
    sb.append(getLine(tree));
    sb.append(":");
    sb.append(getCharPositionInLine(tree));
  }

  String getMsg(Tree tree) {
    return getMsg((ASTNode) tree);
  }

  String getMsg(ASTNode tree, String reason) {
    return getMsg(tree) + ": " + reason;
  }

  String getMsg(Tree tree, String reason) {
    return getMsg((ASTNode) tree, reason);
  }

  public String getMsg(String reason) {
    return mesg + " " + reason;
  }

  public String getMsg() {
    return mesg;
  }

}
