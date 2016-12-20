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

package org.apache.hadoop.hive.jdbc;

import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.service.HiveInterface;
import org.apache.thrift.TException;

public class HiveDatabaseMetaData implements java.sql.DatabaseMetaData {

  private final HiveInterface client;
  private static final String CATALOG_SEPARATOR = ".";

  private static final int maxColumnNameLength = 128;
  private String dbName = null;

  public HiveDatabaseMetaData(HiveInterface client, String db) {
    this.client = client;
    dbName = db;
  }

  public boolean allProceduresAreCallable() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.allProceduresAreCallable() not supported");
  }

  public boolean allTablesAreSelectable() throws SQLException {
    return true;
  }

  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.autoCommitFailureClosesAllResultSets() not supported");
  }

  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.dataDefinitionCausesTransactionCommit() not supported");
  }

  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.dataDefinitionIgnoredInTransactions() not supported");
  }

  public boolean deletesAreDetected(int type) throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.deletesAreDetected(int type) not supported");
  }

  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.doesMaxRowSizeIncludeBlobs() not supported");
  }

  public ResultSet getAttributes(String catalog, String schemaPattern,
      String typeNamePattern, String attributeNamePattern) throws SQLException {
    throw new SQLException(
        "Method HiveDatabaseMetaData.getAttributes(String catalog, String schemaPattern,      String typeNamePattern, String attributeNamePattern) not supported");
  }

  public ResultSet getBestRowIdentifier(String catalog, String schema,
      String table, int scope, boolean nullable) throws SQLException {
    throw new SQLException(
        "Method HiveDatabaseMetaData.getBestRowIdentifier(String catalog, String schema,      String table, int scope, boolean nullable) not supported");
  }

  public String getCatalogSeparator() throws SQLException {
    return CATALOG_SEPARATOR;
  }

  public String getCatalogTerm() throws SQLException {
    return "database";
  }

  public ResultSet getCatalogs() throws SQLException {
    try {
      final List<String> catalogs = new ArrayList();
      catalogs.add(dbName);
      return new HiveMetaDataResultSet<String>(Arrays.asList("TABLE_CAT"),
          Arrays.asList("STRING"), catalogs) {
        private int cnt = 0;

        public boolean next() throws SQLException {
          if (cnt < data.size()) {
            List<Object> a = new ArrayList<Object>(1);
            a.add(data.get(cnt));
            row = a;
            cnt++;
            return true;
          } else {
            return false;
          }
        }
      };
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  public ResultSet getClientInfoProperties() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getClientInfoProperties() not supported");
  }

  public ResultSet getColumnPrivileges(String catalog, String schema,
      String table, String columnNamePattern) throws SQLException {
    throw new SQLException(
        "Method HiveDatabaseMetaData.getColumnPrivileges(String catalog, String schema,      String table, String columnNamePattern) not supported");
  }

  private String convertPattern(final String pattern) {
    if (pattern == null) {
      return ".*";
    } else {
      return pattern.replace("%", ".*").replace("_", ".");
    }
  }

  public ResultSet getColumns(String catalog, final String schemaPattern,
      final String tableNamePattern, final String columnNamePattern)
      throws SQLException {
    List<JdbcColumn> columns = new ArrayList<JdbcColumn>();
    try {
      if (catalog == null) {
        catalog = dbName;
      }

      String regtableNamePattern = convertPattern(tableNamePattern);
      String regcolumnNamePattern = convertPattern(columnNamePattern);

      List<String> tables = client.get_tables(catalog, "*");
      for (String table : tables) {
        if (table.matches(regtableNamePattern)) {
          List<FieldSchema> fields = client.get_fields_jdbc(catalog, table);
          List<FieldSchema> partFields = client.get_part_fields_jdbc(catalog,
              table);
          List<String> partKeys = new ArrayList<String>();
          if (partFields != null) {
            for (FieldSchema partField : partFields) {
              partKeys.add(partField.getName().toLowerCase());
            }
          }

          int ordinalPos = 1;
          for (FieldSchema field : fields) {
            if (field.getName().matches(regcolumnNamePattern)) {
              columns.add(new JdbcColumn(field.getName(), table, catalog, field
                  .getType(), field.getComment(), ordinalPos, partKeys
                  .contains(field.getName().toLowerCase())));
              ordinalPos++;
            }
          }
        }
      }

      Collections.sort(columns, new GetColumnsComparator());

      return new HiveMetaDataResultSet<JdbcColumn>(Arrays.asList("TABLE_CAT",
          "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME",
          "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX",
          "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE",
          "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION",
          "IS_NULLABLE", "SCOPE_CATLOG", "SCOPE_SCHEMA", "SCOPE_TABLE",
          "SOURCE_DATA_TYPE", "PART_KEY"), Arrays.asList("STRING", "STRING",
          "STRING", "STRING", "I32", "STRING", "I32", "I32", "I32", "I32",
          "I32", "STRING", "STRING", "I32", "I32", "I32", "I32", "STRING",
          "STRING", "STRING", "STRING", "I32", "BOOLEAN"), columns) {

        private int cnt = 0;

        public boolean next() throws SQLException {
          if (cnt < data.size()) {
            List<Object> a = new ArrayList<Object>(20);
            JdbcColumn column = data.get(cnt);
            a.add(column.getTableCatalog());
            a.add(null);
            a.add(column.getTableName());
            a.add(column.getColumnName());
            a.add(column.getSqlType());
            a.add(column.getType());
            a.add(column.getColumnSize());
            a.add(null);
            a.add(column.getDecimalDigits());
            a.add(column.getNumPrecRadix());
            a.add(DatabaseMetaData.columnNullable);
            a.add(column.getComment());
            a.add(null);
            a.add(null);
            a.add(null);
            a.add(null);
            a.add(column.getOrdinalPos());
            a.add("YES");
            a.add(null);
            a.add(null);
            a.add(null);
            a.add(null);
            a.add(column.isPart_key());
            row = a;
            cnt++;
            return true;
          } else {
            return false;
          }
        }
      };
    } catch (Exception e) {
      e.printStackTrace();
      throw new SQLException(e);
    }
  }

  private class GetColumnsComparator implements Comparator<JdbcColumn> {

    public int compare(JdbcColumn o1, JdbcColumn o2) {
      int compareName = o1.getTableName().compareTo(o2.getTableName());
      if (compareName == 0) {
        if (o1.getOrdinalPos() > o2.getOrdinalPos()) {
          return 1;
        } else if (o1.getOrdinalPos() < o2.getOrdinalPos()) {
          return -1;
        }
        return 0;
      } else {
        return compareName;
      }
    }
  }

  public Connection getConnection() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getConnection() not supported");
  }

  public ResultSet getCrossReference(String primaryCatalog,
      String primarySchema, String primaryTable, String foreignCatalog,
      String foreignSchema, String foreignTable) throws SQLException {
    throw new SQLException(
        "Method HiveDatabaseMetaData.getCrossReference() not supported");
  }

  public int getDatabaseMajorVersion() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getDatabaseMajorVersion() not supported");
  }

  public int getDatabaseMinorVersion() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getDatabaseMinorVersion() not supported");
  }

  public String getDatabaseProductName() throws SQLException {
    return "TDW";
  }

  public String getDatabaseProductVersion() throws SQLException {
    try {
      return client.getVersion();
    } catch (TException e) {
      throw new SQLException(e);
    }
  }

  public int getDefaultTransactionIsolation() throws SQLException {
    return Connection.TRANSACTION_NONE;
  }

  public int getDriverMajorVersion() {
    return 0;
  }

  public int getDriverMinorVersion() {
    return 0;
  }

  public String getDriverName() throws SQLException {
    return fetchManifestAttribute(Attributes.Name.IMPLEMENTATION_TITLE);
  }

  public String getDriverVersion() throws SQLException {
    return fetchManifestAttribute(Attributes.Name.IMPLEMENTATION_VERSION);
  }

  public ResultSet getExportedKeys(String catalog, String schema, String table)
      throws SQLException {
    throw new SQLException(
        "Method HiveDatabaseMetaData.getExportedKeys(String catalog, String schema, String table) not supported");
  }

  public String getExtraNameCharacters() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getExtraNameCharacters() not supported");
  }

  public ResultSet getFunctionColumns(String arg0, String arg1, String arg2,
      String arg3) throws SQLException {
    throw new SQLException(
        "Method HiveDatabaseMetaData.getFunctionColumns(String arg0, String arg1, String arg2,      String arg3) not supported");
  }

  public ResultSet getFunctions(String arg0, String arg1, String arg2)
      throws SQLException {
    throw new SQLException(
        "Method HiveDatabaseMetaData.getFunctions(String arg0, String arg1, String arg2) not supported");
  }

  public String getIdentifierQuoteString() throws SQLException {
    return "`";
  }

  public ResultSet getImportedKeys(String catalog, String schema, String table)
      throws SQLException {
    throw new SQLException(
        "Method HiveDatabaseMetaData.getImportedKeys(String catalog, String schema, String table) not supported");
  }

  public ResultSet getIndexInfo(String catalog, String schema, String table,
      boolean unique, boolean approximate) throws SQLException {
    throw new SQLException(
        "Method HiveDatabaseMetaData.getIndexInfo(String catalog, String schema, String table,      boolean unique, boolean approximate) not supported");
  }

  public int getJDBCMajorVersion() throws SQLException {
    return 3;
  }

  public int getJDBCMinorVersion() throws SQLException {
    return 0;
  }

  public int getMaxBinaryLiteralLength() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getMaxBinaryLiteralLength() not supported");
  }

  public int getMaxCatalogNameLength() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getMaxCatalogNameLength() not supported");
  }

  public int getMaxCharLiteralLength() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getMaxCharLiteralLength() not supported");
  }

  public int getMaxColumnNameLength() throws SQLException {
    return maxColumnNameLength;
  }

  public int getMaxColumnsInGroupBy() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getMaxColumnsInGroupBy() not supported");
  }

  public int getMaxColumnsInIndex() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getMaxColumnsInIndex() not supported");
  }

  public int getMaxColumnsInOrderBy() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getMaxColumnsInOrderBy() not supported");
  }

  public int getMaxColumnsInSelect() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getMaxColumnsInSelect() not supported");
  }

  public int getMaxColumnsInTable() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getMaxColumnsInTable() not supported");
  }

  public int getMaxConnections() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getMaxConnections() not supported");
  }

  public int getMaxCursorNameLength() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getMaxCursorNameLength() not supported");
  }

  public int getMaxIndexLength() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getMaxIndexLength() not supported");
  }

  public int getMaxProcedureNameLength() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getMaxProcedureNameLength() not supported");
  }

  public int getMaxRowSize() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getMaxRowSize() not supported");
  }

  public int getMaxSchemaNameLength() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getMaxSchemaNameLength() not supported");
  }

  public int getMaxStatementLength() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getMaxStatementLength() not supported");
  }

  public int getMaxStatements() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getMaxStatements() not supported");
  }

  public int getMaxTableNameLength() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getMaxTableNameLength() not supported");
  }

  public int getMaxTablesInSelect() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getMaxTablesInSelect() not supported");
  }

  public int getMaxUserNameLength() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getMaxUserNameLength() not supported");
  }

  public String getNumericFunctions() throws SQLException {
    return "";
  }

  public ResultSet getPrimaryKeys(String catalog, String schema, String table)
      throws SQLException {
    throw new SQLException(
        "Method HiveDatabaseMetaData.getPrimaryKeys(String catalog, String schema, String table) not supported");
  }

  public ResultSet getProcedureColumns(String catalog, String schemaPattern,
      String procedureNamePattern, String columnNamePattern)
      throws SQLException {
    throw new SQLException(
        "Method HiveDatabaseMetaData.getProcedureColumns(String catalog, String schemaPattern,      String procedureNamePattern, String columnNamePattern) not supported");
  }

  public String getProcedureTerm() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getProcedureTerm() not supported");
  }

  public ResultSet getProcedures(String catalog, String schemaPattern,
      String procedureNamePattern) throws SQLException {
    return null;
  }

  public int getResultSetHoldability() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getResultSetHoldability() not supported");
  }

  public RowIdLifetime getRowIdLifetime() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getRowIdLifetime() not supported");
  }

  public String getSQLKeywords() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getSQLKeywords() not supported");
  }

  public int getSQLStateType() throws SQLException {
    return DatabaseMetaData.sqlStateSQL99;
  }

  public String getSchemaTerm() throws SQLException {
    return "";
  }

  public ResultSet getSchemas() throws SQLException {
    return getSchemas(null, null);
  }

  public ResultSet getSchemas(String catalog, String schemaPattern)
      throws SQLException {
    return new HiveMetaDataResultSet(Arrays.asList("TABLE_SCHEM",
        "TABLE_CATALOG"), Arrays.asList("STRING", "STRING"), null) {

      public boolean next() throws SQLException {
        return false;
      }
    };

  }

  public String getSearchStringEscape() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getSearchStringEscape() not supported");
  }

  public String getStringFunctions() throws SQLException {
    return "";
  }

  public ResultSet getSuperTables(String catalog, String schemaPattern,
      String tableNamePattern) throws SQLException {
    throw new SQLException(
        "Method HiveDatabaseMetaData.getSuperTables(String catalog, String schemaPattern,      String tableNamePattern) not supported");
  }

  public ResultSet getSuperTypes(String catalog, String schemaPattern,
      String typeNamePattern) throws SQLException {
    throw new SQLException(
        "Method HiveDatabaseMetaData.getSuperTypes(String catalog, String schemaPattern,      String typeNamePattern) not supported");
  }

  public String getSystemFunctions() throws SQLException {
    return "";
  }

  public ResultSet getTablePrivileges(String catalog, String schemaPattern,
      String tableNamePattern) throws SQLException {
    throw new SQLException(
        "Method HiveDatabaseMetaData.getTablePrivileges(String catalog, String schemaPattern,      String tableNamePattern) not supported");
  }

  public ResultSet getTableTypes() throws SQLException {
    final TableType[] tt = TableType.values();
    ResultSet result = new HiveMetaDataResultSet<TableType>(
        Arrays.asList("TABLE_TYPE"), Arrays.asList("STRING"),
        new ArrayList<TableType>(Arrays.asList(tt))) {
      private int cnt = 0;

      public boolean next() throws SQLException {
        if (cnt < data.size()) {
          List<Object> a = new ArrayList<Object>(1);
          a.add(toJdbcTableType(data.get(cnt).name()));
          row = a;
          cnt++;
          return true;
        } else {
          return false;
        }
      }
    };
    return result;
  }

  public ResultSet getTables(String catalog, String schemaPattern,
      String tableNamePattern, String[] types) throws SQLException {
    final List<String> tablesstr;
    final List<JdbcTable> resultTables = new ArrayList();
    final String resultCatalog;
    if (catalog == null) {
      resultCatalog = dbName;
    } else {
      resultCatalog = catalog;
    }

    String regtableNamePattern = convertPattern(tableNamePattern);
    try {
      tablesstr = client.get_tables_jdbc(resultCatalog, "*");
      if (tablesstr == null || tablesstr.size() == 0) {
        return null;
      }
      for (String nameType : tablesstr) {
        String name[] = nameType.split(":");
        if (name.length != 2)
          continue;

        if (name[0].matches(regtableNamePattern))
          resultTables.add(new JdbcTable(resultCatalog, name[0], name[1], "",
              false));
      }

    } catch (Exception x) {
      throw new SQLException(x.getMessage());
    }

    Collections.sort(resultTables, new GetTablesComparator());

    ResultSet result = new HiveMetaDataResultSet<JdbcTable>(Arrays.asList(
        "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS",
        "EXTERNAL"), Arrays.asList("STRING", "STRING", "STRING", "STRING",
        "STRING", "BOOLEAN"), resultTables) {
      private int cnt = 0;

      public boolean next() throws SQLException {
        if (cnt < data.size()) {
          List<Object> a = new ArrayList<Object>(5);
          JdbcTable table = data.get(cnt);
          a.add(table.getTableCatalog());
          a.add(null);
          a.add(table.getTableName());
          try {
            a.add(table.getSqlTableType());
          } catch (Exception e) {
            throw new SQLException(e);
          }
          a.add("no remarks");
          String jdbcType = table.getSqlTableType();
          if (jdbcType == "EXTERNAL TABLE")
            a.add(true);
          else
            a.add(false);
          row = a;
          cnt++;
          return true;
        } else {
          return false;
        }
      }
    };
    return result;
  }

  private class GetTablesComparator implements Comparator<JdbcTable> {

    public int compare(JdbcTable o1, JdbcTable o2) {

      return o1.getTableName().compareTo(o2.getTableName());
    }
  }

  public static String toJdbcTableType(String hivetabletype) {
    if (hivetabletype == null) {
      return null;
    } else if (hivetabletype.equals(TableType.MANAGED_TABLE.toString())) {
      return "TABLE";
    } else if (hivetabletype.equals(TableType.VIRTUAL_VIEW.toString())) {
      return "VIEW";
    } else if (hivetabletype.equals(TableType.EXTERNAL_TABLE.toString())) {
      return "EXTERNAL TABLE";
    } else {
      return hivetabletype;
    }
  }

  public String getTimeDateFunctions() throws SQLException {
    return "";
  }

  public ResultSet getTypeInfo() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getTypeInfo() not supported");
  }

  public ResultSet getUDTs(String catalog, String schemaPattern,
      String typeNamePattern, int[] types) throws SQLException {

    return new HiveMetaDataResultSet(Arrays.asList("TYPE_CAT", "TYPE_SCHEM",
        "TYPE_NAME", "CLASS_NAME", "DATA_TYPE", "REMARKS", "BASE_TYPE"),
        Arrays.asList("STRING", "STRING", "STRING", "STRING", "I32", "STRING",
            "I32"), null) {

      public boolean next() throws SQLException {
        return false;
      }
    };
  }

  public String getURL() throws SQLException {
    throw new SQLException("Method HIveDatabaseMetaData.getURL() not supported");
  }

  public String getUserName() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.getUserName() not supported");
  }

  public ResultSet getVersionColumns(String catalog, String schema, String table)
      throws SQLException {
    throw new SQLException(
        "Method HiveDatabaseMetaData.getVersionColumns(String catalog, String schema, String table) not supported");
  }

  public boolean insertsAreDetected(int type) throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.insertsAreDetected(int type) not supported");
  }

  public boolean isCatalogAtStart() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.isCatalogAtStart() not supported");
  }

  public boolean isReadOnly() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.isReadOnly() not supported");
  }

  public boolean locatorsUpdateCopy() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.locatorsUpdateCopy() not supported");
  }

  public boolean nullPlusNonNullIsNull() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.nullPlusNonNullIsNull() not supported");
  }

  public boolean nullsAreSortedAtEnd() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.nullsAreSortedAtEnd() not supported");
  }

  public boolean nullsAreSortedAtStart() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.nullsAreSortedAtStart() not supported");
  }

  public boolean nullsAreSortedHigh() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.nullsAreSortedHigh() not supported");
  }

  public boolean nullsAreSortedLow() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.nullsAreSortedLow() not supported");
  }

  public boolean othersDeletesAreVisible(int type) throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.othersDeletesAreVisible(int type) not supported");
  }

  public boolean othersInsertsAreVisible(int type) throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.othersInsertsAreVisible(int type) not supported");
  }

  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.othersUpdatesAreVisible(int type) not supported");
  }

  public boolean ownDeletesAreVisible(int type) throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.ownDeletesAreVisible(int type) not supported");
  }

  public boolean ownInsertsAreVisible(int type) throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.ownInsertsAreVisible(int type) not supported");
  }

  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.ownUpdatesAreVisible(int type) not supported");
  }

  public boolean storesLowerCaseIdentifiers() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.storesLowerCaseIdentifiers() not supported");
  }

  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.storesLowerCaseQuotedIdentifiers() not supported");
  }

  public boolean storesMixedCaseIdentifiers() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.storesMixedCaseIdentifiers() not supported");
  }

  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.storesMixedCaseQuotedIdentifiers() not supported");
  }

  public boolean storesUpperCaseIdentifiers() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.storesUpperCaseIdentifiers() not supported");
  }

  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.storesUpperCaseQuotedIdentifiers() not supported");
  }

  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    throw new SQLException(
        "Method HiveDatabaseMetaData.supportsANSI92EntryLevelSQL() not supported");
  }

  public boolean supportsANSI92FullSQL() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsANSI92FullSQL() not supported");
  }

  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    throw new SQLException(
        "Method HiveDatabaseMetaData.supportsANSI92IntermediateSQL() not supported");
  }

  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    return true;
  }

  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    return false;
  }

  public boolean supportsBatchUpdates() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    return false;
  }

  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsColumnAliasing() throws SQLException {
    return true;
  }

  public boolean supportsConvert() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsConvert() not supported");
  }

  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsConvert(int fromType, int toType) not supported");
  }

  public boolean supportsCoreSQLGrammar() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsCoreSQLGrammar() not supported");
  }

  public boolean supportsCorrelatedSubqueries() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsCorrelatedSubqueries() not supported");
  }

  public boolean supportsDataDefinitionAndDataManipulationTransactions()
      throws SQLException {
    throw new SQLException(
        "Method HiveDatabaseMetaData.supportsDataDefinitionAndDataManipulationTransactions() not supported");
  }

  public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsDataManipulationTransactionsOnly() not supported");
  }

  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsDifferentTableCorrelationNames() not supported");
  }

  public boolean supportsExpressionsInOrderBy() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsExpressionsInOrderBy() not supported");
  }

  public boolean supportsExtendedSQLGrammar() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsExtendedSQLGrammar() not supported");
  }

  public boolean supportsFullOuterJoins() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsFullOuterJoins() not supported");
  }

  public boolean supportsGetGeneratedKeys() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsGetGeneratedKeys() not supported");
  }

  public boolean supportsGroupBy() throws SQLException {
    return true;
  }

  public boolean supportsGroupByBeyondSelect() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsGroupByBeyondSelect() not supported");
  }

  public boolean supportsGroupByUnrelated() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsGroupByUnrelated() not supported");
  }

  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsIntegrityEnhancementFacility() not supported");
  }

  public boolean supportsLikeEscapeClause() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsLikeEscapeClause() not supported");
  }

  public boolean supportsLimitedOuterJoins() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsLimitedOuterJoins() not supported");
  }

  public boolean supportsMinimumSQLGrammar() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsMinimumSQLGrammar() not supported");
  }

  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsMixedCaseIdentifiers() not supported");
  }

  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsMixedCaseQuotedIdentifiers() not supported");
  }

  public boolean supportsMultipleOpenResults() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsMultipleOpenResults() not supported");
  }

  public boolean supportsMultipleResultSets() throws SQLException {
    return false;
  }

  public boolean supportsMultipleTransactions() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsMultipleTransactions() not supported");
  }

  public boolean supportsNamedParameters() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsNamedParameters() not supported");
  }

  public boolean supportsNonNullableColumns() throws SQLException {
    return false;
  }

  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsOpenCursorsAcrossCommit() not supported");
  }

  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsOpenCursorsAcrossRollback() not supported");
  }

  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsOpenStatementsAcrossCommit() not supported");
  }

  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsOpenStatementsAcrossRollback() not supported");
  }

  public boolean supportsOrderByUnrelated() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsOrderByUnrelated() not supported");
  }

  public boolean supportsOuterJoins() throws SQLException {
    return true;
  }

  public boolean supportsPositionedDelete() throws SQLException {
    return false;
  }

  public boolean supportsPositionedUpdate() throws SQLException {
    return false;
  }

  public boolean supportsResultSetConcurrency(int type, int concurrency)
      throws SQLException {
    throw new SQLException(
        "Method HiveDatabaseMetaData.supportsResultSetConcurrency(int type, int concurrency) not supported");
  }

  public boolean supportsResultSetHoldability(int holdability)
      throws SQLException {
    return false;
  }

  public boolean supportsResultSetType(int type) throws SQLException {
    return true;
  }

  public boolean supportsSavepoints() throws SQLException {
    return false;
  }

  public boolean supportsSchemasInDataManipulation() throws SQLException {
    return false;
  }

  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    return false;
  }

  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    return false;
  }

  public boolean supportsSelectForUpdate() throws SQLException {
    return false;
  }

  public boolean supportsStatementPooling() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsStatementPooling() not supported");
  }

  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsStoredFunctionsUsingCallSyntax() not supported");
  }

  public boolean supportsStoredProcedures() throws SQLException {
    return false;
  }

  public boolean supportsSubqueriesInComparisons() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsSubqueriesInComparisons() not supported");
  }

  public boolean supportsSubqueriesInExists() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsSubqueriesInExists() not supported");
  }

  public boolean supportsSubqueriesInIns() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsSubqueriesInIns() not supported");
  }

  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsSubqueriesInQuantifieds() not supported");
  }

  public boolean supportsTableCorrelationNames() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsTableCorrelationNames() not supported");
  }

  public boolean supportsTransactionIsolationLevel(int level)
      throws SQLException {
    throw new SQLException(
        "Method HiveDatabaseMetaData.supportsTransactionIsolationLevel(int level) not supported");
  }

  public boolean supportsTransactions() throws SQLException {
    return false;
  }

  public boolean supportsUnion() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsUnion() not supported");
  }

  public boolean supportsUnionAll() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.supportsUnionAll() not supported");
  }

  public boolean updatesAreDetected(int type) throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.updatesAreDetected(int type) not supported");
  }

  public boolean usesLocalFilePerTable() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.usesLocalFilePerTable() not supported");
  }

  public boolean usesLocalFiles() throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.usesLocalFiles() not supported");
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.isWrapperFor(Class<?> iface) not supported");
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException(
        "Method HIveDatabaseMetaData.unwrap(Class<T> iface) not supported");
  }

  private static Attributes manifestAttributes = null;

  private synchronized void loadManifestAttributes() throws IOException {
    if (manifestAttributes != null) {
      return;
    }
    Class clazz = this.getClass();
    String classContainer = clazz.getProtectionDomain().getCodeSource()
        .getLocation().toString();
    URL manifestUrl = new URL("jar:" + classContainer
        + "!/META-INF/MANIFEST.MF");
    Manifest manifest = new Manifest(manifestUrl.openStream());
    manifestAttributes = manifest.getMainAttributes();
  }

  private String fetchManifestAttribute(Attributes.Name attributeName)
      throws SQLException {
    try {
      loadManifestAttributes();
    } catch (IOException e) {
      throw new SQLException("Couldn't load manifest attributes.", e);
    }
    return manifestAttributes.getValue(attributeName);
  }

  public static void main(String[] args) throws SQLException {
    HiveDatabaseMetaData meta = new HiveDatabaseMetaData(null, null);
    System.out.println("DriverName: " + meta.getDriverName());
    System.out.println("DriverVersion: " + meta.getDriverVersion());
  }
}
