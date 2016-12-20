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

import java.util.*;
import org.apache.hadoop.hive.ql.metadata.*;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.InsertPartDesc;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.tableSpec;
import org.apache.hadoop.hive.ql.parse.QB.PartRefType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class QBMetaData {

  public static final int DEST_INVALID = 0;
  public static final int DEST_TABLE = 1;
  public static final int DEST_PARTITION = 2;
  public static final int DEST_DFS_FILE = 3;
  public static final int DEST_REDUCE = 4;
  public static final int DEST_LOCAL_FILE = 5;

  private HashMap<String, TablePartition> aliasToTable;
  private HashMap<String, Table> nameToDestTable;
  private HashMap<String, Partition> nameToDestPartition;
  private HashMap<String, String> nameToDestFile;
  private HashMap<String, Integer> nameToDestType;
  private HashMap<String, Boolean> nameToTmpDir;
  private HashMap<String, ArrayList<String>> insertColumnsToDestTable;
  private HashMap<String, Boolean> nameToDestOverwrite;

  private HashMap<String, InsertPartDesc> nameToDestPartitionDesc;

  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(QBMetaData.class.getName());

  public QBMetaData() {
    this.aliasToTable = new HashMap<String, TablePartition>();
    this.nameToDestTable = new HashMap<String, Table>();
    this.nameToDestPartition = new HashMap<String, Partition>();
    this.nameToDestFile = new HashMap<String, String>();
    this.nameToDestType = new HashMap<String, Integer>();
    this.nameToDestOverwrite = new HashMap<String, Boolean>();
    this.nameToTmpDir = new HashMap<String, Boolean>();
    this.insertColumnsToDestTable = new HashMap<String, ArrayList<String>>();

    this.nameToDestPartitionDesc = new HashMap<String, InsertPartDesc>();
  }

  public HashMap<String, TablePartition> getAliasToTable() {
    return aliasToTable;
  }

  public TablePartition getTableForAlias(String alias) {
    return this.aliasToTable.get(alias.toLowerCase());
  }

  public void setSrcForAlias(String alias, TablePartition tabp) {
    this.aliasToTable.put(alias, tabp);
  }

  public void setDestForAlias(String alias, Table tab, boolean overwrite) {
    this.nameToDestType.put(alias, Integer.valueOf(DEST_TABLE));
    this.nameToDestTable.put(alias, tab);
    this.nameToDestOverwrite.put(alias, overwrite);
  }

  public void setDestForAlias(String alias, Table tab, String partName,
      String subPartName, boolean overwrite) {
    if (partName != null) {
      this.nameToDestType.put(alias, Integer.valueOf(DEST_PARTITION));
    } else {
      this.nameToDestType.put(alias, Integer.valueOf(DEST_TABLE));
    }

    this.nameToDestTable.put(alias, tab);
    this.nameToDestOverwrite.put(alias, overwrite);
  }

  public void setDestForAlias(String alias, tableSpec ts, boolean overwrite) {
    Table tab = ts.tableHandle;
    InsertPartDesc insertPartDesc = ts.getInsertPartDesc();
    if (insertPartDesc != null) {
      boolean isInsertPart = insertPartDesc.isInsertPart();
      if (isInsertPart) {
        this.nameToDestPartitionDesc.put(alias, insertPartDesc);
        this.nameToDestType.put(alias, Integer.valueOf(DEST_PARTITION));
      } else {
        this.nameToDestType.put(alias, Integer.valueOf(DEST_TABLE));
      }

    } else {
      this.nameToDestType.put(alias, Integer.valueOf(DEST_TABLE));
    }

    this.nameToDestTable.put(alias, tab);
    this.nameToDestOverwrite.put(alias, overwrite);
  }

  public void setDestForAlias(String alias, Partition part, boolean overwrite) {
    this.nameToDestType.put(alias, Integer.valueOf(DEST_PARTITION));
    this.nameToDestPartition.put(alias, part);
    this.nameToDestOverwrite.put(alias, overwrite);
  }

  public void setDestForAlias(String alias, String fname, boolean isDfsFile,
      boolean overwrite) {
    this.nameToDestType.put(alias, isDfsFile ? Integer.valueOf(DEST_DFS_FILE)
        : Integer.valueOf(DEST_LOCAL_FILE));
    this.nameToDestFile.put(alias, fname);
    this.nameToDestOverwrite.put(alias, overwrite);
  }

  public void setDestForTmpdir(String alias, boolean isTmp) {
    this.nameToTmpDir.put(alias, isTmp);
  }

  public boolean getDestForTmpdir(String alias) {
    return this.nameToTmpDir.get(alias.toLowerCase());
  }

  public HashMap<String, String> getNameToDestFile() {
    return nameToDestFile;
  }

  public Boolean isDestOverwriteForAlias(String alias) {
    return this.nameToDestOverwrite.get(alias.toLowerCase());
  }

  public Integer getDestTypeForAlias(String alias) {
    return this.nameToDestType.get(alias.toLowerCase());
  }

  public Table getDestTableForAlias(String alias) {
    return this.nameToDestTable.get(alias.toLowerCase());
  }

  public InsertPartDesc getInsertPartDesc(String alias) {
    return this.nameToDestPartitionDesc.get(alias);
  }

  public Partition getDestPartitionForAlias(String alias) {
    return this.nameToDestPartition.get(alias.toLowerCase());
  }

  public String getDestFileForAlias(String alias) {
    return this.nameToDestFile.get(alias.toLowerCase());
  }

  public TablePartition getSrcForAlias(String alias) {
    return this.aliasToTable.get(alias.toLowerCase());
  }

  public HashMap<String, ArrayList<String>> getInsertColumnsToDestTable() {
    return insertColumnsToDestTable;
  }

  public void addInsertColumnsToDestTable(String desttable,
      ArrayList<String> columns) {
    this.insertColumnsToDestTable.put(desttable, columns);
  }

}
