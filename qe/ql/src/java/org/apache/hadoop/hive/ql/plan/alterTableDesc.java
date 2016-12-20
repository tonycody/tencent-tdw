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

package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.Utilities;

@explain(displayName = "Alter Table")
public class alterTableDesc extends ddlDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  public static enum alterTableTypes {
    RENAME, ADDCOLS, REPLACECOLS, ADDPROPS, ADDSERDE, ADDSERDEPROPS, ADDINDEX, DROPINDEX, RENAMECOLUMN
  };

  alterTableTypes op;
  String oldName;
  String newName;
  List<FieldSchema> newCols;
  String serdeName;
  Map<String, String> props;
  boolean expectView;

  String oldColName;
  String newColName;
  String newColType;
  String newColComment;
  boolean first;
  String afterCol;

  public alterTableDesc(String tblName, String oldColName, String newColName,
      String newType, String newComment, boolean first, String afterCol) {
    super();
    this.oldName = tblName;
    this.oldColName = oldColName;
    this.newColName = newColName;
    this.newColType = newType;
    this.newColComment = newComment;
    this.first = first;
    this.afterCol = afterCol;
    this.op = alterTableTypes.RENAMECOLUMN;
  }

  indexInfoDesc indexInfo;

  public alterTableDesc(String oldName, String newName) {
    op = alterTableTypes.RENAME;
    this.oldName = oldName;
    this.newName = newName;
  }

  public alterTableDesc(String name, List<FieldSchema> newCols,
      alterTableTypes alterType) {
    this.op = alterType;
    this.oldName = name;
    this.newCols = newCols;
  }

  public alterTableDesc(alterTableTypes alterType) {
    this(alterType, false);
  }

  public alterTableDesc(alterTableTypes alterType, boolean expectView) {
    op = alterType;
    this.expectView = expectView;
  }

  @explain(displayName = "old name")
  public String getOldName() {
    return oldName;
  }

  public void setOldName(String oldName) {
    this.oldName = oldName;
  }

  @explain(displayName = "new name")
  public String getNewName() {
    return newName;
  }

  public void setNewName(String newName) {
    this.newName = newName;
  }

  public alterTableTypes getOp() {
    return op;
  }

  @explain(displayName = "type")
  public String getAlterTableTypeString() {
    switch (op) {
    case RENAME:
      return "rename";
    case ADDCOLS:
      return "add columns";
    case REPLACECOLS:
      return "replace columns";
    }

    return "unknown";
  }

  public void setOp(alterTableTypes op) {
    this.op = op;
  }

  public List<FieldSchema> getNewCols() {
    return newCols;
  }

  @explain(displayName = "new columns")
  public List<String> getNewColsString() {
    return Utilities.getFieldSchemaString(getNewCols());
  }

  public void setNewCols(List<FieldSchema> newCols) {
    this.newCols = newCols;
  }

  @explain(displayName = "deserializer library")
  public String getSerdeName() {
    return serdeName;
  }

  public void setSerdeName(String serdeName) {
    this.serdeName = serdeName;
  }

  @explain(displayName = "properties")
  public Map<String, String> getProps() {
    return props;
  }

  public void setProps(Map<String, String> props) {
    this.props = props;
  }

  public indexInfoDesc getIndexInfo() {
    return this.indexInfo;
  }

  public void setIndexInfo(indexInfoDesc indexInfo) {
    this.indexInfo = indexInfo;
  }

  public String getOldColName() {
    return oldColName;
  }

  public void setOldColName(String oldColName) {
    this.oldColName = oldColName;
  }

  public String getNewColName() {
    return newColName;
  }

  public void setNewColName(String newColName) {
    this.newColName = newColName;
  }

  public String getNewColType() {
    return newColType;
  }

  public void setNewColType(String newType) {
    this.newColType = newType;
  }

  public String getNewColComment() {
    return newColComment;
  }

  public void setNewColComment(String newComment) {
    this.newColComment = newComment;
  }

  public boolean getFirst() {
    return first;
  }

  public void setFirst(boolean first) {
    this.first = first;
  }

  public String getAfterCol() {
    return afterCol;
  }

  public void setAfterCol(String afterCol) {
    this.afterCol = afterCol;
  }

}
