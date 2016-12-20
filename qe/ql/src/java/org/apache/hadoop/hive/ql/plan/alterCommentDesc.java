/**
* Tencent is pleased to support the open source community by making TDW available.
* Copyright (C) 2014 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use 
* this file except in compliance with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed 
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS 
* OF ANY KIND, either express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/
package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.Utilities;

@explain(displayName = "Alter Comment")
public class alterCommentDesc extends ddlDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  public static enum alterCommentTypes {
    TABLECOMMENT, VIEWCOMMENT, COLUMNCOMMENT
  };

  alterCommentTypes ct;
  String dbName = null;
  String tableName = null;
  String colName = null;
  String newComment = null;
  boolean setNull = false;

  public alterCommentDesc(String dbName, String tblName, String comment,
      boolean isView) {
    super();
    this.dbName = dbName;
    this.tableName = tblName;
    this.newComment = comment;
    if (!isView)
      this.ct = alterCommentTypes.TABLECOMMENT;
    else
      this.ct = alterCommentTypes.VIEWCOMMENT;
    this.setNull = (comment == null ? true : false);
  }

  public alterCommentDesc(String dbName, String tblName, String colName,
      String comment) {
    super();
    this.dbName = dbName;
    this.tableName = tblName;
    this.colName = colName;
    this.newComment = comment;
    this.ct = alterCommentTypes.COLUMNCOMMENT;
  }

  @explain(displayName = "db name")
  public String getDbName() {
    return this.dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  @explain(displayName = "table name")
  public String getTblName() {
    return this.tableName;
  }

  public void setTblName(String tblName) {
    this.tableName = tblName;
  }

  @explain(displayName = "comment")
  public String getColName() {
    return this.colName;
  }

  public void setColName(String colName) {
    this.colName = colName;
  }

  @explain(displayName = "new comment")
  public String getComment() {
    return newComment;
  }

  public void setComment(String comment) {
    this.newComment = comment;
  }

  public alterCommentTypes getCT() {
    return ct;
  }

  @explain(displayName = "comment type")
  public String getCommentTypeString() {
    switch (ct) {
    case TABLECOMMENT:
      return "alter table comment";
    case VIEWCOMMENT:
      return "alter view comment";
    case COLUMNCOMMENT:
      return "alter column comment";
    }

    return "unknown";
  }

  public void setCT(alterCommentTypes ct) {
    this.ct = ct;
  }
}
