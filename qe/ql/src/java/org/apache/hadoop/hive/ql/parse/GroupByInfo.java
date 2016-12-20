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
package org.apache.hadoop.hive.ql.parse;

import java.util.ArrayList;

public class GroupByInfo {

  private ASTNode from = null;

  private ASTNode destination = null;

  private ArrayList<ASTNode> select = new ArrayList<ASTNode>();

  private ASTNode where = null;

  private ArrayList<ASTNode> groupBy = new ArrayList<ASTNode>();

  private ASTNode having = null;

  private ArrayList<ASTNode> otherInsertNodes = new ArrayList<ASTNode>();

  private ArrayList<String> inRollupAndCube = new ArrayList<String>();

  public void setFrom(ASTNode from) {
    this.from = from;
  }

  public ASTNode getFrom() {
    return this.from;
  }

  public void setDestination(ASTNode destination) {
    this.destination = destination;
  }

  public ASTNode getDestination() {
    return this.destination;
  }

  public void setSelect(ASTNode selectNode) {
    this.select.add(selectNode);
  }

  public ArrayList<ASTNode> getSelect() {
    return this.select;
  }

  public void setWhere(ASTNode where) {
    this.where = where;
  }

  public ASTNode getWhere() {
    return this.where;
  }

  public void setGroupBy(ASTNode groupBy) {
    this.groupBy.add(groupBy);
  }

  public ArrayList<ASTNode> getGroupBy() {
    return this.groupBy;
  }

  public void setHaving(ASTNode having) {
    this.having = having;
  }

  public ASTNode getHaving() {
    return this.having;
  }

  public void setOtherInsertNodes(ASTNode insertNode) {
    this.otherInsertNodes.add(insertNode);
  }

  public ArrayList<ASTNode> getOtherInsertNodes() {
    return this.otherInsertNodes;
  }

  public void setInRollupAndCube(String inRAC) {
    inRollupAndCube.add(inRAC);
  }

  public ArrayList<String> getInRollupAndCube() {
    return inRollupAndCube;
  }

}
