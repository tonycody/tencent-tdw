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

public class MultiExistsTree {

  private boolean isSelectDI = false;
  private ArrayList<Boolean> isNots = new ArrayList<Boolean>();
  private ArrayList<Boolean> isJoinSources = new ArrayList<Boolean>();
  private boolean outerJoinSource = false;
  private int existsCount = 0;

  private ASTNode outerFrom = null;
  private String outerSource;

  private ASTNode outerDestination = null;
  private ArrayList<ASTNode> outerSelects = new ArrayList<ASTNode>();

  private ArrayList<ASTNode> otherOuterWhere = new ArrayList<ASTNode>();

  private ArrayList<ASTNode> innerFroms = new ArrayList<ASTNode>();
  private ArrayList<String> innerSources = new ArrayList<String>();

  private ArrayList<ArrayList<ASTNode>> innerSelects = new ArrayList<ArrayList<ASTNode>>();

  private ArrayList<ASTNode> existsConditions = new ArrayList<ASTNode>();

  private ArrayList<ASTNode> innerGroupBys = new ArrayList<ASTNode>();
  private ArrayList<ArrayList<ASTNode>> otherInnerInsertNodes = new ArrayList<ArrayList<ASTNode>>();
  private ArrayList<ArrayList<ASTNode>> otherInnerNodes = new ArrayList<ArrayList<ASTNode>>();

  private ASTNode outerGroupBy = null;
  private ArrayList<ASTNode> otherInsertNodes = new ArrayList<ASTNode>();
  private ArrayList<ASTNode> otherNodes = new ArrayList<ASTNode>();

  boolean getIsSelectDI() {
    return this.isSelectDI;
  }

  void setIsSelectDI(boolean isSelectDI) {
    this.isSelectDI = isSelectDI;
  }

  ArrayList<Boolean> getIsNotExists() {
    return isNots;
  }

  void setOuterJoinSource(boolean outerJoinSource) {
    this.outerJoinSource = outerJoinSource;
  }

  boolean getOuterJoinSource() {
    return this.outerJoinSource;
  }

  void setIsJoinSource(boolean isJoinSource) {
    this.isJoinSources.add(isJoinSource);
  }

  ArrayList<Boolean> getIsJoinSources() {
    return this.isJoinSources;
  }

  void setNotExists(boolean isNot) {
    this.existsCount++;
    this.isNots.add(isNot);
  }

  int getExistsCount() {
    return this.existsCount;
  }

  ASTNode getOuterFrom() {
    return this.outerFrom;
  }

  void setOuterFrom(ASTNode outerFrom) {
    this.outerFrom = outerFrom;
  }

  String getOuterSource() {
    return this.outerSource;
  }

  void setOuterSource(String outerSource) {
    this.outerSource = outerSource;
  }

  ASTNode getOuterDestination() {
    return this.outerDestination;
  }

  void setOuterDestination(ASTNode outerDestination) {
    this.outerDestination = outerDestination;
  }

  ArrayList<ASTNode> getOuterSelects() {
    return this.outerSelects;
  }

  void setOuterSelect(ASTNode outerSelect) {
    this.outerSelects.add(outerSelect);
  }

  ArrayList<ASTNode> getOtherOuterWhere() {
    return this.otherOuterWhere;
  }

  void setOtherOuterWhere(ASTNode otherOuterWhere) {
    this.otherOuterWhere.add(otherOuterWhere);
  }

  ArrayList<ASTNode> getInnerFroms() {
    return this.innerFroms;
  }

  void setInnerFrom(ASTNode innerFrom) {
    this.innerFroms.add(innerFrom);
  }

  ArrayList<String> getInnerSources() {
    return this.innerSources;
  }

  void setInnerSource(String innerSource) {
    this.innerSources.add(innerSource);
  }

  ArrayList<ArrayList<ASTNode>> getInnerSelects() {
    return this.innerSelects;
  }

  void setInnerSelect(ArrayList<ASTNode> innerSelect) {
    this.innerSelects.add(innerSelect);
  }

  ArrayList<ASTNode> getExistsCondition() {
    return this.existsConditions;
  }

  void setExistsCondition(ASTNode existsCondition) {
    this.existsConditions.add(existsCondition);
  }

  ArrayList<ASTNode> getInnerGroupBys() {
    return this.innerGroupBys;
  }

  void setInnerGroupBy(ASTNode innerGroupBy) {
    this.innerGroupBys.add(innerGroupBy);
  }

  ASTNode getOuterGroupBy() {
    return this.outerGroupBy;
  }

  void setOuterGroupBy(ASTNode outerGroupBy) {
    this.outerGroupBy = outerGroupBy;
  }

  ArrayList<ASTNode> getOtherInsertNodes() {
    return this.otherInsertNodes;
  }

  void setOtherInsertNodes(ASTNode node) {
    this.otherInsertNodes.add(node);
  }

  ArrayList<ASTNode> getOtherNodes() {
    return this.otherNodes;
  }

  void setOtherNodes(ASTNode node) {
    this.otherNodes.add(node);
  }

}
