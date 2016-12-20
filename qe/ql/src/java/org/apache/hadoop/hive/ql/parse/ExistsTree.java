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

public class ExistsTree {

  private boolean isNot = false;

  private ASTNode outerFrom = null;
  private String outerSource;

  private ASTNode outerDestination = null;
  private ArrayList<ASTNode> outerSelects = new ArrayList<ASTNode>();

  private ArrayList<ASTNode> otherOuterWhere = new ArrayList<ASTNode>();

  private ASTNode innerFrom = null;
  private String innerSource;

  private ASTNode innerDestination = null;
  private ArrayList<ASTNode> innerSelects = new ArrayList<ASTNode>();

  private ASTNode existsCondition = null;
  private ArrayList<ASTNode> otherInnerWhere = new ArrayList<ASTNode>();

  private ASTNode innerGroupBy = null;
  private ArrayList<ASTNode> otherInnerInsertNodes = new ArrayList<ASTNode>();
  private ArrayList<ASTNode> otherInnerNodes = new ArrayList<ASTNode>();

  private ASTNode outerGroupBy = null;
  private ArrayList<ASTNode> otherInsertNodes = new ArrayList<ASTNode>();
  private ArrayList<ASTNode> otherNodes = new ArrayList<ASTNode>();

  boolean isNotExists() {
    return isNot;
  }

  void setNotExists(boolean isNot) {
    this.isNot = isNot;
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

  ASTNode getInnerFrom() {
    return this.innerFrom;
  }

  void setInnerFrom(ASTNode innerFrom) {
    this.innerFrom = innerFrom;
  }

  String getInnerSource() {
    return this.innerSource;
  }

  void setInnerSource(String innerSource) {
    this.innerSource = innerSource;
  }

  ASTNode getInnerDestination() {
    return this.innerDestination;
  }

  void setInnerDestination(ASTNode InnerDestination) {
    this.innerDestination = InnerDestination;
  }

  ArrayList<ASTNode> getInnerSelects() {
    return this.innerSelects;
  }

  void setInnerSelect(ASTNode innerSelect) {
    this.innerSelects.add(innerSelect);
  }

  ASTNode getExistsCondition() {
    return this.existsCondition;
  }

  void setExistsCondition(ASTNode existsCondition) {
    this.existsCondition = existsCondition;
  }

  ArrayList<ASTNode> getOtherInnerWhere() {
    return this.otherInnerWhere;
  }

  void setOtherInnerWhere(ASTNode otherInnerWhere) {
    this.otherInnerWhere.add(otherInnerWhere);
  }

  ASTNode getInnerGroupBy() {
    return this.innerGroupBy;
  }

  void setInnerGroupBy(ASTNode innerGroupBy) {
    this.innerGroupBy = innerGroupBy;
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
