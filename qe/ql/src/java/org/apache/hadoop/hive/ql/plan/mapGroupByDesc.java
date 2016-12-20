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

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.plan.groupByDesc.Mode;

@explain(displayName = "Map Group By Operator")
public class mapGroupByDesc implements java.io.Serializable {

  private static final long serialVersionUID = 1L;

  private Mode mode;
  private boolean groupKeyNotReductionKey;
  private boolean isDistinctTagged;
  private boolean merge;

  private ArrayList<fieldsList> keys;
  private ArrayList<nameList> keyNames;
  private ArrayList<aggParamsList> aggregators;
  private ArrayList<nameList> outputColumnNames;
  private ArrayList<idxList> outputMapping;

  public mapGroupByDesc() {
  }

  public mapGroupByDesc(final Mode mode,
      final java.util.ArrayList<nameList> outputColumnNames,
      final java.util.ArrayList<fieldsList> keys,
      final java.util.ArrayList<nameList> keyNames,
      final java.util.ArrayList<aggParamsList> aggregators,
      final java.util.ArrayList<idxList> outputMapping,
      final boolean groupKeyNotReductionKey, final boolean isDistinctTagged,
      final boolean merge) {
    this.mode = mode;
    this.outputColumnNames = outputColumnNames;
    this.keys = keys;
    this.keyNames = keyNames;
    this.aggregators = aggregators;
    this.outputMapping = outputMapping;
    this.groupKeyNotReductionKey = groupKeyNotReductionKey;
    this.isDistinctTagged = isDistinctTagged;
    this.merge = merge;
  }

  public Mode getMode() {
    return this.mode;
  }

  @explain(displayName = "mode")
  public String getModeString() {
    switch (mode) {
    case COMPLETE:
      return "complete";
    case PARTIAL1:
      return "partial1";
    case PARTIAL2:
      return "partial2";
    case PARTIALS:
      return "partials";
    case HASH:
      return "hash";
    case FINAL:
      return "final";
    case MERGEPARTIAL:
      return "mergepartial";
    }

    return "unknown";
  }

  public void setMode(final Mode mode) {
    this.mode = mode;
  }

  @explain(displayName = "keys")
  public java.util.ArrayList<fieldsList> getKeys() {
    return this.keys;
  }

  public void setKeys(final java.util.ArrayList<fieldsList> keys) {
    this.keys = keys;
  }

  public ArrayList<nameList> getKeyNames() {
    return keyNames;
  }

  public void setKeyNames(final java.util.ArrayList<nameList> keyNames) {
    this.keyNames = keyNames;
  }

  @explain(displayName = "outputColumnNames")
  public java.util.ArrayList<nameList> getOutputColumnNames() {
    return outputColumnNames;
  }

  public void setOutputColumnNames(
      java.util.ArrayList<nameList> outputColumnNames) {
    this.outputColumnNames = outputColumnNames;
  }

  @explain(displayName = "aggregations")
  public java.util.ArrayList<aggParamsList> getAggregators() {
    return this.aggregators;
  }

  public void setAggregators(
      final java.util.ArrayList<aggParamsList> aggregators) {
    this.aggregators = aggregators;
  }

  @explain(displayName = "column mapping")
  public ArrayList<idxList> getOutputMapping() {
    return this.outputMapping;
  }

  public void setOutputMapping(final ArrayList<idxList> outputMapping) {
    this.outputMapping = outputMapping;
  }

  public boolean getGroupKeyNotReductionKey() {
    return this.groupKeyNotReductionKey;
  }

  public void setGroupKeyNotReductionKey(final boolean groupKeyNotReductionKey) {
    this.groupKeyNotReductionKey = groupKeyNotReductionKey;
  }

  @explain(displayName = "Tagged")
  public boolean getIsTagged() {
    return isDistinctTagged;
  }

  public void setIsTagged(boolean isTagged) {
    this.isDistinctTagged = isTagged;
  }

  public boolean getMerge() {
    return merge;
  }

  public void setMerge(boolean merge) {
    this.merge = merge;
  }

}
