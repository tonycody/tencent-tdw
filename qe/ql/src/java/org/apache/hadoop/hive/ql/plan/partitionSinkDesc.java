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

import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.InsertPartDesc;

@explain(displayName = "Partition Output Operator")
public class partitionSinkDesc extends fileSinkDesc {

  private static final long serialVersionUID = 1L;
  private ArrayList<exprNodeDesc> partKeys;
  private ArrayList<String> partTypeInfos;
  private ArrayList<String> partTypes;
  private ArrayList<PartSpaceSpec> partSpaces;
  transient private ArrayList<RangePartitionExprTree> exprTrees;

  private InsertPartDesc insertPartDesc = null;

  public partitionSinkDesc() {
  }

  public partitionSinkDesc(final String dirName, final tableDesc table,
      final boolean compressed, final int destTableId,
      final ArrayList<java.lang.String> partTypes,
      final ArrayList<String> partTypeInfos,
      final ArrayList<exprNodeDesc> partKeys,
      final ArrayList<PartSpaceSpec> partSpaces,
      final ArrayList<RangePartitionExprTree> exprTrees,
      final InsertPartDesc insertPartDesc) {
    super(dirName, table, compressed, destTableId);
    this.partTypes = partTypes;
    this.partTypeInfos = partTypeInfos;
    this.partKeys = partKeys;
    this.partSpaces = partSpaces;
    this.partTypeInfos = partTypeInfos;
    //this.exprTrees = exprTrees;
    this.exprTrees = new ArrayList<RangePartitionExprTree>();
    this.insertPartDesc = insertPartDesc;
  }

  public partitionSinkDesc(final String dirName, final tableDesc table,
      final boolean compressed, final int destTableId,
      final ArrayList<java.lang.String> partTypes,
      final ArrayList<String> partTypeInfos,
      final ArrayList<exprNodeDesc> partKeys,
      final ArrayList<PartSpaceSpec> partSpaces,
      final ArrayList<RangePartitionExprTree> exprTrees) {
    super(dirName, table, compressed, destTableId);
    this.partTypes = partTypes;
    this.partTypeInfos = partTypeInfos;
    this.partKeys = partKeys;
    this.partSpaces = partSpaces;
    this.partTypeInfos = partTypeInfos;
    //this.exprTrees = exprTrees;
    this.exprTrees = new ArrayList<RangePartitionExprTree>();
  }

  @explain(displayName = "insert partition desc")
  public InsertPartDesc getInsertPartDesc() {
    return this.insertPartDesc;
  }

  public void setInsertPartDesc(final InsertPartDesc desc) {
    this.insertPartDesc = desc;
  }

  @explain(displayName = "partition types")
  public ArrayList<String> getPartTypes() {
    return this.partTypes;
  }

  public void setPartTypes(final ArrayList<String> partTypes) {
    this.partTypes = partTypes;
  }

  @explain(displayName = "partition values")
  public ArrayList<PartSpaceSpec> getPartSpaces() {
    return this.partSpaces;
  }

  public void setPartSpaces(final ArrayList<PartSpaceSpec> partSpaces) {
    this.partSpaces = partSpaces;
  }

  @explain(displayName = "partition field typeinfos")
  public ArrayList<String> getPartTypeInfos() {
    return this.partTypeInfos;
  }

  public void setPartTypeInfos(ArrayList<String> partTypeInfos) {
    this.partTypeInfos = partTypeInfos;
  }

  @explain(displayName = "partition keys")
  public ArrayList<exprNodeDesc> getPartKeys() {
    return this.partKeys;
  }

  public void setPartKeys(ArrayList<exprNodeDesc> partKeys) {
    this.partKeys = partKeys;
  }

  public ArrayList<RangePartitionExprTree> getExprTrees() {
    return this.exprTrees;
  }

  public void setExprTrees(ArrayList<RangePartitionExprTree> exprTrees) {
    this.exprTrees = exprTrees;
  }

  @Override
  protected partitionSinkDesc clone() throws CloneNotSupportedException {
    return (partitionSinkDesc) super.clone();
  }

}
