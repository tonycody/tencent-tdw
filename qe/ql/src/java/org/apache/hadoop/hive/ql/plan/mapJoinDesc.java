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

import org.apache.hadoop.hive.ql.plan.exprNodeDesc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

@explain(displayName = "Common Join Operator")
public class mapJoinDesc extends joinDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  private Map<Byte, List<exprNodeDesc>> keys;
  private tableDesc keyTblDesc;
  private List<tableDesc> valueTblDescs;

  private int posBigTable;

  private Map<Byte, List<Integer>> retainList;

  private transient String bigTableAlias;

  private LinkedHashMap<String, LinkedHashMap<String, ArrayList<String>>> aliasHashPathNameMapping;

  public mapJoinDesc() {
  }

  public mapJoinDesc(final Map<Byte, List<exprNodeDesc>> keys,
      final tableDesc keyTblDesc, final Map<Byte, List<exprNodeDesc>> values,
      final List<tableDesc> valueTblDescs, ArrayList<String> outputColumnNames,
      final int posBigTable, final joinCond[] conds,
      final Map<Byte, List<exprNodeDesc>> filters, boolean noOuterJoin) {

    super(values, outputColumnNames, noOuterJoin, conds, filters);
    this.keys = keys;
    this.keyTblDesc = keyTblDesc;
    this.valueTblDescs = valueTblDescs;
    this.posBigTable = posBigTable;
    initRetainExprList();
  }

  private void initRetainExprList() {
    retainList = new HashMap<Byte, List<Integer>>();
    Set<Entry<Byte, List<exprNodeDesc>>> set = super.getExprs().entrySet();
    Iterator<Entry<Byte, List<exprNodeDesc>>> setIter = set.iterator();
    while (setIter.hasNext()) {
      Entry<Byte, List<exprNodeDesc>> current = setIter.next();
      List<Integer> list = new ArrayList<Integer>();
      for (int i = 0; i < current.getValue().size(); i++) {
        list.add(i);
      }
      retainList.put(current.getKey(), list);
    }
  }

  public Map<Byte, List<Integer>> getRetainList() {
    return retainList;
  }

  public void setRetainList(Map<Byte, List<Integer>> retainList) {
    this.retainList = retainList;
  }

  @explain(displayName = "keys")
  public Map<Byte, List<exprNodeDesc>> getKeys() {
    return keys;
  }

  public void setKeys(Map<Byte, List<exprNodeDesc>> keys) {
    this.keys = keys;
  }

  @explain(displayName = "Position of Big Table")
  public int getPosBigTable() {
    return posBigTable;
  }

  public void setPosBigTable(int posBigTable) {
    this.posBigTable = posBigTable;
  }

  public tableDesc getKeyTblDesc() {
    return keyTblDesc;
  }

  public void setKeyTblDesc(tableDesc keyTblDesc) {
    this.keyTblDesc = keyTblDesc;
  }

  public List<tableDesc> getValueTblDescs() {
    return valueTblDescs;
  }

  public void setValueTblDescs(List<tableDesc> valueTblDescs) {
    this.valueTblDescs = valueTblDescs;
  }

  public String getBigTableAlias() {
    return bigTableAlias;
  }

  public void setBigTableAlias(String bigTableAlias) {
    this.bigTableAlias = bigTableAlias;
  }

  public LinkedHashMap<String, LinkedHashMap<String, ArrayList<String>>> getAliasHashPathNameMapping() {
    return aliasHashPathNameMapping;
  }

  public void setAliasHashPathNameMapping(
      LinkedHashMap<String, LinkedHashMap<String, ArrayList<String>>> aliasHashParMapping) {
    this.aliasHashPathNameMapping = aliasHashParMapping;
  }
}
