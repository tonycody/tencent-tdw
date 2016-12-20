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

import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@explain(displayName = "Join Operator")
public class joinDesc implements Serializable {
  private static final long serialVersionUID = 1L;
  public static final int INNER_JOIN = 0;
  public static final int LEFT_OUTER_JOIN = 1;
  public static final int RIGHT_OUTER_JOIN = 2;
  public static final int FULL_OUTER_JOIN = 3;
  public static final int UNIQUE_JOIN = 4;
  public static final int LEFT_SEMI_JOIN = 5;

  private boolean handleSkewJoin = false;
  private Map<Byte, String> bigKeysDirMap;
  private Map<Byte, Map<Byte, String>> smallKeysDirMap;
  private int skewKeyDefinition = -1;
  private tableDesc keyTableDesc;
  private Map<Byte, tableDesc> skewKeysValuesTables;

  private Map<Byte, List<exprNodeDesc>> exprs;

  private Map<Byte, List<exprNodeDesc>> filters;

  protected java.util.ArrayList<java.lang.String> outputColumnNames;

  transient private Map<String, Byte> reversedExprs;

  protected boolean noOuterJoin;

  protected joinCond[] conds;

  protected Byte[] tagOrder;

  public joinDesc() {
  }

  public joinDesc(final Map<Byte, List<exprNodeDesc>> exprs,
      ArrayList<String> outputColumnNames, final boolean noOuterJoin,
      final joinCond[] conds, final Map<Byte, List<exprNodeDesc>> filters) {

    this.exprs = exprs;
    this.outputColumnNames = outputColumnNames;
    this.noOuterJoin = noOuterJoin;
    this.conds = conds;

    this.filters = filters;

    tagOrder = new Byte[exprs.size()];
    for (int i = 0; i < tagOrder.length; i++) {
      tagOrder[i] = (byte) i;
    }

  }

  public joinDesc(final Map<Byte, List<exprNodeDesc>> exprs,
      ArrayList<String> outputColumnNames, final boolean noOuterJoin,
      final joinCond[] conds) {
    this(exprs, outputColumnNames, noOuterJoin, conds, null);
  }

  public joinDesc(final Map<Byte, List<exprNodeDesc>> exprs,
      ArrayList<String> outputColumnNames) {
    this(exprs, outputColumnNames, true, null);
  }

  public joinDesc(final Map<Byte, List<exprNodeDesc>> exprs,
      ArrayList<String> outputColumnNames, final joinCond[] conds) {
    this(exprs, outputColumnNames, true, conds, null);

  }

  public Map<Byte, List<exprNodeDesc>> getExprs() {
    return this.exprs;
  }

  public Map<String, Byte> getReversedExprs() {
    return reversedExprs;
  }

  public void setReversedExprs(Map<String, Byte> reversed_Exprs) {
    this.reversedExprs = reversed_Exprs;
  }

  @explain(displayName = "condition expressions")
  public Map<Byte, String> getExprsStringMap() {
    if (getExprs() == null) {
      return null;
    }

    LinkedHashMap<Byte, String> ret = new LinkedHashMap<Byte, String>();

    for (Map.Entry<Byte, List<exprNodeDesc>> ent : getExprs().entrySet()) {
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      if (ent.getValue() != null) {
        for (exprNodeDesc expr : ent.getValue()) {
          if (!first) {
            sb.append(" ");
          }

          first = false;
          sb.append("{");
          sb.append(expr.getExprString());
          sb.append("}");
        }
      }
      ret.put(ent.getKey(), sb.toString());
    }

    return ret;
  }

  public void setExprs(final Map<Byte, List<exprNodeDesc>> exprs) {
    this.exprs = exprs;
  }

  @explain(displayName = "filter predicates")
  public Map<Byte, String> getFiltersStringMap() {
    if (getFilters() == null || getFilters().size() == 0) {
      return null;
    }

    LinkedHashMap<Byte, String> ret = new LinkedHashMap<Byte, String>();
    boolean filtersPresent = false;

    for (Map.Entry<Byte, List<exprNodeDesc>> ent : getFilters().entrySet()) {
      StringBuilder sb = new StringBuilder();
      boolean first = true;
      if (ent.getValue() != null) {
        if (ent.getValue().size() != 0) {
          filtersPresent = true;
        }
        for (exprNodeDesc expr : ent.getValue()) {
          if (!first) {
            sb.append(" ");
          }

          first = false;
          sb.append("{");
          sb.append(expr.getExprString());
          sb.append("}");
        }
      }
      ret.put(ent.getKey(), sb.toString());
    }

    if (filtersPresent) {
      return ret;
    } else {
      return null;
    }
  }

  public Map<Byte, List<exprNodeDesc>> getFilters() {
    return filters;
  }

  public void setFilters(Map<Byte, List<exprNodeDesc>> filters) {
    this.filters = filters;
  }

  @explain(displayName = "outputColumnNames")
  public java.util.ArrayList<java.lang.String> getOutputColumnNames() {
    return outputColumnNames;
  }

  public void setOutputColumnNames(
      java.util.ArrayList<java.lang.String> outputColumnNames) {
    this.outputColumnNames = outputColumnNames;
  }

  public boolean getNoOuterJoin() {
    return this.noOuterJoin;
  }

  public void setNoOuterJoin(final boolean noOuterJoin) {
    this.noOuterJoin = noOuterJoin;
  }

  @explain(displayName = "condition map")
  public List<joinCond> getCondsList() {
    if (conds == null) {
      return null;
    }

    ArrayList<joinCond> l = new ArrayList<joinCond>();
    for (joinCond cond : conds) {
      l.add(cond);
    }

    return l;
  }

  public joinCond[] getConds() {
    return this.conds;
  }

  public void setConds(final joinCond[] conds) {
    this.conds = conds;
  }

  public Byte[] getTagOrder() {
    return tagOrder;
  }

  public void setTagOrder(Byte[] tagOrder) {
    this.tagOrder = tagOrder;
  }

  public boolean isNoOuterJoin() {
    for (org.apache.hadoop.hive.ql.plan.joinCond cond : conds) {
      if (cond.getType() == joinDesc.FULL_OUTER_JOIN
          || (cond.getType() == joinDesc.LEFT_OUTER_JOIN)
          || cond.getType() == joinDesc.RIGHT_OUTER_JOIN)
        return false;
    }
    return true;
  }

  public void setKeyTableDesc(tableDesc keyTblDesc) {
    this.keyTableDesc = keyTblDesc;
  }

  public tableDesc getKeyTableDesc() {
    return keyTableDesc;
  }

  public void setHandleSkewJoin(boolean handleSkewJoin2) {
    this.handleSkewJoin = handleSkewJoin2;
  }

  @explain(displayName = "handleSkewJoin")
  public boolean getHandleSkewJoin() {
    return handleSkewJoin;
  }

  public Map<Byte, String> getBigKeysDirMap() {
    return bigKeysDirMap;
  }

  public void setBigKeysDirMap(Map<Byte, String> bigKeysDirMap) {
    this.bigKeysDirMap = bigKeysDirMap;
  }

  public Map<Byte, Map<Byte, String>> getSmallKeysDirMap() {
    return smallKeysDirMap;
  }

  public void setSmallKeysDirMap(Map<Byte, Map<Byte, String>> smallKeysDirMap) {
    this.smallKeysDirMap = smallKeysDirMap;
  }

  public Map<Byte, tableDesc> getSkewKeysValuesTables() {
    return skewKeysValuesTables;
  }

  public void setSkewKeysValuesTables(Map<Byte, tableDesc> skewKeysValuesTables) {
    this.skewKeysValuesTables = skewKeysValuesTables;
  }

  public int getSkewKeyDefinition() {
    return skewKeyDefinition;
  }

  public void setSkewKeyDefinition(int skewKeyDefinition) {
    this.skewKeyDefinition = skewKeyDefinition;
  }

}
