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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class RowResolver {

  private RowSchema rowSchema;
  private HashMap<String, LinkedHashMap<String, ColumnInfo>> rslvMap;
  private HashMap<String, ArrayList<ColumnInfo>> rslvListMap;

  private HashMap<String, String[]> invRslvMap;
  private Map<String, ASTNode> expressionMap;

  private boolean isExprResolver;

  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(RowResolver.class.getName());

  public RowResolver() {
    rowSchema = new RowSchema();
    rslvMap = new HashMap<String, LinkedHashMap<String, ColumnInfo>>();
    rslvListMap = new HashMap<String, ArrayList<ColumnInfo>>();
    invRslvMap = new HashMap<String, String[]>();
    expressionMap = new HashMap<String, ASTNode>();
    isExprResolver = false;
  }

  public void putExpression(ASTNode node, ColumnInfo colInfo) {
    String treeAsString = node.toStringTree();
    expressionMap.put(treeAsString, node);
    put("", treeAsString, colInfo);
  }

  public void putExpression(ASTNode node, ColumnInfo colInfo,
      String treeStringPrefix) {
    String treeAsString = node.toStringTree();
    expressionMap.put(treeAsString, node);
    put("", treeStringPrefix + treeAsString, colInfo);
  }

  public ColumnInfo getExpression(ASTNode node) throws SemanticException {
    return get("", node.toStringTree());
  }

  public ASTNode getExpressionSource(ASTNode node) {
    return expressionMap.get(node.toStringTree());
  }

  public void put(String tab_alias, String col_alias, ColumnInfo colInfo) {
    if (tab_alias != null) {
      tab_alias = tab_alias.toLowerCase();
    }
    col_alias = col_alias.toLowerCase();
    if (rowSchema.getSignature() == null) {
      rowSchema.setSignature(new Vector<ColumnInfo>());
    }

    rowSchema.getSignature().add(colInfo);

    LinkedHashMap<String, ColumnInfo> f_map = rslvMap.get(tab_alias);
    if (f_map == null) {
      f_map = new LinkedHashMap<String, ColumnInfo>();
      rslvMap.put(tab_alias, f_map);
    }
    f_map.put(col_alias, colInfo);

    ArrayList<ColumnInfo> f_list = rslvListMap.get(tab_alias);
    if (f_list == null) {
      f_list = new ArrayList<ColumnInfo>();
      rslvListMap.put(tab_alias, f_list);
    }
    f_list.add(colInfo);

    String[] qualifiedAlias = new String[2];
    qualifiedAlias[0] = tab_alias;
    qualifiedAlias[1] = col_alias;
    invRslvMap.put(colInfo.getInternalName(), qualifiedAlias);
  }

  public Set<String> getRslvNameSet() {
    return rslvListMap.keySet();
  }

  public boolean hasTableAlias(String tab_alias) {
    if (tab_alias != null)
      tab_alias = tab_alias.toLowerCase();
    return rslvMap.get(tab_alias) != null;
  }

  public ColumnInfo get(String tab_alias, String col_alias)
      throws SemanticException {
    col_alias = col_alias.toLowerCase();
    ColumnInfo ret = null;

    if (tab_alias != null) {
      tab_alias = tab_alias.toLowerCase();
      HashMap<String, ColumnInfo> f_map = rslvMap.get(tab_alias);
      if (f_map == null) {
        return null;
      }
      ret = f_map.get(col_alias);
    } else {
      boolean found = false;
      for (LinkedHashMap<String, ColumnInfo> cmap : rslvMap.values()) {
        for (Map.Entry<String, ColumnInfo> cmapEnt : cmap.entrySet()) {
          if (col_alias.equalsIgnoreCase((String) cmapEnt.getKey())) {
            if (found) {
              throw new SemanticException("Column " + col_alias
                  + " Found in more than One Tables/Subqueries");
            }
            found = true;
            ret = (ColumnInfo) cmapEnt.getValue();
          }
        }
      }
    }

    return ret;
  }

  public Vector<ColumnInfo> getColumnInfos() {
    return rowSchema.getSignature();
  }

  public HashMap<String, ColumnInfo> getFieldMap(String tab_alias) {
    if (tab_alias == null) {
      return rslvMap.get(null);
    } else {
      return rslvMap.get(tab_alias.toLowerCase());
    }
  }

  public ArrayList<ColumnInfo> getFieldList(String tab_alias) {
    return rslvListMap.get(tab_alias.toLowerCase());
  }

  public int getPosition(String internalName) {
    int pos = -1;

    for (ColumnInfo var : rowSchema.getSignature()) {
      ++pos;
      if (var.getInternalName().equals(internalName)) {
        return pos;
      }
    }

    return -1;
  }

  public Set<String> getTableNames() {
    return rslvMap.keySet();
  }

  public String[] reverseLookup(String internalName) {
    return invRslvMap.get(internalName);
  }

  public void setIsExprResolver(boolean isExprResolver) {
    this.isExprResolver = isExprResolver;
  }

  public boolean getIsExprResolver() {
    return isExprResolver;
  }

  public String toString() {
    StringBuffer sb = new StringBuffer();

    for (Map.Entry<String, LinkedHashMap<String, ColumnInfo>> e : rslvMap
        .entrySet()) {
      String tab = (String) e.getKey();
      sb.append(tab + "{");
      HashMap<String, ColumnInfo> f_map = (HashMap<String, ColumnInfo>) e
          .getValue();
      if (f_map != null)
        for (Map.Entry<String, ColumnInfo> entry : f_map.entrySet()) {
          sb.append("(" + (String) entry.getKey() + ","
              + entry.getValue().toString() + ")");
        }
      sb.append("} ");
    }
    return sb.toString();
  }

  public boolean checkCols() {
    Vector<String> vvv = new Vector<String>();
    vvv.clear();
    for (Map.Entry<String, LinkedHashMap<String, ColumnInfo>> e : rslvMap
        .entrySet()) {
      HashMap<String, ColumnInfo> f_map = (HashMap<String, ColumnInfo>) e
          .getValue();
      if (f_map != null) {

        Iterator<Map.Entry<String, ColumnInfo>> it = f_map.entrySet()
            .iterator();
        for (; it.hasNext();) {
          Map.Entry<String, ColumnInfo> entry = it.next();
          String tmpstr = (String) entry.getKey();
          if (vvv.contains(tmpstr)
              && (!tmpstr.equalsIgnoreCase("qq_num_dc_ta_5473084_TYUFGHXCV"))) {
            LOG.info("wrong col: " + tmpstr);
            return false;
          } else if (vvv.contains(tmpstr)
              && tmpstr.equalsIgnoreCase("qq_num_dc_ta_5473084_TYUFGHXCV")) {

          } else {
            vvv.add(tmpstr);
          }
        }

      }
    }
    return true;
  }

  public HashMap<String, LinkedHashMap<String, ColumnInfo>> rslvMap() {
    return rslvMap;
  }

  public void setExpressionMap(Map<String, ASTNode> expressionMap) {
    this.expressionMap = expressionMap;
  }

  public Map<String, ASTNode> getExpressionMap() {
    return expressionMap;
  }

}
