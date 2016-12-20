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

import java.util.HashMap;
import java.util.Vector;
import java.util.List;
import java.util.ArrayList;
import java.util.Map.Entry;

public class QBJoinTree {
  private String leftAlias;
  private String[] rightAliases;
  private String[] leftAliases;
  private QBJoinTree joinSrc;
  private String[] baseSrc;
  private int nextTag;
  private joinCond[] joinCond;
  private boolean noOuterJoin;
  private boolean noSemiJoin;

  private HashMap<String, ArrayList<ASTNode>> rhsSemijoin;

  private Vector<Vector<ASTNode>> expressions;

  private Vector<Vector<ASTNode>> filters;

  private ArrayList<ArrayList<ASTNode>> filtersForPushing;

  private boolean mapSideJoin;
  private List<String> mapAliases;

  private List<String> streamAliases;

  public QBJoinTree() {
    nextTag = 0;
    noOuterJoin = true;
    noSemiJoin = true;
    rhsSemijoin = new HashMap<String, ArrayList<ASTNode>>();
  }

  public String getLeftAlias() {
    return leftAlias;
  }

  public void setLeftAlias(String leftAlias) {
    this.leftAlias = leftAlias;
  }

  public String[] getRightAliases() {
    return rightAliases;
  }

  public void setRightAliases(String[] rightAliases) {
    this.rightAliases = rightAliases;
  }

  public String[] getLeftAliases() {
    return leftAliases;
  }

  public void setLeftAliases(String[] leftAliases) {
    this.leftAliases = leftAliases;
  }

  public Vector<Vector<ASTNode>> getExpressions() {
    return expressions;
  }

  public void setExpressions(Vector<Vector<ASTNode>> expressions) {
    this.expressions = expressions;
  }

  public String[] getBaseSrc() {
    return baseSrc;
  }

  public void setBaseSrc(String[] baseSrc) {
    this.baseSrc = baseSrc;
  }

  public QBJoinTree getJoinSrc() {
    return joinSrc;
  }

  public void setJoinSrc(QBJoinTree joinSrc) {
    this.joinSrc = joinSrc;
  }

  public int getNextTag() {
    return nextTag++;
  }

  public String getJoinStreamDesc() {
    return "$INTNAME";
  }

  public joinCond[] getJoinCond() {
    return joinCond;
  }

  public void setJoinCond(joinCond[] joinCond) {
    this.joinCond = joinCond;
  }

  public boolean getNoOuterJoin() {
    return noOuterJoin;
  }

  public void setNoOuterJoin(boolean noOuterJoin) {
    this.noOuterJoin = noOuterJoin;
  }

  public boolean getNoSemiJoin() {
    return noSemiJoin;
  }

  public void setNoSemiJoin(boolean semi) {
    this.noSemiJoin = semi;
  }

  public Vector<Vector<ASTNode>> getFilters() {
    return filters;
  }

  public void setFilters(Vector<Vector<ASTNode>> filters) {
    this.filters = filters;
  }

  public ArrayList<ArrayList<ASTNode>> getFiltersForPushing() {
    return filtersForPushing;
  }

  public void setFiltersForPushing(ArrayList<ArrayList<ASTNode>> filters) {
    this.filtersForPushing = filters;
  }

  public boolean isMapSideJoin() {
    return mapSideJoin;
  }

  public void setMapSideJoin(boolean mapSideJoin) {
    this.mapSideJoin = mapSideJoin;
  }

  public List<String> getMapAliases() {
    return mapAliases;
  }

  public void setMapAliases(List<String> mapAliases) {
    this.mapAliases = mapAliases;
  }

  public List<String> getStreamAliases() {
    return streamAliases;
  }

  public void setStreamAliases(List<String> streamAliases) {
    this.streamAliases = streamAliases;
  }

  public void addRHSSemijoin(String alias) {
    if (!rhsSemijoin.containsKey(alias)) {
      rhsSemijoin.put(alias, null);
    }
  }

  public void addRHSSemijoinColumns(String alias, ArrayList<ASTNode> columns) {
    ArrayList<ASTNode> cols = rhsSemijoin.get(alias);
    if (cols == null) {
      rhsSemijoin.put(alias, columns);
    } else {
      cols.addAll(columns);
    }
  }

  public void addRHSSemijoinColumns(String alias, ASTNode column) {
    ArrayList<ASTNode> cols = rhsSemijoin.get(alias);
    if (cols == null) {
      cols = new ArrayList<ASTNode>();
      cols.add(column);
      rhsSemijoin.put(alias, cols);
    } else {
      cols.add(column);
    }
  }

  public ArrayList<ASTNode> getRHSSemijoinColumns(String alias) {
    return rhsSemijoin.get(alias);
  }

  public void mergeRHSSemijoin(QBJoinTree src) {
    for (Entry<String, ArrayList<ASTNode>> e : src.rhsSemijoin.entrySet()) {
      String key = e.getKey();
      ArrayList<ASTNode> value = this.rhsSemijoin.get(key);
      if (value == null) {
        this.rhsSemijoin.put(key, e.getValue());
      } else {
        value.addAll(e.getValue());
      }
    }
  }
}
