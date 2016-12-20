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

package org.apache.hadoop.hive.metastore.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MPartition {

  private String dbName;
  private String tableName;
  private int level;
  private String parType;
  private MFieldSchema parKey;
  private List<String> partNames;
  private List<MPartSpace> parSpaces;

  public MPartition() {
  }

  public MPartition(String dbName, String tableName, int level, String parType,
      MFieldSchema parKey, Map<String, List<String>> parValues) {
    this.dbName = dbName;
    this.tableName = tableName;
    this.level = level;
    this.parType = parType;
    this.parKey = parKey;
    this.partNames = new ArrayList<String>();
    this.parSpaces = new ArrayList<MPartSpace>();
    convertToPartSpaces(parValues, partNames, parSpaces);
  }

  public int getLevel() {
    return level;
  }

  public void setLevel(int level) {
    this.level = level;
  }

  public String getParType() {
    return this.parType;
  }

  public void setParType(String parType) {
    this.parType = parType;
  }

  public MFieldSchema getParKey() {
    return parKey;
  }

  public void setParKey(MFieldSchema parKey) {
    this.parKey = parKey;
  }

  public Map<String, List<String>> getParSpaces() {
    return convertToPartValues(this.partNames, this.parSpaces);
  }

  public Map<String, MPartSpace> getPartSpaceMap() {
    Map<String, MPartSpace> map = new LinkedHashMap<String, MPartSpace>();
    for (int i = 0; i < partNames.size(); i++) {
      map.put(partNames.get(i), parSpaces.get(i));
    }
    return map;
  }

  public void setParSpaces(Map<String, List<String>> parValues) {
    if (this.parSpaces != null)
      parSpaces.clear();
    else
      parSpaces = new ArrayList<MPartSpace>();
    if (this.partNames != null)
      partNames.clear();
    else
      partNames = new ArrayList<String>();
    convertToPartSpaces(parValues, partNames, parSpaces);
  }

  public List<MPartSpace> setParSpacesForAlterPartition(
      Map<String, List<String>> parValues) {
    int newSize = parValues.size();
    int oldSize = partNames.size();
    int pos = 0;
    List<MPartSpace> needDeleteParSpaces = new ArrayList<MPartSpace>();

    if (newSize > oldSize) {
      for (Map.Entry<String, List<String>> entry : parValues.entrySet()) {
        if (!partNames.contains(entry.getKey())) {
          partNames.add(pos, entry.getKey());
          parSpaces.add(pos, new MPartSpace(entry.getValue()));
        }
        pos++;
      }
    }

    if (newSize < oldSize) {
      for (int i = 0; i < oldSize;) {
        if (!parValues.containsKey(partNames.get(i))) {
          partNames.remove(i);
          needDeleteParSpaces.add(parSpaces.remove(i));
          oldSize = partNames.size();
        } else {
          i++;
        }
      }
    }

    return needDeleteParSpaces;
  }

  public String getDBName() {
    return dbName;
  }

  public void setDBName(String dbName) {
    this.dbName = dbName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  static void convertToPartSpaces(Map<String, List<String>> partValues_,
      List<String> partNames_, List<MPartSpace> partSpaces_) {
    Map<String, MPartSpace> partSpace = new LinkedHashMap<String, MPartSpace>();
    for (Map.Entry<String, List<String>> entry : partValues_.entrySet()) {
      partNames_.add(entry.getKey());
      partSpaces_.add(new MPartSpace(entry.getValue()));
    }
  }

  static Map<String, List<String>> convertToPartValues(List<String> partNames_,
      List<MPartSpace> partSpaces_) {
    Map<String, List<String>> partValues = new LinkedHashMap<String, List<String>>();
    for (int i = 0; i < partNames_.size(); i++) {
      partValues.put(partNames_.get(i), partSpaces_.get(i).getValues());
    }
    return partValues;
  }

  public List<String> getPartNames() {
    return partNames;
  }

  public List<MPartSpace> getPartSpaceForAlter() {
    return parSpaces;
  }

  public void setPartNames(List<String> partNames) {
    this.partNames = partNames;
  }

  public void setParSpaces(List<MPartSpace> parSpaces) {
    this.parSpaces = parSpaces;
  }

}
