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

package org.apache.hadoop.hive.ql.exec;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.mapJoinDesc;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectKey;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinObjectValue;
import org.apache.hadoop.hive.ql.exec.persistence.HashMapWrapper;
import org.apache.hadoop.hive.ql.exec.persistence.RowContainer;
import org.apache.hadoop.hive.ql.exec.persistence.SortedKeyValueList;

public class MapJoinOperator extends CommonJoinOperator<mapJoinDesc> implements
    Serializable {
  private static final long serialVersionUID = 1L;
  static final private Log LOG = LogFactory.getLog(MapJoinOperator.class
      .getName());

  transient protected Map<Byte, List<ExprNodeEvaluator>> joinKeys;

  transient protected Map<Byte, List<ObjectInspector>> joinKeysObjectInspectors;

  transient protected Map<Byte, List<ObjectInspector>> joinKeysStandardObjectInspectors;

  transient private int posBigTable;
  transient int mapJoinRowsKey;

  transient protected Map<Byte, HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue>> mapJoinTables;

  transient protected RowContainer<ArrayList<Object>> emptyList = null;

  transient protected Map<Byte, SortedKeyValueList<ArrayList<Object>, MapJoinObjectValue>> mapJoinLists;

  transient private boolean isSortedMergeJoin = false;
  transient private int rowBucketSize;
  transient private boolean firsttime = true;

  public static class MapJoinObjectCtx {
    ObjectInspector standardOI;
    SerDe serde;
    tableDesc tblDesc;
    Configuration conf;

    public MapJoinObjectCtx(ObjectInspector standardOI, SerDe serde) {
      this(standardOI, serde, null, null);
    }

    public MapJoinObjectCtx(ObjectInspector standardOI, SerDe serde,
        tableDesc tblDesc, Configuration conf) {
      this.standardOI = standardOI;
      this.serde = serde;
      this.tblDesc = tblDesc;
      this.conf = conf;
    }

    public ObjectInspector getStandardOI() {
      return standardOI;
    }

    public SerDe getSerDe() {
      return serde;
    }

    public tableDesc getTblDesc() {
      return tblDesc;
    }

    public Configuration getConf() {
      return conf;
    }

  }

  transient static Map<Integer, MapJoinObjectCtx> mapMetadata = new HashMap<Integer, MapJoinObjectCtx>();
  transient static int nextVal = 0;

  static public Map<Integer, MapJoinObjectCtx> getMapMetadata() {
    return mapMetadata;
  }

  transient boolean firstRow;

  public static int metadataKeyTag;
  transient int[] metadataValueTag;
  transient List<File> hTables;
  transient int numMapRowsRead;
  transient int heartbeatInterval;

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    super.initializeOp(hconf);
    numMapRowsRead = 0;

    this.isSortedMergeJoin = hconf.getBoolean("Sorted.Merge.Map.Join", false);

    firstRow = true;

    heartbeatInterval = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVESENDHEARTBEAT);

    joinKeys = new HashMap<Byte, List<ExprNodeEvaluator>>();

    populateJoinKeyValue(joinKeys, conf.getKeys());
    joinKeysObjectInspectors = getObjectInspectorsFromEvaluators(joinKeys,
        inputObjInspectors);
    joinKeysStandardObjectInspectors = getStandardObjectInspectors(joinKeysObjectInspectors);

    posBigTable = conf.getPosBigTable();

    metadataValueTag = new int[numAliases];
    for (int pos = 0; pos < numAliases; pos++)
      metadataValueTag[pos] = -1;

    mapJoinTables = new HashMap<Byte, HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue>>();
    hTables = new ArrayList<File>();

    isSortedMergeJoin = hconf.getBoolean("Sorted.Merge.Map.Join", false);
    if (isSortedMergeJoin) {
      mapJoinLists = new HashMap<Byte, SortedKeyValueList<ArrayList<Object>, MapJoinObjectValue>>();

      rowBucketSize = HiveConf.getIntVar(hconf,
          HiveConf.ConfVars.HIVEMAPJOINBUCKETCACHESIZE);
      for (int pos = 0; pos < numAliases; pos++) {
        if (pos == posBigTable)
          continue;

        int listSize = HiveConf.getIntVar(hconf,
            HiveConf.ConfVars.HIVEMAPJOINCACHEROWS);
        SortedKeyValueList<ArrayList<Object>, MapJoinObjectValue> kvList = new SortedKeyValueList<ArrayList<Object>, MapJoinObjectValue>(
            listSize);
        mapJoinLists.put(Byte.valueOf((byte) pos), kvList);
      }
    }

    for (int pos = 0; pos < numAliases; pos++) {
      if (pos == posBigTable)
        continue;

      int cacheSize = HiveConf.getIntVar(hconf,
          HiveConf.ConfVars.HIVEMAPJOINCACHEROWS);
      HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue> hashTable = new HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue>(
          cacheSize,hconf);

      mapJoinTables.put(Byte.valueOf((byte) pos), hashTable);
    }

    emptyList = new RowContainer<ArrayList<Object>>(1, hconf);
    RowContainer bigPosRC = getRowContainer(hconf, (byte) posBigTable,
        order[posBigTable], joinCacheSize);
    storage.put((byte) posBigTable, bigPosRC);

    mapJoinRowsKey = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVEMAPJOINROWSIZE);

    List<? extends StructField> structFields = ((StructObjectInspector) outputObjInspector)
        .getAllStructFieldRefs();
    if (conf.getOutputColumnNames().size() < structFields.size()) {
      List<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
      for (Byte alias : order) {
        int sz = conf.getExprs().get(alias).size();
        List<Integer> retained = conf.getRetainList().get(alias);
        for (int i = 0; i < sz; i++) {
          int pos = retained.get(i);
          structFieldObjectInspectors.add(structFields.get(pos)
              .getFieldObjectInspector());
        }

      }

      outputObjInspector = ObjectInspectorFactory
          .getStandardStructObjectInspector(conf.getOutputColumnNames(),
              structFieldObjectInspectors);
    }
    initializeChildren(hconf);
  }

  @Override
  public void process(Object row, int tag) throws HiveException {

    if (this.isSortedMergeJoin) {
      try {
        alias = (byte) tag;

        if ((lastAlias == null) || (!lastAlias.equals(alias)))
          nextSz = joinEmitInterval;

        ArrayList<Object> key = computeKeys(row, joinKeys.get(alias),
            joinKeysObjectInspectors.get(alias));

        if (noOuterJoin || tag != posBigTable) {
          boolean isNull = false;
          for (Object keyo : key) {
            if (keyo == null)
              isNull = true;
          }
          if (isNull) {
            return;
          }
        }

        ArrayList<Object> value = computeValues(row, joinValues.get(alias),
            joinValuesObjectInspectors.get(alias), joinFilters.get(alias),
            joinFilterObjectInspectors.get(alias), noOuterJoin);

        row = null;

        if (tag != posBigTable) {
          if (firstRow) {
            metadataKeyTag = nextVal++;

            tableDesc keyTableDesc = conf.getKeyTblDesc();
            SerDe keySerializer = (SerDe) ReflectionUtils.newInstance(
                keyTableDesc.getDeserializerClass(), null);
            keySerializer.initialize(null, keyTableDesc.getProperties());

            mapMetadata.put(
                Integer.valueOf(metadataKeyTag),
                new MapJoinObjectCtx(ObjectInspectorUtils
                    .getStandardObjectInspector(
                        keySerializer.getObjectInspector(),
                        ObjectInspectorCopyOption.WRITABLE), keySerializer,
                    keyTableDesc, hconf));
            firstRow = false;
          }

          numMapRowsRead++;
          if (((numMapRowsRead % heartbeatInterval) == 0) && (reporter != null))
            reporter.progress();

          SortedKeyValueList<ArrayList<Object>, MapJoinObjectValue> kvList = mapJoinLists
              .get(alias);
          RowContainer res = null;
          boolean isNewKey = !kvList.equalLastPutKey(key);

          if (isNewKey) {
            res = new RowContainer(rowBucketSize, hconf);
            res.add(value);
          } else {
            kvList.saveToLastPutValue(value);
          }

          if (metadataValueTag[tag] == -1) {
            metadataValueTag[tag] = nextVal++;

            tableDesc valueTableDesc = conf.getValueTblDescs().get(tag);
            SerDe valueSerDe = (SerDe) ReflectionUtils.newInstance(
                valueTableDesc.getDeserializerClass(), null);
            valueSerDe.initialize(null, valueTableDesc.getProperties());

            mapMetadata.put(
                Integer.valueOf(metadataValueTag[tag]),
                new MapJoinObjectCtx(ObjectInspectorUtils
                    .getStandardObjectInspector(
                        valueSerDe.getObjectInspector(),
                        ObjectInspectorCopyOption.WRITABLE), valueSerDe,
                    valueTableDesc, hconf));
          }

          if (isNewKey) {
            MapJoinObjectValue valueObj = new MapJoinObjectValue(
                metadataValueTag[tag], res);
            valueObj.setConf(hconf);

            kvList.put(key, valueObj);
            if (numMapRowsRead % 100000 == 0)
              LOG.info("Used memory "
                  + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime()
                      .freeMemory()) + " for " + numMapRowsRead + " rows!");
          }
          return;
        }

        if (firsttime) {
          for (Byte pos : order) {
            if (pos.intValue() != tag) {
              SortedKeyValueList<ArrayList<Object>, MapJoinObjectValue> kvList = mapJoinLists
                  .get(pos);

              kvList.closePFile();
            }
          }

          firsttime = false;
        }

        storage.get(alias).add(value);
        for (Byte pos : order) {
          if (pos.intValue() != tag) {
            MapJoinObjectValue o = (MapJoinObjectValue) mapJoinLists.get(pos)
                .get(key);

            if (o == null) {
              storage.put(pos, dummyObjVectors[pos.intValue()]);
            } else {
              storage.put(pos, o.getObj());
            }
          }
        }

        checkAndGenObject();

        storage.get(alias).clear();

        for (Byte pos : order)
          if (pos.intValue() != tag)
            storage.put(pos, null);

      } catch (SerDeException e) {
        e.printStackTrace();
        throw new HiveException(e);
      }

    } else {
      try {
        alias = (byte) tag;

        if ((lastAlias == null) || (!lastAlias.equals(alias)))
          nextSz = joinEmitInterval;

        ArrayList<Object> key = computeKeys(row, joinKeys.get(alias),
            joinKeysObjectInspectors.get(alias));

        if (noOuterJoin || tag != posBigTable) {
          boolean isNull = false;
          for (Object keyo : key) {
            if (keyo == null)
              isNull = true;
          }
          if (isNull) {
            return;
          }
        }

        ArrayList<Object> value = computeValues(row, joinValues.get(alias),
            joinValuesObjectInspectors.get(alias), joinFilters.get(alias),
            joinFilterObjectInspectors.get(alias), noOuterJoin);

        row = null;

        if (tag != posBigTable) {
          if (firstRow) {
            metadataKeyTag = nextVal++;

            tableDesc keyTableDesc = conf.getKeyTblDesc();
            SerDe keySerializer = (SerDe) ReflectionUtils.newInstance(
                keyTableDesc.getDeserializerClass(), null);
            keySerializer.initialize(null, keyTableDesc.getProperties());

            mapMetadata.put(
                Integer.valueOf(metadataKeyTag),
                new MapJoinObjectCtx(ObjectInspectorUtils
                    .getStandardObjectInspector(
                        keySerializer.getObjectInspector(),
                        ObjectInspectorCopyOption.WRITABLE), keySerializer,
                    keyTableDesc, hconf));
            firstRow = false;
          }

          numMapRowsRead++;
          if (((numMapRowsRead % heartbeatInterval) == 0) && (reporter != null))
            reporter.progress();

          HashMapWrapper<MapJoinObjectKey, MapJoinObjectValue> hashTable = mapJoinTables
              .get(alias);
          MapJoinObjectKey keyMap = new MapJoinObjectKey(metadataKeyTag, key);
          MapJoinObjectValue o = hashTable.get(keyMap);
          RowContainer res = null;

          boolean needNewKey = true;
          if (o == null) {
            int bucketSize = HiveConf.getIntVar(hconf,
                HiveConf.ConfVars.HIVEMAPJOINBUCKETCACHESIZE);
            res = getRowContainer(this.hconf, (byte) tag, order[tag],
                bucketSize);
            res.add(value);
          } else {
            res = o.getObj();
            res.add(value);
            if (hashTable.cacheSize() > 0) {
              o.setObj(res);
              needNewKey = false;
            }
          }

          if (metadataValueTag[tag] == -1) {
            metadataValueTag[tag] = nextVal++;

            tableDesc valueTableDesc = conf.getValueTblDescs().get(tag);
            SerDe valueSerDe = (SerDe) ReflectionUtils.newInstance(
                valueTableDesc.getDeserializerClass(), null);
            valueSerDe.initialize(null, valueTableDesc.getProperties());

            mapMetadata.put(
                Integer.valueOf(metadataValueTag[tag]),

                new MapJoinObjectCtx(ObjectInspectorUtils
                    .getStandardObjectInspector(
                        valueSerDe.getObjectInspector(),
                        ObjectInspectorCopyOption.WRITABLE), valueSerDe,
                    valueTableDesc, hconf));
          }

          if (needNewKey) {
            MapJoinObjectKey keyObj = new MapJoinObjectKey(metadataKeyTag, key);
            MapJoinObjectValue valueObj = new MapJoinObjectValue(
                metadataValueTag[tag], res);
            valueObj.setConf(hconf);

            if (res.size() > mapJoinRowsKey) {
              if (res.size() % 100 == 0) {
                LOG.warn("Number of values for a given key " + keyObj + " are "
                    + res.size());
                LOG.warn("used memory " + Runtime.getRuntime().totalMemory());
              }
            }
            hashTable.put(keyObj, valueObj);
            if (numMapRowsRead % 100000 == 0)
              LOG.info("used memory "
                  + (Runtime.getRuntime().totalMemory() - Runtime.getRuntime()
                      .freeMemory()) + " for " + numMapRowsRead + " rows!");
          }

          return;
        }

        storage.get(alias).add(value);
        for (Byte pos : order) {
          if (pos.intValue() != tag) {
            MapJoinObjectKey keyMap = new MapJoinObjectKey(metadataKeyTag, key);
            MapJoinObjectValue o = (MapJoinObjectValue) mapJoinTables.get(pos)
                .get(keyMap);

            if (o == null) {
              if (noOuterJoin) {
                storage.put(pos, emptyList);
              } else {
                storage.put(pos, dummyObjVectors[pos.intValue()]);
              }
            } else {
              storage.put(pos, o.getObj());
            }
          }
        }

        checkAndGenObject();

        storage.get(alias).clear();

        for (Byte pos : order)
          if (pos.intValue() != tag)
            storage.put(pos, null);

      } catch (SerDeException e) {
        e.printStackTrace();
        throw new HiveException(e);
      }
    }
  }

  public void closeOp(boolean abort) throws HiveException {
    for (HashMapWrapper hashTable : mapJoinTables.values()) {
      hashTable.close();
    }

    super.closeOp(abort);
  }

  public String getName() {
    return "MAPJOIN";
  }

}
