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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.joinCond;
import org.apache.hadoop.hive.ql.plan.joinDesc;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.ql.exec.JoinOperator.Counter;
import org.apache.hadoop.hive.ql.exec.persistence.RowContainer;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public abstract class CommonJoinOperator<T extends joinDesc> extends
    Operator<T> implements Serializable {
  private static final long serialVersionUID = 1L;
  static final protected Log LOG = LogFactory.getLog(CommonJoinOperator.class
      .getName());

  public static class IntermediateObject {
    ArrayList<Object>[] objs;
    int curSize;

    public IntermediateObject(ArrayList<Object>[] objs, int curSize) {
      this.objs = objs;
      this.curSize = curSize;
    }

    public ArrayList<Object>[] getObjs() {
      return objs;
    }

    public int getCurSize() {
      return curSize;
    }

    public void pushObj(ArrayList<Object> newObj) {
      objs[curSize++] = newObj;
    }

    public void popObj() {
      curSize--;
    }

    public Object topObj() {
      return objs[curSize - 1];
    }

  }

  transient protected int numAliases;

  transient protected Map<Byte, List<ExprNodeEvaluator>> joinValues;

  protected transient Map<Byte, List<ExprNodeEvaluator>> joinFilters;

  transient protected Map<Byte, List<ObjectInspector>> joinValuesObjectInspectors;

  protected transient Map<Byte, List<ObjectInspector>> joinFilterObjectInspectors;

  transient protected Map<Byte, List<ObjectInspector>> joinValuesStandardObjectInspectors;

  protected transient Map<Byte, List<ObjectInspector>> rowContainerStandardObjectInspectors;

  transient static protected Byte[] order;
  transient protected joinCond[] condn;
  transient protected boolean noOuterJoin;
  transient private Object[] dummyObj;
  transient protected RowContainer<ArrayList<Object>>[] dummyObjVectors;
  transient protected int totalSz;

  transient private Map<Integer, Set<String>> posToAliasMap;

  transient LazyBinarySerDe[] spillTableSerDe;
  transient protected Map<Byte, tableDesc> spillTableDesc;

  HashMap<Byte, RowContainer<ArrayList<Object>>> storage;
  int joinEmitInterval = -1;
  int joinCacheSize = 0;
  int nextSz = 0;
  transient Byte lastAlias = null;

  transient boolean handleSkewJoin = false;

  protected int populateJoinKeyValue(Map<Byte, List<ExprNodeEvaluator>> outMap,
      Map<Byte, List<exprNodeDesc>> inputMap) {

    int total = 0;

    Iterator<Map.Entry<Byte, List<exprNodeDesc>>> entryIter = inputMap
        .entrySet().iterator();
    while (entryIter.hasNext()) {
      Map.Entry<Byte, List<exprNodeDesc>> e = (Map.Entry<Byte, List<exprNodeDesc>>) entryIter
          .next();
      Byte key = order[e.getKey()];

      List<exprNodeDesc> expr = (List<exprNodeDesc>) e.getValue();
      int sz = expr.size();
      total += sz;

      List<ExprNodeEvaluator> valueFields = new ArrayList<ExprNodeEvaluator>();

      for (int j = 0; j < sz; j++)
        valueFields.add(ExprNodeEvaluatorFactory.get(expr.get(j)));

      outMap.put(key, valueFields);
    }

    return total;
  }

  protected static HashMap<Byte, List<ObjectInspector>> getObjectInspectorsFromEvaluators(
      Map<Byte, List<ExprNodeEvaluator>> exprEntries,
      ObjectInspector[] inputObjInspector) throws HiveException {
    HashMap<Byte, List<ObjectInspector>> result = new HashMap<Byte, List<ObjectInspector>>();
    for (Entry<Byte, List<ExprNodeEvaluator>> exprEntry : exprEntries
        .entrySet()) {
      Byte alias = exprEntry.getKey();
      List<ExprNodeEvaluator> exprList = exprEntry.getValue();
      ArrayList<ObjectInspector> fieldOIList = new ArrayList<ObjectInspector>();
      for (int i = 0; i < exprList.size(); i++) {
        fieldOIList.add(exprList.get(i).initialize(inputObjInspector[alias]));
      }
      result.put(alias, fieldOIList);
    }
    return result;
  }

  protected static HashMap<Byte, List<ObjectInspector>> getStandardObjectInspectors(
      Map<Byte, List<ObjectInspector>> aliasToObjectInspectors) {
    HashMap<Byte, List<ObjectInspector>> result = new HashMap<Byte, List<ObjectInspector>>();
    for (Entry<Byte, List<ObjectInspector>> oiEntry : aliasToObjectInspectors
        .entrySet()) {
      Byte alias = oiEntry.getKey();
      List<ObjectInspector> oiList = oiEntry.getValue();
      ArrayList<ObjectInspector> fieldOIList = new ArrayList<ObjectInspector>(
          oiList.size());
      for (int i = 0; i < oiList.size(); i++) {
        fieldOIList.add(ObjectInspectorUtils.getStandardObjectInspector(
            oiList.get(i), ObjectInspectorCopyOption.WRITABLE));
      }
      result.put(alias, fieldOIList);
    }
    return result;

  }

  protected static <T extends joinDesc> ObjectInspector getJoinOutputObjectInspector(
      Byte[] order, Map<Byte, List<ObjectInspector>> aliasToObjectInspectors,
      T conf) {
    ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
    for (Byte alias : order) {
      List<ObjectInspector> oiList = aliasToObjectInspectors.get(alias);
      structFieldObjectInspectors.addAll(oiList);
    }

    StructObjectInspector joinOutputObjectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(conf.getOutputColumnNames(),
            structFieldObjectInspectors);
    return joinOutputObjectInspector;
  }

  Configuration hconf;

  protected void initializeOp(Configuration hconf) throws HiveException {
    LOG.info("COMMONJOIN "
        + ((StructObjectInspector) inputObjInspectors[0]).getTypeName());
    this.handleSkewJoin = conf.getHandleSkewJoin();

    totalSz = 0;
    this.hconf = hconf;
    storage = new HashMap<Byte, RowContainer<ArrayList<Object>>>();

    numAliases = conf.getExprs().size();

    joinValues = new HashMap<Byte, List<ExprNodeEvaluator>>();

    joinFilters = new HashMap<Byte, List<ExprNodeEvaluator>>();

    if (order == null) {
      order = conf.getTagOrder();
    }
    condn = conf.getConds();
    noOuterJoin = conf.isNoOuterJoin();

    totalSz = populateJoinKeyValue(joinValues, conf.getExprs());
    populateJoinKeyValue(joinFilters, conf.getFilters());

    joinValuesObjectInspectors = getObjectInspectorsFromEvaluators(joinValues,
        inputObjInspectors);
    joinFilterObjectInspectors = getObjectInspectorsFromEvaluators(joinFilters,
        inputObjInspectors);
    joinValuesStandardObjectInspectors = getStandardObjectInspectors(joinValuesObjectInspectors);

    if (noOuterJoin) {
      rowContainerStandardObjectInspectors = joinValuesStandardObjectInspectors;
    } else {
      Map<Byte, List<ObjectInspector>> rowContainerObjectInspectors = new HashMap<Byte, List<ObjectInspector>>();
      for (Byte alias : order) {
        ArrayList<ObjectInspector> rcOIs = new ArrayList<ObjectInspector>();
        rcOIs.addAll(joinValuesObjectInspectors.get(alias));
        rcOIs
            .add(PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
        rowContainerObjectInspectors.put(alias, rcOIs);
      }
      rowContainerStandardObjectInspectors = getStandardObjectInspectors(rowContainerObjectInspectors);
    }

    dummyObj = new Object[numAliases];
    dummyObjVectors = new RowContainer[numAliases];

    joinEmitInterval = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVEJOINEMITINTERVAL);
    joinCacheSize = HiveConf.getIntVar(hconf,
        HiveConf.ConfVars.HIVEJOINCACHESIZE);

    byte pos = 0;
    for (Byte alias : order) {
      int sz = conf.getExprs().get(alias).size();
      ArrayList<Object> nr = new ArrayList<Object>(sz);

      for (int j = 0; j < sz; j++)
        nr.add(null);

      if (!noOuterJoin) {
        nr.add(new BooleanWritable(false));
      }

      dummyObj[pos] = nr;
      RowContainer<ArrayList<Object>> values = getRowContainer(hconf, pos,
          alias, 1);
      values.add((ArrayList<Object>) dummyObj[pos]);
      dummyObjVectors[pos] = values;
//syslog
//    LOG.info("hive query id " + hconf.getStrings("hive.query.id","xx"));
//sysout
//    System.out.println("hive query id " + hconf.get("hive.query.id","xx"));
      RowContainer rc = getRowContainer(hconf, pos, alias, joinCacheSize);
      storage.put(pos, rc);
      pos++;
    }

    forwardCache = new Object[totalSz];

    outputObjInspector = getJoinOutputObjectInspector(order,
        joinValuesStandardObjectInspectors, conf);
    LOG.info("JOIN "
        + ((StructObjectInspector) outputObjInspector).getTypeName()
        + " totalsz = " + totalSz);
  }

  private SerDe getSpillSerDe(byte alias) {
    tableDesc desc = getSpillTableDesc(alias);
    if (desc == null)
      return null;
    SerDe sd = (SerDe) ReflectionUtils.newInstance(desc.getDeserializerClass(),
        null);
    try {
      sd.initialize(null, desc.getProperties());
    } catch (SerDeException e) {
      e.printStackTrace();
      return null;
    }
    return sd;
  }

  private void initSpillTables() {
    Map<Byte, List<exprNodeDesc>> exprs = conf.getExprs();

    spillTableDesc = new HashMap<Byte, tableDesc>(exprs.size());
    for (int tag = 0; tag < exprs.size(); tag++) {
      List<exprNodeDesc> valueCols = exprs.get((byte) tag);
      int columnSize = valueCols.size();
      StringBuffer colNames = new StringBuffer();
      StringBuffer colTypes = new StringBuffer();
      if (columnSize <= 0)
        continue;
      for (int k = 0; k < columnSize; k++) {
        String newColName = tag + "_VALUE_" + k;
        colNames.append(newColName);
        colNames.append(',');
        colTypes.append(valueCols.get(k).getTypeString());
        colTypes.append(',');
      }
      if (!noOuterJoin) {
        colNames.append("filtered");
        colNames.append(',');
        colTypes.append(TypeInfoFactory.booleanTypeInfo.getTypeName());
        colTypes.append(',');
      }
      colNames.setLength(colNames.length() - 1);
      colTypes.setLength(colTypes.length() - 1);
      tableDesc tblDesc = new tableDesc(LazyBinarySerDe.class,
          SequenceFileInputFormat.class, HiveSequenceFileOutputFormat.class,
          Utilities.makeProperties(
              org.apache.hadoop.hive.serde.Constants.SERIALIZATION_FORMAT, ""
                  + Utilities.ctrlaCode,
              org.apache.hadoop.hive.serde.Constants.LIST_COLUMNS,
              colNames.toString(),
              org.apache.hadoop.hive.serde.Constants.LIST_COLUMN_TYPES,
              colTypes.toString()));
      spillTableDesc.put((byte) tag, tblDesc);
    }
  }

  transient boolean newGroupStarted = false;

  public tableDesc getSpillTableDesc(Byte alias) {
    if (spillTableDesc == null || spillTableDesc.size() == 0)
      initSpillTables();
    return spillTableDesc.get(alias);
  }

  public Map<Byte, tableDesc> getSpillTableDesc() {
    if (spillTableDesc == null)
      initSpillTables();
    return spillTableDesc;
  }

  public void startGroup() throws HiveException {
    LOG.trace("Join: Starting new group");
    newGroupStarted = true;
    for (RowContainer<ArrayList<Object>> alw : storage.values()) {
      alw.clear();
    }
  }

  protected int getNextSize(int sz) {
    if (sz >= 100000)
      return sz + 100000;

    return 2 * sz;
  }

  transient protected Byte alias;

  protected static ArrayList<Object> computeKeys(Object row,
      List<ExprNodeEvaluator> keyFields, List<ObjectInspector> keyFieldsOI)
      throws HiveException {

    ArrayList<Object> nr = new ArrayList<Object>(keyFields.size());
    for (int i = 0; i < keyFields.size(); i++) {

      nr.add(ObjectInspectorUtils.copyToStandardObject(keyFields.get(i)
          .evaluate(row), keyFieldsOI.get(i),
          ObjectInspectorCopyOption.WRITABLE));
    }

    return nr;
  }

  protected static ArrayList<Object> computeValues(Object row,
      List<ExprNodeEvaluator> valueFields, List<ObjectInspector> valueFieldsOI,
      List<ExprNodeEvaluator> filters, List<ObjectInspector> filtersOI,
      boolean noOuterJoin) throws HiveException {

    ArrayList<Object> nr = new ArrayList<Object>(valueFields.size());
    for (int i = 0; i < valueFields.size(); i++) {
      nr.add((Object) ObjectInspectorUtils.copyToStandardObject(valueFields
          .get(i).evaluate(row), valueFieldsOI.get(i),
          ObjectInspectorCopyOption.WRITABLE));
    }
    if (!noOuterJoin) {
      nr.add(new BooleanWritable(isFiltered(row, filters, filtersOI)));
    }

    return nr;
  }

  transient Object[] forwardCache;

  private void createForwardJoinObject(IntermediateObject intObj,
      boolean[] nullsArr) throws HiveException {
    int p = 0;
    for (int i = 0; i < numAliases; i++) {
      Byte alias = order[i];
      int sz = joinValues.get(alias).size();
      if (nullsArr[i]) {
        for (int j = 0; j < sz; j++) {
          forwardCache[p++] = null;
        }
      } else {
        ArrayList<Object> obj = intObj.getObjs()[i];
        for (int j = 0; j < sz; j++) {
          forwardCache[p++] = obj.get(j);
        }
      }
    }
    reporter.incrCounter(Counter.JOIN_FORWARD_ROW_COUNT, 1);
    forward(forwardCache, outputObjInspector);
  }

  private void copyOldArray(boolean[] src, boolean[] dest) {
    for (int i = 0; i < src.length; i++)
      dest[i] = src[i];
  }

  private ArrayList<boolean[]> joinObjectsInnerJoin(
      ArrayList<boolean[]> resNulls, ArrayList<boolean[]> inputNulls,
      ArrayList<Object> newObj, IntermediateObject intObj, int left,
      boolean newObjNull) {
    if (newObjNull)
      return resNulls;
    Iterator<boolean[]> nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext()) {
      boolean[] oldNulls = nullsIter.next();
      boolean oldObjNull = oldNulls[left];
      if (!oldObjNull) {
        boolean[] newNulls = new boolean[intObj.getCurSize()];
        copyOldArray(oldNulls, newNulls);
        newNulls[oldNulls.length] = false;
        resNulls.add(newNulls);
      }
    }
    return resNulls;
  }

  private ArrayList<boolean[]> joinObjectsLeftSemiJoin(
      ArrayList<boolean[]> resNulls, ArrayList<boolean[]> inputNulls,
      ArrayList<Object> newObj, IntermediateObject intObj, int left,
      boolean newObjNull) {
    if (newObjNull)
      return resNulls;
    Iterator<boolean[]> nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext()) {
      boolean[] oldNulls = nullsIter.next();
      boolean oldObjNull = oldNulls[left];
      if (!oldObjNull) {
        boolean[] newNulls = new boolean[intObj.getCurSize()];
        copyOldArray(oldNulls, newNulls);
        newNulls[oldNulls.length] = false;
        resNulls.add(newNulls);
      }
    }
    return resNulls;
  }

  private ArrayList<boolean[]> joinObjectsLeftOuterJoin(
      ArrayList<boolean[]> resNulls, ArrayList<boolean[]> inputNulls,
      ArrayList<Object> newObj, IntermediateObject intObj, int left,
      boolean newObjNull) {
    newObjNull = newObjNull
        || ((BooleanWritable) (intObj.getObjs()[left].get(joinValues.get(
            order[left]).size()))).get();

    Iterator<boolean[]> nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext()) {
      boolean[] oldNulls = nullsIter.next();
      boolean oldObjNull = oldNulls[left];
      boolean[] newNulls = new boolean[intObj.getCurSize()];
      copyOldArray(oldNulls, newNulls);
      if (oldObjNull)
        newNulls[oldNulls.length] = true;
      else
        newNulls[oldNulls.length] = newObjNull;
      resNulls.add(newNulls);
    }
    return resNulls;
  }

  private ArrayList<boolean[]> joinObjectsRightOuterJoin(
      ArrayList<boolean[]> resNulls, ArrayList<boolean[]> inputNulls,
      ArrayList<Object> newObj, IntermediateObject intObj, int left,
      boolean newObjNull, boolean firstRow) {
    if (newObjNull)
      return resNulls;

    if (inputNulls.isEmpty() && firstRow) {
      boolean[] newNulls = new boolean[intObj.getCurSize()];
      for (int i = 0; i < intObj.getCurSize() - 1; i++)
        newNulls[i] = true;
      newNulls[intObj.getCurSize() - 1] = newObjNull;
      resNulls.add(newNulls);
      return resNulls;
    }

    boolean allOldObjsNull = firstRow;

    Iterator<boolean[]> nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext()) {
      boolean[] oldNulls = nullsIter.next();
      if (!oldNulls[left]) {
        allOldObjsNull = false;
        break;
      }
    }

    if (((BooleanWritable) newObj.get(newObj.size() - 1)).get()) {
      allOldObjsNull = true;
    }

    nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext()) {
      boolean[] oldNulls = nullsIter.next();
      boolean oldObjNull = oldNulls[left] || allOldObjsNull;

      if (!oldObjNull) {
        boolean[] newNulls = new boolean[intObj.getCurSize()];
        copyOldArray(oldNulls, newNulls);
        newNulls[oldNulls.length] = newObjNull;
        resNulls.add(newNulls);
      } else if (allOldObjsNull) {
        boolean[] newNulls = new boolean[intObj.getCurSize()];
        for (int i = 0; i < intObj.getCurSize() - 1; i++)
          newNulls[i] = true;
        newNulls[oldNulls.length] = newObjNull;
        resNulls.add(newNulls);
        return resNulls;
      }
    }
    return resNulls;
  }

  private ArrayList<boolean[]> joinObjectsFullOuterJoin(
      ArrayList<boolean[]> resNulls, ArrayList<boolean[]> inputNulls,
      ArrayList<Object> newObj, IntermediateObject intObj, int left,
      boolean newObjNull, boolean firstRow) {
    if (newObjNull) {
      Iterator<boolean[]> nullsIter = inputNulls.iterator();
      while (nullsIter.hasNext()) {
        boolean[] oldNulls = nullsIter.next();
        boolean[] newNulls = new boolean[intObj.getCurSize()];
        copyOldArray(oldNulls, newNulls);
        newNulls[oldNulls.length] = newObjNull;
        resNulls.add(newNulls);
      }
      return resNulls;
    }

    if (inputNulls.isEmpty() && firstRow) {
      boolean[] newNulls = new boolean[intObj.getCurSize()];
      for (int i = 0; i < intObj.getCurSize() - 1; i++)
        newNulls[i] = true;
      newNulls[intObj.getCurSize() - 1] = newObjNull;
      resNulls.add(newNulls);
      return resNulls;
    }

    boolean allOldObjsNull = firstRow;

    Iterator<boolean[]> nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext()) {
      boolean[] oldNulls = nullsIter.next();
      if (!oldNulls[left]) {
        allOldObjsNull = false;
        break;
      }
    }
    if (((BooleanWritable) newObj.get(newObj.size() - 1)).get()) {
      allOldObjsNull = true;
    }

    boolean rhsPreserved = false;

    nullsIter = inputNulls.iterator();
    while (nullsIter.hasNext()) {
      boolean[] oldNulls = nullsIter.next();
      boolean oldObjNull = oldNulls[left]
          || ((BooleanWritable) (intObj.getObjs()[left].get(joinValues.get(
              order[left]).size()))).get() || allOldObjsNull;

      if (!oldObjNull) {
        boolean[] newNulls = new boolean[intObj.getCurSize()];
        copyOldArray(oldNulls, newNulls);
        newNulls[oldNulls.length] = newObjNull;
        resNulls.add(newNulls);
      } else if (oldObjNull) {
        boolean[] newNulls = new boolean[intObj.getCurSize()];
        copyOldArray(oldNulls, newNulls);
        newNulls[oldNulls.length] = true;
        resNulls.add(newNulls);

        if (allOldObjsNull && !rhsPreserved) {
          newNulls = new boolean[intObj.getCurSize()];
          for (int i = 0; i < oldNulls.length; i++)
            newNulls[i] = true;
          newNulls[oldNulls.length] = false;
          resNulls.add(newNulls);
          rhsPreserved = true;
        }
      }
    }
    return resNulls;
  }

  private ArrayList<boolean[]> joinObjects(ArrayList<boolean[]> inputNulls,
      ArrayList<Object> newObj, IntermediateObject intObj, int joinPos,
      boolean firstRow) {
    ArrayList<boolean[]> resNulls = new ArrayList<boolean[]>();
    boolean newObjNull = newObj == dummyObj[joinPos] ? true : false;
    if (joinPos == 0) {
      if (newObjNull)
        return null;
      boolean[] nulls = new boolean[1];
      nulls[0] = newObjNull;
      resNulls.add(nulls);
      return resNulls;
    }

    int left = condn[joinPos - 1].getLeft();
    int type = condn[joinPos - 1].getType();

    if (((type == joinDesc.RIGHT_OUTER_JOIN) || (type == joinDesc.FULL_OUTER_JOIN))
        && !newObjNull && (inputNulls == null) && firstRow) {
      boolean[] newNulls = new boolean[intObj.getCurSize()];
      for (int i = 0; i < newNulls.length - 1; i++)
        newNulls[i] = true;
      newNulls[newNulls.length - 1] = false;
      resNulls.add(newNulls);
      return resNulls;
    }

    if (inputNulls == null)
      return null;

    if (type == joinDesc.INNER_JOIN)
      return joinObjectsInnerJoin(resNulls, inputNulls, newObj, intObj, left,
          newObjNull);
    else if (type == joinDesc.LEFT_OUTER_JOIN)
      return joinObjectsLeftOuterJoin(resNulls, inputNulls, newObj, intObj,
          left, newObjNull);
    else if (type == joinDesc.RIGHT_OUTER_JOIN)
      return joinObjectsRightOuterJoin(resNulls, inputNulls, newObj, intObj,
          left, newObjNull, firstRow);
    else if (type == joinDesc.LEFT_SEMI_JOIN)
      return joinObjectsLeftSemiJoin(resNulls, inputNulls, newObj, intObj,
          left, newObjNull);

    assert (type == joinDesc.FULL_OUTER_JOIN);
    return joinObjectsFullOuterJoin(resNulls, inputNulls, newObj, intObj, left,
        newObjNull, firstRow);
  }

  private void genObject(ArrayList<boolean[]> inputNulls, int aliasNum,
      IntermediateObject intObj, boolean firstRow) throws HiveException {
    boolean childFirstRow = firstRow;
    boolean skipping = false;

    if (aliasNum < numAliases) {

      RowContainer<ArrayList<Object>> aliasRes = storage.get(order[aliasNum]);

      for (ArrayList<Object> newObj = aliasRes.first(); newObj != null; newObj = aliasRes
          .next()) {

        if (aliasNum > 0
            && condn[aliasNum - 1].getType() == joinDesc.LEFT_SEMI_JOIN
            && newObj != dummyObj[aliasNum]) {
          skipping = true;
        }

        intObj.pushObj(newObj);

        ArrayList<boolean[]> newNulls = joinObjects(inputNulls, newObj, intObj,
            aliasNum, childFirstRow);

        genObject(newNulls, aliasNum + 1, intObj, firstRow);

        intObj.popObj();
        firstRow = false;

        if (skipping) {
          break;
        }
      }
    } else {
      if (inputNulls == null)
        return;
      Iterator<boolean[]> nullsIter = inputNulls.iterator();
      while (nullsIter.hasNext()) {
        boolean[] nullsVec = nullsIter.next();
        createForwardJoinObject(intObj, nullsVec);
      }
    }
  }

  public void endGroup() throws HiveException {
    LOG.trace("Join Op: endGroup called: numValues=" + numAliases);
    checkAndGenObject();
  }

  private void genUniqueJoinObject(int aliasNum, IntermediateObject intObj)
      throws HiveException {
    if (aliasNum == numAliases) {
      int p = 0;
      for (int i = 0; i < numAliases; i++) {
        int sz = joinValues.get(order[i]).size();
        ArrayList<Object> obj = intObj.getObjs()[i];
        for (int j = 0; j < sz; j++) {
          forwardCache[p++] = obj.get(j);
        }
      }

      reporter.incrCounter(Counter.JOIN_FORWARD_ROW_COUNT, 1);
      forward(forwardCache, outputObjInspector);
      return;
    }

    RowContainer<ArrayList<Object>> alias = storage.get(order[aliasNum]);
    for (ArrayList<Object> row = alias.first(); row != null; row = alias.next()) {
      intObj.pushObj(row);
      genUniqueJoinObject(aliasNum + 1, intObj);
      intObj.popObj();
    }
  }

  protected void checkAndGenObject() throws HiveException {

    if (condn[0].getType() == joinDesc.UNIQUE_JOIN) {
      IntermediateObject intObj = new IntermediateObject(
          new ArrayList[numAliases], 0);

      boolean preserve = false;
      boolean hasNulls = false;
      for (int i = 0; i < numAliases; i++) {
        Byte alias = order[i];
        RowContainer<ArrayList<Object>> alw = storage.get(alias);
        if (alw.size() == 0) {
          alw.add((ArrayList<Object>) dummyObj[i]);
          hasNulls = true;
        } else if (condn[i].getPreserved()) {
          preserve = true;
        }
      }
      if (hasNulls && !preserve) {
        return;
      }

      LOG.trace("calling genUniqueJoinObject");
      genUniqueJoinObject(0, new IntermediateObject(new ArrayList[numAliases],
          0));
      LOG.trace("called genUniqueJoinObject");
    } else {
      for (int i = 0; i < numAliases; i++) {
        Byte alias = order[i];
        RowContainer<ArrayList<Object>> alw = storage.get(alias);
        if (alw.size() == 0) {
          if (noOuterJoin) {
            LOG.trace("No data for alias=" + i);
            return;
          } else {
            alw.add((ArrayList<Object>) dummyObj[i]);
          }
        }
      }

      genObject(null, 0, new IntermediateObject(new ArrayList[numAliases], 0),
          true);
    }

  }

  public void closeOp(boolean abort) throws HiveException {
    LOG.trace("Join Op close");
    for (RowContainer<ArrayList<Object>> alw : storage.values()) {
      if (alw != null)
        alw.clear();
    }
    storage.clear();
    super.closeOp(abort);
  }

  @Override
  public String getName() {
    return "JOIN";
  }

  public Map<Integer, Set<String>> getPosToAliasMap() {
    return posToAliasMap;
  }

  public void setPosToAliasMap(Map<Integer, Set<String>> posToAliasMap) {
    this.posToAliasMap = posToAliasMap;
  }

  RowContainer getRowContainer(Configuration hconf, byte pos, Byte alias,
      int containerSize) throws HiveException {
    tableDesc tblDesc = getSpillTableDesc(alias);
    SerDe serde = getSpillSerDe(alias);

    if (serde == null) {
      containerSize = 1;
    }

    RowContainer rc = new RowContainer(containerSize, hconf);
    StructObjectInspector rcOI = null;
    if (tblDesc != null) {
      List<String> colNames = Utilities.getColumnNames(tblDesc.getProperties());
      rcOI = ObjectInspectorFactory.getStandardStructObjectInspector(colNames,
          rowContainerStandardObjectInspectors.get(pos));

    }

    rc.setSerDe(serde, rcOI);
    rc.setTableDesc(tblDesc);
    return rc;
  }

  protected static Boolean isFiltered(Object row,
      List<ExprNodeEvaluator> filters, List<ObjectInspector> ois)
      throws HiveException {
    Boolean ret = false;
    for (int j = 0; j < filters.size(); j++) {
      Object condition = filters.get(j).evaluate(row);
      ret = (Boolean) ((PrimitiveObjectInspector) ois.get(j))
          .getPrimitiveJavaObject(condition);
      if (ret == null || !ret) {
        return true;
      }
    }
    return false;
  }

}
