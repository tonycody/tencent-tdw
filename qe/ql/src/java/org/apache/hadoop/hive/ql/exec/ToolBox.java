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
package org.apache.hadoop.hive.ql.exec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyByteObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyDoubleObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyFloatObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyIntObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyLongObjectInspector;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyPrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.io.BytesWritable;

public class ToolBox {
  public final static String dotDelimiter = ".";
  public final static String colonDelimiter = ":";
  public final static String blankDelimiter = " ";
  public final static String tabDelimiter = "\t";
  public final static String commaDelimiter = ",";
  public final static String starDelimiter = "*";
  public final static String hiveDelimiter = "\1";

  public final static String CBR_SWITCH_ATTR = "Cb.Switch";
  public final static String CB_OPT_ATTR = "Cb.Optimization";
  public final static String STATISTICS_COLUMNS_ATTR = "statistics.columns";
  public final static String TABLE_HEADER_NAMES_ATTR = "TABLEHEADERNAMESATTR";
  public final static String TABLE_HEADER_TYPES_ATTR = "TABLEHEADERTYPESATTR";

  private static Log LOG = LogFactory.getLog("ToolBox");

  public enum SortMethod {
    AscendSort, DescendSort
  };

  SortMethod sortMethod;
  ArrayList<Tuple> tupleList;

  public class Tuple {
    String value;
    Integer freq;

    public Tuple(String v, Integer i) {
      this.value = v;
      this.freq = i;
    }

    public Tuple() {
    }

    public String getString() {
      return this.value;
    }

    public Integer getInteger() {
      return this.freq;
    }
  }

  public class TupleComparatorAscend implements Comparator<Tuple> {
    public int compare(Tuple t1, Tuple t2) {
      int res = t1.getInteger().compareTo(t2.getInteger());
      if (res == 0) {
        if (t1.getString().compareTo(t2.getString()) < 0) {
          res = -1;
        }
      }
      if (res == 0) {
        res = t1.hashCode() - t2.hashCode();
      }
      return res;
    }
  }

  public class TupleComparatorDescend implements Comparator<Tuple> {
    public int compare(Tuple t1, Tuple t2) {
      int res = t1.getInteger().compareTo(t2.getInteger());
      if (res == 0) {
        if (t1.getString().compareTo(t2.getString()) < 0) {
          res = -1;
        }
      }
      if (res == 0) {
        res = t1.hashCode() - t2.hashCode();
      }
      return -res;
    }
  }

  public ToolBox() {
    tupleList = new ArrayList<Tuple>();
  }

  public void push(String value, Integer fre) {
    Tuple t = new Tuple(value, fre);
    tupleList.add(t);
  }

  public void ascendSort() {
    Comparator<Tuple> comparator = new TupleComparatorAscend();
    Collections.sort(tupleList, comparator);
  }

  public void descendSort() {
    Comparator<Tuple> comparator = new TupleComparatorDescend();
    Collections.sort(tupleList, comparator);
  }

  public int getCapacity() {
    return tupleList.size();
  }

  public String getStringAtIdx(int idx) {
    return tupleList.get(idx).getString();
  }

  public Integer getIntegeAtIdx(int idx) {
    return tupleList.get(idx).getInteger();
  }

  void compact(java.util.Map<String, Integer> para, SortMethod sm,
      final Object o) {
    tupleList.clear();
    for (String key : para.keySet()) {
      push(key, para.get(key));
    }

    if (sm == ToolBox.SortMethod.AscendSort) {
      ascendSort();
    } else {
      assert (sm == ToolBox.SortMethod.DescendSort);
      descendSort();
    }

    if (o.getClass().getName().equalsIgnoreCase(Double.class.getName())) {
      double tailFactor = ((Double) o).doubleValue();
      para.clear();
      for (int idx = 0; idx < (int) (getCapacity() * tailFactor); idx++) {
        para.put(getStringAtIdx(idx), getIntegeAtIdx(idx));
      }
    } else {
      assert (o.getClass().getName().equalsIgnoreCase(Integer.class.getName()));
      int tailFactor = ((Integer) o).intValue();
      para.clear();
      for (int idx = 0; idx < tailFactor; idx++) {
        para.put(getStringAtIdx(idx), getIntegeAtIdx(idx));
      }
    }

  }

  void compactByAscendSort(java.util.Map<String, Integer> para,
      final double tailFactor) {
    tupleList.clear();
    for (String key : para.keySet()) {
      push(key, para.get(key));
    }
    ascendSort();
    para.clear();
    for (int idx = 0; idx < (int) (getCapacity() * tailFactor); idx++) {
      para.put(getStringAtIdx(idx), getIntegeAtIdx(idx));
    }
  }

  void compactByDescendSort(java.util.Map<String, Integer> para,
      final double tailFactor) {
    tupleList.clear();
    for (String key : para.keySet()) {
      push(key, para.get(key));
    }
    descendSort();
    para.clear();
    for (int idx = 0; idx < (int) (getCapacity() * tailFactor); idx++) {
      para.put(getStringAtIdx(idx), getIntegeAtIdx(idx));
    }
  }

  static HiveKey getHiveKey(String key, byte streamTag) {
    HiveKey keyWritable = new HiveKey();
    int keylen = key.length();
    keyWritable.setSize(keylen + 1);
    System.arraycopy(key.getBytes(), 0, keyWritable.get(), 0, keylen);
    keyWritable.get()[keylen] = streamTag;
    final int r = 0;
    keyWritable.setHashCode(r);
    return keyWritable;
  }

  static String getOriginalKey(BytesWritable key) {

    byte[] b = key.getBytes();
    return new String(b, 0, key.getSize() - 1);
  }

  static String retrieveComponent(String s, String d, int idx) {
    StringTokenizer st = new StringTokenizer(s, d);
    String ret = null;
    for (int i = 0; i < idx; i++) {
      ret = st.nextToken();
    }
    return ret;
  }

  static <T> ArrayList<TreeMap<String, T>> aggregateKey(
      TreeMap<String, T> para, String delimiter, int idx) {
    ArrayList<TreeMap<String, T>> a = new ArrayList<TreeMap<String, T>>();
    String prekey = null;
    TreeMap<String, T> h = null;
    for (String s : para.keySet()) {
      if (prekey == null) {
        prekey = retrieveComponent(s, delimiter, idx);
        h = new TreeMap<String, T>();
        h.put(s, para.get(s));
      } else if (prekey.equals(s)) {
        h.put(s, para.get(s));
      } else {
        prekey = retrieveComponent(s, delimiter, idx);
        ;
        a.add(h);
        h = new TreeMap<String, T>();
        h.put(s, para.get(s));
      }

    }
    a.add(h);
    return a;
  }

  @Deprecated
  static ArrayList<TreeMap<String, String>> aggregateKey_string(
      TreeMap<String, String> para, String delimiter, int idx) {
    ArrayList<TreeMap<String, String>> a = new ArrayList<TreeMap<String, String>>();
    String prekey = null;
    TreeMap<String, String> h = null;
    for (String s : para.keySet()) {
      if (prekey == null) {
        prekey = retrieveComponent(s, delimiter, idx);
        h = new TreeMap<String, String>();
        h.put(s, para.get(s));
      } else if (prekey.equals(s)) {
        h.put(s, para.get(s));
      } else {
        prekey = retrieveComponent(s, delimiter, idx);
        ;
        a.add(h);
        h = new TreeMap<String, String>();
        h.put(s, para.get(s));
      }

    }
    a.add(h);
    return a;
  }

  @Deprecated
  static ArrayList<TreeMap<String, Integer>> aggregateKey_Integer(
      TreeMap<String, Integer> para, String delimiter, int idx) {
    ArrayList<TreeMap<String, Integer>> a = new ArrayList<TreeMap<String, Integer>>();
    String prekey = null;
    TreeMap<String, Integer> h = null;
    for (String s : para.keySet()) {
      if (prekey == null) {
        prekey = retrieveComponent(s, delimiter, idx);
        h = new TreeMap<String, Integer>();
        h.put(s, para.get(s));
      } else if (prekey.equals(s)) {
        h.put(s, para.get(s));
      } else {
        prekey = retrieveComponent(s, delimiter, idx);
        ;
        a.add(h);
        h = new TreeMap<String, Integer>();
        h.put(s, para.get(s));
      }

    }
    a.add(h);
    return a;
  }

  static double calDistincValue(TreeMap<String, Integer> para,
      int num_sampled_rows) {

    int num_multiple = 0;
    int num_distinct = para.keySet().size();
    LOG.debug("num_distinct: " + num_distinct);
    double stat_distinct_values;
    for (String s : para.keySet()) {
      if (para.get(s) > 1) {
        num_multiple++;
      }
    }
    LOG.debug("num_multiple: " + num_multiple);
    if (num_multiple == 0) {
      stat_distinct_values = -1;
      return stat_distinct_values;
    } else if (num_multiple == num_distinct) {
      stat_distinct_values = num_distinct;
      return stat_distinct_values;
    }
    int totalrows = num_sampled_rows;
    int f1 = num_distinct - num_multiple;
    int d = num_distinct;
    int numer = num_sampled_rows * d;
    int denom = (num_sampled_rows - f1) + f1 * num_sampled_rows / totalrows;
    LOG.debug("numer: " + numer);
    LOG.debug("denom: " + denom);
    int distinct_values = numer / denom;
    if (distinct_values < d) {
      distinct_values = d;
    } else if (distinct_values > totalrows) {
      distinct_values = totalrows;
    }
    LOG.debug("distinct_values: " + distinct_values);
    LOG.debug("totalrows: " + totalrows);
    stat_distinct_values = Math.floor(distinct_values + 0.5);
    if (stat_distinct_values > 0.1 * totalrows) {
      stat_distinct_values = -(stat_distinct_values / totalrows);
    }
    LOG.debug("stat_distinct_values: " + stat_distinct_values);
    return stat_distinct_values;
  }

  static String convertHivePrimitiveStringToLazyTypeString(String hivePrimitive) {
    String lazyType = null;
    if (hivePrimitive.equalsIgnoreCase("STRING"))
      lazyType = "LazyString";
    else if (hivePrimitive.equalsIgnoreCase("BIGINT"))
      lazyType = "LazyInteger";
    else if (hivePrimitive.equalsIgnoreCase("INT"))
      lazyType = "LazyInteger";
    else if (hivePrimitive.equalsIgnoreCase("TINYINT"))
      lazyType = "LazyByte";
    else if (hivePrimitive.equalsIgnoreCase("DOUBLE"))
      lazyType = "LazyDouble";
    else if (hivePrimitive.equalsIgnoreCase("FLOAT"))
      lazyType = "LazyFloat";
    else if (hivePrimitive.equalsIgnoreCase("BOOLEAN"))
      lazyType = "LazyBoolean";

    return lazyType;
  }

  static String convertLazyObjectToString(Object value) {
    String className = value.getClass().getName();
    if (className.endsWith("LazyByte")) {
      byte b = ((LazyByteObjectInspector) LazyPrimitiveObjectInspectorFactory
          .getLazyObjectInspector(PrimitiveCategory.BYTE, false, (byte) 0,
              false)).get(value);
      return String.valueOf(b);
    }
    if (className.endsWith("LazyInteger")) {
      int i = ((LazyIntObjectInspector) LazyPrimitiveObjectInspectorFactory
          .getLazyObjectInspector(PrimitiveCategory.INT, false, (byte) 0, false))
          .get(value);
      return String.valueOf(i);
    }
    if (className.endsWith("LazyLong")) {
      long l = ((LazyLongObjectInspector) LazyPrimitiveObjectInspectorFactory
          .getLazyObjectInspector(PrimitiveCategory.LONG, false, (byte) 0,
              false)).get(value);
      return String.valueOf(l);
    }
    if (className.endsWith("LazyFloat")) {
      float f = ((LazyFloatObjectInspector) LazyPrimitiveObjectInspectorFactory
          .getLazyObjectInspector(PrimitiveCategory.FLOAT, false, (byte) 0,
              false)).get(value);
      return String.valueOf(f);
    }

    if (className.endsWith("LazyDouble")) {
      double d = ((LazyDoubleObjectInspector) LazyPrimitiveObjectInspectorFactory
          .getLazyObjectInspector(PrimitiveCategory.DOUBLE, false, (byte) 0,
              false)).get(value);
      return String.valueOf(d);
    }

    if (className.endsWith("LazyString")) {
      return ((LazyString) value).getWritableObject().toString();

    }
    return new String("");

  }

  public static void debugNode(org.apache.hadoop.hive.ql.lib.Node para,
      int indent) {
    if (para.getChildren() == null) {
      return;
    }
    for (org.apache.hadoop.hive.ql.lib.Node node : para.getChildren()) {
      for (int idx = 0; idx < indent; idx++) {
        System.out.print(" ");

      }
      debugNode(node, indent + 1);
    }

  }

  static public class tableTuple {
    String tableName;
    String fieldName;

    public tableTuple() {
      tableName = null;
      fieldName = null;
    }

    public tableTuple(String tn, String fn) {
      this.tableName = tn;
      this.fieldName = fn;
    }

    public String getTableName() {
      return tableName;
    }

    public void setTableName(String tn) {
      this.tableName = tn;
    }

    public String getFieldName() {
      return fieldName;
    }

  }

  static public class tableAliasTuple {
    String tableName;
    String alias;

    public tableAliasTuple(String tn, String al) {
      this.tableName = tn;
      this.alias = al;
    }

    public tableAliasTuple() {

    }

    public String getTableName() {
      return tableName;
    }

    public String getAlias() {
      return alias;
    }

  }

  static public class tableDistinctTuple {
    String tableName;
    String distinctField;

    public tableDistinctTuple(String tn, String df) {
      this.tableName = tn;
      this.distinctField = df;
    }

    public tableDistinctTuple() {

    }

    public String getTableName() {
      return tableName;
    }

    public String getDistinctField() {
      return distinctField;
    }
  }

}
