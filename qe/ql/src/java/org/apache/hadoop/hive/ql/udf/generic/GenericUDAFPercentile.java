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
package org.apache.hadoop.hive.ql.udf.generic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

@description(name = "percentile", value = "_FUNC_(expr, pc) - Returns the percentile(s) of expr at pc (range: [0,1])."
    + "pc can be a double or double array")
public class GenericUDAFPercentile extends AbstractGenericUDAFResolver {

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    if (parameters.length != 2) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Please specify two arguments.");
    }

    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
          "Only primitive type arguments are accepted but "
              + parameters[0].getTypeName() + " is passed.");
    }

    switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
    case LONG:
      break;
    default:
      throw new UDFArgumentTypeException(0,
          "Only bigint type arguments are accepted but "
              + parameters[0].getTypeName() + " was passed as parameter 1.");
    }

    boolean wantManyQuantiles = false;
    switch (parameters[1].getCategory()) {
    case PRIMITIVE:
      switch (((PrimitiveTypeInfo) parameters[1]).getPrimitiveCategory()) {
      case FLOAT:
      case DOUBLE:
        break;
      default:
        throw new UDFArgumentTypeException(
            1,
            "Only a float/double or float/double array argument is accepted as parameter 2, but "
                + parameters[1].getTypeName() + " was passed instead.");
      }
      break;

    case LIST:
      if (((ListTypeInfo) parameters[1]).getListElementTypeInfo().getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(1,
            "A float/double array argument may be passed as parameter 2, but "
                + parameters[1].getTypeName() + " was passed instead.");
      }
      switch (((PrimitiveTypeInfo) ((ListTypeInfo) parameters[1])
          .getListElementTypeInfo()).getPrimitiveCategory()) {
      case FLOAT:
      case DOUBLE:
        break;
      default:
        throw new UDFArgumentTypeException(1,
            "A float/double array argument may be passed as parameter 2, but "
                + parameters[1].getTypeName() + " was passed instead.");
      }
      wantManyQuantiles = true;
      break;

    default:
      throw new UDFArgumentTypeException(
          1,
          "Only a float/double or float/double array argument is accepted as parameter 2, but "
              + parameters[1].getTypeName() + " was passed instead.");
    }

    if (wantManyQuantiles) {
      return new GenericPercentileLongArrayEvaluator();
    } else {
      return new GenericPercentileLongEvaluator();
    }
  }

  public static class MyComparator implements
      Comparator<Map.Entry<LongWritable, LongWritable>> {
    @Override
    public int compare(Map.Entry<LongWritable, LongWritable> o1,
        Map.Entry<LongWritable, LongWritable> o2) {
      return o1.getKey().compareTo(o2.getKey());
    }
  }

  static class PercentileAggBuf implements AggregationBuffer {
    private Map<LongWritable, LongWritable> counts;
    private List<DoubleWritable> percentiles;
  }

  private static void increment(PercentileAggBuf s, LongWritable o, long i) {
    LongWritable count = s.counts.get(o);
    if (count == null) {
      LongWritable key = new LongWritable();
      key.set(o.get());
      s.counts.put(key, new LongWritable(i));
    } else {
      count.set(count.get() + i);
    }
  }

  private static double getPercentile(
      List<Map.Entry<LongWritable, LongWritable>> entriesList, double position) {
    long lower = (long) Math.floor(position);
    long higher = (long) Math.ceil(position);

    int i = 0;
    while (entriesList.get(i).getValue().get() < lower + 1) {
      i++;
    }

    long lowerKey = entriesList.get(i).getKey().get();
    if (higher == lower) {
      return lowerKey;
    }

    if (entriesList.get(i).getValue().get() < higher + 1) {
      i++;
    }
    long higherKey = entriesList.get(i).getKey().get();

    if (higherKey == lowerKey) {
      return lowerKey;
    }

    return (higher - position) * lowerKey + (position - lower) * higherKey;
  }

  public static class GenericPercentileLongEvaluator extends
      GenericUDAFPercentileEvaluator {
    protected PrimitiveObjectInspector pOI;
    private DoubleWritable result;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      super.init(m, parameters);

      if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
        colOI = (PrimitiveObjectInspector) parameters[0];
        pOI = (PrimitiveObjectInspector) parameters[1];
      } else {
        soi = (StructObjectInspector) parameters[0];
        colField = soi.getStructFieldRef("col");
        pField = soi.getStructFieldRef("p");
        colFieldOI = (StandardMapObjectInspector) colField
            .getFieldObjectInspector();
        pFieldOI = (StandardListObjectInspector) pField
            .getFieldObjectInspector();
      }

      if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
        ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
        foi.add(ObjectInspectorFactory.getStandardMapObjectInspector(
            PrimitiveObjectInspectorFactory.writableLongObjectInspector,
            PrimitiveObjectInspectorFactory.writableLongObjectInspector));
        foi.add(ObjectInspectorFactory
            .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));
        ArrayList<String> fname = new ArrayList<String>();
        fname.add("col");
        fname.add("p");
        partialResult = new Object[2];
        partialResult[0] = new HashMap<LongWritable, LongWritable>();
        partialResult[1] = new ArrayList<DoubleWritable>();

        return ObjectInspectorFactory.getStandardStructObjectInspector(fname,
            foi);
      } else {
        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
      }
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
        throws HiveException {
      if (parameters[0] == null || parameters[1] == null) {
        return;
      }

      PercentileAggBuf myagg = (PercentileAggBuf) agg;
      if (myagg.percentiles.size() == 0) {
        Double percent = PrimitiveObjectInspectorUtils.getDouble(parameters[1],
            pOI);
        if (percent < 0.0 || percent > 1.0) {
          throw new RuntimeException(
              "Percentile value must be wihin the range of 0 to 1.");
        }
        myagg.percentiles.add(new DoubleWritable(percent));
      }

      Long colVal = PrimitiveObjectInspectorUtils.getLong(parameters[0], colOI);
      if (colVal != null) {
        increment(myagg, new LongWritable(colVal), 1);
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      PercentileAggBuf state = (PercentileAggBuf) agg;
      if (state.counts == null || state.counts.size() == 0) {
        return null;
      }

      Set<Map.Entry<LongWritable, LongWritable>> entries = state.counts
          .entrySet();
      List<Map.Entry<LongWritable, LongWritable>> entriesList = new ArrayList<Map.Entry<LongWritable, LongWritable>>(
          entries);
      Collections.sort(entriesList, new MyComparator());

      long total = 0;
      for (int i = 0; i < entriesList.size(); i++) {
        LongWritable count = entriesList.get(i).getValue();
        total += count.get();
        count.set(total);
      }

      if (result == null) {
        result = new DoubleWritable();
      }

      long maxPosition = total - 1;
      double position = maxPosition * state.percentiles.get(0).get();
      result.set(getPercentile(entriesList, position));
      return result;
    }
  }

  public static class GenericPercentileLongArrayEvaluator extends
      GenericUDAFPercentileEvaluator {
    protected StandardListObjectInspector pOI;
    private List<DoubleWritable> results;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      super.init(m, parameters);

      if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
        colOI = (PrimitiveObjectInspector) parameters[0];
        pOI = (StandardListObjectInspector) parameters[1];
      } else {
        soi = (StructObjectInspector) parameters[0];
        colField = soi.getStructFieldRef("col");
        pField = soi.getStructFieldRef("p");
        colFieldOI = (StandardMapObjectInspector) colField
            .getFieldObjectInspector();
        pFieldOI = (StandardListObjectInspector) pField
            .getFieldObjectInspector();
      }

      if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
        ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
        foi.add(ObjectInspectorFactory.getStandardMapObjectInspector(
            PrimitiveObjectInspectorFactory.writableLongObjectInspector,
            PrimitiveObjectInspectorFactory.writableLongObjectInspector));
        foi.add(ObjectInspectorFactory
            .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector));
        ArrayList<String> fname = new ArrayList<String>();
        fname.add("col");
        fname.add("p");
        partialResult = new Object[2];
        partialResult[0] = new HashMap<LongWritable, LongWritable>();
        partialResult[1] = new ArrayList<DoubleWritable>();

        return ObjectInspectorFactory.getStandardStructObjectInspector(fname,
            foi);
      } else {
        return ObjectInspectorFactory
            .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
      }
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
        throws HiveException {
      if (parameters[0] == null || parameters[1] == null) {
        return;
      }

      PercentileAggBuf myagg = (PercentileAggBuf) agg;
      if (myagg.percentiles.size() == 0) {
        List<DoubleWritable> percentiles = (List<DoubleWritable>) pOI
            .getList(parameters[1]);
        for (DoubleWritable percent : percentiles) {
          if (percent.get() < 0.0 || percent.get() > 1.0) {
            throw new RuntimeException(
                "Percentile value must be wihin the range of 0 to 1.");
          }
          myagg.percentiles.add(percent);
        }
      }

      Long colVal = PrimitiveObjectInspectorUtils.getLong(parameters[0], colOI);
      if (colVal != null) {
        increment(myagg, new LongWritable(colVal), 1);
      }

    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      PercentileAggBuf state = (PercentileAggBuf) agg;
      if (state.counts == null || state.counts.size() == 0) {
        return null;
      }

      Set<Map.Entry<LongWritable, LongWritable>> entries = state.counts
          .entrySet();
      List<Map.Entry<LongWritable, LongWritable>> entriesList = new ArrayList<Map.Entry<LongWritable, LongWritable>>(
          entries);
      Collections.sort(entriesList, new MyComparator());

      long total = 0;
      for (int i = 0; i < entriesList.size(); i++) {
        LongWritable count = entriesList.get(i).getValue();
        total += count.get();
        count.set(total);
      }

      long maxPosition = total - 1;

      if (results == null) {
        results = new ArrayList<DoubleWritable>();
        for (int i = 0; i < state.percentiles.size(); i++) {
          results.add(new DoubleWritable());
        }
      }

      for (int i = 0; i < state.percentiles.size(); i++) {
        double position = maxPosition * state.percentiles.get(i).get();
        results.get(i).set(getPercentile(entriesList, position));
      }
      return results;
    }
  }

  public abstract static class GenericUDAFPercentileEvaluator extends
      GenericUDAFEvaluator {
    protected PrimitiveObjectInspector colOI;

    StructObjectInspector soi;
    StructField colField;
    StructField pField;
    StandardMapObjectInspector colFieldOI;
    StandardListObjectInspector pFieldOI;

    Object[] partialResult;

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      PercentileAggBuf result = new PercentileAggBuf();
      result.counts = new HashMap<LongWritable, LongWritable>();
      result.percentiles = new ArrayList<DoubleWritable>();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      PercentileAggBuf result = (PercentileAggBuf) agg;
      result.counts.clear();
      result.percentiles.clear();
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      PercentileAggBuf result = (PercentileAggBuf) agg;

      ((Map) partialResult[0]).clear();
      for (LongWritable col : result.counts.keySet()) {
        ((Map) partialResult[0]).put(col, result.counts.get(col));
      }

      ((List) partialResult[1]).clear();
      for (DoubleWritable percent : result.percentiles) {
        ((List) partialResult[1]).add(percent);
      }

      return partialResult;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial)
        throws HiveException {
      if (partial != null) {
        Object partialCol = soi.getStructFieldData(partial, colField);
        Object partialP = soi.getStructFieldData(partial, pField);
        Map<LongWritable, LongWritable> colMap = (Map<LongWritable, LongWritable>) colFieldOI
            .getMap(partialCol);
        List<DoubleWritable> pList = (List<DoubleWritable>) pFieldOI
            .getList(partialP);

        PercentileAggBuf myagg = (PercentileAggBuf) agg;
        if (myagg.percentiles.isEmpty()) {
          for (DoubleWritable percent : pList) {
            myagg.percentiles.add(percent);
          }
        }

        for (LongWritable one : colMap.keySet()) {
          increment(myagg, one, colMap.get(one).get());
        }
      }
    }
  }

}
