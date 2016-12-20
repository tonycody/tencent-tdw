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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.persistence.AnalysisBuffer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

public class GenericUDWFSum implements GenericUDWFResolver {

  @Override
  public GenericUDWFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    if (parameters.length != 1) {
      throw new UDFArgumentTypeException(parameters.length - 1,
          "Exactly one argument is expected.");
    }

    if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(0,
          "Only primitive type arguments are accepted but "
              + parameters[0].getTypeName() + " is passed.");
    }
    switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
      return new GenericUDWFSumLong();
    case FLOAT:
    case DOUBLE:
    case STRING:
      return new GenericUDWFSumDouble();
    case BOOLEAN:
    default:
      throw new UDFArgumentTypeException(0,
          "Only numeric or string type arguments are accepted but "
              + parameters[0].getTypeName() + " is passed.");
    }
  }

  public static class GenericUDWFSumLong extends GenericUDWFEvaluator {

    transient protected Log LOG = LogFactory.getLog(this.getClass().getName());

    private static final long serialVersionUID = -1002517159134016911L;
    PrimitiveObjectInspector inputOI;
    LongWritable result;

    public ObjectInspector init(ObjectInspector[] parameters)
        throws HiveException {
      result = new LongWritable(0);
      inputOI = (PrimitiveObjectInspector) parameters[0];
      return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    }

    static class SumLongAgg implements AnalysisEvaluatorBuffer {
      boolean empty;
      long sum;
    }

    public AnalysisEvaluatorBuffer getNewAnalysisEvaBuffer() {
      SumLongAgg result = new SumLongAgg();
      reset(result);
      return result;
    }

    public void analysis(AnalysisEvaluatorBuffer agg, Object[] parameters) {
      if (parameters == null || parameters.length <= 0 || parameters[0] == null)
        return;

      SumLongAgg myagg = (SumLongAgg) agg;
      myagg.empty = false;
      myagg.sum += PrimitiveObjectInspectorUtils
          .getLong(parameters[0], inputOI);
    }

    public Object terminate(AnalysisEvaluatorBuffer agg,
        ExprNodeEvaluator[] analysisParameterFields,
        AnalysisBuffer<Object> analysisBuffer, int currrowid, boolean absolute,
        BooleanTrans canternimate) {
      canternimate.set(true);
      SumLongAgg myagg = (SumLongAgg) agg;
      if (myagg.empty) {
        return null;
      }
      result.set(myagg.sum);
      return result;
    }

    public void reset(AnalysisEvaluatorBuffer agg) {
      SumLongAgg myagg = (SumLongAgg) agg;
      myagg.empty = true;
      myagg.sum = 0;
    }

    @Override
    public Object terminateCurrent(AnalysisEvaluatorBuffer analysisEvaBuffer,
        Object obj) {
      SumLongAgg myagg = (SumLongAgg) analysisEvaBuffer;
      if (myagg.empty) {
        return null;
      }
      return new LongWritable(myagg.sum);
    }
  }

  public static class GenericUDWFSumDouble extends GenericUDWFEvaluator {

    private static final long serialVersionUID = -4031378354346971677L;
    PrimitiveObjectInspector inputOI;
    DoubleWritable result;

    public ObjectInspector init(ObjectInspector[] parameters)
        throws HiveException {
      result = new DoubleWritable(0);
      inputOI = (PrimitiveObjectInspector) parameters[0];
      return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    }

    static class SumDoubleAgg implements AnalysisEvaluatorBuffer {
      boolean empty;
      double sum;
    }

    public AnalysisEvaluatorBuffer getNewAnalysisEvaBuffer() {
      SumDoubleAgg result = new SumDoubleAgg();
      reset(result);
      return result;
    }

    public void analysis(AnalysisEvaluatorBuffer agg, Object[] parameters) {
      if (parameters == null || parameters.length <= 0 || parameters[0] == null)
        return;
      SumDoubleAgg myagg = (SumDoubleAgg) agg;
      myagg.empty = false;
      myagg.sum += PrimitiveObjectInspectorUtils.getDouble(parameters[0],
          inputOI);
    }

    public Object terminate(AnalysisEvaluatorBuffer agg,
        ExprNodeEvaluator[] analysisParameterFields,
        AnalysisBuffer<Object> analysisBuffer, int currrowid, boolean absolute,
        BooleanTrans canternimate) {
      canternimate.set(true);
      SumDoubleAgg myagg = (SumDoubleAgg) agg;
      if (myagg.empty) {
        return null;
      }
      result.set(myagg.sum);
      return result;
    }

    public void reset(AnalysisEvaluatorBuffer agg) {
      SumDoubleAgg myagg = (SumDoubleAgg) agg;
      myagg.empty = true;
      myagg.sum = 0;
    }

    @Override
    public Object terminateCurrent(AnalysisEvaluatorBuffer analysisEvaBuffer,
        Object obj) {
      SumDoubleAgg myagg = (SumDoubleAgg) analysisEvaBuffer;
      if (myagg.empty) {
        return null;
      }
      return new DoubleWritable(myagg.sum);
    }
  }
}
