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

public class GenericUDWFAvg implements GenericUDWFResolver {

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
    case FLOAT:
    case DOUBLE:
    case STRING:
      return new GenericUDWFAvgEvaluator();
    case BOOLEAN:
    default:
      throw new UDFArgumentTypeException(0,
          "Only numeric or string type arguments are accepted but "
              + parameters[0].getTypeName() + " is passed.");
    }
  }

  public static class GenericUDWFAvgEvaluator extends GenericUDWFEvaluator {

    private static final long serialVersionUID = 3933356702897258030L;
    PrimitiveObjectInspector inputOI;
    DoubleWritable result;

    public ObjectInspector init(ObjectInspector[] parameters)
        throws HiveException {
      result = new DoubleWritable(0);
      inputOI = (PrimitiveObjectInspector) parameters[0];
      return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    }

    static class AvgAgg implements AnalysisEvaluatorBuffer {
      boolean finish = false;
      long count;
      double sum;
      double result;
    }

    public AnalysisEvaluatorBuffer getNewAnalysisEvaBuffer() {
      AvgAgg result = new AvgAgg();
      reset(result);
      return result;
    }

    public void analysis(AnalysisEvaluatorBuffer agg, Object[] parameters) {
      AvgAgg myagg = (AvgAgg) agg;
      myagg.finish = false;

      if (parameters == null || parameters.length <= 0 || parameters[0] == null)
        return;
      Object p = parameters[0];
      if (p != null) {
        double v = PrimitiveObjectInspectorUtils.getDouble(p, inputOI);
        myagg.count++;
        myagg.sum += v;
      }
    }

    @Override
    public Object terminate(AnalysisEvaluatorBuffer agg,
        ExprNodeEvaluator[] analysisParameterFields,
        AnalysisBuffer<Object> analysisBuffer, int currrowid, boolean absolute,
        BooleanTrans canternimate) {
      canternimate.set(true);
      AvgAgg myagg = (AvgAgg) agg;
      if (myagg.count == 0) {
        return null;
      } else {
        if (!myagg.finish) {
          myagg.finish = true;
          myagg.result = myagg.sum / myagg.count;
          result.set(myagg.result);
        }
        return result;
      }
    }

    public void reset(AnalysisEvaluatorBuffer agg) {
      AvgAgg myagg = (AvgAgg) agg;
      myagg.finish = false;
      myagg.count = 0;
      myagg.sum = 0;
      myagg.result = 0;
    }

    @Override
    public Object terminateCurrent(AnalysisEvaluatorBuffer agg, Object obj) {
      AvgAgg myagg = (AvgAgg) agg;
      myagg.result = myagg.sum / myagg.count;
      return new DoubleWritable(myagg.result);
    }
  }
}
