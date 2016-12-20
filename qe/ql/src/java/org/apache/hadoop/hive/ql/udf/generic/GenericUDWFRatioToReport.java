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

import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.persistence.AnalysisBuffer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class GenericUDWFRatioToReport implements GenericUDWFResolver {

  @Override
  public GenericUDWFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    return new GenericUDWFRatioToReportDouble();
  }

  public static class GenericUDWFRatioToReportDouble extends
      GenericUDWFEvaluator {

    private static final long serialVersionUID = 6678237595976259328L;
    PrimitiveObjectInspector inputOI;
    DoubleWritable result;

    public ObjectInspector init(ObjectInspector[] parameters)
        throws HiveException {
      result = new DoubleWritable(0);
      inputOI = (PrimitiveObjectInspector) parameters[0];
      return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
    }

    static class RatioToReportAgg implements AnalysisEvaluatorBuffer {
      boolean empty;
      double sum;
    }

    public AnalysisEvaluatorBuffer getNewAnalysisEvaBuffer() {
      RatioToReportAgg result = new RatioToReportAgg();
      reset(result);
      return result;
    }

    public void analysis(AnalysisEvaluatorBuffer analysisEvaBuffer,
        Object[] parameters) {
      if (parameters == null || parameters.length <= 0 || parameters[0] == null)
        return;
      RatioToReportAgg myagg = (RatioToReportAgg) analysisEvaBuffer;
      myagg.empty = false;
      myagg.sum += PrimitiveObjectInspectorUtils.getDouble(parameters[0],
          inputOI);
    }

    public Object terminate(AnalysisEvaluatorBuffer analysisEvaBuffer,
        ExprNodeEvaluator[] analysisParameterFields,
        AnalysisBuffer<Object> analysisBuffer, int currrowid, boolean absolute,
        BooleanTrans canternimate) {
      canternimate.set(true);
      RatioToReportAgg myagg = (RatioToReportAgg) analysisEvaBuffer;
      if (myagg.empty || myagg.sum == 0) {
        return null;
      }
      double currvalue = 0;
      try {
        Object row = analysisBuffer.getByRowid(currrowid);
        ArrayList<Object> rowfull = ((ArrayList<Object>) row);
        row = rowfull.get(0);

        Object obj = analysisParameterFields[0].evaluate(row);
        if (obj == null)
          return null;
        currvalue = PrimitiveObjectInspectorUtils.getDouble(obj, inputOI);
      } catch (NumberFormatException e) {
        e.printStackTrace();
      } catch (HiveException e) {
        e.printStackTrace();
      }
      result.set(currvalue / myagg.sum);
      return result;
    }

    public void reset(AnalysisEvaluatorBuffer analysisEvaBuffer) {
      RatioToReportAgg agg = (RatioToReportAgg) analysisEvaBuffer;
      agg.empty = true;
      agg.sum = 0;
    }

    @Override
    public Object terminateCurrent(AnalysisEvaluatorBuffer analysisEvaBuffer,
        Object obj) {
      RatioToReportAgg myagg = (RatioToReportAgg) analysisEvaBuffer;
      if (myagg.empty || myagg.sum == 0 || obj == null) {
        return null;
      }
      double currvalue = PrimitiveObjectInspectorUtils.getDouble(obj, inputOI);
      return new DoubleWritable(currvalue / myagg.sum);
    }
  }
}
