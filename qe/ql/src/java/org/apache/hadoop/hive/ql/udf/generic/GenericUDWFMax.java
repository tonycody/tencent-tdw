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
import org.apache.hadoop.hive.ql.exec.persistence.AnalysisBuffer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class GenericUDWFMax implements GenericUDWFResolver {

  @Override
  public GenericUDWFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    return new GenericUDWFMaxEvaluator();
  }

  public static class GenericUDWFMaxEvaluator extends GenericUDWFEvaluator {

    private static final long serialVersionUID = 900432238450969708L;
    PrimitiveObjectInspector inputOI;

    public ObjectInspector init(ObjectInspector[] parameters)
        throws HiveException {
      inputOI = (PrimitiveObjectInspector) parameters[0];
      return inputOI;
    }

    static class MaxAgg implements AnalysisEvaluatorBuffer {
      private Object maxobj;
    }

    public AnalysisEvaluatorBuffer getNewAnalysisEvaBuffer() {
      return new MaxAgg();
    }

    public void analysis(AnalysisEvaluatorBuffer analysisEvaBuffer,
        Object[] parameters) {
      if (parameters == null || parameters.length <= 0 || parameters[0] == null)
        return;
      MaxAgg agg = (MaxAgg) analysisEvaBuffer;
      if (agg.maxobj == null) {
        agg.maxobj = parameters[0];
      } else {
        if (ObjectInspectorUtils.compare(agg.maxobj, inputOI, parameters[0],
            inputOI) < 0) {
          agg.maxobj = parameters[0];
        }
      }
    }

    public Object terminate(AnalysisEvaluatorBuffer analysisEvaBuffer,
        ExprNodeEvaluator[] analysisParameterFields,
        AnalysisBuffer<Object> analysisBuffer, int currrowid, boolean absolute,
        BooleanTrans canternimate) {
      canternimate.set(true);
      MaxAgg agg = (MaxAgg) analysisEvaBuffer;
      return agg.maxobj;
    }

    public void reset(AnalysisEvaluatorBuffer analysisEvaBuffer) {
      MaxAgg agg = (MaxAgg) analysisEvaBuffer;
      agg.maxobj = null;
    }

    @Override
    public Object terminateCurrent(AnalysisEvaluatorBuffer analysisEvaBuffer,
        Object obj) {
      MaxAgg agg = (MaxAgg) analysisEvaBuffer;
      return ObjectInspectorUtils.copyToStandardObject(agg.maxobj, inputOI);
    }
  }
}
