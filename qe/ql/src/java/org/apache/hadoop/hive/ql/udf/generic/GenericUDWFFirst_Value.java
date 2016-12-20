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
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class GenericUDWFFirst_Value implements GenericUDWFResolver {

  @Override
  public GenericUDWFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    return new GenericUDWFFFirst_valueEvaluator();
  }

  public static class GenericUDWFFFirst_valueEvaluator extends
      GenericUDWFEvaluator {

    private static final long serialVersionUID = -6511136220239205508L;

    ObjectInspector resObjectInspector = null;

    public ObjectInspector init(ObjectInspector[] parameters)
        throws HiveException {
      resObjectInspector = parameters[0];
      return parameters[0];
    }

    static class FirstValueAgg implements AnalysisEvaluatorBuffer {
      boolean first = true;
      Object res = null;
    }

    public AnalysisEvaluatorBuffer getNewAnalysisEvaBuffer() {
      return new FirstValueAgg();
    }

    public void analysis(AnalysisEvaluatorBuffer analysisEvaBuffer,
        Object[] parameters) {
      FirstValueAgg myagg = (FirstValueAgg) analysisEvaBuffer;
      if (myagg.first) {
        myagg.res = ObjectInspectorUtils.copyToStandardObject(parameters[0],
            this.resObjectInspector);
        myagg.first = false;
      }
    }

    public Object terminate(AnalysisEvaluatorBuffer analysisEvaBuffer,
        ExprNodeEvaluator[] analysisParameterFields,
        AnalysisBuffer<Object> analysisBuffer, int currrowid, boolean absolute,
        BooleanTrans canternimate) {
      canternimate.set(true);
      FirstValueAgg myagg = (FirstValueAgg) analysisEvaBuffer;
      return myagg.res;
    }

    public void reset(AnalysisEvaluatorBuffer analysisEvaluatorBuffer) {
      FirstValueAgg myagg = (FirstValueAgg) analysisEvaluatorBuffer;
      myagg.first = true;
      myagg.res = null;
    }

    @Override
    public Object terminateCurrent(AnalysisEvaluatorBuffer analysisEvaBuffer,
        Object obj) {
      FirstValueAgg myagg = (FirstValueAgg) analysisEvaBuffer;
      return myagg.res;
    }
  }
}
