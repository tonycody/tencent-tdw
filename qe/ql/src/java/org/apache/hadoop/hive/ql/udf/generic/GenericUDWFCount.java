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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

public class GenericUDWFCount implements GenericUDWFResolver {

  @Override
  public GenericUDWFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    return new GenericUDWFCountEvaluator();
  }

  public static class GenericUDWFCountEvaluator extends GenericUDWFEvaluator {

    private static final long serialVersionUID = 6676985536898826886L;
    LongWritable result;
    private boolean countAllColumns = false;

    public ObjectInspector init(ObjectInspector[] parameters)
        throws HiveException {
      if (parameters == null || parameters.length == 0) {
        countAllColumns = true;
      }
      result = new LongWritable(0);
      return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    }

    static class CountAgg implements AnalysisEvaluatorBuffer {
      long value;
    }

    public AnalysisEvaluatorBuffer getNewAnalysisEvaBuffer() {
      CountAgg result = new CountAgg();
      reset(result);
      return result;
    }

    public void analysis(AnalysisEvaluatorBuffer agg, Object[] parameters) {
      if (parameters == null) {
        return;
      }
      if (countAllColumns) {
        ((CountAgg) agg).value++;
      } else {
        boolean countThisRow = true;
        for (Object nextParam : parameters) {
          if (nextParam == null) {
            countThisRow = false;
            break;
          }
        }
        if (countThisRow) {
          ((CountAgg) agg).value++;
        }
      }
    }

    public Object terminate(AnalysisEvaluatorBuffer agg,
        ExprNodeEvaluator[] analysisParameterFields,
        AnalysisBuffer<Object> analysisBuffer, int currrowid, boolean absolute,
        BooleanTrans canternimate) {
      canternimate.set(true);
      result.set(((CountAgg) agg).value);
      return result;
    }

    public void reset(AnalysisEvaluatorBuffer agg) {
      ((CountAgg) agg).value = 0;
    }

    @Override
    public Object terminateCurrent(AnalysisEvaluatorBuffer agg, Object obj) {
      return new LongWritable(((CountAgg) agg).value);
    }
  }
}
