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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;

public class GenericUDWFLag implements GenericUDWFResolver {

  @Override
  public GenericUDWFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    return new GenericUDWFFLagEvaluator();
  }

  public static class GenericUDWFFLagEvaluator extends GenericUDWFEvaluator {

    private static final long serialVersionUID = -6511136220239205508L;

    public ObjectInspector init(ObjectInspector[] parameters)
        throws HiveException {
      return parameters[0];
    }

    static class FLagAgg implements AnalysisEvaluatorBuffer {
    }

    public AnalysisEvaluatorBuffer getNewAnalysisEvaBuffer() {
      return new FLagAgg();
    }

    public void analysis(AnalysisEvaluatorBuffer analysisEvaBuffer,
        Object[] parameters) {
    }

    public Object terminate(AnalysisEvaluatorBuffer analysisEvaBuffer,
        ExprNodeEvaluator[] analysisParameterFields,
        AnalysisBuffer<Object> analysisBuffer, int currrowid, boolean absolute,
        BooleanTrans canternimate) {
      Object resobj = null;
      int lag = 1;
      if (analysisParameterFields.length > 1) {
        try {
          lag = ((IntWritable) analysisParameterFields[1].evaluate(null)).get();
        } catch (NumberFormatException e) {
          e.printStackTrace();
        } catch (HiveException e) {
          e.printStackTrace();
        }
      }
      Object defaultvalue = null;
      if (analysisParameterFields.length > 2) {
        try {
          defaultvalue = analysisParameterFields[2].evaluate(null);
        } catch (HiveException e) {
          e.printStackTrace();
        }
      }

      if (!absolute && currrowid < lag)
        canternimate.set(false);
      else {
        canternimate.set(true);
        if (currrowid < lag) {
          resobj = defaultvalue;
        } else {
          try {
            Object row = analysisBuffer.getByRowid(currrowid - lag);
            ArrayList<Object> rowfull = ((ArrayList<Object>) row);
            row = rowfull.get(0);
            resobj = analysisParameterFields[0].evaluate(row);
          } catch (HiveException e) {
            e.printStackTrace();
          }
        }
      }
      return resobj;
    }

    public void reset(AnalysisEvaluatorBuffer analysisEvaluatorBuffer) {
    }

    @Override
    public Object terminateCurrent(AnalysisEvaluatorBuffer analysisEvaBuffer,
        Object obj) {
      return null;
    }
  }
}
