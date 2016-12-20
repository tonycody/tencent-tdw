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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

public class GenericUDWRank implements GenericUDWFResolver {

  @Override
  public GenericUDWFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    return new GenericUDWFRankEvaluator();
  }

  public static class GenericUDWFRankEvaluator extends GenericUDWFEvaluator {

    private static final long serialVersionUID = -7704613724101321440L;
    ObjectInspector[] inputOI;
    LongWritable result;

    public ObjectInspector init(ObjectInspector[] parameters)
        throws HiveException {
      if (parameters != null && parameters.length > 0)
        inputOI = parameters;
      result = new LongWritable(0);
      return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    }

    static class RankAgg implements AnalysisEvaluatorBuffer {
      Object[] lastobj = null;
      long rowid;
      long rank;
      boolean firstrow = true;
    }

    public AnalysisEvaluatorBuffer getNewAnalysisEvaBuffer() {
      RankAgg agg = new RankAgg();
      reset(agg);
      return agg;
    }

    public void analysis(AnalysisEvaluatorBuffer analysisEvaBuffer,
        Object[] parameters) {
    }

    public Object terminate(AnalysisEvaluatorBuffer analysisEvaBuffer,
        ExprNodeEvaluator[] analysisParameterFields,
        AnalysisBuffer<Object> analysisBuffer, int currrowid, boolean absolute,
        BooleanTrans canternimate) {
      canternimate.set(true);
      RankAgg agg = (RankAgg) analysisEvaBuffer;
      if (agg.rowid == currrowid) {
        result.set(agg.rank);
        return result;
      }
      agg.rowid = currrowid;
      Object row = analysisBuffer.getByRowid(currrowid);
      ArrayList<Object> rowfull = ((ArrayList<Object>) row);
      row = rowfull.get(0);

      if (agg.firstrow) {
        agg.firstrow = false;
        agg.lastobj = new Object[analysisParameterFields.length];
        try {
          for (int i = 0; i < agg.lastobj.length; i++) {
            agg.lastobj[i] = analysisParameterFields[i].evaluate(row);
          }
        } catch (HiveException e) {
          e.printStackTrace();
        }
        agg.rank = agg.rowid + 1;
      } else {
        try {
          boolean newobj = false;
          Object[] objs = new Object[analysisParameterFields.length];
          for (int i = 0; i < objs.length; i++) {
            Object obj = analysisParameterFields[i].evaluate(row);
            if (ObjectInspectorUtils.compare(agg.lastobj[i], inputOI[i], obj,
                inputOI[i]) != 0) {
              newobj = true;
            }
            objs[i] = obj;
          }
          if (newobj) {
            agg.lastobj = objs;
            agg.rank = agg.rowid + 1;
          }
        } catch (HiveException e) {
          e.printStackTrace();
        }
      }
      result.set(agg.rank);
      return result;
    }

    public void reset(AnalysisEvaluatorBuffer analysisEvaBuffer) {
      RankAgg agg = (RankAgg) analysisEvaBuffer;
      agg.lastobj = null;
      agg.rowid = -1;
      agg.rank = 0;
      agg.firstrow = true;
    }

    @Override
    public Object terminateCurrent(AnalysisEvaluatorBuffer analysisEvaBuffer,
        Object obj) {
      return null;
    }
  }
}
