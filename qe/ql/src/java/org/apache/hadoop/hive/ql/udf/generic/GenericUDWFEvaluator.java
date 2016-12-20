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

import java.io.Serializable;

import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.persistence.AnalysisBuffer;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public abstract class GenericUDWFEvaluator implements Serializable {

  private static final long serialVersionUID = -8331603959746780944L;

  public static interface AnalysisEvaluatorBuffer {

  }

  abstract public ObjectInspector init(ObjectInspector[] parameters)
      throws HiveException;

  abstract public AnalysisEvaluatorBuffer getNewAnalysisEvaBuffer();

  abstract public void analysis(AnalysisEvaluatorBuffer analysisEvaBuffer,
      Object[] parameters);

  public static class BooleanTrans {
    private boolean value = false;

    public void set(boolean value) {
      this.value = value;
    }

    public boolean get() {
      return value;
    }
  }

  abstract public Object terminate(AnalysisEvaluatorBuffer analysisEvaBuffer,
      ExprNodeEvaluator[] analysisParameterFields,
      AnalysisBuffer<Object> analysisBuffer, int currrowid, boolean absolute,
      BooleanTrans canternimate);

  abstract public Object terminateCurrent(
      AnalysisEvaluatorBuffer analysisEvaBuffer, Object obj);

  abstract public void reset(AnalysisEvaluatorBuffer analysisEvaluatorBuffer);
}
