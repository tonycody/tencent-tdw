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
package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.MapOperator.Counter;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.LocalSelectDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.selectDesc;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.LongWritable;

public class LocalSelectOperator extends Operator<LocalSelectDesc> implements
    Serializable {

  private static final long serialVersionUID = 1L;
  transient protected ExprNodeEvaluator[] eval;
  transient Object[] output;

  protected void initializeOp(Configuration hconf) throws HiveException {
    ArrayList<exprNodeDesc> colList = conf.getColList();
    eval = new ExprNodeEvaluator[colList.size()];

    for (int i = 0; i < colList.size(); i++) {
      assert (colList.get(i) != null);
      eval[i] = ExprNodeEvaluatorFactory.get(colList.get(i));
    }

    output = new Object[eval.length];

    outputObjInspector = initEvaluatorsAndReturnStruct(eval,
        conf.getOutputColumnNames(), inputObjInspectors[0]);

    initializeChildren(hconf);
  }

  public void process(Object row, int tag) throws HiveException {
    for (int i = 0; i < eval.length; i++) {
      try {
        output[i] = eval[i].evaluate(row);
      } catch (HiveException e) {
        throw new RuntimeException(e.getMessage());
      } catch (RuntimeException e) {
        throw new HiveException("Error evaluating "
            + conf.getColList().get(i).getExprString(), e);
      }
    }

    forward(output, outputObjInspector);
  }

  @Override
  public String getName() {
    return new String("LOCALSEL");
  }

}
