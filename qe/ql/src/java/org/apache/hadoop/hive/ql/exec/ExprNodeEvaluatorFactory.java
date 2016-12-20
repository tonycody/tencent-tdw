/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeNullDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeFieldDesc;

public class ExprNodeEvaluatorFactory {

  public ExprNodeEvaluatorFactory() {
  }

  public static ExprNodeEvaluator get(exprNodeDesc desc) {
    if (desc instanceof exprNodeConstantDesc) {
      return new ExprNodeConstantEvaluator((exprNodeConstantDesc) desc);
    }
    if (desc instanceof exprNodeColumnDesc) {
      return new ExprNodeColumnEvaluator((exprNodeColumnDesc) desc);
    }

    if (desc instanceof exprNodeGenericFuncDesc) {
      return new ExprNodeGenericFuncEvaluator((exprNodeGenericFuncDesc) desc);
    }
    if (desc instanceof exprNodeFieldDesc) {
      return new ExprNodeFieldEvaluator((exprNodeFieldDesc) desc);
    }
    if (desc instanceof exprNodeNullDesc) {
      return new ExprNodeNullEvaluator((exprNodeNullDesc) desc);
    }

    throw new RuntimeException(
        "Cannot find ExprNodeEvaluator for the exprNodeDesc = " + desc);
  }
}
