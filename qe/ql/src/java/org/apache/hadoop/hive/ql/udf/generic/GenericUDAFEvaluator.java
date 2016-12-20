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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

@UDFType(deterministic = true)
public abstract class GenericUDAFEvaluator {

  static public enum Mode {

    PARTIAL1,

    PARTIAL2,

    FINAL,

    COMPLETE
  };

  Mode mode;

  public GenericUDAFEvaluator() {
  }

  public ObjectInspector init(Mode m, ObjectInspector[] parameters)
      throws HiveException {
    mode = m;
    return null;
  }

  public static interface AggregationBuffer {
  };

  public abstract AggregationBuffer getNewAggregationBuffer()
      throws HiveException;

  public abstract void reset(AggregationBuffer agg) throws HiveException;

  public void aggregate(AggregationBuffer agg, Object[] parameters)
      throws HiveException {
    if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
      iterate(agg, parameters);
    } else {
      assert (parameters.length == 1);
      merge(agg, parameters[0]);
    }
  }

  public Object evaluate(AggregationBuffer agg) throws HiveException {
    if (mode == Mode.PARTIAL1 || mode == Mode.PARTIAL2) {
      return terminatePartial(agg);
    } else {
      return terminate(agg);
    }
  }

  public abstract void iterate(AggregationBuffer agg, Object[] parameters)
      throws HiveException;

  public abstract Object terminatePartial(AggregationBuffer agg)
      throws HiveException;

  public abstract void merge(AggregationBuffer agg, Object partial)
      throws HiveException;

  public abstract Object terminate(AggregationBuffer agg) throws HiveException;

}
