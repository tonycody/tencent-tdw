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

import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

public class ListPartitioner implements Partitioner {

  ObjectInspector partKeyInspector;
  ExprNodeEvaluator partKeyEvaluator;
  Map<Object, Integer> partValueSpaces;
  int defaultPart;

  public ListPartitioner(ObjectInspector partKeyInspector,
      ExprNodeEvaluator partKeyEvaluator, Map<Object, Integer> partValueSpaces,
      int defaultPartition) {
    this.partKeyInspector = partKeyInspector;
    this.partKeyEvaluator = partKeyEvaluator;
    this.partValueSpaces = partValueSpaces;
    this.defaultPart = defaultPartition;
  }

  @Override
  public int getPartition(Object row) throws HiveException {
    Object partKey = this.partKeyEvaluator.evaluate(row);
    partKey = ((PrimitiveObjectInspector) partKeyInspector)
        .getPrimitiveWritableObject(partKey);
    if (partKey == null) {
      if (defaultPart == -1)
        throw new HiveException(
            "No default partition defined to accept a null part key.");
      return defaultPart;
    }

    Integer partition = partValueSpaces.get(partKey);
    if (partition == null) {
      if (defaultPart == -1)
        throw new HiveException("No default partition defined to accept value "
            + partKey + " (class : " + partKey.getClass() + ").");
      return defaultPart;
    } else
      return partition;
  }

}
