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

public class HashPartitioner implements Partitioner {

  ObjectInspector partKeyInspector;
  ExprNodeEvaluator partKeyEvaluator;
  int numHashPartitions;

  public HashPartitioner(ObjectInspector partKeyInspector,
      ExprNodeEvaluator partKeyEvaluator, int numHashPartitions) {
    this.partKeyInspector = partKeyInspector;
    this.partKeyEvaluator = partKeyEvaluator;
    this.numHashPartitions = numHashPartitions;
  }

  public int getPartition(Object row) throws HiveException {
    Object partKey = this.partKeyEvaluator.evaluate(row);
    partKey = ((PrimitiveObjectInspector) partKeyInspector)
        .getPrimitiveWritableObject(partKey);
    if (partKey == null) {
      return 0;
    }

    return (partKey.hashCode() & Integer.MAX_VALUE) % numHashPartitions;
  }

}
