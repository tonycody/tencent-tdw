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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.io.IntWritable;

public class RangePartitioner implements Partitioner {

  transient protected Log LOG = LogFactory.getLog(this.getClass().getName());
  ExprNodeEvaluator[] partValueSpaces;
  int defaultPart;
  
  ExprNodeEvaluator partKeyEvaluator;
  Map<Object, Integer> partIndexMap = new HashMap<Object, Integer>();
  private ObjectInspector partKeyInspector;
  int maxCacheSize = 0;

  public RangePartitioner(ExprNodeEvaluator[] partValueSpaces, 
      int defaultPart, ExprNodeEvaluator partKeyEvaluator, int maxCacheSize) {
    this.partValueSpaces = partValueSpaces;
    this.defaultPart = defaultPart;
    this.partKeyEvaluator = partKeyEvaluator;
    this.maxCacheSize = maxCacheSize;
  }
  
  public RangePartitioner(ObjectInspector partKeyInspector,
      ExprNodeEvaluator partKeyEvaluator, ExprNodeEvaluator[] partValueSpaces,
      int defaultPartition, int maxCacheSize) {
    this.partKeyInspector = partKeyInspector;
    this.partKeyEvaluator = partKeyEvaluator;
    this.partValueSpaces = partValueSpaces;
    this.defaultPart = defaultPartition;
    this.maxCacheSize = maxCacheSize;
  }

  public int getPartition(Object row) throws HiveException {
    Object partKey = this.partKeyEvaluator.evaluate(row);
    partKey = ((PrimitiveObjectInspector) partKeyInspector)
        .getPrimitiveWritableObject(partKey);
    
    if (partKey instanceof DoubleWritable) {
      partKey = ((DoubleWritable) partKey).get();
    }
    if (partIndexMap.containsKey(partKey)){
      return partIndexMap.get(partKey);
    }
    
    int low = 0;
    int high = defaultPart < 0 ? partValueSpaces.length - 1
        : partValueSpaces.length - 2;

    while (low <= high) {
      int mid = (low + high) >>> 1;

      IntWritable cmpWritable = (IntWritable) partValueSpaces[mid]
          .evaluate(row);
      if (cmpWritable == null) {
        if (defaultPart == -1)
          throw new HiveException(
              "No default partition defined to accept a null part key.");
        return defaultPart;
      }

      int cmp = cmpWritable.get();
      if (cmp < 0)
        low = mid + 1;
      else if (cmp > 0)
        high = mid - 1;
      else{
        low = mid + 1;
        break;
      }       
    }

    if(partIndexMap.size() < maxCacheSize){
      partIndexMap.put(partKey, low);
    }
    else{
      //LOG.info("XXXXpart index cache size over:" + maxCacheSize);
    }
    
    // no default partition and no matched partition
    if (defaultPart == -1) {
      if (low > (partValueSpaces.length - 1))
        throw new HiveException("No default partition defined to accept value "
            + partKey + " (class : " + partKey.getClass() + ").");
    }
    
    return low;
  }

}
