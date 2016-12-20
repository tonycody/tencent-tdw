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
package IndexService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

@SuppressWarnings("deprecation")
public class IndexMergePartitioner<K2, V2> implements
    Partitioner<IndexKey, IndexValue> {
  public static final Log LOG = LogFactory.getLog(IndexMergePartitioner.class);

  @Override
  public int getPartition(IndexKey key, IndexValue value, int numPartitions) {
    return (key.getfvs().get(0).hashCode() & Integer.MAX_VALUE) % numPartitions;
  }

  @Override
  public void configure(JobConf job) {

  }

}
