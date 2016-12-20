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
package org.apache.hadoop.hive.ql.metadata;

import java.util.Comparator;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;

public class PartitionComparator implements Comparator<RangePartitionItem> {
  ObjectInspectorConverters.Converter converter = null;
  ObjectInspectorConverters.Converter converter1 = null;
  ObjectInspectorConverters.Converter converter2 = null;

  public PartitionComparator() {
    super();
  }

  public PartitionComparator(ObjectInspectorConverters.Converter conv) {
    converter = conv;
  }

  public PartitionComparator(ObjectInspectorConverters.Converter conv1,
      ObjectInspectorConverters.Converter conv2) {
    converter1 = conv1;
    converter2 = conv2;
  }

  @Override
  public int compare(RangePartitionItem arg0, RangePartitionItem arg1) {
    return ((Comparable) converter1.convert(arg0.value))
        .compareTo((Comparable) converter2.convert(arg1.value));
  }

}
