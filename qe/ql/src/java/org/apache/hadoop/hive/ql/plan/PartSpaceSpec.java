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
package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

@explain(displayName = "Partition Space Spec")
public class PartSpaceSpec implements Serializable {

  private static final long serialVersionUID = 1L;
  private Map<String, PartValuesList> partSpace;

  public PartSpaceSpec() {
  }

  public PartSpaceSpec(final Map<String, PartValuesList> partSpace) {
    this.partSpace = partSpace;
  }

  @explain(displayName = "Partition space")
  public Map<String, PartValuesList> getPartSpace() {
    return this.partSpace;
  }

  public void setPartSpace(final Map<String, PartValuesList> partSpace) {
    this.partSpace = partSpace;
  }

  public static PartSpaceSpec convertToPartSpaceSpec(
      Map<String, List<String>> partSpaces) {
    Map<String, PartValuesList> newPartSpaces = new LinkedHashMap<String, PartValuesList>();

    for (Entry<String, List<String>> entry : partSpaces.entrySet()) {
      ArrayList<String> values = new ArrayList<String>();
      values.addAll(entry.getValue());
      PartValuesList pvList = new PartValuesList(values);
      newPartSpaces.put(entry.getKey(), pvList);
    }
    return new PartSpaceSpec(newPartSpaces);
  }

}
