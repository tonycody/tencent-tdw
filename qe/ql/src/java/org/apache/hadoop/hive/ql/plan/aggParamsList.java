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
import java.util.List;

@explain(displayName = "Aggregation Parameters List")
public class aggParamsList implements Serializable {

  private static final long serialVersionUID = 1L;

  private List<aggregationDesc> aggParams;

  public aggParamsList() {
  }

  public aggParamsList(final List<aggregationDesc> aggParams) {
    this.aggParams = aggParams;
  }

  public void setAggParams(List<aggregationDesc> aggParams) {
    this.aggParams = aggParams;
  }

  @explain(displayName = "Aggregation Parameters")
  public List<aggregationDesc> getAggParams() {
    return this.aggParams;
  }

}
