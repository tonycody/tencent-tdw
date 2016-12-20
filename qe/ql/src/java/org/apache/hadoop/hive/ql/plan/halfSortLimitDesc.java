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

@explain(displayName = "HalfSortLimit")
public class halfSortLimitDesc implements Serializable {
  private static final long serialVersionUID = 1L;

  private ArrayList<exprNodeDesc> sortCols;
  private String order;
  private int limit;

  public halfSortLimitDesc() {
  }

  public halfSortLimitDesc(final ArrayList<exprNodeDesc> sortCols,
      final String order, final int limit) {
    this.sortCols = sortCols;
    this.order = order;
    this.limit = limit;
  }

  public int getLimit() {
    return this.limit;
  }

  public void setLimit(final int limit) {
    this.limit = limit;
  }

  public String getOrder() {
    return this.order;
  }

  public void setOrder(final String order) {
    this.order = order;
  }

  public ArrayList<exprNodeDesc> getSortCols() {
    return sortCols;
  }

  public void setSortCols(final ArrayList<exprNodeDesc> sortCols) {
    this.sortCols = sortCols;
  }
}
