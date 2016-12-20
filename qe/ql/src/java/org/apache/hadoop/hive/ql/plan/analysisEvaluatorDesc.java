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

import org.apache.hadoop.hive.ql.udf.generic.GenericUDWFEvaluator;
import org.apache.hadoop.util.ReflectionUtils;

public class analysisEvaluatorDesc implements java.io.Serializable {
  private static final long serialVersionUID = 1L;
  private String genericUDWFName = null;
  private Class<? extends GenericUDWFEvaluator> genericUDWFEvaluatorClass = null;
  private GenericUDWFEvaluator genericUDWFEvaluator = null;
  private java.util.ArrayList<exprNodeDesc> parameters = null;
  private java.util.ArrayList<exprNodeDesc> partitionBys = null;
  private java.util.ArrayList<exprNodeDesc> orderBys = null;
  private boolean distinct = false;
  private boolean isOrderBy = false;

  public analysisEvaluatorDesc() {
  }

  public analysisEvaluatorDesc(final String genericUDWFName,
      final GenericUDWFEvaluator genericUDWFEvaluator,
      final java.util.ArrayList<exprNodeDesc> parameters,
      final java.util.ArrayList<exprNodeDesc> partitionBys,
      final boolean distinct, final java.util.ArrayList<exprNodeDesc> orderBys,
      boolean isOrderBy) {
    this.genericUDWFName = genericUDWFName;
    if (genericUDWFEvaluator instanceof Serializable) {
      this.genericUDWFEvaluator = genericUDWFEvaluator;
      this.genericUDWFEvaluatorClass = null;
    } else {
      this.genericUDWFEvaluator = null;
      this.genericUDWFEvaluatorClass = genericUDWFEvaluator.getClass();
    }
    this.parameters = parameters;
    this.partitionBys = partitionBys;
    this.distinct = distinct;
    this.orderBys = orderBys;
    this.isOrderBy = isOrderBy;
  }

  public GenericUDWFEvaluator createGenericUDAFEvaluator() {
    return (genericUDWFEvaluator != null) ? genericUDWFEvaluator
        : (GenericUDWFEvaluator) ReflectionUtils.newInstance(
            genericUDWFEvaluatorClass, null);
  }

  public void setGenericUDWFName(final String genericUDWFName) {
    this.genericUDWFName = genericUDWFName;
  }

  public String getGenericUDWFName() {
    return genericUDWFName;
  }

  public void setGenericUDWFEvaluator(
      final GenericUDWFEvaluator genericUDWFEvaluator) {
    this.genericUDWFEvaluator = genericUDWFEvaluator;
  }

  public GenericUDWFEvaluator getGenericUDWFEvaluator() {
    return genericUDWFEvaluator;
  }

  public void setGenericUDWFEvaluatorClass(
      final Class<? extends GenericUDWFEvaluator> genericUDWFEvaluatorClass) {
    this.genericUDWFEvaluatorClass = genericUDWFEvaluatorClass;
  }

  public Class<? extends GenericUDWFEvaluator> getGenericUDWFEvaluatorClass() {
    return genericUDWFEvaluatorClass;
  }

  public java.util.ArrayList<exprNodeDesc> getParameters() {
    return this.parameters;
  }

  public void setParameters(final java.util.ArrayList<exprNodeDesc> parameters) {
    this.parameters = parameters;
  }

  public java.util.ArrayList<exprNodeDesc> getPartitionBys() {
    return this.partitionBys;
  }

  public void setPartitionBys(
      final java.util.ArrayList<exprNodeDesc> partitionBys) {
    this.partitionBys = partitionBys;
  }

  public java.util.ArrayList<exprNodeDesc> getOrderBys() {
    return this.orderBys;
  }

  public void setOrderBys(final java.util.ArrayList<exprNodeDesc> orderBys) {
    this.orderBys = orderBys;
  }

  public boolean isOrderBy() {
    return isOrderBy;
  }

  public void setOrderBy(boolean isOrderBy) {
    this.isOrderBy = isOrderBy;
  }

  public boolean getDistinct() {
    return this.distinct;
  }

  public void setDistinct(final boolean distinct) {
    this.distinct = distinct;
  }

  @explain(displayName = "expr")
  public String getExprString() {
    StringBuilder sb = new StringBuilder();
    sb.append(genericUDWFName);
    sb.append("(");
    if (distinct) {
      sb.append("DISTINCT ");
    }
    boolean first = true;
    if (parameters != null) {
      for (exprNodeDesc exp : parameters) {
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append(exp.getExprString());
      }
    }

    sb.append(")");
    sb.append(" OVER (");

    first = true;
    if (partitionBys != null) {
      for (exprNodeDesc exp : partitionBys) {
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append(exp.getExprString());
      }
      sb.append(" ");
    }

    first = true;
    if (orderBys != null) {
      for (exprNodeDesc exp : orderBys) {
        if (first) {
          first = false;
        } else {
          sb.append(", ");
        }
        sb.append(exp.getExprString());
      }
      sb.append(" ");
    }

    sb.append(")");
    return sb.toString();
  }

  public boolean hasAggregateOrderBy() {
    String name = this.genericUDWFName.toLowerCase();
    if (isOrderBy
        && (name.contains("sum") || name.contains("count")
            || name.contains("max") || name.contains("min")
            || name.contains("avg") || name.contains("ratio_to_report"))) {
      return true;
    }
    return false;
  }

}
