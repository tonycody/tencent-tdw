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

import java.io.IOException;
import java.io.Serializable;
import java.util.Comparator;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.halfSortLimitDesc;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HalfSortLimitOperator extends Operator<halfSortLimitDesc>
    implements Serializable {

  static final private Log LOG = LogFactory.getLog(HalfSortLimitOperator.class
      .getName());

  private static final long serialVersionUID = 1L;

  class RowWrapper {
    private Object row;
    private ArrayList<ExprNodeEvaluator> keyFields;
    private ArrayList<ObjectInspector> keyInspectors;

    public RowWrapper(Object row, ArrayList<ExprNodeEvaluator> keyFields,
        ArrayList<ObjectInspector> keyInspectors) {
      this.row = row;
      this.keyFields = keyFields;
      this.keyInspectors = keyInspectors;
    }

    public Object getRow() {
      return row;
    }

    public ArrayList<ExprNodeEvaluator> getKeyFields() {
      return keyFields;
    }

    public ExprNodeEvaluator getKeyField(int i) {
      return keyFields.get(i);
    }

    public ArrayList<ObjectInspector> getKeyInspectors() {
      return keyInspectors;
    }

    public ObjectInspector getKeyInspector(int i) {
      return keyInspectors.get(i);
    }
  };

  class RowComparator implements Comparator {
    private boolean[] orders;

    public RowComparator(final String order) throws HiveException {
      orders = new boolean[order.length()];
      for (int i = 0; i < order.length(); i++) {
        orders[i] = order.charAt(i) == '+' ? true : false;
      }
    }

    public int compare(Object row1, Object row2) {
      try {
        RowWrapper r1 = (RowWrapper) row1;
        RowWrapper r2 = (RowWrapper) row2;
        assert r1.getKeyFields().size() == r2.getKeyFields().size();
        assert r1.getKeyFields().size() == orders.length;

        int keyCount = r1.getKeyFields().size();
        for (int i = 0; i < keyCount; i++) {
          Object key1 = r1.getKeyField(i).evaluate(r1.getRow());
          Object key2 = r2.getKeyField(i).evaluate(r2.getRow());

          int r = ObjectInspectorUtils.compare(key1, r1.getKeyInspector(i),
              key2, r2.getKeyInspector(i));
          if (r != 0) {
            if (orders[i] == false) {
              return -r;
            } else {
              return r;
            }
          }
        }
        return 0;
      } catch (Throwable e) {
        e.printStackTrace();
        LOG.error("Exception in RowComparator.compare()" + e.toString());
        return 0;
      }
    }
  };

  transient private ArrayList<ExprNodeEvaluator> originKeyFields;
  transient private ArrayList<ObjectInspector> originKeyInspectors;

  transient private ArrayList<ExprNodeEvaluator> newKeyFields;
  transient private ArrayList<ObjectInspector> newKeyInspectors;

  transient private RowComparator comp;
  transient private Heap<RowWrapper> heap;

  transient private int processRows;
  transient private int heartbeatInterval;

  protected void initializeOp(Configuration hconf) throws HiveException {
    try {
      ObjectInspector rowInspector = inputObjInspectors[0];
      outputObjInspector = ObjectInspectorUtils.getStandardObjectInspector(
          rowInspector, ObjectInspectorCopyOption.DEFAULT);

      originKeyFields = new ArrayList<ExprNodeEvaluator>();
      originKeyInspectors = new ArrayList<ObjectInspector>();
      newKeyFields = new ArrayList<ExprNodeEvaluator>();
      newKeyInspectors = new ArrayList<ObjectInspector>();

      for (int i = 0; i < conf.getSortCols().size(); i++) {
        originKeyFields.add(ExprNodeEvaluatorFactory.get(conf.getSortCols()
            .get(i)));
        originKeyInspectors
            .add(originKeyFields.get(i).initialize(rowInspector));
        newKeyFields.add(ExprNodeEvaluatorFactory
            .get(conf.getSortCols().get(i)));
        newKeyInspectors
            .add(newKeyFields.get(i).initialize(outputObjInspector));
      }

      comp = new RowComparator(conf.getOrder());
      heap = new Heap<RowWrapper>(comp);

      heartbeatInterval = HiveConf.getIntVar(hconf,
          HiveConf.ConfVars.HIVESENDHEARTBEAT);
      processRows = 0;

      initializeChildren(hconf);
    } catch (Throwable e) {
      throw new HiveException(e);
    }
  }

  public void process(Object row, int tag) throws HiveException {
    if (heap.size() < conf.getLimit()) {
      ObjectInspector rowInspector = inputObjInspectors[tag];
      Object o = ObjectInspectorUtils.copyToStandardObject(row, rowInspector);
      RowWrapper newRow = new RowWrapper(o, newKeyFields, newKeyInspectors);
      heap.add(newRow);
    } else {
      RowWrapper thisRow = new RowWrapper(row, originKeyFields,
          originKeyInspectors);
      if (comp.compare(thisRow, heap.get(0)) < 0) {
        ObjectInspector rowInspector = inputObjInspectors[tag];
        Object o = ObjectInspectorUtils.copyToStandardObject(row, rowInspector);
        RowWrapper newRow = new RowWrapper(o, newKeyFields, newKeyInspectors);
        heap.remove(0);
        heap.add(newRow);
      }
    }

    if ((((++processRows) % heartbeatInterval) == 0) && (reporter != null)) {
      reporter.progress();
    }
  }

  public void closeOp(boolean abort) throws HiveException {
    if (!abort) {
      heap.sort();
      for (int i = 0; i < heap.size(); i++) {
        forward(heap.get(i).getRow(), outputObjInspector);
      }
    }
    heap.clear();
  }

  public String getName() {
    return "HSL";
  }
}
