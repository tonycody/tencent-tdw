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

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.uniqueDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;

public class UniqueOperator extends Operator<uniqueDesc> implements
    Serializable {
  private static final long serialVersionUID = 1L;

  transient protected ExprNodeEvaluator[] keyFields;
  transient protected ObjectInspector[] keyObjectInspectors;
  transient protected ObjectInspector[] currentKeyObjectInspectors;
  transient protected Object[] keyObjects;

  protected transient ArrayList<ObjectInspector> objectInspectors;
  transient ArrayList<String> fieldNames;

  ObjectInspector rowInspector;
  ObjectInspector standardRowInspector;

  transient StructObjectInspector newKeyObjectInspector;
  transient StructObjectInspector currentKeyObjectInspector;

  transient protected ArrayList<Object> currentKeys;
  transient protected ArrayList<Object> newKeys;

  protected void initializeOp(Configuration hconf) throws HiveException {

    rowInspector = inputObjInspectors[0];

    standardRowInspector = ObjectInspectorUtils
        .getStandardObjectInspector(rowInspector);

    ArrayList<exprNodeDesc> fields = conf.getExprs();
    int numFields = fields.size();
    keyFields = new ExprNodeEvaluator[numFields];
    keyObjectInspectors = new ObjectInspector[numFields];
    currentKeyObjectInspectors = new ObjectInspector[numFields];
    keyObjects = new Object[numFields];

    ObjectInspector rowInspector;
    rowInspector = inputObjInspectors[0];

    for (int j = 0; j < numFields; j++) {

      keyFields[j] = ExprNodeEvaluatorFactory.get(fields.get(j));
      keyObjectInspectors[j] = keyFields[j].initialize(rowInspector);
      currentKeyObjectInspectors[j] = ObjectInspectorUtils
          .getStandardObjectInspector(keyObjectInspectors[j],
              ObjectInspectorCopyOption.WRITABLE);
      keyObjects[j] = null;
    }

    newKeys = new ArrayList<Object>(numFields);
    currentKeys = new ArrayList<Object>(numFields);
    System.out.println("xxxx numFields = " + numFields);

    fieldNames = conf.getOutputColumnNames();

    objectInspectors = new ArrayList<ObjectInspector>(keyFields.length);
    for (int j = 0; j < keyFields.length; j++) {
      objectInspectors.add(null);
      objectInspectors.set(j, currentKeyObjectInspectors[j]);
    }

    outputObjInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(fieldNames, objectInspectors);

    initializeChildren(hconf);
  }

  @Override
  public synchronized void process(Object row, int tag) throws HiveException {

    try {
      newKeys.clear();
      for (int i = 0; i < keyFields.length; i++) {
        if (keyObjectInspectors[i] == null) {
          keyObjectInspectors[i] = keyFields[i].initialize(rowInspector);
        }
        keyObjects[i] = keyFields[i].evaluate(row);
        newKeys.add(keyObjects[i]);
      }

      if (currentKeys.size() == 0) {
        forward(newKeys, outputObjInspector);
        currentKeys = new ArrayList<Object>(newKeys.size());
        deepCopyElements(newKeys.toArray(), keyObjectInspectors, currentKeys,
            ObjectInspectorCopyOption.WRITABLE);
      } else {
        boolean keysAreEqual = ObjectInspectorUtils.compare(newKeys.toArray(),
            keyObjectInspectors, currentKeys.toArray(),
            currentKeyObjectInspectors) == 0;
        if (!keysAreEqual) {
          forward(newKeys, outputObjInspector);
          deepCopyElements(newKeys.toArray(), keyObjectInspectors, currentKeys,
              ObjectInspectorCopyOption.WRITABLE);
        }
      }

    } catch (HiveException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveException(e);
    }

  }

  private static void deepCopyElements(Object[] keys,
      ObjectInspector[] keyObjectInspectors, ArrayList<Object> result,
      ObjectInspectorCopyOption copyOption) {
    result.clear();
    for (int i = 0; i < keys.length; i++) {
      result.add(ObjectInspectorUtils.copyToStandardObject(keys[i],
          keyObjectInspectors[i], copyOption));
    }
  }

  @Override
  public String getName() {
    return new String("UNIQ");
  }
}
