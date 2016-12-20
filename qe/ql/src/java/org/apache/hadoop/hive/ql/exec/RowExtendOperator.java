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

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.rowExtendDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class RowExtendOperator extends Operator<rowExtendDesc> {

  static final private Log LOG = LogFactory.getLog(RowExtendOperator.class
      .getName());

  private static final long serialVersionUID = 1L;

  transient int[] groupingOffsArray;

  transient ObjectInspector[] gbyKeyObjectInspectors;
  transient ObjectInspector[] valueObjectInspectors;

  transient protected ExprNodeEvaluator[] gbyKeyFields;
  transient protected ExprNodeEvaluator[] valueFields;

  transient Object[] cachedGbyKeys;

  transient Object[] cachedObjects;

  ObjectInspector rowInspector;
  ArrayList<Long> tagids;
  boolean[][] nulltags;

  protected void initializeOp(Configuration hconf) throws HiveException {
    rowInspector = inputObjInspectors[0];

    ArrayList<Integer> groupingOffs = conf.getGroupingColOffs();

    int gbyKeySize = conf.getGroupByKeys().size();
    int valueSize = conf.getValues().size();
    gbyKeyObjectInspectors = new ObjectInspector[gbyKeySize];
    valueObjectInspectors = new ObjectInspector[valueSize];
    ArrayList<ObjectInspector> objectInspectors = new ArrayList<ObjectInspector>();
    for (int i = 0; i < groupingOffs.size(); i++) {
      objectInspectors
          .add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
    }
    objectInspectors
        .add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
    gbyKeyFields = new ExprNodeEvaluator[gbyKeySize];
    for (int i = 0; i < gbyKeyFields.length; i++) {
      gbyKeyFields[i] = ExprNodeEvaluatorFactory.get(conf.getGroupByKeys().get(
          i));
      gbyKeyObjectInspectors[i] = gbyKeyFields[i].initialize(rowInspector);
      objectInspectors.add(gbyKeyObjectInspectors[i]);
    }

    valueFields = new ExprNodeEvaluator[valueSize];
    for (int i = 0; i < valueSize; i++) {
      valueFields[i] = ExprNodeEvaluatorFactory.get(conf.getValues().get(i));
      valueObjectInspectors[i] = valueFields[i].initialize(rowInspector);
      objectInspectors.add(valueObjectInspectors[i]);
    }

    tagids = conf.getTagids();
    System.out.println("tagids: " + tagids);

    HashMap<Integer, Integer> idx2cidx = conf.getGbkIdx2compactIdx();

    nulltags = new boolean[tagids.size()][gbyKeySize];
    for (int i = 0; i < nulltags.length; i++) {
      for (int j = 0; j < nulltags[i].length; j++) {
        nulltags[i][j] = true;
      }
    }
    for (int i = 0; i < nulltags.length; i++) {
      long tag = tagids.get(i);
      for (int j = 0; j < idx2cidx.size(); j++) {
        int j1 = idx2cidx.get(j);
        nulltags[i][j1] = nulltags[i][j1] && (((1 << j) & tag) == 0);
      }
    }

    groupingOffsArray = new int[groupingOffs.size()];
    for (int i = 0; i < groupingOffsArray.length; i++) {
      groupingOffsArray[i] = idx2cidx.get(groupingOffs.get(i));
    }

    for (int i = 0; i < nulltags.length; i++) {
      for (int j = 0; j < nulltags[i].length; j++) {
        System.out.print(nulltags[i][j] + "\t");
      }
      System.out.println();
    }

    ArrayList<String> outputColName = conf.getOutputColumnNames();

    outputObjInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(outputColName, objectInspectors);
    cachedObjects = new Object[outputColName.size()];

    cachedGbyKeys = new Object[gbyKeySize];

    initializeChildren(hconf);
  }

  public void process(Object row, int tag) throws HiveException {

    for (int i = 0; i < this.gbyKeyFields.length; i++) {
      cachedGbyKeys[i] = gbyKeyFields[i].evaluate(row);
    }

    int startpos = groupingOffsArray.length;

    for (int i = 0; i < valueFields.length; i++) {
      cachedObjects[startpos + i + gbyKeyFields.length + 1] = valueFields[i]
          .evaluate(row);
    }

    for (int i = 0; i < nulltags.length; i++) {
      for (int j = 0; j < groupingOffsArray.length; j++) {
        if (nulltags[i][groupingOffsArray[j]]) {
          cachedObjects[j] = 1;
        } else {
          cachedObjects[j] = 0;
        }
      }
      cachedObjects[startpos] = tagids.get(i);
      for (int j = 0; j < nulltags[i].length; j++) {
        cachedObjects[startpos + j + 1] = nulltags[i][j] ? null
            : cachedGbyKeys[j];
      }
      forward(cachedObjects, outputObjInspector);
    }
  }

  @Override
  public String getName() {
    return new String("ROWEXTEND");
  }
}
