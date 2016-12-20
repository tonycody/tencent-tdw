/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.unionDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ReturnObjectInspectorResolver;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class UnionOperator extends Operator<unionDesc> implements Serializable {
  private static final long serialVersionUID = 1L;

  StructObjectInspector[] parentObjInspectors;
  List<? extends StructField>[] parentFields;

  ReturnObjectInspectorResolver[] columnTypeResolvers;
  boolean[] needsTransform;

  ArrayList<Object> outputRow;

  protected void initializeOp(Configuration hconf) throws HiveException {

    int parents = parentOperators.size();
    parentObjInspectors = new StructObjectInspector[parents];
    parentFields = new List[parents];
    for (int p = 0; p < parents; p++) {
      parentObjInspectors[p] = (StructObjectInspector) inputObjInspectors[p];
      parentFields[p] = parentObjInspectors[p].getAllStructFieldRefs();
    }

    int columns = parentFields[0].size();
    ArrayList<String> columnNames = new ArrayList<String>(columns);
    for (int c = 0; c < columns; c++) {
      columnNames.add(parentFields[0].get(c).getFieldName());
    }

    columnTypeResolvers = new ReturnObjectInspectorResolver[columns];
    for (int c = 0; c < columns; c++) {
      columnTypeResolvers[c] = new ReturnObjectInspectorResolver(true);
    }

    for (int p = 0; p < parents; p++) {
      assert (parentFields[p].size() == columns);
      for (int c = 0; c < columns; c++) {
        columnTypeResolvers[c].update(parentFields[p].get(c)
            .getFieldObjectInspector());
      }
    }

    ArrayList<ObjectInspector> outputFieldOIs = new ArrayList<ObjectInspector>(
        columns);
    for (int c = 0; c < columns; c++) {
      outputFieldOIs.add(columnTypeResolvers[c].get());
    }

    outputObjInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(columnNames, outputFieldOIs);
    outputRow = new ArrayList<Object>(columns);
    for (int c = 0; c < columns; c++) {
      outputRow.add(null);
    }

    needsTransform = new boolean[parents];
    for (int p = 0; p < parents; p++) {
      needsTransform[p] = (inputObjInspectors[p] != outputObjInspector);
      if (needsTransform[p]) {
        LOG.info("Union Operator needs to transform row from parent[" + p
            + "] from " + inputObjInspectors[p] + " to " + outputObjInspector);
      }
    }
    initializeChildren(hconf);
  }

  @Override
  public synchronized void process(Object row, int tag) throws HiveException {

    StructObjectInspector soi = parentObjInspectors[tag];
    List<? extends StructField> fields = parentFields[tag];

    if (needsTransform[tag]) {
      for (int c = 0; c < fields.size(); c++) {
        outputRow.set(c, columnTypeResolvers[c].convertIfNecessary(soi
            .getStructFieldData(row, fields.get(c)), fields.get(c)
            .getFieldObjectInspector()));
      }
      forward(outputRow, outputObjInspector);
    } else {
      forward(row, inputObjInspectors[tag]);
    }
  }

  @Override
  public String getName() {
    return new String("UNION");
  }
}
