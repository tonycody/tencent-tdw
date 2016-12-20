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

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.lateralViewJoinDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

public class LateralViewJoinOperator extends Operator<lateralViewJoinDesc> {

  private static final long serialVersionUID = 1L;

  static final int SELECT_TAG = 0;
  static final int UDTF_TAG = 1;

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {

    ArrayList<ObjectInspector> ois = new ArrayList<ObjectInspector>();
    ArrayList<String> fieldNames = conf.getOutputInternalColNames();

    StructObjectInspector soi = (StructObjectInspector) inputObjInspectors[SELECT_TAG];
    List<? extends StructField> sfs = soi.getAllStructFieldRefs();
    for (StructField sf : sfs) {
      ois.add(sf.getFieldObjectInspector());
    }

    soi = (StructObjectInspector) inputObjInspectors[UDTF_TAG];
    sfs = soi.getAllStructFieldRefs();
    for (StructField sf : sfs) {
      ois.add(sf.getFieldObjectInspector());
    }

    outputObjInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(fieldNames, ois);

    super.initializeOp(hconf);
  }

  ArrayList<Object> acc = new ArrayList<Object>();
  ArrayList<Object> selectObjs = new ArrayList<Object>();

  @Override
  public void process(Object row, int tag) throws HiveException {
    StructObjectInspector soi = (StructObjectInspector) inputObjInspectors[tag];
    if (tag == SELECT_TAG) {
      selectObjs.clear();
      selectObjs.addAll(soi.getStructFieldsDataAsList(row));
    } else if (tag == UDTF_TAG) {
      acc.clear();
      acc.addAll(selectObjs);
      acc.addAll(soi.getStructFieldsDataAsList(row));
      forward(acc, outputObjInspector);
    } else {
      throw new HiveException("Invalid tag");
    }

  }

  public String getName() {
    return "LVJ";
  }

}
