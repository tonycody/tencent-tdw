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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.udtfDesc;
import org.apache.hadoop.hive.ql.udf.generic.UDTFCollector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;

public class UDTFOperator extends Operator<udtfDesc> implements Serializable {
  private static final long serialVersionUID = 1L;

  protected final Log LOG = LogFactory.getLog(this.getClass().getName());

  ObjectInspector[] udtfInputOIs = null;
  Object[] objToSendToUDTF = null;
  Object[] forwardObj = new Object[1];
  boolean copyToStandard = false;

  transient AutoProgressor autoProgressor;
  transient boolean closeCalled = false;

  protected void initializeOp(Configuration hconf) throws HiveException {
    conf.getGenericUDTF().setCollector(new UDTFCollector(this));
    List<? extends StructField> inputFields = ((StandardStructObjectInspector) inputObjInspectors[0])
        .getAllStructFieldRefs();

    udtfInputOIs = new ObjectInspector[inputFields.size()];
    for (int i = 0; i < inputFields.size(); i++) {
      udtfInputOIs[i] = inputFields.get(i).getFieldObjectInspector();
    }
    objToSendToUDTF = new Object[inputFields.size()];
    StructObjectInspector udtfOutputOI = conf.getGenericUDTF().initialize(
        udtfInputOIs);

    if ((conf.getGenericUDTF().toString().equalsIgnoreCase("explode"))
        && udtfInputOIs.length == 2) {
      copyToStandard = true;
    }

    outputObjInspector = udtfOutputOI;

    if (HiveConf.getBoolVar(hconf, HiveConf.ConfVars.HIVEUDTFAUTOPROGRESS)) {
      autoProgressor = new AutoProgressor(this.getClass().getName(), reporter,
          Utilities.getDefaultNotificationInterval(hconf));
      autoProgressor.go();
    }

    super.initializeOp(hconf);
  }

  public void process(Object row, int tag) throws HiveException {
    StandardStructObjectInspector soi = (StandardStructObjectInspector) inputObjInspectors[tag];
    List<? extends StructField> fields = soi.getAllStructFieldRefs();

    for (int i = 0; i < fields.size(); i++) {
      if (!copyToStandard)
        objToSendToUDTF[i] = soi.getStructFieldData(row, fields.get(i));
      else
        objToSendToUDTF[i] = ObjectInspectorUtils.copyToStandardObject(soi
            .getStructFieldData(row, fields.get(i)), fields.get(i)
            .getFieldObjectInspector(), ObjectInspectorCopyOption.WRITABLE);
    }

    conf.getGenericUDTF().process(objToSendToUDTF);

  }

  public void forwardUDTFOutput(Object o) throws HiveException {
    forward(o, outputObjInspector);
  }

  public String getName() {
    return "UDTF";
  }

  protected void closeOp(boolean abort) throws HiveException {
    conf.getGenericUDTF().close();
  }
}
