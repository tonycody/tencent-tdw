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

package org.apache.hadoop.hive.ql.udf.generic;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.AmbiguousMethodException;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ConversionHelper;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.util.ReflectionUtils;

public class GenericUDFBridge extends GenericUDF implements Serializable {

  private static Log LOG = LogFactory.getLog(GenericUDFBridge.class.getName());

  String udfName;

  boolean isOperator;

  Class<? extends UDF> udfClass;

  public GenericUDFBridge(String udfName, boolean isOperator,
      Class<? extends UDF> udfClass) {
    this.udfName = udfName;
    this.isOperator = isOperator;
    this.udfClass = udfClass;
  }

  public GenericUDFBridge() {
  }

  public void setUdfName(String udfName) {
    this.udfName = udfName;
  }

  public String getUdfName() {
    return udfName;
  }

  public boolean isOperator() {
    return isOperator;
  }

  public void setOperator(boolean isOperator) {
    this.isOperator = isOperator;
  }

  public void setUdfClass(Class<? extends UDF> udfClass) {
    this.udfClass = udfClass;
  }

  public Class<? extends UDF> getUdfClass() {
    return udfClass;
  }

  transient Method udfMethod;

  transient ConversionHelper conversionHelper;

  transient UDF udf;

  transient Object[] realArguments;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {

    udf = (UDF) ReflectionUtils.newInstance(udfClass, null);

    ArrayList<TypeInfo> argumentTypeInfos = new ArrayList<TypeInfo>(
        arguments.length);
    for (int i = 0; i < arguments.length; i++) {
      argumentTypeInfos.add(TypeInfoUtils
          .getTypeInfoFromObjectInspector(arguments[i]));
    }
    udfMethod = udf.getResolver().getEvalMethod(argumentTypeInfos);

    conversionHelper = new ConversionHelper(udfMethod, arguments);

    realArguments = new Object[arguments.length];

    ObjectInspector returnOI = ObjectInspectorFactory
        .getReflectionObjectInspector(udfMethod.getGenericReturnType(),
            ObjectInspectorOptions.JAVA);

    return returnOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    assert (arguments.length == realArguments.length);

    for (int i = 0; i < realArguments.length; i++) {
      realArguments[i] = arguments[i].get();
    }

    Object result = FunctionRegistry.invoke(udfMethod, udf,
        conversionHelper.convertIfNecessary(realArguments));

    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    if (isOperator) {
      if (children.length == 1) {
        return "(" + udfName + " " + children[0] + ")";
      } else if (children.length == 2) {
        assert children.length == 2;
        return "(" + children[0] + " " + udfName + " " + children[1] + ")";
      }
    } else {
      StringBuilder sb = new StringBuilder();
      sb.append(udfName);
      sb.append("(");
      for (int i = 0; i < children.length; i++) {
        sb.append(children[i]);
        if (i + 1 < children.length) {
          sb.append(", ");
        }
      }
      sb.append(")");
      return sb.toString();
    }
    return udfName;
  }

}
