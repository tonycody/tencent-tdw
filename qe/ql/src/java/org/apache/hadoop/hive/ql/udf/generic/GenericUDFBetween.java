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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;

public class GenericUDFBetween extends GenericUDF {

  int flag = 0;
  ObjectInspector[] argumentOIs;
  GenericUDFUtils.ReturnObjectInspectorResolver inputOIResolver;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    this.argumentOIs = arguments;
    inputOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);

    boolean r = inputOIResolver.update(arguments[0]);

    if (arguments.length != 3) {
      throw new UDFArgumentLengthException(
          "The function Between(expr1,expr2,expr3) accepts exactly 3 arguments.");
    }

    PrimitiveObjectInspector poi;
    boolean conditionTypeIsOk = (arguments[0].getCategory() == ObjectInspector.Category.PRIMITIVE);
    if (conditionTypeIsOk) {
      poi = ((PrimitiveObjectInspector) arguments[0]);
      conditionTypeIsOk = (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING
          || poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.INT || poi
          .getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.DOUBLE);
    }

    if (!conditionTypeIsOk) {
      throw new UDFArgumentTypeException(0,
          "The first argument of function Between should be string,double or int but \""
              + arguments[0].getTypeName() + "\" is found");
    } else {
      poi = ((PrimitiveObjectInspector) arguments[0]);
      if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.INT) {
        flag = 1;
      } else if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.DOUBLE) {
        flag = 2;
      } else if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
        flag = 3;
      } else {
        flag = 0;
      }
    }

    if (!(inputOIResolver.update(arguments[1]) && inputOIResolver
        .update(arguments[2]))) {
      throw new UDFArgumentTypeException(
          2,
          "The second and the third arguments of function Between should have the same type, "
              + "but they are different: \""
              + arguments[1].getTypeName()
              + "\" and \"" + arguments[2].getTypeName() + "\"");
    }

    return inputOIResolver.get();
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {

    Object condition = arguments[0].get();
    Object arg1 = arguments[1].get();
    Object arg2 = arguments[2].get();
    if (condition == null || arg1 == null || arg2 == null)
      return null;

    if (flag == 1) {

      int intWrable = ((IntObjectInspector) argumentOIs[0]).get(condition);
      int intArg1 = ((IntObjectInspector) argumentOIs[1]).get(arg1);
      int intArg2 = ((IntObjectInspector) argumentOIs[2]).get(arg2);
      if ((intWrable >= intArg1) && (intWrable <= intArg2)) {
        IntWritable re = new IntWritable(1);
        return re;
      } else {
        IntWritable re = new IntWritable(-1);
        return re;
      }
    } else if (flag == 2) {
      double doubleWrable = ((DoubleObjectInspector) argumentOIs[0])
          .get(condition);
      double doubleArg1 = ((DoubleObjectInspector) argumentOIs[1]).get(arg1);
      double doubleArg2 = ((DoubleObjectInspector) argumentOIs[2]).get(arg2);
      if ((doubleWrable >= doubleArg1) && (doubleWrable <= doubleArg2)) {
        DoubleWritable re = new DoubleWritable(1);
        return re;
      } else {
        DoubleWritable re = new DoubleWritable(-1);
        return re;
      }
    } else if (flag == 3) {
      Text tWrable = ((StringObjectInspector) argumentOIs[0])
          .getPrimitiveWritableObject(condition);
      Text tArg1 = ((StringObjectInspector) argumentOIs[1])
          .getPrimitiveWritableObject(arg1);
      Text tArg2 = ((StringObjectInspector) argumentOIs[2])
          .getPrimitiveWritableObject(arg2);
      String str0 = tWrable.toString();
      String str1 = tArg1.toString();
      String str2 = tArg2.toString();
      if ((str0.compareTo(str1) >= 0) && (str0.compareTo(str2) <= 0)) {
        Text re = new Text("1");
        return re;
      } else {
        Text re = new Text("-1");
        return re;
      }
    } else {
      return null;
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return null;
  }

}
