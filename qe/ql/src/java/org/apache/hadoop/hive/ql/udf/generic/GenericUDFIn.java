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

import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;

@description(name = "in", value = "expr _FUNC_(expr1,expr2,expr3,...) - Returns true if expr is equal to expr1 or expr2 or ... \nNOT IN is in the contary!", extended = "Example:\n"
    + "  > SELECT * FROM src WHERE col _FUNC_(1, 3, 9);\n"
    + "  > SELECT * FROM src WHERE col NOT _FUNC_('THE', 'TDW');\n")
public class GenericUDFIn extends GenericUDF {

  int flag = 0;
  ObjectInspector[] argumentOIs;
  GenericUDFUtils.ReturnObjectInspectorResolver inputOIResolver;
  private BooleanWritable resultCache = new BooleanWritable();

  ArrayList<Boolean> needConverts = new ArrayList<Boolean>();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    this.argumentOIs = arguments;
    inputOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);

    inputOIResolver.update(arguments[0]);
    PrimitiveObjectInspector poi;
    boolean conditionTypeIsOk = (arguments[0].getCategory() == ObjectInspector.Category.PRIMITIVE);
    if (conditionTypeIsOk) {
      poi = ((PrimitiveObjectInspector) arguments[0]);
      conditionTypeIsOk = (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING
          || poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.INT || poi
          .getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.DOUBLE)
          || poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN
          || poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.FLOAT
          || poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.LONG;
    }

    if (!conditionTypeIsOk) {
      throw new UDFArgumentTypeException(
          0,
          "The expression for In should be string, boolean, float, double, bigint or int but \""
              + arguments[0].getTypeName() + "\" is found");
    } else {
      poi = ((PrimitiveObjectInspector) arguments[0]);
      needConverts.add(false);
      if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.INT) {
        flag = 1;
      } else if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.DOUBLE) {
        flag = 2;
      } else if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
        flag = 3;
      } else if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN) {
        flag = 4;
      } else if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.FLOAT) {
        flag = 5;
      } else if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.LONG) {
        flag = 6;
      } else {
        flag = 0;
      }
    }

    for (int i = 1; i < arguments.length; i++) {
      if (!inputOIResolver.update(arguments[i])) {
        throw new UDFArgumentTypeException(i, "The argument is wrong!!!");
      }
      conditionTypeIsOk = (arguments[i].getCategory() == ObjectInspector.Category.PRIMITIVE);
      if (conditionTypeIsOk) {
        poi = ((PrimitiveObjectInspector) arguments[i]);
        if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.INT) {
          if ((flag != 1) && (flag != 0) && (flag != 6)) {
            throw new UDFArgumentTypeException(i,
                "The argument is wrong(integer should not be here)!!!");
          }

          needConverts.add(true);
        } else if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.DOUBLE) {
          if ((flag != 2) && (flag != 0) && (flag != 5)) {
            throw new UDFArgumentTypeException(i,
                "The argument is wrong(double should not be here)!!!");
          }

          needConverts.add(false);
        } else if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
          if ((flag != 3) && (flag != 0)) {
            throw new UDFArgumentTypeException(i,
                "The argument is wrong(string should not be here)!!!");
          }
        } else if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.BOOLEAN) {
          if ((flag != 4) && (flag != 0)) {
            throw new UDFArgumentTypeException(i,
                "The argument is wrong(boolean should not be here)!!!");
          }
        } else if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.FLOAT) {
          if ((flag != 5) && (flag != 0) && (flag != 2)) {
            throw new UDFArgumentTypeException(i,
                "The argument is wrong(float should not be here)!!!");
          }

          needConverts.add(true);
        } else if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.LONG) {
          if ((flag != 6) && (flag != 0) && (flag != 1)) {
            throw new UDFArgumentTypeException(i,
                "The argument is wrong(bigint should not be here)!!!");
          }

          needConverts.add(false);
        } else {
          needConverts.add(false);
        }
      } else {
        throw new UDFArgumentTypeException(0,
            "The argument should be PRIMITIVE!!!");
      }
    }
    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {

    Object condition = arguments[0].get();

    BooleanWritable result = this.resultCache;

    if (condition != null) {
      if (flag == 1) {
        int re = ((IntObjectInspector) argumentOIs[0]).get(condition);

        for (int i = 1; i < arguments.length; i++) {
          if (needConverts.get(i)) {
            Object otherValue = arguments[i].get();
            if (otherValue == null) {
              continue;
            }
            int tmp = ((IntObjectInspector) argumentOIs[i]).get(otherValue);

            if (re == tmp) {
              result.set(true);
              return result;
            }
          } else {
            Object otherValue = arguments[i].get();
            if (otherValue == null) {
              continue;
            }
            long tmp = ((LongObjectInspector) argumentOIs[i]).get(otherValue);

            if (re == tmp) {
              result.set(true);
              return result;
            }
          }

        }
        result.set(false);
        return result;
      } else if (flag == 2) {
        double re = ((DoubleObjectInspector) argumentOIs[0]).get(condition);

        for (int i = 1; i < arguments.length; i++) {
          if (needConverts.get(i)) {
            Object otherValue = arguments[i].get();
            if (otherValue == null) {
              continue;
            }
            float tmp = ((FloatObjectInspector) argumentOIs[i]).get(otherValue);

            if (re == tmp) {
              result.set(true);
              return result;
            }
          } else {
            Object otherValue = arguments[i].get();
            if (otherValue == null) {
              continue;
            }
            double tmp = ((DoubleObjectInspector) argumentOIs[i])
                .get(otherValue);

            if (re == tmp) {
              result.set(true);
              return result;
            }
          }

        }
        result.set(false);
        return result;
      } else if (flag == 3) {
        String re = ((StringObjectInspector) argumentOIs[0])
            .getPrimitiveJavaObject(condition);

        for (int i = 1; i < arguments.length; i++) {
          Object otherValue = arguments[i].get();
          if (otherValue == null) {
            continue;
          }
          String tmp = ((StringObjectInspector) argumentOIs[i])
              .getPrimitiveJavaObject(otherValue);

          if (re.equals(tmp)) {
            result.set(true);
            return result;
          }
        }
        result.set(false);
        return result;
      } else if (flag == 4) {
        Boolean re = ((BooleanObjectInspector) argumentOIs[0]).get(condition);

        for (int i = 1; i < arguments.length; i++) {
          Object otherValue = arguments[i].get();
          if (otherValue == null) {
            continue;
          }
          Boolean tmp = ((BooleanObjectInspector) argumentOIs[i])
              .get(otherValue);

          if (re == tmp) {
            result.set(true);
            return result;
          }
        }
        result.set(false);
        return result;
      } else if (flag == 5) {
        float re = ((FloatObjectInspector) argumentOIs[0]).get(condition);

        for (int i = 1; i < arguments.length; i++) {
          if (needConverts.get(i)) {
            Object otherValue = arguments[i].get();
            if (otherValue == null) {
              continue;
            }
            float tmp = ((FloatObjectInspector) argumentOIs[i]).get(otherValue);

            if (re == tmp) {
              result.set(true);
              return result;
            }
          } else {
            Object otherValue = arguments[i].get();
            if (otherValue == null) {
              continue;
            }
            double tmp = ((DoubleObjectInspector) argumentOIs[i])
                .get(otherValue);

            if (re == tmp) {
              result.set(true);
              return result;
            }
          }
        }

        result.set(false);
        return result;
      } else if (flag == 6) {
        long re = ((LongObjectInspector) argumentOIs[0]).get(condition);

        for (int i = 1; i < arguments.length; i++) {
          if (needConverts.get(i)) {
            Object otherValue = arguments[i].get();
            if (otherValue == null) {
              continue;
            }
            int tmp = ((IntObjectInspector) argumentOIs[i]).get(otherValue);

            if (re == tmp) {
              result.set(true);
              return result;
            }
          } else {
            Object otherValue = arguments[i].get();
            if (otherValue == null) {
              continue;
            }
            long tmp = ((LongObjectInspector) argumentOIs[i]).get(otherValue);

            if (re == tmp) {
              result.set(true);
              return result;
            }
          }

        }
        result.set(false);
        return result;
      } else {
        return null;
      }

    } else {
      for (int i = 1; i < arguments.length; i++) {
        Object otherValue = arguments[i].get();
        if (otherValue == null) {
          result.set(true);
          return result;
        }
      }
      result.set(false);
      return result;
    }

  }

  @Override
  public String getDisplayString(String[] children) {
    int length = children.length;
    assert length >= 2;
    String result = children[1] + " is in";
    for (int i = 1; i < length; i++)
      result += " " + children[i];
    return result;
  }

}
