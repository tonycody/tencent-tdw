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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.VoidObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.LongWritable;

public class GenericUDFWhen extends GenericUDF {

  private static Log LOG = LogFactory.getLog(GenericUDFWhen.class.getName());

  ObjectInspector[] argumentOIs;
  GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
  int isintorbigint = -1;
  int isfindthetype = -1;
  int iscontainbigint = -1;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentTypeException {

    this.argumentOIs = arguments;
    returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver();

    if (arguments.length < 2) {
      throw new UDFArgumentTypeException(0,
          "The argument number is not enough!");
    }
    if (arguments.length > 255) {
      throw new UDFArgumentTypeException(0, "The argument number is too many!");
    }

    if (testint(arguments[1])) {
      isfindthetype = 1;
      isintorbigint = 1;
      if (testbigint(arguments[1])) {
        iscontainbigint = 1;
      } else {
        iscontainbigint = 0;
      }
    } else if (testnull(arguments[1])) {
      isfindthetype = 0;
    } else {
      isfindthetype = 1;
      isintorbigint = 0;
      iscontainbigint = 0;
    }

    for (int i = 0; i + 1 < arguments.length; i += 2) {
      if (!arguments[i].getTypeName().equals(Constants.BOOLEAN_TYPE_NAME)) {
        throw new UDFArgumentTypeException(i, "\""
            + Constants.BOOLEAN_TYPE_NAME + "\" is expected after WHEN, "
            + "but \"" + arguments[i].getTypeName() + "\" is found");
      }
      if (!returnOIResolver.update(arguments[i + 1])) {
        if ((isintorbigint == 1) && testint(arguments[i + 1])) {
          if (testbigint(arguments[i + 1])) {
            iscontainbigint = 1;
          }
        } else {
          throw new UDFArgumentTypeException(i + 1,
              "The expressions after THEN should have the same type: \""
                  + returnOIResolver.get().getTypeName()
                  + "\" is expected but \"" + arguments[i + 1].getTypeName()
                  + "\" is found");
        }
      } else {
        if ((!testnull(arguments[i + 1])) && (isfindthetype == 0)) {
          if (testint(arguments[i + 1])) {
            isfindthetype = i + 1;
            isintorbigint = 1;
            if (testbigint(arguments[i + 1])) {
              iscontainbigint = 1;
            }
          } else {
            isfindthetype = i + 1;
            isintorbigint = 0;
          }
        }
      }
    }
    if (arguments.length % 2 == 1) {
      int i = arguments.length - 2;
      if (!returnOIResolver.update(arguments[i + 1])) {
        if ((isintorbigint == 1) && testint(arguments[i + 1])) {
          if (testbigint(arguments[i + 1])) {
            iscontainbigint = 1;
          }
        } else {
          throw new UDFArgumentTypeException(i + 1,
              "The expression after ELSE should have the same type as those after THEN: \""
                  + returnOIResolver.get().getTypeName()
                  + "\" is expected but \"" + arguments[i + 1].getTypeName()
                  + "\" is found");
        }

      } else {
        if ((!testnull(arguments[i + 1])) && (isfindthetype == 0)) {
          if (testint(arguments[i + 1])) {
            isfindthetype = i + 1;
            isintorbigint = 1;
            if (testbigint(arguments[i + 1])) {
              iscontainbigint = 1;
            }
          } else {
            isfindthetype = i + 1;
            isintorbigint = 0;
          }
        }
      }
    }

    if (isfindthetype == 0) {
      throw new UDFArgumentTypeException(0,
          "The arguments can not be all null!");
    }

    if (iscontainbigint == 1) {
      return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    }
    return returnOIResolver.get();
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {

    if (iscontainbigint == 1) {
      for (int i = 0; i + 1 < arguments.length; i += 2) {
        Object caseKey = arguments[i].get();
        if (caseKey != null
            && ((BooleanObjectInspector) argumentOIs[i]).get(caseKey)) {
          LongWritable tmpexprValue = myconvert(arguments[i + 1],
              argumentOIs[i + 1]);
          return tmpexprValue;
        }
      }
      if (arguments.length % 2 == 1) {
        int i = arguments.length - 2;
        LongWritable tmpexprValue = myconvert(arguments[i + 1],
            argumentOIs[i + 1]);
        return tmpexprValue;
      }
    } else {
      for (int i = 0; i + 1 < arguments.length; i += 2) {
        Object caseKey = arguments[i].get();
        if (caseKey != null
            && ((BooleanObjectInspector) argumentOIs[i]).get(caseKey)) {
          Object caseValue = arguments[i + 1].get();
          return returnOIResolver.convertIfNecessary(caseValue,
              argumentOIs[i + 1]);
        }
      }
      if (arguments.length % 2 == 1) {
        int i = arguments.length - 2;
        Object elseValue = arguments[i + 1].get();
        return returnOIResolver.convertIfNecessary(elseValue,
            argumentOIs[i + 1]);
      }
    }

    return null;
  }

  private boolean testint(ObjectInspector iii) {
    if (iii.getCategory() != ObjectInspector.Category.PRIMITIVE) {
      return false;
    }
    PrimitiveObjectInspector poi = ((PrimitiveObjectInspector) iii);
    if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.INT
        || poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.LONG) {
      return true;
    } else {
      return false;
    }
  }

  private boolean testbigint(ObjectInspector iii) {
    if (iii.getCategory() != ObjectInspector.Category.PRIMITIVE) {
      return false;
    }
    PrimitiveObjectInspector poi = ((PrimitiveObjectInspector) iii);
    if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.LONG) {
      return true;
    } else {
      return false;
    }
  }

  private boolean testnull(ObjectInspector iii) {
    if (iii.getCategory() != ObjectInspector.Category.PRIMITIVE) {
      return false;
    }
    PrimitiveObjectInspector poi = ((PrimitiveObjectInspector) iii);
    if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.VOID) {
      return true;
    } else {
      return false;
    }
  }

  private LongWritable myconvert(DeferredObject input, ObjectInspector oi)
      throws HiveException {
    if (input.get() == null) {
      return null;
    }
    PrimitiveObjectInspector poi = ((PrimitiveObjectInspector) oi);
    if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.INT) {
      int re = ((IntObjectInspector) oi).get(input.get());
      return new LongWritable((long) re);
    } else if (poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.LONG) {
      long tmp = ((LongObjectInspector) oi).get(input.get());
      return new LongWritable(tmp);
    } else {
      return null;
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length >= 2);
    StringBuilder sb = new StringBuilder();
    sb.append("CASE");
    for (int i = 0; i + 1 < children.length; i += 2) {
      sb.append(" WHEN (");
      sb.append(children[i]);
      sb.append(") THEN (");
      sb.append(children[i + 1]);
      sb.append(")");
    }
    if (children.length % 2 == 1) {
      sb.append(" ELSE (");
      sb.append(children[children.length - 1]);
      sb.append(")");
    }
    sb.append(" END");
    return sb.toString();
  }

}
