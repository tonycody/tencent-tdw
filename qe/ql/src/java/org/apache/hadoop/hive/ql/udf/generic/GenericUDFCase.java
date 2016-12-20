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

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.LongWritable;

public class GenericUDFCase extends GenericUDF {
  private ObjectInspector[] argumentOIs;
  private GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
  private GenericUDFUtils.ReturnObjectInspectorResolver caseOIResolver;

  int isintorbigint_case = -1;
  int isfindthetype_case = -1;
  int iscontainbigint_case = -1;
  int isintorbigint_return = -1;
  int isfindthetype_return = -1;
  int iscontainbigint_return = -1;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentTypeException {

    argumentOIs = arguments;
    caseOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver();
    returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver();

    if (arguments.length < 3) {
      throw new UDFArgumentTypeException(0,
          "The argument number is not enough!");
    }
    if (arguments.length > 255) {
      throw new UDFArgumentTypeException(0, "The argument number is too many!");
    }

    if (testint(arguments[0])) {
      isfindthetype_case = 0;
      isintorbigint_case = 1;
      if (testbigint(arguments[0])) {
        iscontainbigint_case = 1;
      } else {
        iscontainbigint_case = 0;
      }
    } else if (testnull(arguments[0])) {
    } else {
      isfindthetype_case = 0;
      isintorbigint_case = 0;
    }

    if (testint(arguments[2])) {
      isfindthetype_return = 2;
      isintorbigint_return = 1;
      if (testbigint(arguments[2])) {
        iscontainbigint_return = 1;
      } else {
        iscontainbigint_return = 0;
      }
    } else if (testnull(arguments[2])) {
    } else {
      isfindthetype_return = 2;
      isintorbigint_return = 0;
    }

    boolean r = caseOIResolver.update(arguments[0]);
    assert (r);
    for (int i = 1; i + 1 < arguments.length; i += 2) {
      if (!caseOIResolver.update(arguments[i])) {
        if ((isintorbigint_case == 1) && testint(arguments[i])) {
          if (testbigint(arguments[i])) {
            iscontainbigint_case = 1;
          }
        } else {
          throw new UDFArgumentTypeException(i,
              "The expressions after WHEN should have the same type with that after CASE: \""
                  + caseOIResolver.get().getTypeName()
                  + "\" is expected but \"" + arguments[i].getTypeName()
                  + "\" is found");
        }
      } else {
        if ((!testnull(arguments[i])) && (isfindthetype_case == -1)) {
          if (testint(arguments[i])) {
            isfindthetype_case = i;
            isintorbigint_case = 1;
            if (testbigint(arguments[i])) {
              iscontainbigint_case = 1;
            }
          } else {
            isfindthetype_case = i;
            isintorbigint_case = 0;
          }
        }
      }
      if (!returnOIResolver.update(arguments[i + 1])) {
        if ((isintorbigint_return == 1) && testint(arguments[i + 1])) {
          if (testbigint(arguments[i + 1])) {
            iscontainbigint_return = 1;
          }
        } else {
          throw new UDFArgumentTypeException(i + 1,
              "The expressions after THEN should have the same type: \""
                  + returnOIResolver.get().getTypeName()
                  + "\" is expected but \"" + arguments[i + 1].getTypeName()
                  + "\" is found");
        }
      } else {
        if ((!testnull(arguments[i + 1])) && (isfindthetype_return == -1)) {
          if (testint(arguments[i + 1])) {
            isfindthetype_return = i + 1;
            isintorbigint_return = 1;
            if (testbigint(arguments[i + 1])) {
              iscontainbigint_return = 1;
            }
          } else {
            isfindthetype_return = i + 1;
            isintorbigint_return = 0;
          }
        }
      }
    }

    if (arguments.length % 2 == 0) {
      int i = arguments.length - 2;
      if (!returnOIResolver.update(arguments[i + 1])) {
        if ((isintorbigint_return == 1) && testint(arguments[i + 1])) {
          if (testbigint(arguments[i + 1])) {
            iscontainbigint_return = 1;
          }
        } else {
          throw new UDFArgumentTypeException(i + 1,
              "The expression after ELSE should have the same type as those after THEN: \""
                  + returnOIResolver.get().getTypeName()
                  + "\" is expected but \"" + arguments[i + 1].getTypeName()
                  + "\" is found");
        }
      } else {
        if ((!testnull(arguments[i + 1])) && (isfindthetype_return == -1)) {
          if (testint(arguments[i + 1])) {
            isfindthetype_return = i + 1;
            isintorbigint_return = 1;
            if (testbigint(arguments[i + 1])) {
              iscontainbigint_return = 1;
            }
          } else {
            isfindthetype_return = i + 1;
            isintorbigint_return = 0;
          }
        }
      }
    }

    if (iscontainbigint_return == 1) {
      return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    }
    return returnOIResolver.get();
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {

    Object exprValue = arguments[0].get();
    boolean testfristisnull = false;
    if (exprValue == null) {
      testfristisnull = true;
    }

    if ((iscontainbigint_case == 1) && (iscontainbigint_return != 1)) {
      LongWritable firstValue = null;
      if (!testfristisnull) {
        firstValue = myconvert(arguments[0], argumentOIs[0]);
      }
      boolean tmpflag = false;
      LongWritable tmpcaseKey = null;
      for (int i = 1; i + 1 < arguments.length; i += 2) {
        Object caseKey = arguments[i].get();
        if (caseKey == null) {
          tmpflag = true;
        } else {
          tmpcaseKey = myconvert(arguments[i], argumentOIs[i]);
        }

        if ((!tmpflag) && (!testfristisnull)) {
          if (firstValue.get() == tmpcaseKey.get()) {
            Object caseValue = arguments[i + 1].get();
            return returnOIResolver.convertIfNecessary(caseValue,
                argumentOIs[i + 1]);
          }
        }
        tmpflag = false;
      }
      if (arguments.length % 2 == 0) {
        int i = arguments.length - 2;
        Object elseValue = arguments[i + 1].get();
        return returnOIResolver.convertIfNecessary(elseValue,
            argumentOIs[i + 1]);
      }
      return null;
    } else if ((iscontainbigint_case == 1) && (iscontainbigint_return == 1)) {
      LongWritable firstValue = null;
      if (!testfristisnull) {
        firstValue = myconvert(arguments[0], argumentOIs[0]);
      }
      boolean tmpflag = false;
      LongWritable tmpcaseKey = null;
      for (int i = 1; i + 1 < arguments.length; i += 2) {
        Object caseKey = arguments[i].get();
        if (caseKey == null) {
          tmpflag = true;
        } else {
          tmpcaseKey = myconvert(arguments[i], argumentOIs[i]);
        }

        if ((!tmpflag) && (!testfristisnull)) {
          if (firstValue.get() == tmpcaseKey.get()) {
            LongWritable tmpexprValue = myconvert(arguments[i + 1],
                argumentOIs[i + 1]);
            return tmpexprValue;
          }
        }
        tmpflag = false;
      }
      if (arguments.length % 2 == 0) {
        int i = arguments.length - 2;
        LongWritable tmpexprValue = myconvert(arguments[i + 1],
            argumentOIs[i + 1]);
        return tmpexprValue;
      }
      return null;
    } else if ((iscontainbigint_case != 1) && (iscontainbigint_return == 1)) {
      for (int i = 1; i + 1 < arguments.length; i += 2) {
        Object caseKey = arguments[i].get();
        if (PrimitiveObjectInspectorUtils.comparePrimitiveObjects(exprValue,
            (PrimitiveObjectInspector) argumentOIs[0], caseKey,
            (PrimitiveObjectInspector) argumentOIs[i])) {
          LongWritable tmpexprValue = myconvert(arguments[i + 1],
              argumentOIs[i + 1]);
          return tmpexprValue;
        }
      }
      if (arguments.length % 2 == 0) {
        int i = arguments.length - 2;
        LongWritable tmpexprValue = myconvert(arguments[i + 1],
            argumentOIs[i + 1]);
        return tmpexprValue;
      }
      return null;
    } else if ((iscontainbigint_case != 1) && (iscontainbigint_return != 1)) {
      for (int i = 1; i + 1 < arguments.length; i += 2) {
        Object caseKey = arguments[i].get();
        if (PrimitiveObjectInspectorUtils.comparePrimitiveObjects(exprValue,
            (PrimitiveObjectInspector) argumentOIs[0], caseKey,
            (PrimitiveObjectInspector) argumentOIs[i])) {
          Object caseValue = arguments[i + 1].get();
          return returnOIResolver.convertIfNecessary(caseValue,
              argumentOIs[i + 1]);
        }
      }
      if (arguments.length % 2 == 0) {
        int i = arguments.length - 2;
        Object elseValue = arguments[i + 1].get();
        return returnOIResolver.convertIfNecessary(elseValue,
            argumentOIs[i + 1]);
      }
      return null;
    } else {
      return null;
    }

  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length >= 3);
    StringBuilder sb = new StringBuilder();
    sb.append("CASE (");
    sb.append(children[0]);
    sb.append(")");
    for (int i = 1; i + 1 < children.length; i += 2) {
      sb.append(" WHEN (");
      sb.append(children[i]);
      sb.append(") THEN (");
      sb.append(children[i + 1]);
      sb.append(")");
    }
    if (children.length % 2 == 0) {
      sb.append(" ELSE (");
      sb.append(children[children.length - 1]);
      sb.append(")");
    }
    sb.append(" END");
    return sb.toString();
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
}
