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

package org.apache.hadoop.hive.ql.udf.generic;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

@description(name = "regext_instr", value = "_FUNC_(source_char,pattern[,position [,occurrence[,return_option[,match_parameter]]]]) - Returns the position the pattern in the source character", extended = "Generic UDF for string function REGEXP_INSTR \n"
    + "http://docs.oracle.com/cd/B19306_01/server.102/b14200/functions129.htm \n"
    + "1. source_char => source character \n"
    + "2. pattern => pattern \n"
    + "3. position => is a positive integer search from which char,1st default \n"
    + "4. occurrence =>is a positive integer  indicating which occurrence of pattern in source_char\n"
    + "5. return_option=>\n"
    + "0 - returns the position of the first character of the occurrence. Default\n"
    + "1 - returns the position of the character following the occurrence\n"
    + "6. match_parameter=> \n"
    + " 'i' - case-insensitive\n"
    + "'c' - case-sensitive\n"
    + "'n' - allows the period (.), which is the match-any-character character, to match the newline character\n"
    + "'m' - treats the source string as multiple lines \n"
    + "'x' - ignores whitespace characters\n"
    + "as the Oracle do, If you specify multiple contradictory values, TDW uses the last value.\n"
    + "For example, if you specify 'ic', then Oracle uses case-sensitive matching. \n"
    + "If you specify a character other than those shown above, then TDW returns an error.\n"

    + "Example: \n "
    + "  > SELECT _FUNC_('500 Oracle Parkway, Redwood Shores, CA', '[s|r|p]', 3, 2, 0, 'i') FROM src LIMIT 1;\n"
    + "  12 will be return. \n"
    + "  > SELECT _FUNC_('500 Oracle Parkway, Redwood Shores, CA', '[s|r|p]', 3, 2, 1, 'i') FROM src LIMIT 1;\n"
    + "  13 will be return.\n")
public class GenericUDFRegExpInstr extends GenericUDF {
  ObjectInspectorConverters.Converter[] converters;
  ObjectInspector[] argumentOIs;

  String str;
  String substr;

  private Pattern p = null;

  private static Log LOG = LogFactory.getLog(GenericUDFRegExpInstr.class
      .getName());

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentTypeException, UDFArgumentLengthException,
      UDFArgumentException {
    this.argumentOIs = arguments;

    if (arguments.length < 2) {
      throw new UDFArgumentLengthException(
          "The function INSTR accepts at least 2 arguments.");
    }

    if (arguments.length > 6) {
      throw new UDFArgumentLengthException(
          "The function INSTR accepts at most 6 arguments.");
    }

    for (int i = 0; i < arguments.length; i++) {
      Category category = arguments[i].getCategory();
      if (category != Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(i, "The "
            + GenericUDFUtils.getOrdinal(i + 1)
            + " argument of function INSTR is expected to a "
            + Category.PRIMITIVE.toString().toLowerCase() + " type, but "
            + category.toString().toLowerCase() + " is found");
      }
    }

    converters = new ObjectInspectorConverters.Converter[2];

    for (int i = 0; i < 2; i++) {
      PrimitiveObjectInspector po = ((PrimitiveObjectInspector) arguments[i]);

      if (po.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
        throw new UDFArgumentException("String type is needed");
      }

      converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
          PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }

    if (arguments.length > 2) {
      PrimitiveObjectInspector po3 = ((PrimitiveObjectInspector) arguments[2]);
      if (po3.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
        throw new UDFArgumentException("the 3rd argument must be integer");
      }

      if (arguments.length > 3) {
        PrimitiveObjectInspector po4 = ((PrimitiveObjectInspector) arguments[3]);
        if (po4.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
          throw new UDFArgumentException("the 4th argument must be integer");
        }
      }

      if (arguments.length > 4) {
        PrimitiveObjectInspector po5 = ((PrimitiveObjectInspector) arguments[4]);
        if (po5.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
          throw new UDFArgumentException(
              "the 5th argument must be integer: 0 or 1");
        }
      }

      if (arguments.length > 5) {
        PrimitiveObjectInspector po6 = ((PrimitiveObjectInspector) arguments[5]);
        if (po6.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
          throw new UDFArgumentException("the 6th argument must be string");
        }
      }
    }

    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    IntWritable intWritable = new IntWritable(0);

    int position = 1;
    int occurrence = 1;
    int return_option = 0;
    String match_parameter = "c";

    if (arguments[0].get() == null || arguments[1].get() == null) {
      return null;
    }

    str = (String) converters[0].convert(arguments[0].get());
    substr = (String) converters[1].convert(arguments[1].get());

    String strFromPosition = str;

    if (arguments.length > 2) {
      position = ((IntObjectInspector) argumentOIs[2]).get(arguments[2].get());
      if (position > str.length() || position < 1) {
        LOG.warn("illegal start position");
        return intWritable;
      }

      if (arguments.length > 3) {
        occurrence = ((IntObjectInspector) argumentOIs[3]).get(arguments[3]
            .get());
        if (occurrence > str.length() || occurrence < 0) {
          LOG.warn("illegal occurrence");
          return intWritable;
        }
      }

      if (arguments.length > 4) {
        return_option = ((IntObjectInspector) argumentOIs[4]).get(arguments[4]
            .get());
        if ((return_option != 0) && (return_option != 1)) {
          LOG.warn("illegal occurrence");
          return intWritable;
        }
      }

      if (arguments.length > 5) {
        match_parameter = ((StringObjectInspector) argumentOIs[5])
            .getPrimitiveJavaObject(arguments[5].get());
        match_parameter = match_parameter.toLowerCase();
        char[] tmp_char = match_parameter.toCharArray();

        String tmp_str = "icnmx";
        for (int i = 0; i < match_parameter.length(); i++) {
          if (!tmp_str.contains(String.valueOf(tmp_char[i]))) {
            LOG.warn("illegal match_parameter");
            return intWritable;
          }
        }
        match_parameter = String
            .valueOf(tmp_char[match_parameter.length() - 1]);
      }
    }

    try {
      if (match_parameter.compareToIgnoreCase("i") == 0) {
        p = Pattern.compile(substr.toString(), Pattern.CASE_INSENSITIVE);
      } else if (match_parameter.compareToIgnoreCase("c") == 0) {
        p = Pattern.compile(substr.toString());
      } else if (match_parameter.compareToIgnoreCase("n") == 0) {
        p = Pattern.compile(substr.toString(), Pattern.DOTALL);
      } else if (match_parameter.compareToIgnoreCase("m") == 0) {
        p = Pattern.compile(substr.toString(), Pattern.MULTILINE);
      } else if (match_parameter.compareToIgnoreCase("x") == 0) {
        p = Pattern.compile(substr.toString(), Pattern.COMMENTS);
      } else {
        p = Pattern.compile(substr.toString());
      }

      if (position > 1) {
        strFromPosition = str.substring(position - 1);
      }

      Matcher m = p.matcher(strFromPosition.toString());
      for (int i = 0; i < occurrence; i++) {
        if (m.find()) {
          if (return_option == 0) {
            intWritable.set(m.start() + position);
          } else {
            intWritable.set(m.end() + position);
          }
        } else {
          LOG.info("m.find() = " + m.find());
          intWritable.set(0);
          break;
        }
      }
      return intWritable;
    } catch (PatternSyntaxException e) {
      LOG.info("PatternSyntaxException");
      return intWritable;
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return null;
  }
}
