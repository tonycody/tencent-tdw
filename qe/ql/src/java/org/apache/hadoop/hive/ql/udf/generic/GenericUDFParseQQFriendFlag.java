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

import java.util.ArrayList;
import java.util.Arrays;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;

@description(name = "parseQQFriendFlag", value = "_FUNC_(str) - parse QQ friend flag ")
public class GenericUDFParseQQFriendFlag extends GenericUDF {
  private ObjectInspectorConverters.Converter[] converters;
  private static Log LOG = LogFactory.getLog(GenericUDFRegExpInstr.class
	      .getName());

  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException(
          "The function SPLIT(s, regexp) takes exactly 2 arguments.");
    }

    converters = new ObjectInspectorConverters.Converter[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    return ObjectInspectorFactory
        .getStandardListObjectInspector(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
  }

  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    assert (arguments.length == 2);

    if (arguments[0].get() == null || arguments[1].get() == null) {
      return null;
    }
    
	Text s = (Text) converters[0].convert(arguments[0].get());
	Text regex = (Text) converters[1].convert(arguments[1].get());

	ArrayList<Text> result = new ArrayList<Text>();
	ArrayList<String> strArray = new ArrayList<String>(Arrays.asList("0","0","0","0","0"));

	for (String str : s.toString().split(regex.toString())) {
		//LOG.info("strrrr: " + str);
		if (str.equals("1001")) {
			strArray.set(0, "1");
		} else if (str.equals("1002")) {
			strArray.set(1, "1");
		} else if (str.equals("1003")) {
			strArray.set(2, "1");
		} else if (str.equals("1004")) {
			strArray.set(3, "1");
		} else if (Integer.valueOf(str).intValue() >= 3000 && Integer.valueOf(str).intValue() <= 3500) {
			strArray.set(4, str);
		}
	}

    for (String str : strArray) {
        result.add(new Text(str));
    }

    return result;
  }

  public String getDisplayString(String[] children) {
    assert (children.length == 2);
    return "split(" + children[0] + ", " + children[1] + ")";
  }

}
