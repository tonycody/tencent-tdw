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

import java.net.*;
import org.apache.hadoop.io.Text;

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

@description(name = "url_decode", value = "_FUNC_(url, encode) - decode url", extended = "Generic UDF for url decode function url_decode \n")
public class GenericUDFUrlDecode extends GenericUDF {
  ObjectInspectorConverters.Converter[] converters;
  ObjectInspector[] argumentOIs;

  String urlStr;
  String enc;

  private static Log LOG = LogFactory.getLog(GenericUDFRegExpInstr.class
      .getName());

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentTypeException, UDFArgumentLengthException,
      UDFArgumentException {
    this.argumentOIs = arguments;

    if (arguments.length < 2) {
      throw new UDFArgumentLengthException(
          "The function url_decode accepts 2 arguments.");
    }

    for (int i = 0; i < arguments.length; i++) {
      Category category = arguments[i].getCategory();
      if (category != Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(i, "The "
            + GenericUDFUtils.getOrdinal(i + 1)
            + " argument of function url_decode is expected to a "
            + Category.PRIMITIVE.toString().toLowerCase() + " type, but "
            + category.toString().toLowerCase() + " is found");
      }
    }

    converters = new ObjectInspectorConverters.Converter[2];

    for (int i = 0; i < 2; i++) {
      PrimitiveObjectInspector po = ((PrimitiveObjectInspector) arguments[i]);

      if (po.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
        throw new UDFArgumentException("String type is needed in parament " + i
            + 1);
      }

      converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
          PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    }

    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    String decodedUrlStr;
    if (arguments[0].get() == null || arguments[1].get() == null) {
      return null;
    }

    urlStr = (String) converters[0].convert(arguments[0].get());
    enc = (String) converters[1].convert(arguments[1].get());

    try {
      decodedUrlStr = URLDecoder.decode(urlStr, enc);
      return new Text(decodedUrlStr);
    } catch (java.io.UnsupportedEncodingException e) {
      LOG.info("UnsupportedEncodingException");
      return null;
    } catch (Exception ex) {
      LOG.info("other exception in URLDecoder.decode function");
      return null;
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return null;
  }
}
