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

import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.metadata.*;
import org.apache.hadoop.io.*;

public class GenericUDFTestGBK extends GenericUDF {

  private StringObjectInspector oi;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentLengthException(
          "The function GenericUDFTestGBK(s) takes exactly 1 arguments.");
    }

    oi = (StringObjectInspector) arguments[0];

    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {

    try {
      Text str = oi.getPrimitiveWritableObject(arguments[0].get());
      byte[] bytes = str.getBytes();
      String s = new String(bytes, "GBK");
      Text new_str = new Text(s.getBytes("UTF-8"));
      return new_str;
    } catch (Exception e) {
      return new Text("Charset conversion failed.");
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return "GBKToUTF8( " + children[0] + " )";
  }
}
