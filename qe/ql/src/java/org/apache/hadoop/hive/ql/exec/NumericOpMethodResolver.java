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

import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class NumericOpMethodResolver implements UDFMethodResolver {
  public static boolean opSwitch;
  static {
    HiveConf hc = new HiveConf(NumericOpMethodResolver.class);
    opSwitch = hc.getBoolean("tdw.udf.switch.nopr.rules", false);
  };

  Class<? extends UDF> udfClass;

  public NumericOpMethodResolver(Class<? extends UDF> udfClass) {
    this.udfClass = udfClass;
  }

  @Override
  public Method getEvalMethod(List<TypeInfo> argTypeInfos)
      throws AmbiguousMethodException, UDFArgumentException {
    assert (argTypeInfos.size() == 2);
    boolean switchOn = opSwitch;
    List<TypeInfo> pTypeInfos = null;
    if (switchOn) {

      List<TypeInfo> modArgTypeInfos = new ArrayList<TypeInfo>();

      if (argTypeInfos.get(0).equals(TypeInfoFactory.stringTypeInfo)
          || argTypeInfos.get(1).equals(TypeInfoFactory.stringTypeInfo)) {
        modArgTypeInfos.add(TypeInfoFactory.doubleTypeInfo);
        modArgTypeInfos.add(TypeInfoFactory.doubleTypeInfo);
      } else {
        for (int i = 0; i < 2; i++) {
          if (argTypeInfos.get(i).equals(TypeInfoFactory.voidTypeInfo)) {
            modArgTypeInfos.add(TypeInfoFactory.byteTypeInfo);
          } else {
            modArgTypeInfos.add(argTypeInfos.get(i));
          }
        }
      }

      TypeInfo commonType = FunctionRegistry.getCommonClass(
          modArgTypeInfos.get(0), modArgTypeInfos.get(1));

      if (commonType == null) {
        throw new UDFArgumentException("Unable to find a common class between"
            + "types " + modArgTypeInfos.get(0).getTypeName() + " and "
            + modArgTypeInfos.get(1).getTypeName());
      }

      pTypeInfos = new ArrayList<TypeInfo>();
      pTypeInfos.add(commonType);
      pTypeInfos.add(commonType);

    } else {
      pTypeInfos = null;
      if (argTypeInfos.get(0).equals(TypeInfoFactory.voidTypeInfo)
          || argTypeInfos.get(1).equals(TypeInfoFactory.voidTypeInfo)) {
        pTypeInfos = new ArrayList<TypeInfo>();
        pTypeInfos.add(TypeInfoFactory.doubleTypeInfo);
        pTypeInfos.add(TypeInfoFactory.doubleTypeInfo);
      } else if (argTypeInfos.get(0).equals(TypeInfoFactory.stringTypeInfo) ||

      argTypeInfos.get(1).equals(TypeInfoFactory.stringTypeInfo)) {
        pTypeInfos = new ArrayList<TypeInfo>();
        pTypeInfos.add(TypeInfoFactory.doubleTypeInfo);
        pTypeInfos.add(TypeInfoFactory.doubleTypeInfo);
      } else if (argTypeInfos.get(0) == argTypeInfos.get(1)) {
        pTypeInfos = argTypeInfos;

      } else {
        pTypeInfos = new ArrayList<TypeInfo>();
        pTypeInfos.add(TypeInfoFactory.doubleTypeInfo);
        pTypeInfos.add(TypeInfoFactory.doubleTypeInfo);

      }
    }
    Method udfMethod = null;

    for (Method m : Arrays.asList(udfClass.getMethods())) {
      if (m.getName().equals("evaluate")) {

        List<TypeInfo> argumentTypeInfos = TypeInfoUtils.getParameterTypeInfos(
            m, pTypeInfos.size());
        if (argumentTypeInfos == null) {
          continue;
        }

        boolean match = (argumentTypeInfos.size() == pTypeInfos.size());

        for (int i = 0; i < pTypeInfos.size() && match; i++) {
          TypeInfo accepted = argumentTypeInfos.get(i);
          if (!accepted.equals(pTypeInfos.get(i))) {
            match = false;
          }
        }

        if (match) {
          if (udfMethod != null) {
            throw new AmbiguousMethodException(udfClass, argTypeInfos);
          } else {
            udfMethod = m;
          }
        }
      }
    }
    return udfMethod;
  }
}
