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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class DefaultUDAFEvaluatorResolver implements UDAFEvaluatorResolver {

  private Class<? extends UDAF> udafClass;

  public DefaultUDAFEvaluatorResolver(Class<? extends UDAF> udafClass) {
    this.udafClass = udafClass;
  }

  public Class<? extends UDAFEvaluator> getEvaluatorClass(
      List<TypeInfo> argClasses) throws AmbiguousMethodException {

    ArrayList<Class<? extends UDAFEvaluator>> classList = new ArrayList<Class<? extends UDAFEvaluator>>();

    for (Class<?> enclClass : udafClass.getClasses()) {
      for (Class<?> iface : enclClass.getInterfaces()) {
        if (iface == UDAFEvaluator.class) {
          classList.add((Class<? extends UDAFEvaluator>) enclClass);
        }
      }
    }

    ArrayList<Method> mList = new ArrayList<Method>();
    for (Class<? extends UDAFEvaluator> evaluator : classList) {
      for (Method m : evaluator.getMethods()) {
        if (m.getName().equalsIgnoreCase("iterate")) {
          mList.add(m);
        }
      }
    }

    Method m = FunctionRegistry.getMethodInternal(mList, false, argClasses);
    if (m == null) {
      throw new AmbiguousMethodException(udafClass, argClasses);
    }

    return (Class<? extends UDAFEvaluator>) m.getDeclaringClass();
  }

}
