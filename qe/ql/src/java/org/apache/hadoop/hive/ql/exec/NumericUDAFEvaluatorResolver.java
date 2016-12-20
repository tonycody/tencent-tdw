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

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class NumericUDAFEvaluatorResolver extends DefaultUDAFEvaluatorResolver {

  public NumericUDAFEvaluatorResolver(Class<? extends UDAF> udafClass) {
    super(udafClass);
  }

  @Override
  public Class<? extends UDAFEvaluator> getEvaluatorClass(
      List<TypeInfo> argTypeInfos) throws AmbiguousMethodException {
    ArrayList<TypeInfo> args = new ArrayList<TypeInfo>();
    for (TypeInfo arg : argTypeInfos) {
      if (arg.equals(TypeInfoFactory.voidTypeInfo)
          || arg.equals(TypeInfoFactory.stringTypeInfo)) {
        args.add(TypeInfoFactory.doubleTypeInfo);
      } else {
        args.add(arg);
      }
    }

    return super.getEvaluatorClass(args);
  }
}
