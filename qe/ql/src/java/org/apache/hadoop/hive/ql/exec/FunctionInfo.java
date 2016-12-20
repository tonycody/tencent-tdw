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

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDWFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;

public class FunctionInfo {
  private boolean isNative;

  private String displayName;

  private GenericUDF genericUDF;

  private GenericUDAFResolver genericUDAFResolver;

  private GenericUDWFResolver genericUDWFResolver;

  private GenericUDTF genericUDTF = null;

  public FunctionInfo(boolean isNative, String displayName,
      GenericUDF genericUDF) {
    this.isNative = isNative;
    this.displayName = displayName;
    this.genericUDF = genericUDF;
    this.genericUDAFResolver = null;
  }

  public FunctionInfo(boolean isNative, String displayName,
      GenericUDAFResolver genericUDAFResolver) {
    this.isNative = isNative;
    this.displayName = displayName;
    this.genericUDAFResolver = genericUDAFResolver;
    this.genericUDF = null;
  }

  public FunctionInfo(boolean isNative, String displayName,
      GenericUDWFResolver genericUDWFResolver) {
    this.isNative = isNative;
    this.displayName = displayName;
    this.genericUDWFResolver = genericUDWFResolver;
  }

  public FunctionInfo(boolean isNative, String displayName,
      GenericUDTF genericUDTF) {
    this.isNative = isNative;
    this.displayName = displayName;
    this.genericUDTF = genericUDTF;
  }

  public GenericUDTF getGenericUDTF() {
    if (genericUDTF == null)
      return null;
    return FunctionRegistry.cloneGenericUDTF(genericUDTF);
  }

  public boolean isAnalysisFunction() {
    return genericUDWFResolver != null;
  }

  public GenericUDWFResolver getGenericUDWFResolver() {
    return genericUDWFResolver;
  }

  public GenericUDF getGenericUDF() {
    if (genericUDF == null) {
      return null;
    }
    return FunctionRegistry.cloneGenericUDF(genericUDF);
  }

  public GenericUDAFResolver getGenericUDAFResolver() {
    return genericUDAFResolver;
  }

  public String getDisplayName() {
    return displayName;
  }

  public boolean isNative() {
    return isNative;
  }
}
