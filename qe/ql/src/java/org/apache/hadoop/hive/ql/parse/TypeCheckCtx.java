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

package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;

public class TypeCheckCtx implements NodeProcessorCtx {

  private RowResolver inputRR;

  private UnparseTranslator unparseTranslator;

  private String error;

  private QB qb;

  public TypeCheckCtx(RowResolver inputRR) {
    this.setInputRR(inputRR);
    this.error = null;
  }

  public TypeCheckCtx(RowResolver inputRR, QB qb) {
    super();
    this.inputRR = inputRR;
    this.qb = qb;
  }

  public QB getQb() {
    return qb;
  }

  public void setQb(QB qb) {
    this.qb = qb;
  }

  public void setInputRR(RowResolver inputRR) {
    this.inputRR = inputRR;
  }

  public RowResolver getInputRR() {
    return inputRR;
  }

  public void setUnparseTranslator(UnparseTranslator unparseTranslator) {
    this.unparseTranslator = unparseTranslator;
  }

  public UnparseTranslator getUnparseTranslator() {
    return unparseTranslator;
  }

  public void setError(String error) {
    this.error = error;
  }

  public String getError() {
    return error;
  }

}
