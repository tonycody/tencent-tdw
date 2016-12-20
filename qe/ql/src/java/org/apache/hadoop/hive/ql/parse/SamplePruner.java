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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.fs.Path;

public class SamplePruner {

  private String tabAlias;

  private TableSample tableSample;

  @SuppressWarnings("nls")
  private static final Log LOG = LogFactory
      .getLog("hive.ql.parse.SamplePruner");

  public SamplePruner() {

  }

  public SamplePruner(String alias, TableSample tableSample) {
    this.tabAlias = alias;
    this.tableSample = tableSample;
  }

  public String getTabAlias() {
    return this.tabAlias;
  }

  public void setTabAlias(String tabAlias) {
    this.tabAlias = tabAlias;
  }

  public TableSample getTableSample() {
    return this.tableSample;
  }

  public void setTableSample(TableSample tableSample) {
    this.tableSample = tableSample;
  }

  @SuppressWarnings("nls")
  public Path[] prune(Partition part) throws SemanticException {
    int num = this.tableSample.getNumerator();
    int den = this.tableSample.getDenominator();
    int bucketCount = part.getBucketCount();
    String fullScanMsg = "";
    if (this.tableSample.getInputPruning()) {
      LOG.trace("numerator = " + num);
      LOG.trace("denominator = " + den);
      LOG.trace("bucket count = " + bucketCount);
      if (bucketCount == den) {
        Path[] ret = new Path[1];
        ret[0] = part.getBucketPath(num - 1);
        return (ret);
      } else if (bucketCount > den && bucketCount % den == 0) {
        int numPathsInSample = bucketCount / den;
        Path[] ret = new Path[numPathsInSample];
        for (int i = 0; i < numPathsInSample; i++) {
          ret[i] = part.getBucketPath(i * den + num - 1);
        }
        return ret;
      } else if (bucketCount < den && den % bucketCount == 0) {
        Path[] ret = new Path[1];
        ret[0] = part.getBucketPath((num - 1) % bucketCount);
        return ret;
      } else {
        fullScanMsg = "Tablesample denominator " + den
            + " is not multiple/divisor of bucket count " + bucketCount
            + " of table " + this.tabAlias;
      }
    } else {
      fullScanMsg = "Tablesample not on clustered columns";
    }
    LOG.warn(fullScanMsg + ", using full table scan");
    Path[] ret = part.getPath();
    return ret;
  }
}
