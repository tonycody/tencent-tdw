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

package org.apache.hadoop.hive.ql.exec.errors;

import java.util.List;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.mapred.JobConf;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

public class MapAggrMemErrorHeuristic extends RegexErrorHeuristic {

  private static final String OUT_OF_MEMORY_REGEX = "OutOfMemoryError";
  private boolean configMatches = false;

  private Log LOG = LogFactory.getLog(this.getClass().getName());
  private LogHelper console = new LogHelper(LOG);

  public MapAggrMemErrorHeuristic() {
    setQueryRegex("group by");
    getLogRegexes().add(OUT_OF_MEMORY_REGEX);
  }

  @Override
  public void init(String query, JobConf conf) {
    super.init(query, conf);
    configMatches = HiveConf.getBoolVar(conf,
        HiveConf.ConfVars.HIVEMAPSIDEAGGREGATE);
  }

  @Override
  public ErrorAndSolution getErrorAndSolution() {
    ErrorAndSolution es = null;
    LOG.info("getErrorAndSolution in MapAggrMemError");

    if (getQueryMatches() && configMatches) {
      List<String> matchingLines = getRegexToLogLines()
          .get(OUT_OF_MEMORY_REGEX);

      LOG.info("matchingLines.size() = " + matchingLines.size());

      if (matchingLines.size() > 0) {
        String confName = HiveConf.ConfVars.HIVEMAPAGGRHASHMEMORY.toString();
        float confValue = HiveConf.getFloatVar(getConf(),
            HiveConf.ConfVars.HIVEMAPAGGRHASHMEMORY);

        LOG.info("error exist in MapAggrMemError");
        es = new ErrorAndSolution(
            "Out of memory due to hash maps used in map-side aggregation.",
            "Currently " + confName + " is set to " + confValue + ". "
                + "Try setting it to a lower value. i.e " + "'set " + confName
                + " = " + confValue / 2 + ";'");
      }
    }
    reset();
    return es;
  }
}
