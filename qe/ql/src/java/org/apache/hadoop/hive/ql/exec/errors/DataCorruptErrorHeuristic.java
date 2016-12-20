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
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

public class DataCorruptErrorHeuristic extends RegexErrorHeuristic {

  private static final String SPLIT_REGEX = "split:.*";
  private static final String EXCEPTION_REGEX = "EOFException";

  private Log LOG = LogFactory.getLog(this.getClass().getName());
  private LogHelper console = new LogHelper(LOG);

  public DataCorruptErrorHeuristic() {
    setQueryRegex(".*");
    getLogRegexes().add(SPLIT_REGEX);
    getLogRegexes().add(EXCEPTION_REGEX);
  }

  @Override
  public ErrorAndSolution getErrorAndSolution() {
    ErrorAndSolution es = null;

    if (getQueryMatches()) {

      LOG.info("getErrorAndSolution in DataCorruptError");
      Map<String, List<String>> rll = getRegexToLogLines();
      if (rll.get(EXCEPTION_REGEX).size() > 0
          && rll.get(SPLIT_REGEX).size() > 0) {

        assert (rll.get(SPLIT_REGEX).size() == 1);
        String splitLogLine = rll.get(SPLIT_REGEX).get(0);

        Pattern p = Pattern.compile(SPLIT_REGEX, Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(splitLogLine);
        m.find();
        String splitStr = m.group();

        LOG.info("error exist in DataCorruptError");
        es = new ErrorAndSolution("Data file " + splitStr + " is corrupted.",
            "Replace file. i.e. by re-running the query that produced the "
                + "source table / partition.");
      }
    }
    reset();
    return es;
  }
}
