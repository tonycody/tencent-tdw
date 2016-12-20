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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.JobConf;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

public abstract class RegexErrorHeuristic implements ErrorHeuristic {

  private String query = null;
  private JobConf conf = null;

  private String queryRegex = null;
  private boolean queryMatches = false;

  private Log LOG = LogFactory.getLog(this.getClass().getName());
  private LogHelper console = new LogHelper(LOG);

  private final Set<String> logRegexes = new HashSet<String>();

  private final Map<String, List<String>> regexToLogLines = new HashMap<String, List<String>>();
  private final Map<String, Pattern> regexToPattern = new HashMap<String, Pattern>();

  public RegexErrorHeuristic() {
  }

  protected void setQueryRegex(String queryRegex) {
    this.queryRegex = queryRegex;
  }

  protected String getQueryRegex() {
    return queryRegex;
  }

  protected boolean getQueryMatches() {
    return queryMatches;
  }

  protected Set<String> getLogRegexes() {
    return this.logRegexes;
  }

  protected Map<String, List<String>> getRegexToLogLines() {
    return this.regexToLogLines;
  }

  protected JobConf getConf() {
    return conf;
  }

  @Override
  public void init(String query, JobConf conf) {
    this.query = query;
    this.conf = conf;

    assert ((logRegexes != null) && (queryRegex != null));

    Pattern queryPattern = Pattern
        .compile(queryRegex, Pattern.CASE_INSENSITIVE);
    queryMatches = queryPattern.matcher(query).find();

    LOG.info("queryMatches = " + queryMatches);

    for (String regex : logRegexes) {
      regexToPattern.put(regex,
          Pattern.compile(regex, Pattern.CASE_INSENSITIVE));
      regexToLogLines.put(regex, new ArrayList<String>());
    }

  }

  @Override
  public abstract ErrorAndSolution getErrorAndSolution();

  @Override
  public void processLogLine(String line) {
    if (queryMatches) {
      for (Entry<String, Pattern> e : regexToPattern.entrySet()) {
        String regex = e.getKey();
        Pattern p = e.getValue();
        boolean lineMatches = p.matcher(line).find();
        if (lineMatches) {
          regexToLogLines.get(regex).add(line);
        }
      }
    }
  }

  protected void reset() {
    for (List<String> lst : regexToLogLines.values()) {
      lst.clear();
    }
  }
}
