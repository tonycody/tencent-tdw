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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

public class TaskLogProcessor {
  private final Map<ErrorHeuristic, HeuristicStats> heuristics = new HashMap<ErrorHeuristic, HeuristicStats>();
  private final List<String> taskLogUrls = new ArrayList<String>();

  private JobConf conf = null;
  private String query = null;

  private Log LOG = LogFactory.getLog(this.getClass().getName());
  private LogHelper console = new LogHelper(LOG);

  public TaskLogProcessor(JobConf conf) {
    this.conf = conf;
    query = HiveConf.getVar(conf, HiveConf.ConfVars.HIVEQUERYSTRING);

    LOG.info("query = " + query);

    heuristics.put(new ScriptErrorHeuristic(), new HeuristicStats());
    heuristics.put(new MapAggrMemErrorHeuristic(), new HeuristicStats());
    heuristics.put(new DataCorruptErrorHeuristic(), new HeuristicStats());
    for (ErrorHeuristic e : heuristics.keySet()) {
      e.init(query, conf);
    }
  }

  public void addTaskAttemptLogUrl(String url) {
    taskLogUrls.add(url);
  }

  private static class HeuristicStats {

    private int triggerCount = 0;
    private final List<ErrorAndSolution> ens = new ArrayList<ErrorAndSolution>();

    HeuristicStats() {
    }

    int getTriggerCount() {
      return triggerCount;
    }

    void incTriggerCount() {
      triggerCount++;
    }

    List<ErrorAndSolution> getErrorAndSolutions() {
      return ens;
    }

    void addErrorAndSolution(ErrorAndSolution e) {
      ens.add(e);
    }
  }

  public List<ErrorAndSolution> getErrors() {

    for (String urlString : taskLogUrls) {

      URL taskAttemptLogUrl;
      try {
        taskAttemptLogUrl = new URL(urlString);
      } catch (MalformedURLException e) {
        throw new RuntimeException("Bad task log url", e);
      }
      BufferedReader in;
      try {
        in = new BufferedReader(new InputStreamReader(
            taskAttemptLogUrl.openStream()));
        String inputLine;
        inputLine = in.readLine();
        if (inputLine == null) {
        }

        while ((inputLine = in.readLine()) != null) {
          LOG.info("=========== inputLine =============\n" + inputLine);
          for (ErrorHeuristic e : heuristics.keySet()) {
            e.processLogLine(inputLine);
          }
        }
        in.close();
      } catch (IOException e) {
        throw new RuntimeException("Error while reading from task log url", e);
      }

      for (Entry<ErrorHeuristic, HeuristicStats> ent : heuristics.entrySet()) {
        ErrorHeuristic eh = ent.getKey();
        HeuristicStats hs = ent.getValue();

        ErrorAndSolution es = eh.getErrorAndSolution();
        if (es != null) {
          hs.incTriggerCount();
          hs.addErrorAndSolution(es);
        }
      }

    }

    int max = 0;
    for (HeuristicStats hs : heuristics.values()) {
      if (hs.getTriggerCount() > max) {
        max = hs.getTriggerCount();
      }
    }

    List<ErrorAndSolution> errors = new ArrayList<ErrorAndSolution>();
    for (HeuristicStats hs : heuristics.values()) {
      if (hs.getTriggerCount() == max) {
        if (hs.getErrorAndSolutions().size() > 0) {
          errors.add(hs.getErrorAndSolutions().get(0));
        }
      }
    }

    return errors;
  }
}
