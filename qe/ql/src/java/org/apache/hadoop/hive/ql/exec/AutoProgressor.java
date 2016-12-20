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

import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.Reporter;

public class AutoProgressor {
  protected Log LOG = LogFactory.getLog(this.getClass().getName());

  Timer rpTimer = null;
  String logClassName = null;
  int notificationInterval;
  Reporter reporter;

  class ReporterTask extends TimerTask {

    private Reporter rp;

    public ReporterTask(Reporter rp) {
      if (rp != null)
        this.rp = rp;
    }

    @Override
    public void run() {
      if (rp != null) {
        LOG.info("ReporterTask calling reporter.progress() for " + logClassName);
        rp.progress();
      }
    }
  }

  AutoProgressor(String logClassName, Reporter reporter,
      int notificationInterval) {
    this.logClassName = logClassName;
    this.reporter = reporter;
  }

  public void go() {
    LOG.info("Running ReporterTask every " + notificationInterval
        + " miliseconds.");
    rpTimer = new Timer(true);
    rpTimer.scheduleAtFixedRate(new ReporterTask(reporter), 0,
        notificationInterval);
  }
}
