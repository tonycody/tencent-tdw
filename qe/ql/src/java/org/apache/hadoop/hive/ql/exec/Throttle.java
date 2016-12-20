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

import java.io.*;
import java.util.*;
import java.util.regex.Pattern;
import java.net.URL;
import java.net.URLEncoder;
import java.net.URLDecoder;
import java.net.MalformedURLException;
import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobTracker;

public class Throttle {

  static private int DEFAULT_MEMORY_GC_PERCENT = 100;

  static private int DEFAULT_RETRY_PERIOD = 60;

  static private int DEFAUlT_RETRY_TIMES = 3;

  static void checkJobTracker(JobConf conf, Log LOG) {

    try {
      byte buffer[] = new byte[1024];
      int threshold = conf.getInt("mapred.throttle.threshold.percent",
          DEFAULT_MEMORY_GC_PERCENT);
      int retry = conf.getInt("mapred.throttle.retry.period",
          DEFAULT_RETRY_PERIOD);

      // If the threshold is 100 percent, then there is no throttling
      if (threshold == 100) {
        return;
      }

      // This is the Job Tracker URL
      String tracker = JobTrackerURLResolver.getURL(conf) + "/gc.jsp?threshold=" + threshold;

      while (true) {
        // read in the first 1K characters from the URL
        URL url = new URL(tracker);
        LOG.debug("Throttle: URL " + tracker);
        InputStream in = url.openStream();
        int numRead = in.read(buffer);
        in.close();
        String fetchString = new String(buffer);

        // fetch the xml tag <dogc>xxx</dogc>
        Pattern dowait = Pattern.compile("<dogc>",
                         Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);
        String[] results = dowait.split(fetchString);
        if (results.length != 2) {
          throw new IOException("Throttle: Unable to parse response of URL " + url + 
                                ". Get retuned " + fetchString);
        }
        dowait = Pattern.compile("</dogc>",
                         Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.MULTILINE);
        results = dowait.split(results[1]);
        if (results.length < 1) {
          throw new IOException("Throttle: Unable to parse response of URL " + url + 
                                ". Get retuned " + fetchString);
        }

        // if the jobtracker signalled that the threshold is not exceeded, 
        // then we return immediately.
        if (results[0].trim().compareToIgnoreCase("false") == 0) {
          return;
        }

        // The JobTracker has exceeded its threshold and is doing a GC.
        // The client has to wait and retry.
        LOG.warn("Job is being throttled because of resource crunch on the " +
                 "JobTracker. Will retry in " + retry + " seconds..");
        Thread.sleep(retry * 1000L);
      }
    } catch (Exception e) {
      LOG.warn("Job is not being throttled. " + e);
    }
  }

  private static String getTracker(JobConf conf, Log LOG, JobClient jobClient,
      int threshold) throws IOException {
    //String hostName = jobClient.getHostName(conf);
    //conf.set("mapred.job.tracker", hostName);
    //String hostName=conf.get("mapred.job.tracker");
    String hostName=ShimLoader.getHadoopShims().getJobLauncherRpcAddress(conf);
    if (hostName == null || hostName.equals("")) {
      throw new IOException("hostName is null");
    }
    hostName = NetUtils.createSocketAddr(hostName).getAddress().getHostName();
    LOG.info("jobtracker is :" + hostName);
    String port = conf.get("info.port", "8080");
    return "http://" + hostName + ":" + port + "/gc.jsp?threshold=" + threshold;
  }

  public static void checkJobTrackerWithHotBackup(JobConf conf, Log LOG,
      JobClient jobClient) {
    try {
      byte buffer[] = new byte[1024];
      int threshold = conf.getInt("mapred.throttle.threshold.percent",
          DEFAULT_MEMORY_GC_PERCENT);
      int retry = conf.getInt("mapred.throttle.retry.period",
          DEFAULT_RETRY_PERIOD);
      int jtRetry = conf.getInt("mapred.job.tracker.retry.times",
          DEFAUlT_RETRY_TIMES);

      if (threshold == 100) {
        LOG.info("threshold: " + threshold);
        return;
      }

      String tracker = getTracker(conf, LOG, jobClient, threshold);

      while (true) {
        URL url = new URL(tracker);
        LOG.debug("Throttle: URL " + tracker);

        InputStream in = null;
        boolean succ = false;
        while (jtRetry-- > 0) {
          try {
            in = url.openStream();
          } catch (FileNotFoundException fnfe) {
            throw new IOException(fnfe);
          } catch (IOException urlIOE) {
            try {
              tracker = getTracker(conf, LOG, jobClient, threshold);
            } catch (IOException trackerIOE) {
              if (jtRetry == 0) {
                throw new IOException(trackerIOE);
              }
              continue;
            }
          }
        }

        if (in == null) {
          throw new IOException("get info from http failed, tracker: "
              + tracker);
        }

        int numRead = in.read(buffer);
        in.close();
        String fetchString = new String(buffer);

        Pattern dowait = Pattern.compile("<dogc>", Pattern.CASE_INSENSITIVE
            | Pattern.DOTALL | Pattern.MULTILINE);
        String[] results = dowait.split(fetchString);
        if (results.length != 2) {
          throw new IOException("Throttle: Unable to parse response of URL "
              + url + ". Get retuned " + fetchString);
        }

        dowait = Pattern.compile("</dogc>", Pattern.CASE_INSENSITIVE
            | Pattern.DOTALL | Pattern.MULTILINE);
        results = dowait.split(results[1]);
        if (results.length < 1) {
          throw new IOException("Throttle: Unable to parse response of URL "
              + url + ". Get retuned " + fetchString);
        }

        if (results[0].trim().compareToIgnoreCase("false") == 0) {
          LOG.info("results: " + results[0].trim());
          return;
        }

        LOG.warn("Job is being throttled because of resource crunch on the "
            + "JobTracker. Will retry in " + retry + " seconds..");
        Thread.sleep(retry * 1000L);
      }
    } catch (Exception e) {
      LOG.warn("Job is not being throttled. " + e);
    }
  }
}
