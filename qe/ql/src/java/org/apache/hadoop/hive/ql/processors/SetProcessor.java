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

package org.apache.hadoop.hive.ql.processors;

import java.net.InetAddress;
import java.sql.Connection;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.ACLSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.DDLSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ExplainSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.LoadSemanticAnalyzer;
import org.apache.hadoop.hive.ql.session.SessionState;

public class SetProcessor implements CommandProcessor {
  static final private Log LOG = LogFactory
      .getLog("hive.ql.processors.SetProcessor");

  private static String prefix = "set: ";

  public static boolean getBoolean(String value) {
    if (value.equals("on") || value.equals("true"))
      return true;
    if (value.equals("off") || value.equals("false"))
      return false;
    throw new IllegalArgumentException(prefix + "'" + value
        + "' is not a boolean");
  }

  private void dumpOptions(Properties p) {
    SessionState ss = SessionState.get();

    ss.out.println("silent=" + (ss.getIsSilent() ? "on" : "off"));
    for (Object one : p.keySet()) {
      String oneProp = (String) one;
      String oneValue = p.getProperty(oneProp);
      ss.out.println(oneProp + "=" + oneValue);
    }
  }

  private void dumpOption(Properties p, String s) {
    SessionState ss = SessionState.get();

    if (p.getProperty(s) != null) {
      ss.out.println(s + "=" + p.getProperty(s));
    } else {
      ss.out.println(s + " is undefined");
    }
  }

  private void initializeBIStore() {
    try {
      HiveConf conf = SessionState.get().getConf();
      
      if (SessionState.get().getBIStore() == null) {
        SessionState.get().initBIStore();
      }
    } catch (Exception e) {
      LOG.error("getBIStore failed!");
      e.printStackTrace();
    }
  }

  private String insertToBi(String command) {
    int timeout = SessionState.get().getConf().getInt("hive.pg.timeout", 10);
//    int timeout = 1;
    String ddlQueryId = "abcdefghijkl";
    SessionState ss = SessionState.get();
    boolean testinsert = false;
    if (SessionState.get() != null && SessionState.get().getBIStore() != null) {
      try {
        InetAddress inet = InetAddress.getLocalHost();
        String IP = inet.getHostAddress().toString();
        SessionState.get().getBIStore().setDDLQueryIP(IP);

        ddlQueryId = ss.getQueryIDForLog();

        try {
          testinsert = true;
          Connection cc = null;
          try {
            if (SessionState.get().getBIStore() != null) {
              cc = ss.getBIStore().openConnect(timeout);
              if (cc != null) {
                ss.getBIStore().insertDDLInfo(cc, ddlQueryId, command);
                ss.getBIStore().closeConnect(cc);
              } else {
                LOG.error("add_tdw_query_info into BI failed: " + ddlQueryId);
              }
            }
          } catch (Exception e) {
            ss.getBIStore().closeConnect(cc);
            LOG.error("add_tdw_query_info into BI failed: " + ddlQueryId);
            testinsert = false;
          }
        } catch (Exception e) {
          LOG.info("add_tdw_query_info into BI failed: " + ddlQueryId
              + " errormesg:" + e.getMessage());
          testinsert = false;
        }
      } catch (Exception e) {
        LOG.info("add_tdw_query_info into BI failed: " + ddlQueryId
            + " errormesg:" + e.getMessage());
        testinsert = false;
      }
      if (!testinsert) {
        LOG.info("add_tdw_query_info failed: " + ddlQueryId);
      }
    }

    return ddlQueryId;
  }

  private void recordToBI(String ddlQueryId, boolean queryresult) {
    if (SessionState.get() != null && SessionState.get().getBIStore() != null) {
      SessionState ss = SessionState.get();
      int timeout = ss.getConf().getInt("hive.pg.timeout", 10);
//      int timeout = 1;

      try {
        try {
          Connection cc = null;
          try {
            if (SessionState.get().getBIStore() != null) {
              cc = ss.getBIStore().openConnect(timeout);
              if (cc != null) {
                ss.getBIStore().updateDDLQueryInfo(cc, ddlQueryId, queryresult,
                    ss.getConf().get("usp.param"), ss.getUserName());
                ss.getBIStore().closeConnect(cc);
              } else {
                LOG.error("update_tdw_query_info into BI failed: " + ddlQueryId);
              }
            }
          } catch (Exception e) {
            ss.getBIStore().closeConnect(cc);
            LOG.error("update_tdw_query_info into BI failed: " + ddlQueryId);
          }
        } catch (Exception e) {
        }
      } catch (Exception e) {
      }
    }
  }

  public int run(String command) {
    SessionState ss = SessionState.get();

    String nwcmd = command.trim();
    //TODO: add privilege check here
/*    if (nwcmd.equals("")) {
      dumpOptions(ss.getConf().getChangedProperties());
      return 0;
    }

    if (nwcmd.equals("-v")) {
      dumpOptions(ss.getConf().getAllProperties());
      return 0;
    }
*/
    String[] part = new String[2];

    int eqIndex = nwcmd.indexOf('=');
    if (eqIndex == -1) {
//      dumpOption(ss.getConf().getAllProperties(), nwcmd);
      return (0);
    } else if (eqIndex == nwcmd.length() - 1) {
      part[0] = nwcmd.substring(0, nwcmd.length() - 1);
      part[1] = "";
    } else {
      part[0] = nwcmd.substring(0, eqIndex).trim();
      part[1] = nwcmd.substring(eqIndex + 1).trim();
    }

    String ddlQueryId = "";

    initializeBIStore();

    ddlQueryId = insertToBi(command);

    try {
      if (part[0].equals("silent")) {
        boolean val = getBoolean(part[1]);
        ss.setIsSilent(val);
      } else {
        ss.getConf().set(part[0], part[1]);
      }
    } catch (IllegalArgumentException err) {
      ss.err.println(err.getMessage());
      ss.ssLog(err.getMessage());
      recordToBI(ddlQueryId, false);
      return 1;
    }

    recordToBI(ddlQueryId, true);
    return 0;
  }
}
