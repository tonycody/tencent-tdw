/**
* Tencent is pleased to support the open source community by making TDW available.
* Copyright (C) 2014 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use 
* this file except in compliance with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed 
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS 
* OF ANY KIND, either express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/

package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.tdw_sys_fields_statistics;
import org.apache.hadoop.hive.metastore.api.tdw_sys_table_statistics;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.plan.staticsInfoCollectWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

public class staticsInfoCollectTask extends Task<staticsInfoCollectWork>
    implements Serializable {
  private static final long serialVersionUID = 1L;

  tdw_sys_table_statistics table_statistics;

  String table_name = "";
  String splitToken = "\1";

  String stat_num_records_token = "stat_num_records";
  String stat_total_size_token = "stat_total_size";
  String stat_num_files_token = "stat_num_files";
  String stat_num_blocks_token = "stat_num_blocks";
  String stat_num_units_token = "stat_num_units";

  String MCVList_token = "MCVList";
  String HistogramTable_token = "HistogramTable";

  String Mcvlist_field_token = "Mcvlist" + splitToken;

  String stat_nullfac_token = "StatNullFac";
  String stat_avg_field_width_token = "StatAvgFieldWidth";
  String stat_distinct_values_token = "DistinctValue";

  boolean MCVListProcessing = false;
  boolean HistogramTableProcessing = false;

  HashMap<String, tdw_sys_fields_statistics> fields_statistics;

  public void addtoMcvList(tdw_sys_fields_statistics t, String value, String Num) {
    String strValue = t.getStat_values_1();
    String strNum = t.getStat_numbers_1();

    strValue = strValue + splitToken + value;
    strNum = strNum + splitToken + Num;

    t.setStat_values_1(strValue);
    t.setStat_numbers_1(strNum);
  }

  public void addtoHist(tdw_sys_fields_statistics t, String value, String Num) {
    String strValue = t.getStat_values_2();
    String strNum = t.getStat_numbers_2();

    strValue = strValue + splitToken + value;
    strNum = strNum + splitToken + Num;

    t.setStat_values_2(strValue);
    t.setStat_numbers_2(strNum);
  }

  public tdw_sys_fields_statistics getFields_statistic(String field) {
    tdw_sys_fields_statistics t;
    if ((t = fields_statistics.get(field)) == null) {
      t = new tdw_sys_fields_statistics();
      t.setStat_table_name(table_statistics.getStat_table_name());
      t.setStat_field_name(field);
      t.setStat_numbers_1("");
      t.setStat_numbers_2("");
      t.setStat_numbers_3("");
      t.setStat_values_1("");
      t.setStat_values_2("");
      t.setStat_values_3("");
      fields_statistics.put(field, t);
    }
    return t;
  }

  public void initialize(HiveConf conf, DriverContext ctx) {
    super.initialize(conf, ctx);
    table_statistics = new tdw_sys_table_statistics();
    table_statistics.setStat_table_name("");
    fields_statistics = new LinkedHashMap<String, tdw_sys_fields_statistics>();
  }

  public int execute() {

    try {
      JobConf job = new JobConf(conf, ExecDriver.class);
      Path dir = work.getPath();
      FileSystem fs = dir.getFileSystem(job);
      if (!fs.isDirectory(dir)) {
        LOG.error("staticsInfoCollectTask Err:" + dir.toString()
            + " is not a directory");
        console.printError("staticsInfoCollectTask Err:" + dir.toString()
            + " is not a directory");
        if (SessionState.get() != null)
          SessionState.get().ssLog(
              "staticsInfoCollectTask Err:" + dir.toString()
                  + " is not a directory");
        return -1;
      }
      FileStatus[] fss = fs.listStatus(dir);
      if (fss.length != 1) {
        LOG.error("staticsInfoCollectTask Err:" + dir.toString()
            + " must have only one file");
        console.printError("staticsInfoCollectTask Err:" + dir.toString()
            + " must have only one file");
        if (SessionState.get() != null)
          SessionState.get().ssLog(
              "staticsInfoCollectTask Err:" + dir.toString()
                  + " must have only one file");
        return -1;
      }
      FileStatus f = fss[0];
      if (!fs.isDirectory(f.getPath())) {

        FSDataInputStream is = fs.open(f.getPath());
        String line;
        boolean bFirst = true;
        while ((line = is.readLine()) != null) {
          processLine(line, bFirst);
          if (bFirst) {
            bFirst = false;
          }
        }
      } else {
        LOG.error("staticsInfoCollectTask Err:" + f.getPath()
            + " is a directory");
        if (SessionState.get() != null)
          SessionState.get().ssLog(
              "staticsInfoCollectTask Err:" + f.getPath() + " is a directory");
        return -1;
      }

      table_statistics.setStat_db_name(SessionState.get().getDbName());
      Hive.get().delete_table_statistics(table_statistics.getStat_table_name(),
          table_statistics.getStat_db_name());
      Hive.get().add_table_statistics(table_statistics);
      for (tdw_sys_fields_statistics fields_statistic : fields_statistics
          .values()) {
        fields_statistic.setStat_db_name(SessionState.get().getDbName());
        Hive.get().delete_fields_statistics(
            fields_statistic.getStat_table_name(),
            fields_statistic.getStat_db_name(),
            fields_statistic.getStat_field_name());
        Hive.get().add_fields_statistics(fields_statistic);
      }

      LOG.error(table_statistics.getStat_table_name() + " : "
          + table_statistics.getStat_db_name());
      tdw_sys_table_statistics table = Hive.get().get_table_statistics(
          table_statistics.getStat_table_name(),
          table_statistics.getStat_db_name());
      LOG.error(table.toString());
      if (SessionState.get() != null)
        SessionState.get().ssLog(table.toString());
      Hive.mapjoinstat canMapJoin = Hive.get().canTableMapJoin(
          table.getStat_table_name());
      LOG.error(table.getStat_table_name() + " canMapJoin=" + canMapJoin);
      if (SessionState.get() != null)
        SessionState.get().ssLog(
            table.getStat_table_name() + " canMapJoin=" + canMapJoin);

      List<tdw_sys_fields_statistics> fields = Hive.get()
          .get_fields_statistics_multi(table.getStat_table_name(),
              table.getStat_db_name(), -1);
      for (tdw_sys_fields_statistics field : fields) {
        LOG.error(field.toString());
        if (SessionState.get() != null)
          SessionState.get().ssLog(field.toString());
        Hive.skewstat st = Hive.get().getSkew(field.getStat_table_name(),
            field.getStat_field_name());
        LOG.error(field.getStat_table_name() + "." + field.getStat_field_name()
            + " skewstat=" + st);
        if (SessionState.get() != null)
          SessionState.get().ssLog(
              field.getStat_table_name() + "." + field.getStat_field_name()
                  + " skewstat=" + st);

      }

      if (work.getRmDir()) {
        fs.delete(dir, true);
      }

    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      if (SessionState.get() != null)
        SessionState.get().ssLog(StringUtils.stringifyException(e));
    }

    return 0;
  }

  private String getLastSplit(String in, String splitToken) {

    int index;
    if ((index = in.lastIndexOf(splitToken)) != -1) {
      String sub = in.substring(index + 1);
      return sub.trim();
    }

    return "";
  }

  int MAX = Integer.MAX_VALUE;

  private int parseInt(String str) {
    long input = Long.parseLong(str);

    if (input >= MAX) {
      return MAX;
    } else {
      return (int) input;
    }

  }

  private int processLine(String Line, boolean bFirst) {

    if (bFirst) {
      table_statistics.setStat_table_name(Line.trim());
      return 0;
    }

    if (Line.contains(stat_num_records_token)) {
      String str = getLastSplit(Line, splitToken);
      try {
        table_statistics.setStat_num_records(parseInt(str));
      } catch (Exception e) {
        LOG.error(StringUtils.stringifyException(e));
        table_statistics.setStat_num_records(MAX);
      }

      return 0;
    }

    if (Line.contains(stat_num_units_token)) {
      String str = getLastSplit(Line, splitToken);
      try {
        table_statistics.setStat_num_units(parseInt(str));
      } catch (Exception e) {
        LOG.error(StringUtils.stringifyException(e));
        table_statistics.setStat_num_units(MAX);
      }

      return 0;
    }

    if (Line.contains(stat_total_size_token)) {
      String str = getLastSplit(Line, splitToken);
      try {
        table_statistics.setStat_total_size(parseInt(str));
      } catch (Exception e) {
        LOG.error(StringUtils.stringifyException(e));
        table_statistics.setStat_total_size(MAX);
      }

      return 0;
    }

    if (Line.contains(stat_num_files_token)) {
      String str = getLastSplit(Line, splitToken);
      try {
        table_statistics.setStat_num_files(parseInt(str));
      } catch (Exception e) {
        LOG.error(StringUtils.stringifyException(e));
        table_statistics.setStat_num_files(MAX);
      }
      return 0;
    }

    if (Line.contains(stat_num_blocks_token)) {
      String str = getLastSplit(Line, splitToken);
      try {
        table_statistics.setStat_num_blocks(parseInt(str));
      } catch (Exception e) {
        LOG.error(StringUtils.stringifyException(e));
        table_statistics.setStat_num_blocks(MAX);
      }
      return 0;
    }

    if (Line.contains(stat_nullfac_token)) {
      String[] strs = Line.split(splitToken);
      if (strs.length != 4) {
        return -1;
      } else {
        String field = strs[2].trim();
        String number = strs[3].trim();
        getFields_statistic(field).setStat_nullfac(Double.valueOf(number));
        return 0;
      }
    }

    if (Line.contains(stat_avg_field_width_token)) {
      String[] strs = Line.split(splitToken);
      if (strs.length != 4) {
        return -1;
      } else {
        String field = strs[2].trim();
        String number = strs[3].trim();
        getFields_statistic(field).setStat_avg_field_width(
            Integer.valueOf(number));
        return 0;
      }
    }

    if (Line.contains(stat_distinct_values_token)) {
      String[] strs = Line.split(splitToken);
      if (strs.length != 4) {
        return -1;
      } else {
        String field = strs[2].trim();
        String number = strs[3].trim();
        getFields_statistic(field).setStat_distinct_values(
            Double.valueOf(number));
        return 0;
      }
    }

    if (Line.trim().equals(MCVList_token)) {
      MCVListProcessing = true;
      HistogramTableProcessing = false;
      return 0;
    }

    if (Line.trim().equals(HistogramTable_token)) {
      HistogramTableProcessing = true;
      MCVListProcessing = false;
      return 0;
    }

    if (Line.contains(Mcvlist_field_token)) {

      if (MCVListProcessing) {
        String[] strs = Line.split(splitToken);
        if (strs.length != 6) {
          return -1;
        } else {
          String field = strs[2].trim();
          String value = strs[3].trim();
          String num = strs[5].trim();
          addtoMcvList(getFields_statistic(field), value, num);
          return 0;
        }
      }

      if (HistogramTableProcessing) {
        String[] strs = Line.split(splitToken);
        if (strs.length != 5) {
          return -1;
        } else {
          String field = strs[3].trim();
          String value = strs[4].trim();
          String num = strs[0].trim();
          addtoHist(getFields_statistic(field), value, num);
          return 0;
        }
      }
    }

    return 0;

  }

  @Override
  public List<? extends Node> getChildren() {
    return super.getChildTasks();
  }

  @Override
  public String getName() {
    return "STATICSINFO";
  }
};
