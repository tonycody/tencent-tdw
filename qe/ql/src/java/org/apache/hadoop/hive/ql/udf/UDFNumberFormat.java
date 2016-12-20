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
package org.apache.hadoop.hive.ql.udf;

import java.text.NumberFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

public class UDFNumberFormat extends UDF {

  private static Log LOG = LogFactory.getLog(UDFNumberFormat.class.getName());
  public static int maxdefault = 10000;

  public static enum FormatMode {
  };

  public String evaluate(DoubleWritable d, IntWritable fracMax,
      IntWritable fracMin, IntWritable intMax, IntWritable intMin) {
    if (d == null || fracMax == null || intMax == null) {
      return null;
    }
    if (fracMin == null || intMin == null) {
      return null;
    }
    double ori = d.get();
    try {
      NumberFormat nFormat = NumberFormat.getNumberInstance();
      nFormat.setMaximumFractionDigits(fracMax.get());
      nFormat.setMaximumIntegerDigits(intMax.get());
      nFormat.setMinimumFractionDigits(fracMin.get());
      nFormat.setMinimumIntegerDigits(intMin.get());
      nFormat.setGroupingUsed(false);
      return nFormat.format(ori);
    } catch (Exception e) {
      LOG.error("can not format value:  " + ori);
      return null;
    }
  }

  public String evaluate(LongWritable d, IntWritable fracMax,
      IntWritable fracMin, IntWritable intMax, IntWritable intMin) {
    if (d == null || fracMax == null || intMax == null) {
      return null;
    }
    if (fracMin == null || intMin == null) {
      return null;
    }
    long ori = d.get();
    try {
      NumberFormat nFormat = NumberFormat.getNumberInstance();
      nFormat.setMaximumFractionDigits(fracMax.get());
      nFormat.setMaximumIntegerDigits(intMax.get());
      nFormat.setMinimumFractionDigits(fracMin.get());
      nFormat.setMinimumIntegerDigits(intMin.get());
      nFormat.setGroupingUsed(false);
      return nFormat.format(ori);
    } catch (Exception e) {
      LOG.error("can not format value:  " + ori);
      return null;
    }
  }

  public String evaluate(DoubleWritable d) {
    if (d == null) {
      return null;
    }
    double ori = d.get();
    try {
      NumberFormat nFormat = NumberFormat.getNumberInstance();
      nFormat.setMaximumFractionDigits(maxdefault);
      nFormat.setMaximumIntegerDigits(maxdefault);
      nFormat.setGroupingUsed(false);
      return nFormat.format(ori);
    } catch (Exception e) {
      LOG.error("can not format value:  " + ori);
      return null;
    }
  }

  public String evaluate(DoubleWritable d, IntWritable fracMax,
      IntWritable intMax) {
    if (d == null || fracMax == null || intMax == null) {
      return null;
    }
    double ori = d.get();
    try {
      NumberFormat nFormat = NumberFormat.getNumberInstance();
      nFormat.setMaximumFractionDigits(fracMax.get());
      nFormat.setMaximumIntegerDigits(intMax.get());
      nFormat.setGroupingUsed(false);
      return nFormat.format(ori);
    } catch (Exception e) {
      LOG.error("can not format value:  " + ori);
      return null;
    }
  }
}
