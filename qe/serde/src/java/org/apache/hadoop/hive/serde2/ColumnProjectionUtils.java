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

package org.apache.hadoop.hive.serde2;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;

public final class ColumnProjectionUtils {

  public static final String READ_COLUMN_IDS_CONF_STR = "hive.io.file.readcolumn.ids";

  public static void setReadColumnIDs(Configuration conf, List<Integer> ids) {
    String id = toReadColumnIDString(ids);
    setReadColumnIDConf(conf, id);
  }

  public static void appendReadColumnIDs(Configuration conf, List<Integer> ids) {
    String id = toReadColumnIDString(ids);
    if (id != null) {
      String old = conf.get(READ_COLUMN_IDS_CONF_STR, null);
      String newConfStr = id;
      if (old != null) {
        newConfStr = newConfStr + StringUtils.COMMA_STR + old;
      }

      setReadColumnIDConf(conf, newConfStr);
    }
  }

  private static void setReadColumnIDConf(Configuration conf, String id) {
    if (id == null || id.length() <= 0) {
      conf.set(READ_COLUMN_IDS_CONF_STR, "");
      return;
    }

    conf.set(READ_COLUMN_IDS_CONF_STR, id);
  }

  private static String toReadColumnIDString(List<Integer> ids) {
    String id = null;
    if (ids != null) {
      for (int i = 0; i < ids.size(); i++) {
        if (i == 0) {
          id = "" + ids.get(i);
        } else {
          id = id + StringUtils.COMMA_STR + ids.get(i);
        }
      }
    }
    return id;
  }

  public static ArrayList<Integer> getReadColumnIDs(Configuration conf) {
    if (conf == null) {
      return new ArrayList<Integer>(0);
    }
    String skips = conf.get(READ_COLUMN_IDS_CONF_STR, "");
    String[] list = StringUtils.split(skips);
    ArrayList<Integer> result = new ArrayList<Integer>(list.length);
    for (String element : list) {
      Integer toAdd = Integer.parseInt(element);
      if (!result.contains(toAdd)) {
        result.add(toAdd);
      }
    }
    return result;
  }

  public static void setFullyReadColumns(Configuration conf) {
    conf.set(READ_COLUMN_IDS_CONF_STR, "");
  }

  private ColumnProjectionUtils() {
  }

}
