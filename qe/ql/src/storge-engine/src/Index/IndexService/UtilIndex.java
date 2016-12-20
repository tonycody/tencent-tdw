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
package IndexService;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class UtilIndex {
  private static Configuration conf = new Configuration();
  private static FileSystem fs;
  static {
    try {
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void testIndexMR() throws IOException {
    String inputdir = "columndir";
    String ids = "0";
    String inputfiles = getdatafiles(new Path(inputdir), true, ids);
    System.out.println(inputfiles);
    String outputdir = "indexdir";
    IndexMR.run(conf, inputfiles, false, ids, outputdir);
  }

  public static void testIndexMRstring() throws Exception {
    String inputdir = "teststringdir";
    String ids = "6";
    String inputfiles = getdatafiles(new Path(inputdir), false, ids);
    System.out.println(inputfiles);
    String outputdir = "indexdir";
    IndexMR.run(conf, inputfiles, false, ids, outputdir);
  }

  private static String getdatafiles(Path part, boolean column, String indexids)
      throws IOException {
    StringBuffer sb = new StringBuffer();
    FileStatus[] part2s = fs.listStatus(part);
    for (FileStatus p2ss : part2s) {
      if (fs.isFile(p2ss.getPath())) {
        String filename = p2ss.getPath().makeQualified(fs).toString();
        if (!column || checkidx(filename, indexids)) {
          sb.append(filename + ",");
        }
      } else {
        FileStatus[] filess = fs.listStatus(p2ss.getPath());
        for (FileStatus fstts : filess) {
          String filename = fstts.getPath().makeQualified(fs).toString();
          if (!column || checkidx(filename, indexids)) {
            sb.append(filename + ",");
          }
        }
      }
    }
    return sb.substring(0, sb.length() - 1);
  }

  private static boolean checkidx(String filename, String indexids) {
    if (!filename.contains("_idx"))
      return false;
    String[] idx1s = filename.substring(filename.indexOf("_idx") + 4)
        .split("_");
    String[] idx2s = indexids.split(",");
    label: for (String idx2 : idx2s) {
      for (String idx1 : idx1s) {
        if (idx2.equals(idx1))
          continue label;
      }
      return false;
    }
    return true;
  }

  public static void main(String[] args) throws Exception {
    testIndexMRstring();
  }

}
