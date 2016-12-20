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
package FormatStorage1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class HdfsRenameUtil {
  public static void main(String[] args) throws IOException {
    if (args.length <= 0) {
      System.out
          .println("input a src file which contains the src dirs and dest dirs, seperated by \\t");
      return;
    }
    BufferedReader br = new BufferedReader(new InputStreamReader(
        new FileInputStream(new File(args[0]))));
    String str;
    ArrayList<Path> src = new ArrayList<Path>();
    ArrayList<Path> dest = new ArrayList<Path>();
    while ((str = br.readLine()) != null) {
      String[] strs = str.split("\t");
      if (strs.length != 2) {
        throw new IOException("input format is wrong:\t<<" + str + ">>");
      }
      src.add(new Path(strs[0]));
      dest.add(new Path(strs[1]));
    }
    br.close();
    System.out.println("there are " + src.size() + " dirs to rename..");
    Configuration conf = new Configuration();
    for (int i = 0; i < src.size(); i++) {
      src.get(i).getFileSystem(conf).rename(src.get(i), dest.get(i));
      System.out.println("rename dir: <" + src.get(i).toString() + "> to: <"
          + dest.get(i).toString() + ">");
    }
  }
}
