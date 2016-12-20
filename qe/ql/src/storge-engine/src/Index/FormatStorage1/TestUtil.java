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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class TestUtil {
  static String file = "testtesttesttesttesttest";
  static Configuration conf = new Configuration();
  static FileSystem fs;
  static {
    // conf.set("fs.default.name", "file:///");
    try {
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws IOException {
    String file = "/se/index/indextest/testtable/nopart/indexfile0";
    IFormatDataFile ifdf = new IFormatDataFile(conf);
    ifdf.open(file);
    ifdf.next().show();
    ifdf.next().show();
    ifdf.next().show();
    ifdf.close();
  }
}
