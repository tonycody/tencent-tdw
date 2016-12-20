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
package Tool;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import FormatStorage.FormatDataFile;
import FormatStorage.Unit.Record;

public class CheckFormatStorageData {
  static void getRecordByLine(String file, int line) throws Exception {
    FormatDataFile fd = new FormatDataFile(new Configuration());
    fd.open(file);

    fd.head().show();

    Record record = fd.getRecordByLine(line);
    if (record == null) {
      System.out.println("Null Record Return, line:" + line);
      fd.close();
      return;
    }

    record.show();

    System.out.println("once more ... ");
    record = fd.getRecordByLine(line);
    if (record == null) {
      System.out.println("Null Record Return, line:" + line);
      fd.close();
      return;
    }

    record.show();
  }

  public static void main(String[] argv) throws Exception {
    try {
      if (argv.length != 2) {
        System.out.println("cmd: file line");
        return;
      }

      String file = argv[0];
      int line = Integer.valueOf(argv[1]);

      getRecordByLine(file, line);

    } catch (IOException e) {
      e.printStackTrace();
      System.out.println("get IOException:" + e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("get exception:" + e.getMessage());
    }
  }

}
