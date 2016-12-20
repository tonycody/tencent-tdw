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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import IndexService.IndexServer.IndexItemStatus;

public class IndexServerTestModeTest {
  public static void main(String[] args) throws IOException {
    File file = new File("indexconf");
    ArrayList<IndexItemStatus> statuss = new ArrayList<IndexItemStatus>();
    boolean flag = false;
    if (flag)
      if (file.exists()) {
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        int num = dis.readInt();
        for (int i = 0; i < num; i++) {
          IndexItemStatus status = new IndexItemStatus();
          status.read(dis);
          statuss.add(status);
        }
        dis.close();
      }

    IndexItemStatus status = new IndexItemStatus();

    status.datadir = "/se/warehouse";
    status.database = "testdb";
    status.table = "testformat";
    status.indexname = "test";
    status.field = "2";
    status.indexlocation = "/se/index/indextest1/" + status.table;
    status.status = 0;
    status.tabletype = "format";

    statuss.add(status);

    DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
    dos.writeInt(statuss.size());
    for (int i = 0; i < statuss.size(); i++) {
      statuss.get(i).write(dos);
    }
    dos.close();

  }
}
