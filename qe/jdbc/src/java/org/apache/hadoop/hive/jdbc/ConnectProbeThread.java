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
package org.apache.hadoop.hive.jdbc;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;

public class ConnectProbeThread implements Runnable {
  private Socket soc;
  private boolean isRunning = true;

  public ConnectProbeThread(Socket socket) {
    soc = socket;
  }

  public PrintWriter getWriter(Socket socket) throws IOException {
    OutputStream socketOut = socket.getOutputStream();
    return new PrintWriter(socketOut, true);
  }

  public void stopRunning() {
    isRunning = false;
  }

  public void run() {
    PrintWriter pw = null;
    try {
      pw = getWriter(soc);
    } catch (Exception x) {
      x.printStackTrace();
    }
    while (isRunning) {
      try {
        Thread.sleep(300000);
        if (isRunning) {
          pw.print("\0\0\0\0");
          pw.flush();
        }
      } catch (Exception x) {
        x.printStackTrace();
      }
    }
  }
}
