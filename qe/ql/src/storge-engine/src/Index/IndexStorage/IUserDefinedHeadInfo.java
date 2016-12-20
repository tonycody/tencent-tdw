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
package IndexStorage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

public class IUserDefinedHeadInfo implements IPersistable {

  HashMap<Integer, String> infos = new HashMap<Integer, String>();

  public void addInfo(int keyid, String info) {
    infos.put(keyid, info);
  }

  @Override
  public void persistent(DataOutput out) throws IOException {
    out.writeInt(infos.size());
    for (Entry<Integer, String> info : infos.entrySet()) {
      out.writeInt(info.getKey());
      out.writeUTF(info.getValue());
    }
  }

  @Override
  public void unpersistent(DataInput in) throws IOException {
    infos = new HashMap<Integer, String>();
    int num = in.readInt();
    for (int i = 0; i < num; i++) {
      int keyid = in.readInt();
      String value = in.readUTF();
      infos.put(keyid, value);
    }
  }

  public HashMap<Integer, String> infos() {
    return infos;
  }
}
