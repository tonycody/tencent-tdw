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
import java.util.TreeMap;

public class IFieldMap implements IPersistable {

  private TreeMap<Short, IFieldType> fieldtypes;

  public IFieldMap() {
    this.fieldtypes = new TreeMap<Short, IFieldType>();
  }

  public boolean addFieldType(IFieldType ft) {
    ft.setIndex((short) fieldtypes.size());
    this.fieldtypes.put(ft.getIndex(), ft);
    return true;
  }

  public TreeMap<Short, IFieldType> fieldtypes() {
    return fieldtypes;
  }

  @Override
  public void persistent(DataOutput out) throws IOException {
    out.writeInt(fieldtypes.size());
    for (int i = 0; i < fieldtypes.size(); i++) {
      fieldtypes.get((short) i).persistent(out);
    }
  }

  @Override
  public void unpersistent(DataInput in) throws IOException {
    fieldtypes = new TreeMap<Short, IFieldType>();
    int num = in.readInt();
    for (int i = 0; i < num; i++) {
      IFieldType ft = new IFieldType();
      ft.unpersistent(in);
      fieldtypes.put(ft.getIndex(), ft);
    }
  }

  public String showtypes() {
    StringBuffer sb = new StringBuffer();
    StringBuffer sb2 = new StringBuffer();
    sb.append("idx:\t");
    sb2.append("type:\t");
    for (IFieldType ft : this.fieldtypes.values()) {
      sb.append(ft.getIndex() + "\t");
      sb2.append(ft.getType() + "\t");
    }
    sb.append("\r\n").append(sb2.toString()).append("\r\n");
    return sb.toString();
  }
}
