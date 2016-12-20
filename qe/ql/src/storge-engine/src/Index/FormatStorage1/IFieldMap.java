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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

public class IFieldMap implements IPersistable {

  private HashMap<Integer, IRecord.IFType> fieldtypes;

  public IFieldMap() {
    this.fieldtypes = new HashMap<Integer, IRecord.IFType>();
  }

  public boolean addFieldType(IRecord.IFType ft) {
    if (this.fieldtypes.containsKey(ft.idx())) {
      return false;
    }
    this.setFieldType(ft);
    return true;
  }

  public boolean setFieldType(IRecord.IFType ft) {
    this.fieldtypes.put(ft.idx(), ft);
    return true;
  }

  public HashMap<Integer, IRecord.IFType> fieldtypes() {
    return fieldtypes;
  }

  @Override
  public void persistent(DataOutput out) throws IOException {
    out.writeShort(fieldtypes.size());

    for (IRecord.IFType ft : fieldtypes.values()) {
      ft.persistent(out);
    }
  }

  @Override
  public void unpersistent(DataInput in) throws IOException {
    fieldtypes = new HashMap<Integer, IRecord.IFType>();
    int num = 0;
    num = in.readShort();

    for (int i = 0; i < num; i++) {
      IRecord.IFType ft = new IRecord.IFType();
      ft.unpersistent(in);
      fieldtypes.put(ft.idx(), ft);
    }
  }

  public String showtypes() {
    StringBuffer sb = new StringBuffer();
    StringBuffer sb2 = new StringBuffer();
    sb.append("idx:\t");
    sb2.append("type:\t");
    for (IRecord.IFType ft : this.fieldtypes.values()) {
      sb.append(ft.idx() + "\t");
      sb2.append(ft.type() + "\t");
    }
    sb.append("\r\n").append(sb2.toString()).append("\r\n");
    return sb.toString();
  }

}
