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

public class IKeyIndex implements IPersistable, Comparable<IKeyIndex> {
  private IFieldType fieldType;
  private IFieldValue beginkey;
  private IFieldValue endkey;
  private int recnum = 0;

  public IKeyIndex(IFieldType fieldType) {
    this.fieldType = fieldType;
  }

  public IKeyIndex(IFieldValue beginkey, IFieldValue endkey, int recnum) {
    this.fieldType = beginkey.fieldType();
    this.beginkey = beginkey;
    this.endkey = endkey;
    this.recnum = recnum;
  }

  @Override
  public void persistent(DataOutput out) throws IOException {
    beginkey.persistent(out);
    endkey.persistent(out);
    out.writeInt(recnum);
  }

  @Override
  public void unpersistent(DataInput in) throws IOException {
    beginkey = new IFieldValue(this.fieldType);
    beginkey.unpersistent(in);
    endkey = new IFieldValue(this.fieldType);
    endkey.unpersistent(in);
    this.recnum = in.readInt();
  }

  @Override
  public int compareTo(IKeyIndex o) {
    return endkey.compareTo(o.endkey);
  }

  public int recnum() {
    return recnum;
  }

  public void setRecnum(int recnum) {
    this.recnum = recnum;
  }

  public IFieldValue beginkey() {
    return beginkey;
  }

  public IFieldValue endkey() {
    return endkey;
  }
}
