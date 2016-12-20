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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.WritableComparable;

import FormatStorage1.IRecord;

public class IndexKey implements WritableComparable<IndexKey> {

  private ArrayList<IRecord.IFValue> fvs;

  public IndexKey() {
    this.fvs = new ArrayList<IRecord.IFValue>();
  }

  public void addfv(IRecord.IFValue fv) {
    fvs.add(fv);
  }

  public int size() {
    return fvs.size();
  }

  public ArrayList<IRecord.IFValue> getfvs() {
    return fvs;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    fvs = new ArrayList<IRecord.IFValue>();
    int num = in.readInt();
    for (int i = 0; i < num; i++) {
      IRecord.IFValue ifv = new IRecord.IFValue();
      ifv.readFields(in);
      fvs.add(ifv);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(fvs.size());
    for (IRecord.IFValue fv : fvs) {
      fv.write(out);
    }
  }

  @Override
  public int compareTo(IndexKey key) {
    ArrayList<IRecord.IFValue> fvs1 = this.getfvs();
    ArrayList<IRecord.IFValue> fvs2 = key.getfvs();
    int len = Math.min(fvs1.size(), fvs2.size());
    for (int i = 0; i < len; i++) {
      int res = fvs1.get(i).compareTo(fvs2.get(i));
      if (res != 0)
        return res;
    }
    if (fvs1.size() == fvs2.size()) {
      return 0;
    } else if (fvs1.size() > fvs2.size()) {
      return 1;
    } else {
      return -1;
    }
  }

  public void reset() {
    this.fvs.clear();
  }

  public String show() {
    StringBuffer sb = new StringBuffer();
    for (int j = 0; j < this.fvs.size(); j++) {
      sb.append(this.fvs.get(j).show() + " ");
    }
    return sb.toString();
  }
}
