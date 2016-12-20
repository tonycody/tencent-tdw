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

import Comm.ConstVar;

public class ILineIndex implements IPersistable, Comparable<ILineIndex> {
  private int beginline;
  private int endline;

  public ILineIndex() {
  }

  public ILineIndex(int beginline, int endline) {
    this.beginline = beginline;
    this.endline = endline;
  }

  public int size() {
    return 2 * ConstVar.Sizeof_Int;
  }

  @Override
  public void persistent(DataOutput out) throws IOException {
    out.writeInt(beginline);
    out.writeInt(endline);
  }

  @Override
  public void unpersistent(DataInput in) throws IOException {
    beginline = in.readInt();
    endline = in.readInt();
  }

  @Override
  public int compareTo(ILineIndex o) {
    return endline - o.endline;
  }

  public int beginline() {
    return beginline;
  }

  public int endline() {
    return endline;
  }

  public void setbeginline(int beginLine) {
    this.beginline = beginLine;
  }

  public void setendline(int endline) {
    this.endline = endline;
  }

  public int recnum() {
    return this.endline - this.beginline + 1;
  }
}
