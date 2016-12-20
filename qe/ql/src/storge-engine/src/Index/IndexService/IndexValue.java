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

import org.apache.hadoop.io.Writable;

public class IndexValue implements Writable {

  private int fileindex = -1;
  private int rowid = -1;

  public IndexValue() {
  }

  public IndexValue(int fileindex, int rowid) {
    this.fileindex = fileindex;
    this.rowid = rowid;
  }

  public void setFileindex(int fileindex) {
    this.fileindex = fileindex;
  }

  public void setRowid(int rowid) {
    this.rowid = rowid;

  }

  public int getFileindex() {
    return fileindex;
  }

  public int getRowid() {
    return rowid;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.fileindex = in.readShort();
    this.rowid = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeShort(fileindex);
    out.writeInt(rowid);

  }

  public String show() {
    return fileindex + " " + rowid + " ";
  }

}
