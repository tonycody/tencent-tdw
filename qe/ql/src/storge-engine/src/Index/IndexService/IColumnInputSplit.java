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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;

import FormatStorage1.IRecord;

@SuppressWarnings("deprecation")
public class IColumnInputSplit implements InputSplit {
  public static final Log LOG = LogFactory.getLog(IColumnInputSplit.class);

  Path file;
  long length;

  boolean splitbyline;
  int beginline;
  IRecord.IFValue beginkey;
  int recnum;
  boolean wholefileASasplit = false;

  private String[] hosts;

  public IColumnInputSplit() {
  }

  public IColumnInputSplit(Path filename, long length, String[] hosts) {
    this.file = filename;
    this.hosts = hosts;
    this.length = length;
    this.splitbyline = true;
    this.wholefileASasplit = true;
  }

  public IColumnInputSplit(Path filename, long length, int beginline,
      int recnum, String[] hosts) {
    this.file = filename;
    this.length = length;
    this.beginline = beginline;
    this.recnum = recnum;
    this.splitbyline = true;
    this.hosts = hosts;
  }

  public IColumnInputSplit(Path filename, long length,
      IRecord.IFValue beginkey, int recnum, String[] hosts) {
    this.file = filename;
    this.length = length;
    this.beginkey = beginkey;
    this.recnum = recnum;
    this.splitbyline = false;
    this.hosts = hosts;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.file = new Path(in.readUTF());
    this.splitbyline = in.readBoolean();
    this.wholefileASasplit = in.readBoolean();
    if (splitbyline) {
      if (!wholefileASasplit) {
        this.beginline = in.readInt();
        this.recnum = in.readInt();
      }
    } else {
      this.beginkey = new IRecord.IFValue();
      beginkey.readFields(in);
      this.recnum = in.readInt();
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(file.toString());
    out.writeBoolean(splitbyline);
    out.writeBoolean(this.wholefileASasplit);
    if (splitbyline) {
      if (!wholefileASasplit) {
        out.writeInt(beginline);
        out.writeInt(recnum);
      }
    } else {
      beginkey.write(out);
      out.writeInt(recnum);
    }
  }

  @Override
  public long getLength() throws IOException {
    return length;
  }

  @Override
  public String[] getLocations() throws IOException {
    return hosts;
  }

  public Object getPath() {
    return file;
  }
}
