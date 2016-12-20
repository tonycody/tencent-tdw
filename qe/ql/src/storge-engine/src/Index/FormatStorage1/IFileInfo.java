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

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import Comm.ConstVar;
import FormatStorage1.IUnitPool.UnitOperator;

public class IFileInfo {

  private FileSystem fs;

  private String fileName;
  private Configuration conf;
  private long confUnitSize;
  private long confSegmentSize;

  private IHead head;
  private boolean var;
  private boolean compressed;
  private boolean havelineindex = true;
  private boolean havekeyindex;

  private long filelength;
  private int currentline;

  private byte workStatus;
  private FSDataInputStream in;
  private FSDataOutputStream out;
  private IUnitPool unitPool;
  private UnitOperator unitOperator;
  private int recordlen;

  private int formatfiletype = ConstVar.NewFormatMagic;

  public boolean printlog = false;

  public IFileInfo(Configuration conf) throws IOException {
    this.workStatus = ConstVar.WS_Init;
    this.conf = conf;
    fs = FileSystem.get(conf);

    this.confSegmentSize = conf.getLong("dfs.block.size",
        ConstVar.DefaultSegmentSize);
    this.confUnitSize = conf.getLong(ConstVar.ConfUnitSize,
        ConstVar.DefaultUnitSize);
    this.conf.setInt("io.compression.codec.lzo.buffersize", 128 * 1024);
    this.currentline = 0;
    this.printlog = conf.getBoolean("printlog", false);
  }

  private void initialHead() throws IOException {
    var = head.getVar() == 1 ? true : false;
    if (!var) {
      Collection<IRecord.IFType> fts = head.fieldMap().fieldtypes().values();
      recordlen = fts.size() / 8 + 1;
      for (IRecord.IFType ft : fts) {
        recordlen += ft.len();
      }
    }
    compressed = head.getCompress() == 1 ? true : false;
    havelineindex = head.lineindex() == 1 ? true : false;
    havekeyindex = head.getPrimaryIndex() > -1 ? true : false;
    unitPool = new IUnitPool(this);
    unitOperator = unitPool.unitOperator();
  }

  public void initialize(String fileName, IHead head) throws IOException {
    this.workStatus = ConstVar.WS_Write;
    this.fileName = fileName;
    Path p = new Path(fileName);
    this.fs = p.getFileSystem(conf);
    this.out = fs.create(p);
    this.head = head;
    this.formatfiletype = head.formatfiletype();
    this.head.persistent(out);
    this.initialHead();
  }

  public void initialize(String fileName) throws IOException {
    this.workStatus = ConstVar.WS_Read;
    this.fileName = fileName;
    Path p = new Path(fileName);
    this.fs = p.getFileSystem(conf);

    this.filelength = fs.getFileStatus(p).getLen();
    this.in = fs.open(p);
    this.head = new IHead();
    this.head.unpersistent(in);
    this.formatfiletype = head.formatfiletype();
    this.initialHead();
  }

  public void uninitialize() {
    if (this.in != null) {
      try {
        this.in.close();
      } catch (Exception x) {

      }
    }

    if (this.out != null) {
      try {
        this.out.close();
      } catch (Exception x) {

      }
    }
  }

  public String filename() {
    return this.fileName;
  }

  public Configuration conf() {
    return conf;
  }

  public IHead head() {
    return head;
  }

  public byte workStatus() {
    return workStatus;
  }

  public FSDataInputStream in() {
    return in;
  }

  public FSDataOutputStream out() {
    return out;
  }

  public long confSegmentSize() {
    return confSegmentSize;
  }

  public long confUnitSize() {
    return this.confUnitSize;
  }

  public boolean havelineindex() {
    return havelineindex;
  }

  public boolean havekeyindex() {
    return havekeyindex;
  }

  public boolean isVar() {
    return this.var;
  }

  public boolean compressed() {
    return this.compressed;
  }

  public int currentline() {
    return this.currentline;
  }

  public UnitOperator unitOperator() {
    return this.unitOperator;
  }

  public int recordlen() {
    return recordlen;
  }

  public long filelength() {
    return this.filelength;
  }

  public void increasecurrentline() {
    this.currentline++;
  }

  public void setcurrentline(int line) {
    this.currentline = line;
  }

  public int formatfiletype() {
    return formatfiletype;
  }
}
