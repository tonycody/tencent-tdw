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
import java.util.ArrayList;

public class ISegmentIndex implements IPersistable {
  private IFileInfo fileInfo;
  private int segnum;
  private ArrayList<Long> segoffsets;
  private ArrayList<Integer> seglens;
  private ArrayList<IKeyIndex> keyindexs;
  private ArrayList<ILineIndex> lineindexs;
  private IFieldType keyfieldType;
  private long segindexoffset;

  public ISegmentIndex(IFileInfo fileInfo) {
    this.fileInfo = fileInfo;
    this.segnum = 0;
    this.segoffsets = new ArrayList<Long>();
    this.seglens = new ArrayList<Integer>();
    if (fileInfo.havekeyindex()) {
      this.keyindexs = new ArrayList<IKeyIndex>();
      this.keyfieldType = fileInfo.head().fieldMap().fieldtypes()
          .get(fileInfo.head().getPrimaryIndex());
    }
    if (fileInfo.havelineindex()) {
      this.lineindexs = new ArrayList<ILineIndex>();
    }
  }

  @Override
  public void persistent(DataOutput out) throws IOException {
    out.writeInt(segnum);
    for (long offset : segoffsets) {
      out.writeLong(offset);
    }
    for (int len : seglens) {
      out.writeInt(len);
    }
    if (fileInfo.havekeyindex()) {
      for (IKeyIndex key : keyindexs) {
        key.persistent(out);
      }
    }
    if (fileInfo.havelineindex()) {
      for (ILineIndex line : lineindexs) {
        line.persistent(out);
      }
    }
    out.writeLong(segindexoffset);
  }

  @Override
  public void unpersistent(DataInput in) throws IOException {
    this.segnum = in.readInt();
    for (int i = 0; i < segnum; i++) {
      segoffsets.add(in.readLong());
    }
    for (int i = 0; i < segnum; i++) {
      seglens.add(in.readInt());
    }
    if (fileInfo.havekeyindex()) {
      for (int i = 0; i < segnum; i++) {
        IKeyIndex keyIndex = new IKeyIndex(keyfieldType);
        keyIndex.unpersistent(in);
        keyindexs.add(keyIndex);
      }
    }
    if (fileInfo.havelineindex()) {
      for (int i = 0; i < segnum; i++) {
        ILineIndex lineIndex = new ILineIndex();
        lineIndex.unpersistent(in);
        lineindexs.add(lineIndex);
      }
    }
  }

  public void update(ISegmentInfo seginfo) {
    this.segoffsets.add(seginfo.offset());
    this.seglens.add(seginfo.segLen());
    segnum++;
    if (fileInfo.havekeyindex()) {
      keyindexs.add(new IKeyIndex(seginfo.beginKey(), seginfo.endKey(), seginfo
          .recnum()));
    }
    if (fileInfo.havelineindex()) {
      lineindexs.add(new ILineIndex(seginfo.beginLine(), seginfo.endLine()));
    }
  }

  public int getSegid(int line) {
    int segid = BinarySearchFirst.binarySearchFirst(lineindexs.toArray(),
        new ILineIndex(line, line));
    if (segid < 0) {
      segid = -1 * (segid + 1);
    }
    return segid;
  }

  public long getSegOffset(int segid) {
    if (segid >= segoffsets.size())
      return -1;
    return segoffsets.get(segid);
  }

  public int getseglen(int segid) {
    return seglens.get(segid);
  }

  public int getSegid(IFieldValue fv) {
    int segid = BinarySearchFirst.binarySearchFirst(keyindexs.toArray(),
        new IKeyIndex(fv, fv, 1));
    if (segid < 0) {
      segid = -1 * (segid + 1);
    }
    return segid;
  }

  public int getSegnum() {
    return segnum;
  }

  public ILineIndex getILineIndex(int segid) {
    return this.lineindexs.get(segid);
  }

  public IKeyIndex getIKeyIndex(int segid) {
    return this.keyindexs.get(segid);
  }

  public void setofflen(long pos) {
    this.segindexoffset = pos;
  }
}
