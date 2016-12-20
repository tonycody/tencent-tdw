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

import Comm.ConstVar;

public class IUnitIndex implements IPersistable {
  private IFileInfo fileInfo;
  private int unitnum = 0;
  private IFieldType keyfieldType;
  private ArrayList<Long> unitoffsets;
  private ArrayList<IKeyIndex> keyindexs;
  private ArrayList<ILineIndex> lineindexs;
  private long metaoffset;

  public IUnitIndex(IFileInfo fileInfo) throws IOException {
    this.fileInfo = fileInfo;
    this.unitoffsets = new ArrayList<Long>();
    if (fileInfo.havekeyindex()) {
      this.keyfieldType = fileInfo.head().fieldMap().fieldtypes()
          .get(fileInfo.head().getPrimaryIndex());
      this.keyindexs = new ArrayList<IKeyIndex>();
    }
    if (fileInfo.havelineindex())
      this.lineindexs = new ArrayList<ILineIndex>();
  }

  @Override
  public void persistent(DataOutput out) throws IOException {
    out.writeInt(unitnum);
    for (long offset : unitoffsets) {
      out.writeLong(offset);
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
    out.writeLong(metaoffset);
  }

  @Override
  public void unpersistent(DataInput in) throws IOException {
    this.unitnum = in.readInt();
    for (int i = 0; i < unitnum; i++) {
      unitoffsets.add(in.readLong());
    }
    if (fileInfo.havekeyindex()) {
      for (int i = 0; i < unitnum; i++) {
        IKeyIndex keyIndex = new IKeyIndex(keyfieldType);
        keyIndex.unpersistent(in);
        keyindexs.add(keyIndex);
      }
    }
    if (fileInfo.havelineindex()) {
      for (int i = 0; i < unitnum; i++) {
        ILineIndex lineIndex = new ILineIndex();
        lineIndex.unpersistent(in);
        lineindexs.add(lineIndex);
      }
    }
  }

  public void update(IUnitInfo iunitinfo) {
    this.unitoffsets.add(iunitinfo.offset());
    unitnum++;
    if (fileInfo.havekeyindex()) {
      this.keyindexs.add(new IKeyIndex(iunitinfo.beginKey(),
          iunitinfo.endKey(), iunitinfo.recordNum()));
    }
    if (fileInfo.havelineindex()) {
      this.lineindexs.add(new ILineIndex(iunitinfo.beginLine(), iunitinfo
          .endLine()));
    }
  }

  public int getUnitid(int line) {
    int unitid = BinarySearchFirst.binarySearchFirst(lineindexs.toArray(),
        new ILineIndex(line, line));
    if (unitid < 0)
      unitid = -1 * (unitid + 1);
    return unitid;
  }

  public int getUnitid(IFieldValue fv) {
    int unitid = BinarySearchFirst.binarySearchFirst(keyindexs.toArray(),
        new IKeyIndex(fv, fv, 1));
    if (unitid < 0)
      unitid = -1 * (unitid + 1);
    return unitid;
  }

  public int getUnitnum() {
    return unitnum;
  }

  public long getUnitOffset(int unitid) {
    if (unitid >= unitoffsets.size())
      return -1;
    return unitoffsets.get(unitid);
  }

  public ILineIndex getLineIndex(int unitid) {
    return this.lineindexs.get(unitid);
  }

  public IKeyIndex getKeyIndex(int unitid) {
    return this.keyindexs.get(unitid);
  }

  public void setmetaoffset(long pos) {
    metaoffset = pos;
  }

  public int plus1len() {
    int indexlen = ConstVar.Sizeof_Int;
    indexlen += (unitoffsets.size() + 1) * ConstVar.Sizeof_Long;
    if (fileInfo.havekeyindex()) {
      indexlen += (keyindexs.size() + 1) * 2 * keyfieldType.getLen();
    }
    if (fileInfo.havelineindex()) {
      indexlen += (lineindexs.size() + 1) * 2 * ConstVar.Sizeof_Int;
    }
    indexlen += ConstVar.Sizeof_Long + ConstVar.Sizeof_Int;
    return indexlen;
  }
}
