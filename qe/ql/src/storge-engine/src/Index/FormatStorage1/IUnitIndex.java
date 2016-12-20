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
import java.util.ArrayList;

import org.apache.hadoop.fs.FSDataInputStream;

import Comm.ConstVar;

public class IUnitIndex implements IPersistable {
  private IFileInfo fileInfo;
  private int unitnum = 0;
  private IRecord.IFType keyfieldType;
  private ArrayList<Integer> unitlens;
  private ArrayList<Long> unitoffsets;
  private ArrayList<IKeyIndex> keyindexs;
  private ArrayList<ILineIndex> lineindexs;
  private long unitindexoffset;

  public IUnitIndex(IFileInfo fileInfo) throws IOException {
    this.fileInfo = fileInfo;
    this.unitoffsets = new ArrayList<Long>();
    this.unitlens = new ArrayList<Integer>();
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

    if (fileInfo.formatfiletype() == ConstVar.OldFormatFile) {
      long keyIndexOffset = -1;
      long lineIndexOffset = -1;
      if (fileInfo.havekeyindex()) {
        keyIndexOffset = fileInfo.out().getPos();
        for (IKeyIndex key : keyindexs) {
          key.beginkey().persistent(out);
          key.endkey().persistent(out);
        }
      }
      lineIndexOffset = fileInfo.out().getPos();
      for (int i = 0; i < this.unitnum; i++) {
        ILineIndex li = lineindexs.get(i);
        out.writeInt(li.beginline());
        out.writeInt(li.endline() + 1);
        out.writeLong(unitoffsets.get(i));
        out.writeLong(unitlens.get(i));
        out.writeInt(i);
      }

      if (unitnum == 0) {
        out.writeInt(0);
      } else
        out.writeInt(lineindexs.get(unitnum - 1).endline()
            - lineindexs.get(0).beginline() + 1);
      out.writeInt(unitnum);
      out.writeLong(keyIndexOffset);
      out.writeLong(lineIndexOffset);

    } else {
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
      out.writeLong(unitindexoffset);
    }
  }

  @Override
  public void unpersistent(DataInput in) throws IOException {
    if (fileInfo.formatfiletype() == ConstVar.OldFormatFile) {
      FSDataInputStream fsdin = (FSDataInputStream) in;
      int recordNum = fsdin.readInt();
      this.unitnum = fsdin.readInt();
      long keyIndexOffset = fsdin.readLong();
      long lineIndexOffset = fsdin.readLong();

      if (keyIndexOffset != -1) {
        fsdin.seek(keyIndexOffset);
        for (int i = 0; i < unitnum; i++) {
          int beginkey = in.readInt();
          int endkey = in.readInt();
          keyindexs.add(new IKeyIndex(new IRecord.IFValue(beginkey, -1),
              new IRecord.IFValue(endkey, -1), 0));
        }
      }

      if (lineIndexOffset == -1 && recordNum > 0) {
        throw new IOException("line index offset error when load seg index:"
            + lineIndexOffset);
      }

      if (recordNum > 0) {
        fsdin.seek(lineIndexOffset);

        for (int i = 0; i < unitnum; i++) {
          ILineIndex lineIndex = new ILineIndex();
          lineIndex.setbeginline(in.readInt());
          lineIndex.setendline(in.readInt() - 1);
          unitoffsets.add(in.readLong());
          unitlens.add((int) in.readLong());
          @SuppressWarnings("unused")
          int idx = in.readInt();
          lineindexs.add(lineIndex);
          if (fileInfo.havekeyindex())
            keyindexs.get(i).setRecnum(
                lineIndex.endline() - lineIndex.beginline() + 1);
        }
      }

    } else {

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
  }

  public void update(IUnitInfo iunitinfo) {
    this.unitoffsets.add(iunitinfo.offset());
    this.unitlens.add(iunitinfo.unitlen());
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

  public int getUnitid(IRecord.IFValue fv) {
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

  public int getUnitLen(int unitid) {
    return this.unitlens.get(unitid);
  }

  public ILineIndex getLineIndex(int unitid) {
    return this.lineindexs.get(unitid);
  }

  public IKeyIndex getKeyIndex(int unitid) {
    return this.keyindexs.get(unitid);
  }

  public void setunitindexoffset(long unitindexoffset) {
    this.unitindexoffset = unitindexoffset;
  }

  public int plus1currunitindexsize() {
    if (fileInfo.formatfiletype() == ConstVar.OldFormatFile) {
      int indexlen = 0;
      if (fileInfo.havekeyindex()) {
        indexlen += (keyindexs.size() + 1) * 2 * keyfieldType.len();
      }
      if (fileInfo.havelineindex()) {
        indexlen += (lineindexs.size() + 1)
            * (ConstVar.Sizeof_Int * 3 + ConstVar.Sizeof_Long * 2);
      }
      indexlen += ConstVar.Sizeof_Long * 2 + ConstVar.Sizeof_Int * 2;
      return indexlen;
    } else {
      int indexlen = ConstVar.Sizeof_Int;
      indexlen += (unitoffsets.size() + 1) * ConstVar.Sizeof_Long;
      if (fileInfo.havekeyindex()) {
        indexlen += (keyindexs.size() + 1) * 2 * keyfieldType.len();
      }
      if (fileInfo.havelineindex()) {
        indexlen += (lineindexs.size() + 1) * 2 * ConstVar.Sizeof_Int;
      }
      indexlen += ConstVar.Sizeof_Long + ConstVar.Sizeof_Int;
      return indexlen;
    }
  }

  public int currunitindexsize() {
    if (fileInfo.formatfiletype() == ConstVar.OldFormatFile) {
      int indexlen = 0;
      if (fileInfo.havekeyindex()) {
        indexlen += keyindexs.size() * 2 * keyfieldType.len();
      }
      if (fileInfo.havelineindex()) {
        indexlen += lineindexs.size()
            * (ConstVar.Sizeof_Int * 3 + ConstVar.Sizeof_Long * 2);
      }
      indexlen += ConstVar.Sizeof_Long * 2 + ConstVar.Sizeof_Int * 2;
      return indexlen;
    } else {
      int indexlen = ConstVar.Sizeof_Int;
      indexlen += (unitoffsets.size() + 1) * ConstVar.Sizeof_Long;
      if (fileInfo.havekeyindex()) {
        indexlen += (keyindexs.size() + 1) * 2 * keyfieldType.len();
      }
      if (fileInfo.havelineindex()) {
        indexlen += (lineindexs.size() + 1) * 2 * ConstVar.Sizeof_Int;
      }
      indexlen += ConstVar.Sizeof_Long + ConstVar.Sizeof_Int;
      return indexlen;
    }
  }
}
