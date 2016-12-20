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
import java.util.Arrays;

import org.apache.hadoop.fs.FSDataInputStream;

import Comm.ConstVar;

public class ISegmentIndex implements IPersistable {
  private IFileInfo fileInfo;
  private int segnum;
  private ArrayList<Long> segoffsets;
  private ArrayList<Integer> seglens;
  private ArrayList<IKeyIndex> keyindexs;
  private ArrayList<ILineIndex> lineindexs;
  private IRecord.IFType keyfieldType;
  private long segindexoffset;
  private int recnum = -1;

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
      if (fileInfo.havelineindex()) {
        lineIndexOffset = fileInfo.out().getPos();
        for (int i = 0; i < this.segnum; i++) {
          ILineIndex li = lineindexs.get(i);
          out.writeInt(li.beginline());
          out.writeInt(li.endline() + 1);
          out.writeLong(segoffsets.get(i));
          out.writeLong(seglens.get(i));
          out.writeInt(i);
        }
      }

      if (segnum == 0) {
        out.writeInt(0);
      } else {
        out.writeInt(lineindexs.get(segnum - 1).endline()
            - lineindexs.get(0).beginline() + 1);
      }
      out.writeInt(segnum);
      out.writeLong(keyIndexOffset);
      out.writeLong(lineIndexOffset);

    } else {

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
  }

  @Override
  public void unpersistent(DataInput in) throws IOException {
    if (fileInfo.formatfiletype() == ConstVar.OldFormatFile) {
      FSDataInputStream fsdin = (FSDataInputStream) in;
      int recordNum = fsdin.readInt();
      this.segnum = fsdin.readInt();
      long keyIndexOffset = fsdin.readLong();
      long lineIndexOffset = fsdin.readLong();

      if (keyIndexOffset != -1) {
        fsdin.seek(keyIndexOffset);
        for (int i = 0; i < segnum; i++) {
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

        for (int i = 0; i < segnum; i++) {
          ILineIndex lineIndex = new ILineIndex();
          lineIndex.setbeginline(in.readInt());
          lineIndex.setendline(in.readInt() - 1);
          segoffsets.add(in.readLong());
          seglens.add((int) in.readLong());
          @SuppressWarnings("unused")
          int idx = in.readInt();
          lineindexs.add(lineIndex);
          if (fileInfo.havekeyindex())
            keyindexs.get(i).setRecnum(
                lineIndex.endline() - lineIndex.beginline() + 1);
        }
      }

    } else {
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

  public int[] getsigidsinoffsetrange(long offset, int len) {
    Object[] offsets = this.segoffsets.toArray();
    int segid = Arrays.binarySearch(offsets, offset);
    if (segid < 0) {
      segid = -segid - 1;
    }
    long offset1 = offset + len;
    int segid1 = Arrays.binarySearch(offsets, offset1);
    if (segid1 < 0) {
      segid1 = -segid1 - 1;
    }
    int[] sids = { segid, segid1 };
    return sids;
  }

  public long getSegOffset(int segid) {
    if (segid >= segoffsets.size())
      return -1;
    return segoffsets.get(segid);
  }

  public int getseglen(int segid) {
    return seglens.get(segid);
  }

  public int getSegid(IRecord.IFValue fv) {
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

  public void setkeysegindexs(ArrayList<IKeyIndex> keyindexs) {
    this.keyindexs = keyindexs;
  }

  public void setlinesegindexs(ArrayList<ILineIndex> lineindexs) {
    this.lineindexs = lineindexs;
  }

  public void setoffsets(ArrayList<Long> segoffsets) {
    this.segoffsets = segoffsets;
  }

  public int recnum() {
    if (this.recnum == -1 && this.lineindexs != null
        && this.lineindexs.size() > 0) {
      recnum = this.lineindexs.get(lineindexs.size() - 1).endline() + 1;
    }
    return recnum;
  }
}
