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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.DefaultCodec;

import com.hadoop.compression.lzo.LzoCodec;

import Comm.ConstVar;
import Comm.Util;

public class IUnitPool {
  private UnitOperator unitOperator;

  private static final int unitbuffersize = (int) (2.5 * 1024 * 1024);

  class UnitData {
    String unitname;
    byte[] unitdata;
    int unitdatalen;

    public UnitData(String unitname, byte[] unitdata, int unitdatalen) {
      this.unitname = unitname;
      this.unitdata = unitdata;
      this.unitdatalen = unitdatalen;
    }
  }

  public IUnitPool(IFileInfo fileInfo) throws IOException {
    unitOperator = new UnitOperator(fileInfo);
  }

  public class UnitOperator {

    private IFileInfo fileInfo;
    private IUnitInfo iunitinfo;

    private UnitData unitdata;

    private DataOutputBuffer chunksWriteBuffer;
    private DataOutputBuffer metasWriteBuffer;
    private CompressionOutputStream compressionOutputStream;
    private CompressionOutputStream metacompressionOutputStream;
    private DataOutputStream compressionDataOutputStream;
    private DataOutputStream metacompressionDataOutputStream;

    private SeekDIBuffer chunksReadBuffer;
    private DataInputBuffer metasReadBuffer = null;
    private CompressionInputStream compressionInputStream;
    private CompressionInputStream metacompressionInputStream;

    private int[] offsets = new int[100];

    private long metaOffset = -1;
    private int currunitline = 0;

    private long currentwritingunitlength = 0;
    private long currentwritingunitreclength = 0;

    byte[] tmpdata = new byte[unitbuffersize];
    byte[] tmpmetaoff = new byte[ConstVar.Sizeof_Long];

    DataOutputBuffer tmpdob = new DataOutputBuffer();
    DataInputBuffer tmpdib = new DataInputBuffer();

    public UnitOperator(IFileInfo fileInfo) throws IOException {
      this.unitdata = new UnitData(null, new byte[unitbuffersize], 0);
      this.fileInfo = fileInfo;
      if (fileInfo.workStatus() == ConstVar.WS_Write) {
        chunksWriteBuffer = new DataOutputBuffer();
        metasWriteBuffer = new DataOutputBuffer();
        if (fileInfo.compressed()) {
          fileInfo.head().getCompressStyle();
          if (fileInfo.head().getCompressStyle() == -1) {
            DefaultCodec codec = new DefaultCodec();
            codec.setConf(fileInfo.conf());
            compressionOutputStream = codec
                .createOutputStream(chunksWriteBuffer);
            metacompressionOutputStream = codec
                .createOutputStream(metasWriteBuffer);
          } else {
            LzoCodec codec = new LzoCodec();
            codec.setConf(fileInfo.conf());
            compressionOutputStream = codec
                .createOutputStream(chunksWriteBuffer);
            metacompressionOutputStream = codec
                .createOutputStream(metasWriteBuffer);
          }
          compressionDataOutputStream = new DataOutputStream(
              compressionOutputStream);
          metacompressionDataOutputStream = new DataOutputStream(
              metacompressionOutputStream);
        }
      } else {
        chunksReadBuffer = new SeekDIBuffer();
        metasReadBuffer = new DataInputBuffer();
        if (fileInfo.compressed()) {
          if (fileInfo.head().getCompressStyle() == -1) {
            DefaultCodec codec = new DefaultCodec();
            codec.setConf(fileInfo.conf());
            compressionInputStream = codec.createInputStream(chunksReadBuffer);
            metacompressionInputStream = codec
                .createInputStream(metasReadBuffer);
          } else {
            LzoCodec codec = new LzoCodec();
            codec.setConf(fileInfo.conf());
            compressionInputStream = codec.createInputStream(chunksReadBuffer);
            metacompressionInputStream = codec
                .createInputStream(metasReadBuffer);
          }
        }
      }
    }

    public boolean addRecord(IRecord record) throws IOException {
      if (fileInfo.workStatus() != ConstVar.WS_Write)
        return false;

      int recordlen = record.len();
      currentwritingunitlength += recordlen;
      currentwritingunitreclength += recordlen;
      if (fileInfo.isVar()) {
        if (fileInfo.formatfiletype() == ConstVar.OldFormatFile) {
          currentwritingunitlength += ConstVar.Sizeof_Long;
          if (metaOffset < 0)
            metaOffset = iunitinfo.offset();
          if (fileInfo.compressed()) {
            metacompressionDataOutputStream.writeLong(metaOffset);
          } else {
            metasWriteBuffer.writeLong(metaOffset);
          }
          metaOffset += recordlen;
        } else {
          currentwritingunitlength += ConstVar.Sizeof_Short;
          if (fileInfo.compressed()) {
            metacompressionDataOutputStream.writeShort(recordlen);
          } else {
            metasWriteBuffer.writeShort(recordlen);
          }
        }
      }

      if (fileInfo.compressed()) {
        record.persistent(compressionDataOutputStream);
      } else {
        record.persistent(chunksWriteBuffer);
      }
      return true;
    }

    public void persistentUnit() throws IOException {
      if (fileInfo.compressed()) {
        compressionDataOutputStream.flush();
        compressionOutputStream.finish();
        if (fileInfo.isVar()) {
          metacompressionDataOutputStream.flush();
          metacompressionOutputStream.finish();
        }
      }
      if (fileInfo.formatfiletype() == ConstVar.OldFormatFile) {
        fileInfo.out().write(chunksWriteBuffer.getData(), 0,
            chunksWriteBuffer.getLength());
        chunksWriteBuffer.reset();
        long metaoffset = fileInfo.out().getPos();
        if (fileInfo.isVar()) {
          fileInfo.out().write(metasWriteBuffer.getData(), 0,
              metasWriteBuffer.getLength());
          metasWriteBuffer.reset();
        }
        fileInfo.out().writeInt(iunitinfo.recordNum());
        fileInfo.out().writeLong(metaoffset);
        fileInfo.out().writeInt((int) currentwritingunitreclength);
      } else {
        int len = chunksWriteBuffer.getLength();
        if (fileInfo.isVar()) {
          len += metasWriteBuffer.getLength();
        }
        fileInfo.out().writeInt(len);
        if (fileInfo.isVar()) {
          fileInfo.out().writeInt(metasWriteBuffer.getLength());
          fileInfo.out().write(metasWriteBuffer.getData(), 0,
              metasWriteBuffer.getLength());
          metasWriteBuffer.reset();
        }
        fileInfo.out().write(chunksWriteBuffer.getData(), 0,
            chunksWriteBuffer.getLength());
        chunksWriteBuffer.reset();
      }
      if (fileInfo.compressed()) {
        compressionOutputStream.resetState();
        if (fileInfo.isVar()) {
          metacompressionOutputStream.resetState();
        }
      }
    }

    public void rebuildUnit(long curroffset) throws IOException {
      if (fileInfo.formatfiletype() == ConstVar.OldFormatFile
          && fileInfo.isVar()) {
        IFormatDataFile.LOG.info("rebuild a unit:\tunitid:" + iunitinfo.segid()
            + ":" + iunitinfo.unitid());
        if (fileInfo.compressed()) {
          metacompressionDataOutputStream.flush();
          metacompressionOutputStream.finish();
        }

        DataOutputBuffer dobmeta = new DataOutputBuffer();

        dobmeta.write(metasWriteBuffer.getData(), 0,
            metasWriteBuffer.getLength());
        metasWriteBuffer.reset();

        if (metasReadBuffer == null) {
          metasReadBuffer = new DataInputBuffer();
          if (fileInfo.compressed()) {
            if (fileInfo.head().getCompressStyle() == -1) {
              DefaultCodec codec = new DefaultCodec();
              codec.setConf(fileInfo.conf());
              metacompressionInputStream = codec
                  .createInputStream(metasReadBuffer);
            } else {
              LzoCodec codec = new LzoCodec();
              codec.setConf(fileInfo.conf());
              metacompressionInputStream = codec
                  .createInputStream(metasReadBuffer);
            }
          }
        }
        metasReadBuffer.reset(dobmeta.getData(), 0, dobmeta.getLength());

        long oldoffset = iunitinfo.offset();
        tmpdob.reset();
        if (fileInfo.compressed()) {
          metacompressionOutputStream.resetState();
          metacompressionInputStream.resetState();
          DataInputStream metaInputStream = new DataInputStream(
              metacompressionInputStream);
          for (int i = 0; i < iunitinfo.recordNum(); i++) {
            long offseti = metaInputStream.readLong() - oldoffset + curroffset;
            metacompressionDataOutputStream.writeLong(offseti);
          }
        } else {
          for (int i = 0; i < iunitinfo.recordNum(); i++) {
            long offseti = metasReadBuffer.readLong() - oldoffset + curroffset;

            metasWriteBuffer.writeLong(offseti);
          }
        }
      }
    }

    private void unpersistentUnit() throws IOException {
      if (fileInfo.formatfiletype() == ConstVar.OldFormatFile) {
        fileInfo.in().seek(iunitinfo.offset());
        if (tmpdata.length < iunitinfo.unitlen())
          tmpdata = new byte[iunitinfo.unitlen()];
        int readlen = 0;
        while (readlen < iunitinfo.unitlen()) {
          readlen += fileInfo.in().read(tmpdata, readlen,
              iunitinfo.unitlen() - readlen);
        }
        tmpdib.reset(tmpdata, iunitinfo.unitlen()
            - ConstVar.DataChunkMetaOffset, ConstVar.DataChunkMetaOffset);
        int recordNum = tmpdib.readInt();
        int metaOffset = (int) (tmpdib.readLong() - iunitinfo.offset());
        @SuppressWarnings("unused")
        int chunksLen = tmpdib.readInt();

        tmpdob.reset();
        if (fileInfo.isVar()) {
          metasReadBuffer.reset(tmpdata, metaOffset, iunitinfo.unitlen()
              - ConstVar.DataChunkMetaOffset - metaOffset);
          long lastoffset = iunitinfo.offset();
          long offseti = lastoffset;
          if (fileInfo.compressed()) {
            metacompressionInputStream.resetState();
            DataInputStream metaInputStream = new DataInputStream(
                metacompressionInputStream);
            if (recordNum > 0)
              metaInputStream.readLong();
            for (int i = 1; i < recordNum; i++) {
              offseti = metaInputStream.readLong();
              tmpdob.writeShort((int) (offseti - lastoffset));
              lastoffset = offseti;
            }
          } else {
            if (recordNum > 0)
              metasReadBuffer.readLong();
            for (int i = 1; i < recordNum; i++) {
              offseti = metasReadBuffer.readLong();
              tmpdob.writeShort((int) (offseti - lastoffset));
              lastoffset = offseti;
            }
          }
          tmpdob.writeShort((int) (iunitinfo.offset() - lastoffset
              + iunitinfo.unitlen() - ConstVar.DataChunkMetaOffset));
        }

        chunksReadBuffer.reset(tmpdata, 0, metaOffset);
        if (fileInfo.compressed()) {
          compressionInputStream.resetState();
          IOUtils.copyBytes(compressionInputStream, tmpdob, fileInfo.conf(),
              false);
        } else {
          IOUtils.copyBytes(chunksReadBuffer, tmpdob, fileInfo.conf(), false);
        }

        tmpdob.flush();
        unitdata.unitdatalen = tmpdob.getLength();
        if (unitdata.unitdata.length < unitdata.unitdatalen)
          unitdata.unitdata = new byte[unitdata.unitdatalen];
        System.arraycopy(tmpdob.getData(), 0, unitdata.unitdata, 0,
            unitdata.unitdatalen);
      } else {

        fileInfo.in().seek(iunitinfo.offset());
        int len = fileInfo.in().readInt();
        int metalen = 0;
        if (fileInfo.isVar())
          metalen = fileInfo.in().readInt();
        if (!fileInfo.compressed()) {
          unitdata.unitdatalen = len;
          if (unitdata.unitdata.length < unitdata.unitdatalen)
            unitdata.unitdata = new byte[unitdata.unitdatalen];
          fileInfo.in().read(unitdata.unitdata, 0, len);
        } else {
          if (tmpdata.length < len)
            tmpdata = new byte[len];

          fileInfo.in().read(tmpdata, 0, len);
          tmpdob.reset();
          if (fileInfo.isVar()) {
            metasReadBuffer.reset(tmpdata, 0, metalen);
            metacompressionInputStream.resetState();
            IOUtils.copyBytes(metacompressionInputStream, tmpdob,
                fileInfo.conf(), false);
          }
          chunksReadBuffer.reset(tmpdata, metalen, len - metalen);
          compressionInputStream.resetState();
          IOUtils.copyBytes(compressionInputStream, tmpdob, fileInfo.conf(),
              false);
          tmpdob.flush();

          unitdata.unitdatalen = tmpdob.getLength();
          if (unitdata.unitdata.length < unitdata.unitdatalen)
            unitdata.unitdata = new byte[unitdata.unitdatalen];
          System.arraycopy(tmpdob.getData(), 0, unitdata.unitdata, 0,
              unitdata.unitdatalen);
        }
      }
    }

    public boolean seek(IUnitInfo iunitinfo, int line) throws IOException {
      if (!fileInfo.havelineindex())
        return false;

      this.reset(iunitinfo);
      this.currunitline = line;
      return chunksReadBuffer.seek(offsets[line - iunitinfo.beginLine()]);
    }

    public boolean seek(IUnitInfo iunitinfo, IRecord.IFValue fv)
        throws IOException {
      if (!fileInfo.havekeyindex())
        return false;

      this.reset(iunitinfo);

      int relativeline = bisearch(fv);
      this.currunitline = iunitinfo.beginLine() + relativeline;
      chunksReadBuffer.seek(offsets[relativeline]);
      return true;
    }

    public boolean next(IRecord record) throws IOException {
      if (chunksReadBuffer.available() <= 0
          || this.currunitline > iunitinfo.endLine())
        return false;
      if (fileInfo.formatfiletype() == ConstVar.OldFormatFile) {
        chunksReadBuffer
            .seek(offsets[this.currunitline - iunitinfo.beginLine()]);
      }

      record.unpersistent(chunksReadBuffer);
      currunitline++;
      return true;
    }

    public boolean reset(IUnitInfo unitinfo) throws IOException {
      if (fileInfo.workStatus() == ConstVar.WS_Write) {
        this.iunitinfo = unitinfo;
        chunksWriteBuffer.reset();
        metasWriteBuffer.reset();
        currentwritingunitlength = 0;
        currentwritingunitreclength = 0;
        metaOffset = unitinfo.offset();
        if (fileInfo.compressed()) {
          compressionOutputStream.resetState();
          metacompressionOutputStream.resetState();
        }
      } else {
        this.currunitline = unitinfo.beginLine();
        if (this.iunitinfo != unitinfo) {
          this.iunitinfo = unitinfo;
        }

        String unitname = unitinfo.segid() + "-" + unitinfo.unitid();
        if (unitdata.unitname != unitname) {
          unitdata.unitname = unitname;
          unpersistentUnit();
        }

        int split = 0;
        if (offsets.length < unitinfo.recordNum())
          offsets = new int[unitinfo.recordNum()];
        if (fileInfo.isVar()) {
          split = unitinfo.recordNum() * ConstVar.Sizeof_Short;
          metasReadBuffer.reset(unitdata.unitdata, split);
          int lastlen = metasReadBuffer.readShort();
          offsets[0] = split;
          for (int i = 1; i < unitinfo.recordNum(); i++) {
            offsets[i] = offsets[i - 1] + lastlen;
            lastlen = metasReadBuffer.readShort();
          }
        } else {
          for (int i = 0; i < unitinfo.recordNum(); i++) {
            offsets[i] = i * fileInfo.recordlen();
          }
        }
        chunksReadBuffer.reset(unitdata.unitdata, split, unitdata.unitdatalen
            - split);

      }
      return true;
    }

    public boolean isCurrentUnit(IUnitInfo iunitinfo) {
      if (this.iunitinfo.segid() == iunitinfo.segid()
          && this.iunitinfo.unitid() == iunitinfo.unitid())
        return true;
      return false;
    }

    public long getCurrentUnitSize() {
      long res = 0;
      if (fileInfo.formatfiletype() == ConstVar.OldFormatFile) {
        res = currentwritingunitlength + ConstVar.Sizeof_Int
            + ConstVar.Sizeof_Int + ConstVar.Sizeof_Long;
      } else {
        res = ConstVar.Sizeof_Int + currentwritingunitlength;
        if (fileInfo.isVar()) {
          res += ConstVar.Sizeof_Int;
        }
      }
      return res;
    }

    private int bisearch(IRecord.IFValue fv) throws IOException {
      int begin = 0;
      int end = iunitinfo.recordNum() - 1;
      int mid = begin + ((end - begin) >>> 1);
      IRecord rec3 = new IRecord(fileInfo.head().fieldMap().fieldtypes());
      boolean equal = false;
      while (begin <= end) {
        chunksReadBuffer.seek(offsets[mid]);
        rec3.unpersistent(chunksReadBuffer);
        IRecord.IFValue fv3 = rec3.getByIdx(fileInfo.head().getPrimaryIndex());
        int x = fv3.compareTo(fv);
        if (x > 0) {
          end = mid - 1;
        } else if (x < 0) {
          begin = mid + 1;
        } else {
          equal = true;
          break;
        }
        mid = (begin + end) >>> 1;
      }
      if (equal) {
        while (true) {
          if (mid <= 0)
            break;
          mid--;
          chunksReadBuffer.seek(offsets[mid]);
          rec3.unpersistent(chunksReadBuffer);
          IRecord.IFValue fv3 = rec3
              .getByIdx(fileInfo.head().getPrimaryIndex());
          if (fv3.compareTo(fv) < 0) {
            begin = mid + 1;
            break;
          }
        }
      }
      return begin;
    }
  }

  public UnitOperator unitOperator() {
    return this.unitOperator;
  }
}
