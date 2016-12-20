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
  private LRUCache<String, UnitData> pools;
  private UnitOperator unitOperator;

  class UnitData {
    byte[] unitdata;
    int unitdatalen;

    public UnitData(byte[] unitdata, int unitdatalen) {
      this.unitdata = unitdata;
      this.unitdatalen = unitdatalen;
    }
  }

  public IUnitPool(IFileInfo fileInfo) throws IOException {
    pools = new LRUCache<String, UnitData>();
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

    private SeekDIBuffer chunksReadBuffer;
    private DataInputBuffer metasReadBuffer;
    private CompressionInputStream compressionInputStream;
    private CompressionInputStream metacompressionInputStream;

    private short[] lens;
    private int[] offsets;

    public UnitOperator(IFileInfo fileInfo) throws IOException {
      this.fileInfo = fileInfo;
      if (fileInfo.workStatus() == ConstVar.WS_Write) {
        chunksWriteBuffer = new DataOutputBuffer();
        metasWriteBuffer = new DataOutputBuffer();
        if (fileInfo.compressed()) {
          fileInfo.head().getCompressStyle();
          if (fileInfo.head().getCompressStyle() == 2) {
            LzoCodec codec = new LzoCodec();
            codec.setConf(fileInfo.conf());
            compressionOutputStream = codec
                .createOutputStream(chunksWriteBuffer);
            metacompressionOutputStream = codec
                .createOutputStream(metasWriteBuffer);
          } else {
            DefaultCodec codec = new DefaultCodec();
            codec.setConf(fileInfo.conf());
            compressionOutputStream = codec
                .createOutputStream(chunksWriteBuffer);
            metacompressionOutputStream = codec
                .createOutputStream(metasWriteBuffer);
          }
        }
      } else {
        chunksReadBuffer = new SeekDIBuffer();
        metasReadBuffer = new DataInputBuffer();
        if (fileInfo.compressed()) {
          if (fileInfo.head().getCompressStyle() == 2) {
            LzoCodec codec = new LzoCodec();
            codec.setConf(fileInfo.conf());
            compressionInputStream = codec.createInputStream(chunksReadBuffer);
            metacompressionInputStream = codec
                .createInputStream(metasReadBuffer);
          } else {
            DefaultCodec codec = new DefaultCodec();
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

      record.setFieldTypes(fileInfo.head().fieldMap().fieldtypes());
      if (fileInfo.isVar()) {
        if (fileInfo.compressed()) {
          byte[] len = new byte[2];
          Util.short2bytes(len, (short) record.len());
          metacompressionOutputStream.write(len);
        } else {
          metasWriteBuffer.writeShort(record.len());
        }
      }
      if (fileInfo.compressed()) {
        record.persistent(new DataOutputStream(compressionOutputStream));
      } else {
        record.persistent(chunksWriteBuffer);
      }
      return true;
    }

    public void persistentUnit() throws IOException {
      if (fileInfo.compressed()) {
        compressionOutputStream.finish();
        if (fileInfo.isVar()) {
          metacompressionOutputStream.finish();
        }
      }
      int len = chunksWriteBuffer.getLength();
      if (fileInfo.isVar()) {
        len += metasWriteBuffer.getLength();
      }
      fileInfo.out().writeInt(len);
      if (fileInfo.isVar()) {
        fileInfo.out().writeInt(metasWriteBuffer.getLength());
        fileInfo.out().write(metasWriteBuffer.getData(), 0,
            metasWriteBuffer.getLength());
      }
      fileInfo.out().write(chunksWriteBuffer.getData(), 0,
          chunksWriteBuffer.getLength());
      if (fileInfo.compressed()) {
        compressionOutputStream.resetState();
        if (fileInfo.isVar()) {
          metacompressionOutputStream.resetState();
        }
      }
    }

    private void unpersistentUnit() throws IOException {
      fileInfo.in().seek(iunitinfo.offset());
      int len = fileInfo.in().readInt();
      int metalen = 0;
      if (fileInfo.isVar())
        metalen = fileInfo.in().readInt();
      if (!fileInfo.compressed()) {
        unitdata = new UnitData(new byte[len], len);
        fileInfo.in().read(unitdata.unitdata);
      } else {
        byte[] data = new byte[len];
        fileInfo.in().read(data);
        DataOutputBuffer dob = new DataOutputBuffer();
        if (fileInfo.isVar()) {
          metasReadBuffer.reset(data, 0, metalen);
          metacompressionInputStream.resetState();
          IOUtils.copyBytes(metacompressionInputStream, dob, fileInfo.conf(),
              false);
        }
        chunksReadBuffer.reset(data, metalen, len - metalen);
        compressionInputStream.resetState();
        IOUtils.copyBytes(compressionInputStream, dob, fileInfo.conf(), false);
        dob.flush();
        unitdata = new UnitData(dob.getData(), dob.getLength());
        dob.close();
      }
    }

    public boolean seek(IUnitInfo iunitinfo, int line) throws IOException {
      if (!fileInfo.havelineindex())
        return false;

      this.reset(iunitinfo);

      return chunksReadBuffer.seek(offsets[line - iunitinfo.beginLine()]);
    }

    public boolean seek(IUnitInfo iunitinfo, IFieldValue fv) throws IOException {
      if (!fileInfo.havekeyindex())
        return false;

      this.reset(iunitinfo);

      int line = bisearch(fv);
      chunksReadBuffer.seek(offsets[line]);
      return true;
    }

    public boolean next(IRecord record) throws IOException {
      if (chunksReadBuffer.available() <= 0)
        return false;
      record.unpersistent(chunksReadBuffer);
      return true;
    }

    public boolean reset(IUnitInfo unitinfo) throws IOException {
      if (fileInfo.workStatus() == ConstVar.WS_Write) {
        this.iunitinfo = unitinfo;
        chunksWriteBuffer.reset();
        metasWriteBuffer.reset();
      } else {
        if (this.iunitinfo != unitinfo) {
          this.iunitinfo = unitinfo;

          String unitname = unitinfo.segid() + "-" + unitinfo.unitid();
          if (pools.containsKey(unitname)) {
            unitdata = pools.get(unitname);
          } else {
            unpersistentUnit();
            pools.put(unitname, unitdata);
          }

          int split = 0;
          lens = new short[unitinfo.recordNum()];
          offsets = new int[unitinfo.recordNum()];
          if (fileInfo.isVar()) {
            split = unitinfo.recordNum() * ConstVar.Sizeof_Short;
            metasReadBuffer.reset(unitdata.unitdata, split);
            lens[0] = metasReadBuffer.readShort();
            offsets[0] = split;
            for (int i = 1; i < unitinfo.recordNum(); i++) {
              lens[i] = metasReadBuffer.readShort();
              offsets[i] = offsets[i - 1] + lens[i - 1];
            }
          } else {
            for (int i = 0; i < unitinfo.recordNum(); i++) {
              lens[i] = (short) fileInfo.recordlen();
              offsets[i] = i * fileInfo.recordlen();
            }
          }
          chunksReadBuffer.reset(unitdata.unitdata, split, unitdata.unitdatalen
              - split);
        }
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
      return ConstVar.Sizeof_Int + chunksWriteBuffer.getLength()
          + metasWriteBuffer.getLength();
    }

    private int bisearch(IFieldValue fv) throws IOException {
      int begin = 0;
      int end = offsets.length - 1;
      int mid = begin + ((end - begin) >>> 1);
      IRecord rec3 = new IRecord();
      rec3.setFieldTypes(fileInfo.head().fieldMap().fieldtypes());
      boolean equal = false;
      while (begin <= end) {
        chunksReadBuffer.seek(offsets[mid]);
        rec3.unpersistent(chunksReadBuffer);
        IFieldValue fv3 = rec3.fieldValues().get(
            fileInfo.head().getPrimaryIndex());
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
          IFieldValue fv3 = rec3.fieldValues().get(
              fileInfo.head().getPrimaryIndex());
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
