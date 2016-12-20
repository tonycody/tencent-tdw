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
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import Comm.ConstVar;

public class IFormatDataFile implements IFileInterface {
  public static final Log LOG = LogFactory.getLog("IFormatDataFile");

  Configuration conf = null;

  private IFileInfo fileInfo;

  private HashMap<Integer, ISegment> segments;
  private ISegment currentSegment = null;
  private ISegmentIndex segindex;

  HashMap<Integer, IRecord.IFType> readfieldtypes;

  private int processMethod = 0;

  public IFormatDataFile(Configuration conf) throws IOException {
    this.conf = conf;
    fileInfo = new IFileInfo(conf);

    if (this.conf != null) {
      processMethod = this.conf.getInt("hive.errordata.process.level", 0);
    }
  }

  @Override
  public void create(String fileName, IHead head) throws IOException {
    checkHeadInfo(head);
    fileInfo.initialize(fileName, head);
    segments = new HashMap<Integer, ISegment>();
    this.segindex = new ISegmentIndex(fileInfo);
  }

  @Override
  public int addRecord(IRecord record) throws IOException {
    if (fileInfo.workStatus() != ConstVar.WS_Write)
      throw new IOException("not write mode");

    if (!checkRecord(record)) {
      StringBuffer sb = new StringBuffer();
      sb.append("add error:\r\n");
      sb.append("fileinfo--->\r\n");
      sb.append(fileInfo.head().fieldMap().showtypes());
      sb.append("record--->\r\n");
      sb.append(record.showtype());
      throw new IOException(sb.toString());
    }

    if (fileInfo.formatfiletype() == ConstVar.OldFormatFile) {
      record.setoldformat(true);
    }

    if (record.len() > 32767) {
      String str = null;
      int len = 0;
      StringBuffer sb = null;
      switch (processMethod) {
      case 0:
        break;
      case 1:
        sb = new StringBuffer();
        sb.append("add error:\r\n");
        sb.append("fileinfo--->\r\n");
        sb.append("fileinfo--->\r\n");
        sb.append(fileInfo.head().fieldMap().showtypes());
        sb.append("record--->\r\n");
        sb.append(record.showtype());
        str = record.showstr();
        if (str != null) {
          len = str.length() > 32767 ? 32767 : str.length();
          str = str.substring(0, len);
        }
        sb.append(record.toString());
        sb.append(str);
        LOG.info(sb.toString());
        return 0;

      case 2:
        sb = new StringBuffer();
        sb.append("add error:\r\n");
        sb.append("fileinfo--->\r\n");
        sb.append("fileinfo--->\r\n");
        sb.append(fileInfo.head().fieldMap().showtypes());
        sb.append("record--->\r\n");
        sb.append(record.showtype());
        str = record.showstr();
        if (str != null) {
          len = str.length() > 32767 ? 32767 : str.length();
          str = str.substring(0, len);
        }
        sb.append(record.toString());
        sb.append(str);
        throw new IOException(sb.toString());
      default:
        break;
      }
    }

    if (currentSegment == null) {
      currentSegment = new ISegment(fileInfo, segments.size());
    }
    if (!currentSegment.addRecord(record)) {
      IUnit unit = currentSegment.currentUnit();
      currentSegment.finish(true);
      addCurrentSegment();
      currentSegment = new ISegment(fileInfo, segments.size());
      currentSegment.setCurrentunit(unit);
      currentSegment.addCurrentUnit(false);
    }
    fileInfo.increasecurrentline();
    return 0;
  }

  private boolean checkRecord(IRecord record) {
    return true;
  }

  private void addCurrentSegment() throws IOException {
    segments.put(currentSegment.seginfo().segid(), currentSegment);
    segindex.update(currentSegment.seginfo());
  }

  private void checkHeadInfo(IHead head) {

  }

  @Override
  public void open(String fileName) throws IOException {
    LOG.info("open file:\t" + fileName);
    try {
      fileInfo.initialize(fileName);
      this.segments = new HashMap<Integer, ISegment>();
      this.segindex = new ISegmentIndex(fileInfo);
      unpersistentSegmentIndex();
      this.readfieldtypes = fileInfo.head().fieldMap().fieldtypes();
    } catch (Exception x) {
      fileInfo.uninitialize();
      throw new IOException(x);
    }
  }

  @Override
  public void open(String fileName, ArrayList<Integer> idxs) throws IOException {
    LOG.info("open file:\t" + fileName);
    try {
      fileInfo.initialize(fileName);
      this.segments = new HashMap<Integer, ISegment>();
      this.segindex = new ISegmentIndex(fileInfo);
      unpersistentSegmentIndex();
      this.readfieldtypes = new HashMap<Integer, IRecord.IFType>();
      HashMap<Integer, IRecord.IFType> fieldtypes = fileInfo.head().fieldMap()
          .fieldtypes();
      for (Integer id : idxs) {
        this.readfieldtypes.put(id, fieldtypes.get(id));
      }
    } catch (Exception x) {
      fileInfo.uninitialize();
      throw new IOException(x);
    }
  }

  private void unpersistentSegmentIndex() throws IOException {
    if (fileInfo.formatfiletype() == ConstVar.OldFormatFile) {
      fileInfo.in().seek(fileInfo.filelength() - ConstVar.IndexMetaOffset);
    } else {
      fileInfo.in().seek(fileInfo.filelength() - ConstVar.Sizeof_Long);
      long metaoffset = fileInfo.in().readLong();
      fileInfo.in().seek(metaoffset);
    }
    segindex.unpersistent(fileInfo.in());
  }

  @Override
  public boolean seek(int line) throws IOException {
    if (!fileInfo.havelineindex())
      return false;
    int segid = segindex.getSegid(line);
    if (segid >= segindex.getSegnum()) {
      return false;
    }
    if (!positioncurrentsegment(segid)) {
      return false;
    }
    if (currentSegment.seek(line)) {
      fileInfo.setcurrentline(line);
      return true;
    }
    return false;
  }

  private boolean positioncurrentsegment(int segid) throws IOException {
    if (segments.containsKey(segid)) {
      currentSegment = segments.get(segid);
    } else {
      if (segid >= segindex.getSegnum()) {
        return false;
      }
      currentSegment = new ISegment(fileInfo, segindex, segid);
      segments.put(segid, currentSegment);
    }
    return true;
  }

  @Override
  public boolean seek(IRecord.IFValue fv) throws IOException {
    if (!fileInfo.havekeyindex())
      return false;
    int segid = segindex.getSegid(fv);
    if (segid >= segindex.getSegnum()) {
      return false;
    }
    if (!positioncurrentsegment(segid)) {
      return false;
    }
    if (currentSegment.seek(fv)) {
      return true;
    }
    return false;
  }

  @Override
  public IRecord next() throws IOException {
    IRecord record = this.getIRecordObj();
    if (!this.next(record))
      return null;
    return record;
  }

  @Override
  public boolean next(IRecord record) throws IOException {
    if (currentSegment == null) {
      if (!positioncurrentsegment(0))
        return false;
    }
    if (fileInfo.formatfiletype() == ConstVar.OldFormatFile) {
      record.setoldformat(true);
    }
    if (!currentSegment.next(record)) {
      if (!positioncurrentsegment(currentSegment.seginfo().segid() + 1)) {
        return false;
      }
      currentSegment.next(record);
    }
    return true;
  }

  @Override
  public IRecord getByLine(int line) throws IOException {
    if (this.seek(line))
      return next();
    return null;
  }

  @Override
  public ArrayList<IRecord> getByKey(IRecord.IFValue ifv) throws IOException {
    ArrayList<IRecord> res = new ArrayList<IRecord>();
    if (this.seek(ifv)) {
      int compare = 0;
      while (compare == 0) {
        IRecord record = next();
        IRecord.IFValue ifv1 = record.getByIdx(fileInfo.head()
            .getPrimaryIndex());
        compare = ifv.compareTo(ifv1);
        if (compare == 0)
          res.add(record);
      }
    }
    return res;
  }

  @Override
  public ArrayList<IRecord> getRangeByline(int beginline, int endline)
      throws IOException {
    ArrayList<IRecord> res = new ArrayList<IRecord>();
    if (seek(beginline))
      for (int i = beginline; i < endline + 1; i++) {
        res.add(next());
      }
    return res;
  }

  @Override
  public ArrayList<IRecord> getRangeByKey(IRecord.IFValue beginkey,
      IRecord.IFValue endkey) throws IOException {
    ArrayList<IRecord> res = new ArrayList<IRecord>();
    if (seek(beginkey)) {
      int compare = 0;
      while (compare == 0) {
        IRecord record = next();
        IRecord.IFValue key = record
            .getByIdx(fileInfo.head().getPrimaryIndex());
        compare = key.compareTo(endkey);
        if (compare <= 0)
          res.add(record);
      }
    }
    return res;
  }

  @Override
  public void close() throws IOException {
    LOG.info("fd close:\t" + fileInfo.filename());
    if (fileInfo.workStatus() == ConstVar.WS_Write) {
      if (currentSegment != null) {
        if (!currentSegment.addCurrentUnit(true)) {
          IUnit unit = currentSegment.currentUnit();
          currentSegment.finish(true);
          addCurrentSegment();
          currentSegment = new ISegment(fileInfo, segments.size());
          currentSegment.setCurrentunit(unit);
          currentSegment.addCurrentUnit(false);
        }
        currentSegment.finish(false);
        addCurrentSegment();
      }
      segindex.setofflen(fileInfo.out().getPos());
      segindex.persistent(fileInfo.out());
      fileInfo.out().close();
    } else if (fileInfo.workStatus() == ConstVar.WS_Read) {
      fileInfo.in().close();
    }
    closeInternal();
  }

  private void closeInternal() throws IOException {
    this.currentSegment = null;
    this.segments = null;
    this.segindex = null;
    this.fileInfo = new IFileInfo(conf);
  }

  @Override
  public int recnum() {
    if (!fileInfo.havelineindex()) {
      return -1;
    }
    if (segindex.getSegnum() == 0)
      return 0;
    return segindex.getILineIndex(segindex.getSegnum() - 1).endline() + 1;
  }

  public IFileInfo fileInfo() {
    return fileInfo;
  }

  public ISegmentIndex segIndex() {
    return segindex;
  }

  @Override
  public IRecord getIRecordObj() {
    if (fileInfo.workStatus() == ConstVar.WS_Write) {
      IRecord rec = new IRecord(fileInfo.head().fieldMap().fieldtypes());
      return rec;
    }
    if (fileInfo.workStatus() == ConstVar.WS_Read) {
      return new IRecord(fileInfo.head().fieldMap().fieldtypes(),
          readfieldtypes);
    }
    return null;
  }

  public ISegment currSegment() {
    return this.currentSegment;
  }

}
