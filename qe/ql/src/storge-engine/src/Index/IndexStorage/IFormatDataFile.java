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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;

import Comm.ConstVar;

public class IFormatDataFile {

  private IFileInfo fileInfo;

  private HashMap<Integer, ISegment> segments;
  private ISegment currentSegment = null;
  private ISegmentIndex segindex;

  public IFormatDataFile(Configuration conf) throws IOException {
    fileInfo = new IFileInfo(conf);
  }

  public void create(String fileName, IHead head) throws IOException {
    checkHeadInfo(head);
    fileInfo.initialize(fileName, head);
    segments = new HashMap<Integer, ISegment>();
    this.segindex = new ISegmentIndex(fileInfo);
  }

  public int addRecord(IRecord record) throws IOException {
    if (fileInfo.workStatus() != ConstVar.WS_Write)
      return 1;
    if (!checkRecord(record)) {
      StringBuffer sb = new StringBuffer();
      sb.append("add error:\r\n");
      sb.append("fileinfo--->\r\n");
      sb.append(fileInfo.head().fieldMap().showtypes());
      sb.append("record--->\r\n");
      sb.append(record.showtype());
      throw new IOException(sb.toString());
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
      currentSegment.addCurrentUnit();
    }
    fileInfo.increasecurrentline();
    return 0;
  }

  private boolean checkRecord(IRecord record) {
    TreeMap<Short, IFieldType> types = fileInfo.head().fieldMap().fieldtypes();
    for (IFieldValue fv : record.fieldValues().values()) {
      if (!types.containsKey(fv.idx())
          || (types.get(fv.idx()).getType() != fv.type())) {
        return false;
      }
    }
    record.setFixLen(!fileInfo.isVar());
    return true;
  }

  private void addCurrentSegment() throws IOException {
    segments.put(currentSegment.seginfo().segid(), currentSegment);
    segindex.update(currentSegment.seginfo());
  }

  private void checkHeadInfo(IHead head) {

  }

  public void open(String fileName) throws IOException {
    fileInfo.initialize(fileName);
    this.segments = new HashMap<Integer, ISegment>();
    this.segindex = new ISegmentIndex(fileInfo);
    unpersistentSegmentIndex();
  }

  private void unpersistentSegmentIndex() throws IOException {
    fileInfo.in().seek(fileInfo.filelength() - ConstVar.Sizeof_Long);
    long metaoffset = fileInfo.in().readLong();
    fileInfo.in().seek(metaoffset);
    segindex.unpersistent(fileInfo.in());
  }

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

  public boolean seek(IFieldValue fv) throws IOException {
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

  public IRecord next() throws IOException {
    IRecord record = new IRecord();
    if (!this.next(record))
      return null;
    return record;
  }

  public boolean next(IRecord record) throws IOException {
    record.setFieldTypes(fileInfo.head().fieldMap().fieldtypes());
    record.setFixLen(!fileInfo.isVar());
    if (currentSegment == null) {
      currentSegment = new ISegment(fileInfo, segindex, 0);
    }
    if (!currentSegment.next(record)) {
      if (!positioncurrentsegment(currentSegment.seginfo().segid() + 1)) {
        return false;
      }
      currentSegment.next(record);
    }
    return true;
  }

  public IRecord getByLine(int line) throws IOException {
    if (this.seek(line))
      return next();
    return null;
  }

  public ArrayList<IRecord> getByKey(IFieldValue ifv) throws IOException {
    ArrayList<IRecord> res = new ArrayList<IRecord>();
    if (this.seek(ifv)) {
      int compare = 0;
      while (compare == 0) {
        IRecord record = next();
        IFieldValue ifv1 = record.fieldValues().get(
            fileInfo.head().getPrimaryIndex());
        compare = ifv.compareTo(ifv1);
        if (compare == 0)
          res.add(record);
      }
    }
    return res;
  }

  public ArrayList<IRecord> getRangeByline(int beginline, int endline)
      throws IOException {
    ArrayList<IRecord> res = new ArrayList<IRecord>();
    if (seek(beginline))
      for (int i = beginline; i < endline + 1; i++) {
        res.add(next());
      }
    return res;
  }

  public ArrayList<IRecord> getRangeByKey(IFieldValue beginkey,
      IFieldValue endkey) throws IOException {
    ArrayList<IRecord> res = new ArrayList<IRecord>();
    if (seek(beginkey)) {
      int compare = 0;
      while (compare == 0) {
        IRecord record = next();
        IFieldValue key = record.fieldValues().get(
            fileInfo.head().getPrimaryIndex());
        compare = key.compareTo(endkey);
        if (compare <= 0)
          res.add(record);
      }
    }
    return res;
  }

  public void close() throws IOException {
    if (fileInfo.workStatus() == ConstVar.WS_Write) {
      if (currentSegment != null) {
        currentSegment.close();
        addCurrentSegment();
      }
      segindex.setofflen(fileInfo.out().getPos());
      segindex.persistent(fileInfo.out());
      fileInfo.out().close();
    } else if (fileInfo.workStatus() == ConstVar.WS_Read) {
      fileInfo.in().close();
    }
  }

  public int recnum() {
    if (!fileInfo.havelineindex()) {
      return -1;
    }
    return segindex.getILineIndex(segindex.getSegnum() - 1).endline() + 1;
  }

  public IFileInfo fileInfo() {
    return fileInfo;
  }

  public ISegmentIndex segIndex() {
    return segindex;
  }
}
