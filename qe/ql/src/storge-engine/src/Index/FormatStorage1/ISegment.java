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
import java.util.HashMap;

import Comm.ConstVar;

public class ISegment {
  private IFileInfo fileInfo;
  private HashMap<Integer, IUnit> units;
  private IUnit currentUnit = null;
  private IUnitIndex unitIndex;
  private ISegmentInfo seginfo;

  public ISegment(IFileInfo fileInfo, int segid) throws IOException {
    this.fileInfo = fileInfo;
    units = new HashMap<Integer, IUnit>();
    unitIndex = new IUnitIndex(fileInfo);
    seginfo = new ISegmentInfo(fileInfo, segid);
  }

  public ISegment(IFileInfo fileInfo, ISegmentIndex segindex, int segid)
      throws IOException {
    this.fileInfo = fileInfo;
    units = new HashMap<Integer, IUnit>();

    if (fileInfo.formatfiletype() == ConstVar.OldFormatFile) {
      fileInfo.in().seek(
          segindex.getSegOffset(segid) + segindex.getseglen(segid)
              - ConstVar.IndexMetaOffset);
    } else {
      fileInfo.in().seek(
          segindex.getSegOffset(segid) + segindex.getseglen(segid)
              - ConstVar.Sizeof_Long);
      fileInfo.in().seek(fileInfo.in().readLong());
    }
    unitIndex = new IUnitIndex(fileInfo);
    unitIndex.setunitindexoffset(segindex.getSegOffset(segid));
    unitIndex.unpersistent(fileInfo.in());
    seginfo = new ISegmentInfo(fileInfo, segindex, segid);
  }

  public boolean addRecord(IRecord record) throws IOException {
    if (currentUnit == null) {
      currentUnit = new IUnit(fileInfo, seginfo.segid(), units.size());
    }
    if (!currentUnit.addRecord(record)) {
      if (!checkSegmentSize()) {
        return false;
      }
      addCurrentUnit(false);
    }
    return true;
  }

  private boolean checkSegmentSize() throws IOException {
    long x = fileInfo.out().getPos() / fileInfo.confSegmentSize();
    long y = (fileInfo.out().getPos()
        + fileInfo.unitOperator().getCurrentUnitSize()
        + unitIndex.plus1currunitindexsize() + ConstVar.Sizeof_Long + ConstVar.Sizeof_Int)
        / fileInfo.confSegmentSize();
    return x == y ? true : false;
  }

  public void finish(boolean filldummy) throws IOException {
    unitIndex.setunitindexoffset(fileInfo.out().getPos());
    unitIndex.persistent(fileInfo.out());
    seginfo.setSegLen((int) (fileInfo.out().getPos() - seginfo.offset()));
    if (filldummy)
      filldummy();
  }

  private void filldummy() throws IOException {
    int fillnum = (int) (fileInfo.confSegmentSize() - fileInfo.out().getPos()
        % fileInfo.confSegmentSize());
    byte[] dummy = new byte[fillnum];
    fileInfo.out().write(dummy);
  }

  public boolean addCurrentUnit(boolean check) throws IOException {
    if (currentUnit == null)
      return true;
    if (check) {
      if (!checkSegmentSize())
        return false;
    }
    currentUnit.persistent();
    units.put(currentUnit.iunitinfo().unitid(), currentUnit);
    unitIndex.update(currentUnit.iunitinfo());
    seginfo.update(currentUnit.iunitinfo());
    currentUnit = null;
    return true;
  }

  public boolean seek(int line) throws IOException {
    if (!fileInfo.havelineindex())
      return false;
    int unitid = unitIndex.getUnitid(line);
    if (!positioncurrentunit(unitid)) {
      return false;
    }
    return currentUnit.seek(line);
  }

  private boolean positioncurrentunit(int unitid) throws IOException {
    if (units.containsKey(unitid)) {
      currentUnit = units.get(unitid);
    } else {
      if (unitid >= unitIndex.getUnitnum()) {
        return false;
      }
      currentUnit = new IUnit(fileInfo, unitIndex, seginfo.segid(), unitid);
      units.put(unitid, currentUnit);
    }
    return true;
  }

  public boolean seek(IRecord.IFValue fv) throws IOException {
    if (!fileInfo.havekeyindex())
      return false;
    int unitid = unitIndex.getUnitid(fv);
    if (!positioncurrentunit(unitid)) {
      return false;
    }
    return currentUnit.seek(fv);
  }

  public boolean next(IRecord record) throws IOException {
    if (currentUnit == null) {
      positioncurrentunit(0);
    }
    if (!currentUnit.next(record)) {
      if (!positioncurrentunit(currentUnit.iunitinfo().unitid() + 1)) {
        return false;
      }
      currentUnit.next(record);
    }
    return true;
  }

  public IUnit currentUnit() {
    return this.currentUnit;
  }

  public void setCurrentunit(IUnit unit) {
    this.currentUnit = unit;
  }

  public ISegmentInfo seginfo() {
    return this.seginfo;
  }

  public IUnitIndex unitindex() {
    return this.unitIndex;
  }
}
