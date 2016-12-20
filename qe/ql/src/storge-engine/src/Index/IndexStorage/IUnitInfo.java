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

public class IUnitInfo {
  private int segid;
  private int unitid;

  private IFieldValue beginKey = null;
  private IFieldValue endKey = null;
  private int beginLine = -1;
  private int endLine = -1;

  private int recordNum = 0;
  private long offset;
  private IFileInfo fileInfo;

  public IUnitInfo(IFileInfo fileInfo, int segid, int unitid) {
    this.fileInfo = fileInfo;
    this.segid = segid;
    this.unitid = unitid;
    if (fileInfo.havelineindex()) {
      beginLine = fileInfo.currentline();
      endLine = beginLine - 1;
    }
  }

  public IUnitInfo(IFileInfo fileInfo, IUnitIndex unitIndex, int segid,
      int unitid) {
    this.fileInfo = fileInfo;
    this.segid = segid;
    this.unitid = unitid;

    offset = unitIndex.getUnitOffset(unitid);
    if (fileInfo.havekeyindex()) {
      IKeyIndex keyIndex = unitIndex.getKeyIndex(unitid);
      beginKey = keyIndex.beginkey();
      endKey = keyIndex.endkey();
      recordNum = keyIndex.recnum();
    }
    if (fileInfo.havelineindex()) {
      ILineIndex lineIndex = unitIndex.getLineIndex(unitid);
      beginLine = lineIndex.beginline();
      endLine = lineIndex.endline();
      recordNum = endLine - beginLine + 1;
    }
  }

  public int segid() {
    return segid;
  }

  public int unitid() {
    return unitid;
  }

  public long offset() {
    return offset;
  }

  public int recordNum() {
    return this.recordNum;
  }

  public IFieldValue beginKey() {
    return beginKey;
  }

  public IFieldValue endKey() {
    return endKey;
  }

  public int beginLine() {
    return beginLine;
  }

  public int endLine() {
    return endLine;
  }

  public void setOffset(long offset) {
    this.offset = offset;
  }

  public boolean update(IRecord record) {
    if (fileInfo.havekeyindex()) {
      IFieldValue fv = record.fieldValues().get(
          fileInfo.head().getPrimaryIndex());
      if (beginKey == null)
        this.beginKey = fv;
      if (endKey != null && (fv.compareTo(endKey) < 0)) {
        return false;
      }
      this.endKey = fv;
    }
    if (fileInfo.havelineindex()) {
      this.endLine++;
    }
    this.recordNum++;
    return true;
  }
}
