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

import Comm.ConstVar;

public class ISegmentInfo {

  private int segid;
  private long offset;

  private IRecord.IFValue beginKey = null;
  private IRecord.IFValue endKey = null;
  private int beginLine = -1;
  private int endLine = -1;

  private int segLen;
  private int recnum = 0;
  private IFileInfo fileInfo;

  public ISegmentInfo(IFileInfo fileInfo, int segid) throws IOException {
    this.fileInfo = fileInfo;
    this.segid = segid;
    if (fileInfo.workStatus() == ConstVar.WS_Write) {
      offset = fileInfo.out().getPos();
    }
  }

  public ISegmentInfo(IFileInfo fileInfo, ISegmentIndex segindex, int segid)
      throws IOException {
    this.fileInfo = fileInfo;
    this.segid = segid;
    offset = segindex.getSegOffset(segid);
    segLen = segindex.getseglen(segid);

    if (fileInfo.havelineindex()) {
      this.beginLine = segindex.getILineIndex(segid).beginline();
      this.endLine = segindex.getILineIndex(segid).endline();
      this.recnum = endLine - beginLine + 1;
    }

    if (fileInfo.havekeyindex()) {
      this.beginKey = segindex.getIKeyIndex(segid).beginkey();
      this.endKey = segindex.getIKeyIndex(segid).endkey();
      this.recnum = segindex.getIKeyIndex(segid).recnum();
    }
  }

  public int segid() {
    return this.segid;
  }

  public long offset() {
    return this.offset;
  }

  public int recnum() {
    return recnum;
  }

  public int segLen() {
    return segLen;
  }

  public IRecord.IFValue beginKey() {
    return beginKey;
  }

  public IRecord.IFValue endKey() {
    return endKey;
  }

  public int beginLine() {
    return beginLine;
  }

  public int endLine() {
    return endLine;
  }

  public void setSegLen(int len) {
    this.segLen = len;
  }

  public void update(IUnitInfo iunitinfo) {
    if (fileInfo.havekeyindex()) {
      if (beginKey == null) {
        this.beginKey = iunitinfo.beginKey();
      }
      this.endKey = iunitinfo.endKey();
    }
    if (fileInfo.havelineindex()) {
      if (beginLine() == -1)
        this.beginLine = iunitinfo.beginLine();
      this.endLine = iunitinfo.endLine();
    }
    this.recnum += iunitinfo.recordNum();
  }
}
