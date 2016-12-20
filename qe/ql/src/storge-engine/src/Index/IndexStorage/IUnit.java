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

public class IUnit {

  private IFileInfo fileInfo;
  private IUnitInfo iunitinfo;

  public IUnit(IFileInfo fileInfo, int segid, int unitid) throws IOException {
    this.fileInfo = fileInfo;
    iunitinfo = new IUnitInfo(fileInfo, segid, unitid);
    fileInfo.unitOperator().reset(iunitinfo);
  }

  public IUnit(IFileInfo fileInfo, IUnitIndex unitIndex, int segid, int unitid)
      throws IOException {
    this.fileInfo = fileInfo;
    iunitinfo = new IUnitInfo(fileInfo, unitIndex, segid, unitid);
    fileInfo.unitOperator().reset(iunitinfo);
  }

  public boolean addRecord(IRecord record) throws IOException {
    if (!iunitinfo.update(record))
      throw new IOException("!!!!!!!!key index must be increase");

    fileInfo.unitOperator().addRecord(record);
    return checkUnitSize();
  }

  public void persistent() throws IOException {
    iunitinfo.setOffset(fileInfo.out().getPos());
    fileInfo.unitOperator().persistentUnit();
  }

  private boolean checkUnitSize() {
    return fileInfo.confUnitSize() > fileInfo.unitOperator()
        .getCurrentUnitSize() ? true : false;
  }

  public boolean seek(int line) throws IOException {
    return fileInfo.unitOperator().seek(this.iunitinfo, line);
  }

  public boolean seek(IFieldValue fv) throws IOException {
    return fileInfo.unitOperator().seek(this.iunitinfo, fv);
  }

  public boolean next(IRecord record) throws IOException {
    return fileInfo.unitOperator().next(record);
  }

  public IUnitInfo iunitinfo() {
    return iunitinfo;
  }
}
