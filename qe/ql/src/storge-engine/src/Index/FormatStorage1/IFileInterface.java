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

public interface IFileInterface {

  public void create(String fileName, IHead head) throws IOException;

  public int addRecord(IRecord record) throws IOException;

  public void open(String fileName) throws IOException;

  public void open(String fileName, ArrayList<Integer> idxs) throws IOException;

  public boolean seek(int line) throws IOException;

  public boolean seek(IRecord.IFValue fv) throws IOException;

  public IRecord next() throws IOException;

  public boolean next(IRecord record) throws IOException;

  public IRecord getByLine(int line) throws IOException;

  public ArrayList<IRecord> getByKey(IRecord.IFValue ifv) throws IOException;

  public ArrayList<IRecord> getRangeByline(int beginline, int endline)
      throws IOException;

  public ArrayList<IRecord> getRangeByKey(IRecord.IFValue beginkey,
      IRecord.IFValue endkey) throws IOException;

  public void close() throws IOException;

  public int recnum();

  public IRecord getIRecordObj();

}
