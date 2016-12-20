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
package ColumnStorage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import Comm.ConstVar;
import Comm.SEException;
import FormatStorage.FormatDataFile;

public class ColumnProject {
  Log LOG = LogFactory.getLog("ColumnProject");

  static class ColumnInfo {
    String name;
    Set<Short> idxs = new HashSet<Short>();
    int beginLine = 0;
    int endLine = 0;
    int beginKey = 0;
    int endKey = 0;
  }

  ArrayList<ColumnInfo> infos = new ArrayList<ColumnInfo>();
  Configuration conf = null;

  ColumnProject(Configuration conf) {
    this.conf = conf;
  }

  public ColumnProject(Path path, Configuration conf) throws Exception {
    String name = path.toString() + ConstVar.Navigator;
    Path naviPath = new Path(name);

    this.conf = conf;
    FileSystem fs = FileSystem.get(conf);

    loadColmnInfoFromHeadInfo(fs, path);

  }

  void saveNavigator(FileSystem fs, Path naviPath) throws IOException {
    int size = infos.size();
    if (size == 0) {
      return;
    }

    try {
      FSDataOutputStream out = fs.create(naviPath);

      out.writeInt(ConstVar.NaviMagic);
      out.writeShort((short) size);

      for (int i = 0; i < size; i++) {
        saveColumnInfo(out, infos.get(i));
      }

      out.close();
    } catch (IOException e) {
      LOG.error("save Column info fail:" + e.getMessage());
    }

  }

  void saveColumnInfo(FSDataOutputStream out, ColumnInfo info)
      throws IOException {
    short nameLen = (short) info.name.length();
    if (nameLen == 0) {
      return;
    }

    short idxLen = (short) info.idxs.size();
    if (idxLen == 0) {
      return;
    }

    out.writeShort(nameLen);
    out.write(info.name.getBytes());

    out.writeShort(idxLen);

    Iterator<Short> iterator = info.idxs.iterator();
    while (iterator.hasNext()) {
      out.writeShort(iterator.next());
    }

    out.writeInt(info.beginKey);
    out.writeInt(info.endKey);
    out.writeInt(info.beginLine);
    out.writeInt(info.endLine);
  }

  void loadColmnInfoFromHeadInfo(FileSystem fs, Path path) throws Exception {
    FileStatus[] status = fs.listStatus(path);
    if (status == null) {
      return;
    }
    if (status == null || status.length == 0) {
      return;
    }

    for (int i = 0; i < status.length; i++) {
      String fileName = status[i].getPath().toString();
      try {
        FormatDataFile fd = new FormatDataFile(conf);
        fd.open(fileName);

        ColumnInfo columnInfo = new ColumnInfo();
        columnInfo.idxs = fd.head.fieldMap().idxs();
        columnInfo.name = fileName;

        infos.add(columnInfo);

        fd.close();
      } catch (SEException.ErrorFileFormat e) {
        LOG.info("get error file format exception:" + e.getMessage()
            + ", file:" + fileName);
        continue;
      } catch (Exception e) {
        LOG.error("load column info fail:" + e.getMessage());
        throw e;
      }
    }

  }

  void showInfos() {
    LOG.info("infos.size:" + infos.size());
    for (int i = 0; i < infos.size(); i++) {
      ColumnInfo info = infos.get(i);
      System.out.println("name:" + info.name);

      Iterator<Short> iterator = info.idxs.iterator();
      while (iterator.hasNext()) {
        System.out.println(iterator.next());
      }
    }
  }

  void loadColmnInfoFromNavigator(FileSystem fs, Path naviPath)
      throws Exception {
    FSDataInputStream in = fs.open(naviPath);

    int magic = in.readInt();
    if (magic != ConstVar.NaviMagic) {
      throw new SEException.ErrorFileFormat("invalid navi magic:" + magic
          + ",file:" + naviPath.toString());
    }

    short infoNum = in.readShort();
    for (int i = 0; i < infoNum; i++) {
      infos.add(loadColumnInfo(in));
    }
  }

  ColumnInfo loadColumnInfo(FSDataInputStream in) throws IOException {
    ColumnInfo info = new ColumnInfo();

    short nameLen = in.readShort();
    byte[] nameBuf = new byte[nameLen];
    in.readFully(nameBuf);
    info.name = new String(nameBuf);

    short idxLen = in.readShort();
    for (int i = 0; i < idxLen; i++) {
      info.idxs.add(in.readShort());
    }

    info.beginKey = in.readInt();
    info.endKey = in.readInt();
    info.beginLine = in.readInt();
    info.endLine = in.readInt();

    return info;
  }

  public ArrayList<ColumnInfo> infos() {
    return infos;
  }

  public ArrayList<String> getFileNameByIndex(ArrayList<Short> idx) {
    if (idx == null) {
      return null;
    }

    if (idx.size() == 0) {
      return null;
    }

    if (infos.size() == 0) {
      return null;
    }

    LinkedHashSet<String> result = new LinkedHashSet<String>();
    short foundTimes = 0;
    int size = idx.size();
    int count = 0;
    for (int i = 0; i < size; i++) {
      count = 0;
      while (count < infos.size()) {
        ColumnInfo info = infos.get(count);

        if (!info.idxs.contains(idx.get(i))) {

          count++;
          continue;
        } else {
          foundTimes++;
          result.add(info.name);
          break;
        }
      }
    }

    if (foundTimes == size) {
      ArrayList<String> rrArrayList = new ArrayList<String>();
      Iterator<String> iterator = result.iterator();
      while (iterator.hasNext()) {
        rrArrayList.add(iterator.next());
      }

      return rrArrayList;
    } else {
      return null;
    }
  }
}
