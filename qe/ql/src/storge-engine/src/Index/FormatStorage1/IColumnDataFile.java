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
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import Comm.ConstVar;

public class IColumnDataFile implements IFileInterface {

  public static final Log LOG = LogFactory.getLog("IColumnDataFile");

  Configuration conf;
  int formatfiletype;
  HashMap<ArrayList<Integer>, IFormatDataFile> cp2ifdfs;
  HashMap<Integer, IFormatDataFile> idx2ifdfs;
  ArrayList<ArrayList<Integer>> columnprojects;
  private byte workstatus = ConstVar.WS_Init;
  private HashMap<Integer, IRecord.IFType> fieldtypes;
  TreeSet<Integer> readidxs;
  IHead head;

  public IColumnDataFile(Configuration conf) throws IOException {
    this.conf = conf;
    this.cp2ifdfs = new HashMap<ArrayList<Integer>, IFormatDataFile>();
    this.idx2ifdfs = new HashMap<Integer, IFormatDataFile>();
  }

  public void create(String fileName, IHead head,
      ArrayList<ArrayList<Integer>> columnprojects) throws IOException {
    if (columnprojects == null) {
      this.create(fileName, head);
      return;
    }
    this.columnprojects = columnprojects;
    this.head = head;
    fieldtypes = head.fieldMap().fieldtypes();
    int primaryindex = head.getPrimaryIndex();
    StringBuffer sb = new StringBuffer();
    for (ArrayList<Integer> cp : columnprojects) {
      IHead subhead = new IHead(head);
      subhead.setPrimaryIndex((short) -1);
      IFieldMap fieldMap = new IFieldMap();
      sb.setLength(0);
      sb.append("_idx");
      for (int i = 0; i < cp.size(); i++) {
        int c = cp.get(i);
        if (c == primaryindex)
          subhead.setPrimaryIndex(i);
        fieldMap.addFieldType(fieldtypes.get(c));
        sb.append(c).append("_");
      }
      subhead.setFieldMap(fieldMap);
      IFormatDataFile ifdf = new IFormatDataFile(conf);
      String subfile = fileName + sb.substring(0, sb.length() - 1);
      ifdf.create(subfile, subhead);
      this.cp2ifdfs.put(cp, ifdf);
      for (Integer c : cp) {
        this.idx2ifdfs.put(c, ifdf);
      }
    }
    workstatus = ConstVar.WS_Write;
  }

  @Override
  public void create(String fileName, IHead head) throws IOException {
    ArrayList<ArrayList<Integer>> columnprojects = new ArrayList<ArrayList<Integer>>();
    for (int i = 0; i < head.fieldMap().fieldtypes().size(); i++) {
      ArrayList<Integer> cp = new ArrayList<Integer>();
      cp.add(i);
      columnprojects.add(cp);
    }
    create(fileName, head, columnprojects);
  }

  @Override
  public int addRecord(IRecord record) throws IOException {
    for (Map.Entry<ArrayList<Integer>, IFormatDataFile> en : cp2ifdfs
        .entrySet()) {
      ArrayList<Integer> cp = en.getKey();
      IFormatDataFile ifdf = en.getValue();
      IRecord subrecord = ifdf.getIRecordObj();

      for (int i = 0; i < cp.size(); i++) {
        IRecord.IFValue fv = record.getByIdx(cp.get(i));
        if (fv != null)
          subrecord.addFieldValue(fv);
      }
      int res = ifdf.addRecord(subrecord);
      if (res != 0) {
        return res;
      }
    }
    return 0;
  }

  public void open(String fileName, ArrayList<Integer> idxs) throws IOException {
    this.columnprojects = new ArrayList<ArrayList<Integer>>();
    this.head = new IHead();
    this.readidxs = new TreeSet<Integer>();
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] statuss = fs.globStatus(new Path(fileName + "_idx*"));
    this.cp2ifdfs.clear();
    this.idx2ifdfs.clear();
    if (idxs != null && idxs.size() > 0) {
      readidxs.addAll(idxs);
      for (int i = 0; i < statuss.length; i++) {
        String file = statuss[i].getPath().toString();
        IFormatDataFile ifdf = null;
        String idxstr = file.substring(file.lastIndexOf("_idx") + 4);
        String[] sts = idxstr.split("_");
        TreeSet<Integer> ts = new TreeSet<Integer>();
        for (int j = 0; j < sts.length; j++) {
          ts.add(Integer.parseInt(sts[j]));
        }

        boolean contains = false;
        for (Integer id : idxs) {
          if (ts.contains(id)) {
            contains = true;
            if (ifdf == null) {
              ifdf = new IFormatDataFile(conf);
              ifdf.open(file);
            }
            this.idx2ifdfs.put(id, ifdf);
          }
        }
        if (contains) {
          ArrayList<Integer> cp = new ArrayList<Integer>();
          cp.addAll(ts);
          this.columnprojects.add(cp);
          this.cp2ifdfs.put(cp, ifdf);
        }
      }

    } else {
      for (int i = 0; i < statuss.length; i++) {
        String file = statuss[i].getPath().toString();
        IFormatDataFile ifdf = null;
        String idxstr = file.substring(file.lastIndexOf("_idx") + 4);
        String[] sts = idxstr.split("_");
        TreeSet<Integer> ts = new TreeSet<Integer>();
        ifdf = new IFormatDataFile(conf);
        ifdf.open(file);

        for (int j = 0; j < sts.length; j++) {
          int id = Integer.parseInt(sts[j]);
          ts.add(id);
          this.idx2ifdfs.put(id, ifdf);
        }

        ArrayList<Integer> cp = new ArrayList<Integer>();
        cp.addAll(ts);
        this.readidxs.addAll(ts);
        this.columnprojects.add(cp);
        this.cp2ifdfs.put(cp, ifdf);
      }
    }
    this.fieldtypes = new HashMap<Integer, IRecord.IFType>();
    for (Integer idx : this.readidxs) {
      this.fieldtypes.put(idx, this.idx2ifdfs.get(idx).fileInfo().head()
          .fieldMap().fieldtypes().get(idx));
    }
    workstatus = ConstVar.WS_Read;
  }

  @Override
  public void open(String fileName) throws IOException {
    this.open(fileName, null);
  }

  @Override
  public boolean seek(int line) throws IOException {
    for (IFormatDataFile ifdf : cp2ifdfs.values()) {
      if (!ifdf.seek(line)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean seek(IRecord.IFValue fv) throws IOException {

    throw new IOException("not support right now");
  }

  @Override
  public IRecord next() throws IOException {
    IRecord record = this.getIRecordObj();
    if (this.next(record))
      return record;
    return null;
  }

  @Override
  public boolean next(IRecord record) throws IOException {
    record.clear();
    for (IFormatDataFile ifdf : this.cp2ifdfs.values()) {
      IRecord subrec = ifdf.next();
      if (subrec == null)
        return false;
      for (int i = 0; i < subrec.fieldnum(); i++) {
        IRecord.IFValue fv = subrec.getByPos(i);
        if (fv != null && this.readidxs.contains(fv.idx()))
          record.addFieldValue(fv);
      }
    }
    return true;
  }

  @Override
  public IRecord getByLine(int line) throws IOException {
    if (this.seek(line))
      return this.next();
    return null;
  }

  @Override
  public ArrayList<IRecord> getByKey(IRecord.IFValue ifv) throws IOException {
    throw new IOException("not support roght now");
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
    throw new IOException("not support roght now");
  }

  @Override
  public void close() throws IOException {
    for (IFormatDataFile ifdf : cp2ifdfs.values()) {
      ifdf.close();
    }
  }

  @Override
  public int recnum() {
    return this.cp2ifdfs.values().iterator().next().recnum();
  }

  @Override
  public IRecord getIRecordObj() {
    if (workstatus == ConstVar.WS_Init)
      return null;
    IRecord rec = new IRecord(fieldtypes);
    return rec;
  }

  public HashMap<Integer, IRecord.IFType> fieldtypes() {
    return fieldtypes;
  }

  public static void main(String[] args) throws IOException {
  }
}
