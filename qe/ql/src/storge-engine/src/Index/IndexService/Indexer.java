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
package IndexService;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import FormatStorage1.IColumnDataFile;
import FormatStorage1.IFormatDataFile;
import FormatStorage1.IRecord;

public class Indexer {
  public static final Log LOG = LogFactory.getLog(Indexer.class);

  private static Configuration conf = new Configuration();
  private static FileSystem fs;
  static {
    try {
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  ArrayList<IRecord.IFValue> valuestart;
  ArrayList<IRecord.IFValue> valueend;
  int fieldnum;
  HashMap<String, IFormatDataFile> openedifdfs = new HashMap<String, IFormatDataFile>();
  HashMap<String, IColumnDataFile> openedicdfs = new HashMap<String, IColumnDataFile>();
  ArrayList<String> indexfilesrelated = new ArrayList<String>();
  int currentindexfile = -1;
  IFormatDataFile currentifdf;
  IRecord returnrecord = null;
  boolean recordgot = false;

  boolean returnallfield;
  ArrayList<Integer> idxs = null;
  IRecord indexrec = null;

  public Indexer(String indexdir, List<String> parts,
      List<IRecord.IFValue> startvalue, List<IRecord.IFValue> endvalue,
      String indexids, int fieldnum) throws IOException {
    this.fieldnum = fieldnum;
    if (indexids == null || indexids.length() <= 0) {
      returnallfield = true;
    } else {
      idxs = new ArrayList<Integer>();
      String[] strs = indexids.split(",");
      for (String str : strs) {
        idxs.add(Integer.parseInt(str.trim()));
      }
      returnallfield = false;
    }

    valuestart = new ArrayList<IRecord.IFValue>();
    for (IRecord.IFValue fv : startvalue) {
      valuestart.add((IRecord.IFValue) fv.clone());
    }
    valueend = new ArrayList<IRecord.IFValue>();
    for (IRecord.IFValue fv : endvalue) {
      valueend.add((IRecord.IFValue) fv.clone());
    }

    if (valuestart.size() != valueend.size()) {
      throw new IOException(
          "the startvalue and the endvalue must have the same size");
    }

    for (int i = 0; i < valuestart.size(); i++) {
      if (valuestart.get(i).type().type() != valueend.get(i).type().type()) {
        throw new IOException(
            "the start value and the end value must have the same type in every fieldvalue");
      }
    }

    indexfilesrelated.clear();
    indexdir = indexdir.endsWith("/") ? indexdir : (indexdir + "/");
    if (parts == null || parts.size() <= 0) {
      parts = getIndexPartName(indexdir);
    }

    for (String part : parts) {
      Path partpath = new Path(indexdir + part);
      if (fs.exists(partpath)) {
        FileStatus[] indexfiles = fs.listStatus(partpath);
        for (FileStatus fileStatus : indexfiles) {
          indexfilesrelated.add(fileStatus.getPath().toString());
        }
      }
    }
    currentindexfile = 0;
    if (!initializeifdf()) {
      currentifdf = null;
    }
  }

  private boolean initializeifdf() throws IOException {
    for (; currentindexfile < indexfilesrelated.size(); currentindexfile++) {
      currentifdf = new IFormatDataFile(conf);
      currentifdf.open(indexfilesrelated.get(currentindexfile));
      indexrec = currentifdf.getIRecordObj();
      if (currentifdf.fileInfo().head().fieldMap().fieldtypes().size() < valuestart
          .size() + 2) {
        throw new IOException(
            "input value field size is more than index can support");
      } else {
        boolean ok = true;
        for (int i = 0; i < valuestart.size(); i++) {
          if (currentifdf.fileInfo().head().fieldMap().fieldtypes().get(i)
              .type() != valuestart.get(i).type().type()) {
            ok = false;
            LOG.info("index type:\t"
                + currentifdf.fileInfo().head().fieldMap().fieldtypes().get(i)
                    .type());
            LOG.info("input type:\t" + valuestart.get(i).type().type());
            throw new IOException(
                "input value field type is not fit the index field type");
          }
        }

        if (ok && currentifdf.seek(valuestart.get(0))) {
          return true;
        }
      }
      currentifdf.close();
    }
    return false;
  }

  public Indexer(String indexdir, List<String> parts,
      List<IRecord.IFValue> values, String indexids, int fieldnum)
      throws IOException {
    this(indexdir, parts, values, values, indexids, fieldnum);
  }

  public boolean hasNext() throws Exception {
    if (recordgot) {
      return true;
    }

    while (true) {
      if (currentindexfile >= indexfilesrelated.size()) {
        return false;
      }
      if (currentifdf != null && getirecord(currentifdf, indexrec)) {
        break;
      } else {
        if (currentifdf != null)
          currentifdf.close();
        currentifdf = null;
        currentindexfile++;
        if (!initializeifdf())
          return false;
        indexrec = currentifdf.getIRecordObj();
      }
    }

    IRecord.IFValue ifv = indexrec.getByIdx(indexrec.fieldnum() - 2);
    int fileindex = (Short) ifv.data();
    int line = (Integer) indexrec.getByIdx(indexrec.fieldnum() - 1).data();
    String datafile = currentifdf.fileInfo().head().getUdi().infos()
        .get(fileindex);
    boolean column = currentifdf.fileInfo().head().getUdi().infos().get(123456)
        .equals("column");
    if (!column) {
      if (!openedifdfs.containsKey(datafile)) {
        IFormatDataFile ifdf = new IFormatDataFile(conf);
        ifdf.open(datafile);
        openedifdfs.put(datafile, ifdf);
      }
      IRecord record = openedifdfs.get(datafile).getByLine(line);
      returnrecord = record;
    } else {
      if (!openedicdfs.containsKey(datafile)) {
        IColumnDataFile icdf = new IColumnDataFile(conf);
        if (returnallfield)
          icdf.open(datafile);
        else
          icdf.open(datafile, idxs);
        openedicdfs.put(datafile, icdf);
      }
      IRecord record = openedicdfs.get(datafile).getByLine(line);
      returnrecord = record;
    }
    recordgot = true;
    return true;
  }

  private boolean getirecord(IFormatDataFile ifdf, IRecord irec)
      throws IOException {
    while (ifdf.next(irec)) {
      int flag = check(irec);
      if (flag == 0)
        return true;
      if (flag == 1)
        return false;
    }
    return false;
  }

  private int check(IRecord irec) {
    int compare = -1;
    if (irec.fieldnum() < valueend.size() + 2)
      return -1;
    for (int i = 0; i < valuestart.size(); i++) {
      compare = irec.getByIdx(i).compareTo(valuestart.get(i));
      if (compare < 0)
        return -1;
      if (compare > 0)
        break;
    }
    for (int i = 0; i < valueend.size(); i++) {
      compare = irec.getByIdx(i).compareTo(valueend.get(i));
      if (compare > 0)
        return 1;
      if (compare < 0)
        break;
    }
    return 0;
  }

  public IRecord next() throws Exception {
    if (recordgot) {
      recordgot = false;
      return returnrecord;
    }
    if (hasNext())
      return returnrecord;
    return null;
  }

  public void close() {
    try {
      if (currentifdf != null) {
        currentifdf.close();
        currentifdf = null;
      }
      if (openedifdfs != null)
        for (IFormatDataFile ifdf : openedifdfs.values()) {
          if (ifdf != null) {
            ifdf.close();
          }
        }
      if (openedicdfs != null)
        for (IColumnDataFile icdf : openedicdfs.values()) {
          if (icdf != null) {
            icdf.close();
          }
        }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public Indexer() {
  }

  public int recordsnum(String indexdir, List<String> parts,
      List<IRecord.IFValue> startvalue) throws IOException {
    return recordsnum(indexdir, parts, startvalue, startvalue);
  }

  public int recordsnum(String indexdir, List<String> parts,
      List<IRecord.IFValue> startvalue, List<IRecord.IFValue> endvalue)
      throws IOException {
    int num = 0;
    if (parts == null || parts.size() <= 0) {
      parts = getIndexPartName(indexdir);
    }
    indexdir = indexdir.endsWith("/") ? indexdir : (indexdir + "/");
    List<IRecord.IFValue> valstart = new ArrayList<IRecord.IFValue>();
    for (IRecord.IFValue fv : startvalue) {
      valstart.add(fv.clone());
    }
    List<IRecord.IFValue> valend = new ArrayList<IRecord.IFValue>();
    for (IRecord.IFValue fv : endvalue) {
      valend.add(fv.clone());
    }

    if (valstart.size() != valend.size()) {
      throw new IOException(
          "the startvalue and the endvalue must have the same size");
    }

    for (int i = 0; i < valstart.size(); i++) {
      if (valstart.get(i).type().type() != valend.get(i).type().type()) {
        throw new IOException(
            "the start value and the end value must have the same type in every fieldvalue");
      }
    }

    for (String partname : parts) {
      Path partfile = new Path(indexdir + partname);
      FileStatus[] indexfiles = fs.listStatus(partfile);
      for (FileStatus indexfile : indexfiles) {
        IFormatDataFile ifdf = new IFormatDataFile(conf);
        ifdf.open(indexfile.getPath().toString());
        List<IndexValue> res = getIndexResRange(ifdf, valstart, valend, -1);
        ifdf.close();
        num += res.size();
      }
    }
    return num;
  }

  public List<IRecord> get(String indexdir, List<String> parts,
      List<IRecord.IFValue> values, int limit, String indexids, int fieldnum)
      throws Exception {
    return getRange(indexdir, parts, values, values, limit, indexids, fieldnum);
  }

  private List<String> getIndexPartName(String indexdir) throws IOException {
    List<String> result = new ArrayList<String>();
    Path indexpartdir = new Path(indexdir);
    if (fs.exists(indexpartdir)) {
      FileStatus[] fss = fs.listStatus(indexpartdir);
      for (FileStatus status : fss) {
        result.add(status.getPath().getName());
      }
    }
    return result;
  }

  private List<IRecord> getRecord(
      TreeMap<String, TreeSet<Integer>> indexresult,
      HashMap<String, Boolean> iscolumn, String indexids, int limit,
      int fieldnum) throws IOException {
    long time = System.currentTimeMillis();
    List<IRecord> result = new ArrayList<IRecord>();
    boolean returnallfields = false;
    String[] idxids;
    ArrayList<Integer> idxs = new ArrayList<Integer>();
    if (indexids == null) {
      returnallfields = true;
      idxids = new String[0];
    } else {
      idxids = indexids.split(",");
      for (int i = 0; i < idxids.length; i++) {
        idxs.add(Integer.parseInt(idxids[i].trim()));
      }
    }
    int i = 0;
    label: for (String file : indexresult.keySet()) {
      if (!iscolumn.get(file)) {
        IFormatDataFile ifdf = new IFormatDataFile(conf);
        if (returnallfields)
          ifdf.open(file);
        else
          ifdf.open(file, idxs);
        for (Integer line : indexresult.get(file)) {
          if (limit >= 0 && i >= limit) {
            ifdf.close();
            break label;
          }
          IRecord rec = ifdf.getByLine(line);
          result.add(rec);
          i++;
        }
        ifdf.close();
      } else {
        IColumnDataFile icdf = new IColumnDataFile(conf);
        if (returnallfields)
          icdf.open(file);
        else
          icdf.open(file, idxs);
        for (Integer line : indexresult.get(file)) {
          if (limit >= 0 && i >= limit) {
            icdf.close();
            break label;
          }
          IRecord rec = icdf.getByLine(line);
          result.add(rec);
          i++;
        }
        icdf.close();
      }
    }
    System.out.println("getRecord time:\t"
        + (System.currentTimeMillis() - time) / 1000 + "s");
    return result;
  }

  public List<IRecord> getRange(String indexdir, List<String> parts,
      List<IRecord.IFValue> startvalue, List<IRecord.IFValue> endvalue,
      int limit, String indexids, int fieldnum) throws IOException {

    ArrayList<IRecord.IFValue> valuestart1 = new ArrayList<IRecord.IFValue>();
    for (IRecord.IFValue fv : startvalue) {
      valuestart1.add(fv.clone());
    }
    ArrayList<IRecord.IFValue> valueend1 = new ArrayList<IRecord.IFValue>();
    for (IRecord.IFValue fv : endvalue) {
      valueend1.add(fv.clone());
    }

    if (valuestart1.size() != valueend1.size()) {
      throw new IOException(
          "the startvalue and the endvalue must have the same size");
    }

    for (int i = 0; i < valuestart1.size(); i++) {
      if (valuestart1.get(i).type().type() != valueend1.get(i).type().type()) {
        throw new IOException(
            "the start value and the end value must have the same type in every fieldvalue");
      }
    }

    if (parts == null || parts.size() <= 0) {
      parts = getIndexPartName(indexdir);
    }

    List<IRecord> result = new ArrayList<IRecord>();
    for (String part : parts) {
      if (limit > 0 && limit - result.size() <= 0)
        break;
      result.addAll(getRange1(indexdir, part, valuestart1, valueend1, indexids,
          limit - result.size(), fieldnum));
    }
    return result;
  }

  private List<IRecord> getRange1(String indexdir, String partname,
      List<IRecord.IFValue> startvalue, List<IRecord.IFValue> endvalue,
      String indexids, int limit, int fieldnum) throws IOException {
    String dir = indexdir.endsWith("/") ? indexdir : (indexdir + "/");
    Path partfile = new Path(dir + partname);

    TreeMap<String, TreeSet<Integer>> indexresult = new TreeMap<String, TreeSet<Integer>>();
    HashMap<String, Boolean> iscolumn = new HashMap<String, Boolean>();
    FileStatus[] fss = fs.listStatus(partfile);
    long time = System.currentTimeMillis();
    for (FileStatus status : fss) {
      IFormatDataFile ifdf = new IFormatDataFile(conf);
      ifdf.open(status.getPath().toString());

      if (ifdf.fileInfo().head().fieldMap().fieldtypes().size() < startvalue
          .size() + 2) {
        throw new IOException(
            "input value field size is more than index can support");
      }
      for (int i = 0; i < startvalue.size(); i++) {
        if (ifdf.fileInfo().head().fieldMap().fieldtypes().get(i).type() != startvalue
            .get(i).type().type()) {
          throw new IOException(
              "input value field type is not fit the index field type");
        }
      }

      HashMap<Integer, String> infos = ifdf.fileInfo().head().getUdi().infos();
      List<IndexValue> res = getIndexResRange(ifdf, startvalue, endvalue, limit);
      for (IndexValue iv : res) {
        int fileid = iv.getFileindex();
        String filename = infos.get(fileid);
        if (!indexresult.containsKey(filename)) {
          indexresult.put(filename, new TreeSet<Integer>());
          iscolumn.put(
              filename,
              ifdf.fileInfo().head().getUdi().infos().get(123456)
                  .equals("column"));

        }
        indexresult.get(filename).add(iv.getRowid());
      }
      ifdf.close();
    }
    System.out.println("getIndexResRange time:\t"
        + (System.currentTimeMillis() - time) / 1000 + "s");
    System.out.println("related file num:\t" + indexresult.size());
    return getRecord(indexresult, iscolumn, indexids, limit, fieldnum);
  }

  private List<IndexValue> getIndexResRange(IFormatDataFile ifdf,
      List<IRecord.IFValue> valstart, List<IRecord.IFValue> valend, int limit)
      throws IOException {
    List<IndexValue> result = new ArrayList<IndexValue>();
    if (valstart == null || valstart.size() <= 0)
      return result;
    IRecord.IFValue fv = valstart.get(0);
    if (!ifdf.seek(fv)) {
      return result;
    }
    int recnum = 0;
    int compare = 0;
    while (compare <= 0) {
      if (limit > 0 && recnum >= limit)
        break;
      IRecord record = ifdf.getIRecordObj();
      if (!ifdf.next(record)) {
        break;
      }

      for (int i = 0; i < valstart.size(); i++) {
        compare = record.getByIdx(i).compareTo(valstart.get(i));
        if (compare != 0)
          break;
      }
      if (compare < 0)
        continue;
      for (int i = 0; i < valend.size(); i++) {
        compare = record.getByIdx(i).compareTo(valend.get(i));
        if (compare != 0)
          break;
      }
      if (compare <= 0) {
        int fileindex = (Short) record.getByIdx(record.fieldnum() - 2).data();
        int line = (Integer) record.getByIdx(record.fieldnum() - 1).data();
        IndexValue iv = new IndexValue(fileindex, line);
        result.add(iv);
        recnum++;
      }
    }
    return result;
  }

  static void test() throws IOException {
    String indexdir = "/se/index/indextest1/testformat";
    ArrayList<IRecord.IFValue> values = new ArrayList<IRecord.IFValue>();
    values.add(new IRecord.IFValue(100, 2));
    ArrayList<IRecord.IFValue> values1 = new ArrayList<IRecord.IFValue>();
    values1.add(new IRecord.IFValue(10333443, 2));
    ArrayList<Integer> idxs = new ArrayList<Integer>();
    idxs.add(0);
    idxs.add(3);

    Indexer indexer = new Indexer();
    List<IRecord> recs = indexer.getRange(indexdir, null, values, values1, -1,
        "0,2,3,4", -1);
    for (IRecord rec : recs) {
      rec.show();
    }

  }

  public static void main(String[] args) throws IOException {
    System.out
        .println("input cmd:   indexdir  idx  type  startvalue  endvalue");
    String str = new BufferedReader(new InputStreamReader(System.in))
        .readLine();
    StringTokenizer st = new StringTokenizer(str);
    String indexdir = st.nextToken();
    int idx = Integer.parseInt(st.nextToken());
    String type = st.nextToken();
    String startvalue = st.nextToken();
    String endvalue = null;
    if (st.hasMoreTokens())
      endvalue = st.nextToken();
    else
      endvalue = startvalue;
    ArrayList<IRecord.IFValue> values = new ArrayList<IRecord.IFValue>();
    if (type.equalsIgnoreCase("byte"))
      values.add(new IRecord.IFValue(Byte.parseByte(startvalue), idx));
    if (type.equalsIgnoreCase("short"))
      values.add(new IRecord.IFValue(Short.parseShort(startvalue), idx));
    if (type.equalsIgnoreCase("int"))
      values.add(new IRecord.IFValue(Integer.parseInt(startvalue), idx));
    if (type.equalsIgnoreCase("long"))
      values.add(new IRecord.IFValue(Long.parseLong(startvalue), idx));
    if (type.equalsIgnoreCase("float"))
      values.add(new IRecord.IFValue(Float.parseFloat(startvalue), idx));
    if (type.equalsIgnoreCase("double"))
      values.add(new IRecord.IFValue(Double.parseDouble(startvalue), idx));
    if (type.equalsIgnoreCase("string"))
      values.add(new IRecord.IFValue(startvalue, idx));
    ArrayList<IRecord.IFValue> values1 = new ArrayList<IRecord.IFValue>();
    if (type.equalsIgnoreCase("byte"))
      values1.add(new IRecord.IFValue(Byte.parseByte(endvalue), idx));
    if (type.equalsIgnoreCase("short"))
      values1.add(new IRecord.IFValue(Short.parseShort(endvalue), idx));
    if (type.equalsIgnoreCase("int"))
      values1.add(new IRecord.IFValue(Integer.parseInt(endvalue), idx));
    if (type.equalsIgnoreCase("long"))
      values1.add(new IRecord.IFValue(Long.parseLong(endvalue), idx));
    if (type.equalsIgnoreCase("float"))
      values1.add(new IRecord.IFValue(Float.parseFloat(endvalue), idx));
    if (type.equalsIgnoreCase("double"))
      values1.add(new IRecord.IFValue(Double.parseDouble(endvalue), idx));
    if (type.equalsIgnoreCase("string"))
      values1.add(new IRecord.IFValue(endvalue, idx));
    Indexer indexer = new Indexer();
    List<IRecord> recs = indexer.getRange(indexdir, null, values, values1, 100,
        null, -1);
    for (IRecord rec : recs) {
      rec.show();
    }

  }
}
