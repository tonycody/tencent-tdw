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
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import Comm.ConstVar;
import FormatStorage.FieldMap;
import FormatStorage.FormatDataFile;
import FormatStorage.Head;
import FormatStorage.FieldMap.Field;
import FormatStorage.Unit.FieldValue;
import FormatStorage.Unit.Record;
import IndexService.IndexMergeMR;
import IndexStorage.IFieldMap;
import IndexStorage.IFieldType;
import IndexStorage.IFieldValue;
import IndexStorage.IFileInfo;
import IndexStorage.IFormatDataFile;
import IndexStorage.IHead;
import IndexStorage.IRecord;
import IndexStorage.ISegmentInfo;
import IndexStorage.IUnitInfo;
import IndexStorage.IUserDefinedHeadInfo;

public class TestUtil {
  static String file = "testtesttesttesttesttest";
  static Configuration conf = new Configuration();
  static FileSystem fs;
  static {
    // conf.set("fs.default.name", "file:///");
    try {
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void writeifdfile(String fileName, int num) throws IOException {
    IFormatDataFile ifdf = new IFormatDataFile(new Configuration());
    IHead head = new IHead();
    IFieldMap fieldMap = new IFieldMap();
    fieldMap.addFieldType(new IFieldType.IFieldByteType());
    fieldMap.addFieldType(new IFieldType.IFieldShortType());
    fieldMap.addFieldType(new IFieldType.IFieldIntType());
    fieldMap.addFieldType(new IFieldType.IFieldLongType());
    fieldMap.addFieldType(new IFieldType.IFieldFloatType());
    fieldMap.addFieldType(new IFieldType.IFieldDoubleType());
    head.setFieldMap(fieldMap);
    head.setPrimaryIndex((short) 2);
    IUserDefinedHeadInfo udi = new IUserDefinedHeadInfo();
    udi.addInfo(0, fileName);

    ifdf.create(fileName, head);

    for (int i = 0; i < num; i++) {
      IRecord record = new IRecord();
      record.addFieldValue(new IFieldValue((byte) i));
      record.addFieldValue(new IFieldValue((short) (2 * i)));
      record.addFieldValue(new IFieldValue(3 * i));
      record.addFieldValue(new IFieldValue((long) 4 * i));
      record.addFieldValue(new IFieldValue((float) 5 * i));
      record.addFieldValue(new IFieldValue((double) 6 * i, (short) 5));
      ifdf.addRecord(record);
    }
    ifdf.close();

  }

  public static IFileInfo genfileinfo(boolean var, int index)
      throws IOException {
    IFileInfo fileInfo = new IFileInfo(conf);
    IHead head = new IHead();
    IFieldMap fieldMap = new IFieldMap();
    fieldMap.addFieldType(new IFieldType.IFieldByteType());
    fieldMap.addFieldType(new IFieldType.IFieldShortType());
    fieldMap.addFieldType(new IFieldType.IFieldIntType());
    fieldMap.addFieldType(new IFieldType.IFieldLongType());
    fieldMap.addFieldType(new IFieldType.IFieldFloatType());
    fieldMap.addFieldType(new IFieldType.IFieldDoubleType());
    if (var) {
      fieldMap.addFieldType(new IFieldType.IFieldStringType());
    }
    head.setFieldMap(fieldMap);
    if (index > -1) {
      head.setPrimaryIndex((short) index);
    }
    IUserDefinedHeadInfo udi = new IUserDefinedHeadInfo();
    udi.addInfo(0, file);
    head.setUdi(udi);

    fileInfo.initialize(file, head);
    return fileInfo;
  }

  public static IUnitInfo genunitinfo(IFileInfo fileInfo, int sid, int uid,
      int num) {
    IUnitInfo info = new IUnitInfo(fileInfo, sid, uid);
    int x = sid * num * num + uid * num;
    for (int i = 0; i < num; i++) {
      IRecord record = new IRecord();
      record.addFieldValue(new IFieldValue((byte) (x + i)));
      record.addFieldValue(new IFieldValue((short) (x + i)));
      record.addFieldValue(new IFieldValue(x + i));
      record.addFieldValue(new IFieldValue((long) (x + i)));
      record.addFieldValue(new IFieldValue((float) (x + i)));
      record.addFieldValue(new IFieldValue((double) (x + i), (short) 5));
      info.update(record);
      fileInfo.increasecurrentline();
    }

    return info;
  }

  public static ISegmentInfo genseginfo(IFileInfo fileInfo, int sid, int num)
      throws IOException {
    ISegmentInfo info = new ISegmentInfo(fileInfo, sid);

    for (int i = 0; i < num; i++) {
      info.update(genunitinfo(fileInfo, sid, i, num));
    }

    return info;
  }

  public static IRecord genrecord(int i, boolean var) {
    IRecord record = new IRecord();
    record.addFieldValue(new IFieldValue((byte) (i)));
    record.addFieldValue(new IFieldValue((short) (i)));
    record.addFieldValue(new IFieldValue(i));
    record.addFieldValue(new IFieldValue((long) (i)));
    record.addFieldValue(new IFieldValue((float) (i)));
    record.addFieldValue(new IFieldValue((double) (i), (short) 5));
    if (var) {
      record.addFieldValue(new IFieldValue("test"));
    }

    return record;
  }

  public static void genfdfseq(String datadir, int filenum, int recnum,
      boolean var, boolean overwrite) throws Exception {
    Head head = new Head();
    FieldMap fieldMap = new FieldMap();

    fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte,
        (short) 0));
    fieldMap.addField(new Field(ConstVar.FieldType_Short,
        ConstVar.Sizeof_Short, (short) 1));
    fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int,
        (short) 2));
    fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long,
        (short) 3));
    fieldMap.addField(new Field(ConstVar.FieldType_Float,
        ConstVar.Sizeof_Float, (short) 4));
    fieldMap.addField(new Field(ConstVar.FieldType_Double,
        ConstVar.Sizeof_Double, (short) 5));
    if (var) {
      fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short) 6));
    }
    head.setFieldMap(fieldMap);

    datadir = datadir.endsWith("/") ? datadir : (datadir + "/");
    if (overwrite && fs.exists(new Path(datadir))) {
      fs.delete(new Path(datadir), true);
      fs.mkdirs(new Path(datadir));
    } else if (overwrite) {
      fs.mkdirs(new Path(datadir));
    }
    int x = fs.listStatus(new Path(datadir)).length;

    for (int i = 0; i < filenum; i++) {
      FormatDataFile fdf = new FormatDataFile(conf);
      fdf.create(datadir + "datafile" + (x + i), head);
      int fieldnum = var ? 7 : 6;
      for (int j = 0; j < recnum; j++) {
        Record record = new Record(fieldnum);
        record.addValue(new FieldValue((byte) j, (short) 0));
        record.addValue(new FieldValue((short) j, (short) 1));
        record.addValue(new FieldValue((int) j, (short) 2));
        record.addValue(new FieldValue((long) j, (short) 3));
        record.addValue(new FieldValue((float) j, (short) 4));
        record.addValue(new FieldValue((double) j, (short) 5));
        if (var) {
          record.addValue(new FieldValue("test", (short) 6));
        }
        fdf.addRecord(record);
      }
      fdf.close();
    }
  }

  public static void genfdfrandom(String datadir, int filenum, int recnum,
      boolean var, boolean overwrite) throws Exception {
    Head head = new Head();
    FieldMap fieldMap = new FieldMap();

    fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte,
        (short) 0));
    fieldMap.addField(new Field(ConstVar.FieldType_Short,
        ConstVar.Sizeof_Short, (short) 1));
    fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int,
        (short) 2));
    fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long,
        (short) 3));
    fieldMap.addField(new Field(ConstVar.FieldType_Float,
        ConstVar.Sizeof_Float, (short) 4));
    fieldMap.addField(new Field(ConstVar.FieldType_Double,
        ConstVar.Sizeof_Double, (short) 5));
    if (var) {
      fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short) 6));
    }
    head.setFieldMap(fieldMap);

    datadir = datadir.endsWith("/") ? datadir : (datadir + "/");
    if (overwrite && fs.exists(new Path(datadir))) {
      fs.delete(new Path(datadir), true);
      fs.mkdirs(new Path(datadir));
    }
    int x = fs.listStatus(new Path(datadir)).length;
    Random r = new Random();
    for (int i = 0; i < filenum; i++) {
      FormatDataFile fdf = new FormatDataFile(conf);
      fdf.create(datadir + "datafile" + (x + i), head);
      int fieldnum = var ? 7 : 6;
      for (int j = 0; j < recnum; j++) {
        Record record = new Record(fieldnum);
        record.addValue(new FieldValue((byte) r.nextInt(), (short) 0));
        record.addValue(new FieldValue((short) r.nextInt(), (short) 1));
        record.addValue(new FieldValue(r.nextInt(), (short) 2));
        record.addValue(new FieldValue(r.nextLong(), (short) 3));
        record.addValue(new FieldValue(r.nextFloat(), (short) 4));
        record.addValue(new FieldValue(r.nextDouble(), (short) 5));
        if (var) {
          record.addValue(new FieldValue("test", (short) 6));
        }
      }
      fdf.close();
    }
  }

  public static void genifdfindex(String indexdir, int filenum, int recnum,
      short idx, boolean overwrite) throws IOException {
    indexdir = indexdir.endsWith("/") ? indexdir : (indexdir + "/");
    if (overwrite && fs.exists(new Path(indexdir))) {
      fs.delete(new Path(indexdir), true);
      fs.mkdirs(new Path(indexdir));
    }

    IHead head = new IHead();
    head.setPrimaryIndex(idx);
    IFieldMap map = new IFieldMap();
    map.addFieldType(new IFieldType.IFieldIntType());
    map.addFieldType(new IFieldType.IFieldShortType());
    map.addFieldType(new IFieldType.IFieldIntType());
    head.setFieldMap(map);
    IUserDefinedHeadInfo udi = new IUserDefinedHeadInfo();
    udi.addInfo(0, "testdata1");
    udi.addInfo(1, "testdata2");
    udi.addInfo(2, "testdata3");
    udi.addInfo(3, "testdata4");
    udi.addInfo(4, "testdata5");
    head.setUdi(udi);

    Random r = new Random();
    int x = fs.listStatus(new Path(indexdir)).length;
    for (int i = 0; i < filenum; i++) {
      IFormatDataFile ifdf = new IFormatDataFile(conf);
      ifdf.create(indexdir + "indexfile" + (x + i), head);
      int start = r.nextInt(5);
      for (int j = 0; j < recnum; j++) {
        IRecord rec = new IRecord();
        rec.addFieldValue(new IFieldValue(start));
        start += r.nextInt(5);
        rec.addFieldValue(new IFieldValue((short) r.nextInt(5)));
        rec.addFieldValue(new IFieldValue(r.nextInt(100000)));
        ifdf.addRecord(rec);
      }
      ifdf.close();
    }
  }

  public static void main(String[] args) throws Exception {
    String indexdir = "indexdir";
    StringBuffer sb = new StringBuffer();
    FileStatus[] ss = fs.listStatus(new Path(indexdir));
    for (FileStatus fileStatus : ss) {
      sb.append(fileStatus.getPath().toString()).append(",");
    }
    IndexMergeMR.run(sb.substring(0, sb.length() - 1), "indexdir1", conf);
  }
}
