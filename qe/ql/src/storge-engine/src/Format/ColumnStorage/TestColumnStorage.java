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
import java.util.List;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell.ExitCodeException;

import Comm.Util;
import Comm.ConstVar;
import Comm.Util;
import FormatStorage.FieldMap;
import FormatStorage.FormatDataFile;
import FormatStorage.Head;
import FormatStorage.FieldMap.Field;
import FormatStorage.Unit.FieldValue;
import FormatStorage.Unit.Record;

public class TestColumnStorage {
  public static final Log LOG = LogFactory.getLog("TestFormatDataFile");

  static String fileName1 = "TestColumnStorage/p1/field0-4";
  static String fileName2 = "TestColumnStorage/p1/field5-8";
  static String fileName3 = "TestColumnStorage/p1/field9-10";
  // "hdfs://tdw-172-25-38-253:54310/user/tdwadmin/TestColumnStorage";;
  static String dire = "TestColumnStorage/p1";;

  static int recordNum = 100;

  static FieldMap fieldMap = new FieldMap();

  static void writeFile1() throws Exception {
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

    head.setFieldMap(fieldMap);
    FormatDataFile fd = new FormatDataFile(new Configuration());
    fd.create(fileName1, head);

    for (int i = 0; i < recordNum; i++) {
      Record record = new Record(5);
      record.addValue(new FieldValue((byte) (1 + i), (short) 0));
      record.addValue(new FieldValue((short) (2 + i), (short) 1));
      record.addValue(new FieldValue((int) (3 + i), (short) 2));
      record.addValue(new FieldValue((long) (4 + i), (short) 3));
      record.addValue(new FieldValue((float) (5.5 + i), (short) 4));

      fd.addRecord(record);
    }

    fd.close();
  }

  static void writeFile2() throws Exception {
    Head head = new Head();
    FieldMap fieldMap = new FieldMap();
    fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte,
        (short) 5));
    fieldMap.addField(new Field(ConstVar.FieldType_Short,
        ConstVar.Sizeof_Short, (short) 6));
    fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int,
        (short) 7));
    fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long,
        (short) 8));

    head.setFieldMap(fieldMap);
    FormatDataFile fd = new FormatDataFile(new Configuration());
    fd.create(fileName2, head);

    for (int i = 0; i < recordNum; i++) {
      Record record = new Record(4);
      record.addValue(new FieldValue((byte) (1 + i), (short) 5));
      record.addValue(new FieldValue((short) (2 + i), (short) 6));
      record.addValue(new FieldValue((int) (3 + i), (short) 7));
      record.addValue(new FieldValue((long) (4 + i), (short) 8));

      fd.addRecord(record);
    }

    fd.close();
  }

  static void writeFile3() throws Exception {
    Head head = new Head();
    FieldMap fieldMap = new FieldMap();

    fieldMap.addField(new Field(ConstVar.FieldType_Double,
        ConstVar.Sizeof_Double, (short) 9));
    fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short) 10));

    head.setFieldMap(fieldMap);
    FormatDataFile fd = new FormatDataFile(new Configuration());
    fd.create(fileName3, head);

    for (int i = 0; i < recordNum; i++) {
      Record record = new Record(2);

      record.addValue(new FieldValue((double) (6.6 + i), (short) 9));
      record.addValue(new FieldValue("hello konten" + i, (short) 10));

      fd.addRecord(record);
    }

    fd.close();
  }

  static void getRecordByLine1(int line) throws Exception {
    ArrayList<Short> idx = new ArrayList<Short>();
    idx.add((short) 0);
    idx.add((short) 3);
    idx.add((short) 7);

    Path path = new Path(dire);
    ColumnStorageClient cs = new ColumnStorageClient(path, idx,
        new Configuration());

    Record record = cs.getRecordByLine(line);

    record.show();

    record.trim(fieldMap);

    LOG.info("after trim");
    record.show();

    List<FieldValue> list = record.toList();
    for (int i = 0; i < list.size(); i++)
      LOG.info("list.idx:" + list.get(i).idx() + ", type:" + list.get(i).type());
  }

  public static void main(String[] argv) throws Exception {

    try {
      if (argv.length < 1) {
        System.out.println("TestColumnStorage cmd[write | read ]");
        return;
      }

      String cmd = argv[0];
      if (cmd.equals("write")) {
        writeFile1();
        writeFile2();
        writeFile3();
      } else {
        if (argv.length < 2) {
          System.out.println("TestColumnStorage read line");
          return;
        }

        fieldMap.addField(new Field(ConstVar.FieldType_Byte,
            ConstVar.Sizeof_Byte, (short) 0));
        fieldMap.addField(new Field(ConstVar.FieldType_Short,
            ConstVar.Sizeof_Short, (short) 1));
        fieldMap.addField(new Field(ConstVar.FieldType_Int,
            ConstVar.Sizeof_Int, (short) 2));
        fieldMap.addField(new Field(ConstVar.FieldType_Long,
            ConstVar.Sizeof_Long, (short) 3));
        fieldMap.addField(new Field(ConstVar.FieldType_Float,
            ConstVar.Sizeof_Float, (short) 4));
        fieldMap.addField(new Field(ConstVar.FieldType_Byte,
            ConstVar.Sizeof_Byte, (short) 5));
        fieldMap.addField(new Field(ConstVar.FieldType_Short,
            ConstVar.Sizeof_Short, (short) 6));
        fieldMap.addField(new Field(ConstVar.FieldType_Int,
            ConstVar.Sizeof_Int, (short) 7));
        fieldMap.addField(new Field(ConstVar.FieldType_Long,
            ConstVar.Sizeof_Long, (short) 8));
        fieldMap.addField(new Field(ConstVar.FieldType_Double,
            ConstVar.Sizeof_Double, (short) 9));
        fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short) 10));

        int line = Integer.valueOf(argv[1]);
        getRecordByLine1(line);
      }
    } catch (IOException e) {
      e.printStackTrace();
      System.out.println("get IOException:" + e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("get exception:" + e.getMessage());
    }
  }

  static void judgeNotFixedRecord(Record record, int line) {
    int index = 0;
    byte[] buf = record.fieldValues().get(index).value();
    if (buf == null) {
      System.out.println("value should not null");
    }
    if (buf[0] != (byte) (1 + line)) {
      System.out.println("error bvalue:" + buf[0] + ",:" + (byte) (1 + line));
    }
    byte type = record.fieldValues().get(index).type();
    int len = record.fieldValues().get(index).len();
    short idx = record.fieldValues().get(index).idx();
    if (len != ConstVar.Sizeof_Byte) {
      System.out.println("error len:" + len);
    }
    if (type != ConstVar.FieldType_Byte) {
      System.out.println("error fieldType:" + type);
    }
    if (idx != 1) {
      System.out.println("error idx:" + idx);
    }

    index = 1;
    buf = record.fieldValues().get(index).value();
    if (buf == null) {
      System.out.println("value should not null");
    }
    short sval = Util.bytes2short(buf, 0, ConstVar.Sizeof_Short);
    if (sval != (short) (2 + line)) {
      System.out.println("error svalue:" + sval + ",:" + (short) (2 + line));
    }
    type = record.fieldValues().get(index).type();
    len = record.fieldValues().get(index).len();
    idx = record.fieldValues().get(index).idx();
    if (len != ConstVar.Sizeof_Short) {
      System.out.println("error len:" + len);
    }
    if (type != ConstVar.FieldType_Short) {
      System.out.println("error fieldType:" + type);
    }
    if (idx != 3) {
      System.out.println("error idx:" + idx);
    }

    index = 2;
    buf = record.fieldValues().get(index).value();
    if (buf == null) {
      System.out.println("value should not null");
    }
    int ival = Util.bytes2int(buf, 0, ConstVar.Sizeof_Int);
    if (ival != 3 + line) {
      System.out.println("error ivalue:" + ival + ",:" + (3 + line));
    }
    type = record.fieldValues().get(index).type();
    len = record.fieldValues().get(index).len();
    idx = record.fieldValues().get(index).idx();
    if (len != ConstVar.Sizeof_Int) {
      System.out.println("error len:" + len);
    }
    if (type != ConstVar.FieldType_Int) {
      System.out.println("error fieldType:" + type);
    }
    if (idx != 5) {
      System.out.println("error idx:" + idx);
    }

    index = 3;
    buf = record.fieldValues().get(index).value();
    if (buf == null) {
      System.out.println("value should not null");
    }
    long lval = Util.bytes2long(buf, 0, ConstVar.Sizeof_Long);
    if (lval != 4 + line) {
      System.out.println("error lvalue:" + lval + ",:" + (4 + line));
    }
    type = record.fieldValues().get(index).type();
    len = record.fieldValues().get(index).len();
    idx = record.fieldValues().get(index).idx();
    if (len != ConstVar.Sizeof_Long) {
      System.out.println("error len:" + len);
    }
    if (type != ConstVar.FieldType_Long) {
      System.out.println("error fieldType:" + type);
    }
    if (idx != 7) {
      System.out.println("error idx:" + idx);
    }

    index = 4;
    buf = record.fieldValues().get(index).value();
    if (buf == null) {
      System.out.println("value should not null");
    }
    float fval = Util.bytes2float(buf, 0);
    if (fval != (float) (5.5 + line)) {
      System.out.println("error fvalue:" + fval + ",:" + (float) (5.5 + line));
    }
    type = record.fieldValues().get(index).type();
    len = record.fieldValues().get(index).len();
    idx = record.fieldValues().get(index).idx();
    if (len != ConstVar.Sizeof_Float) {
      System.out.println("error len:" + len);
    }
    if (type != ConstVar.FieldType_Float) {
      System.out.println("error fieldType:" + type);
    }
    if (idx != 9) {
      System.out.println("error idx:" + idx);
    }

    index = 5;
    buf = record.fieldValues().get(index).value();
    if (buf == null) {
      System.out.println("value should not null");
    }
    double dval = Util.bytes2double(buf, 0);
    if (dval != (double) (6.6 + line)) {
      System.out.println("error dvalue:" + dval + ",:" + (double) (6.6 + line));
    }
    type = record.fieldValues().get(index).type();
    len = record.fieldValues().get(index).len();
    idx = record.fieldValues().get(index).idx();
    if (len != ConstVar.Sizeof_Double) {
      System.out.println("error len:" + len);
    }
    if (type != ConstVar.FieldType_Double) {
      System.out.println("error fieldType:" + type);
    }
    if (idx != 11) {
      System.out.println("error idx:" + idx);
    }

    index = 6;
    buf = record.fieldValues().get(index).value();
    if (buf == null) {
      System.out.println("value should not null");
    }
    String str = new String(buf);
    String tstr = "hello konten" + line;
    if (!str.equals(tstr)) {
      System.out.println("error val:" + str + ",tstr:" + tstr);
    }
    type = record.fieldValues().get(index).type();
    len = record.fieldValues().get(index).len();
    idx = record.fieldValues().get(index).idx();
    if (len != str.length()) {
      System.out.println("error len:" + len);
    }
    if (type != ConstVar.FieldType_String) {
      System.out.println("error fieldType:" + type);
    }
    if (idx != 13) {
      System.out.println("error idx:" + idx);
    }
  }
}
