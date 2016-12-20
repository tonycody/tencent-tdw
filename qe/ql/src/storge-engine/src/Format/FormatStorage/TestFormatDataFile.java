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

package FormatStorage;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell.ExitCodeException;

import Comm.Util;
import Comm.ConstVar;
import Comm.Util;
import FormatStorage.FieldMap.Field;
import FormatStorage.Unit.FieldValue;
import FormatStorage.Unit.Record;

public class TestFormatDataFile {
  public static final Log LOG = LogFactory.getLog("TestFormatDataFile");

  static String fileName = "MR_input/TestFormatDataFile";
  static Head head = new Head();
  static int recordNum = 100;

  static void writeFile() throws Exception {
    Configuration conf = new Configuration();
    FormatDataFile fd = new FormatDataFile(conf);
    fd.create(fileName, head);

    for (int i = 0; i < recordNum; i++) {
      Record record = new Record((short) 7);
      record.addValue(new FieldValue((byte) (1 + i), (short) 0));
      record.addValue(new FieldValue((short) (2 + i), (short) 1));
      record.addValue(new FieldValue((int) (3 + i), (short) 2));
      record.addValue(new FieldValue((long) (4 + i), (short) 3));
      record.addValue(new FieldValue((float) (5.5 + i), (short) 4));
      record.addValue(new FieldValue((double) (6.6 + i), (short) 5));
      record.addValue(new FieldValue("hello konten" + i, (short) 6));

      fd.addRecord(record);
    }

    fd.close();
  }

  static void getRecordByLine() throws Exception {
    Configuration conf = new Configuration();
    FormatDataFile fd2 = new FormatDataFile(conf);
    fd2.open(fileName);
    Record record = fd2.getRecordByLine(-1);
    if (record != null) {
      System.out.println("should get null, line -1");
      fd2.close();
      return;
    }

    fd2.getRecordByLine(100);
    fd2.getRecordByLine(300);

    fd2.getRecordByLine(2000000);
    fd2.getRecordByLine(2100000);

    fd2.getRecordByLine(30000000);
    fd2.getRecordByLine(3000000);

    for (int i = 0; i < recordNum; i++) {
      record = fd2.getRecordByLine(i);
      if (record == null) {
        System.out.println("should not get null, line:" + i);
        fd2.close();
        return;
      }

      judgeNotFixedRecord(record, i);
    }
  }

  static void getRecordByValue() throws Exception {
    Configuration conf = new Configuration();
    FormatDataFile fd3 = new FormatDataFile(conf);
    fd3.open(fileName);

    FieldValue[] values = new FieldValue[2];
    values[0] = new FieldValue((byte) 2, (short) 1);
    values[1] = new FieldValue((short) 2, (short) 3);

    Record[] records = fd3.getRecordByOrder(values, values.length);
    if (records != null) {
      System.out.println("should get null");
    }

    for (int i = 0; i < 1; i++) {
      values[0] = new FieldValue((byte) (1 + i), (short) 1);
      values[1] = new FieldValue((short) (2 + i), (short) 3);

      records = fd3.getRecordByOrder(values, values.length);
      if (records == null) {
        System.out.println("should not get null:" + i);
        return;
      }
      if (records.length != 1) {
        System.out.println("error record len:" + records.length);
      }

      judgeNotFixedRecord(records[0], i);
    }

  }

  public static void main(String[] argv) throws Exception {

    try {
      FieldMap fieldMap = new FieldMap();
      fieldMap.addField(new Field(ConstVar.FieldType_Byte,
          ConstVar.Sizeof_Byte, (short) 1));
      fieldMap.addField(new Field(ConstVar.FieldType_Short,
          ConstVar.Sizeof_Short, (short) 3));
      fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int,
          (short) 5));
      fieldMap.addField(new Field(ConstVar.FieldType_Long,
          ConstVar.Sizeof_Long, (short) 7));
      fieldMap.addField(new Field(ConstVar.FieldType_Float,
          ConstVar.Sizeof_Float, (short) 9));
      fieldMap.addField(new Field(ConstVar.FieldType_Double,
          ConstVar.Sizeof_Double, (short) 11));
      fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short) 13));

      head.setFieldMap(fieldMap);

      getRecordByLine();

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
    byte[] buf = record.fieldValues().get(index).value;
    if (buf == null) {
      System.out.println("value should not null");
    }
    if (buf[0] != (byte) (1 + line)) {
      System.out.println("error bvalue:" + buf[0] + ",:" + (byte) (1 + line));
    }
    byte type = record.fieldValues().get(index).type;
    int len = record.fieldValues().get(index).len;
    short idx = record.fieldValues().get(index).idx;
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
    buf = record.fieldValues().get(index).value;
    if (buf == null) {
      System.out.println("value should not null");
    }
    short sval = Util.bytes2short(buf, 0, ConstVar.Sizeof_Short);
    if (sval != (short) (2 + line)) {
      System.out.println("error svalue:" + sval + ",:" + (short) (2 + line));
    }
    type = record.fieldValues().get(index).type;
    len = record.fieldValues().get(index).len;
    idx = record.fieldValues().get(index).idx;
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
    buf = record.fieldValues().get(index).value;
    if (buf == null) {
      System.out.println("value should not null");
    }
    int ival = Util.bytes2int(buf, 0, ConstVar.Sizeof_Int);
    if (ival != 3 + line) {
      System.out.println("error ivalue:" + ival + ",:" + (3 + line));
    }
    type = record.fieldValues().get(index).type;
    len = record.fieldValues().get(index).len;
    idx = record.fieldValues().get(index).idx;
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
    buf = record.fieldValues().get(index).value;
    if (buf == null) {
      System.out.println("value should not null");
    }
    long lval = Util.bytes2long(buf, 0, ConstVar.Sizeof_Long);
    if (lval != 4 + line) {
      System.out.println("error lvalue:" + lval + ",:" + (4 + line));
    }
    type = record.fieldValues().get(index).type;
    len = record.fieldValues().get(index).len;
    idx = record.fieldValues().get(index).idx;
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
    buf = record.fieldValues().get(index).value;
    if (buf == null) {
      System.out.println("value should not null");
    }
    float fval = Util.bytes2float(buf, 0);
    if (fval != (float) (5.5 + line)) {
      System.out.println("error fvalue:" + fval + ",:" + (float) (5.5 + line));
    }
    type = record.fieldValues().get(index).type;
    len = record.fieldValues().get(index).len;
    idx = record.fieldValues().get(index).idx;
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
    buf = record.fieldValues().get(index).value;
    if (buf == null) {
      System.out.println("value should not null");
    }
    double dval = Util.bytes2double(buf, 0);
    if (dval != (double) (6.6 + line)) {
      System.out.println("error dvalue:" + dval + ",:" + (double) (6.6 + line));
    }
    type = record.fieldValues().get(index).type;
    len = record.fieldValues().get(index).len;
    idx = record.fieldValues().get(index).idx;
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
    buf = record.fieldValues().get(index).value;
    if (buf == null) {
      System.out.println("value should not null");
    }
    String str = new String(buf);
    String tstr = "hello konten" + line;
    if (!str.equals(tstr)) {
      System.out.println("error val:" + str + ",tstr:" + tstr);
    }
    type = record.fieldValues().get(index).type;
    len = record.fieldValues().get(index).len;
    idx = record.fieldValues().get(index).idx;
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
