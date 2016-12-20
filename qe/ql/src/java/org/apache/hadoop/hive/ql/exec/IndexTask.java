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

package org.apache.hadoop.hive.ql.exec;

import java.io.DataOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.IndexQueryInfo;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.IndexValue;
import org.apache.hadoop.hive.ql.plan.indexWork;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.lib.Node;

import StorageEngineClient.FormatStorageSerDe;
import FormatStorage1.IRecord;
import IndexService.Indexer;

@SuppressWarnings("deprecation")
public class IndexTask extends Task<indexWork> implements Serializable {
  private static final long serialVersionUID = 1L;

  private LazySimpleSerDe mSerde;
  private FormatStorageSerDe formatSerde;
  private JobConf job;

  static final private int separator = Utilities.ctrlaCode;
  static final private int terminator = Utilities.newLineCode;
  static final private int MAX_OUTPUT_RECORD = 10000;

  @SuppressWarnings("unchecked")
  public void initialize(HiveConf conf, DriverContext ctx) {
    super.initialize(conf, ctx);

    try {
      job = new JobConf(conf, ExecDriver.class);

      mSerde = new LazySimpleSerDe();
      Properties mSerdeProp = new Properties();
      mSerdeProp.put(Constants.SERIALIZATION_FORMAT, "" + Utilities.tabCode);
      mSerdeProp.put(Constants.SERIALIZATION_NULL_FORMAT,
          ((indexWork) work).getSerializationNullFormat());
      mSerde.initialize(job, mSerdeProp);
      mSerde.setUseJSONSerialize(true);

      formatSerde = new FormatStorageSerDe();
      formatSerde.initialize(job, work.getProperties());

      new TextInputFormat();
      new IgnoreKeyTextOutputFormat();

    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
  }

  public int execute() {
    try {
      IndexQueryInfo indexQueryInfo = work.getIndexQueryInfo();

      List<IRecord> resultRecord = queryRecordByIndex(indexQueryInfo);
      if (resultRecord == null) {
        System.out.println("0 result return");
        return 0;
      }

      DataOutput outStream = work.getResFile().getFileSystem(conf)
          .create(work.getResFile());

      for (int i = 0; i < resultRecord.size(); i++) {
        IRecord record = resultRecord.get(i);

        try {
          outputResult(record, outStream);
        } catch (Exception e) {
          e.printStackTrace();
          System.out.println("output record " + i + ", fail:" + e.getMessage());
        }
      }
      ((FSDataOutputStream) outStream).close();

      return (0);
    } catch (Exception e) {
      console.printError("Failed with exception " + e.getMessage(), "\n"
          + StringUtils.stringifyException(e));
      return (1);
    }
  }

  private void outputResult(IRecord record, DataOutput out) throws Exception {
    String selectFieldString = work.getIndexQueryInfo().fieldList;
    if (selectFieldString == null || selectFieldString.length() == 0) {
      for (int i = 0; i < record.fieldnum(); i++) {
        if (i == 0) {
          out.writeBytes(record.getFieldStr(i));
        } else {
          out.write(separator);

          out.writeBytes(record.getFieldStr(i));
        }
      }
      out.write(terminator);
    }

    else {
      String[] seletcFields = selectFieldString.split(",");
      for (int i = 0; i < seletcFields.length; i++) {
        int idx = Integer.valueOf(seletcFields[i]);
        if (i == 0) {
          out.writeBytes(record.getFieldStr(idx));
        } else {
          out.write(separator);

          out.writeBytes(record.getFieldStr(idx));
        }
      }
      out.write(terminator);
    }

  }

  private List<IRecord> queryRecordByIndex(IndexQueryInfo indexQueryInfo) {
    String location = indexQueryInfo.location;
    String selectList = indexQueryInfo.fieldList;
    List<String> partList = indexQueryInfo.partList;
    List<IndexValue> values = indexQueryInfo.values;
    int limitNum = indexQueryInfo.limit;
    int fieldNum = indexQueryInfo.fieldNum;

    try {
      if (values.isEmpty()) {
        return null;
      }

      int size = values.size();
      ArrayList<IRecord.IFValue> fieldValues = new ArrayList<IRecord.IFValue>(
          size);
      for (int i = 0; i < values.size(); i++) {
        fieldValues.add(IndexValue2FieldValue(values.get(i)));
      }

      Indexer index = new Indexer();
      List<IRecord> result = new ArrayList<IRecord>();

      if (limitNum > MAX_OUTPUT_RECORD) {
        System.out.println("Result num:" + limitNum
            + ", execeed MAX OUTPUT RECORD:" + MAX_OUTPUT_RECORD
            + ". set limit less than 10000");
        fieldNum = MAX_OUTPUT_RECORD;
      }

      {

      }
      {
        result = index.get(location, partList, fieldValues, limitNum,
            selectList, fieldNum);
      }

      return result;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  private IRecord.IFValue IndexValue2FieldValue(IndexValue indexValue) {
    IRecord.IFValue fieldValue = null;
    String type = indexValue.type;

    if (type.equalsIgnoreCase(Constants.TINYINT_TYPE_NAME)) {
      fieldValue = new IRecord.IFValue(((Byte) indexValue.value).byteValue(),
          (short) -1);
    } else if (type.equalsIgnoreCase(Constants.SMALLINT_TYPE_NAME)) {
      fieldValue = new IRecord.IFValue(((Short) indexValue.value).shortValue(),
          (short) -1);
    } else if (type.equalsIgnoreCase(Constants.INT_TYPE_NAME)) {
      fieldValue = new IRecord.IFValue(((Integer) indexValue.value).intValue(),
          (short) -1);
    } else if (type.equalsIgnoreCase(Constants.BIGINT_TYPE_NAME)) {
      fieldValue = new IRecord.IFValue(((Long) indexValue.value).longValue(),
          (short) -1);
    } else if (type.equalsIgnoreCase(Constants.FLOAT_TYPE_NAME)) {
      fieldValue = new IRecord.IFValue(((Float) indexValue.value).floatValue(),
          (short) -1);
    } else if (type.equalsIgnoreCase(Constants.DOUBLE_TYPE_NAME)) {
      fieldValue = new IRecord.IFValue(
          ((Double) indexValue.value).doubleValue(), (short) -1);
    } else if (type.equalsIgnoreCase(Constants.STRING_TYPE_NAME)) {
      fieldValue = new IRecord.IFValue((String) indexValue.value, (short) -1);
    } else {
      return null;
    }

    return fieldValue;
  }

  @Override
  public List<? extends Node> getChildren() {
    return super.getChildTasks();
  }

  @Override
  public String getName() {
    return "INDEX";
  }
}
