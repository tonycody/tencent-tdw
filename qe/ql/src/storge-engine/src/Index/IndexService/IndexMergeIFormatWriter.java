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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

import Comm.ConstVar;
import FormatStorage1.IFieldMap;
import FormatStorage1.IFormatDataFile;
import FormatStorage1.IHead;
import FormatStorage1.IRecord;
import FormatStorage1.IUserDefinedHeadInfo;

@SuppressWarnings("deprecation")
public class IndexMergeIFormatWriter<K, V> implements java.io.Closeable,
    RecordWriter<IndexKey, IndexValue> {
  public static final Log LOG = LogFactory
      .getLog(IndexMergeIFormatWriter.class);

  JobConf conf;
  Progressable progress;
  IFormatDataFile ifdf = null;
  IHead ihead;
  IRecord record;

  public IndexMergeIFormatWriter(String fileName, JobConf job)
      throws IOException {
    this.conf = job;
    ifdf = new IFormatDataFile(job);
    ihead = new IHead();
    String[] fieldStrings = job.getStrings(ConstVar.HD_fieldMap);
    IFieldMap fieldMap = new IFieldMap();
    for (int i = 0; i < fieldStrings.length; i++) {
      String[] def = fieldStrings[i].split(ConstVar.RecordSplit);
      byte type = Byte.valueOf(def[0]);
      int index = Short.valueOf(def[1]);
      fieldMap.addFieldType(new IRecord.IFType(type, index));
    }
    ihead.setFieldMap(fieldMap);

    String[] files = job.getStrings(ConstVar.HD_index_filemap);
    IUserDefinedHeadInfo iudhi = new IUserDefinedHeadInfo();
    iudhi.addInfo(123456, job.get("datafiletype"));
    for (int i = 0; i < files.length; i++) {
      iudhi.addInfo(i, files[i]);
    }

    ihead.setUdi(iudhi);
    ihead.setPrimaryIndex(0);
    ifdf.create(fileName, ihead);
    record = ifdf.getIRecordObj();
  }

  @Override
  public void write(IndexKey key, IndexValue value) throws IOException {
    record.clear();
    ArrayList<IRecord.IFValue> fvs = key.getfvs();
    for (int i = 0; i < fvs.size(); i++) {
      record.addFieldValue(fvs.get(i));
    }
    IRecord.IFValue fv = new IRecord.IFValue((short) value.getFileindex(),
        fvs.size());
    record.addFieldValue(fv);
    fv = new IRecord.IFValue(value.getRowid(), fvs.size() + 1);
    record.addFieldValue(fv);
    int res = ifdf.addRecord(record);
    if (res != 0) {
      throw new IOException("add record fail:\t" + res);
    }
  }

  @Override
  public void close() throws IOException {
    ifdf.close();
  }

  @Override
  public void close(Reporter reporter) throws IOException {
    this.close();
  }
}
