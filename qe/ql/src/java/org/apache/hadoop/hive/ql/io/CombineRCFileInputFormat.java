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
package org.apache.hadoop.hive.ql.io;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import StorageEngineClient.CombineFileInputFormat;
import StorageEngineClient.CombineFileSplit;

public class CombineRCFileInputFormat<K extends LongWritable, V extends BytesRefArrayWritable>
    extends CombineFileInputFormat<K, V> {
  public static final Log LOG = LogFactory
      .getLog(CombineRCFileInputFormat.class);

  public CombineRCFileInputFormat() {
    super();
    LOG.info("new  CombineRCFileInputFormat");
  }

  @Override
  public RecordReader<K, V> getRecordReader(InputSplit split, JobConf job,
      Reporter reporter) throws IOException {
    LOG.info("enter CombineRCFileInputFormat RecordReader");
    return new CombineRCFileRecordReader<K, V>(job, (CombineFileSplit) split,
        reporter);
  }

}
