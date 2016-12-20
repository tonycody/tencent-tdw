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
package StorageEngineClient;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import FormatStorage1.IRecord;

@SuppressWarnings("deprecation")
public class FormatStorageOutputFormat<K, V> extends
    FileOutputFormat<LongWritable, IRecord> {
  public static final Log LOG = LogFactory
      .getLog(FormatStorageOutputFormat.class);

  @Override
  public RecordWriter<LongWritable, IRecord> getRecordWriter(
      FileSystem ignored, JobConf job, String name, Progressable progress)
      throws IOException {

    return null;
  }
}
