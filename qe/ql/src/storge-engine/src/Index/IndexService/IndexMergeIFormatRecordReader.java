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
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.RecordReader;

import Comm.ConstVar;
import FormatStorage1.IFormatDataFile;
import FormatStorage1.IRecord;

public class IndexMergeIFormatRecordReader<K, V> implements
    RecordReader<IndexKey, IndexValue> {
  public static final Log LOG = LogFactory
      .getLog(IndexMergeIFormatRecordReader.class);

  Configuration conf;
  IRecord record;

  private IFormatDataFile ifdf;
  int recnum = 0;
  int currentrec = 0;

  private HashMap<Integer, String> splitinfos;
  private HashMap<String, Integer> filesmap;

  public IndexMergeIFormatRecordReader(Configuration conf,
      IndexMergeIFormatSplit split) throws IOException {
    this.conf = conf;

    this.filesmap = new HashMap<String, Integer>();
    String[] strs = conf.getStrings(ConstVar.HD_index_filemap);

    for (int i = 0; i < strs.length; i++) {
      filesmap.put(strs[i], i);
    }

    ifdf = new IFormatDataFile(conf);
    ifdf.open(split.getPath().toString());
    this.splitinfos = ifdf.fileInfo().head().getUdi().infos();

    if (split.wholefileASasplit) {
      this.recnum = ifdf.segIndex().recnum();
    } else {
      this.recnum = split.recnum;
    }

    ifdf.seek(split.beginline);
    record = ifdf.getIRecordObj();

  }

  @Override
  public boolean next(IndexKey key, IndexValue value) throws IOException {
    if (currentrec >= recnum)
      return false;
    ifdf.next(record);
    currentrec++;
    key.reset();
    for (int i = 0; i < record.fieldnum() - 2; i++) {
      key.addfv(record.getByIdx(i));
    }

    int fileindex = (Short) record.getByIdx(record.fieldnum() - 2).data();
    int line = (Integer) record.getByIdx(record.fieldnum() - 1).data();

    value.setFileindex(filesmap.get(splitinfos.get(fileindex)));
    value.setRowid(line);

    return true;
  }

  @Override
  public void close() throws IOException {
    if (ifdf != null) {
      try {
        ifdf.close();
      } catch (Exception e) {
        LOG.error("close fail:" + e.getMessage());
      }
    }
  }

  @Override
  public IndexKey createKey() {
    return new IndexKey();
  }

  @Override
  public IndexValue createValue() {
    return new IndexValue();
  }

  @Override
  public long getPos() throws IOException {
    return currentrec;
  }

  @Override
  public float getProgress() throws IOException {
    return (float) currentrec / recnum;
  }
}
