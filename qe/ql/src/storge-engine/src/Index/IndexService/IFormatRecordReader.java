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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.RecordReader;

import Comm.ConstVar;
import FormatStorage1.IFormatDataFile;
import FormatStorage1.IRecord;

public class IFormatRecordReader<K, V> implements
    RecordReader<IndexKey, IndexValue> {
  public static final Log LOG = LogFactory.getLog(IFormatRecordReader.class);

  Configuration conf;

  int currentline = 0;

  int beginLine = 0;
  int endLine = 0;

  IFormatDataFile ifdf = null;
  IRecord record;
  ArrayList<Integer> indexkeys;
  int fileindex;

  public IFormatRecordReader(Configuration conf, IFormatInputSplit split)
      throws IOException {

    this.conf = conf;

    String[] indexfilemap = conf.getStrings(ConstVar.HD_index_filemap);
    String splitfilename = split.getPath().toString();
    for (int i = 0; i < indexfilemap.length; i++) {
      if (splitfilename.endsWith(indexfilemap[i])) {
        this.fileindex = i;
        break;
      }
    }

    indexkeys = new ArrayList<Integer>();
    String idss = conf.get("index.ids");

    String[] strs = idss.split(",");
    for (int i = 0; i < strs.length; i++) {
      indexkeys.add(Integer.parseInt(strs[i]));
    }

    ifdf = new IFormatDataFile(conf);
    ifdf.open(split.getPath().toString());

    if (split.wholefileASasplit) {
      this.beginLine = 0;
      this.endLine = ifdf.segIndex().recnum() - 1;
    } else {
      beginLine = split.beginline;
      endLine = split.beginline + split.recnum - 1;
    }

    currentline = beginLine;
    ifdf.seek(currentline);
    record = ifdf.getIRecordObj();
  }

  @Override
  public boolean next(IndexKey key, IndexValue value) throws IOException {
    if (currentline > endLine) {
      return false;
    }

    label: while (true) {
      key.reset();

      if (!ifdf.next(record))
        return false;
      for (int idx : indexkeys) {
        IRecord.IFValue fv = record.getByIdx(idx);
        if (fv == null) {
          currentline++;
          continue label;
        }
        key.addfv(fv);
      }

      value.setFileindex(this.fileindex);
      value.setRowid(currentline);

      currentline++;
      return true;
    }
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
    return currentline;
  }

  @Override
  public float getProgress() throws IOException {
    return (float) (currentline - this.beginLine)
        / (float) (this.endLine - this.beginLine + 1);
  }
}
