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
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

import Comm.ConstVar;
import FormatStorage1.IColumnDataFile;
import FormatStorage1.IFieldMap;
import FormatStorage1.IFormatDataFile;
import FormatStorage1.IHead;
import FormatStorage1.IRecord;

@SuppressWarnings({ "unchecked", "deprecation" })
public class ColumnStorageHiveOutputFormat<K, V> implements
    HiveOutputFormat<WritableComparable, Writable> {
  public static final Log LOG = LogFactory
      .getLog(ColumnStorageHiveOutputFormat.class);

  @Override
  public RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
      Class<? extends Writable> valueClass, boolean isCompressed,
      Properties tbl, Progressable progress) throws IOException {

    boolean usenewformat = jc.getBoolean("fdf.newformat", false);
    IHead head = new IHead(usenewformat ? ConstVar.NewFormatFile
        : ConstVar.OldFormatFile);
    String columnTypeProperty = tbl.getProperty(Constants.LIST_COLUMN_TYPES);
    IFieldMap map = new IFieldMap();
    ArrayList<TypeInfo> types;
    if (columnTypeProperty == null) {
      types = new ArrayList<TypeInfo>();
      map.addFieldType(new IRecord.IFType(ConstVar.FieldType_Int, 0));
    } else
      types = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    String compress = tbl.getProperty(ConstVar.Compress);
    if (compress != null && compress.equalsIgnoreCase("true"))
      head.setCompress((byte) 1);
    int i = 0;
    for (TypeInfo type : types) {
      byte fdftype = 0;
      String name = type.getTypeName();
      if (name.equals(Constants.TINYINT_TYPE_NAME))
        fdftype = ConstVar.FieldType_Byte;
      else if (name.equals(Constants.SMALLINT_TYPE_NAME))
        fdftype = ConstVar.FieldType_Short;
      else if (name.equals(Constants.INT_TYPE_NAME))
        fdftype = ConstVar.FieldType_Int;
      else if (name.equals(Constants.BIGINT_TYPE_NAME))
        fdftype = ConstVar.FieldType_Long;
      else if (name.equals(Constants.FLOAT_TYPE_NAME))
        fdftype = ConstVar.FieldType_Float;
      else if (name.equals(Constants.DOUBLE_TYPE_NAME))
        fdftype = ConstVar.FieldType_Double;
      else if (name.equals(Constants.STRING_TYPE_NAME))
        fdftype = ConstVar.FieldType_String;

      map.addFieldType(new IRecord.IFType(fdftype, i++));
    }
    head.setFieldMap(map);

    ArrayList<ArrayList<Integer>> columnprojects = null;
    String projectionString = jc.get(ConstVar.Projection);
    if (projectionString != null) {
      columnprojects = new ArrayList<ArrayList<Integer>>();
      String[] projectionList = projectionString.split(ConstVar.RecordSplit);
      for (String str : projectionList) {
        ArrayList<Integer> cp = new ArrayList<Integer>();
        String[] item = str.split(ConstVar.FieldSplit);
        for (String s : item) {
          cp.add(Integer.valueOf(s));
        }
        columnprojects.add(cp);
      }
    }

    if (!jc.getBoolean(ConstVar.NeedPostfix, true)) {
      final Configuration conf = new Configuration(jc);
      final IFormatDataFile ifdf = new IFormatDataFile(conf);
      ifdf.create(finalOutPath.toString(), head);
      return new RecordWriter() {

        @Override
        public void write(Writable w) throws IOException {
        }

        @Override
        public void close(boolean abort) throws IOException {
          ifdf.close();
        }

      };
    }

    final IColumnDataFile icdf = new IColumnDataFile(jc);
    icdf.create(finalOutPath.toString(), head, columnprojects);

    LOG.info(finalOutPath.toString());
    LOG.info("output file compress?\t" + compress);
    LOG.info("head:\t" + head.toStr());

    return new RecordWriter() {

      @Override
      public void write(Writable w) throws IOException {
        icdf.addRecord((IRecord) w);
      }

      @Override
      public void close(boolean abort) throws IOException {
        icdf.close();
      }

    };
  }

}
