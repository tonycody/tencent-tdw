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

import org.apache.hadoop.io.compress.zlib.*;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;

import Comm.ConstVar;
import FormatStorage1.IFieldMap;
import FormatStorage1.IFormatDataFile;
import FormatStorage1.IHead;
import FormatStorage1.IRecord;

@SuppressWarnings({ "deprecation", "unchecked" })
public class FormatStorageHiveOutputFormat<K, V> implements
    HiveOutputFormat<WritableComparable, Writable> {
  public static final Log LOG = LogFactory
      .getLog(FormatStorageHiveOutputFormat.class);

  @Override
  public RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
      Class<? extends Writable> valueClass, boolean isCompressed,
      Properties tbl, Progressable progress) throws IOException {

    boolean usenewformat = jc.getBoolean("fdf.newformat", false);
    IHead head = new IHead(usenewformat ? ConstVar.NewFormatFile
        : ConstVar.OldFormatFile);
    boolean flag = true;
    if (flag) {
      String columnTypeProperty = tbl.getProperty(Constants.LIST_COLUMN_TYPES);
      ArrayList<TypeInfo> types;
      if (columnTypeProperty == null)
        types = new ArrayList<TypeInfo>();
      else
        types = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
      String compress = tbl.getProperty(ConstVar.Compress);
      String ifdfCompressionOn = jc.get(ConstVar.CompressionConfName);
      if ((ifdfCompressionOn != null && ifdfCompressionOn
          .equalsIgnoreCase("true"))
          || (compress != null && compress.equalsIgnoreCase("true"))) {
        head.setCompress((byte) 1);
        String strCompressionMethod = jc
            .get(ConstVar.CompressionMethodConfName);
        if (strCompressionMethod != null) {
          try {
            byte compressionMethod = Byte.valueOf(strCompressionMethod)
                .byteValue();
            head.setCompressStyle(compressionMethod);
          } catch (NumberFormatException e) {
          }
        }

        String compressionLevel = jc.get(ConstVar.ZlibCompressionLevelConfName);
        if (compressionLevel != null) {
          if (compressionLevel.equalsIgnoreCase("bestSpeed")) {
            jc.set("zlib.compress.level",
                ZlibCompressor.CompressionLevel.BEST_SPEED.toString());
          } else if (compressionLevel.equalsIgnoreCase("bestCompression")) {
            jc.set("zlib.compress.level",
                ZlibCompressor.CompressionLevel.BEST_COMPRESSION.toString());
          }
        }
      }

      IFieldMap map = new IFieldMap();
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
        else if (name.equals(Constants.TIMESTAMP_TYPE_NAME))
          fdftype = ConstVar.FieldType_String;
        map.addFieldType(new IRecord.IFType(fdftype, i++));
      }
      head.setFieldMap(map);
    }

    final IFormatDataFile ifdf = new IFormatDataFile(jc);
    ifdf.create(finalOutPath.toString(), head);

    return new RecordWriter() {

      @Override
      public void write(Writable w) throws IOException {
        ifdf.addRecord((IRecord) w);
      }

      @Override
      public void close(boolean abort) throws IOException {
        ifdf.close();
      }
    };
  }

}
