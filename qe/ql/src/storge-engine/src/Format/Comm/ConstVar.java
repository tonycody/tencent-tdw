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
package Comm;

public class ConstVar {
  public final static byte BitMask = 0x01;

  public final static int Sizeof_Boolean = 1;
  public final static int Sizeof_Byte = 1;
  public final static int Sizeof_Char = 2;
  public final static int Sizeof_Short = 2;
  public final static int Sizeof_Int = 4;
  public final static int Sizeof_Float = 4;
  public final static int Sizeof_Long = 8;
  public final static int Sizeof_Double = 8;

  public final static byte OP_GetSpecial = 0;
  public final static byte OP_GetAll = 1;

  public final static int MaxRecord = 3000000;
  public final static int MaxUnitLen = 200;
  public final static int MaxFieldNum = Short.MAX_VALUE;

  public final static byte FieldType_Unknown = -1;
  public final static byte FieldType_User = 0;
  public final static byte FieldType_Boolean = 1;
  public final static byte FieldType_Char = 2;
  public final static byte FieldType_Byte = 3;
  public final static byte FieldType_Short = 4;
  public final static byte FieldType_Int = 5;
  public final static byte FieldType_Long = 6;
  public final static byte FieldType_Float = 7;
  public final static byte FieldType_Double = 8;
  public final static byte FieldType_String = 9;

  public final static String fieldName_Unknown = "unknown";
  public final static String fieldName_User = "user";
  public final static String fieldName_Boolean = "boolean";
  public final static String fieldName_Char = "char";
  public final static String fieldName_Byte = "byte";
  public final static String fieldName_Short = "short";
  public final static String fieldName_Int = "int";
  public final static String fieldName_Long = "long";
  public final static String fieldName_Float = "float";
  public final static String fieldName_Double = "double";
  public final static String fieldName_String = "string";

  public final static short BaseHeadLen = 13;
  public final static short BaseHeadLen_01 = BaseHeadLen;
  public final static short VER_01 = 01;

  public final static int DataMagic = 0x272a;
  public final static int NaviMagic = 0x132b46c;
  public final static int NewFormatMagic = 0x132DC5B;

  public final static short Ver = 0x0001;
  public final static byte NotPrimaryKey = -1;
  public final static byte Compressed = 1;
  public final static byte LZOCompress = 0;
  public final static byte VarFlag = 1;
  public final static byte ENCODED = 1;

  public final static int LineMode = 0x01;
  public final static int KeyMode = 0x10;

  public final static int KeyIndexRecordLen = 8;
  public final static int LineIndexRecordLen = 28;

  public final static int IndexMetaOffset = 24;

  public final static int DataChunkMetaOffset = 16;

  public final static byte WS_Init = 0;
  public final static byte WS_Write = 1;
  public final static byte WS_Read = 2;

  public final static String HD_magic = "magic";
  public final static String HD_ver = "ver";
  public final static String HD_var = "var";
  public final static String HD_compress = "compress";
  public final static String HD_compressStyle = "compressStyle";
  public final static String HD_primaryIndex = "primaryIndex";
  public final static String HD_encode = "encode";
  public final static String HD_encodeStyle = "encodeStyle";
  public final static String HD_key = "key";
  public final static String HD_fieldMap = "fieldMap";
  public final static String HD_lineindex = "fieldMap";
  public final static String HD_fileformattype = "fieldMap";
  public final static String HD_udi = "fieldMap";

  public final static String RecordSplit = ";";
  public final static String FieldSplit = ",";

  public final static int DefaultUnitSize = 2 * 1024 * 1024;
  public final static int DefaultSegmentSize = 64 * 1024 * 1024;
  public final static int DefaultPoolSize = 1;

  public final static String FormatStorageConf = "formatstorage_conf.xml";
  public final static String ConfUnitSize = "unit.size";
  public final static String ConfSegmentSize = "segment.size";
  public final static String ConfPoolSize = "pool.size";

  public final static String InputPath = "mapred.input.dir";

  public final static String Navigator = "/Navigator";
  public final static String Projection = "projection";
  public final static String TableType = "type";
  public final static String Compress = "compress";
  public final static String CompressionConfName = "ifdf.head.compression";
  public final static String CompressionMethodConfName = "ifdf.head.compressionMethod";
  public final static String ZlibCompressionLevelConfName = "ifdf.head.zlibCompressionLevel";

  public final static String READ_COLUMN_IDS_CONF_STR = "hive.io.file.readcolumn.ids";
  public final static String NeedPostfix = "NeedPostfix";
  public static final String HD_index_filemap = "indexFileMap";

  public static final int OldFormatFile = 0;
  public static final int NewFormatFile = 1;

}
