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
package IndexStorage;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import IndexService.IndexMR;

public class TestIFDF {
  static Configuration conf = new Configuration();
  static FileSystem fs;
  static {
    try {
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void testIndexMR() throws Exception {
    String datadir = "datadir";
    String indexdir = "indexdir";
    int filenum = 5;
    int recnum = 1000;

    TestUtil.genfdfseq(datadir, filenum, recnum, true, true);
    FileStatus[] ss = fs.listStatus(new Path(datadir));
    StringBuffer sb = new StringBuffer();
    for (FileStatus fileStatus : ss) {
      sb.append(fileStatus.getPath().toString()).append(",");
    }
    IndexMR.run(conf, sb.substring(0, sb.length() - 1), false, "2", indexdir);
    IFormatDataFile ifdf = new IFormatDataFile(conf);
    ifdf.open(indexdir + "/part-00000");
    for (int i = 0; i < 100; i++) {
      ifdf.next().show();
    }

  }

  public static void test() throws IOException {
    String file = "indexdir/part-00000";
    IHead head = new IHead();
    IFieldMap map = new IFieldMap();
    map.addFieldType(new IFieldType.IFieldByteType());
    map.addFieldType(new IFieldType.IFieldShortType());
    map.addFieldType(new IFieldType.IFieldIntType());
    head.setFieldMap(map);
    head.setPrimaryIndex((short) 2);
    IFormatDataFile ifdf = new IFormatDataFile(conf);
    ifdf.create(file, head);

    for (int i = 0; i < 100000000; i++) {
      IRecord rec = new IRecord();
      rec.addFieldValue(new IFieldValue((byte) i));
      rec.addFieldValue(new IFieldValue((short) i));
      rec.addFieldValue(new IFieldValue((int) i));
      ifdf.addRecord(rec);
      if (i % 1000000 == 0)
        System.out.println(i);
    }
    ifdf.close();

    ifdf = new IFormatDataFile(conf);
    ifdf.open(file);
    for (int i = 0; i < 20; i++) {
      ifdf.next().show();
    }

  }

  public static void teststringkey(String file) throws IOException {
    Configuration conf = new Configuration();
    IHead head = new IHead();
    IFieldMap map = new IFieldMap();
    map.addFieldType(new IFieldType.IFieldStringType());
    map.addFieldType(new IFieldType.IFieldShortType());
    map.addFieldType(new IFieldType.IFieldIntType());
    head.setFieldMap(map);
    head.setPrimaryIndex((short) 0);
    FileSystem.get(conf).delete(new Path(file), true);
    IFormatDataFile ifdf = new IFormatDataFile(conf);
    ifdf.create(file, head);

    String sss = "aaaaaa";
    for (int i = 0; i < 100000; i++) {
      IRecord rec = new IRecord();

      sss = increase(sss);

      rec.addFieldValue(new IFieldValue(sss.toString()));
      rec.addFieldValue(new IFieldValue((short) i));
      rec.addFieldValue(new IFieldValue((int) i));
      ifdf.addRecord(rec);
      if ((i + 1) % 1000000 == 0)
        System.out.println(i);
    }
    ifdf.close();

  }

  public static void readifdf(String file, String str) throws IOException {
    IFormatDataFile ifdf = new IFormatDataFile(conf);
    ifdf.open(file);
    ifdf.seek(new IFieldValue(str));
    ifdf.next().show();
    ifdf.close();

  }

  private static String increase(String sss) {

    int l = sss.length() - 1;
    char ch = 'a';
    while (l >= 0) {
      ch = sss.charAt(l);
      if (ch != 'z') {
        break;
      }
      l--;
    }

    if (l < 0) {
      StringBuffer sb = new StringBuffer();
      for (int i = 0; i < sss.length() + 1; i++) {
        sb.append("a");
      }
      return sb.toString();
    }

    StringBuffer sb = new StringBuffer();
    sb.append(sss.substring(0, l));
    sb.append((char) (ch + 1));
    for (int i = 0; i < sss.length() - l - 1; i++) {
      sb.append('a');
    }

    return sb.toString();

  }

  public static void testwriteifdf() throws IOException {
    String datadir = "/se/ifdf/";
    int filenum = 1;
    int recnum = 1000000;
    short index = 2;
    boolean var = true;
    boolean compress = true;
    boolean seq = true;
    boolean overwrite = true;

    UtilIndexStorage.writeIFDF(datadir, filenum, recnum, index, var, compress,
        seq, overwrite);
    compress = false;
    overwrite = false;
    UtilIndexStorage.writeIFDF(datadir, filenum, recnum, index, var, compress,
        seq, overwrite);
  }

  public static void testreadifdf(String filename) throws IOException {
    IFormatDataFile ifdf = new IFormatDataFile(conf);
    ifdf.open(filename);
    ifdf.seek(1000);
    IRecord rec = ifdf.next();
    rec.show();
    ifdf.close();

  }

  public static void main(String[] args) throws IOException {
    String file = "teststring";
    teststringkey(file);
  }
}
