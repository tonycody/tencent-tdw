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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import Comm.ConstVar;
import Comm.Util;
import FormatStorage.FieldMap;
import FormatStorage.FormatDataFile;
import FormatStorage.Head;
import FormatStorage.FieldMap.Field;
import FormatStorage.Unit.FieldValue;
import FormatStorage.Unit.Record;

public class UtilIndexStorage {

  static Configuration conf = new Configuration();
  static FileSystem fs;
  static Random r = new Random();
  static {
    try {
      fs = FileSystem.get(conf);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void setconf(Configuration conf1) {
    conf = conf1;
    try {
      fs = FileSystem.get(conf1);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void writeFDF(String datadir, int filenum, int recnum,
      short index, boolean var, boolean compress, boolean seq, boolean overwrite)
      throws Exception {
    Head head = new Head();
    FieldMap fieldMap = new FieldMap();

    fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte,
        (short) 0));
    fieldMap.addField(new Field(ConstVar.FieldType_Short,
        ConstVar.Sizeof_Short, (short) 1));
    fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int,
        (short) 2));
    fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long,
        (short) 3));
    fieldMap.addField(new Field(ConstVar.FieldType_Float,
        ConstVar.Sizeof_Float, (short) 4));
    fieldMap.addField(new Field(ConstVar.FieldType_Double,
        ConstVar.Sizeof_Double, (short) 5));
    if (var) {
      fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short) 6));
    }
    head.setFieldMap(fieldMap);

    if (index > -1) {
      head.setPrimaryIndex(index);
    }

    if (compress) {
      head.setCompress((byte) 1);
    }

    datadir = datadir.endsWith("/") ? datadir : (datadir + "/");
    if (overwrite && fs.exists(new Path(datadir))) {
      fs.delete(new Path(datadir), true);
      fs.mkdirs(new Path(datadir));
    } else if (overwrite || !fs.exists(new Path(datadir))) {
      fs.mkdirs(new Path(datadir));
    }

    FileStatus[] fss = fs.listStatus(new Path(datadir));
    int x = 0;
    if (fss != null) {
      x = fss.length;
    }

    String tmpdir = "/se/tmp/";
    if (!fs.exists(new Path(tmpdir)))
      fs.mkdirs(new Path(tmpdir));
    for (int i = 0; i < filenum; i++) {
      FormatDataFile fdf = new FormatDataFile(conf);
      String tmpfile = tmpdir + "fdf" + (x + i);
      String datafile = datadir + "fdf" + (x + i);
      fdf.create(tmpfile, head);
      int fieldnum = var ? 7 : 6;
      String sss = "aaaaaa";
      for (int j = 0; j < recnum; j++) {
        Record record = new Record(fieldnum);
        if (seq) {
          record.addValue(new FieldValue((byte) (j), (short) 0));
          record.addValue(new FieldValue((short) (j), (short) 1));
          record.addValue(new FieldValue((int) (j), (short) 2));
          record.addValue(new FieldValue((long) (j), (short) 3));
          record.addValue(new FieldValue((float) (j), (short) 4));
          record.addValue(new FieldValue((double) (j), (short) 5));
          if (var) {
            sss = increase(sss);
            record.addValue(new FieldValue(sss, (short) 6));
          }
        } else {
          record.addValue(new FieldValue((byte) r.nextInt(), (short) 0));
          record.addValue(new FieldValue((short) r.nextInt(), (short) 1));
          record.addValue(new FieldValue(r.nextInt(), (short) 2));
          record.addValue(new FieldValue(r.nextLong(), (short) 3));
          record.addValue(new FieldValue(r.nextFloat(), (short) 4));
          record.addValue(new FieldValue(r.nextDouble(), (short) 5));
          if (var) {
            record.addValue(new FieldValue(getrandomstr(), (short) 6));
          }
        }
        fdf.addRecord(record);
        if ((j + 1) % 1000000 == 0) {
          System.out.println((j + 1) + "\trecords written");
        }
      }
      fdf.close();
      fs.rename(new Path(tmpfile), new Path(datafile));
    }

  }

  public static void writeIFDF(String datadir, int filenum, int recnum,
      short index, boolean var, boolean compress, boolean seq, boolean overwrite)
      throws IOException {
    IHead head = new IHead();
    IFieldMap fieldMap = new IFieldMap();
    fieldMap.addFieldType(new IFieldType.IFieldByteType());
    fieldMap.addFieldType(new IFieldType.IFieldShortType());
    fieldMap.addFieldType(new IFieldType.IFieldIntType());
    fieldMap.addFieldType(new IFieldType.IFieldLongType());
    fieldMap.addFieldType(new IFieldType.IFieldFloatType());
    fieldMap.addFieldType(new IFieldType.IFieldDoubleType());
    if (var) {
      fieldMap.addFieldType(new IFieldType.IFieldStringType());
    }
    head.setFieldMap(fieldMap);

    if (compress) {
      head.setCompress((byte) 1);
      head.setCompressStyle((byte) 2);
    }

    if (index > -1) {
      head.setPrimaryIndex(index);
    }

    datadir = datadir.endsWith("/") ? datadir : (datadir + "/");
    Path datapath = new Path(datadir);

    if (overwrite) {
      if (fs.exists(datapath)) {
        fs.delete(datapath, true);
      }
    }
    if (!fs.exists(datapath))
      fs.mkdirs(datapath);

    int x = fs.listStatus(datapath).length;
    conf.set("commpresscodec", "lzocodec");
    for (int i = 0; i < filenum; i++) {
      IFormatDataFile ifdf = new IFormatDataFile(conf);
      ifdf.create(datadir + "ifdf" + (x + i), head);
      String sss = "aaaaaa";
      for (int j = 0; j < recnum; j++) {
        IRecord record = new IRecord();
        if (seq) {
          record.addFieldValue(new IFieldValue((byte) (j)));
          record.addFieldValue(new IFieldValue((short) (j)));
          record.addFieldValue(new IFieldValue(j));
          record.addFieldValue(new IFieldValue((long) (j)));
          record.addFieldValue(new IFieldValue((float) (j)));
          record.addFieldValue(new IFieldValue((double) (j)));
          if (var) {
            record.addFieldValue(new IFieldValue(increase(sss)));
          }
        } else {
          record.addFieldValue(new IFieldValue((byte) (r.nextInt())));
          record.addFieldValue(new IFieldValue((short) (r.nextInt())));
          record.addFieldValue(new IFieldValue(r.nextInt()));
          record.addFieldValue(new IFieldValue(r.nextLong()));
          record.addFieldValue(new IFieldValue(r.nextFloat()));
          record.addFieldValue(new IFieldValue(r.nextDouble()));
          if (var) {
            record.addFieldValue(new IFieldValue(getrandomstr()));
          }
        }
        ifdf.addRecord(record);
        if ((j + 1) % 1000000 == 0) {
          System.out.println((j + 1) + "\trecords written");
        }
      }
      ifdf.close();

    }

  }

  public static void writeColumnFDF(String datadir, int filenum, int recnum,
      short index, boolean var, boolean compress, boolean seq, boolean overwrite)
      throws Exception {
    Head head0 = new Head();
    FieldMap fieldMap0 = new FieldMap();
    fieldMap0.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte,
        (short) 0));
    head0.setFieldMap(fieldMap0);

    Head head1 = new Head();
    FieldMap fieldMap1 = new FieldMap();
    fieldMap1.addField(new Field(ConstVar.FieldType_Short,
        ConstVar.Sizeof_Short, (short) 1));
    head1.setFieldMap(fieldMap1);

    Head head2 = new Head();
    FieldMap fieldMap2 = new FieldMap();
    fieldMap2.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int,
        (short) 2));
    head2.setFieldMap(fieldMap2);

    Head head3 = new Head();
    FieldMap fieldMap3 = new FieldMap();
    fieldMap3.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long,
        (short) 3));
    head3.setFieldMap(fieldMap3);

    Head head4 = new Head();
    FieldMap fieldMap4 = new FieldMap();
    fieldMap4.addField(new Field(ConstVar.FieldType_Float,
        ConstVar.Sizeof_Float, (short) 4));
    head4.setFieldMap(fieldMap4);

    Head head5 = new Head();
    FieldMap fieldMap5 = new FieldMap();
    fieldMap5.addField(new Field(ConstVar.FieldType_Double,
        ConstVar.Sizeof_Double, (short) 5));
    head5.setFieldMap(fieldMap5);

    Head head6 = new Head();
    FieldMap fieldMap6 = new FieldMap();
    if (var) {
      fieldMap6.addField(new Field(ConstVar.FieldType_String, 0, (short) 6));
      head6.setFieldMap(fieldMap6);
    }

    if (index == 0) {
      head0.setPrimaryIndex(index);
    }
    if (index == 1) {
      head0.setPrimaryIndex(index);
    }
    if (index == 2) {
      head0.setPrimaryIndex(index);
    }
    if (index == 3) {
      head0.setPrimaryIndex(index);
    }
    if (index == 4) {
      head0.setPrimaryIndex(index);
    }
    if (index == 5) {
      head0.setPrimaryIndex(index);
    }
    if (index == 6) {
      head0.setPrimaryIndex(index);
    }

    if (compress) {
      head0.setCompress((byte) 1);
      head1.setCompress((byte) 1);
      head2.setCompress((byte) 1);
      head3.setCompress((byte) 1);
      head4.setCompress((byte) 1);
      head5.setCompress((byte) 1);
      head6.setCompress((byte) 1);
    }

    datadir = datadir.endsWith("/") ? datadir : (datadir + "/");
    if (overwrite && fs.exists(new Path(datadir))) {
      fs.delete(new Path(datadir), true);
      fs.mkdirs(new Path(datadir));
    } else if (overwrite) {
      fs.mkdirs(new Path(datadir));
    }
    int x = fs.listStatus(new Path(datadir)).length;

    for (int i = 0; i < filenum; i++) {
      FormatDataFile fdf0 = new FormatDataFile(conf);
      FormatDataFile fdf1 = new FormatDataFile(conf);
      FormatDataFile fdf2 = new FormatDataFile(conf);
      FormatDataFile fdf3 = new FormatDataFile(conf);
      FormatDataFile fdf4 = new FormatDataFile(conf);
      FormatDataFile fdf5 = new FormatDataFile(conf);
      FormatDataFile fdf6 = new FormatDataFile(conf);

      fdf0.create(datadir + "datafile" + (x + i) + "-idx0", head0);
      fdf1.create(datadir + "datafile" + (x + i) + "-idx1", head1);
      fdf2.create(datadir + "datafile" + (x + i) + "-idx2", head2);
      fdf3.create(datadir + "datafile" + (x + i) + "-idx3", head3);
      fdf4.create(datadir + "datafile" + (x + i) + "-idx4", head4);
      fdf5.create(datadir + "datafile" + (x + i) + "-idx5", head5);
      if (var) {
        fdf6.create(datadir + "datafile" + (x + i) + "-idx6", head6);
      }
      String sss = "aaaaaa";
      for (int j = 0; j < recnum; j++) {
        if (seq) {
          Record record = new Record(1);
          record.addValue(new FieldValue((byte) (j), (short) 0));
          fdf0.addRecord(record);

          record = new Record(1);
          record.addValue(new FieldValue((short) (j), (short) 1));
          fdf1.addRecord(record);

          record = new Record(1);
          record.addValue(new FieldValue((int) (j), (short) 2));
          fdf2.addRecord(record);

          record = new Record(1);
          record.addValue(new FieldValue((long) (j), (short) 3));
          fdf3.addRecord(record);

          record = new Record(1);
          record.addValue(new FieldValue((float) (j), (short) 4));
          fdf4.addRecord(record);

          record = new Record(1);
          record.addValue(new FieldValue((double) (j), (short) 5));
          fdf5.addRecord(record);

          if (var) {
            record = new Record(1);
            sss = increase(sss);
            record.addValue(new FieldValue(sss, (short) 6));
            fdf6.addRecord(record);
          }
        } else {
          Record record = new Record(1);
          record.addValue(new FieldValue((byte) r.nextInt(), (short) 0));
          fdf0.addRecord(record);

          record = new Record(1);
          record.addValue(new FieldValue((short) r.nextInt(), (short) 1));
          fdf1.addRecord(record);

          record = new Record(1);
          record.addValue(new FieldValue(r.nextInt(), (short) 2));
          fdf2.addRecord(record);

          record = new Record(1);
          record.addValue(new FieldValue(r.nextLong(), (short) 3));
          fdf3.addRecord(record);

          record = new Record(1);
          record.addValue(new FieldValue(r.nextFloat(), (short) 4));
          fdf4.addRecord(record);

          record = new Record(1);
          record.addValue(new FieldValue(r.nextDouble(), (short) 5));
          fdf5.addRecord(record);

          if (var) {
            record = new Record(1);
            record.addValue(new FieldValue(getrandomstr(), (short) 6));
            fdf6.addRecord(record);
          }
        }
        if ((j + 1) % 1000000 == 0) {
          System.out.println((j + 1) + "\trecords written");
        }

      }
      fdf0.close();
      fdf1.close();
      fdf2.close();
      fdf3.close();
      fdf4.close();
      fdf5.close();
      if (var) {
        fdf6.close();
      }
    }

  }

  private static String getrandomstr() {
    StringBuffer sb = new StringBuffer();
    int n = r.nextInt(6);
    for (int i = 0; i < n + 6; i++) {
      sb.append((char) (r.nextInt(26) + 'a'));
    }
    return sb.toString();
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

  public static void readFDFshell(String fileName) {
    FormatDataFile fdf;
    try {
      fdf = new FormatDataFile(conf);
      fdf.open(fileName);
    } catch (Exception e2) {
      e2.printStackTrace();
      System.out.println(fileName + ": not exists...");
      return;
    }

    Record record = new Record();
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    while (true) {
      try {
        System.out.println("input param: [seekrowid] [num]");
        String str = br.readLine();
        if (str.equals("q")) {
          break;
        }
        StringTokenizer st = new StringTokenizer(str);
        int line = 0;
        if (st.hasMoreTokens()) {
          line = Integer.parseInt(st.nextToken());
        } else {
          line = -1;
        }
        int num;
        if (st.hasMoreTokens()) {
          num = Integer.parseInt(st.nextToken());
        } else {
          num = 10;
        }

        long time = System.currentTimeMillis();
        if (line > -1) {
          for (int i = 0; i < num; i++) {
            record = fdf.getRecordByLine(line);
            show(record);
            line++;
          }
          System.out
              .println("gettime:\t" + (System.currentTimeMillis() - time));
        }

      } catch (Exception e) {
        e.printStackTrace();
        try {
          Thread.sleep(500);
        } catch (InterruptedException e1) {
          e1.printStackTrace();
        }
        System.out.println("input again ^_^");
      }
    }

  }

  public static void readIFDFshell(String fileName) {
    IFormatDataFile fdf;
    try {
      fdf = new IFormatDataFile(conf);
      fdf.open(fileName);
    } catch (IOException e2) {
      e2.printStackTrace();
      System.out.println(fileName + ": not exists...");
      return;
    }

    IRecord record = new IRecord();
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    while (true) {
      try {
        System.out.println();
        System.out.println("input param: [seekrowid] [num]");
        String str = br.readLine();
        if (str.equals("q")) {
          break;
        }
        StringTokenizer st = new StringTokenizer(str);
        int line;
        if (st.hasMoreTokens()) {
          line = Integer.parseInt(st.nextToken());
        } else {
          line = -1;
        }
        int num;
        if (st.hasMoreTokens()) {
          num = Integer.parseInt(st.nextToken());

        } else {
          num = 10;
        }

        boolean seek = false;
        long time = System.currentTimeMillis();
        if (line > -1) {
          seek = fdf.seek(line);
        }
        System.out.println("seektime:\t" + (System.currentTimeMillis() - time));
        System.out.println("seek:\t" + seek);

        for (int i = 0; i < num; i++) {
          if (!fdf.next(record)) {
            break;
          }
          record.show();
        }

      } catch (Exception e) {
        e.printStackTrace();
        try {
          Thread.sleep(500);
        } catch (InterruptedException e1) {
          e1.printStackTrace();
        }
        System.out.println("input again ^_^");
      }
    }
  }

  private static void show(Record record) {
    for (FieldValue fv : record.fieldValues()) {
      String str = "null";
      switch (fv.type()) {
      case ConstVar.FieldType_Boolean:
        str = fv.value()[0] == 1 ? "true" : "false";
        break;
      case ConstVar.FieldType_Byte:
        str = String.valueOf(fv.value()[0]);
        break;
      case ConstVar.FieldType_Char:
        str = String.valueOf(fv.value()[0]);
        break;
      case ConstVar.FieldType_Short:
        str = String.valueOf(Util.bytes2short(fv.value(), 0,
            ConstVar.Sizeof_Short));
        break;
      case ConstVar.FieldType_Int:
        str = String
            .valueOf(Util.bytes2int(fv.value(), 0, ConstVar.Sizeof_Int));
        break;
      case ConstVar.FieldType_Long:
        str = String.valueOf(Util.bytes2long(fv.value(), 0,
            ConstVar.Sizeof_Long));
        break;
      case ConstVar.FieldType_Float:
        str = String.valueOf(Util.bytes2float(fv.value(), 0));
        break;
      case ConstVar.FieldType_Double:
        str = String.valueOf(Util.bytes2double(fv.value(), 0));
        break;
      case ConstVar.FieldType_String:
        str = new String(fv.value());
      }
      System.out.print(str + "\t");
    }
    System.out.println();
  }

  public static void shell() throws Exception {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    String str;
    System.out.println("cfdf | cifdf | rfdf | rifdf | h | q");
    while (true) {
      System.out.println("please input the cmd");
      str = br.readLine();
      if (str.equals("h")) {
        System.out.println("cfdf | cifdf | rfdf | rifdf | h | q");
        System.out.println();
      } else if (str.equals("q")) {
        break;
      } else if (str.equals("cfdf")) {
        writeshell(br, "fdf");
      } else if (str.equals("cifdf")) {
        writeshell(br, "ifdf");
      } else if (str.equals("rfdf")) {
        System.out.println("please input the filename");
        readFDFshell(br.readLine());
      } else if (str.equals("rifdf")) {
        System.out.println("please input the filename");
        readIFDFshell(br.readLine());
      }

    }

  }

  private static void writeshell(BufferedReader br, String writetype)
      throws IOException {
    String datadir = "/se/data";
    String wt = writetype;
    int filenum = 10;
    int recnum = 10000;
    short index = -1;
    boolean var = false;
    boolean compress = false;
    boolean seq = true;
    boolean overwrite = true;
    String str;
    while (true) {
      System.out.println("write--> please input the cmd: h | q | e");
      str = br.readLine();
      if (str.equals("h")) {

      } else if (str.equals("q")) {
        return;
      } else if (str.equals("e")) {
        try {
          if (wt.equals("fdf")) {
            writeFDF(datadir, filenum, recnum, index, var, compress, seq,
                overwrite);
          } else if (wt.equals("cfdf")) {
            writeColumnFDF(datadir, filenum, recnum, index, var, compress, seq,
                overwrite);
          } else if (wt.equals("ifdf")) {
            writeIFDF(datadir, filenum, recnum, index, var, compress, seq,
                overwrite);
          }
        } catch (IOException e) {
          e.printStackTrace();
        } catch (Exception e) {
          e.printStackTrace();
        }
      } else if (str.equals("sp")) {
        System.out.println("current param:\t");
        System.out.println("\twritetype:\t" + wt);
        System.out.println("\tdatadir:\t" + datadir);
        System.out.println("\tfilenum:\t" + filenum);
        System.out.println("\trecnum:\t\t" + recnum);
        System.out.println("\tindex:\t\t" + index);
        System.out.println("\tvar:\t\t" + var);
        System.out.println("\tcompress:\t" + compress);
        System.out.println("\tseq:\t\t" + seq);
        System.out.println("\toverwrite:\t" + overwrite);
      } else {
        String[] strs = str.split(" ");
        if (strs.length < 2)
          continue;
        if (strs[0].equals("setdatadir")) {
          datadir = strs[1];
        } else if (strs[0].equals("setwt")) {
          wt = strs[1];
        } else if (strs[0].equals("setidx")) {
          index = Short.valueOf(strs[1]);
        } else if (strs[0].equals("setvar")) {
          var = strs[1].equals("true") ? true : false;
        } else if (strs[0].equals("setcp")) {
          compress = strs[1].equals("true") ? true : false;
        } else if (strs[0].equals("setseq")) {
          seq = strs[1].equals("true") ? true : false;
        } else if (strs[0].equals("setow")) {
          overwrite = strs[1].equals("true") ? true : false;
        } else if (strs[0].equals("setfn")) {
          filenum = Integer.valueOf(strs[1]);
        } else if (strs[0].equals("setrn")) {
          recnum = Integer.valueOf(strs[1]);
        }
      }
    }

  }

  public static void main(String[] args) throws Exception {
    shell();
  }

}
