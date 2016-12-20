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
package FormatStorage1;

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
import FormatStorage1.IRecord.IFType;

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
            record.addValue(new FieldValue(sss, (short) 6));
            sss = increase(sss);
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
      short index, boolean var, boolean compress, boolean seq,
      boolean overwrite, boolean new1, boolean containsnull, double nullprob,
      IHead head) throws IOException {
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
    conf.set("printlog", "true");
    for (int i = 0; i < filenum; i++) {
      IFormatDataFile ifdf = new IFormatDataFile(conf);
      ifdf.create(datadir + "ifdf" + (x + i), head);
      String sss = "aaaaaa";
      IRecord record = ifdf.getIRecordObj();
      for (int j = 0; j < recnum; j++) {
        record.clear();
        for (int idx : head.fieldMap().fieldtypes().keySet()) {
          IFType type = head.fieldMap().fieldtypes().get(idx);
          if (seq) {

            if (type.type() == ConstVar.FieldType_Byte)
              if (!containsnull || r.nextDouble() > nullprob)
                record.addFieldValue(new IRecord.IFValue((byte) (j), idx));
            if (type.type() == ConstVar.FieldType_Short)
              if (!containsnull || r.nextDouble() > nullprob)
                record.addFieldValue(new IRecord.IFValue((short) (j), idx));
            if (type.type() == ConstVar.FieldType_Int)
              if (!containsnull || r.nextDouble() > nullprob)
                record.addFieldValue(new IRecord.IFValue(j, idx));
            if (type.type() == ConstVar.FieldType_Long)
              if (!containsnull || r.nextDouble() > nullprob)
                record.addFieldValue(new IRecord.IFValue((long) (j), idx));
            if (type.type() == ConstVar.FieldType_Float)
              if (!containsnull || r.nextDouble() > nullprob)
                record.addFieldValue(new IRecord.IFValue((float) (j), idx));
            if (type.type() == ConstVar.FieldType_Double)
              if (!containsnull || r.nextDouble() > nullprob)
                record.addFieldValue(new IRecord.IFValue((double) (j), idx));
            if (type.type() == ConstVar.FieldType_String) {
              if (!containsnull || r.nextDouble() > nullprob)
                record.addFieldValue(new IRecord.IFValue(sss, idx));
              sss = increase(sss);
            }
          } else {
            if (type.type() == ConstVar.FieldType_Byte)
              if (!containsnull || r.nextDouble() > nullprob)
                record.addFieldValue(new IRecord.IFValue((byte) (r.nextInt()),
                    idx));
            if (type.type() == ConstVar.FieldType_Short)
              if (!containsnull || r.nextDouble() > nullprob)
                record.addFieldValue(new IRecord.IFValue((short) (r.nextInt()),
                    idx));
            if (type.type() == ConstVar.FieldType_Int)
              if (!containsnull || r.nextDouble() > nullprob)
                record.addFieldValue(new IRecord.IFValue(r.nextInt(), idx));
            if (type.type() == ConstVar.FieldType_Long)
              if (!containsnull || r.nextDouble() > nullprob)
                record.addFieldValue(new IRecord.IFValue(r.nextLong(), idx));
            if (type.type() == ConstVar.FieldType_Float)
              if (!containsnull || r.nextDouble() > nullprob)
                record.addFieldValue(new IRecord.IFValue(r.nextFloat(), idx));
            if (type.type() == ConstVar.FieldType_Double)
              if (!containsnull || r.nextDouble() > nullprob)
                record.addFieldValue(new IRecord.IFValue(r.nextDouble(), idx));
            if (type.type() == ConstVar.FieldType_String) {
              if (!containsnull || r.nextDouble() > nullprob)
                record.addFieldValue(new IRecord.IFValue(getrandomstr(), idx));
            }
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

  private static void writeColumnIFDF(String datadir, int filenum, int recnum,
      short index, boolean var, boolean compress, boolean seq,
      boolean overwrite, boolean new1, boolean containsnull, double nullprob)
      throws IOException {
    IHead head = new IHead(new1 ? ConstVar.NewFormatFile
        : ConstVar.OldFormatFile);
    IFieldMap fieldMap = new IFieldMap();
    fieldMap.addFieldType(new IRecord.IFType(ConstVar.FieldType_Byte, 0));
    fieldMap.addFieldType(new IRecord.IFType(ConstVar.FieldType_Short, 1));
    fieldMap.addFieldType(new IRecord.IFType(ConstVar.FieldType_Int, 2));
    fieldMap.addFieldType(new IRecord.IFType(ConstVar.FieldType_Long, 3));
    fieldMap.addFieldType(new IRecord.IFType(ConstVar.FieldType_Float, 4));
    fieldMap.addFieldType(new IRecord.IFType(ConstVar.FieldType_Double, 5));
    if (var) {
      fieldMap.addFieldType(new IRecord.IFType(ConstVar.FieldType_String, 6));
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
      IColumnDataFile icdf = new IColumnDataFile(conf);
      icdf.create(datadir + "icdf" + (x + i), head);
      String sss = "aaaaaa";
      for (int j = 0; j < recnum; j++) {
        IRecord record = new IRecord(fieldMap.fieldtypes());
        if (seq) {
          if (!containsnull || r.nextDouble() > nullprob)
            record.addFieldValue(new IRecord.IFValue((byte) (j), 0));
          if (!containsnull || r.nextDouble() > nullprob)
            record.addFieldValue(new IRecord.IFValue((short) (j), 1));
          if (!containsnull || r.nextDouble() > nullprob)
            record.addFieldValue(new IRecord.IFValue(j, 2));
          if (!containsnull || r.nextDouble() > nullprob)
            record.addFieldValue(new IRecord.IFValue((long) (j), 3));
          if (!containsnull || r.nextDouble() > nullprob)
            record.addFieldValue(new IRecord.IFValue((float) (j), 4));
          if (!containsnull || r.nextDouble() > nullprob)
            record.addFieldValue(new IRecord.IFValue((double) (j), 5));
          if (var) {
            if (!containsnull || r.nextDouble() > nullprob)
              record.addFieldValue(new IRecord.IFValue(sss, 6));
            sss = increase(sss);
          }
        } else {
          if (!containsnull || r.nextDouble() > nullprob)

            record.addFieldValue(new IRecord.IFValue((byte) (r.nextInt()), 0));
          if (!containsnull || r.nextDouble() > nullprob)
            record.addFieldValue(new IRecord.IFValue((short) (r.nextInt()), 1));
          if (!containsnull || r.nextDouble() > nullprob)
            record.addFieldValue(new IRecord.IFValue(r.nextInt(), 2));
          if (!containsnull || r.nextDouble() > nullprob)
            record.addFieldValue(new IRecord.IFValue(r.nextLong(), 3));
          if (!containsnull || r.nextDouble() > nullprob)
            record.addFieldValue(new IRecord.IFValue(r.nextFloat(), 4));
          if (!containsnull || r.nextDouble() > nullprob)
            record.addFieldValue(new IRecord.IFValue(r.nextDouble(), 5));
          if (var) {
            if (!containsnull || r.nextDouble() > nullprob)
              record.addFieldValue(new IRecord.IFValue(getrandomstr(), 6));
          }
        }
        icdf.addRecord(record);
        if ((j + 1) % 1000000 == 0) {
          System.out.println((j + 1) + "\trecords written");
        }
      }
      icdf.close();

    }

  }

  private static String getrandomstr() {
    StringBuffer sb = new StringBuffer();
    int n = r.nextInt(12);
    for (int i = 0; i < n; i++) {
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
          num = 1;
        }

        long time = System.currentTimeMillis();
        if (line > -1) {
          fdf.seek(line);
          for (int i = 0; i < num; i++) {
            record = fdf.getNextRecord(record);
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

    IRecord record = fdf.getIRecordObj();
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
          num = 1;
        }

        boolean seek = false;
        long time = System.currentTimeMillis();
        if (line > -1) {
          seek = fdf.seek(line);
        }
        System.out.println("seektime:\t" + (System.currentTimeMillis() - time));
        System.out.println("seek:\t" + seek);

        if (seek)
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

  public static void readIFDFVshell(String fileName) {
    IFormatDataFile fdf;
    try {
      fdf = new IFormatDataFile(conf);
      fdf.open(fileName);
    } catch (IOException e2) {
      e2.printStackTrace();
      System.out.println(fileName + ": not exists...");
      return;
    }

    IRecord record = fdf.getIRecordObj();
    int pi = fdf.fileInfo().head().getPrimaryIndex();
    IRecord.IFType type = fdf.fileInfo().head().fieldMap().fieldtypes().get(pi);
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
        String v;
        if (st.hasMoreTokens()) {
          v = st.nextToken();
        } else {
          v = null;
        }
        int num;
        if (st.hasMoreTokens()) {
          num = Integer.parseInt(st.nextToken());
        } else {
          num = 1;
        }

        boolean seek = false;
        long time = System.currentTimeMillis();
        if (v != null) {
          seek = fdf.seek(new IRecord.IFValue(type, TypeConvertUtil.convert(
              ConstVar.FieldType_String, type.type(), v)));
        }
        System.out.println("seektime:\t" + (System.currentTimeMillis() - time));
        System.out.println("seek:\t" + seek);

        if (seek)
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
      if (fv.value() != null && fv.value().length > 0)
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
          str = String.valueOf(Util.bytes2int(fv.value(), 0,
              ConstVar.Sizeof_Int));
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
          if (fv.value() == null || fv.value().length <= 0)
            str = null;
          else
            str = new String(fv.value());
        }
      System.out.print(str + "\t");
    }
    System.out.println();
  }

  public static void shell() throws Exception {
    System.out.println("<< enter shell >>");
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
      } else if (str.equals("rifdfv")) {
        System.out.println("please input the filename");
        readIFDFVshell(br.readLine());
      }
    }
  }

  private static void writeshell(BufferedReader br, String writetype)
      throws IOException {
    System.out.println("<< enter write shell >>");

    String datadir = "/se/data1";
    String wt = writetype;
    int filenum = 1;
    int recnum = 10000;
    short index = -1;
    boolean var = true;
    boolean compress = false;
    boolean seq = false;
    boolean overwrite = true;
    boolean newformat = false;
    boolean containsnull = false;
    double nullprob = 0.5;
    String str;
    String typestr = "byte,short,int,long,float,double,string";
    while (true) {
      System.out.println("write--> please input the cmd: h | q | e");
      str = br.readLine();
      if (str.equals("h")) {

      } else if (str.equals("q")) {
        return;
      } else if (str.equals("e")) {
        long time = System.currentTimeMillis();
        System.out.println(time);
        try {
          if (wt.equals("fdf")) {
            writeFDF(datadir, filenum, recnum, index, var, compress, seq,
                overwrite);
          } else if (wt.equals("cfdf")) {
          } else if (wt.equals("cifdf")) {
            writeColumnIFDF(datadir, filenum, recnum, index, var, compress,
                seq, overwrite, newformat, containsnull, nullprob);
          } else if (wt.equals("ifdf")) {
            IHead head = new IHead(newformat ? ConstVar.NewFormatFile
                : ConstVar.OldFormatFile);
            String[] ss = typestr.split(",");
            IFieldMap fieldMap = new IFieldMap();
            for (int i = 0; i < ss.length; i++) {
              if (ss[i].equalsIgnoreCase("byte"))
                fieldMap.addFieldType(new IRecord.IFType(
                    ConstVar.FieldType_Byte, i));
              else if (ss[i].equalsIgnoreCase("short"))
                fieldMap.addFieldType(new IRecord.IFType(
                    ConstVar.FieldType_Short, i));
              else if (ss[i].equalsIgnoreCase("int"))
                fieldMap.addFieldType(new IRecord.IFType(
                    ConstVar.FieldType_Int, i));
              else if (ss[i].equalsIgnoreCase("long"))
                fieldMap.addFieldType(new IRecord.IFType(
                    ConstVar.FieldType_Long, i));
              else if (ss[i].equalsIgnoreCase("float"))
                fieldMap.addFieldType(new IRecord.IFType(
                    ConstVar.FieldType_Float, i));
              else if (ss[i].equalsIgnoreCase("double"))
                fieldMap.addFieldType(new IRecord.IFType(
                    ConstVar.FieldType_Double, i));
              else if (ss[i].equalsIgnoreCase("string"))
                fieldMap.addFieldType(new IRecord.IFType(
                    ConstVar.FieldType_String, i));
              else {
                System.out.println("type error");
                return;
              }
            }
            head.setFieldMap(fieldMap);

            if (compress) {
              head.setCompress((byte) 1);
              head.setCompressStyle((byte) 2);
            }

            if (index > -1) {
              head.setPrimaryIndex(index);
            }

            writeIFDF(datadir, filenum, recnum, index, var, compress, seq,
                overwrite, newformat, containsnull, nullprob, head);
          }
        } catch (IOException e) {
          e.printStackTrace();
        } catch (Exception e) {
          e.printStackTrace();
        }
        System.err.println("time\t" + (System.currentTimeMillis() - time));
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
        System.out.println("\tnewformat:\t" + newformat);
        System.out.println("\tcontainsnull:\t" + containsnull);
        System.out.println("\tnullprob:\t" + nullprob);
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
        } else if (strs[0].equals("setfmt")) {
          newformat = strs[1].equals("true") ? true : false;
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
        } else if (strs[0].equals("setts")) {
          typestr = strs[1];
        } else if (strs[0].equals("setcn")) {
          containsnull = strs[1].equals("true") ? true : false;
        } else if (strs[0].equals("setnp")) {
          nullprob = Double.parseDouble(strs[1]);
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    System.out.println("main");
    shell();
  }

}
