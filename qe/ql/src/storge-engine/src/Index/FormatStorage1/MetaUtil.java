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

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class MetaUtil {

  public static void meta(String[] args) throws IOException {
    String filename = args[1];
    IFormatDataFile ifdf = new IFormatDataFile(new Configuration());
    ifdf.open(filename);
    System.out.println(ifdf.fileInfo().head().toStr());
    System.out.println("recnum:\t" + ifdf.recnum());
    ISegmentIndex isi = ifdf.segIndex();
    int segnum = isi.getSegnum();
    System.out.println("segnum:\t" + segnum);
    for (int i = 0; i < segnum; i++) {
      System.out.print("seg:" + i + "\tlen:" + isi.getseglen(i) + "\toffset:"
          + isi.getSegOffset(i));
      System.out.println("\tbeginline:" + isi.getILineIndex(i).beginline()
          + "\tendline:" + isi.getILineIndex(i).endline());
    }
    ifdf.close();
  }

  private static void metadetail(String[] args) throws IOException {
    String filename = args[1];
    IFormatDataFile ifdf = new IFormatDataFile(new Configuration());
    ifdf.open(filename);

    System.out.println(ifdf.fileInfo().head().toStr());
    System.out.println("recnum: " + ifdf.recnum());
    ISegmentIndex segindex = ifdf.segIndex();
    int segnum = segindex.getSegnum();
    System.out.println("segnum: " + segnum);
    for (int i = 0; i < segnum; i++) {
      System.out.print("seg: " + i + "\tlen: " + segindex.getseglen(i)
          + "\toffset: " + segindex.getSegOffset(i));
      System.out.println("\tbeginline: "
          + segindex.getILineIndex(i).beginline() + "\tendline: "
          + segindex.getILineIndex(i).endline());
      ifdf.seek(segindex.getILineIndex(i).beginline());
      IUnitIndex unitindex = ifdf.currSegment().unitindex();
      int unitnum = unitindex.getUnitnum();
      System.out.println("unitnum: " + unitnum);
      for (int j = 0; j < unitnum; j++) {
        System.out.print("unit: " + j + " \t" + unitindex.getUnitLen(j) + "\t"
            + unitindex.getUnitOffset(j));
        System.out.println("\t" + unitindex.getLineIndex(j).beginline() + "\t"
            + unitindex.getLineIndex(j).endline());
      }
    }
    ifdf.close();
  }

  private static void position(String[] args) throws IOException {
    String filename = args[1];
    int line = Integer.valueOf(args[2]);
    IFormatDataFile ifdf = new IFormatDataFile(new Configuration());
    ifdf.open(filename);
    ifdf.seek(line);
    System.out.print("segid:" + ifdf.segIndex().getSegid(line));
    System.out.println("\tunitid:"
        + ifdf.currSegment().unitindex().getUnitid(line));
    ifdf.close();
  }

  public static void read(String[] args) throws IOException {
    String filename = args[1];
    IFormatDataFile ifdf = new IFormatDataFile(new Configuration());
    ifdf.open(filename);
    int start = 0;
    int num = ifdf.recnum();
    if (args.length > 2) {
      start = Integer.parseInt(args[2]);
      num = num - start;
      if (args.length > 3) {
        num = Math.min(Integer.parseInt(args[3]), num);
      }
    }
    System.out.println("start:" + start + "\tnum:" + num);
    IRecord record = ifdf.getIRecordObj();
    ifdf.seek(start);
    long time = System.currentTimeMillis();
    for (int i = start; i < num + start; i++) {
      try {
        ifdf.next(record);
      } catch (Exception e) {
        System.out.println("read fail:\t" + i);
        record.show();
        e.printStackTrace();
        return;
      }
      if (i % 1000000 == 0)
        System.out.println(i);
    }
    long t = System.currentTimeMillis() - time;
    System.out.println(num + "\t" + t + "\t");
    ifdf.close();
  }

  private static void readp(String[] args) throws IOException {

    String filename = args[1];
    IFormatDataFile ifdf = new IFormatDataFile(new Configuration());
    ifdf.open(filename);
    int start = 0;
    int num = ifdf.recnum();
    if (args.length > 2) {
      start = Integer.parseInt(args[2]);
      num = num - start;
      if (args.length > 3) {
        num = Math.min(Integer.parseInt(args[3]), num);
      }
    }
    System.out.println("start:" + start + "\tnum:" + num);
    IRecord record = ifdf.getIRecordObj();
    ifdf.seek(start);
    long time = System.currentTimeMillis();
    for (int i = start; i < num + start; i++) {
      try {
        ifdf.next(record);
        record.show();
      } catch (Exception e) {
        System.out.println("read fail:\t" + i);
        record.show();
        e.printStackTrace();
        return;
      }
      if (i % 1000000 == 0)
        System.out.println(i);
    }
    long t = System.currentTimeMillis() - time;
    System.out.println(num + "\t" + t + "\t");
    ifdf.close();
  }

  private static void rebuild(String[] args) throws IOException {
    int segs = args.length - 2;
    int[] starts, lens;
    String inputfile = null;
    String outputfile = null;
    inputfile = args[1];
    outputfile = inputfile;
    int first = 2;
    if (!args[2].startsWith("start=")) {
      outputfile = args[2];
      segs = args.length - 3;
      first = 3;
    }
    if (args.length - first == 0 || (args.length - first) % 2 != 0) {
      throw new IOException("error");
    }
    starts = new int[segs / 2];
    lens = new int[segs / 2];
    for (int i = first; i < args.length; i = i + 2) {
      String[] strs = args[i].split("=");
      if (strs[0].equals("start")) {
        starts[(i - first) / 2] = Integer.valueOf(strs[1]);
      } else {
        throw new IOException("error");
      }
      strs = args[i + 1].split("=");
      if (strs[0].equals("len")) {
        lens[(i - first) / 2] = Integer.valueOf(strs[1]);
      } else if (strs[0].equals("end")) {
        lens[(i - first) / 2] = Integer.valueOf(strs[1])
            - starts[(i - first) / 2] + 1;
      } else {
        throw new IOException("error");
      }
    }
    Configuration conf = new Configuration();
    IFormatDataFile ifdfin = new IFormatDataFile(conf);
    IFormatDataFile ifdfout = new IFormatDataFile(conf);
    ifdfin.open(inputfile);
    ifdfout.create(outputfile + "_rebuild_tmp", ifdfin.fileInfo().head());
    for (int i = 0; i < starts.length; i++) {
      ifdfin.seek(starts[i]);
      for (int j = 0; j < lens[i]; j++) {
        ifdfout.addRecord(ifdfin.next());
      }
    }
    ifdfin.close();
    ifdfout.close();
    FileSystem fs = FileSystem.get(conf);
    if (inputfile.equals(outputfile)) {
      fs.delete(new Path(inputfile), true);
    }
    fs.rename(new Path(outputfile + "_rebuild_tmp"), new Path(outputfile));
  }

  private static void delete(String[] args) throws IOException {
    String inputfile = args[1];
    String outputfile = inputfile;
    int idx = 2;

    if (!args[2].startsWith("segid=") && !args[2].startsWith("unitid=")
        && !args[2].startsWith("lines=")) {
      outputfile = args[2];
      idx = 3;
    }

    int segid = -1, unitid = -1;
    boolean line = false;
    TreeSet<Integer> lines = new TreeSet<Integer>();
    TreeMap<Integer, TreeSet<Integer>> seg2unitID = new TreeMap<Integer, TreeSet<Integer>>();

    while (idx < args.length) {
      String[] strs = args[idx++].split("=");
      if (strs[0].equals("segid")) {
        segid = Integer.valueOf(strs[1]);
        seg2unitID.put(segid, new TreeSet<Integer>());
      } else if (strs[0].equals("unitid")) {
        unitid = Integer.valueOf(strs[1]);
        seg2unitID.lastEntry().getValue().add(unitid);
      } else if (strs[0].equals("lines")) {
        String[] linestr = strs[1].split(",");
        line = true;
        for (int j = 0; j < linestr.length; j++) {
          lines.add(Integer.valueOf(linestr[j]));
        }
      } else {
        throw new IOException("error");
      }
    }
    Configuration conf = new Configuration();
    IFormatDataFile ifdfin = new IFormatDataFile(conf);
    ifdfin.open(inputfile);

    ArrayList<Integer> starts = new ArrayList<Integer>();
    ArrayList<Integer> ends = new ArrayList<Integer>();
    if (line) {
      starts.add(0);
      for (int i : lines) {
        ends.add(i);
        starts.add(i + 1);
      }
      ends.add(ifdfin.recnum());
    } else {
      starts.add(0);
      for (int sid : seg2unitID.keySet()) {
        int segbl = ifdfin.segIndex().getILineIndex(sid).beginline();
        int segel = ifdfin.segIndex().getILineIndex(sid).endline();
        if (seg2unitID.get(sid) == null || seg2unitID.get(sid).size() == 0) {
          ends.add(segbl);
          starts.add(segel + 1);
        } else {
          ifdfin.seek(segbl);
          for (Integer uid : seg2unitID.get(sid)) {
            int ubl = ifdfin.currSegment().unitindex().getLineIndex(uid)
                .beginline();
            int uel = ifdfin.currSegment().unitindex().getLineIndex(uid)
                .endline();
            ends.add(ubl);
            starts.add(uel + 1);
          }
        }
      }
      ends.add(ifdfin.recnum());
    }
    System.out.println("starts:\t" + starts);
    System.out.println("ends:\t" + ends);

    IFormatDataFile ifdfout = new IFormatDataFile(conf);
    ifdfout.create(outputfile + "_delete_tmp", ifdfin.fileInfo().head());
    for (int i = 0; i < starts.size(); i++) {
      int bl = starts.get(i);
      ifdfin.seek(bl);
      for (int j = bl; j < ends.get(i); j++) {
        try {
          ifdfout.addRecord(ifdfin.next());
        } catch (Exception e) {
          System.out.println(j);
          e.printStackTrace();
          return;
        }
      }
    }
    ifdfout.close();
    ifdfin.close();
    FileSystem fs = FileSystem.get(conf);
    if (inputfile.equals(outputfile)) {
      fs.delete(new Path(inputfile), true);
    }
    fs.rename(new Path(outputfile + "_delete_tmp"), new Path(outputfile));
  }

  public static void main(String[] args) throws IOException {
    if (args[0].equals("meta")) {
      meta(args);
    } else if (args[0].equals("metadetail")) {
      metadetail(args);
    } else if (args[0].equals("position")) {
      position(args);
    } else if (args[0].equals("read")) {
      read(args);
    } else if (args[0].equals("readp")) {
      readp(args);
    } else if (args[0].equals("rebuild")) {
      rebuild(args);
    } else if (args[0].equals("delete")) {
      delete(args);
    }
  }
}
