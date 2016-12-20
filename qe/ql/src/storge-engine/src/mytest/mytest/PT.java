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

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import ColumnStorage.ColumnStorageClient;
import Comm.ConstVar;
import FormatStorage.FieldMap;
import FormatStorage.FormatDataFile;
import FormatStorage.Head;
import FormatStorage.FieldMap.Field;
import FormatStorage.Unit.FieldValue;
import FormatStorage.Unit.Record;

public class PT {
	static Configuration conf;
	static FileSystem fs;
	static {
		conf = new Configuration();
		conf.set("dfs.block.size", "67108864");
		conf.setBoolean("hadoop.native.lib", true);
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void testgenrawfile(String filename, int recordnum)
			throws IOException {
		Random r = new Random();
		FSDataOutputStream fos = fs.create(new Path(filename));
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < recordnum; i++) {
			fos.writeByte(i / 1000);
			fos.writeShort(i / 1000);
			fos.writeInt(i / 1000);
			fos.writeLong(i / 1000);
			fos.writeFloat(i / 1000);
			fos.writeDouble(i / 1000);
			int strnum = r.nextInt(12) + 7;
			sb.setLength(0);
			for (int j = 0; j < strnum; j++) {
				sb.append((char) ('a' + j));
			}
			fos.writeUTF(sb.toString());
			if (i % 1000000 == 0) {
			}
		}
		fos.close();
	}

	public static void testgenrawfiler(String filename, int recordnum)
			throws IOException {
		Random r = new Random();
		FSDataOutputStream fos = fs.create(new Path(filename));
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < recordnum; i++) {
			fos.writeByte(r.nextInt());
			fos.writeShort(r.nextInt());
			fos.writeInt(r.nextInt());
			fos.writeLong(r.nextLong());
			fos.writeFloat(r.nextFloat());
			fos.writeDouble(r.nextDouble());
			int strnum = r.nextInt(12) + 7;
			sb.setLength(0);
			for (int j = 0; j < strnum; j++) {
				sb.append((char) ('a' + r.nextInt(26)));
			}
			fos.writeUTF(sb.toString());
			if (i % 1000000 == 0) {
			}
		}
		fos.close();

	}

	public static void testwritefdf(String filename, String fromfile,
			boolean compress, short keyindex) throws Exception {
		FSDataInputStream fis = fs.open(new Path(fromfile));
		FormatDataFile fdf = createfdf(filename, compress, keyindex);
		for (int i = 0; i < 10000 * 10000; i++) {
			Record record = readrecord(fis);
			if (record == null) {
				break;
			}
			fdf.addRecord(record);
			if (i % 1000000 == 0) {
				System.out.println(i + "\trecords");
			}
		}
		fdf.close();
		fis.close();
	}

	public static void testwritecolumn(String filename, String fromfile,
			boolean compress) throws Exception {

		FormatDataFile[] fdfs = createfdfs(filename, compress);
		FSDataInputStream fis = fs.open(new Path(fromfile));
		for (int i = 0; i < 10000 * 10000; i++) {

			Record record = readrecord(fis);
			if (record == null) {
				break;
			}
			ArrayList<FieldValue> fvs = record.fieldValues();
			for (int j = 0; j < fvs.size(); j++) {
				Record re = new Record(1);
				re.addValue(fvs.get(j));
				fdfs[j].addRecord(re);
			}
			if (i % 1000000 == 0) {
			}

		}
		for (int i = 0; i < fdfs.length; i++) {
			fdfs[i].close();
		}
		fis.close();

	}

	public static void testreadseq(String filename, int num, boolean compress)
			throws Exception {
		FormatDataFile fd2 = new FormatDataFile(conf);
		fd2.open(filename);
		if (compress) {
			Record record = new Record();
			for (int i = 0; i < num; i++) {
				fd2.getNextRecord(record);
			}

		} else {
			for (int i = 0; i < num; i++) {
				fd2.getRecordByLine(i);
			}

		}
		fd2.close();

	}

	public static void testreadrand(String filename, int num, int size)
			throws Exception {
		FormatDataFile fd2 = new FormatDataFile(conf);
		Random r = new Random();
		fd2.open(filename);
		for (int i = 0; i < num; i++) {
			int x = r.nextInt(size);
			fd2.getRecordByLine(x);
		}
		fd2.close();
	}

	public static void testreadcolumnseq(String filename, int num,
			boolean compress, String mode) throws Exception {

		Path path = new Path(filename);
		ArrayList<Short> vector = new ArrayList<Short>(10);

		if (mode == null || mode.equals("-1")) {
			for (short i = 0; i < 7; i++) {
				vector.add(i);
			}
		} else if (mode.equals("half")) {
			short x = 0;
			vector.add(x);
			x = 1;
			vector.add(x);
			x = 2;
			vector.add(x);
			x = 3;
			vector.add(x);
		} else {
			vector.add(Short.valueOf(mode));
		}

		Configuration conf = new Configuration();
		ColumnStorageClient client = new ColumnStorageClient(path, vector, conf);

		if (compress) {
			for (int i = 0; i < num; i++) {
				client.getNextRecord();
			}

		} else {

			for (int i = 0; i < num; i++) {
				client.getRecordByLine(i);
			}
		}

		client.close();

	}

	public static void testreadcolumnrand(String filename, int num, int size,
			String mode) throws Exception {
		Path path = new Path(filename);
		ArrayList<Short> vector = new ArrayList<Short>();

		if (mode == null || mode.equals("-1")) {
			for (short i = 0; i < 7; i++) {
				vector.add(i);
			}
		} else if (mode.equals("half")) {
			short x = 0;
			vector.add(x);
			x = 1;
			vector.add(x);
			x = 2;
			vector.add(x);
			x = 3;
			vector.add(x);
		} else {
			vector.add(Short.valueOf(mode));
		}

		Configuration conf = new Configuration();
		ColumnStorageClient client = new ColumnStorageClient(path, vector, conf);
		Random r = new Random();
		for (int i = 0; i < num; i++) {
			client.getRecordByLine(r.nextInt(size));
			if (i % 1000000 == 0) {
			}
		}
		client.close();

	}

	private static FormatDataFile[] createfdfs(String filename, boolean compress)
			throws Exception {
		if (fs.exists(new Path(filename)))
			fs.delete(new Path(filename), true);
		String fn = filename.endsWith("/") ? filename : (filename + "/");

		String byteFileName = fn + "Column_Byte";
		String shortFileName = fn + "Column_Short";
		String intFileName = fn + "Column_Int";
		String longFileName = fn + "Column_Long";
		String floatFileName = fn + "Column_Float";
		String doubleFileName = fn + "Column_Double";
		String stringFileName = fn + "Column_String";
		FormatDataFile[] fdfs = new FormatDataFile[7];
		Configuration conf = new Configuration();

		FieldMap byteFieldMap = new FieldMap();
		byteFieldMap.addField(new Field(ConstVar.FieldType_Byte,
				ConstVar.Sizeof_Byte, (short) 0));
		Head byteHead = new Head();
		byteHead.setCompress(compress ? (byte) 1 : (byte) 0);
		byteHead.setCompressStyle(ConstVar.LZOCompress);
		byteHead.setFieldMap(byteFieldMap);
		FormatDataFile byteFD = new FormatDataFile(conf);
		byteFD.create(byteFileName, byteHead);

		FieldMap shortFieldMap = new FieldMap();
		shortFieldMap.addField(new Field(ConstVar.FieldType_Short,
				ConstVar.Sizeof_Short, (short) 1));
		Head shortHead = new Head();
		shortHead.setCompress(compress ? (byte) 1 : (byte) 0);
		shortHead.setCompressStyle(ConstVar.LZOCompress);
		shortHead.setFieldMap(shortFieldMap);
		FormatDataFile shortFD = new FormatDataFile(conf);
		shortFD.create(shortFileName, shortHead);

		FieldMap intFieldMap = new FieldMap();
		intFieldMap.addField(new Field(ConstVar.FieldType_Int,
				ConstVar.Sizeof_Int, (short) 2));
		Head intHead = new Head();
		intHead.setCompress(compress ? (byte) 1 : (byte) 0);
		intHead.setCompressStyle(ConstVar.LZOCompress);
		intHead.setFieldMap(intFieldMap);
		FormatDataFile intFD = new FormatDataFile(conf);
		intFD.create(intFileName, intHead);

		FieldMap longFieldMap = new FieldMap();
		longFieldMap.addField(new Field(ConstVar.FieldType_Long,
				ConstVar.Sizeof_Long, (short) 3));
		Head longHead = new Head();
		longHead.setFieldMap(longFieldMap);
		longHead.setCompress(compress ? (byte) 1 : (byte) 0);
		longHead.setCompressStyle(ConstVar.LZOCompress);
		FormatDataFile longFD = new FormatDataFile(conf);
		longFD.create(longFileName, longHead);

		FieldMap floatFieldMap = new FieldMap();
		floatFieldMap.addField(new Field(ConstVar.FieldType_Float,
				ConstVar.Sizeof_Float, (short) 4));
		Head floatHead = new Head();
		floatHead.setCompress(compress ? (byte) 1 : (byte) 0);
		floatHead.setCompressStyle(ConstVar.LZOCompress);
		floatHead.setFieldMap(floatFieldMap);
		FormatDataFile floatFD = new FormatDataFile(conf);
		floatFD.create(floatFileName, floatHead);

		FieldMap doubleFieldMap = new FieldMap();
		doubleFieldMap.addField(new Field(ConstVar.FieldType_Double,
				ConstVar.Sizeof_Double, (short) 5));
		Head doubleHead = new Head();
		doubleHead.setCompress(compress ? (byte) 1 : (byte) 0);
		doubleHead.setCompressStyle(ConstVar.LZOCompress);
		doubleHead.setFieldMap(doubleFieldMap);
		FormatDataFile doubleFD = new FormatDataFile(conf);
		doubleFD.create(doubleFileName, doubleHead);

		FieldMap strFieldMap = new FieldMap();
		strFieldMap
				.addField(new Field(ConstVar.FieldType_String, 0, (short) 6));
		Head strHead = new Head();
		strHead.setCompress(compress ? (byte) 1 : (byte) 0);
		strHead.setCompressStyle(ConstVar.LZOCompress);
		strHead.setFieldMap(strFieldMap);
		FormatDataFile strFD = new FormatDataFile(conf);
		strFD.create(stringFileName, strHead);

		fdfs[0] = byteFD;
		fdfs[1] = shortFD;
		fdfs[2] = intFD;
		fdfs[3] = longFD;
		fdfs[4] = floatFD;
		fdfs[5] = doubleFD;
		fdfs[6] = strFD;
		return fdfs;
	}

	private static Record readrecord(FSDataInputStream fis) {
		Record record;
		try {
			record = new Record((short) 7);
			record.addValue(new FieldValue((byte) (fis.readByte()), (short) 0));
			record
					.addValue(new FieldValue((short) (fis.readShort()),
							(short) 1));
			record.addValue(new FieldValue((int) (fis.readInt()), (short) 2));
			record.addValue(new FieldValue((long) (fis.readLong()), (short) 3));
			record
					.addValue(new FieldValue((float) (fis.readFloat()),
							(short) 4));
			record.addValue(new FieldValue((double) (fis.readDouble()),
					(short) 5));
			String str = fis.readUTF();
			record.addValue(new FieldValue(str, (short) 6));

			return record;
		} catch (Exception e) {
			return null;
		}
	}

	public static FormatDataFile createfdf(String filename, boolean compress,
			short keyindex) throws Exception {
		Head head = new Head();
		FieldMap fieldMap = new FieldMap();

		fieldMap.addField(new Field(ConstVar.FieldType_Byte,
				ConstVar.Sizeof_Byte, (short) 0));
		fieldMap.addField(new Field(ConstVar.FieldType_Short,
				ConstVar.Sizeof_Short, (short) 1));
		fieldMap.addField(new Field(ConstVar.FieldType_Int,
				ConstVar.Sizeof_Int, (short) 2));
		fieldMap.addField(new Field(ConstVar.FieldType_Long,
				ConstVar.Sizeof_Long, (short) 3));
		fieldMap.addField(new Field(ConstVar.FieldType_Float,
				ConstVar.Sizeof_Float, (short) 4));
		fieldMap.addField(new Field(ConstVar.FieldType_Double,
				ConstVar.Sizeof_Double, (short) 5));
		fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short) 6));

		head.setFieldMap(fieldMap);
		head.setPrimaryIndex(keyindex);

		if (compress) {
			head.setCompress((byte) 1);
			head.setCompressStyle(ConstVar.LZOCompress);
		}

		Configuration conf = new Configuration();
		FormatDataFile fd = new FormatDataFile(conf);
		fd.create(filename, head);
		return fd;

	}

	public static long main1(String[] args, FileWriter fw) throws Exception {
		if (args.length < 1) {
			System.out
					.println("input cmd param: [ writeraw | writerawr | writefdf | writecolumn | readseq | readrand | readcolumnseq | readcolumnrand ]");
			return -1;
		}
		String cmd = args[0];
		long time = System.currentTimeMillis();
		int recordnum = 0;
		if (cmd.equals("writeraw")) {
			if (args.length < 3) {
				System.out.println("input param: filename recordnum\t");
				return -1;
			}
			String filename = args[1];
			recordnum = Integer.valueOf(args[2]);
			testgenrawfile(filename, recordnum);
		} else if (cmd.equals("writerawr")) {
			if (args.length < 3) {
				System.out.println("input param: filename recordnum\t");
				return -1;
			}
			String filename = args[1];
			recordnum = Integer.valueOf(args[2]);
			testgenrawfiler(filename, recordnum);

		} else if (cmd.equals("writefdf")) {

			if (args.length < 3) {
				System.out
						.println("input param: filename fromfile [compress]\t");
				return -1;
			}
			String filename = args[1];
			String fromfile = args[2];
			boolean compress = false;
			if (args.length >= 4 && args[3].equals("compress")) {
				compress = true;
			}
			testwritefdf(filename, fromfile, compress, (short) -1);

		} else if (cmd.equals("writecolumn")) {
			if (args.length < 3) {
				System.out
						.println("input param: filename fromfile [compress]\t");
				return -1;
			}
			String filename = args[1];
			String fromfile = args[2];
			boolean compress = false;
			if (args.length >= 4 && args[3].equals("compress")) {
				compress = true;
			}
			testwritecolumn(filename, fromfile, compress);

		} else if (cmd.equals("readseq")) {
			if (args.length < 3) {
				System.out.println("input param: filename number");
				return -1;
			}
			String filename = args[1];
			recordnum = Integer.valueOf(args[2]);
			boolean compress = false;
			if (args.length >= 4 && args[3].equals("compress")) {
				compress = true;
			}

			testreadseq(filename, recordnum, compress);

		} else if (cmd.equals("readrand")) {
			if (args.length < 3) {
				System.out.println("input param: filename number size");
				return -1;
			}
			String filename = args[1];
			recordnum = Integer.valueOf(args[2]);
			int size = Integer.valueOf(args[3]);
			testreadrand(filename, recordnum, size);
		} else if (cmd.equals("readcolumnseq")) {
			if (args.length < 3) {
				System.out.println("input param: filename number");
				return -1;
			}
			String filename = args[1];
			recordnum = Integer.valueOf(args[2]);
			boolean compress = false;
			String mode = null;
			if (args.length >= 4) {
				if (args[3].equals("compress"))
					compress = true;
				else
					mode = args[3];
			}
			if (args.length >= 5) {
				mode = args[4];
			}
			testreadcolumnseq(filename, recordnum, compress, mode);

		} else if (cmd.equals("readcolumnrand")) {
			if (args.length < 3) {
				System.out.println("input param: filename number size");
				return -1;
			}
			String filename = args[1];
			recordnum = Integer.valueOf(args[2]);
			int size = Integer.valueOf(args[3]);
			String mode = null;
			if (args.length >= 5) {
				mode = args[4];
			}
			testreadcolumnrand(filename, recordnum, size, mode);

		} else {
			System.out
					.println("input cmd param: [ writeraw | writefdf | writecolumn | readseq | readrand | readcolumnseq | readcolumnrand ]");

		}

		long runtime = System.currentTimeMillis() - time;
		String str = recordnum + "\t" + runtime + "\t";
		fw.append(str);
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < args.length; i++) {
			sb.append(args[i] + "\t");
		}
		sb.append("\r\n");
		fw.append(sb.toString());
		fw.flush();
		System.out.println(sb.toString());
		return runtime;

	}

	public static void main(String[] args) throws Exception {
		FileWriter fw = new FileWriter("result.txt");
		String r45_s_f_100w = "readseq /pt/f0p45/f 1000000";
		String r45_s_f_1000w = "readseq /pt/f0p45/f 10000000";
		String r45_s_fc_100w = "readseq /pt/fc0p45/f 1000000 compress";
		String r45_s_fc_1000w = "readseq /pt/fc0p45/f 10000000 compress";
		String r15_s_f_100w = "readseq /pt/f0p15/f 1000000";
		String r15_s_f_1000w = "readseq /pt/f0p15/f 10000000";
		String r15_s_fc_100w = "readseq /pt/fc0p15/f 1000000 compress";
		String r15_s_fc_1000w = "readseq /pt/fc0p15/f 10000000 compress";

		String r45_s_c_100w = "readcolumnseq /pt/c0p45/f 1000000";
		String r45_s_c_1000w = "readcolumnseq /pt/c0p45/f 10000000";
		String r45_s_cc_100w = "readcolumnseq /pt/cc0p45/f 1000000 compress";
		String r45_s_cc_1000w = "readcolumnseq /pt/cc0p45/f 10000000 compress";
		String r15_s_c_100w = "readcolumnseq /pt/c0p15/f 1000000";
		String r15_s_c_1000w = "readcolumnseq /pt/c0p15/f 10000000";
		String r15_s_cc_100w = "readcolumnseq /pt/cc0p15/f 1000000 compress";
		String r15_s_cc_1000w = "readcolumnseq /pt/cc0p15/f 10000000 compress";

		String r45_r_f_100 = "readrand /pt/f0p45/f 100 10000000";
		String r45_r_f_500 = "readrand /pt/f0p45/f 500 10000000";
		String r45_r_fc_100 = "readrand /pt/fc0p45/f 100 10000000";
		String r45_r_fc_500 = "readrand /pt/fc0p45/f 500 10000000";
		String r15_r_f_100 = "readrand /pt/f0p15/f 100 10000000";
		String r15_r_f_500 = "readrand /pt/f0p15/f 500 10000000";
		String r15_r_fc_100 = "readrand /pt/fc0p15/f 100 10000000";
		String r15_r_fc_500 = "readrand /pt/fc0p15/f 500 10000000";

		String r45_r_c_100 = "readcolumnrand /pt/c0p45/f 100 10000000";
		String r45_r_c_500 = "readcolumnrand /pt/c0p45/f 500 10000000";
		String r45_r_cc_100 = "readcolumnrand /pt/cc0p45/f 100 10000000";
		String r45_r_cc_500 = "readcolumnrand /pt/cc0p45/f 500 10000000";
		String r15_r_c_100 = "readcolumnrand /pt/c0p15/f 100 10000000";
		String r15_r_c_500 = "readcolumnrand /pt/c0p15/f 500 10000000";
		String r15_r_cc_100 = "readcolumnrand /pt/cc0p15/f 100 10000000";
		String r15_r_cc_500 = "readcolumnrand /pt/cc0p15/f 500 10000000";

		String r45_s_c_100w_m0 = "readcolumnseq /pt/c0p45/f 1000000 0";
		String r45_s_c_1000w_m0 = "readcolumnseq /pt/c0p45/f 10000000 0";
		String r45_s_cc_100w_m0 = "readcolumnseq /pt/cc0p45/f 1000000 compress 0";
		String r45_s_cc_1000w_m0 = "readcolumnseq /pt/cc0p45/f 10000000 compress 0";
		String r15_s_c_100w_m0 = "readcolumnseq /pt/c0p15/f 1000000 0";
		String r15_s_c_1000w_m0 = "readcolumnseq /pt/c0p15/f 10000000 0";
		String r15_s_cc_100w_m0 = "readcolumnseq /pt/cc0p15/f 1000000 compress 0";
		String r15_s_cc_1000w_m0 = "readcolumnseq /pt/cc0p15/f 10000000 compress 0";

		String r45_r_c_100_m0 = "readcolumnrand /pt/c0p45/f 100 10000000 0";
		String r45_r_c_500_m0 = "readcolumnrand /pt/c0p45/f 500 10000000 0";
		String r45_r_cc_100_m0 = "readcolumnrand /pt/cc0p45/f 100 10000000 0";
		String r45_r_cc_500_m0 = "readcolumnrand /pt/cc0p45/f 500 10000000 0";
		String r15_r_c_100_m0 = "readcolumnrand /pt/c0p15/f 100 10000000 0";
		String r15_r_c_500_m0 = "readcolumnrand /pt/c0p15/f 500 10000000 0";
		String r15_r_cc_100_m0 = "readcolumnrand /pt/cc0p15/f 100 10000000 0";
		String r15_r_cc_500_m0 = "readcolumnrand /pt/cc0p15/f 500 10000000 0";

		String r45_s_c_100w_m1 = "readcolumnseq /pt/c0p45/f 1000000 1";
		String r45_s_c_1000w_m1 = "readcolumnseq /pt/c0p45/f 10000000 1";
		String r45_s_cc_100w_m1 = "readcolumnseq /pt/cc0p45/f 1000000 compress 1";
		String r45_s_cc_1000w_m1 = "readcolumnseq /pt/cc0p45/f 10000000 compress 1";
		String r15_s_c_100w_m1 = "readcolumnseq /pt/c0p15/f 1000000 1";
		String r15_s_c_1000w_m1 = "readcolumnseq /pt/c0p15/f 10000000 1";
		String r15_s_cc_100w_m1 = "readcolumnseq /pt/cc0p15/f 1000000 compress 1";
		String r15_s_cc_1000w_m1 = "readcolumnseq /pt/cc0p15/f 10000000 compress 1";

		String r45_r_c_100_m1 = "readcolumnrand /pt/c0p45/f 100 10000000 1";
		String r45_r_c_500_m1 = "readcolumnrand /pt/c0p45/f 500 10000000 1";
		String r45_r_cc_100_m1 = "readcolumnrand /pt/cc0p45/f 100 10000000 1";
		String r45_r_cc_500_m1 = "readcolumnrand /pt/cc0p45/f 500 10000000 1";
		String r15_r_c_100_m1 = "readcolumnrand /pt/c0p15/f 100 10000000 1";
		String r15_r_c_500_m1 = "readcolumnrand /pt/c0p15/f 500 10000000 1";
		String r15_r_cc_100_m1 = "readcolumnrand /pt/cc0p15/f 100 10000000 1";
		String r15_r_cc_500_m1 = "readcolumnrand /pt/cc0p15/f 500 10000000 1";

		String r45_s_c_100w_m6 = "readcolumnseq /pt/c0p45/f 1000000 6";
		String r45_s_c_1000w_m6 = "readcolumnseq /pt/c0p45/f 10000000 6";
		String r45_s_cc_100w_m6 = "readcolumnseq /pt/cc0p45/f 1000000 compress 6";
		String r45_s_cc_1000w_m6 = "readcolumnseq /pt/cc0p45/f 10000000 compress 6";
		String r15_s_c_100w_m6 = "readcolumnseq /pt/c0p15/f 1000000 6";
		String r15_s_c_1000w_m6 = "readcolumnseq /pt/c0p15/f 10000000 6";
		String r15_s_cc_100w_m6 = "readcolumnseq /pt/cc0p15/f 1000000 compress 6";
		String r15_s_cc_1000w_m6 = "readcolumnseq /pt/cc0p15/f 10000000 compress 6";

		String r45_r_c_100_m6 = "readcolumnrand /pt/c0p45/f 100 10000000 6";
		String r45_r_c_500_m6 = "readcolumnrand /pt/c0p45/f 500 10000000 6";
		String r45_r_cc_100_m6 = "readcolumnrand /pt/cc0p45/f 100 10000000 6";
		String r45_r_cc_500_m6 = "readcolumnrand /pt/cc0p45/f 500 10000000 6";
		String r15_r_c_100_m6 = "readcolumnrand /pt/c0p15/f 100 10000000 6";
		String r15_r_c_500_m6 = "readcolumnrand /pt/c0p15/f 500 10000000 6";
		String r15_r_cc_100_m6 = "readcolumnrand /pt/cc0p15/f 100 10000000 6";
		String r15_r_cc_500_m6 = "readcolumnrand /pt/cc0p15/f 500 10000000 6";

		String r45_s_c_100w_half = "readcolumnseq /pt/c0p45/f 1000000 half";
		String r45_s_c_1000w_half = "readcolumnseq /pt/c0p45/f 10000000 half";
		String r45_s_cc_100w_half = "readcolumnseq /pt/cc0p45/f 1000000 compress half";
		String r45_s_cc_1000w_half = "readcolumnseq /pt/cc0p45/f 10000000 compress half";
		String r15_s_c_100w_half = "readcolumnseq /pt/c0p15/f 1000000 half";
		String r15_s_c_1000w_half = "readcolumnseq /pt/c0p15/f 10000000 half";
		String r15_s_cc_100w_half = "readcolumnseq /pt/cc0p15/f 1000000 compress half";
		String r15_s_cc_1000w_half = "readcolumnseq /pt/cc0p15/f 10000000 compress half";

		String r45_r_c_100_half = "readcolumnrand /pt/c0p45/f 100 10000000 half";
		String r45_r_c_500_half = "readcolumnrand /pt/c0p45/f 500 10000000 half";
		String r45_r_cc_100_half = "readcolumnrand /pt/cc0p45/f 100 10000000 half";
		String r45_r_cc_500_half = "readcolumnrand /pt/cc0p45/f 500 10000000 half";
		String r15_r_c_100_half = "readcolumnrand /pt/c0p15/f 100 10000000 half";
		String r15_r_c_500_half = "readcolumnrand /pt/c0p15/f 500 10000000 half";
		String r15_r_cc_100_half = "readcolumnrand /pt/cc0p15/f 100 10000000 half";
		String r15_r_cc_500_half = "readcolumnrand /pt/cc0p15/f 500 10000000 half";

		String[] cmds = new String[32];

		cmds[0] = r45_s_f_100w;
		cmds[1] = r45_s_f_1000w;
		cmds[2] = r45_s_fc_100w;
		cmds[3] = r45_s_fc_1000w;
		cmds[4] = r15_s_f_100w;
		cmds[5] = r15_s_f_1000w;
		cmds[6] = r15_s_fc_100w;
		cmds[7] = r15_s_fc_1000w;

		cmds[8] = r45_s_c_100w;
		cmds[9] = r45_s_c_1000w;
		cmds[10] = r45_s_cc_100w;
		cmds[11] = r45_s_cc_1000w;
		cmds[12] = r15_s_c_100w;
		cmds[13] = r15_s_c_1000w;
		cmds[14] = r15_s_cc_100w;
		cmds[15] = r15_s_cc_1000w;

		cmds[16] = r45_r_f_100;
		cmds[17] = r45_r_f_500;
		cmds[18] = r45_r_fc_100;
		cmds[19] = r45_r_fc_500;
		cmds[20] = r15_r_f_100;
		cmds[21] = r15_r_f_500;
		cmds[22] = r15_r_fc_100;
		cmds[23] = r15_r_fc_500;

		cmds[24] = r45_r_c_100;
		cmds[25] = r45_r_c_500;
		cmds[26] = r45_r_cc_100;
		cmds[27] = r45_r_cc_500;
		cmds[28] = r15_r_c_100;
		cmds[29] = r15_r_c_500;
		cmds[30] = r15_r_cc_100;
		cmds[31] = r15_r_cc_500;

		String[] cmdsmode0 = new String[16];

		cmdsmode0[0] = r45_s_c_100w_m0;
		cmdsmode0[1] = r45_s_c_1000w_m0;
		cmdsmode0[2] = r45_s_cc_100w_m0;
		cmdsmode0[3] = r45_s_cc_1000w_m0;
		cmdsmode0[4] = r15_s_c_100w_m0;
		cmdsmode0[5] = r15_s_c_1000w_m0;
		cmdsmode0[6] = r15_s_cc_100w_m0;
		cmdsmode0[7] = r15_s_cc_1000w_m0;

		cmdsmode0[8] = r45_r_c_100_m0;
		cmdsmode0[9] = r45_r_c_500_m0;
		cmdsmode0[10] = r45_r_cc_100_m0;
		cmdsmode0[11] = r45_r_cc_500_m0;
		cmdsmode0[12] = r15_r_c_100_m0;
		cmdsmode0[13] = r15_r_c_500_m0;
		cmdsmode0[14] = r15_r_cc_100_m0;
		cmdsmode0[15] = r15_r_cc_500_m0;

		String[] cmdsmode1 = new String[16];

		cmdsmode1[0] = r45_s_c_100w_m1;
		cmdsmode1[1] = r45_s_c_1000w_m1;
		cmdsmode1[2] = r45_s_cc_100w_m1;
		cmdsmode1[3] = r45_s_cc_1000w_m1;
		cmdsmode1[4] = r15_s_c_100w_m1;
		cmdsmode1[5] = r15_s_c_1000w_m1;
		cmdsmode1[6] = r15_s_cc_100w_m1;
		cmdsmode1[7] = r15_s_cc_1000w_m1;

		cmdsmode1[8] = r45_r_c_100_m1;
		cmdsmode1[9] = r45_r_c_500_m1;
		cmdsmode1[10] = r45_r_cc_100_m1;
		cmdsmode1[11] = r45_r_cc_500_m1;
		cmdsmode1[12] = r15_r_c_100_m1;
		cmdsmode1[13] = r15_r_c_500_m1;
		cmdsmode1[14] = r15_r_cc_100_m1;
		cmdsmode1[15] = r15_r_cc_500_m1;

		String[] cmdsmode6 = new String[16];

		cmdsmode6[0] = r45_s_c_100w_m6;
		cmdsmode6[1] = r45_s_c_1000w_m6;
		cmdsmode6[2] = r45_s_cc_100w_m6;
		cmdsmode6[3] = r45_s_cc_1000w_m6;
		cmdsmode6[4] = r15_s_c_100w_m6;
		cmdsmode6[5] = r15_s_c_1000w_m6;
		cmdsmode6[6] = r15_s_cc_100w_m6;
		cmdsmode6[7] = r15_s_cc_1000w_m6;

		cmdsmode6[8] = r45_r_c_100_m6;
		cmdsmode6[9] = r45_r_c_500_m6;
		cmdsmode6[10] = r45_r_cc_100_m6;
		cmdsmode6[11] = r45_r_cc_500_m6;
		cmdsmode6[12] = r15_r_c_100_m6;
		cmdsmode6[13] = r15_r_c_500_m6;
		cmdsmode6[14] = r15_r_cc_100_m6;
		cmdsmode6[15] = r15_r_cc_500_m6;

		String[] cmdsmodehalf = new String[16];

		cmdsmodehalf[0] = r45_s_c_100w_half;
		cmdsmodehalf[1] = r45_s_c_1000w_half;
		cmdsmodehalf[2] = r45_s_cc_100w_half;
		cmdsmodehalf[3] = r45_s_cc_1000w_half;
		cmdsmodehalf[4] = r15_s_c_100w_half;
		cmdsmodehalf[5] = r15_s_c_1000w_half;
		cmdsmodehalf[6] = r15_s_cc_100w_half;
		cmdsmodehalf[7] = r15_s_cc_1000w_half;

		cmdsmodehalf[8] = r45_r_c_100_half;
		cmdsmodehalf[9] = r45_r_c_500_half;
		cmdsmodehalf[10] = r45_r_cc_100_half;
		cmdsmodehalf[11] = r45_r_cc_500_half;
		cmdsmodehalf[12] = r15_r_c_100_half;
		cmdsmodehalf[13] = r15_r_c_500_half;
		cmdsmodehalf[14] = r15_r_cc_100_half;
		cmdsmodehalf[15] = r15_r_cc_500_half;

		long[][] times = new long[32][5];
		for (int i = 0; i < 5; i++) {
			for (int j = 0; j < cmds.length; j = j + 2) {
				times[j][i] = main1(cmds[j].split(" "), fw);
			}
			for (int j = 1; j < cmds.length; j = j + 2) {
				times[j][i] = main1(cmds[j].split(" "), fw);
			}
		}
		long[][][] timesmode = new long[4][16][5];
		for (int i = 0; i < 5; i++) {
			for (int j = 0; j < cmdsmode0.length; j++) {
				timesmode[0][j][i] = main1(cmdsmode0[j].split(" "), fw);
				timesmode[1][j][i] = main1(cmdsmode1[j].split(" "), fw);
				timesmode[2][j][i] = main1(cmdsmode6[j].split(" "), fw);
				timesmode[3][j][i] = main1(cmdsmodehalf[j].split(" "), fw);
			}

		}
		fw.close();

		fw = new FileWriter("result1.txt");
		for (int i = 0; i < times.length; i++) {
			for (int j = 0; j < times[1].length; j++) {
				fw.write(String.valueOf(times[i][j]) + "\t");
			}
			fw.write("\r\n");
		}
		fw.close();

		fw = new FileWriter("resultmode.txt");
		for (int i = 0; i < timesmode.length; i++) {
			for (int j = 0; j < timesmode[i].length; j++) {
				for (int j2 = 0; j2 < timesmode[i][j].length; j2++) {
					fw.write(String.valueOf(timesmode[i][j][j2]) + "\t");
				}
				fw.write("\r\n");
			}
			fw.write("\r\n");
			fw.write("\r\n");
		}
		fw.close();

	}

}
