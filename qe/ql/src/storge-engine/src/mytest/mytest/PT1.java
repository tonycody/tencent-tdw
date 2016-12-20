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

public class PT1 {
	static Configuration conf;
	static FileSystem fs;
	static {
		conf = new Configuration();
		conf.setBoolean("hadoop.native.lib", true);
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	static void testgenrawfile(String filename, int recordnum)
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

	static void testgenrawfiler(String filename, int recordnum)
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

	static void testwritefdf(String filename, String fromfile, boolean compress)
			throws Exception {
		FSDataInputStream fis = fs.open(new Path(fromfile));
		FormatDataFile fdf = createfdf(filename, compress);
		for (int i = 0; i < 10000 * 10000; i++) {
			Record record = readrecord(fis);
			if (record == null) {
				break;
			}
			fdf.addRecord(record);
			if (i % 1000000 == 0) {
			}
		}
		fdf.close();
		fis.close();
	}

	static void testwritecolumn(String filename, String fromfile,
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

	static void testreadseq(String filename, int num, boolean compress)
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

	static void testreadrand(String filename, int num, int size)
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

	static void testreadcolumnseq(String filename, int num, boolean compress,
			String mode) throws Exception {

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

	static void testreadcolumnrand(String filename, int num, int size,
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

	private static FormatDataFile createfdf(String filename, boolean compress)
			throws Exception {
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
		if (compress) {
			head.setCompress((byte) 1);
			head.setCompressStyle(ConstVar.LZOCompress);
		}

		Configuration conf = new Configuration();
		FormatDataFile fd = new FormatDataFile(conf);
		fd.create(filename, head);
		return fd;

	}

	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.out
					.println("input cmd param: [ writeraw | writerawr | writefdf | writecolumn | readseq | readrand | readcolumnseq | readcolumnrand ]");
			return;
		}
		String cmd = args[0];
		long time = System.currentTimeMillis();
		int recordnum = 0;
		if (cmd.equals("writeraw")) {
			if (args.length < 3) {
				System.out.println("input param: filename recordnum\t");
				return;
			}
			String filename = args[1];
			recordnum = Integer.valueOf(args[2]);
			testgenrawfile(filename, recordnum);
		} else if (cmd.equals("writerawr")) {
			if (args.length < 3) {
				System.out.println("input param: filename recordnum\t");
				return;
			}
			String filename = args[1];
			recordnum = Integer.valueOf(args[2]);
			testgenrawfiler(filename, recordnum);

		} else if (cmd.equals("writefdf")) {

			if (args.length < 3) {
				System.out
						.println("input param: filename fromfile [compress]\t");
				return;
			}
			String filename = args[1];
			String fromfile = args[2];
			boolean compress = false;
			if (args.length >= 4 && args[3].equals("compress")) {
				compress = true;
			}
			testwritefdf(filename, fromfile, compress);

		} else if (cmd.equals("writecolumn")) {
			if (args.length < 3) {
				System.out
						.println("input param: filename fromfile [compress]\t");
				return;
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
				return;
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
				return;
			}
			String filename = args[1];
			recordnum = Integer.valueOf(args[2]);
			int size = Integer.valueOf(args[3]);
			testreadrand(filename, recordnum, size);
		} else if (cmd.equals("readcolumnseq")) {
			if (args.length < 3) {
				System.out.println("input param: filename number");
				return;
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
				return;
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

		FileWriter fw = new FileWriter("outptfile.txt");

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
		fw.close();
		System.out.println(sb.toString());
		System.out.println(runtime);

	}

}
