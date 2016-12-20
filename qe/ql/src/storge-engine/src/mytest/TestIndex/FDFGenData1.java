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

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import Comm.ConstVar;
import FormatStorage.FieldMap;
import FormatStorage.FormatDataFile;
import FormatStorage.Head;
import FormatStorage.FieldMap.Field;
import FormatStorage.Unit.FieldValue;
import FormatStorage.Unit.Record;
import mytest.PT;

public class FDFGenData1 {

	static void testwritefile(String testfiledir, int num) throws Exception {
		PT.testgenrawfiler(testfiledir + "raw/rawfile", num);
		PT.testwritefdf(testfiledir + "fdf/file1", testfiledir + "raw/rawfile",
				false, (short) -1);
		PT.testgenrawfiler(testfiledir + "raw/rawfile", num);
		PT.testwritefdf(testfiledir + "fdf/file2", testfiledir + "raw/rawfile",
				false, (short) -1);
		PT.testgenrawfiler(testfiledir + "raw/rawfile", num);
		PT.testwritefdf(testfiledir + "fdf/file3", testfiledir + "raw/rawfile",
				false, (short) -1);
		PT.testgenrawfiler(testfiledir + "raw/rawfile", num);
		PT.testwritefdf(testfiledir + "fdf/file4", testfiledir + "raw/rawfile",
				false, (short) -1);
		PT.testgenrawfiler(testfiledir + "raw/rawfile", num);
		PT.testwritefdf(testfiledir + "fdf/file5", testfiledir + "raw/rawfile",
				false, (short) -1);
	}

	public static void testwritefdf(String filename, boolean compress,
			short keyindex) throws Exception {
		FormatDataFile fdf = createfdf(filename, compress, keyindex);
		for (int i = 0; i < 200; i++) {
			Record record = new Record(6);
			record.addValue(new FieldValue((byte) i, (short) 0));
			record.addValue(new FieldValue((short) 100, (short) 1));
			record.addValue(new FieldValue((int) 1000, (short) 2));
			record.addValue(new FieldValue((long) 10000, (short) 3));
			record.addValue(new FieldValue((float) 1.5, (short) 4));
			record.addValue(new FieldValue((double) 1.55, (short) 5));

			fdf.addRecord(record);
			if (i % 1000000 == 0) {
			}
		}
		fdf.close();
	}

	private static FormatDataFile createfdf(String filename, boolean compress,
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

	static void testgenrawfile(FileSystem fs, String filename, int recordnum)
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

	static void writefdf(int recnum, int filenum, String dirname)
			throws Exception {
		Random r = new Random();
		String[] strs = { "alskdfjl", "lsjewedflkavjsldkj", "sdjflj", "sldjfl",
				"lsjdflkajsrwerldkj", "sfsdjflj", "sldfwedjfl",
				"lsjdflksajsldkj", "sdjfsdfsdfweelj", "sldsfdgwdjfl" };
		FormatDataFile[] fdfs = new FormatDataFile[filenum];
		for (int i = 0; i < filenum; i++) {
			fdfs[i] = PT.createfdf(dirname + "/" + i, false, (short) -1);
		}
		for (int i = 0; i < recnum; i++) {
			for (int j = 0; j < filenum; j++) {
				Record record = new Record(7);
				record.addValue(new FieldValue(
						(byte) (Math.random() * Byte.MAX_VALUE), (short) 0));
				record.addValue(new FieldValue(
						(short) (Math.random() * Short.MAX_VALUE), (short) 1));
				record.addValue(new FieldValue(
						(byte) (Math.random() * Integer.MAX_VALUE), (short) 2));
				record.addValue(new FieldValue(
						(byte) (Math.random() * Long.MAX_VALUE), (short) 3));
				record.addValue(new FieldValue(r.nextFloat(), (short) 4));
				record.addValue(new FieldValue(r.nextDouble(), (short) 5));
				record.addValue(new FieldValue(strs[(i * filenum + j)
						% strs.length], (short) 6));

				fdfs[j].addRecord(record);

			}
			System.out.println("a rec");
		}
		for (int i = 0; i < fdfs.length; i++) {
			fdfs[i].close();
		}

	}

	public static void main(String[] args) throws Exception {
		testwritefile(args[0], Integer.parseInt(args[1]));

	}
}
