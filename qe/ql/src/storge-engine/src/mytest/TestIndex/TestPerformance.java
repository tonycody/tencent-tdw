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
import IndexStorage.IFieldMap;
import IndexStorage.IFieldType;
import IndexStorage.IFieldValue;
import IndexStorage.IFormatDataFile;
import IndexStorage.IHead;
import IndexStorage.IRecord;
import IndexStorage.IUserDefinedHeadInfo;

public class TestPerformance {
	static FileSystem fs;
	static Configuration conf = new Configuration();
	static Random r;
	static {
		try {
			fs = FileSystem.get(conf);
			r = new Random();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void testFormatWrite(int recordnum) throws Exception {
		String filedir = "/se/test/fdf/fdf" + recordnum;
		int filenum = 1;
		long time = System.currentTimeMillis();
		genfdf(filedir, filenum, recordnum);
		System.out.println(System.currentTimeMillis() - time);
	}

	public static void testIFormatWrite(int recordnum) throws Exception {
		String filedir = "/se/test/fdf/ifdf" + recordnum;
		long time = System.currentTimeMillis();
		genifdf(filedir, recordnum, false);
		System.out.println(System.currentTimeMillis() - time);
	}

	public static void testFormatRead(int num) throws Exception {
		String filedir = "/se/test/fdf/fdf10000000";
		FormatDataFile fdf = new FormatDataFile(conf);
		fdf.open(filedir);

		long time = System.currentTimeMillis();
		for (int i = 0; i < num; i++) {
			Record rec = fdf.getRecordByLine(i);
		}
		System.out.println(System.currentTimeMillis() - time);
	}

	public static void testIFormatRead(int num) throws Exception {
		String filedir = "/se/test/fdf/ifdf10000000";
		IFormatDataFile ifdf = new IFormatDataFile(conf);
		ifdf.open(filedir);

		long time = System.currentTimeMillis();
		for (int i = 0; i < num; i++) {
			IRecord rec = ifdf.next();
		}
		System.out.println(System.currentTimeMillis() - time);
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
				str = String.valueOf(fv.value());
			}
			System.out.print(str + "\t");
		}
		System.out.println();
	}

	private static void genifdf(String fileName, int recordnum, boolean var)
			throws IOException {
		Configuration conf = new Configuration();
		IFormatDataFile fdf = new IFormatDataFile(conf);
		IHead head = new IHead();

		IFieldMap fieldMap = new IFieldMap();
		fieldMap.addFieldType(new IFieldType.IFieldByteType());
		fieldMap.addFieldType(new IFieldType.IFieldShortType());
		fieldMap.addFieldType(new IFieldType.IFieldIntType());
		fieldMap.addFieldType(new IFieldType.IFieldLongType());
		fieldMap.addFieldType(new IFieldType.IFieldFloatType());
		fieldMap.addFieldType(new IFieldType.IFieldDoubleType());
		if (var)
			fieldMap.addFieldType(new IFieldType.IFieldStringType());

		head.setFieldMap(fieldMap);


		IUserDefinedHeadInfo udi = new IUserDefinedHeadInfo();
		udi.addInfo(0, fileName);

		fdf.create(fileName, head);
		for (int i = 0; i < recordnum; i++) {
			IRecord record = new IRecord();
			record.addFieldValue(new IFieldValue((byte) r.nextInt()));
			record.addFieldValue(new IFieldValue((short) (r.nextInt())));
			record.addFieldValue(new IFieldValue(r.nextInt()));
			record.addFieldValue(new IFieldValue(r.nextLong()));
			record.addFieldValue(new IFieldValue(r.nextFloat()));
			record.addFieldValue(new IFieldValue(r.nextDouble()));
			if (var) {
				StringBuffer sb = new StringBuffer(100);
				for (int j = 0; j < i % 100 + 1; j++) {
					sb.append("a");
				}
				record.addFieldValue(new IFieldValue(sb.toString()));
			}

			fdf.addRecord(record);
			if ((i + 1) % 10000000 == 0)
				System.out.println(i + 1);
		}
		fdf.close();
	}

	private static void genfdf(String filedir, int filenum, int recordnum)
			throws Exception {

		int size = 0;
		FileStatus[] fss = fs.listStatus(new Path(filedir));
		if (fss != null) {
			size += fss.length;
		}

		for (int i = 0; i < filenum; i++) {
			System.err.println("generate a fdf");
			FormatDataFile fdf = createfdf(filedir, false, (short) -1);
			for (int j = 0; j < recordnum; j++) {
				Record record = new Record((short) 6);
				record.addValue(new FieldValue((byte) r.nextInt(), (short) 0));
				record.addValue(new FieldValue((short) r.nextInt(), (short) 1));
				record.addValue(new FieldValue(r.nextInt(), (short) 2));
				record.addValue(new FieldValue(r.nextLong(), (short) 3));
				record
						.addValue(new FieldValue(r.nextFloat() * 10000,
								(short) 4));
				record.addValue(new FieldValue(r.nextDouble() * 100000000,
						(short) 5));
				if ((j + 1) % 1000000 == 0)
					System.err.println((j + 1) + "records written");
				fdf.addRecord(record);
			}
			fdf.close();
		}
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

	public static void main(String[] args) throws Exception {

		testFormatRead(10000);
		testIFormatRead(10000);
		testFormatRead(100000);
		testIFormatRead(100000);
		testFormatRead(1000000);
		testIFormatRead(1000000);
		testFormatRead(10000000);
		testIFormatRead(10000000);
	}
}
