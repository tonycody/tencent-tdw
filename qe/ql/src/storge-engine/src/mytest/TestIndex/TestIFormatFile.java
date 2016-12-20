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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;

import Comm.ConstVar;
import IndexStorage.IFieldMap;
import IndexStorage.IFieldType;
import IndexStorage.IFieldValue;
import IndexStorage.IFormatDataFile;
import IndexStorage.IHead;
import IndexStorage.IRecord;
import IndexStorage.IUserDefinedHeadInfo;

public class TestIFormatFile {

	static void writetest(String fileName, int num, boolean var,
			boolean compress) throws IOException {
		long time = System.currentTimeMillis();
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

		head.setPrimaryIndex((short) 2);
		head.setCompress((byte) (compress ? 1 : 0));
		head.setCompressStyle(ConstVar.LZOCompress);

		IUserDefinedHeadInfo udi = new IUserDefinedHeadInfo();
		udi.addInfo(0, fileName);

		fdf.create(fileName, head);
		for (int i = 0; i < num; i++) {
			IRecord record = new IRecord();
			record.addFieldValue(new IFieldValue((byte) i));
			record.addFieldValue(new IFieldValue((short) (2 * i)));
			record.addFieldValue(new IFieldValue(3 * i));
			record.addFieldValue(new IFieldValue((long) 4 * i));
			record.addFieldValue(new IFieldValue((float) 5 * i));
			record.addFieldValue(new IFieldValue((double) 6 * i, (short) 5));
			if (var) {
				StringBuffer sb = new StringBuffer(100);
				for (int j = 0; j < i % 100 + 1; j++) {
					sb.append("a");
				}
				record.addFieldValue(new IFieldValue(sb.toString()));
			}

			fdf.addRecord(record);
			if (i % 1000000 == 0)
				System.out.println(i);
		}
		fdf.close();
		System.out.println(num + "\trecords-->\twritetime:\t"
				+ (System.currentTimeMillis() - time));
	}

	private static void verify(String filename, int num, int checktime)
			throws IOException {
		boolean flag = false;
		Random r = new Random();
		Configuration conf = new Configuration();
		IFormatDataFile fdf = new IFormatDataFile(conf);
		fdf.open(filename);
		for (int i = 0; i < checktime; i++) {
			int pos = r.nextInt(num);
			IRecord record = gentestrecord(pos);
			fdf.seek(pos);
			IRecord rec = fdf.next();
			if (!compare(record, rec)) {
				flag = true;
				System.out.println("a error:\t" + pos);
				record.show();
				rec.show();
			}
		}
		fdf.close();
		if (flag) {
			System.out.println("has error");
		} else {
			System.out.println("no error");
		}
	}

	private static boolean compare(IRecord record, IRecord rec) {
		if (record.fieldValues().size() != rec.fieldValues().size())
			return false;
		for (short i = 0; i < record.fieldValues().size(); i++) {
			if (record.fieldValues().get(i).compareTo(rec.fieldValues().get(i)) != 0)
				return false;
		}
		return true;
	}

	private static IRecord gentestrecord(int i) {
		IRecord record = new IRecord();
		record.addFieldValue(new IFieldValue((byte) i));
		record.addFieldValue(new IFieldValue((short) (2 * i)));
		record.addFieldValue(new IFieldValue(3 * i));
		record.addFieldValue(new IFieldValue((long) 4 * i));
		record.addFieldValue(new IFieldValue((float) 5 * i));
		record.addFieldValue(new IFieldValue((double) 6 * i));
		return record;
	}

	static void readtestshell(String fileName) throws IOException {
		Configuration conf = new Configuration();
		IFormatDataFile fdf = new IFormatDataFile(conf);
		fdf.open(fileName);

		IRecord record = new IRecord();
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		while (true) {
			try {
				System.out.println("input param: [seekrowid] [num]");
				StringTokenizer st = new StringTokenizer(br.readLine());
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
				System.out.println("seektime:\t"
						+ (System.currentTimeMillis() - time));
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

	static void headtest(String fileName) throws IOException {
		Configuration conf = new Configuration();
		IFormatDataFile fdf = new IFormatDataFile(conf);
		fdf.open(fileName);
		fdf.next();

	}

	static void randomreadtest(String fileName, int num) throws IOException {
		Configuration conf = new Configuration();
		IFormatDataFile fdf = new IFormatDataFile(conf);
		fdf.open(fileName);

		IRecord record = new IRecord();
		int size = fdf.recnum();
		if (size == -1) {
			System.out.println("can not seek");
			return;
		}
		for (int i = 0; i < num; i++) {
			int seekpos = (int) (Math.random() * size);
			long time = System.currentTimeMillis();
			fdf.seek(seekpos);
			fdf.next(record);
			System.out.println((System.currentTimeMillis() - time));
		}
	}

	static void seekfvtest(String fileName, int start, int step, int num)
			throws IOException {
		Configuration conf = new Configuration();
		IFormatDataFile fdf = new IFormatDataFile(conf);
		fdf.open(fileName);

		IRecord record = new IRecord();
		for (int i = 0; i < num; i++) {
			IFieldValue fv = new IFieldValue((short) 345);
			if (fdf.seek(fv)) {
				fdf.next(record);
				record.show();
				fdf.next(record);
				record.show();
				fdf.next(record);
				record.show();
				fdf.next(record);
				record.show();
				fdf.next(record);
				record.show();
				fdf.next(record);
				record.show();
				fdf.next(record);
				record.show();
				fdf.next(record);
				record.show();
				fdf.next(record);
				record.show();
			}
		}

	}

	static void test() throws IOException {
		String filename = "/testtesttest";
		int num = 10000000;
		int checktime = 10000;
		writetest(filename, num, false, false);
		verify(filename, num, checktime);
	}

	static void test1() throws IOException {
		String fileName = "/testtesttest2";

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

		head.setFieldMap(fieldMap);

		head.setPrimaryIndex((short) 2);

		IUserDefinedHeadInfo udi = new IUserDefinedHeadInfo();
		udi.addInfo(0, fileName);

		fdf.create(fileName, head);
		IRecord record = new IRecord();
		int i = 100;
		record.addFieldValue(new IFieldValue((byte) i));
		record.addFieldValue(new IFieldValue((short) (2 * i)));
		record.addFieldValue(new IFieldValue(3 * i));
		record.addFieldValue(new IFieldValue((long) 4 * i));

		fdf.addRecord(record);
		fdf.close();

	}

	public static void main(String[] args) throws Exception {
		String fileName = "indexdir/part-00000";
		String fileName1 = "/testtesttest2";
		String file = "hdfs://172.25.38.253:54310/user/tdw/warehouse/index/default_db/tcssformat/pv1/nopart/indexfile0";
		int num = 1000000;
		readtestshell(fileName);

	}
}
