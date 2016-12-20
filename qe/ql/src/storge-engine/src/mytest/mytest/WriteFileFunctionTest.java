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

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;

import Comm.ConstVar;
import FormatStorage.FieldMap;
import FormatStorage.FormatDataFile;
import FormatStorage.Head;
import FormatStorage.FieldMap.Field;
import FormatStorage.Unit.FieldValue;
import FormatStorage.Unit.Record;

public class WriteFileFunctionTest {
	static void writeFile(String filename) throws Exception {
		Head head = new Head();
		FieldMap fieldMap = new FieldMap();

		fieldMap.addField(new Field(ConstVar.FieldType_Byte,
				ConstVar.Sizeof_Byte, (short) 0));
		fieldMap.addField(new Field(ConstVar.FieldType_Byte,
				ConstVar.Sizeof_Byte, (short) 1));
		fieldMap.addField(new Field(ConstVar.FieldType_Byte,
				ConstVar.Sizeof_Byte, (short) 2));
		fieldMap.addField(new Field(ConstVar.FieldType_Byte,
				ConstVar.Sizeof_Byte, (short) 3));
		fieldMap.addField(new Field(ConstVar.FieldType_Byte,
				ConstVar.Sizeof_Byte, (short) 4));
		fieldMap.addField(new Field(ConstVar.FieldType_Byte,
				ConstVar.Sizeof_Byte, (short) 5));
		fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short) 6));

		head.setFieldMap(fieldMap);

		Configuration conf = new Configuration();
		FormatDataFile fd = new FormatDataFile(conf);
		fd.create(filename, head);

		Record record = new Record((short) 7);
		record.addValue(new FieldValue((byte) (0), (short) 0));
		record.addValue(new FieldValue((byte) (1), (short) 1));
		record.addValue(new FieldValue((byte) (2), (short) 2));
		record.addValue(new FieldValue((byte) (3), (short) 3));
		record.addValue(new FieldValue((byte) (4), (short) 4));
		record.addValue(new FieldValue((byte) (5), (short) 5));
		record.addValue(new FieldValue("testtest", (short) 6));
		fd.addRecord(record);

		record = new Record((short) 7);
		record.addValue(new FieldValue((byte) (10), (short) 0));
		record.addValue(new FieldValue((byte) (11), (short) 1));
		record.addValue(new FieldValue((byte) (12), (short) 2));
		record.addValue(new FieldValue((byte) (13), (short) 3));
		record.addValue(new FieldValue((byte) (14), (short) 4));
		record.addValue(new FieldValue((byte) (15), (short) 5));
		record.addValue(new FieldValue("testtesttest", (short) 6));
		fd.addRecord(record);

		record = new Record((short) 7);
		record.addValue(new FieldValue((byte) (20), (short) 0));
		record.addValue(new FieldValue((byte) (21), (short) 1));
		record.addValue(new FieldValue((byte) (22), (short) 2));
		record.addValue(new FieldValue((byte) (23), (short) 3));
		record.addValue(new FieldValue((byte) (24), (short) 4));
		record.addValue(new FieldValue((byte) (25), (short) 5));
		record.addValue(new FieldValue("testttesttestest", (short) 6));
		fd.addRecord(record);

		fd.close();
	}

	static void getRecordByValue(String filename) throws Exception {
		Configuration conf = new Configuration();
		FormatDataFile fd3 = new FormatDataFile(conf);
		fd3.open(filename);

		FieldValue[] values = new FieldValue[1];
		values[0] = new FieldValue((byte) 1, (short) 1);

		Record[] records = fd3.getRecordByOrder(values, values.length);
		if (records != null) {
			System.out.println("should get null");
		}
		for (int j = 0; j < records.length; j++) {
			ArrayList<FieldValue> vals = records[j].fieldValues();
			for (int k = 0; k < vals.size(); k++) {
				System.out.print(vals.get(k).toObject() + "\t");
			}
			System.out.println();

		}

	}

	static void getRecordByLine(String filename, int line) throws Exception {
		Configuration conf = new Configuration();
		FormatDataFile fd2 = new FormatDataFile(conf);
		fd2.open(filename);
		Record record = fd2.getRecordByLine(-1);
		if (record != null) {
			System.out.println("should get null, line -1");
			fd2.close();
			return;
		}

		Record re = fd2.getRecordByLine(line);
		ArrayList<FieldValue> vals = re.fieldValues();
		for (int i = 0; i < vals.size(); i++) {
			System.out.print(vals.get(i).toObject() + "\t");
		}
		System.out.println();
	}

	public static void main(String[] args) throws Exception {
		String filename = "/test/func/write1";
		writeFile(filename);
		getRecordByLine(filename, 0);
		getRecordByLine(filename, 1);
		getRecordByLine(filename, 2);

		getRecordByValue(filename);
	}

}
