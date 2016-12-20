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

public class TestOpenFile {
	static Head head = new Head();

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

	static void writeFile(String filename, int recnum) throws Exception {

		FieldMap fieldMap = new FieldMap();
		fieldMap.addField(new Field(ConstVar.FieldType_Byte,
				ConstVar.Sizeof_Byte, (short) 1));
		fieldMap.addField(new Field(ConstVar.FieldType_Short,
				ConstVar.Sizeof_Short, (short) 3));
		fieldMap.addField(new Field(ConstVar.FieldType_Int,
				ConstVar.Sizeof_Int, (short) 5));
		fieldMap.addField(new Field(ConstVar.FieldType_Long,
				ConstVar.Sizeof_Long, (short) 7));
		fieldMap.addField(new Field(ConstVar.FieldType_Float,
				ConstVar.Sizeof_Float, (short) 9));
		fieldMap.addField(new Field(ConstVar.FieldType_Double,
				ConstVar.Sizeof_Double, (short) 11));
		fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short) 13));

		head.setFieldMap(fieldMap);

		Configuration conf = new Configuration();
		FormatDataFile fd = new FormatDataFile(conf);
		fd.create(filename, head);

		for (int i = 0; i < recnum; i++) {
			Record record = new Record((short) 7);
			record.addValue(new FieldValue((byte) (1 + i), (short) 0));
			record.addValue(new FieldValue((short) (2 + i), (short) 1));
			record.addValue(new FieldValue((int) (3 + i), (short) 2));
			record.addValue(new FieldValue((long) (4 + i), (short) 3));
			record.addValue(new FieldValue((float) (5.5 + i), (short) 4));
			record.addValue(new FieldValue((double) (6.6 + i), (short) 5));
			record.addValue(new FieldValue("hello konten" + i, (short) 6));
			fd.addRecord(record);
		}

		fd.close();
	}

	static void readtest(String filename, int len, int count) throws Exception {
		for (int i = 0; i < 20; i++) {
			getRecordByLine("/test1/6", (int) (Math.random() * 1000000));

		}

	}

	public static void main(String[] args) throws Exception {
		int[] counts = { 10, 100, 1000, 10000, 100000, 1000000, 10000000,
				100000000 };
		for (int i = 0; i < 20; i++) {
			getRecordByLine("/test1/7", (int) (Math.random() * 1000000));

		}

	}
}
