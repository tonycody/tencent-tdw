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

import java.util.Random;

import org.apache.hadoop.conf.Configuration;

import Comm.ConstVar;
import FormatStorage.FieldMap;
import FormatStorage.FormatDataFile;
import FormatStorage.Head;
import FormatStorage.FieldMap.Field;
import FormatStorage.Unit.FieldValue;
import FormatStorage.Unit.Record;

public class TestFDF {
	static String str = "alksdjagkl;hs;lgadkfj laskjdflkaj;sldgjklaskjdflfkdsahhflaksjlfkweoj;lsjkdjfla;skjdflkajlskdjflkajslkdjflaksjldkfjlaksjldkf";
	static Random r = new Random();

	static void writefdf(String filename, int num) throws Exception {
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

		Configuration conf = new Configuration();
		FormatDataFile fd = new FormatDataFile(conf);
		fd.create(filename, head);

		for (int i = 0; i < num; i++) {
			Record record = new Record((short) 7);
			record.addValue(new FieldValue((byte) (1 + i), (short) 0));
			record.addValue(new FieldValue((short) (2 + i), (short) 1));
			record.addValue(new FieldValue((int) (3 + i), (short) 2));
			record.addValue(new FieldValue((long) (4 + i), (short) 3));
			record.addValue(new FieldValue((float) (5.5 + i), (short) 4));
			record.addValue(new FieldValue((double) (6.6 + i), (short) 5));
			record.addValue(new FieldValue(i
					+ " "
					+ str.substring(r.nextInt(str.length() / 2), str.length()
							/ 2 - 1 + r.nextInt(str.length() / 2)), (short) 6));
			fd.addRecord(record);
		}

		fd.close();

	}

	public static void main(String[] args) throws Exception {
		String file = "testtesttesttest";
		int num = 1000;
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

		Configuration conf = new Configuration();
		FormatDataFile fd = new FormatDataFile(conf);
		fd.create(file, head);
		Record record = new Record((short) 7);
		int i = 1;
		record.addValue(new FieldValue((byte) (1 + i), (short) 0));
		record.addValue(new FieldValue((short) (2 + i), (short) 1));
		record.addValue(new FieldValue((int) (3 + i), (short) 2));
		record.addValue(new FieldValue((long) (4 + i), (short) 3));
		record.addValue(new FieldValue((float) (5.5 + i), (short) 4));
		record.addValue(new FieldValue((double) (6.6 + i), (short) 5));
		record.addValue(new FieldValue(i
				+ " "
				+ str.substring(r.nextInt(str.length() / 2), str.length() / 2
						- 1 + r.nextInt(str.length() / 2)), (short) 6));

		fd.addRecord(record);
		fd.close();

		fd = new FormatDataFile(conf);

		fd.open(file);
		Record rec = fd.getRecordByLine(0);
		rec.show();
		fd.close();

	}

}
