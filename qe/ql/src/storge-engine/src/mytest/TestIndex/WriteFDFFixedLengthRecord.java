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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import Comm.ConstVar;
import FormatStorage.FieldMap;
import FormatStorage.FormatDataFile;
import FormatStorage.Head;
import FormatStorage.FieldMap.Field;
import FormatStorage.Unit.FieldValue;
import FormatStorage.Unit.Record;

public class WriteFDFFixedLengthRecord {
	public static void main(String[] args) throws Exception {
		FormatDataFile fdf = new FormatDataFile(new Configuration());
		String fileName = "/indextest/testfile1";
		FileSystem.get(new Configuration()).delete(new Path(fileName), true);
		Head head = new Head();
		FieldMap fieldMap = new FieldMap();
		fieldMap.addField(new Field(ConstVar.FieldType_Byte,
				ConstVar.Sizeof_Byte, (short) 0));
		fieldMap.addField(new Field(ConstVar.FieldType_Short,
				ConstVar.Sizeof_Byte, (short) 1));
		head.setFieldMap(fieldMap);
		head.setPrimaryIndex((short) 0);
		
		fdf.create(fileName, head);
		for (int i = 0; i < 200; i++) {
			Record record = new Record(2);
			record.addValue(new FieldValue((byte) i, (short) 0));
			record.addValue(new FieldValue((byte) i, (short) 1));
			fdf.addRecord(record);
		}
		fdf.close();
	}
}
