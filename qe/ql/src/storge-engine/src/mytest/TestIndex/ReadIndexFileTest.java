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

import FormatStorage.FormatDataFile;
import FormatStorage.Unit.FieldValue;
import FormatStorage.Unit.Record;

public class ReadIndexFileTest {

	static void readtest() throws Exception {
		String filename = "/tmp/output/part-00000";
		FormatDataFile fdf = new FormatDataFile(new Configuration());
		fdf.open(filename);
		for (int i = 0; i < 5000; i++) {
			Record rec = fdf.getRecordByLine(i);
			rec.show();
		}
		fdf.close();
	}

	static void readvaluetest() throws Exception {
		String filename = "/tmp/output/part-00000";
		FormatDataFile fdf = new FormatDataFile(new Configuration());
		fdf.open(filename);
		FieldValue[] values = new FieldValue[2];
		values[0] = new FieldValue((byte) 100, (short) 0);
		values[1] = new FieldValue((short) -4078, (short) 1);
		Record[] recs = fdf.getRecordByValue(values, 100);
		System.out.println(recs.length);
		for (int i = 0; i < recs.length; i++) {
			recs[i].show();
		}
	}

	public static void main(String[] args) throws Exception {
		readtest();
	}
}
