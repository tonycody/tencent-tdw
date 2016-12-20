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
import java.util.List;

import junit.framework.TestCase;

import Comm.ConstVar;
import Comm.Util;
import FormatStorage.Unit.FieldValue;
import FormatStorage.Unit.Record;
import IndexService.Indexer;

public class TestIndexer extends TestCase {
	String datadir = "datadir";
	String indexdir = "indexdir";
	int filenum = 5;
	int recnum = 10000;
	boolean var = false;
	boolean compress = false;
	boolean seq = true;
	boolean overwrite = true;
	boolean column = false;
	boolean removefile = false;
	ArrayList<FieldValue> startvalue = new ArrayList<FieldValue>();
	Indexer indexer = new Indexer();

	public void testIndexer() throws Exception {
		String datadir = "datadir";
		String indexdir = "indexdir";
		int filenum = 5;
		int recnum = 10000;
		boolean var = false;
		boolean compress = false;
		boolean seq = true;
		boolean overwrite = true;
		boolean column = false;
		boolean removefile = false;
		UtilIndexMR.indexmrtest(datadir, indexdir + "/nopart", filenum, recnum,
				var, compress, seq, overwrite, column, "2", removefile);

		Indexer indexer = new Indexer();
		ArrayList<String> parts = null;
		ArrayList<FieldValue> values = new ArrayList<FieldValue>();
		values.add(new FieldValue(100, (short) 2));
		ArrayList<FieldValue> values1 = new ArrayList<FieldValue>();
		values1.add(new FieldValue(110, (short) 2));
		String indexids = "2,3,4";
		int num = -1;

		System.out
				.println("------------- i am the hualide split line -----------------");
		List<Record> recs = indexer.get(indexdir, parts, values, num, indexids,
				7);
		for (Record record : recs) {
			show(record);
		}
		System.out
				.println("------------- i am the hualide split line -----------------");
		List<Record> recs1 = indexer.getRange(indexdir, parts, values, values1,
				num, indexids, 7);
		for (Record record : recs1) {
			show(record);
		}
		System.out
				.println("------------- i am the hualide split line -----------------");

		indexer = new Indexer(indexdir, parts, values, indexids, 7);
		while (indexer.hasNext()) {
			show(indexer.next());
		}
		indexer.close();
		System.out
				.println("------------- i am the hualide split line -----------------");

		indexer = new Indexer(indexdir, parts, values, values, indexids, 7);
		while (indexer.hasNext()) {
			show(indexer.next());
		}
		indexer.close();
		System.out
				.println("------------- i am the hualide split line -----------------");

		indexer = new Indexer(indexdir, parts, values, values1, indexids, 7);
		while (indexer.hasNext()) {
			show(indexer.next());
		}
		indexer.close();

		startvalue.clear();
		startvalue.add(new FieldValue((int) 100, (short) -1));
		System.out.println(indexer.recordsnum(indexdir, null, startvalue));
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
				str = new String(fv.value());
			}
			System.out.print(str + "\t");
		}
		System.out.println();
	}

	public static void main(String[] args) throws Exception {
		String datadir = "datadir";
		String indexdir = "indexdir";
		int filenum = 5;
		int recnum = 100000;
		boolean var = true;
		boolean compress = false;
		boolean seq = true;
		boolean overwrite = true;
		boolean column = false;
		boolean removefile = false;
		UtilIndexMR.indexmrtest(datadir, indexdir + "/nopart", filenum, recnum,
				var, compress, seq, overwrite, column, "2,6", removefile);

		ArrayList<FieldValue> startvalue = new ArrayList<FieldValue>();
		startvalue.add(new FieldValue(1000, (short) -1));
		startvalue.add(new FieldValue("aaazzz", (short) -1));

		ArrayList<FieldValue> endvalue = new ArrayList<FieldValue>();
		endvalue.add(new FieldValue(1010, (short) -1));
		endvalue.add(new FieldValue("aaazzz", (short) -1));


		Indexer indexer = new Indexer(indexdir, null, startvalue, endvalue,
				"4,1,5,3", 7);
		while (indexer.hasNext()) {
			show(indexer.next());
		}
		indexer.close();


	}
}
