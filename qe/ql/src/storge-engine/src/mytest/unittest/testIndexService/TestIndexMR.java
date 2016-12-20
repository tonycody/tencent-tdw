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

import Comm.ConstVar;
import Comm.Util;
import FormatStorage.Unit.FieldValue;
import FormatStorage.Unit.Record;
import IndexService.Indexer;
import junit.framework.TestCase;

public class TestIndexMR extends TestCase {
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

	Indexer indexer = new Indexer();
	ArrayList<String> parts = null;
	ArrayList<FieldValue> values = new ArrayList<FieldValue>();

	ArrayList<FieldValue> values1 = new ArrayList<FieldValue>();
	String indexids = "2,3,4,0,1,5";

	public void testIndexMR_Format_Byte() throws Exception {

		UtilIndexMR.indexmrtest(datadir, indexdir + "/nopart", filenum, recnum,
				var, compress, seq, overwrite, column, "0", removefile);
		values.clear();
		values.add(new FieldValue((byte) 100, (short) -1));
		List<Record> recs = indexer.get(indexdir, parts, values, -1, indexids,
				7);
		for (Record record : recs) {
			show(record);
		}
	}

	public void testIndexMR_Format_Short() throws Exception {
		UtilIndexMR.indexmrtest(datadir, indexdir + "/nopart", filenum, recnum,
				var, compress, seq, overwrite, column, "1", removefile);
		values.clear();
		values.add(new FieldValue((short) 100, (short) -1));
		List<Record> recs = indexer.get(indexdir, parts, values, -1, indexids,
				7);
		for (Record record : recs) {
			show(record);
		}
	}

	public void testIndexMR_Format_Int() throws Exception {
		UtilIndexMR.indexmrtest(datadir, indexdir + "/nopart", filenum, recnum,
				var, compress, seq, overwrite, column, "2", removefile);
		values.clear();
		values.add(new FieldValue((int) 100, (short) -1));
		List<Record> recs = indexer.get(indexdir, parts, values, -1, indexids,
				7);
		for (Record record : recs) {
			show(record);
		}
	}

	public void testIndexMR_Format_Long() throws Exception {
		UtilIndexMR.indexmrtest(datadir, indexdir + "/nopart", filenum, recnum,
				var, compress, seq, overwrite, column, "3", removefile);
		values.clear();
		values.add(new FieldValue((long) 100, (short) -1));
		List<Record> recs = indexer.get(indexdir, parts, values, -1, indexids,
				7);
		for (Record record : recs) {
			show(record);
		}
	}

	public void testIndexMR_Format_Float() throws Exception {
		UtilIndexMR.indexmrtest(datadir, indexdir + "/nopart", filenum, recnum,
				var, compress, seq, overwrite, column, "4", removefile);
		values.clear();
		values.add(new FieldValue((float) 100, (short) -1));
		List<Record> recs = indexer.get(indexdir, parts, values, -1, indexids,
				7);
		for (Record record : recs) {
			show(record);
		}
	}

	public void testIndexMR_Format_Double() throws Exception {
		UtilIndexMR.indexmrtest(datadir, indexdir + "/nopart", filenum, recnum,
				var, compress, seq, overwrite, column, "5", removefile);

		values.clear();
		values.add(new FieldValue((double) 100, (short) -1));
		List<Record> recs = indexer.get(indexdir, parts, values, -1, indexids,
				7);
		for (Record record : recs) {
			show(record);
		}
	}

	public void testIndexMR_Format_String() throws Exception {
		UtilIndexMR.indexmrtest(datadir, indexdir + "/nopart", filenum, recnum,
				true, compress, seq, overwrite, column, "6", removefile);

		values.clear();
		values.add(new FieldValue("aaaabb", (short) -1));
		List<Record> recs = indexer.get(indexdir, parts, values, -1, indexids,
				7);
		for (Record record : recs) {
			show(record);
		}
	}

	public void testIndexMR_Format_Int_Compress() throws Exception {
	}

	public void testIndexMR_Column_Byte() throws Exception {
		UtilIndexMR.indexmrtest(datadir, indexdir + "/nopart", filenum, recnum,
				var, compress, seq, overwrite, true, "0", removefile);
		values.clear();
		values.add(new FieldValue((byte) 100, (short) -1));
		List<Record> recs = indexer.get(indexdir, parts, values, -1, indexids,
				7);
		for (Record record : recs) {
			show(record);
		}

	}

	public void testIndexMR_Column_Short() throws Exception {
		UtilIndexMR.indexmrtest(datadir, indexdir + "/nopart", filenum, recnum,
				var, compress, seq, overwrite, true, "1", removefile);
		values.clear();
		values.add(new FieldValue((short) 100, (short) -1));
		List<Record> recs = indexer.get(indexdir, parts, values, -1, indexids,
				7);
		for (Record record : recs) {
			show(record);
		}

	}

	public void testIndexMR_Column_Int() throws Exception {
		UtilIndexMR.indexmrtest(datadir, indexdir + "/nopart", filenum, recnum,
				var, compress, seq, overwrite, true, "2", removefile);

		values.clear();
		values.add(new FieldValue((int) 100, (short) -1));
		List<Record> recs = indexer.get(indexdir, parts, values, -1, indexids,
				7);
		for (Record record : recs) {
			show(record);
		}
	}

	public void testIndexMR_Column_Long() throws Exception {
		UtilIndexMR.indexmrtest(datadir, indexdir + "/nopart", filenum, recnum,
				var, compress, seq, overwrite, true, "3", removefile);
		values.clear();
		values.add(new FieldValue((long) 100, (short) -1));
		List<Record> recs = indexer.get(indexdir, parts, values, -1, indexids,
				7);
		for (Record record : recs) {
			show(record);
		}

	}

	public void testIndexMR_Column_Float() throws Exception {
		UtilIndexMR.indexmrtest(datadir, indexdir + "/nopart", filenum, recnum,
				var, compress, seq, overwrite, true, "4", removefile);
		values.clear();
		values.add(new FieldValue((float) 100, (short) -1));
		List<Record> recs = indexer.get(indexdir, parts, values, -1, indexids,
				7);
		for (Record record : recs) {
			show(record);
		}

	}

	public void testIndexMR_Column_Double() throws Exception {
		UtilIndexMR.indexmrtest(datadir, indexdir + "/nopart", filenum, recnum,
				var, compress, seq, overwrite, true, "5", removefile);
		values.clear();
		values.add(new FieldValue((double) 100, (short) -1));
		List<Record> recs = indexer.get(indexdir, parts, values, -1, indexids,
				7);
		for (Record record : recs) {
			show(record);
		}

	}

	public void testIndexMR_Column_String() throws Exception {
		UtilIndexMR.indexmrtest(datadir, indexdir + "/nopart", filenum, recnum,
				true, compress, seq, overwrite, true, "6", removefile);
		values.clear();
		values.add(new FieldValue("aaaabb", (short) -1));
		List<Record> recs = indexer.get(indexdir, parts, values, -1, indexids,
				7);
		for (Record record : recs) {
			show(record);
		}

	}

	public void testIndexMR_Format_Byte_Short() throws Exception {
		UtilIndexMR.indexmrtest(datadir, indexdir + "/nopart", filenum, recnum,
				true, compress, seq, overwrite, column, "0,1", removefile);
		values.clear();
		values.add(new FieldValue((byte) 100, (short) -1));
		values.add(new FieldValue((short) 100, (short) -1));
		List<Record> recs = indexer.get(indexdir, parts, values, -1, indexids,
				7);
		for (Record record : recs) {
			show(record);
		}
	}

	public void testIndexMR_Format_Int_Short() throws Exception {
		UtilIndexMR.indexmrtest(datadir, indexdir + "/nopart", filenum, recnum,
				true, compress, seq, overwrite, column, "2,1", removefile);
		values.clear();
		values.add(new FieldValue((int) 100, (short) -1));
		values.add(new FieldValue((short) 100, (short) -1));
		List<Record> recs = indexer.get(indexdir, parts, values, -1, indexids,
				7);
		for (Record record : recs) {
			show(record);
		}

	}

	public void testIndexMR_Format_Int_String() throws Exception {
		UtilIndexMR.indexmrtest(datadir, indexdir + "/nopart", filenum, recnum,
				true, compress, seq, overwrite, column, "2,6", removefile);
		values.clear();
		values.add(new FieldValue((int) 100, (short) -1));
		values.add(new FieldValue("aaaabb", (short) -1));
		List<Record> recs = indexer.get(indexdir, parts, values, -1, indexids,
				7);
		for (Record record : recs) {
			show(record);
		}

	}

	public void testIndexMR_Format_Int_String_Double() throws Exception {
		UtilIndexMR.indexmrtest(datadir, indexdir + "/nopart", filenum, recnum,
				true, compress, seq, overwrite, column, "2,6,5", removefile);
		values.clear();
		values.add(new FieldValue((int) 100, (short) -1));
		values.add(new FieldValue("aaaabb", (short) -1));
		values.add(new FieldValue((double) 100, (short) -1));
		List<Record> recs = indexer.get(indexdir, parts, values, -1, indexids,
				7);
		for (Record record : recs) {
			show(record);
		}

	}

	public void testIndexMR_Column_Byte_Short() throws Exception {
		UtilIndexMR.indexmrtest(datadir, indexdir + "/nopart", filenum, recnum,
				true, compress, seq, overwrite, true, "0,1", removefile);
		values.clear();
		values.add(new FieldValue((byte) 100, (short) -1));
		values.add(new FieldValue((short) 100, (short) -1));
		List<Record> recs = indexer.get(indexdir, parts, values, -1, indexids,
				7);
		for (Record record : recs) {
			show(record);
		}

	}

	public void testIndexMR_Column_Int_Short() throws Exception {
		UtilIndexMR.indexmrtest(datadir, indexdir + "/nopart", filenum, recnum,
				true, compress, seq, overwrite, true, "2,1", removefile);
		values.clear();
		values.add(new FieldValue((int) 100, (short) -1));
		values.add(new FieldValue((short) 100, (short) -1));
		List<Record> recs = indexer.get(indexdir, parts, values, -1, indexids,
				7);
		for (Record record : recs) {
			show(record);
		}

	}

	public void testIndexMR_Column_Int_String() throws Exception {
		UtilIndexMR.indexmrtest(datadir, indexdir + "/nopart", filenum, recnum,
				true, compress, seq, overwrite, true, "2,6", removefile);
		values.clear();
		values.add(new FieldValue((int) 100, (short) -1));
		values.add(new FieldValue("aaaabb", (short) -1));
		List<Record> recs = indexer.get(indexdir, parts, values, -1, indexids,
				7);
		for (Record record : recs) {
			show(record);
		}

	}

	public void testIndexMR_Column_Int_String_Double() throws Exception {
		UtilIndexMR.indexmrtest(datadir, indexdir + "/nopart", filenum, recnum,
				true, compress, seq, overwrite, true, "2,6,5", removefile);
		values.clear();
		values.add(new FieldValue((int) 100, (short) -1));
		values.add(new FieldValue("aaaabb", (short) -1));
		values.add(new FieldValue((double) 100, (short) -1));
		List<Record> recs = indexer.get(indexdir, parts, values, -1, indexids,
				7);
		for (Record record : recs) {
			show(record);
		}
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


		new TestIndexMR().testIndexMR_Format_String();

	}
}
