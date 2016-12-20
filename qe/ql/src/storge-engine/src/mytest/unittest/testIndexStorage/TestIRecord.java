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
import java.util.TreeMap;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import junit.framework.TestCase;
import Comm.ConstVar;
import IndexStorage.IFieldType;
import IndexStorage.IFieldValue;
import IndexStorage.IRecord;

public class TestIRecord extends TestCase {
	public void testIRecord() {
		IRecord irc = new IRecord();
		assertTrue(irc.addFieldValue(new IFieldValue(1)));

		irc = new IRecord();
		TreeMap<Short, IFieldType> fieldtypes = new TreeMap<Short, IFieldType>();
		fieldtypes.put((short) 0, new IFieldType(ConstVar.FieldType_Double));
		fieldtypes.put((short) 1, new IFieldType(ConstVar.FieldType_Int));
		irc.setFieldTypes(fieldtypes);
		assertTrue(!irc.addFieldValue(new IFieldValue((double) 4)));
		assertTrue(irc.addFieldValue(new IFieldValue((double) 4, (short) 0)));
		assertTrue(irc.addFieldValue(new IFieldValue(4, (short) 1)));

	}

	public void testPersistable() throws IOException {
		IRecord irc = new IRecord();
		TreeMap<Short, IFieldType> fieldtypes = new TreeMap<Short, IFieldType>();
		fieldtypes.put((short) 0, new IFieldType(ConstVar.FieldType_Double));
		fieldtypes.put((short) 1, new IFieldType(ConstVar.FieldType_Int));
		irc.setFieldTypes(fieldtypes);
		irc.addFieldValue(new IFieldValue((double) 4, (short) 0));
		irc.addFieldValue(new IFieldValue(4, (short) 1));

		DataOutputBuffer dob = new DataOutputBuffer();
		irc.persistent(dob);
		byte[] data = dob.getData();

		DataInputBuffer dib = new DataInputBuffer();
		dib.reset(data, data.length);

		IRecord irc2 = new IRecord();
		irc2.setFieldTypes(fieldtypes);
		irc2.unpersistent(dib);

		assertTrue(irc.fieldValues().get((short) 0).compareTo(
				irc2.fieldValues().get((short) 0)) == 0);
		assertTrue(irc.fieldValues().get((short) 1).compareTo(
				irc2.fieldValues().get((short) 1)) == 0);

	}

}
