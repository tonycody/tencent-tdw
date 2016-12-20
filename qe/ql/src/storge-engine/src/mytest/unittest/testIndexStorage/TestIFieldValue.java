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

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import junit.framework.TestCase;
import Comm.ConstVar;
import Comm.Util;
import IndexStorage.IFieldType;
import IndexStorage.IFieldValue;

public class TestIFieldValue extends TestCase {
	public void testIFieldValue() {
		IFieldType ift = new IFieldType(ConstVar.FieldType_Byte);
		IFieldValue ifv = new IFieldValue(ift);
		assertEquals(ConstVar.FieldType_Byte, ifv.type());

		ifv = new IFieldValue((byte) 3);
		assertEquals(3, ifv.value()[0]);

		ifv = new IFieldValue((short) 3);
		assertEquals(3, Util.bytes2short(ifv.value(), 0, ConstVar.Sizeof_Short));

		ifv = new IFieldValue((int) 3);
		assertEquals(3, Util.bytes2int(ifv.value(), 0, ConstVar.Sizeof_Int));

		ifv = new IFieldValue((long) 3);
		assertEquals(3, Util.bytes2long(ifv.value(), 0, ConstVar.Sizeof_Long));

		ifv = new IFieldValue((float) 3.14159);
		assertTrue(Math.abs(3.14159 - Util.bytes2float(ifv.value(), 0)) / 3.14159 < Math
				.pow(10, -6));

		ifv = new IFieldValue((double) 3.14159);
		assertTrue(Math.abs(3.14159 - Util.bytes2double(ifv.value(), 0)) / 3.14159 < Math
				.pow(10, -108));

		ifv = new IFieldValue("aaaa");
		assertTrue("aaaa".equals(new String(ifv.value())));

	}

	public void testWritable() throws IOException {

		IFieldValue ifv = new IFieldValue(100);

		DataOutputBuffer dob = new DataOutputBuffer();
		ifv.write(dob);
		byte[] data = dob.getData();
		DataInputBuffer dib = new DataInputBuffer();
		dib.reset(data, data.length);

		IFieldValue ifv2 = new IFieldValue();
		ifv2.readFields(dib);

		assertEquals(ifv.idx(), ifv2.idx());
		assertEquals(ifv.len(), ifv2.len());
		assertEquals(ifv.type(), ifv2.type());
		assertTrue(ifv.compareTo(ifv2) == 0);

	}

	public void testPersitable() throws IOException {
		IFieldValue ifv = new IFieldValue(100);

		DataOutputBuffer dob = new DataOutputBuffer();
		ifv.persistent(dob);

		byte[] data = dob.getData();

		DataInputBuffer dib = new DataInputBuffer();
		dib.reset(data, data.length);

		IFieldValue ifv2 = new IFieldValue(ifv.fieldType());
		ifv2.unpersistent(dib);

		assertEquals(ifv.idx(), ifv2.idx());
		assertEquals(ifv.len(), ifv2.len());
		assertEquals(ifv.type(), ifv2.type());
		assertTrue(ifv.compareTo(ifv2) == 0);

	}

}
