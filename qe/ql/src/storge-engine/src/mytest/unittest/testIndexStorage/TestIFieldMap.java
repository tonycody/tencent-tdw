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

import IndexStorage.IFieldMap;
import IndexStorage.IFieldType;
import junit.framework.TestCase;

public class TestIFieldMap extends TestCase {

	@Override
	protected void setUp() throws Exception {
	}

	public void testPersistable() throws IOException {
		IFieldMap fieldMap = new IFieldMap();
		fieldMap.addFieldType(new IFieldType.IFieldByteType());
		fieldMap.addFieldType(new IFieldType.IFieldShortType());
		fieldMap.addFieldType(new IFieldType.IFieldIntType());
		fieldMap.addFieldType(new IFieldType.IFieldLongType());
		fieldMap.addFieldType(new IFieldType.IFieldFloatType());
		fieldMap.addFieldType(new IFieldType.IFieldDoubleType());

		DataOutputBuffer dob = new DataOutputBuffer();
		fieldMap.persistent(dob);
		byte[] data = dob.getData();

		DataInputBuffer dib = new DataInputBuffer();
		dib.reset(data, data.length);

		IFieldMap fieldMap2 = new IFieldMap();
		fieldMap2.unpersistent(dib);

		System.out.println();
		assertEquals(fieldMap.fieldtypes().get((short) 0).getType(), fieldMap2
				.fieldtypes().get((short) 0).getType());
		assertEquals(fieldMap.fieldtypes().get((short) 1).getType(), fieldMap2
				.fieldtypes().get((short) 1).getType());
		assertEquals(fieldMap.fieldtypes().get((short) 2).getType(), fieldMap2
				.fieldtypes().get((short) 2).getType());
		assertEquals(fieldMap.fieldtypes().get((short) 3).getType(), fieldMap2
				.fieldtypes().get((short) 3).getType());
		assertEquals(fieldMap.fieldtypes().get((short) 4).getType(), fieldMap2
				.fieldtypes().get((short) 4).getType());
		assertEquals(fieldMap.fieldtypes().get((short) 5).getType(), fieldMap2
				.fieldtypes().get((short) 5).getType());

	}
}
