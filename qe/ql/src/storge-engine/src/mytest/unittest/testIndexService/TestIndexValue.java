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

import junit.framework.TestCase;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import IndexService.IndexValue;

public class TestIndexValue extends TestCase {

	public void testIndexValue() throws IOException {

		IndexValue value = new IndexValue((short) 1, 2);

		DataOutputBuffer dob = new DataOutputBuffer();
		value.write(dob);
		byte[] data = dob.getData();
		DataInputBuffer dib = new DataInputBuffer();
		dib.reset(data, data.length);

		IndexValue value2 = new IndexValue((short) 1, 2);
		value2.readFields(dib);

		assertEquals(value.getFileindex(), value2.getFileindex());
		assertEquals(value.getRowid(), value2.getRowid());

	}
}
