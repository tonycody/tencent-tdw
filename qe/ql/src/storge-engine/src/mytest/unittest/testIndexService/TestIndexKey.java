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

import IndexService.IndexKey;
import IndexStorage.IFieldValue;
import junit.framework.TestCase;

public class TestIndexKey extends TestCase {

	public void testWritable() throws IOException {
		IndexKey key = new IndexKey();
		IFieldValue ifv = new IFieldValue(100);
		key.addfv(ifv);

		DataOutputBuffer dob = new DataOutputBuffer();
		key.write(dob);
		byte[] data = dob.getData();
		DataInputBuffer dib = new DataInputBuffer();
		dib.reset(data, data.length);

		IndexKey key2 = new IndexKey();
		key2.readFields(dib);

		IndexKey key3 = new IndexKey();
		key3.addfv(new IFieldValue(101));

		IndexKey key4 = new IndexKey();
		key4.addfv(new IFieldValue(99));

		assertTrue(key.compareTo(key2) == 0);
		assertTrue(key.compareTo(key3) < 0);
		assertTrue(key.compareTo(key4) > 0);

	}
}
