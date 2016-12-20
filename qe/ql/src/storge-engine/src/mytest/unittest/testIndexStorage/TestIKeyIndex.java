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

import IndexStorage.IFieldValue;
import IndexStorage.IKeyIndex;
import junit.framework.TestCase;

public class TestIKeyIndex extends TestCase {
	public void testIKeyIndex() {

		IFieldValue beginkey = new IFieldValue(100);
		IFieldValue endkey = new IFieldValue(1000);
		IKeyIndex iki = new IKeyIndex(beginkey, endkey, 100);

		assertTrue(iki.beginkey().compareTo(beginkey) == 0);
		assertTrue(iki.endkey().compareTo(endkey) == 0);
	}

	public void testPersistable() throws IOException {
		IFieldValue beginkey = new IFieldValue(100);
		IFieldValue endkey = new IFieldValue(1000);
		IKeyIndex iki = new IKeyIndex(beginkey, endkey, 100);

		DataOutputBuffer dob = new DataOutputBuffer();
		iki.persistent(dob);
		byte[] data = dob.getData();

		DataInputBuffer dib = new DataInputBuffer();
		dib.reset(data, data.length);

		IKeyIndex iki2 = new IKeyIndex(beginkey.fieldType());
		iki2.unpersistent(dib);

		assertTrue(iki.beginkey().compareTo(iki2.beginkey()) == 0);
		assertTrue(iki.endkey().compareTo(iki2.endkey()) == 0);
		assertEquals(iki.recnum(), iki2.recnum());
	}

	public void testComparable() {
		IFieldValue beginkey = new IFieldValue(100);
		IFieldValue endkey = new IFieldValue(1000);
		IKeyIndex iki = new IKeyIndex(beginkey, endkey, 100);

		IFieldValue beginkey2 = new IFieldValue(100);
		IFieldValue endkey2 = new IFieldValue(200);
		IKeyIndex iki2 = new IKeyIndex(beginkey2, endkey2, 100);

		IFieldValue beginkey3 = new IFieldValue(300);
		IFieldValue endkey3 = new IFieldValue(200);
		IKeyIndex iki3 = new IKeyIndex(beginkey3, endkey3, 100);

		assertTrue(iki.compareTo(iki2) > 0);
		assertTrue(iki2.compareTo(iki3) == 0);

	}

}
