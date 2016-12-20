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

import IndexStorage.ILineIndex;
import junit.framework.TestCase;

public class TestILineIndex extends TestCase {
	public void testILineIndex() {
		ILineIndex ili = new ILineIndex(1, 1);
		assertEquals(1, ili.beginline());
		assertEquals(1, ili.endline());
	}

	public void testPersistable() throws IOException {
		ILineIndex ili = new ILineIndex(1, 1);

		DataOutputBuffer dob = new DataOutputBuffer();
		ili.persistent(dob);
		byte[] data = dob.getData();

		DataInputBuffer dib = new DataInputBuffer();
		dib.reset(data, data.length);

		ILineIndex ili2 = new ILineIndex();
		ili2.unpersistent(dib);

		assertEquals(ili.beginline(), ili2.beginline());
		assertEquals(ili.endline(), ili2.endline());
	}

	public void testComparable() {
		ILineIndex ili = new ILineIndex(1, 10);
		ILineIndex ili2 = new ILineIndex(1, 20);
		ILineIndex ili3 = new ILineIndex(10, 20);

		assertTrue(ili.compareTo(ili2) < 0);
		assertTrue(ili2.compareTo(ili3) == 0);

	}

}
