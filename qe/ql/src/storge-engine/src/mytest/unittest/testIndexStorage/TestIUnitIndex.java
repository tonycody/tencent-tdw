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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import IndexStorage.IFileInfo;
import IndexStorage.IUnitIndex;
import junit.framework.TestCase;

public class TestIUnitIndex extends TestCase {
	Configuration conf = new Configuration();

	public void testIUnitIndexWrite() throws IOException {
		IFileInfo fileInfo = TestUtil.genfileinfo(false, 2);

		IUnitIndex iui = new IUnitIndex(fileInfo);

		iui.update(TestUtil.genunitinfo(fileInfo, 0, 0, 5));
		iui.update(TestUtil.genunitinfo(fileInfo, 0, 1, 5));
		iui.update(TestUtil.genunitinfo(fileInfo, 0, 2, 5));
		iui.update(TestUtil.genunitinfo(fileInfo, 0, 3, 5));
		iui.update(TestUtil.genunitinfo(fileInfo, 0, 4, 5));

		System.out.println(iui.getUnitnum());

		assertEquals(0, iui.getUnitid(0));
		assertEquals(0, iui.getUnitid(4));
		assertEquals(1, iui.getUnitid(5));
		assertEquals(1, iui.getUnitid(9));
		assertEquals(4, iui.getUnitid(24));
		assertEquals(5, iui.getUnitid(25));

		DataOutputBuffer dob = new DataOutputBuffer();
		iui.persistent(dob);
		byte[] data = dob.getData();
		DataInputBuffer dib = new DataInputBuffer();
		dib.reset(data, data.length);

		IUnitIndex iui2 = new IUnitIndex(fileInfo);
		iui2.unpersistent(dib);
		for (int j = 0; j < iui.getUnitnum(); j++) {
			assertEquals(iui.getUnitOffset(j), iui2.getUnitOffset(j));
			assertTrue(iui.getKeyIndex(j).compareTo(iui2.getKeyIndex(j)) == 0);
			assertTrue(iui.getLineIndex(j).compareTo(iui2.getLineIndex(j)) == 0);
		}

	}

}
