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

import IndexStorage.IFieldValue;
import IndexStorage.IFileInfo;
import IndexStorage.ISegmentIndex;

public class TestISegmentIndex extends TestCase {

	public void testISegmentIndex() throws IOException {
		IFileInfo fileInfo = TestUtil.genfileinfo(false, 2);
		ISegmentIndex index = new ISegmentIndex(fileInfo);

		index.update(TestUtil.genseginfo(fileInfo, 0, 2));
		index.update(TestUtil.genseginfo(fileInfo, 1, 2));
		index.update(TestUtil.genseginfo(fileInfo, 2, 2));
		index.update(TestUtil.genseginfo(fileInfo, 3, 2));
		index.update(TestUtil.genseginfo(fileInfo, 4, 2));

		assertEquals(5, index.getSegnum());

		assertEquals(0, index.getSegid(0));
		assertEquals(0, index.getSegid(3));
		assertEquals(1, index.getSegid(4));
		assertEquals(1, index.getSegid(7));
		assertEquals(4, index.getSegid(19));
		assertEquals(5, index.getSegid(20));

		assertEquals(0, index.getSegid(new IFieldValue(0)));
		assertEquals(0, index.getSegid(new IFieldValue(3)));
		assertEquals(1, index.getSegid(new IFieldValue(4)));
		assertEquals(1, index.getSegid(new IFieldValue(7)));
		assertEquals(4, index.getSegid(new IFieldValue(19)));
		assertEquals(5, index.getSegid(new IFieldValue(20)));

		fileInfo.out().close();

	}

	public void testPersistable() throws IOException {
		IFileInfo fileInfo = TestUtil.genfileinfo(true, 2);
		ISegmentIndex index = new ISegmentIndex(fileInfo);

		index.update(TestUtil.genseginfo(fileInfo, 0, 2));
		index.update(TestUtil.genseginfo(fileInfo, 1, 2));
		index.update(TestUtil.genseginfo(fileInfo, 2, 2));
		index.update(TestUtil.genseginfo(fileInfo, 3, 2));
		index.update(TestUtil.genseginfo(fileInfo, 4, 2));

		DataOutputBuffer dob = new DataOutputBuffer();
		index.persistent(dob);
		byte[] data = dob.getData();

		DataInputBuffer dib = new DataInputBuffer();
		dib.reset(data, data.length);

		ISegmentIndex index2 = new ISegmentIndex(fileInfo);
		index2.unpersistent(dib);

		assertEquals(5, index2.getSegnum());

		assertEquals(0, index2.getSegid(0));
		assertEquals(0, index2.getSegid(3));
		assertEquals(1, index2.getSegid(4));
		assertEquals(1, index2.getSegid(7));
		assertEquals(4, index2.getSegid(19));
		assertEquals(5, index2.getSegid(20));

		assertEquals(0, index2.getSegid(new IFieldValue(0)));
		assertEquals(0, index2.getSegid(new IFieldValue(3)));
		assertEquals(1, index2.getSegid(new IFieldValue(4)));
		assertEquals(1, index2.getSegid(new IFieldValue(7)));
		assertEquals(4, index2.getSegid(new IFieldValue(19)));
		assertEquals(5, index2.getSegid(new IFieldValue(20)));

	}

}
