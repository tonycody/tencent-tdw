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

import IndexStorage.IFileInfo;
import IndexStorage.ISegmentInfo;
import junit.framework.TestCase;

public class TestISegmentInfo extends TestCase {
	public void testISegmentInfo() throws IOException {
		IFileInfo fileInfo = TestUtil.genfileinfo(false, -1);
		ISegmentInfo info = new ISegmentInfo(fileInfo, 0);
		info.update(TestUtil.genunitinfo(fileInfo, 0, 0, 5));
		info.update(TestUtil.genunitinfo(fileInfo, 0, 1, 5));
		info.update(TestUtil.genunitinfo(fileInfo, 0, 2, 5));
		info.update(TestUtil.genunitinfo(fileInfo, 0, 3, 5));

		assertEquals(0, info.beginLine());
		assertEquals(19, info.endLine());

	}

}
