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

import Comm.Util;
import IndexStorage.IFileInfo;
import IndexStorage.IUnitIndex;
import IndexStorage.IUnitInfo;
import junit.framework.TestCase;

public class TestIUnitInfo extends TestCase {
	public void testIUnitInfoWrite() throws IOException {
		IFileInfo fileInfo = TestUtil.genfileinfo(true, 2);

		IUnitInfo info = new IUnitInfo(fileInfo, 0, 0);

		info.update(TestUtil.genrecord(1, false));
		info.update(TestUtil.genrecord(2, false));
		info.update(TestUtil.genrecord(3, false));
		info.update(TestUtil.genrecord(4, false));
		info.update(TestUtil.genrecord(5, false));

		assertEquals(0, info.beginLine());
		assertEquals(4, info.endLine());
		assertEquals(1, Util.bytes2int(info.beginKey().value(), 0, 4));
		assertEquals(5, Util.bytes2int(info.endKey().value(), 0, 4));
		assertEquals(5, info.recordNum());

		info.setOffset(1000);
		assertEquals(1000, info.offset());

		fileInfo.out().close();

	}

	public void testIUnitInfoRead() throws IOException {

		IFileInfo fileInfo = TestUtil.genfileinfo(false, 2);
		IUnitIndex iui = new IUnitIndex(fileInfo);

		iui.update(TestUtil.genunitinfo(fileInfo, 0, 0, 5));
		iui.update(TestUtil.genunitinfo(fileInfo, 0, 1, 5));
		iui.update(TestUtil.genunitinfo(fileInfo, 0, 2, 5));
		iui.update(TestUtil.genunitinfo(fileInfo, 0, 3, 5));
		iui.update(TestUtil.genunitinfo(fileInfo, 0, 4, 5));

		IUnitInfo info = new IUnitInfo(fileInfo, iui, 0, 1);
		assertEquals(5, info.beginLine());
		assertEquals(9, info.endLine());
		assertEquals(5, Util.bytes2int(info.beginKey().value(), 0, 4));
		assertEquals(9, Util.bytes2int(info.endKey().value(), 0, 4));
		assertEquals(5, info.recordNum());
		fileInfo.out().close();

	}

}
