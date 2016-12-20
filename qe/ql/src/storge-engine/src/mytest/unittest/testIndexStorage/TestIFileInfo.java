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

import Comm.ConstVar;
import IndexStorage.IFieldMap;
import IndexStorage.IFieldType;
import IndexStorage.IFileInfo;
import IndexStorage.IHead;
import IndexStorage.IUserDefinedHeadInfo;
import junit.framework.TestCase;

public class TestIFileInfo extends TestCase {
	public void testIFileInfoWrite() throws IOException {
		IFileInfo fileInfo;
		try {
			Configuration conf = new Configuration();
			fileInfo = new IFileInfo(conf);
			String fileName = "test";
			IHead head = new IHead();
			IFieldMap fieldMap = new IFieldMap();
			fieldMap.addFieldType(new IFieldType.IFieldByteType());
			fieldMap.addFieldType(new IFieldType.IFieldShortType());
			fieldMap.addFieldType(new IFieldType.IFieldIntType());
			fieldMap.addFieldType(new IFieldType.IFieldLongType());
			fieldMap.addFieldType(new IFieldType.IFieldFloatType());
			fieldMap.addFieldType(new IFieldType.IFieldDoubleType());
			head.setFieldMap(fieldMap);
			head.setPrimaryIndex((short) 2);
			IUserDefinedHeadInfo udi = new IUserDefinedHeadInfo();
			udi.addInfo(0, fileName);
			head.setUdi(udi);

			fileInfo.initialize(fileName, head);

			assertEquals(fileName, fileInfo.filename());
			assertTrue(!fileInfo.compressed());
			assertTrue(!fileInfo.isVar());
			assertTrue(fileInfo.havekeyindex());
			assertTrue(fileInfo.havelineindex());
			assertEquals(ConstVar.WS_Write, fileInfo.workStatus());

			fileInfo.setcurrentline(10);
			assertEquals(10, fileInfo.currentline());

			fileInfo.increasecurrentline();
			assertEquals(11, fileInfo.currentline());

			assertEquals(28, fileInfo.recordlen());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void testIFileInfoRead() {

	}
}
