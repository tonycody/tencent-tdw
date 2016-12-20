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
import IndexStorage.IHead;
import IndexStorage.IUserDefinedHeadInfo;
import junit.framework.TestCase;

public class TestIHead extends TestCase {
	public void testIHead() {
		IHead ih = new IHead();
		ih.setCompress((byte) 1);
		ih.setCompressStyle((byte) 1);
		ih.setEncode((byte) 1);
		ih.setEncodeStyle((byte) 1);
		ih.setLineindex((byte) 1);
		ih.setVar((byte) 1);
		ih.setVer((short) 1);
		ih.setMagic(1);
		ih.setPrimaryIndex((short) 1);
		IUserDefinedHeadInfo udi = new IUserDefinedHeadInfo();
		udi.addInfo(0, "aaa");
		udi.addInfo(1, "bbb");
		ih.setUserDefinedInfo(udi);
		ih.setFieldMap(new IFieldMap());

		assertEquals(1, ih.getMagic());
		assertEquals(1, ih.getCompress());
		assertEquals(1, ih.getCompressStyle());
		assertEquals(1, ih.getEncode());
		assertEquals(1, ih.getEncodeStyle());
		assertEquals(1, ih.getPrimaryIndex());
		assertEquals(1, ih.getVar());
		assertEquals(1, ih.getVer());

	}

	public void testPersistable() throws IOException {
		IHead ih = new IHead();
		ih.setCompress((byte) 1);
		ih.setCompressStyle((byte) 1);
		ih.setEncode((byte) 1);
		ih.setEncodeStyle((byte) 1);
		ih.setLineindex((byte) 1);
		ih.setVar((byte) 1);
		ih.setVer((short) 1);
		ih.setMagic(1);
		ih.setPrimaryIndex((short) 1);

		IUserDefinedHeadInfo udi = new IUserDefinedHeadInfo();
		udi.addInfo(0, "aaa");
		udi.addInfo(1, "bbb");

		ih.setUserDefinedInfo(udi);
		IFieldMap fieldMap = new IFieldMap();
		fieldMap.addFieldType(new IFieldType.IFieldByteType());
		fieldMap.addFieldType(new IFieldType.IFieldShortType());
		fieldMap.addFieldType(new IFieldType.IFieldIntType());
		fieldMap.addFieldType(new IFieldType.IFieldLongType());
		fieldMap.addFieldType(new IFieldType.IFieldFloatType());
		fieldMap.addFieldType(new IFieldType.IFieldDoubleType());
		ih.setFieldMap(fieldMap);

		DataOutputBuffer dob = new DataOutputBuffer();
		ih.persistent(dob);
		byte[] data = dob.getData();
		DataInputBuffer dib = new DataInputBuffer();
		dib.reset(data, data.length);

		IHead ih2 = new IHead();
		ih2.unpersistent(dib);

		assertEquals(ih2.getMagic(), ih.getMagic());
		assertEquals(ih2.getCompress(), ih.getCompress());
		assertEquals(ih2.getCompressStyle(), ih.getCompressStyle());
		assertEquals(ih2.getEncode(), ih.getEncode());
		assertEquals(ih2.getEncodeStyle(), ih.getEncodeStyle());
		assertEquals(ih2.getPrimaryIndex(), ih.getPrimaryIndex());
		assertEquals(ih2.getVar(), ih.getVar());
		assertEquals(ih2.getVer(), ih.getVer());
		assertEquals(ih2.lineindex(), ih.lineindex());

	}
}
