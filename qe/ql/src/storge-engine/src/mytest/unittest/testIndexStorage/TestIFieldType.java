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
import Comm.ConstVar;
import IndexStorage.IFieldType;

public class TestIFieldType extends TestCase {
	public void testIFieldType() {

		IFieldType ift = new IFieldType(ConstVar.FieldType_Byte);
		assertEquals(ConstVar.FieldType_Byte, ift.getType());
		assertEquals(ConstVar.Sizeof_Byte, ift.getLen());

		ift = new IFieldType(ConstVar.FieldType_Short);
		assertEquals(ConstVar.FieldType_Short, ift.getType());
		assertEquals(ConstVar.Sizeof_Short, ift.getLen());

		ift = new IFieldType(ConstVar.FieldType_Int);
		assertEquals(ConstVar.FieldType_Int, ift.getType());
		assertEquals(ConstVar.Sizeof_Int, ift.getLen());

		ift = new IFieldType(ConstVar.FieldType_Long);
		assertEquals(ConstVar.FieldType_Long, ift.getType());
		assertEquals(ConstVar.Sizeof_Long, ift.getLen());

		ift = new IFieldType(ConstVar.FieldType_Float);
		assertEquals(ConstVar.FieldType_Float, ift.getType());
		assertEquals(ConstVar.Sizeof_Float, ift.getLen());

		ift = new IFieldType(ConstVar.FieldType_Double);
		assertEquals(ConstVar.FieldType_Double, ift.getType());
		assertEquals(ConstVar.Sizeof_Double, ift.getLen());

		ift = new IFieldType(ConstVar.FieldType_String);
		assertEquals(ConstVar.FieldType_String, ift.getType());

		ift = new IFieldType.IFieldByteType();
		assertEquals(ConstVar.FieldType_Byte, ift.getType());
		assertEquals(ConstVar.Sizeof_Byte, ift.getLen());

		ift = new IFieldType.IFieldShortType();
		assertEquals(ConstVar.FieldType_Short, ift.getType());
		assertEquals(ConstVar.Sizeof_Short, ift.getLen());

		ift = new IFieldType.IFieldIntType();
		assertEquals(ConstVar.FieldType_Int, ift.getType());
		assertEquals(ConstVar.Sizeof_Int, ift.getLen());

		ift = new IFieldType.IFieldLongType();
		assertEquals(ConstVar.FieldType_Long, ift.getType());
		assertEquals(ConstVar.Sizeof_Long, ift.getLen());

		ift = new IFieldType.IFieldFloatType();
		assertEquals(ConstVar.FieldType_Float, ift.getType());
		assertEquals(ConstVar.Sizeof_Float, ift.getLen());

		ift = new IFieldType.IFieldDoubleType();
		assertEquals(ConstVar.FieldType_Double, ift.getType());
		assertEquals(ConstVar.Sizeof_Double, ift.getLen());

		ift = new IFieldType.IFieldStringType();
		assertEquals(ConstVar.FieldType_String, ift.getType());

		ift = new IFieldType.IFieldStringType(10);
		assertEquals(10, ift.getLen());

		ift = new IFieldType.IFieldStringType(10, (short) 3);
		assertEquals(10, ift.getLen());
		assertEquals(3, ift.getIndex());

	}

	public void testWritable() throws IOException {
		IFieldType ift = new IFieldType(ConstVar.FieldType_Int, (short) 1);

		DataOutputBuffer dob = new DataOutputBuffer();
		ift.write(dob);
		byte[] data = dob.getData();
		DataInputBuffer dib = new DataInputBuffer();
		dib.reset(data, data.length);

		IFieldType ift2 = new IFieldType();
		ift2.readFields(dib);

		assertEquals(ift.getIndex(), ift2.getIndex());
		assertEquals(ift.getLen(), ift2.getLen());
		assertEquals(ift.getType(), ift2.getType());
	}

	public void testPersistable() throws IOException {
		IFieldType ift = new IFieldType(ConstVar.FieldType_Int, (short) 1);

		DataOutputBuffer dob = new DataOutputBuffer();
		ift.persistent(dob);
		byte[] data = dob.getData();
		DataInputBuffer dib = new DataInputBuffer();
		dib.reset(data, data.length);

		IFieldType ift2 = new IFieldType();
		ift2.unpersistent(dib);

		assertEquals(ift.getIndex(), ift2.getIndex());
		assertEquals(ift.getLen(), ift2.getLen());
		assertEquals(ift.getType(), ift2.getType());
	}
}
