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

import Comm.Util;
import IndexStorage.IFieldValue;
import IndexStorage.IFormatDataFile;
import IndexStorage.IRecord;
import junit.framework.TestCase;

public class TestIFormatDataFile extends TestCase {
	Configuration conf;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		conf = new Configuration();
	}

	public void testIFDF() {
		try {

			String fileName = "test";

			TestUtil.writeifdfile(fileName, 1000);
			IFormatDataFile ifdf = new IFormatDataFile(conf);
			ifdf.open(fileName);
			assertTrue(ifdf.seek(10));
			IRecord record = ifdf.next();
			assertEquals(10, record.fieldValues().get((short) 0).value()[0]);

			IFieldValue ifv = new IFieldValue(10);
			assertTrue(ifdf.seek(ifv));
			record = ifdf.next();
			assertTrue(Util.bytes2int(record.fieldValues().get((short) 2)
					.value(), 0, 4) >= 10);
			ifdf.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
