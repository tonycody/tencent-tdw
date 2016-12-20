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

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import IndexService.IndexMergeMR;
import IndexStorage.IFormatDataFile;

import unittest.testIndexStorage.TestUtil;

public class TestIndexMergeMR extends TestCase {

	public void testIndexMergeMR() throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		String indexdir = "indexdir";
		String indexdir1 = "indexdir1";
		int filenum = 10;
		int recnum = 1000;
		short idx = 0;
		TestUtil.genifdfindex(indexdir, filenum, recnum, idx, true);
		StringBuffer sb = new StringBuffer();
		FileStatus[] ss = fs.listStatus(new Path(indexdir));
		for (FileStatus fileStatus : ss) {
			sb.append(fileStatus.getPath().toString()).append(",");
		}
		IndexMergeMR.running(sb.substring(0, sb.length() - 1), indexdir1, conf);

		IFormatDataFile ifdf = new IFormatDataFile(conf);
		ifdf.open(indexdir1 + "/part-00000");
		for (int i = 0; i < 100; i++) {
			ifdf.next().show();
		}

		ifdf.close();

		fs.delete(new Path(indexdir), true);
		fs.delete(new Path(indexdir1), true);

	}
}
