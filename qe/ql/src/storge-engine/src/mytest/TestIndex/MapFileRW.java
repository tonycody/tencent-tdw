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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;

import Comm.ConstVar;
import IndexService.IndexKey;
import IndexService.IndexValue;

public class MapFileRW {
	static void write() throws IOException {
		String filename = "/indextest/testmapfile";
		Configuration conf = new Configuration();
		MapFile.Writer writer = new MapFile.Writer(conf, FileSystem.get(conf),
				filename, IndexKey.class, IndexValue.class);

		writer.close();
	}

	static void read(String filename, int num) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		MapFile.Reader reader = new MapFile.Reader(fs, filename, conf);
		IndexKey key = new IndexKey();
		IndexValue value = new IndexValue();
		int i = 0;
		while (reader.next(key, value)) {
			System.out.print(value.getFileindex() + " ");
			System.out.println(value.getRowid());
			key.reset();
			if ((i++) >= num)
				break;

		}
	}

	static void read1(String filename, int num) throws Exception {
	}

	private static void printkv(IndexKey key, IndexValue value) {

		System.out.println(key.show() + value.show());

	}

	public static void main(String[] args) throws Exception {
		String filename = "/se/tmp/1718447158/part-00000";
		read(filename, 500);

	}
}
