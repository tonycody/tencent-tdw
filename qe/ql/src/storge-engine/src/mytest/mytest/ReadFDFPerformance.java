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

import java.util.Random;

import org.apache.hadoop.conf.Configuration;

import FormatStorage.FormatDataFile;

public class ReadFDFPerformance {

	static void readseqfdf(String filename, int num) throws Exception {
		Configuration conf = new Configuration();
		FormatDataFile fd2 = new FormatDataFile(conf);
		fd2.open(filename);

		for (int i = 0; i < num; i++) {
			fd2.getRecordByLine(i);
		}

	}

	static void readranfdf(String filename, int num, int size) throws Exception {
		Random r = new Random();
		Configuration conf = new Configuration();
		FormatDataFile fd2 = new FormatDataFile(conf);
		fd2.open(filename);

		for (int i = 0; i < num; i++) {
			fd2.getRecordByLine(r.nextInt((int) size));
		}
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.out.println("read mode | filename | num | size");
			return;
		}
		String filename = args[1];
		int num = Integer.valueOf(args[2]);
		int size = Integer.valueOf(args[3]);

		String readmode = args[0];
		long time = System.currentTimeMillis();

		if (readmode.equals("readseq")) {
			readseqfdf(filename, num);

		} else {
			readranfdf(filename, num, size);
		}

		System.out.println(System.currentTimeMillis() - time);
	}
}
