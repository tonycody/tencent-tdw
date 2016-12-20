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
import org.apache.hadoop.fs.Path;

import IndexStorage.IFormatDataFile;
import IndexStorage.IRecord;
import IndexStorage.ISegmentIndex;

public class Tmptest {

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		Path path = new Path("/se/tmp/628892613/part-00000");

		String filename = path.toString();
		IFormatDataFile ifd = new IFormatDataFile(conf);
		ifd.open(filename);

		ISegmentIndex segmentIndex = ifd.segIndex();

		for (String str : ifd.fileInfo().head().getUdi().infos().values()) {
			System.out.println(str);
		}
		System.out.println(segmentIndex.getSegnum());
		IRecord record = new IRecord();
		ifd.next(record);
		record.show();
		ifd.next().show();
		ifd.next().show();
		ifd.close();

	}
}
