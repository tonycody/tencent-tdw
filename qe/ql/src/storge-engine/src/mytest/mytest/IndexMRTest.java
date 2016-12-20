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

import org.apache.hadoop.conf.Configuration;

import IndexService.IndexMR;

public class IndexMRTest {

	public static void main(String[] args) {
		String inputfiles = "hdfs://172.25.38.253:54310/user/tdw/warehouse/default_db/tcssformat/attempt_201011021154_0013_m_000000_0.1288685557438";
		String outputdir = "/se/tmp/test/";
		Configuration conf = new Configuration();
		String ids = "2";
		String indexname = "testindex";

		IndexMR.run(new Configuration(), inputfiles, false, ids, outputdir);
	}
}
