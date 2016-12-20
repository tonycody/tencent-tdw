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

import FormatStorage.FieldMap;
import FormatStorage.FormatDataFile;
import FormatStorage.Head;
import FormatStorage.FieldMap.Field;

public class FormatFileTest {
	public static void main(String[] args) throws Exception {
		FormatDataFile fdf = new FormatDataFile(new Configuration());
		String fileName = "";
		Head head = new Head();
		FieldMap fieldMap = new FieldMap();
		fieldMap.addField(new Field());
		head.setFieldMap(fieldMap);

		fdf.create(fileName, head);
	}
}
