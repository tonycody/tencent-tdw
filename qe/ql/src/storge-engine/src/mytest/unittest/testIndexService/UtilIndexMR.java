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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import IndexService.IndexMR;
import IndexStorage.IFormatDataFile;
import IndexStorage.UtilIndexStorage;

public class UtilIndexMR {
	static Configuration conf = new Configuration();
	static FileSystem fs;
	static {
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void indexmrtest(String datadir, String indexdir,
			int filenum, int recnum, boolean var, boolean compress,
			boolean seq, boolean overwrite, boolean column, String idx,
			boolean removefile) throws Exception {

		if (column) {
			UtilIndexStorage.writeColumnFDF(datadir, filenum, recnum,
					(short) -1, var, compress, seq, overwrite);
		} else {
			UtilIndexStorage.writeFDF(datadir, filenum, recnum, (short) -1,
					var, compress, seq, overwrite);
		}

		FileStatus[] ss = fs.listStatus(new Path(datadir));
		StringBuffer sb = new StringBuffer();
		for (FileStatus fileStatus : ss) {
			sb.append(fileStatus.getPath().toString()).append(",");
		}
		System.out.println(sb.toString());
		IndexMR.running(conf, sb.substring(0, sb.length() - 1), column, idx,
				indexdir);

		IFormatDataFile ifdf = new IFormatDataFile(conf);
		ifdf.open(indexdir + "/part-00000");
		ifdf.seek(filenum * recnum / 2);
		for (int i = 0; i < 10; i++) {
			ifdf.next().show();
		}

		ifdf.close();
		fs.delete(new Path(indexdir + "/_logs"), true);
		if (removefile) {
			fs.delete(new Path(datadir), true);
			fs.delete(new Path(indexdir), true);
		}
	}

}
