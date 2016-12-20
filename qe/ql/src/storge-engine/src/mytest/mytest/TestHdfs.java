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
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestHdfs {
	static Configuration conf = new Configuration();
	static FileSystem fs;
	static {
		conf.setBoolean("dfs.support.append", true);
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	Random r = new Random();

	public TestHdfs() {
		String file = "/testhdfs";
		new Append(file).start();
		new Read(file).start();
	}

	class Read extends Thread {

		String file;

		public Read(String file) {
			this.file = file;
		}

		@Override
		public void run() {
			try {
				FSDataInputStream fis = fs.open(new Path(file));
				while (true) {
					int x = r.nextInt(3) + 1;
					Thread.sleep(x * 1000);
					char ch = (char) fis.read();
					System.out.println("read:\t" + ch);
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	class Write extends Thread {
		String file;

		public Write(String file) {
			this.file = file;
		}

		@Override
		public void run() {
		}
	}

	class Append extends Thread {
		String file;

		public Append(String file) {
			this.file = file;
		}

		@Override
		public void run() {
			try {
				FSDataOutputStream fos = fs.append(new Path(file));
				while (true) {
					int x = r.nextInt(3) + 0;
					Thread.sleep(x * 1000);
					char ch = (char) (r.nextInt(26) + 'a');
					fos.write(ch);
					System.out.println("write:\t" + ch);
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws IOException {
		new TestHdfs();
	}
}
