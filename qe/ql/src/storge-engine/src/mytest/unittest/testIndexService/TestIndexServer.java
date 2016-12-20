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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import IndexService.IndexServer;
import IndexStorage.UtilIndexStorage;
import junit.framework.TestCase;

public class TestIndexServer extends TestCase {

	IndexItemStatus status;
	FileSystem fs;
	Configuration conf;

	@Override
	protected void setUp() throws Exception {
		super.setUp();
		conf = new Configuration();
		fs = FileSystem.get(conf);
	}

	public void testMetaDataChecker() throws IOException, InterruptedException {

		status = initializeindexconf();

		IndexServer server = new IndexServer(null);
		server.testmode = true;
		server.metaDataChecker.start();
		Thread.sleep(2000);
		Set<String> keys = server.indexitems.keySet();
		for (String key : keys) {
			System.out.println(key);
			for (IndexServer.IndexItemStatus items : server.indexitems.get(key)
					.values()) {
				assertEquals(items.database, status.database);
				assertEquals(items.datadir, status.datadir);
				assertEquals(items.field, status.field);
				assertEquals(items.indexlocation, status.indexlocation);
				assertEquals(items.indexname, status.indexname);
				assertEquals(items.status, status.status);
				assertEquals(items.table, status.table);
				assertEquals(items.type, status.type);
				assertEquals(items.gettablelocation(), status
						.gettablelocation());
			}
		}

	}

	public void testDataPartMoniter() throws IOException, InterruptedException {
		status = initializeindexconf();

		fs.delete(new Path(status.gettablelocation()), true);
		FSDataOutputStream fos = fs.create(new Path(status.gettablelocation()
				+ "testfile"));
		fos.write("test".getBytes());
		fos.close();

		IndexServer server = new IndexServer(null);
		server.testmode = true;
		server.metaDataChecker.start();
		Thread.sleep(1000);
		server.dataPartMoniter.start();
		Thread.sleep(1000);
		Set<String> keys = server.tablefiles.keySet();
		for (String key : keys) {
			for (HashSet<String> hashstrs : server.tablefiles.get(key).values()) {
				for (String str : hashstrs) {
					assertEquals(new Path(str).getName(), "testfile");
				}
			}
		}
	}

	public void testIndexPartMoniter() throws IOException, InterruptedException {
		status = initializeindexconf();
		fs.delete(new Path(status.gettablelocation()), true);
		fs.delete(new Path(status.gettablelocation()), true);
		FSDataOutputStream fos = fs.create(new Path(status.gettablelocation()
				+ "testfile"));
		fos.write("test".getBytes());
		fos.close();

		IndexServer server = new IndexServer(null);
		server.testmode = true;
		server.metaDataChecker.start();
		Thread.sleep(1000);
		server.dataPartMoniter.start();
		Thread.sleep(1000);
		server.indexPartMoniter.start();
		Thread.sleep(1000);

		Set<String> keys = server.indexfiles.keySet();
		for (String key : keys) {
			for (ConcurrentHashMap<String, HashSet<String>> hashstrs : server.indexfiles
					.get(key).values()) {
				for (HashSet<String> str : hashstrs.values()) {
					for (String s : str) {
						System.out.println(s);
					}
				}
			}
		}
	}

	public void testTaskQueueManager() throws IOException, InterruptedException {
		status = initializeindexconf();
		fs.delete(new Path(status.gettablelocation()), true);
		fs.delete(new Path(status.gettablelocation()), true);
		FSDataOutputStream fos = fs.create(new Path(status.gettablelocation()
				+ "testfile"));
		fos.write("test".getBytes());
		fos.close();

		IndexServer server = new IndexServer(null);
		server.testmode = true;
		server.metaDataChecker.start();
		Thread.sleep(1000);
		server.dataPartMoniter.start();
		Thread.sleep(1000);
		server.indexPartMoniter.start();
		Thread.sleep(1000);
		server.taskQueueManager.start();
		Thread.sleep(1000);

		assertTrue(server.taskQueue != null && server.taskQueue.size() > 0);
	}

	public void testTaskQueueRunner() throws Exception {
		status = initializeindexconf();
		fs.delete(new Path(status.gettablelocation()), true);
		fs.delete(new Path(status.gettablelocation()), true);
		UtilIndexStorage.writeFDF(status.gettablelocation(), 5, 10000,
				(short) -1, false, false, true, true);

		IndexServer server = new IndexServer(null);
		server.testmode = true;
		server.start();
		Thread.sleep(5000);
		server.close();

	}

	public void testServerUpdateIndex() throws Exception {
		status = initializeindexconf();
		fs.delete(new Path(status.gettablelocation()), true);
		fs.delete(new Path(status.gettablelocation()), true);
		UtilIndexStorage.writeFDF(status.gettablelocation(), 5, 10000,
				(short) -1, false, false, true, true);

		IndexServer server = new IndexServer(null);
		server.testmode = true;
		server.start();
		ArrayList<IndexItemStatus> itemstatuss = new ArrayList<IndexItemStatus>();
		while (true) {
			itemstatuss.clear();
			File file = new File("indexconf");
			DataInputStream dis = new DataInputStream(new FileInputStream(file));
			int num = dis.readInt();
			for (int i = 0; i < num; i++) {
				IndexItemStatus status = new IndexItemStatus();
				status.read(dis);
				itemstatuss.add(status);
			}
			dis.close();
			if (itemstatuss.get(0).status == 2) {
				break;
			}
			if (itemstatuss.get(0).status < 0) {
				server.close();
				assertTrue(false);
			}
			Thread.sleep(3000);
		}

		server.close();
	}

	public void testServerMergeIndex() throws Exception {
		Configuration conf = new Configuration();
		conf.set("se.indexer.IPM_merge_interval", "5000");
		IndexServer server = new IndexServer(conf);
		server.testmode = true;
		server.start();
		ArrayList<IndexItemStatus> itemstatuss = new ArrayList<IndexItemStatus>();
		for (int k = 0; k < 0; k++) {
			while (true) {
				itemstatuss.clear();
				File file = new File("indexconf");
				DataInputStream dis = new DataInputStream(new FileInputStream(
						file));
				int num = dis.readInt();
				for (int i = 0; i < num; i++) {
					IndexItemStatus status = new IndexItemStatus();
					status.read(dis);
					itemstatuss.add(status);
				}
				dis.close();
				if (itemstatuss.get(0).status == 2) {
					break;
				}
				if (itemstatuss.get(0).status < 0) {
					server.close();
					assertTrue(false);
				}
				Thread.sleep(3000);
			}
			UtilIndexStorage.writeFDF(status.gettablelocation(), 3, 10000,
					(short) -1, false, false, true, false);
			while (true) {
				itemstatuss.clear();
				File file = new File("indexconf");
				DataInputStream dis = new DataInputStream(new FileInputStream(
						file));
				int num = dis.readInt();
				for (int i = 0; i < num; i++) {
					IndexItemStatus status = new IndexItemStatus();
					status.read(dis);
					itemstatuss.add(status);
				}
				dis.close();
				if (itemstatuss.get(0).status == 1) {
					break;
				}
				Thread.sleep(3000);
			}
		}

		Thread.sleep(15000);
		while (true) {
			itemstatuss.clear();
			File file = new File("indexconf");
			DataInputStream dis = new DataInputStream(new FileInputStream(file));
			int num = dis.readInt();
			for (int i = 0; i < num; i++) {
				IndexItemStatus status = new IndexItemStatus();
				status.read(dis);
				itemstatuss.add(status);
			}
			dis.close();
			if (itemstatuss.get(0).status == 2) {
				break;
			}
			Thread.sleep(3000);
		}

		FileStatus[] fss = fs.listStatus(new Path(
				itemstatuss.get(0).indexlocation + "/nopart"));
		assertEquals(fss.length, 1);

		server.close();
	}

	private IndexItemStatus initializeindexconf() throws IOException {
		File file = new File("indexconf");
		ArrayList<IndexItemStatus> itemstatuss = new ArrayList<IndexItemStatus>();

		String datadir = "/se/data";
		String indexdir = "/se/index";
		String database = "testdbformat";
		String table = "testtable";
		String indexname = "index1";
		String field = "1";

		IndexItemStatus status = new IndexItemStatus();
		status.datadir = datadir;
		status.database = database;
		status.table = table;
		status.indexname = indexname;
		status.field = field;
		status.indexlocation = indexdir + "/" + database + "/" + table + "/"
				+ indexname;
		status.status = 0;
		status.type = 0;
		status.tabletype = "format";
		itemstatuss.add(status);

		DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
		dos.writeInt(itemstatuss.size());
		for (int i = 0; i < itemstatuss.size(); i++) {
			itemstatuss.get(i).write(dos);
		}
		dos.close();
		return status;
	}

}

class IndexItemStatus {
	String datadir;
	String database;
	String table;
	String indexname;
	String field;
	String indexlocation;
	int type;
	int status;

	String tabletype;

	public String gettablelocation() {
		return datadir + "/" + database + "/" + table + "/";
	}

	public void read(DataInputStream dis) throws IOException {
		this.datadir = dis.readUTF();
		this.database = dis.readUTF();
		this.table = dis.readUTF();
		this.indexname = dis.readUTF();
		this.field = dis.readUTF();
		this.indexlocation = dis.readUTF();
		this.status = dis.readInt();
		this.type = dis.readInt();
		this.tabletype = dis.readUTF();

	}

	public void write(DataOutputStream dos) throws IOException {
		dos.writeUTF(datadir);
		dos.writeUTF(database);
		dos.writeUTF(table);
		dos.writeUTF(indexname);
		dos.writeUTF(field);
		dos.writeUTF(indexlocation);
		dos.writeInt(status);
		dos.writeInt(type);
		dos.writeUTF(tabletype);
	}

}
