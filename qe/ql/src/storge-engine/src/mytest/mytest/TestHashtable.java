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

import java.util.Hashtable;

public class TestHashtable {

	static class TestObject {
		int x = 0;
	}

	static Hashtable<Integer, TestObject> ht = new Hashtable<Integer, TestObject>();

	static class Thread1 extends Thread {
		@Override
		public void run() {
			synchronized (ht) {
				try {
					Thread.sleep(2000);
					System.out.println("thread1 sleep over");
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}


	public static void main(String[] args) {
		TestHashtable tht = new TestHashtable();
		TestObject to = new TestObject();
		ht.put(0, to);
		new Thread1().start();
		to.x = 1;
		System.out.println(to.x);
		to.x = 2;
		System.out.println(to.x);
		System.out.println(ht.get(0).x);
	}
}
