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

import IndexStorage.LRUCache;
import junit.framework.TestCase;

public class TestLRUCache extends TestCase {

	public void testLRUCache() {
		LRUCache<Integer, String> lru = new LRUCache<Integer, String>(3);
		lru.put(1, "test1");
		lru.put(2, "test2");
		lru.put(3, "test3");
		assertTrue(lru.containsKey(1));
		assertTrue(lru.containsKey(2));
		assertTrue(lru.containsKey(3));

		lru.put(4, "test4");
		assertTrue(!lru.containsKey(1));
		assertTrue(lru.containsKey(4));
	}
}
