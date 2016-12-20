/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.util.jdbm.htree;

import org.apache.hadoop.hive.ql.util.jdbm.RecordManager;
import org.apache.hadoop.hive.ql.util.jdbm.helper.FastIterator;
import java.io.IOException;

public class HTree {

  private HashDirectory _root;

  private HTree(HashDirectory root) {
    _root = root;
  }

  public static HTree createInstance(RecordManager recman) throws IOException {
    HashDirectory root;
    long recid;

    root = new HashDirectory((byte) 0);
    recid = recman.insert(root);
    root.setPersistenceContext(recman, recid);

    return new HTree(root);
  }

  public static HTree load(RecordManager recman, long root_recid)
      throws IOException {
    HTree tree;
    HashDirectory root;

    root = (HashDirectory) recman.fetch(root_recid);
    root.setPersistenceContext(recman, root_recid);
    tree = new HTree(root);
    return tree;
  }

  public synchronized void put(Object key, Object value) throws IOException {
    _root.put(key, value);
  }

  public synchronized Object get(Object key) throws IOException {
    return _root.get(key);
  }

  public synchronized void remove(Object key) throws IOException {
    _root.remove(key);
  }

  public synchronized FastIterator keys() throws IOException {
    return _root.keys();
  }

  public synchronized FastIterator values() throws IOException {
    return _root.values();
  }

  public long getRecid() {
    return _root.getRecid();
  }

}
