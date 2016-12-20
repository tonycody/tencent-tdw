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

package org.apache.hadoop.hive.ql.exec.persistence;

import org.apache.hadoop.hive.ql.exec.persistence.DCLLItem;

public class MRU<T extends DCLLItem> {

  T head;

  public MRU() {
    head = null;
  }

  public T put(T item) {
    addToHead(item);
    return item;
  }

  public void remove(T v) {
    if (v == null)
      return;
    if (v == head) {
      if (head != head.getNext()) {
        head = (T) head.getNext();
      } else {
        head = null;
      }
    }
    v.remove();
  }

  public T head() {
    return head;
  }

  public T tail() {
    return (T) head.getPrev();
  }

  private void addToHead(T v) {
    if (head == null) {
      head = v;
    } else {
      head.insertBefore(v);
      head = v;
    }
  }

  public void moveToHead(T v) {
    assert (head != null);
    if (head != v) {
      v.remove();
      head.insertBefore(v);
      head = v;
    }
  }

  public void clear() {
    while (head.getNext() != head) {
      head.getNext().remove();
    }
    head.remove();
    head = null;
  }
}
