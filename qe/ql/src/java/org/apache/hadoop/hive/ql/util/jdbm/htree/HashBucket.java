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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import java.util.ArrayList;

final class HashBucket extends HashNode implements Externalizable {

  final static long serialVersionUID = 1L;

  public static final int OVERFLOW_SIZE = 8;

  private int _depth;

  private ArrayList _keys;

  private ArrayList _values;

  public HashBucket() {
  }

  public HashBucket(int level) {
    if (level > HashDirectory.MAX_DEPTH + 1) {
      throw new IllegalArgumentException(
          "Cannot create bucket with depth > MAX_DEPTH+1. " + "Depth=" + level);
    }
    _depth = level;
    _keys = new ArrayList(OVERFLOW_SIZE);
    _values = new ArrayList(OVERFLOW_SIZE);
  }

  public int getElementCount() {
    return _keys.size();
  }

  public boolean isLeaf() {
    return (_depth > HashDirectory.MAX_DEPTH);
  }

  public boolean hasRoom() {
    if (isLeaf()) {
      return true;
    } else {
      return (_keys.size() < OVERFLOW_SIZE);
    }
  }

  public Object addElement(Object key, Object value) {
    int existing = _keys.indexOf(key);
    if (existing != -1) {
      Object before = _values.get(existing);
      _values.set(existing, value);
      return before;
    } else {
      _keys.add(key);
      _values.add(value);
      return null;
    }
  }

  public Object removeElement(Object key) {
    int existing = _keys.indexOf(key);
    if (existing != -1) {
      Object obj = _values.get(existing);
      _keys.remove(existing);
      _values.remove(existing);
      return obj;
    } else {
      return null;
    }
  }

  public Object getValue(Object key) {
    int existing = _keys.indexOf(key);
    if (existing != -1) {
      return _values.get(existing);
    } else {
      return null;
    }
  }

  ArrayList getKeys() {
    return this._keys;
  }

  ArrayList getValues() {
    return this._values;
  }

  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeInt(_depth);

    int entries = _keys.size();
    out.writeInt(entries);

    for (int i = 0; i < entries; i++) {
      out.writeObject(_keys.get(i));
    }
    for (int i = 0; i < entries; i++) {
      out.writeObject(_values.get(i));
    }
  }

  public void readExternal(ObjectInput in) throws IOException,
      ClassNotFoundException {
    _depth = in.readInt();

    int entries = in.readInt();

    int size = Math.max(entries, OVERFLOW_SIZE);
    _keys = new ArrayList(size);
    _values = new ArrayList(size);

    for (int i = 0; i < entries; i++) {
      _keys.add(in.readObject());
    }
    for (int i = 0; i < entries; i++) {
      _values.add(in.readObject());
    }
  }

  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append("HashBucket {depth=");
    buf.append(_depth);
    buf.append(", keys=");
    buf.append(_keys);
    buf.append(", values=");
    buf.append(_values);
    buf.append("}");
    return buf.toString();
  }
}
