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
import org.apache.hadoop.hive.ql.util.jdbm.helper.IterationException;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import java.util.ArrayList;
import java.util.Iterator;

final class HashDirectory extends HashNode implements Externalizable {

  static final long serialVersionUID = 1L;

  static final int MAX_CHILDREN = 256;

  static final int BIT_SIZE = 8;

  static final int MAX_DEPTH = 3;

  private long[] _children;

  private byte _depth;

  private transient RecordManager _recman;

  private transient long _recid;

  public HashDirectory() {
  }

  HashDirectory(byte depth) {
    _depth = depth;
    _children = new long[MAX_CHILDREN];
  }

  void setPersistenceContext(RecordManager recman, long recid) {
    this._recman = recman;
    this._recid = recid;
  }

  long getRecid() {
    return _recid;
  }

  boolean isEmpty() {
    for (int i = 0; i < _children.length; i++) {
      if (_children[i] != 0) {
        return false;
      }
    }
    return true;
  }

  Object get(Object key) throws IOException {
    int hash = hashCode(key);
    long child_recid = _children[hash];
    if (child_recid == 0) {
      return null;
    } else {
      HashNode node = (HashNode) _recman.fetch(child_recid);

      if (node instanceof HashDirectory) {
        HashDirectory dir = (HashDirectory) node;
        dir.setPersistenceContext(_recman, child_recid);
        return dir.get(key);
      } else {
        HashBucket bucket = (HashBucket) node;
        return bucket.getValue(key);
      }
    }
  }

  Object put(Object key, Object value) throws IOException {
    if (value == null) {
      return remove(key);
    }
    int hash = hashCode(key);
    long child_recid = _children[hash];
    if (child_recid == 0) {
      HashBucket bucket = new HashBucket(_depth + 1);

      Object existing = bucket.addElement(key, value);

      long b_recid = _recman.insert(bucket);
      _children[hash] = b_recid;

      _recman.update(_recid, this);

      return existing;
    } else {
      HashNode node = (HashNode) _recman.fetch(child_recid);

      if (node instanceof HashDirectory) {
        HashDirectory dir = (HashDirectory) node;
        dir.setPersistenceContext(_recman, child_recid);
        return dir.put(key, value);
      } else {
        HashBucket bucket = (HashBucket) node;
        if (bucket.hasRoom()) {
          Object existing = bucket.addElement(key, value);
          _recman.update(child_recid, bucket);
          return existing;
        } else {
          if (_depth == MAX_DEPTH) {
            throw new RuntimeException("Cannot create deeper directory. "
                + "Depth=" + _depth);
          }
          HashDirectory dir = new HashDirectory((byte) (_depth + 1));
          long dir_recid = _recman.insert(dir);
          dir.setPersistenceContext(_recman, dir_recid);

          _children[hash] = dir_recid;
          _recman.update(_recid, this);

          _recman.delete(child_recid);

          ArrayList keys = bucket.getKeys();
          ArrayList values = bucket.getValues();
          int entries = keys.size();
          for (int i = 0; i < entries; i++) {
            dir.put(keys.get(i), values.get(i));
          }

          return dir.put(key, value);
        }
      }
    }
  }

  Object remove(Object key) throws IOException {
    int hash = hashCode(key);
    long child_recid = _children[hash];
    if (child_recid == 0) {
      return null;
    } else {
      HashNode node = (HashNode) _recman.fetch(child_recid);

      if (node instanceof HashDirectory) {
        HashDirectory dir = (HashDirectory) node;
        dir.setPersistenceContext(_recman, child_recid);
        Object existing = dir.remove(key);
        if (existing != null) {
          if (dir.isEmpty()) {
            _recman.delete(child_recid);
            _children[hash] = 0;
            _recman.update(_recid, this);
          }
        }
        return existing;
      } else {
        HashBucket bucket = (HashBucket) node;
        Object existing = bucket.removeElement(key);
        if (existing != null) {
          if (bucket.getElementCount() >= 1) {
            _recman.update(child_recid, bucket);
          } else {
            _recman.delete(child_recid);
            _children[hash] = 0;
            _recman.update(_recid, this);
          }
        }
        return existing;
      }
    }
  }

  private int hashCode(Object key) {
    int hashMask = hashMask();
    int hash = key.hashCode();
    hash = hash & hashMask;
    hash = hash >>> ((MAX_DEPTH - _depth) * BIT_SIZE);
    hash = hash % MAX_CHILDREN;

    return hash;
  }

  int hashMask() {
    int bits = MAX_CHILDREN - 1;
    int hashMask = bits << ((MAX_DEPTH - _depth) * BIT_SIZE);

    return hashMask;
  }

  FastIterator keys() throws IOException {
    return new HDIterator(true);
  }

  FastIterator values() throws IOException {
    return new HDIterator(false);
  }

  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeByte(_depth);
    out.writeObject(_children);
  }

  public synchronized void readExternal(ObjectInput in) throws IOException,
      ClassNotFoundException {
    _depth = in.readByte();
    _children = (long[]) in.readObject();
  }

  public class HDIterator extends FastIterator {

    private boolean _iterateKeys;

    private ArrayList _dirStack;
    private ArrayList _childStack;

    private HashDirectory _dir;

    private int _child;

    private Iterator _iter;

    HDIterator(boolean iterateKeys) throws IOException {
      _dirStack = new ArrayList();
      _childStack = new ArrayList();
      _dir = HashDirectory.this;
      _child = -1;
      _iterateKeys = iterateKeys;

      prepareNext();
    }

    public Object next() {
      Object next = null;
      if (_iter != null && _iter.hasNext()) {
        next = _iter.next();
      } else {
        try {
          prepareNext();
        } catch (IOException except) {
          throw new IterationException(except);
        }
        if (_iter != null && _iter.hasNext()) {
          return next();
        }
      }
      return next;
    }

    private void prepareNext() throws IOException {
      long child_recid = 0;

      do {
        _child++;
        if (_child >= MAX_CHILDREN) {

          if (_dirStack.isEmpty()) {
            return;
          }

          _dir = (HashDirectory) _dirStack.remove(_dirStack.size() - 1);
          _child = ((Integer) _childStack.remove(_childStack.size() - 1))
              .intValue();
          continue;
        }
        child_recid = _dir._children[_child];
      } while (child_recid == 0);

      if (child_recid == 0) {
        throw new Error("child_recid cannot be 0");
      }

      HashNode node = (HashNode) _recman.fetch(child_recid);

      if (node instanceof HashDirectory) {
        _dirStack.add(_dir);
        _childStack.add(new Integer(_child));

        _dir = (HashDirectory) node;
        _child = -1;

        _dir.setPersistenceContext(_recman, child_recid);
        prepareNext();
      } else {
        HashBucket bucket = (HashBucket) node;
        if (_iterateKeys) {
          _iter = bucket.getKeys().iterator();
        } else {
          _iter = bucket.getValues().iterator();
        }
      }
    }
  }

}
