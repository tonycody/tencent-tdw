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

package org.apache.hadoop.hive.ql.util.jdbm.helper;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

public class MRU implements CachePolicy {

  Hashtable _hash = new Hashtable();

  int _max;

  CacheEntry _first;

  CacheEntry _last;

  Vector listeners = new Vector();

  public MRU(int max) {
    if (max <= 0) {
      throw new IllegalArgumentException(
          "MRU cache must contain at least one entry");
    }
    _max = max;
  }

  public void put(Object key, Object value) throws CacheEvictionException {
    CacheEntry entry = (CacheEntry) _hash.get(key);
    if (entry != null) {
      entry.setValue(value);
      touchEntry(entry);
    } else {

      if (_hash.size() == _max) {
        entry = purgeEntry();
        entry.setKey(key);
        entry.setValue(value);
      } else {
        entry = new CacheEntry(key, value);
      }
      addEntry(entry);
      _hash.put(entry.getKey(), entry);
    }
  }

  public Object get(Object key) {
    CacheEntry entry = (CacheEntry) _hash.get(key);
    if (entry != null) {
      touchEntry(entry);
      return entry.getValue();
    } else {
      return null;
    }
  }

  public void remove(Object key) {
    CacheEntry entry = (CacheEntry) _hash.get(key);
    if (entry != null) {
      removeEntry(entry);
      _hash.remove(entry.getKey());
    }
  }

  public void removeAll() {
    _hash = new Hashtable();
    _first = null;
    _last = null;
  }

  public Enumeration elements() {
    return new MRUEnumeration(_hash.elements());
  }

  public void addListener(CachePolicyListener listener) {
    if (listener == null) {
      throw new IllegalArgumentException("Cannot add null listener.");
    }
    if (!listeners.contains(listener)) {
      listeners.addElement(listener);
    }
  }

  public void removeListener(CachePolicyListener listener) {
    listeners.removeElement(listener);
  }

  protected void addEntry(CacheEntry entry) {
    if (_first == null) {
      _first = entry;
      _last = entry;
    } else {
      _last.setNext(entry);
      entry.setPrevious(_last);
      _last = entry;
    }
  }

  protected void removeEntry(CacheEntry entry) {
    if (entry == _first) {
      _first = entry.getNext();
    }
    if (_last == entry) {
      _last = entry.getPrevious();
    }
    CacheEntry previous = entry.getPrevious();
    CacheEntry next = entry.getNext();
    if (previous != null) {
      previous.setNext(next);
    }
    if (next != null) {
      next.setPrevious(previous);
    }
    entry.setPrevious(null);
    entry.setNext(null);
  }

  protected void touchEntry(CacheEntry entry) {
    if (_last == entry) {
      return;
    }
    removeEntry(entry);
    addEntry(entry);
  }

  protected CacheEntry purgeEntry() throws CacheEvictionException {
    CacheEntry entry = _first;

    CachePolicyListener listener;
    for (int i = 0; i < listeners.size(); i++) {
      listener = (CachePolicyListener) listeners.elementAt(i);
      listener.cacheObjectEvicted(entry.getValue());
    }

    removeEntry(entry);
    _hash.remove(entry.getKey());

    entry.setValue(null);
    return entry;
  }

}

class CacheEntry {
  private Object _key;
  private Object _value;

  private CacheEntry _previous;
  private CacheEntry _next;

  CacheEntry(Object key, Object value) {
    _key = key;
    _value = value;
  }

  Object getKey() {
    return _key;
  }

  void setKey(Object obj) {
    _key = obj;
  }

  Object getValue() {
    return _value;
  }

  void setValue(Object obj) {
    _value = obj;
  }

  CacheEntry getPrevious() {
    return _previous;
  }

  void setPrevious(CacheEntry entry) {
    _previous = entry;
  }

  CacheEntry getNext() {
    return _next;
  }

  void setNext(CacheEntry entry) {
    _next = entry;
  }
}

class MRUEnumeration implements Enumeration {
  Enumeration _enum;

  MRUEnumeration(Enumeration enume) {
    _enum = enume;
  }

  public boolean hasMoreElements() {
    return _enum.hasMoreElements();
  }

  public Object nextElement() {
    CacheEntry entry = (CacheEntry) _enum.nextElement();
    return entry.getValue();
  }
}
