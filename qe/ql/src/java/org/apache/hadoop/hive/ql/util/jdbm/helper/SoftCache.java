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

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.lang.ref.Reference;
import java.util.Enumeration;
import java.util.Map;
import java.util.HashMap;

public class SoftCache implements CachePolicy {
  private static final int INITIAL_CAPACITY = 128;
  private static final float DEFAULT_LOAD_FACTOR = 1.5f;

  private final ReferenceQueue _clearQueue = new ReferenceQueue();
  private final CachePolicy _internal;
  private final Map _cacheMap;

  public SoftCache() {
    this(new MRU(INITIAL_CAPACITY));
  }

  public SoftCache(CachePolicy internal) throws NullPointerException {
    this(DEFAULT_LOAD_FACTOR, internal);
  }

  public SoftCache(float loadFactor, CachePolicy internal)
      throws IllegalArgumentException, NullPointerException {
    if (internal == null) {
      throw new NullPointerException("Internal cache cannot be null.");
    }
    _internal = internal;
    _cacheMap = new HashMap(INITIAL_CAPACITY, loadFactor);
  }

  public void put(Object key, Object value) throws CacheEvictionException {
    if (key == null) {
      throw new IllegalArgumentException("key cannot be null.");
    } else if (value == null) {
      throw new IllegalArgumentException("value cannot be null.");
    }
    _internal.put(key, value);
    removeClearedEntries();
    _cacheMap.put(key, new Entry(key, value, _clearQueue));
  }

  public Object get(Object key) {
    Object value = _internal.get(key);
    if (value != null) {
      return value;
    }
    removeClearedEntries();
    Entry entry = (Entry) _cacheMap.get(key);
    if (entry == null) {
      return null;
    }
    value = entry.getValue();
    if (value == null) {
      return null;
    }
    try {
      _internal.put(key, value);
    } catch (CacheEvictionException e) {
      _cacheMap.remove(key);
      return null;
    }
    return value;
  }

  public void remove(Object key) {
    _cacheMap.remove(key);
    _internal.remove(key);
  }

  public void removeAll() {
    _cacheMap.clear();
    _internal.removeAll();
  }

  public Enumeration elements() {
    return _internal.elements();
  }

  public void addListener(CachePolicyListener listener)
      throws IllegalArgumentException {
    _internal.addListener(listener);
  }

  public void removeListener(CachePolicyListener listener) {
    _internal.removeListener(listener);
  }

  private final void removeClearedEntries() {
    for (Reference r = _clearQueue.poll(); r != null; r = _clearQueue.poll()) {
      Object key = ((Entry) r).getKey();
      _cacheMap.remove(key);
    }
  }

  private static class Entry extends SoftReference {
    private final Object _key;

    public Entry(Object key, Object value, ReferenceQueue queue) {
      super(value, queue);
      _key = key;
    }

    final Object getKey() {
      return _key;
    }

    final Object getValue() {
      return this.get();
    }
  }
}
