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

package org.apache.hadoop.hive.ql.util.jdbm.recman;

import org.apache.hadoop.hive.ql.util.jdbm.RecordManager;
import org.apache.hadoop.hive.ql.util.jdbm.helper.CacheEvictionException;
import org.apache.hadoop.hive.ql.util.jdbm.helper.CachePolicy;
import org.apache.hadoop.hive.ql.util.jdbm.helper.CachePolicyListener;
import org.apache.hadoop.hive.ql.util.jdbm.helper.DefaultSerializer;
import org.apache.hadoop.hive.ql.util.jdbm.helper.Serializer;
import org.apache.hadoop.hive.ql.util.jdbm.helper.WrappedRuntimeException;

import java.io.IOException;
import java.util.Enumeration;

public class CacheRecordManager implements RecordManager {

  protected RecordManager _recman;

  protected CachePolicy _cache;

  public CacheRecordManager(RecordManager recman, CachePolicy cache) {
    if (recman == null) {
      throw new IllegalArgumentException("Argument 'recman' is null");
    }
    if (cache == null) {
      throw new IllegalArgumentException("Argument 'cache' is null");
    }
    _recman = recman;
    _cache = cache;

    _cache.addListener(new CacheListener());
  }

  public RecordManager getRecordManager() {
    return _recman;
  }

  public CachePolicy getCachePolicy() {
    return _cache;
  }

  public long insert(Object obj) throws IOException {
    return insert(obj, DefaultSerializer.INSTANCE);
  }

  public synchronized long insert(Object obj, Serializer serializer)
      throws IOException {
    checkIfClosed();

    long recid = _recman.insert(obj, serializer);
    try {
      _cache
          .put(new Long(recid), new CacheEntry(recid, obj, serializer, false));
    } catch (CacheEvictionException except) {
      throw new WrappedRuntimeException(except);
    }
    return recid;
  }

  public synchronized void delete(long recid) throws IOException {
    checkIfClosed();

    _recman.delete(recid);
    _cache.remove(new Long(recid));
  }

  public void update(long recid, Object obj) throws IOException {
    update(recid, obj, DefaultSerializer.INSTANCE);
  }

  public synchronized void update(long recid, Object obj, Serializer serializer)
      throws IOException {
    CacheEntry entry;
    Long id;

    checkIfClosed();

    id = new Long(recid);
    try {
      entry = (CacheEntry) _cache.get(id);
      if (entry != null) {
        entry._obj = obj;
        entry._serializer = serializer;
        entry._isDirty = true;
      } else {
        _cache.put(id, new CacheEntry(recid, obj, serializer, true));
      }
    } catch (CacheEvictionException except) {
      throw new IOException(except.getMessage());
    }
  }

  public Object fetch(long recid) throws IOException {
    return fetch(recid, DefaultSerializer.INSTANCE);
  }

  public synchronized Object fetch(long recid, Serializer serializer)
      throws IOException {
    checkIfClosed();

    Long id = new Long(recid);
    CacheEntry entry = (CacheEntry) _cache.get(id);
    if (entry == null) {
      entry = new CacheEntry(recid, null, serializer, false);
      entry._obj = _recman.fetch(recid, serializer);
      try {
        _cache.put(id, entry);
      } catch (CacheEvictionException except) {
        throw new WrappedRuntimeException(except);
      }
    }
    return entry._obj;
  }

  public synchronized void close() throws IOException {
    checkIfClosed();

    updateCacheEntries();
    _recman.close();
    _recman = null;
    _cache = null;
  }

  public synchronized int getRootCount() {
    checkIfClosed();

    return _recman.getRootCount();
  }

  public synchronized long getRoot(int id) throws IOException {
    checkIfClosed();

    return _recman.getRoot(id);
  }

  public synchronized void setRoot(int id, long rowid) throws IOException {
    checkIfClosed();

    _recman.setRoot(id, rowid);
  }

  public synchronized void commit() throws IOException {
    checkIfClosed();
    updateCacheEntries();
    _recman.commit();
  }

  public synchronized void rollback() throws IOException {
    checkIfClosed();

    _recman.rollback();

    _cache.removeAll();
  }

  public synchronized long getNamedObject(String name) throws IOException {
    checkIfClosed();

    return _recman.getNamedObject(name);
  }

  public synchronized void setNamedObject(String name, long recid)
      throws IOException {
    checkIfClosed();

    _recman.setNamedObject(name, recid);
  }

  private void checkIfClosed() throws IllegalStateException {
    if (_recman == null) {
      throw new IllegalStateException("RecordManager has been closed");
    }
  }

  protected void updateCacheEntries() throws IOException {
    Enumeration enume = _cache.elements();
    while (enume.hasMoreElements()) {
      CacheEntry entry = (CacheEntry) enume.nextElement();
      if (entry._isDirty) {
        _recman.update(entry._recid, entry._obj, entry._serializer);
        entry._isDirty = false;
      }
    }
  }

  private class CacheEntry {

    long _recid;
    Object _obj;
    Serializer _serializer;
    boolean _isDirty;

    CacheEntry(long recid, Object obj, Serializer serializer, boolean isDirty) {
      _recid = recid;
      _obj = obj;
      _serializer = serializer;
      _isDirty = isDirty;
    }

  }

  private class CacheListener implements CachePolicyListener {

    public void cacheObjectEvicted(Object obj) throws CacheEvictionException {
      CacheEntry entry = (CacheEntry) obj;
      if (entry._isDirty) {
        try {
          _recman.update(entry._recid, entry._obj, entry._serializer);
        } catch (IOException except) {
          throw new CacheEvictionException(except);
        }
      }
    }

  }
}
