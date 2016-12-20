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

import java.io.File;
import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.util.jdbm.RecordManager;
import org.apache.hadoop.hive.ql.util.jdbm.helper.Serializer;
import org.apache.hadoop.hive.ql.util.jdbm.helper.DefaultSerializer;

public final class BaseRecordManager implements RecordManager {

  private RecordFile _file;

  private PhysicalRowIdManager _physMgr;

  private LogicalRowIdManager _logMgr;

  private PageManager _pageman;

  public static final int NAME_DIRECTORY_ROOT = 0;

  public static final boolean DEBUG = false;

  private Map _nameDirectory;

  public BaseRecordManager(String filename) throws IOException {
    _file = new RecordFile(filename);
    _pageman = new PageManager(_file);
    _physMgr = new PhysicalRowIdManager(_file, _pageman);
    _logMgr = new LogicalRowIdManager(_file, _pageman);
  }

  public BaseRecordManager(File file) throws IOException {
    _file = new RecordFile(file);
    _pageman = new PageManager(_file);
    _physMgr = new PhysicalRowIdManager(_file, _pageman);
    _logMgr = new LogicalRowIdManager(_file, _pageman);
  }

  public synchronized TransactionManager getTransactionManager() {
    checkIfClosed();

    return _file.txnMgr;
  }

  public synchronized void disableTransactions() {
    checkIfClosed();

    _file.disableTransactions();
  }

  public synchronized void close() throws IOException {
    checkIfClosed();

    _pageman.close();
    _pageman = null;

    _file.close();
    _file = null;
  }

  public long insert(Object obj) throws IOException {
    return insert(obj, DefaultSerializer.INSTANCE);
  }

  public synchronized long insert(Object obj, Serializer serializer)
      throws IOException {
    byte[] data;
    long recid;
    Location physRowId;

    checkIfClosed();

    data = serializer.serialize(obj);
    physRowId = _physMgr.insert(data, 0, data.length);
    recid = _logMgr.insert(physRowId).toLong();
    if (DEBUG) {
      System.out.println("BaseRecordManager.insert() recid " + recid
          + " length " + data.length);
    }
    return recid;
  }

  public synchronized void delete(long recid) throws IOException {
    checkIfClosed();
    if (recid <= 0) {
      throw new IllegalArgumentException("Argument 'recid' is invalid: "
          + recid);
    }

    if (DEBUG) {
      System.out.println("BaseRecordManager.delete() recid " + recid);
    }

    Location logRowId = new Location(recid);
    Location physRowId = _logMgr.fetch(logRowId);
    _physMgr.delete(physRowId);
    _logMgr.delete(logRowId);
  }

  public void update(long recid, Object obj) throws IOException {
    update(recid, obj, DefaultSerializer.INSTANCE);
  }

  public synchronized void update(long recid, Object obj, Serializer serializer)
      throws IOException {
    checkIfClosed();
    if (recid <= 0) {
      throw new IllegalArgumentException("Argument 'recid' is invalid: "
          + recid);
    }

    Location logRecid = new Location(recid);
    Location physRecid = _logMgr.fetch(logRecid);

    byte[] data = serializer.serialize(obj);
    if (DEBUG) {
      System.out.println("BaseRecordManager.update() recid " + recid
          + " length " + data.length);
    }

    Location newRecid = _physMgr.update(physRecid, data, 0, data.length);
    if (!newRecid.equals(physRecid)) {
      _logMgr.update(logRecid, newRecid);
    }
  }

  public Object fetch(long recid) throws IOException {
    return fetch(recid, DefaultSerializer.INSTANCE);
  }

  public synchronized Object fetch(long recid, Serializer serializer)
      throws IOException {
    byte[] data;

    checkIfClosed();
    if (recid <= 0) {
      throw new IllegalArgumentException("Argument 'recid' is invalid: "
          + recid);
    }
    data = _physMgr.fetch(_logMgr.fetch(new Location(recid)));
    if (DEBUG) {
      System.out.println("BaseRecordManager.fetch() recid " + recid
          + " length " + data.length);
    }
    return serializer.deserialize(data);
  }

  public int getRootCount() {
    return FileHeader.NROOTS;
  }

  public synchronized long getRoot(int id) throws IOException {
    checkIfClosed();

    return _pageman.getFileHeader().getRoot(id);
  }

  public synchronized void setRoot(int id, long rowid) throws IOException {
    checkIfClosed();

    _pageman.getFileHeader().setRoot(id, rowid);
  }

  public long getNamedObject(String name) throws IOException {
    checkIfClosed();

    Map nameDirectory = getNameDirectory();
    Long recid = (Long) nameDirectory.get(name);
    if (recid == null) {
      return 0;
    }
    return recid.longValue();
  }

  public void setNamedObject(String name, long recid) throws IOException {
    checkIfClosed();

    Map nameDirectory = getNameDirectory();
    if (recid == 0) {
      nameDirectory.remove(name);
    } else {
      nameDirectory.put(name, new Long(recid));
    }
    saveNameDirectory(nameDirectory);
  }

  public synchronized void commit() throws IOException {
    checkIfClosed();

    _pageman.commit();
  }

  public synchronized void rollback() throws IOException {
    checkIfClosed();

    _pageman.rollback();
  }

  private Map getNameDirectory() throws IOException {
    long nameDirectory_recid = getRoot(NAME_DIRECTORY_ROOT);
    if (nameDirectory_recid == 0) {
      _nameDirectory = new HashMap();
      nameDirectory_recid = insert(_nameDirectory);
      setRoot(NAME_DIRECTORY_ROOT, nameDirectory_recid);
    } else {
      _nameDirectory = (Map) fetch(nameDirectory_recid);
    }
    return _nameDirectory;
  }

  private void saveNameDirectory(Map directory) throws IOException {
    long recid = getRoot(NAME_DIRECTORY_ROOT);
    if (recid == 0) {
      throw new IOException("Name directory must exist");
    }
    update(recid, _nameDirectory);
  }

  private void checkIfClosed() throws IllegalStateException {
    if (_file == null) {
      throw new IllegalStateException("RecordManager has been closed");
    }
  }
}
