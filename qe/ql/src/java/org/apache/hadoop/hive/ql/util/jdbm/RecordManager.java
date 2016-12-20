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

package org.apache.hadoop.hive.ql.util.jdbm;

import java.io.IOException;
import org.apache.hadoop.hive.ql.util.jdbm.helper.Serializer;

public interface RecordManager {

  public static final int NAME_DIRECTORY_ROOT = 0;

  public abstract long insert(Object obj) throws IOException;

  public abstract long insert(Object obj, Serializer serializer)
      throws IOException;

  public abstract void delete(long recid) throws IOException;

  public abstract void update(long recid, Object obj) throws IOException;

  public abstract void update(long recid, Object obj, Serializer serializer)
      throws IOException;

  public abstract Object fetch(long recid) throws IOException;

  public abstract Object fetch(long recid, Serializer serializer)
      throws IOException;

  public abstract void close() throws IOException;

  public abstract int getRootCount();

  public abstract long getRoot(int id) throws IOException;

  public abstract void setRoot(int id, long rowid) throws IOException;

  public abstract void commit() throws IOException;

  public abstract void rollback() throws IOException;

  public abstract long getNamedObject(String name) throws IOException;

  public abstract void setNamedObject(String name, long recid)
      throws IOException;

}
