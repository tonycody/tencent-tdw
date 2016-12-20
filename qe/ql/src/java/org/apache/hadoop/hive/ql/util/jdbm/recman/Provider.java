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
import java.util.Properties;

import org.apache.hadoop.hive.ql.util.jdbm.RecordManager;
import org.apache.hadoop.hive.ql.util.jdbm.RecordManagerOptions;
import org.apache.hadoop.hive.ql.util.jdbm.RecordManagerProvider;

import org.apache.hadoop.hive.ql.util.jdbm.helper.MRU;

public final class Provider implements RecordManagerProvider {

  public RecordManager createRecordManager(String name, Properties options)
      throws IOException {
    RecordManager recman;

    recman = new BaseRecordManager(name);
    recman = getCachedRecordManager(recman, options);
    return recman;
  }

  private RecordManager getCachedRecordManager(RecordManager recman,
      Properties options) {
    String value;
    int cacheSize;

    value = options.getProperty(RecordManagerOptions.DISABLE_TRANSACTIONS,
        "false");
    if (value.equalsIgnoreCase("TRUE")) {
      ((BaseRecordManager) recman).disableTransactions();
    }

    value = options.getProperty(RecordManagerOptions.CACHE_SIZE, "1000");
    cacheSize = Integer.parseInt(value);

    value = options.getProperty(RecordManagerOptions.CACHE_TYPE,
        RecordManagerOptions.NORMAL_CACHE);
    if (value.equalsIgnoreCase(RecordManagerOptions.NORMAL_CACHE)) {
      MRU cache = new MRU(cacheSize);
      recman = new CacheRecordManager(recman, cache);
    } else if (value.equalsIgnoreCase(RecordManagerOptions.SOFT_REF_CACHE)) {
      throw new IllegalArgumentException("Soft reference cache not implemented");
    } else if (value.equalsIgnoreCase(RecordManagerOptions.WEAK_REF_CACHE)) {
      throw new IllegalArgumentException("Weak reference cache not implemented");
    } else if (value.equalsIgnoreCase(RecordManagerOptions.NO_CACHE)) {
    } else {
      throw new IllegalArgumentException("Invalid cache type: " + value);
    }

    return recman;
  }

  public RecordManager createRecordManager(File file, Properties options)
      throws IOException {
    RecordManager recman = new BaseRecordManager(file);
    recman = getCachedRecordManager(recman, options);
    return recman;
  }

}
