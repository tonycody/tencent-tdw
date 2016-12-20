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

import java.io.File;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.util.jdbm.RecordManager;
import org.apache.hadoop.hive.ql.util.jdbm.RecordManagerFactory;
import org.apache.hadoop.hive.ql.util.jdbm.RecordManagerOptions;
import org.apache.hadoop.hive.ql.util.jdbm.htree.HTree;
import org.apache.hadoop.hive.ql.util.jdbm.helper.FastIterator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.persistence.MRU;
import org.apache.hadoop.hive.ql.exec.persistence.DCLLItem;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HashMapWrapper<K, V> {

  protected Log LOG = LogFactory.getLog(this.getClass().getName());

  private static final int THRESHOLD = 25000;

  private static final int TMPFILESIZE = 1 * 1024 * 1024 * 1024;

  private int threshold;
  private HashMap<K, MRUItem> mHash;
  private HTree pHash;
  private RecordManager recman;
  private File tmpFile;
  private MRU<MRUItem> MRUList;
  private Configuration conf;

  class MRUItem extends DCLLItem {
    K key;
    V value;

    MRUItem(K k, V v) {
      key = k;
      value = v;
    }
  }

  public HashMapWrapper(int threshold,Configuration conf) {
    this.threshold = threshold;
    this.pHash = null;
    this.recman = null;
    this.tmpFile = null;
    this.conf = conf;
    mHash = new HashMap<K, MRUItem>();
    MRUList = new MRU<MRUItem>();
  }

  public HashMapWrapper() {
    this(THRESHOLD,null);
  }

  public V get(K key) throws HiveException {
    V value = null;

    MRUItem item = mHash.get(key);
    if (item != null) {
      value = item.value;
      MRUList.moveToHead(item);
    } else if (pHash != null) {
      try {
        value = (V) pHash.get(key);
        if (value != null) {
          if (mHash.size() < threshold) {
            mHash.put(key, new MRUItem(key, value));
            pHash.remove(key);
          } else if (threshold > 0) {
            MRUItem tail = MRUList.tail();
            pHash.put(tail.key, tail.value);
            pHash.remove(key);
            recman.commit();

            item = mHash.remove(tail.key);
            item.key = key;
            item.value = value;
            mHash.put(key, item);

            tail.key = key;
            tail.value = value;
            MRUList.moveToHead(tail);
          }
        }
      } catch (Exception e) {
        LOG.warn(e.toString());
        throw new HiveException(e);
      }
    }
    return value;
  }

  public void put(K key, V value) throws HiveException {
    int mm_size = mHash.size();
    MRUItem itm = mHash.get(key);

    if (mm_size < threshold) {
      if (itm != null) {
        itm.value = value;
        MRUList.moveToHead(itm);
        if (!mHash.get(key).value.equals(value))
          LOG.error("HashMapWrapper.put() reuse MRUItem inconsistency [1].");
        assert (mHash.get(key).value.equals(value));
      } else {
        try {
          if (pHash != null && pHash.get(key) != null) {
            pHash.remove(key);
            pHash.put(key, value);
            recman.commit();
            return;
          }
        } catch (Exception e) {
          e.printStackTrace();
          throw new HiveException(e);
        }
        itm = new MRUItem(key, value);
        MRUList.put(itm);
        mHash.put(key, itm);
      }
    } else {
      if (itm != null) {
        itm.value = value;
        MRUList.moveToHead(itm);
        if (!mHash.get(key).value.equals(value))
          LOG.error("HashMapWrapper.put() reuse MRUItem inconsistency [2].");
        assert (mHash.get(key).value.equals(value));
      } else {
        if (pHash == null) {
          pHash = getPersistentHash();
        }
        try {
          if (tmpFile != null && tmpFile.length() >= TMPFILESIZE)
            throw new HiveException(tmpFile.getAbsolutePath()
                + " size is over " + TMPFILESIZE);
          pHash.put(key, value);
          recman.commit();
        } catch (Exception e) {
          LOG.warn(e.toString());
          throw new HiveException(e);
        }
      }
    }
  }

  private HTree getPersistentHash() throws HiveException {
    try {
      if (tmpFile != null) {
        tmpFile.delete();
      }
      tmpFile = File.createTempFile("HashMapWpr" + conf.get("hive.query.id", ""), ".tmp", new File(Utilities.getRandomRctmpDir(conf)));
      LOG.info("HashMapWrapper created temp file " + tmpFile.getAbsolutePath());
      tmpFile.deleteOnExit();

      Properties props = new Properties();
      props.setProperty(RecordManagerOptions.CACHE_TYPE,
          RecordManagerOptions.NO_CACHE);
      props.setProperty(RecordManagerOptions.DISABLE_TRANSACTIONS, "true");

      recman = RecordManagerFactory.createRecordManager(tmpFile, props);
      pHash = HTree.createInstance(recman);
    } catch (Exception e) {
      LOG.warn(e.toString());
      throw new HiveException(e);
    }
    return pHash;
  }

  public void clear() throws HiveException {
    if (mHash != null) {
      mHash.clear();
      MRUList.clear();
    }
    close();
  }

  public void remove(Object key) throws HiveException {
    MRUItem entry = mHash.remove(key);
    if (entry != null) {
      MRUList.remove(entry);
    } else if (pHash != null) {
      try {
        pHash.remove(key);
      } catch (Exception e) {
        LOG.warn(e.toString());
        throw new HiveException(e);
      }
    }
  }

  public Set<K> keySet() {
    HashSet<K> ret = null;
    if (mHash != null) {
      ret = new HashSet<K>();
      ret.addAll(mHash.keySet());
    }
    if (pHash != null) {
      try {
        FastIterator fitr = pHash.keys();
        if (fitr != null) {
          K k;
          while ((k = (K) fitr.next()) != null)
            ret.add(k);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return ret;
  }

  public int cacheSize() {
    return threshold;
  }

  public void close() throws HiveException {

    if (pHash != null) {
      try {
        if (recman != null)
          recman.close();
      } catch (Exception e) {
        throw new HiveException(e);
      }
      tmpFile.delete();
      tmpFile = null;
      pHash = null;
      recman = null;
    }
  }
}
