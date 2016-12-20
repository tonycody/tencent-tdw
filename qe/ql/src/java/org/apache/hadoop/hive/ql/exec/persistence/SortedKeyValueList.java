/**
* Tencent is pleased to support the open source community by making TDW available.
* Copyright (C) 2014 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use 
* this file except in compliance with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed 
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS 
* OF ANY KIND, either express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/
package org.apache.hadoop.hive.ql.exec.persistence;

import java.io.Externalizable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator.MapJoinObjectCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortedKeyValueList<K, V> {

  protected Log LOG = LogFactory.getLog(this.getClass().getName());

  private static final int THRESHOLD = 2500000;

  private int threshold;
  private ArrayList<Object> mKeyList;
  private ArrayList<V> mValueList;
  private Object currentPersistentKey;
  private V currentPersistentValue;
  private Object lastKey;
  private File pFile;
  private int mIndex;
  private int pIndex;
  private int mSize;
  private int pSize;
  private ObjectInputStream oInput;
  private ObjectOutputStream oOutput;

  public SortedKeyValueList(int threshold) {
    this.threshold = threshold;
    this.mKeyList = new ArrayList<Object>(threshold);
    this.mValueList = new ArrayList<V>(threshold);
    this.currentPersistentKey = null;
    this.currentPersistentValue = null;
    this.lastKey = null;
    this.pFile = null;
    this.mIndex = 0;
    this.pIndex = 0;
    this.mSize = 0;
    this.pSize = 0;
    this.oInput = null;
    this.oOutput = null;
  }

  public SortedKeyValueList() {
    this(THRESHOLD);
  }

  private int compareKeys(Object k1, Object k2) {
    return WritableComparator.get(((WritableComparable) k1).getClass())
        .compare((WritableComparable) k1, (WritableComparable) k2);
  }

  public V get(K key) throws HiveException {

    int compareResult;
    Object key0 = ((ArrayList<Object>) key).get(0);

    if (key0 == null) {
      return null;
    }

    if (lastKey != null) {
      compareResult = compareKeys(lastKey, key0);
      if (compareResult > 0) {
        reset();
      }
    }

    if (mIndex < mSize) {

      do {
        compareResult = compareKeys(mKeyList.get(mIndex), key0);
        if (compareResult == 0) {
          lastKey = key0;
          return mValueList.get(mIndex);
        } else if (compareResult > 0) {
          lastKey = key0;
          return null;
        } else {
          mIndex++;
        }
      } while (mIndex < mSize);

      if (oInput == null) {
        try {
          if (pFile == null) {
            lastKey = key0;
            return null;
          }

          oInput = new ObjectInputStream(new FileInputStream(pFile));
        } catch (FileNotFoundException e) {
          LOG.warn(e.toString());
          throw new HiveException(e);
        } catch (IOException e) {
          LOG.warn(e.toString());
          throw new HiveException(e);
        }
      }

      if (pIndex < pSize) {

        if (currentPersistentKey == null) {
          getNewPersistentKeyValue();
        }

        do {
          compareResult = compareKeys(currentPersistentKey, key0);

          if (compareResult == 0) {
            lastKey = key0;
            return currentPersistentValue;
          } else if (compareResult > 0) {
            lastKey = key0;
            return null;
          } else {
            getNewPersistentKeyValue();
          }
        } while (pIndex < pSize);

        lastKey = key0;
        return null;
      } else {
        lastKey = key0;
        return null;
      }
    } else {

      if (pIndex < pSize) {

        if (currentPersistentKey == null) {
          getNewPersistentKeyValue();
        }

        do {
          compareResult = compareKeys(currentPersistentKey, key0);

          if (compareResult == 0) {
            lastKey = key0;
            return currentPersistentValue;
          } else if (compareResult > 0) {
            lastKey = key0;
            return null;
          } else {
            getNewPersistentKeyValue();
          }

        } while (pIndex < pSize);

        lastKey = key0;
        return null;
      }
    }

    lastKey = key0;
    return null;
  }

  private void reset() throws HiveException {

    this.currentPersistentKey = null;
    this.currentPersistentValue = null;

    if (oInput != null) {
      try {
        oInput.close();
        oInput = new ObjectInputStream(new FileInputStream(pFile));
      } catch (IOException e) {
        LOG.warn(e.toString());
        throw new HiveException(e);
      }
      this.pIndex = 0;
    }

    this.mIndex = 0;
  }

  private void getNewPersistentKeyValue() throws HiveException {

    try {
      MapJoinObjectKey mjoKey = (MapJoinObjectKey) oInput.readObject();
      this.currentPersistentKey = mjoKey.getObj().get(0);
      this.currentPersistentValue = (V) oInput.readObject();
      pIndex++;
    } catch (IOException e) {
      LOG.warn(e.toString());
      throw new HiveException(e);
    } catch (ClassNotFoundException e) {
      LOG.warn(e.toString());
      throw new HiveException(e);
    }

  }

  public void put(K key, V value) throws HiveException {

    Object key0 = ((ArrayList<Object>) key).get(0);

    if (mSize < threshold) {
      mSize++;
      mKeyList.add(key0);
      mValueList.add(value);
      lastKey = key0;
    } else {
      if (oOutput == null) {
        createPersistentFile();
      }

      try {
        if (currentPersistentValue != null)
          oOutput.writeObject(currentPersistentValue);

        oOutput.writeObject(new MapJoinObjectKey(
            MapJoinOperator.metadataKeyTag, (ArrayList<Object>) key));
        oOutput.flush();
        currentPersistentValue = value;
        lastKey = key0;
        pSize++;
      } catch (IOException e) {
        LOG.warn(e.toString());
        throw new HiveException(e);
      }
    }
  }

  public boolean equalLastPutKey(K key) {

    if (lastKey == null)
      return false;

    if (compareKeys(((ArrayList<Object>) key).get(0), lastKey) == 0)
      return true;
    else
      return false;
  }

  public void saveToLastPutValue(Object value) throws HiveException {

    if (pFile == null) {
      try {
        ((MapJoinObjectValue) (mValueList.get(mSize - 1))).getObj().add(
            (ArrayList<Object>) value);
      } catch (HiveException e) {
        LOG.warn(e.toString());
        throw new HiveException(e);
      }
    } else {
      ((MapJoinObjectValue) currentPersistentValue).getObj().add(
          (ArrayList<Object>) value);
    }
  }

  private void createPersistentFile() throws HiveException {
    try {

      if (pFile != null) {
        pFile.delete();
      }

      pFile = File.createTempFile("SortedKeyValueList", ".tmp",
          new File("/tmp"));
      LOG.info("SortedKeyValueList created temp file "
          + pFile.getAbsolutePath());

      pFile.deleteOnExit();

      oOutput = new ObjectOutputStream(new FileOutputStream(pFile));
    } catch (Exception e) {
      LOG.warn(e.toString());
      throw new HiveException(e);
    }
  }

  public void clear() throws HiveException {

    mKeyList.clear();
    mValueList.clear();
    mSize = 0;
    mIndex = 0;
    close();
  }

  public int cacheSize() {

    return threshold;
  }

  public void closePFile() throws HiveException {

    if (pFile != null) {
      try {
        if (currentPersistentValue != null) {
          oOutput.writeObject(currentPersistentValue);
          oOutput.flush();
        }

        currentPersistentValue = null;

        oOutput.close();
        oOutput = null;

        lastKey = null;
      } catch (IOException e) {
        LOG.warn(e.toString());
        throw new HiveException(e);
      }
    }
  }

  public void close() throws HiveException {

    if (pFile != null) {

      try {
        if (oOutput != null)
          oOutput.close();

        if (oInput != null)
          oInput.close();
      } catch (Exception e) {
        throw new HiveException(e);
      }

      pFile.delete();

      oOutput = null;
      oInput = null;
      pFile = null;
      pSize = 0;
      pIndex = -1;
      currentPersistentKey = null;
      lastKey = null;
      currentPersistentValue = null;
    }
  }
}
