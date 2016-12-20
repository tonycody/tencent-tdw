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
package FormatStorage;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class UnitPoolManager {
  Log LOG = LogFactory.getLog("UnitPoolManager");

  static class PoolInfo {
    int sidx = -1;
    int uidx = -1;
    byte[] buffer = null;
    Segment segment = null;
  }

  static class PoolIndex {
    int used = 0;
    int pos = 0;
  }

  static class PoolStat {
    long request = 0;
    long hit = 0;
    float rate = 0;
  }

  ArrayList<PoolInfo> pool = new ArrayList<PoolInfo>(100);
  ArrayList<PoolIndex> map = new ArrayList<PoolIndex>(100);

  FormatDataFile fd = null;

  int poolSize = 0;

  PoolStat stat = new PoolStat();

  public UnitPoolManager(int poolSize, FormatDataFile fd) {
    this.fd = fd;

    if (poolSize < 0 || poolSize > 1000) {
      this.poolSize = 1;
    } else {
      this.poolSize = poolSize;
    }

    for (int i = 0; i < poolSize; i++) {
      pool.add(new PoolInfo());

      PoolIndex index = new PoolIndex();
      index.used = 0;
      index.pos = i;
      map.add(index);
    }
  }

  public byte[] getUnitBytes(int segIdx, int unitIdx) {
    int pos = -1;
    byte[] buffer = null;
    for (int i = 0; i < poolSize; i++) {
      PoolInfo info = pool.get(i);
      if (info.sidx == segIdx && info.uidx == unitIdx) {
        pos = i;
        buffer = info.buffer;

        break;
      }
    }

    stat.request++;

    if (pos != -1) {
      stat.hit++;

      for (int i = 0; i < map.size(); i++) {
        PoolIndex index = map.get(i);
        if (index.pos == pos) {
          index.used++;

          map.set(i, index);
        }
      }

    } else {
    }

    return buffer;
  }

  public void poolUnitBytes(int segIdx, int unitIdex, byte[] buf,
      Segment segment) {
    int pos = -1;
    int min = Integer.MAX_VALUE;
    for (int i = 0; i < map.size(); i++) {
      PoolIndex index = map.get(i);

      if (index.used < min) {
        min = index.used;
        pos = index.pos;
      }
    }

    if (pos < 0) {
      return;
    }

    PoolInfo info = pool.get(pos);

    if (info.sidx != -1 && info.uidx != -1) {
      if (info.segment != null) {
        info.segment.units().get(info.uidx).releaseUnitBuffer();
      }
    }

    info.sidx = segIdx;
    info.uidx = unitIdex;
    info.buffer = null;
    info.buffer = buf;
    info.segment = segment;

    pool.set(pos, info);
  }
}
