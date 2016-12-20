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

import java.io.IOException;

final class FreePhysicalRowIdPageManager {
  protected RecordFile _file;

  protected PageManager _pageman;

  FreePhysicalRowIdPageManager(RecordFile file, PageManager pageman)
      throws IOException {
    _file = file;
    _pageman = pageman;
  }

  Location get(int size) throws IOException {
    Location retval = null;
    PageCursor curs = new PageCursor(_pageman, Magic.FREEPHYSIDS_PAGE);

    while (curs.next() != 0) {
      FreePhysicalRowIdPage fp = FreePhysicalRowIdPage
          .getFreePhysicalRowIdPageView(_file.get(curs.getCurrent()));
      int slot = fp.getFirstLargerThan(size);
      if (slot != -1) {
        retval = new Location(fp.get(slot));

        int slotsize = fp.get(slot).getSize();
        fp.free(slot);
        if (fp.getCount() == 0) {
          _file.release(curs.getCurrent(), false);
          _pageman.free(Magic.FREEPHYSIDS_PAGE, curs.getCurrent());
        } else {
          _file.release(curs.getCurrent(), true);
        }

        return retval;
      } else {
        _file.release(curs.getCurrent(), false);
      }

    }
    return null;
  }

  void put(Location rowid, int size) throws IOException {

    FreePhysicalRowId free = null;
    PageCursor curs = new PageCursor(_pageman, Magic.FREEPHYSIDS_PAGE);
    long freePage = 0;
    while (curs.next() != 0) {
      freePage = curs.getCurrent();
      BlockIo curBlock = _file.get(freePage);
      FreePhysicalRowIdPage fp = FreePhysicalRowIdPage
          .getFreePhysicalRowIdPageView(curBlock);
      int slot = fp.getFirstFree();
      if (slot != -1) {
        free = fp.alloc(slot);
        break;
      }

      _file.release(curBlock);
    }
    if (free == null) {
      freePage = _pageman.allocate(Magic.FREEPHYSIDS_PAGE);
      BlockIo curBlock = _file.get(freePage);
      FreePhysicalRowIdPage fp = FreePhysicalRowIdPage
          .getFreePhysicalRowIdPageView(curBlock);
      free = fp.alloc(0);
    }

    free.setBlock(rowid.getBlock());
    free.setOffset(rowid.getOffset());
    free.setSize(size);
    _file.release(freePage, true);
  }
}
