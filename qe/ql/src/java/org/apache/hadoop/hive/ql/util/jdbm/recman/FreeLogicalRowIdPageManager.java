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

final class FreeLogicalRowIdPageManager {
  private RecordFile file;
  private PageManager pageman;

  FreeLogicalRowIdPageManager(RecordFile file, PageManager pageman)
      throws IOException {
    this.file = file;
    this.pageman = pageman;
  }

  Location get() throws IOException {

    Location retval = null;
    PageCursor curs = new PageCursor(pageman, Magic.FREELOGIDS_PAGE);
    while (curs.next() != 0) {
      FreeLogicalRowIdPage fp = FreeLogicalRowIdPage
          .getFreeLogicalRowIdPageView(file.get(curs.getCurrent()));
      int slot = fp.getFirstAllocated();
      if (slot != -1) {
        retval = new Location(fp.get(slot));
        fp.free(slot);
        if (fp.getCount() == 0) {
          file.release(curs.getCurrent(), false);
          pageman.free(Magic.FREELOGIDS_PAGE, curs.getCurrent());
        } else
          file.release(curs.getCurrent(), true);

        return retval;
      } else {
        file.release(curs.getCurrent(), false);
      }
    }
    return null;
  }

  void put(Location rowid) throws IOException {

    PhysicalRowId free = null;
    PageCursor curs = new PageCursor(pageman, Magic.FREELOGIDS_PAGE);
    long freePage = 0;
    while (curs.next() != 0) {
      freePage = curs.getCurrent();
      BlockIo curBlock = file.get(freePage);
      FreeLogicalRowIdPage fp = FreeLogicalRowIdPage
          .getFreeLogicalRowIdPageView(curBlock);
      int slot = fp.getFirstFree();
      if (slot != -1) {
        free = fp.alloc(slot);
        break;
      }

      file.release(curBlock);
    }
    if (free == null) {
      freePage = pageman.allocate(Magic.FREELOGIDS_PAGE);
      BlockIo curBlock = file.get(freePage);
      FreeLogicalRowIdPage fp = FreeLogicalRowIdPage
          .getFreeLogicalRowIdPageView(curBlock);
      free = fp.alloc(0);
    }
    free.setBlock(rowid.getBlock());
    free.setOffset(rowid.getOffset());
    file.release(freePage, true);
  }
}
