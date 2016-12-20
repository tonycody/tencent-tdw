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

final class LogicalRowIdManager {
  private RecordFile file;
  private PageManager pageman;
  private FreeLogicalRowIdPageManager freeman;

  LogicalRowIdManager(RecordFile file, PageManager pageman) throws IOException {
    this.file = file;
    this.pageman = pageman;
    this.freeman = new FreeLogicalRowIdPageManager(file, pageman);

  }

  Location insert(Location loc) throws IOException {
    Location retval = freeman.get();
    if (retval == null) {
      long firstPage = pageman.allocate(Magic.TRANSLATION_PAGE);
      short curOffset = TranslationPage.O_TRANS;
      for (int i = 0; i < TranslationPage.ELEMS_PER_PAGE; i++) {
        freeman.put(new Location(firstPage, curOffset));
        curOffset += PhysicalRowId.SIZE;
      }
      retval = freeman.get();
      if (retval == null) {
        throw new Error("couldn't obtain free translation");
      }
    }
    update(retval, loc);
    return retval;
  }

  void delete(Location rowid) throws IOException {

    freeman.put(rowid);
  }

  void update(Location rowid, Location loc) throws IOException {

    TranslationPage xlatPage = TranslationPage.getTranslationPageView(file
        .get(rowid.getBlock()));
    PhysicalRowId physid = xlatPage.get(rowid.getOffset());
    physid.setBlock(loc.getBlock());
    physid.setOffset(loc.getOffset());
    file.release(rowid.getBlock(), true);
  }

  Location fetch(Location rowid) throws IOException {

    TranslationPage xlatPage = TranslationPage.getTranslationPageView(file
        .get(rowid.getBlock()));
    try {
      Location retval = new Location(xlatPage.get(rowid.getOffset()));
      return retval;
    } finally {
      file.release(rowid.getBlock(), false);
    }
  }

}
