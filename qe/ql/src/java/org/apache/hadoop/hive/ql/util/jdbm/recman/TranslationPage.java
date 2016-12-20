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

final class TranslationPage extends PageHeader {
  static final short O_TRANS = PageHeader.SIZE;
  static final short ELEMS_PER_PAGE = (RecordFile.BLOCK_SIZE - O_TRANS)
      / PhysicalRowId.SIZE;

  final PhysicalRowId[] slots = new PhysicalRowId[ELEMS_PER_PAGE];

  TranslationPage(BlockIo block) {
    super(block);
  }

  static TranslationPage getTranslationPageView(BlockIo block) {
    BlockView view = block.getView();
    if (view != null && view instanceof TranslationPage)
      return (TranslationPage) view;
    else
      return new TranslationPage(block);
  }

  PhysicalRowId get(short offset) {
    int slot = (offset - O_TRANS) / PhysicalRowId.SIZE;
    if (slots[slot] == null)
      slots[slot] = new PhysicalRowId(block, offset);
    return slots[slot];
  }
}
