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

final class PageCursor {
  PageManager pageman;
  long current;
  short type;

  PageCursor(PageManager pageman, long current) {
    this.pageman = pageman;
    this.current = current;
  }

  PageCursor(PageManager pageman, short type) throws IOException {
    this.pageman = pageman;
    this.type = type;
  }

  long getCurrent() throws IOException {
    return current;
  }

  long next() throws IOException {
    if (current == 0)
      current = pageman.getFirst(type);
    else
      current = pageman.getNext(current);
    return current;
  }

  long prev() throws IOException {
    current = pageman.getPrev(current);
    return current;
  }
}
