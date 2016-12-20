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
package org.apache.hadoop.hive.serde2.lazy;

import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazyUnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;

public class LazyUnion extends LazyNonPrimitive<LazyUnionObjectInspector> {

  private boolean parsed;

  private int startPosition;

  private LazyObject<? extends ObjectInspector> field;

  private byte tag;

  private boolean fieldInited = false;

  public LazyUnion(LazyUnionObjectInspector oi) {
    super(oi);
  }

  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    super.init(bytes, start, length);
    parsed = false;
  }

  @SuppressWarnings("unchecked")
  private void parse() {

    byte separator = oi.getSeparator();
    boolean isEscaped = oi.isEscaped();
    byte escapeChar = oi.getEscapeChar();
    boolean tagStarted = false;
    boolean tagParsed = false;
    int tagStart = -1;
    int tagEnd = -1;

    int unionByteEnd = start + length;
    int fieldByteEnd = start;
    byte[] bytes = this.bytes.getData();
    while (fieldByteEnd < unionByteEnd) {
      if (bytes[fieldByteEnd] != separator) {
        if (isEscaped && bytes[fieldByteEnd] == escapeChar
            && fieldByteEnd + 1 < unionByteEnd) {
          fieldByteEnd += 1;
        } else {
          if (!tagStarted) {
            tagStart = fieldByteEnd;
            tagStarted = true;
          }
        }
      } else {
        if (!tagParsed) {
          tagEnd = fieldByteEnd - 1;
          startPosition = fieldByteEnd + 1;
          tagParsed = true;
        }
      }
      fieldByteEnd++;
    }

    tag = LazyByte.parseByte(bytes, tagStart, (tagEnd - tagStart) + 1);
    field = LazyFactory.createLazyObject(oi.getObjectInspectors().get(tag));
    fieldInited = false;
    parsed = true;
  }

  private Object uncheckedGetField() {
    Text nullSequence = oi.getNullSequence();
    int fieldLength = start + length - startPosition;
    if (fieldLength != 0
        && fieldLength == nullSequence.getLength()
        && LazyUtils.compare(bytes.getData(), startPosition, fieldLength,
            nullSequence.getBytes(), 0, nullSequence.getLength()) == 0) {
      return null;
    }

    if (!fieldInited) {
      fieldInited = true;
      field.init(bytes, startPosition, fieldLength);
    }
    return field.getObject();
  }

  public Object getField() {
    if (!parsed) {
      parse();
    }
    return uncheckedGetField();
  }

  public byte getTag() {
    if (!parsed) {
      parse();
    }
    return tag;
  }
}
