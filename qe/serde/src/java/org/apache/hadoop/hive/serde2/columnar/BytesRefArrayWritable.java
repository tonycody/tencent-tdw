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

package org.apache.hadoop.hive.serde2.columnar;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableFactory;

public class BytesRefArrayWritable implements Writable,
    Comparable<BytesRefArrayWritable> {

  private BytesRefWritable[] bytesRefWritables = null;

  private int valid = 0;

  public BytesRefArrayWritable(int capacity) {
    if (capacity < 0) {
      throw new IllegalArgumentException("Capacity can not be negative.");
    }
    bytesRefWritables = new BytesRefWritable[0];
    ensureCapacity(capacity);
  }

  public BytesRefArrayWritable() {
    this(10);
  }

  public int size() {
    return valid;
  }

  public BytesRefWritable get(int index) {
    if (index >= valid) {
      throw new IndexOutOfBoundsException(
          "This BytesRefArrayWritable only has " + valid + " valid values.");
    }
    return bytesRefWritables[index];
  }

  public BytesRefWritable unCheckedGet(int index) {
    return bytesRefWritables[index];
  }

  public void set(int index, BytesRefWritable bytesRefWritable) {
    ensureCapacity(index + 1);
    bytesRefWritables[index] = bytesRefWritable;
    if (valid <= index) {
      valid = index + 1;
    }
  }

  @Override
  public int compareTo(BytesRefArrayWritable other) {
    if (other == null) {
      throw new IllegalArgumentException("Argument can not be null.");
    }
    if (this == other) {
      return 0;
    }
    int sizeDiff = valid - other.valid;
    if (sizeDiff != 0) {
      return sizeDiff;
    }
    for (int i = 0; i < valid; i++) {
      if (other.contains(bytesRefWritables[i])) {
        continue;
      } else {
        return 1;
      }
    }
    return 0;
  }

  public boolean contains(BytesRefWritable bytesRefWritable) {
    if (bytesRefWritable == null) {
      throw new IllegalArgumentException("Argument can not be null.");
    }
    for (int i = 0; i < valid; i++) {
      if (bytesRefWritables[i].equals(bytesRefWritable)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof BytesRefArrayWritable)) {
      return false;
    }
    return compareTo((BytesRefArrayWritable) o) == 0;
  }

  public void clear() {
    valid = 0;
  }

  public void resetValid(int newValidCapacity) {
    ensureCapacity(newValidCapacity);
    valid = newValidCapacity;
  }

  protected void ensureCapacity(int newCapacity) {
    int size = bytesRefWritables.length;
    if (size < newCapacity) {
      bytesRefWritables = Arrays.copyOf(bytesRefWritables, newCapacity);
      while (size < newCapacity) {
        bytesRefWritables[size] = new BytesRefWritable();
        size++;
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int count = in.readInt();
    ensureCapacity(count);
    for (int i = 0; i < count; i++) {
      bytesRefWritables[i].readFields(in);
    }
    valid = count;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(valid);

    for (int i = 0; i < valid; i++) {
      BytesRefWritable cu = bytesRefWritables[i];
      cu.write(out);
    }
  }

  static {
    WritableFactories.setFactory(BytesRefArrayWritable.class,
        new WritableFactory() {

          @Override
          public Writable newInstance() {
            return new BytesRefArrayWritable();
          }

        });
  }
}
