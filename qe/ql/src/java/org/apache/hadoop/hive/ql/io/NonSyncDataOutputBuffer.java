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
package org.apache.hadoop.hive.ql.io;

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.hive.common.io.NonSyncByteArrayOutputStream;

public class NonSyncDataOutputBuffer extends DataOutputStream {

  private NonSyncByteArrayOutputStream buffer;

  public NonSyncDataOutputBuffer() {
    this(new NonSyncByteArrayOutputStream());
  }

  private NonSyncDataOutputBuffer(NonSyncByteArrayOutputStream buffer) {
    super(buffer);
    this.buffer = buffer;
  }

  public byte[] getData() {
    return buffer.getData();
  }

  public int getLength() {
    return buffer.getLength();
  }

  public NonSyncDataOutputBuffer reset() {
    this.written = 0;
    buffer.reset();
    return this;
  }

  public void write(DataInput in, int length) throws IOException {
    buffer.write(in, length);
  }

  public void write(int b) throws IOException {
    buffer.write(b);
    incCount(1);
  }

  public void write(byte b[], int off, int len) throws IOException {
    buffer.write(b, off, len);
    incCount(len);
  }

  private void incCount(int value) {
    if (written + value < 0) {
      written = Integer.MAX_VALUE;
    } else
      written += value;
  }
}
