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
package recordio.utils;

public class Constants {
  public static final int BYTE_SIZE = Byte.SIZE / Byte.SIZE;
  public static final int SHORT_SIZE = Short.SIZE / Byte.SIZE;
  public static final int INT_SIZE = Integer.SIZE / Byte.SIZE;

  public static final int MAX_RECORD_SIZE = 32 * 1024 * 1024 - 1;

  public static final int MIN_VARINT_SIZE_OF_RECORD_SIZE = 1;
  public static final int MAX_VARINT_SIZE_OF_RECORD_SIZE = 4;

  public static final byte[] BLOCK_HEADER_MAGIC = { 0x6d, 0x32, 0x47,
      (byte) 0xc9 };

  public static final int BLOCK_HEADER_TYPE_SIZE = 1;
  public static final int BLOCK_HEADER_CHECKSUM_SIZE = SHORT_SIZE;

  public static final int MIN_BLOCK_HEADER_SIZE = BLOCK_HEADER_MAGIC.length
      + BLOCK_HEADER_TYPE_SIZE + MIN_VARINT_SIZE_OF_RECORD_SIZE
      + BLOCK_HEADER_CHECKSUM_SIZE;
  public static final int MAX_BLOCK_HEADER_SIZE = BLOCK_HEADER_MAGIC.length
      + BLOCK_HEADER_TYPE_SIZE + MAX_VARINT_SIZE_OF_RECORD_SIZE
      + BLOCK_HEADER_CHECKSUM_SIZE;

  public static final int MIN_BLOCK_BODY_SIZE = 1;
  public static final int BLOCK_FOOTER_SIZE = INT_SIZE;

  public static final int MIN_BLOCK_SIZE = MIN_BLOCK_HEADER_SIZE
      + MIN_BLOCK_BODY_SIZE + BLOCK_FOOTER_SIZE;

  public static final int CRC32_INIT_VALUE = 0x5a49bae8;

  public static final int BUFFER_BLOCK_SIZE = 4 * 1024;

  public static final int RECORD_SIZE_BUFFER_SIZE = 8 * BUFFER_BLOCK_SIZE;
  public static final int RECORD_BUFFER_SIZE = 32 * BUFFER_BLOCK_SIZE;
}
