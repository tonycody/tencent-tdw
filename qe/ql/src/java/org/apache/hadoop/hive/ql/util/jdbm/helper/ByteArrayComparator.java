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

package org.apache.hadoop.hive.ql.util.jdbm.helper;

import java.util.Comparator;
import java.io.Serializable;

public final class ByteArrayComparator implements Comparator, Serializable {

  final static long serialVersionUID = 1L;

  public int compare(Object obj1, Object obj2) {
    if (obj1 == null) {
      throw new IllegalArgumentException("Argument 'obj1' is null");
    }

    if (obj2 == null) {
      throw new IllegalArgumentException("Argument 'obj2' is null");
    }

    return compareByteArray((byte[]) obj1, (byte[]) obj2);
  }

  public static int compareByteArray(byte[] thisKey, byte[] otherKey) {
    int len = Math.min(thisKey.length, otherKey.length);

    for (int i = 0; i < len; i++) {
      if (thisKey[i] >= 0) {
        if (otherKey[i] >= 0) {
          if (thisKey[i] < otherKey[i]) {
            return -1;
          } else if (thisKey[i] > otherKey[i]) {
            return 1;
          }
        } else {
          return -1;
        }
      } else {
        if (otherKey[i] >= 0) {
          return 1;
        } else {
          if (thisKey[i] < otherKey[i]) {
            return -1;
          } else if (thisKey[i] > otherKey[i]) {
            return 1;
          }
        }
      }
    }
    if (thisKey.length == otherKey.length) {
      return 0;
    }
    if (thisKey.length < otherKey.length) {
      return -1;
    }
    return 1;
  }

}
