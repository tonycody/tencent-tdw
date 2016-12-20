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

package org.apache.hadoop.hive.serde2.typeinfo;

import java.io.Serializable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;

public abstract class TypeInfo implements Serializable {

  protected TypeInfo() {
  }

  public abstract Category getCategory();

  public abstract String getTypeName();

  public String toString() {
    return getTypeName();
  }

  public boolean equals(Object o) {
    if (!(o instanceof TypeInfo))
      return false;
    TypeInfo dest = (TypeInfo) o;
    if (getCategory() != dest.getCategory())
      return false;

    if (getTypeName() != null && dest.getTypeName() != null) {
      if (!getTypeName().equals(dest.getTypeName())) {
        return false;
      }
    } else if (getTypeName() != null || dest.getTypeName() != null) {
      return false;
    }

    return true;
  }
}
