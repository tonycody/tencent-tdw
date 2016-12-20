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

package org.apache.hadoop.hive.ql.hooks;

import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import java.util.Map;
import java.net.URI;

public class ReadEntity {

  private Partition p;

  private Table t;

  public ReadEntity(Table t) {
    this.t = t;
    this.p = null;
  }

  public ReadEntity(Partition p) {
    this.t = p.getTable();
    this.p = p;
  }

  public static enum Type {
    TABLE, PARTITION
  };

  public Type getType() {
    return p == null ? Type.TABLE : Type.PARTITION;
  }

  public Map<String, String> getParameters() {

    return t.getTTable().getParameters();
  }

  public URI getLocation() {
    if (p != null) {
      return p.getDataLocation();
    } else {
      return t.getDataLocation();
    }
  }

  public Partition getPartition() {
    return p;
  }

  public Table getTable() {
    return t;
  }

  @Override
  public String toString() {
    if (p != null) {
      return p.getTable().getDbName() + "/" + p.getTable().getName() + "/"
          + p.getName();
    } else {
      return t.getDbName() + "/" + t.getName();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o == null)
      return false;

    if (o instanceof ReadEntity) {
      ReadEntity ore = (ReadEntity) o;
      return (toString().equalsIgnoreCase(ore.toString()));
    } else
      return false;
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }
}
