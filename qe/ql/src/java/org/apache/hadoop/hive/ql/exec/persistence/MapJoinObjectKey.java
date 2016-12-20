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

package org.apache.hadoop.hive.ql.exec.persistence;

import java.io.Externalizable;
import java.io.IOException;
import java.lang.Exception;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator.MapJoinObjectCtx;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;

public class MapJoinObjectKey implements Externalizable {

  transient protected int metadataTag;
  transient protected ArrayList<Object> obj;

  public MapJoinObjectKey() {
  }

  public MapJoinObjectKey(int metadataTag, ArrayList<Object> obj) {
    this.metadataTag = metadataTag;
    this.obj = obj;
  }

  public boolean equals(Object o) {
    if (o instanceof MapJoinObjectKey) {
      MapJoinObjectKey mObj = (MapJoinObjectKey) o;
      if (mObj.getMetadataTag() == metadataTag) {
        if ((obj == null) && (mObj.getObj() == null))
          return true;
        if ((obj != null) && (mObj.getObj() != null)
            && (mObj.getObj().equals(obj)))
          return true;
      }
    }

    return false;
  }

  public int hashCode() {
    return (obj == null) ? metadataTag : obj.hashCode();
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException,
      ClassNotFoundException {
    try {
      metadataTag = in.readInt();

      MapJoinObjectCtx ctx = MapJoinOperator.getMapMetadata().get(
          Integer.valueOf(metadataTag));

      Writable val = ctx.getSerDe().getSerializedClass().newInstance();
      val.readFields(in);
      obj = (ArrayList<Object>) ObjectInspectorUtils.copyToStandardObject(ctx
          .getSerDe().deserialize(val), ctx.getSerDe().getObjectInspector(),
          ObjectInspectorCopyOption.WRITABLE);
    } catch (Exception e) {
      throw new IOException(e);
    }

  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    try {
      out.writeInt(metadataTag);

      MapJoinObjectCtx ctx = MapJoinOperator.getMapMetadata().get(
          Integer.valueOf(metadataTag));

      Writable outVal = ctx.getSerDe().serialize(obj, ctx.getStandardOI());
      outVal.write(out);
    } catch (SerDeException e) {
      throw new IOException(e);
    }
  }

  public int getMetadataTag() {
    return metadataTag;
  }

  public void setMetadataTag(int metadataTag) {
    this.metadataTag = metadataTag;
  }

  public ArrayList<Object> getObj() {
    return obj;
  }

  public void setObj(ArrayList<Object> obj) {
    this.obj = obj;
  }

}
