/**
* Tencent is pleased to support the open source community by making TDW available.
* Copyright (C) 2014 THL A29 Limited, a Tencent company. All rights reserved.
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use 
* this file except in compliance with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed 
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS 
* OF ANY KIND, either express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/
package StorageEngineClient;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

@SuppressWarnings("deprecation")
public class MultiFormatStorageSplit extends FileSplit implements InputSplit {
  public static final Log LOG = LogFactory
      .getLog(MultiFormatStorageSplit.class);

  int beginKey;
  int endKey;
  int beginLine;
  int endLine;
  int index;

  long offset;
  long len;

  private String[] hosts;

  Path[] path = null;
  JobConf conf;

  public MultiFormatStorageSplit() {
    super((Path) null, 0, 0, (String[]) null);
  }

  public MultiFormatStorageSplit(Path[] file, String[] hosts) {
    super((Path) null, 0, 0, (String[]) null);

    if (file == null) {
      this.len = 0;
    } else {
      this.len = file.length;
    }

    this.path = file;
    this.hosts = hosts;

    LOG.info("path.size:" + path.length + ",hosts:" + hosts);
  }

  public int getBeginLine() {
    return beginLine;
  }

  public int getEndLine() {
    return endLine;
  }

  public int getBeginKey() {
    return beginKey;
  }

  public int getEndKey() {
    return endKey;
  }

  public long getStart() {
    return offset;
  }

  public Path getPath() {
    if (path == null) {
      LOG.info("in FileSplit.getPath, path null");
    }
    return path[0];
  }

  public Path[] getAllPath() {
    if (path == null) {
      LOG.info("in FileSplit.getPath, path null");
      return new Path[0];
    }
    return path;
  }

  @Override
  public long getLength() {
    return this.len;
  }

  @Override
  public String[] getLocations() throws IOException {
    if (this.hosts == null) {
      return new String[] {};
    } else {
      return this.hosts;
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    len = in.readInt();
    if (path == null) {
      path = new Path[(int) len];
    }

    for (int i = 0; i < len; i++) {
      short strLen = in.readShort();
      if (strLen > 0) {
        byte[] buf = new byte[strLen];
        in.readFully(buf, 0, strLen);

        String string = new String(buf);
        path[i] = new Path(string);
      }
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (path == null || path.length == 0) {
      out.writeInt(0);
    } else {
      out.writeInt(path.length);
      for (int i = 0; i < path.length; i++) {
        int len = path[i].toString().length();
        if (len == 0) {
          out.writeShort((short) 0);
        } else {
          out.writeShort((short) len);
          out.write(path[i].toString().getBytes());
        }
      }
    }
  }
}
