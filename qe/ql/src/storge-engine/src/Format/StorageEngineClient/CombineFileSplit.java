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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

@SuppressWarnings("deprecation")
public class CombineFileSplit implements InputSplit {

  private Path[] paths;
  private long[] startoffset;
  private long[] lengths;
  private String[] locations;
  private long totLength;
  private JobConf job;

  public CombineFileSplit() {
  }

  public CombineFileSplit(JobConf job, Path[] files, long[] start,
      long[] lengths, String[] locations) {
    initSplit(job, files, start, lengths, locations);
  }

  public CombineFileSplit(JobConf job, Path[] files, long[] lengths) {
    long[] startoffset = new long[files.length];
    for (int i = 0; i < startoffset.length; i++) {
      startoffset[i] = 0;
    }
    String[] locations = new String[files.length];
    for (int i = 0; i < locations.length; i++) {
      locations[i] = "";
    }
    initSplit(job, files, startoffset, lengths, locations);
  }

  private void initSplit(JobConf job, Path[] files, long[] start,
      long[] lengths, String[] locations) {
    this.job = job;
    this.startoffset = start;
    this.lengths = lengths;
    this.paths = files;
    this.totLength = 0;
    this.locations = locations;
    for (long length : lengths) {
      totLength += length;
    }
  }

  public CombineFileSplit(CombineFileSplit old) throws IOException {
    this(old.getJob(), old.getPaths(), old.getStartOffsets(), old.getLengths(),
        old.getLocations());
  }

  public JobConf getJob() {
    return job;
  }

  public long getLength() {
    return totLength;
  }

  public long[] getStartOffsets() {
    return startoffset;
  }

  public long[] getLengths() {
    return lengths;
  }

  public long getOffset(int i) {
    return startoffset[i];
  }

  public long getLength(int i) {
    return lengths[i];
  }

  public int getNumPaths() {
    return paths.length;
  }

  public Path getPath(int i) {
    return paths[i];
  }

  public Path[] getPaths() {
    return paths;
  }

  public String[] getLocations() throws IOException {
    return locations;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    totLength = in.readLong();
    int arrLength = in.readInt();
    lengths = new long[arrLength];
    for (int i = 0; i < arrLength; i++) {
      lengths[i] = in.readLong();
    }
    int filesLength = in.readInt();
    paths = new Path[filesLength];
    for (int i = 0; i < filesLength; i++) {
      paths[i] = new Path(Text.readString(in));
    }
    arrLength = in.readInt();
    startoffset = new long[arrLength];
    for (int i = 0; i < arrLength; i++) {
      startoffset[i] = in.readLong();
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(totLength);
    out.writeInt(lengths.length);
    for (long length : lengths) {
      out.writeLong(length);
    }
    out.writeInt(paths.length);
    for (Path p : paths) {
      Text.writeString(out, p.toString());
    }
    out.writeInt(startoffset.length);
    for (long length : startoffset) {
      out.writeLong(length);
    }
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < paths.length; i++) {
      if (i == 0) {
        sb.append("Paths:");
      }
      sb.append(paths[i].toUri().getPath() + ":" + startoffset[i] + "+"
          + lengths[i]);
      if (i < paths.length - 1) {
        sb.append(",");
      }
    }
    if (locations != null) {
      String locs = "";
      StringBuffer locsb = new StringBuffer();
      for (int i = 0; i < locations.length; i++) {
        locsb.append(locations[i] + ":");
      }
      locs = locsb.toString();
      sb.append(" Locations:" + locs + "; ");
    }
    return sb.toString();
  }
}
