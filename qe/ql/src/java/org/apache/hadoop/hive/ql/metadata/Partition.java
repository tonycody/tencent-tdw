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

package org.apache.hadoop.hive.ql.metadata;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryBuffer;

public class Partition {

  @SuppressWarnings("nls")
  static final private Log LOG = LogFactory
      .getLog("hive.ql.metadata.Partition");

  private Table table;
  private org.apache.hadoop.hive.metastore.api.Partition tPartition;

  public org.apache.hadoop.hive.metastore.api.Partition getTPartition() {
    return tPartition;
  }

  private LinkedHashMap<String, String> spec;

  private Path partPath;
  private URI partURI;

  public Partition(Table tbl, org.apache.hadoop.hive.metastore.api.Partition tp)
      throws HiveException {
    initialize(tbl, tp);
  }

  public Partition(Table tbl, Map<String, String> partSpec, Path location)
      throws HiveException {

    List<String> pvals = new ArrayList<String>();
    org.apache.hadoop.hive.metastore.api.Partition tpart = new org.apache.hadoop.hive.metastore.api.Partition();

  }

  private void initialize(Table tbl,
      org.apache.hadoop.hive.metastore.api.Partition tp) throws HiveException {

  }

  public String getName() {
    return partName;
  }

  public Table getTable() {
    return table;
  }

  public Path[] getPath() {
    Path[] ret = new Path[1];
    ret[0] = partPath;
    return (ret);
  }

  public Path getPartitionPath() {
    return partPath;
  }

  final public URI getDataLocation() {
    return partURI;
  }

  public int getBucketCount() {
    return table.getNumBuckets();

  }

  public List<String> getBucketCols() {
    return table.getBucketCols();
  }

  @SuppressWarnings("nls")
  public Path getBucketPath(int bucketNum) {
    try {
      FileSystem fs = FileSystem.get(table.getDataLocation(), Hive.get()
          .getConf());
      String pathPattern = partPath.toString();
      if (getBucketCount() > 0) {
        pathPattern = pathPattern + "/*";
      }
      LOG.info("Path pattern = " + pathPattern);
      FileStatus srcs[] = fs.globStatus(new Path(pathPattern));
      Arrays.sort(srcs);
      for (FileStatus src : srcs) {
        LOG.info("Got file: " + src.getPath());
      }
      return srcs[bucketNum].getPath();
    } catch (Exception e) {
      throw new RuntimeException("Cannot get bucket path for bucket "
          + bucketNum, e);
    }

  }

  private static Pattern bpattern = Pattern
      .compile("part-([0-9][0-9][0-9][0-9][0-9])");

  private String partName;

  @SuppressWarnings("nls")
  public static int getBucketNum(Path p) {

    return 0;
  }

  @SuppressWarnings("nls")
  public Path[] getPath(Sample s) throws HiveException {
    if (s == null) {
      return getPath();
    } else {
      int bcount = getBucketCount();
      if (bcount == 0) {
        return getPath();
      }

      Dimension d = s.getSampleDimension();
      if (!d.getDimensionId().equals(table.getBucketingDimensionId())) {
        return getPath();
      }

      int scount = s.getSampleFraction();
      ArrayList<Path> ret = new ArrayList<Path>();

      if (bcount == scount) {
        ret.add(getBucketPath(s.getSampleNum() - 1));
      } else if (bcount < scount) {
        if ((scount / bcount) * bcount != scount) {
          throw new HiveException("Sample Count" + scount
              + " is not a multiple of bucket count " + bcount + " for table "
              + table.getName());
        }
        ret.add(getBucketPath((s.getSampleNum() - 1) % bcount));
      } else if (bcount > scount) {
        if ((bcount / scount) * scount != bcount) {
          throw new HiveException("Sample Count" + scount
              + " is not a divisor of bucket count " + bcount + " for table "
              + table.getName());
        }
        for (int i = 0; i < bcount / scount; i++) {
          ret.add(getBucketPath(i * scount + (s.getSampleNum() - 1)));
        }
      }
      return (ret.toArray(new Path[ret.size()]));
    }
  }

  public LinkedHashMap<String, String> getSpec() {
    return spec;
  }

  @SuppressWarnings("nls")
  @Override
  public String toString() {
    String pn = "Invalid Partition";
    try {
      pn = Warehouse.makePartName(spec);
    } catch (MetaException e) {
    }
    return table.toString() + "(" + pn + ")";
  }
}
