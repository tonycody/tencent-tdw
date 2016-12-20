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
package org.apache.hadoop.hive.Format.StorageEngineClient;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import StorageEngineClient.CombineFileInputFormat;
import StorageEngineClient.CombineTextFileInputFormat;

import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.NodeBase;

import StorageEngineClient.CombineFileSplit;

import junit.framework.TestCase;

public class TestCombineFormatInputFormat extends TestCase {
  private long maxSplitSize = 0;
  private long minSplitSizeNode = 0;
  private long minSplitSizeRack = 0;
  private long minSizeNode = 0;
  private long minSizeRack = 0;
  private long maxSize = 0;
  private JobConf job = null;
  Class cl = CombineFileInputFormat.class;
  Class cl1 = CombineTextFileInputFormat.class;
  private Method m1 = null;

  protected void setUp() {
    HiveConf conf = new HiveConf(TestCombineFormatInputFormat.class);
    job = new JobConf(conf);

    if (minSplitSizeNode != 0) {
      minSizeNode = minSplitSizeNode;
    } else {
      minSizeNode = job.getLong("mapred.min.split.size.per.node", 0);
    }
    if (minSplitSizeRack != 0) {
      minSizeRack = minSplitSizeRack;
    } else {
      minSizeRack = job.getLong("mapred.min.split.size.per.rack", 0);
    }
    if (maxSplitSize != 0) {
      maxSize = maxSplitSize;
    } else {
      maxSize = job.getLong("mapred.max.split.size", 0);
    }
    if (maxSize == 0) {
      maxSize = (long) (job.getLong("dfs.block.size", 512 * 1024 * 1024) * 0.8);
    }
    if (minSizeNode == 0) {
      minSizeNode = maxSize / 2;
    }
    if (minSizeRack == 0) {
      minSizeRack = maxSize / 2;
    }

    try {
      if (minSizeNode != 0 && maxSize != 0 && minSizeNode > maxSize) {

        throw new IOException("Minimum split size pernode " + minSizeNode
            + " cannot be larger than maximum split size " + maxSize);
      }

      if (minSizeRack != 0 && maxSize != 0 && minSizeRack > maxSize) {
        throw new IOException("Minimum split size per rack" + minSizeRack
            + " cannot be larger than maximum split size " + maxSize);
      }
      if (minSizeRack != 0 && minSizeNode > minSizeRack) {
        throw new IOException("Minimum split size per node" + minSizeNode
            + " cannot be smaller than minimum split size per rack "
            + minSizeRack);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      for (Method onemethod : cl.getDeclaredMethods()) {
        if (onemethod.getName() == "processsplitForUnsplit") {
          m1 = onemethod;
          break;
        }
      }
      m1.setAccessible(true);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  public void testCombineFormatInputFormat() throws IOException {
    List<CombineFileSplit> splits = new ArrayList<CombineFileSplit>();

    long[] filelength = { 1207715618L, 1263719877L, 4562527474L, 3547617779L,
        53 };
    String[] hosts = new String[] { "10.136.4.90", "10.136.138.225" };
    ArrayList<String[]> hostss = new ArrayList<String[]>(5);
    for (int i = 0; i < 5; i++) {
      hostss.add(hosts);
    }

    try {
      m1.invoke(cl1.newInstance(), job, null,
          CombineFileInputFormat.constructBlockToNodes(filelength, hostss),
          maxSize, minSizeNode, minSizeRack, splits, "all");
    } catch (Exception e) {
      e.printStackTrace();
    }

    assert splits.size() == 5;
  }

  public void testCombineFormatInputFormat1() throws IOException {
    List<CombineFileSplit> splits = new ArrayList<CombineFileSplit>();

    long[] filelength = { 207715618L, 263719877L, 562527474L, 547617779L, 53 };
    String[] hosts = new String[] { "10.136.4.90", "10.136.138.225" };
    ArrayList<String[]> hostss = new ArrayList<String[]>(5);
    for (int i = 0; i < 5; i++) {
      hostss.add(hosts);
    }

    try {
      m1.invoke(cl1.newInstance(), job, null,
          CombineFileInputFormat.constructBlockToNodes(filelength, hostss),
          maxSize, minSizeNode, minSizeRack, splits, "all");
    } catch (Exception e) {
      e.printStackTrace();
    }

    assert splits.size() == 4;
    assert splits.get(2).getLengths().length == 2;
  }

  public void testCombineFormatInputFormat2() throws IOException {
    List<CombineFileSplit> splits = new ArrayList<CombineFileSplit>();

    long[] filelength = { 5618L, 9877L, 7474L, 7779L, 53 };
    ArrayList<String[]> hostss = new ArrayList<String[]>(5);
    String[] hosts = new String[] { "10.136.4.90", "10.128.36.39",
        "10.136.138.225" };
    hostss.add(hosts);
    hostss.add(hosts);
    hostss.add(hosts);
    hosts = new String[] { "10.136.4.91", "10.128.37.39", "10.136.138.226" };
    hostss.add(hosts);
    hostss.add(hosts);

    try {
      m1.invoke(cl1.newInstance(), job, null,
          CombineFileInputFormat.constructBlockToNodes(filelength, hostss),
          maxSize, minSizeNode, minSizeRack, splits, "all");
    } catch (Exception e) {
      e.printStackTrace();
    }

    assert splits.size() == 1;
    assert splits.get(0).getLengths().length == 5;
    String[] tmp = splits.get(0).getLocations();
    Arrays.sort(tmp);
    assert StringUtils.join(tmp, ',').equals(
        "10.128.36.39,10.136.138.225,10.136.4.90") == true;
  }
}
