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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import FormatStorage1.IRecord;

@SuppressWarnings("deprecation")
public class CombineColumnStorageFileInputFormat extends
    CombineFileInputFormat<LongWritable, IRecord> {
  public static final Log LOG = LogFactory
      .getLog(CombineColumnStorageFileInputFormat.class);

  public CombineColumnStorageFileInputFormat() {
    super();
    LOG.info("new  CombineColumnStorageFileInputFormat");
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {

    long minSizeNode = 0;
    long minSizeRack = 0;
    long maxSize = 0;

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

    Path[] paths = FileUtil.stat2Paths(listStatus(job));
    List<CombineFileSplit> splits = new ArrayList<CombineFileSplit>();
    if (paths.length == 0) {
      return splits.toArray(new CombineFileSplit[splits.size()]);
    }

    for (MultiPathFilter onepool : pools) {
      ArrayList<Path> myPaths = new ArrayList<Path>();

      for (int i = 0; i < paths.length; i++) {
        if (paths[i] == null) {
          continue;
        }
        Path p = new Path(paths[i].toUri().getPath());
        if (onepool.accept(p)) {
          myPaths.add(paths[i]);
          paths[i] = null;
        }
      }
      getMoreSplits(job, myPaths.toArray(new Path[myPaths.size()]), maxSize,
          minSizeNode, minSizeRack, splits);
    }

    ArrayList<Path> myPaths = new ArrayList<Path>();
    for (int i = 0; i < paths.length; i++) {
      if (paths[i] == null) {
        continue;
      }
      myPaths.add(paths[i]);
    }
    LOG.info("myPaths size:\t" + myPaths.size());
    getMoreSplits(job, myPaths.toArray(new Path[myPaths.size()]), maxSize,
        minSizeNode, minSizeRack, splits);
    if (splits.size() == 0)
      return super.getSplits(job, numSplits);
    LOG.info("splits #:\t" + splits.size());
    return splits.toArray(new CombineFileSplit[splits.size()]);
  }

  private void getMoreSplits(JobConf job, Path[] paths1, long maxSize,
      long minSizeNode, long minSizeRack, List<CombineFileSplit> splits)
      throws IOException {
    if (paths1.length == 0) {
      return;
    }

    HashMap<String, Path> strpath = new HashMap<String, Path>();
    HashMap<String, Long> pathmap = new HashMap<String, Long>();
    for (Path p : paths1) {
      if (p.toString().contains("_idx")) {
        String pstr = p.toString().substring(0,
            p.toString().lastIndexOf("_idx"));
        if (!pathmap.containsKey(pstr)) {
          pathmap.put(pstr, 0l);
          strpath.put(pstr, p);
        }
        long len = p.getFileSystem(job).getFileStatus(p).getLen();
        pathmap.put(pstr, pathmap.get(pstr) + len);
      }
    }

    Path[] paths = new Path[pathmap.size()];

    int ii = 0;
    for (String str : pathmap.keySet()) {
      paths[ii++] = strpath.get(str);
    }

    OneFileInfo[] files;

    HashMap<String, List<OneBlockInfo>> rackToBlocks = new HashMap<String, List<OneBlockInfo>>();

    HashMap<OneBlockInfo, String[]> blockToNodes = new HashMap<OneBlockInfo, String[]>();

    HashMap<String, List<OneBlockInfo>> nodeToBlocks = new HashMap<String, List<OneBlockInfo>>();

    files = new OneFileInfo[paths.length];

    long totLength = 0;
    ii = 0;
    for (String str : pathmap.keySet()) {
      files[ii] = new OneFileInfo(strpath.get(str), job, rackToBlocks,
          blockToNodes, nodeToBlocks);
      totLength += pathmap.get(str);
    }
    ArrayList<OneBlockInfo> validBlocks = new ArrayList<OneBlockInfo>();
    ArrayList<String> nodes = new ArrayList<String>();
    long curSplitSize = 0;

    for (Iterator<Map.Entry<String, List<OneBlockInfo>>> iter = nodeToBlocks
        .entrySet().iterator(); iter.hasNext();) {

      Map.Entry<String, List<OneBlockInfo>> one = iter.next();
      nodes.add(one.getKey());
      List<OneBlockInfo> blocksInNode = one.getValue();

      for (OneBlockInfo oneblock : blocksInNode) {
        if (blockToNodes.containsKey(oneblock)) {
          validBlocks.add(oneblock);
          blockToNodes.remove(oneblock);
          curSplitSize += oneblock.length;

          if (maxSize != 0 && curSplitSize >= maxSize) {
            addCreatedSplit(job, splits, nodes, validBlocks);
            curSplitSize = 0;
            validBlocks.clear();
          }
        }
      }
      if (minSizeNode != 0 && curSplitSize >= minSizeNode) {
        addCreatedSplit(job, splits, nodes, validBlocks);
      } else {
        for (OneBlockInfo oneblock : validBlocks) {
          blockToNodes.put(oneblock, oneblock.hosts);
        }
      }
      validBlocks.clear();
      nodes.clear();
      curSplitSize = 0;
    }

    ArrayList<OneBlockInfo> overflowBlocks = new ArrayList<OneBlockInfo>();
    ArrayList<String> racks = new ArrayList<String>();

    while (blockToNodes.size() > 0) {

      for (Iterator<Map.Entry<String, List<OneBlockInfo>>> iter = rackToBlocks
          .entrySet().iterator(); iter.hasNext();) {

        Map.Entry<String, List<OneBlockInfo>> one = iter.next();
        List<OneBlockInfo> blocks = one.getValue();

        boolean createdSplit = false;
        for (OneBlockInfo oneblock : blocks) {
          if (blockToNodes.containsKey(oneblock)) {
            validBlocks.add(oneblock);
            blockToNodes.remove(oneblock);
            curSplitSize += oneblock.length;
            for (int i = 0; i < oneblock.hosts.length; i++) {
              racks.add(oneblock.hosts[i]);
            }

            if (maxSize != 0 && curSplitSize >= maxSize) {
              addCreatedSplit(job, splits, racks, validBlocks);
              createdSplit = true;
              break;
            }
          }
        }

        if (createdSplit) {
          curSplitSize = 0;
          validBlocks.clear();
          racks.clear();
          continue;
        }

        if (!validBlocks.isEmpty()) {
          if (minSizeRack != 0 && curSplitSize >= minSizeRack) {
            addCreatedSplit(job, splits, racks, validBlocks);
          } else {
            overflowBlocks.addAll(validBlocks);
          }
        }
        curSplitSize = 0;
        validBlocks.clear();
        racks.clear();
      }
    }

    assert blockToNodes.isEmpty();
    assert curSplitSize == 0;
    assert validBlocks.isEmpty();
    assert racks.isEmpty();

    for (OneBlockInfo oneblock : overflowBlocks) {
      validBlocks.add(oneblock);
      curSplitSize += oneblock.length;

      for (int i = 0; i < oneblock.racks.length; i++) {
        racks.add(oneblock.hosts[i]);
      }

      if (maxSize != 0 && curSplitSize >= maxSize) {
        addCreatedSplit(job, splits, racks, validBlocks);
        curSplitSize = 0;
        validBlocks.clear();
        racks.clear();
      }
    }

    if (!validBlocks.isEmpty()) {
      addCreatedSplit(job, splits, racks, validBlocks);
    }
  }

  private void addCreatedSplit(JobConf job, List<CombineFileSplit> splitList,
      List<String> racks, ArrayList<OneBlockInfo> validBlocks) {
    Path[] fl = new Path[validBlocks.size()];
    long[] offset = new long[validBlocks.size()];
    long[] length = new long[validBlocks.size()];
    String[] rackLocations = racks.toArray(new String[racks.size()]);
    for (int i = 0; i < validBlocks.size(); i++) {
      fl[i] = validBlocks.get(i).onepath;
      offset[i] = validBlocks.get(i).offset;
      length[i] = validBlocks.get(i).length;
    }

    CombineFileSplit thissplit = new CombineFileSplit(job, fl, offset, length,
        rackLocations);
    splitList.add(thissplit);
  }

  @Override
  public RecordReader<LongWritable, IRecord> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException {
    return new CombineFileRecordReader<LongWritable, IRecord>(job,
        (CombineFileSplit) split, reporter, ColumnStorageIRecordReader.class);
  }
}
