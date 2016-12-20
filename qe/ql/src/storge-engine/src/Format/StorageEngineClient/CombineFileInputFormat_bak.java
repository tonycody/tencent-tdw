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
import java.util.List;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.NetworkTopology;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RecordReader;

@SuppressWarnings("deprecation")
public abstract class CombineFileInputFormat_bak<K, V> extends
    FileInputFormat<K, V> implements JobConfigurable {
  public static final Log LOG = LogFactory.getLog(CombineFileInputFormat.class);

  protected long maxSplitSize = 0;
  protected long minSplitSizeNode = 0;
  protected long minSplitSizeRack = 0;

  protected ArrayList<MultiPathFilter> pools = new ArrayList<MultiPathFilter>();

  protected void setMaxSplitSize(long maxSplitSize) {
    this.maxSplitSize = maxSplitSize;
  }

  protected void setMinSplitSizeNode(long minSplitSizeNode) {
    this.minSplitSizeNode = minSplitSizeNode;
  }

  protected void setMinSplitSizeRack(long minSplitSizeRack) {
    this.minSplitSizeRack = minSplitSizeRack;
  }

  protected void createPool(JobConf conf, List<PathFilter> filters) {
    pools.add(new MultiPathFilter(filters));
  }

  protected void createPool(JobConf conf, PathFilter... filters) {
    MultiPathFilter multi = new MultiPathFilter();
    for (PathFilter f : filters) {
      multi.add(f);
    }
    pools.add(multi);
  }

  public CombineFileInputFormat_bak() {
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
    LOG.info("splits #:\t" + splits.size());

    return splits.toArray(new CombineFileSplit[splits.size()]);
  }

  private void getMoreSplits(JobConf job, Path[] paths1, long maxSize,
      long minSizeNode, long minSizeRack, List<CombineFileSplit> splits)
      throws IOException {
    if (paths1.length == 0) {
      return;
    }

    Path[] paths = paths1;
    ArrayList<Path> splitable = new ArrayList<Path>();
    ArrayList<Path> unsplitable = new ArrayList<Path>();
    for (int i = 0; i < paths1.length; i++) {
      if (isSplitable(paths1[i].getFileSystem(job), paths1[i])) {
        splitable.add(paths1[i]);
      } else {
        unsplitable.add(paths1[i]);
      }
    }
    if (unsplitable.size() != 0) {
      paths = new Path[splitable.size()];
      splitable.toArray(paths);
    }

    OneFileInfo[] files;

    HashMap<String, List<OneBlockInfo>> rackToBlocks = new HashMap<String, List<OneBlockInfo>>();

    HashMap<OneBlockInfo, String[]> blockToNodes = new HashMap<OneBlockInfo, String[]>();

    HashMap<String, List<OneBlockInfo>> nodeToBlocks = new HashMap<String, List<OneBlockInfo>>();

    files = new OneFileInfo[paths.length];

    long totLength = 0;
    for (int i = 0; i < paths.length; i++) {
      files[i] = new OneFileInfo(paths[i], job, rackToBlocks, blockToNodes,
          nodeToBlocks);
      totLength += files[i].getLength();
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

    if (unsplitable.size() != 0) {
      curSplitSize = 0;
      nodes.clear();
      validBlocks.clear();
      for (Path path : unsplitable) {
        FileSystem fs = path.getFileSystem(job);
        FileStatus stat = fs.getFileStatus(path);
        LOG.info(path.toString());
        long len = fs.getFileStatus(path).getLen();
        curSplitSize += len;
        BlockLocation[] locations = path.getFileSystem(job)
            .getFileBlockLocations(stat, 0, len);
        OneBlockInfo oneblock = new OneBlockInfo(path, 0, len,
            locations[0].getHosts(), locations[0].getTopologyPaths());
        validBlocks.add(oneblock);
        for (int i = 0; i < oneblock.hosts.length; i++) {
          nodes.add(oneblock.hosts[i]);
        }

        if (maxSize != 0 && curSplitSize >= maxSize) {
          addCreatedSplit(job, splits, nodes, validBlocks);
          curSplitSize = 0;
          validBlocks.clear();
          nodes.clear();
        }
      }
      if (!validBlocks.isEmpty()) {
        addCreatedSplit(job, splits, nodes, validBlocks);
      }
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

  private CompressionCodecFactory compressionCodecs = null;

  public void configure(JobConf conf) {
    compressionCodecs = new CompressionCodecFactory(conf);
  }

  @Override
  protected boolean isSplitable(FileSystem fs, Path filename) {
    return compressionCodecs.getCodec(filename) == null;
  }

  public abstract RecordReader<K, V> getRecordReader(InputSplit split,
      JobConf job, Reporter reporter) throws IOException;

  static class OneFileInfo {
    private long fileSize;
    private OneBlockInfo[] blocks;

    OneFileInfo(Path path, JobConf job,
        HashMap<String, List<OneBlockInfo>> rackToBlocks,
        HashMap<OneBlockInfo, String[]> blockToNodes,
        HashMap<String, List<OneBlockInfo>> nodeToBlocks) throws IOException {
      this.fileSize = 0;

      FileSystem fs = path.getFileSystem(job);
      FileStatus stat = fs.getFileStatus(path);
      BlockLocation[] locations = fs.getFileBlockLocations(stat, 0,
          stat.getLen());
      if (locations == null) {
        blocks = new OneBlockInfo[0];
      } else {
        blocks = new OneBlockInfo[locations.length];
        for (int i = 0; i < locations.length; i++) {

          fileSize += locations[i].getLength();
          OneBlockInfo oneblock = new OneBlockInfo(path,
              locations[i].getOffset(), locations[i].getLength(),
              locations[i].getHosts(), locations[i].getTopologyPaths());
          blocks[i] = oneblock;

          blockToNodes.put(oneblock, oneblock.hosts);

          for (int j = 0; j < oneblock.racks.length; j++) {
            String rack = oneblock.racks[j];
            List<OneBlockInfo> blklist = rackToBlocks.get(rack);
            if (blklist == null) {
              blklist = new ArrayList<OneBlockInfo>();
              rackToBlocks.put(rack, blklist);
            }
            blklist.add(oneblock);
          }

          for (int j = 0; j < oneblock.hosts.length; j++) {
            String node = oneblock.hosts[j];
            List<OneBlockInfo> blklist = nodeToBlocks.get(node);
            if (blklist == null) {
              blklist = new ArrayList<OneBlockInfo>();
              nodeToBlocks.put(node, blklist);
            }
            blklist.add(oneblock);
          }
        }
      }
    }

    long getLength() {
      return fileSize;
    }
  }

  static class OneBlockInfo {
    Path onepath;
    long offset;
    long length;
    String[] hosts;
    String[] racks;

    OneBlockInfo(Path path, long offset, long len, String[] hosts,
        String[] topologyPaths) {
      this.onepath = path;
      this.offset = offset;
      this.hosts = hosts;
      this.length = len;
      assert (hosts.length == topologyPaths.length || topologyPaths.length == 0);

      if (topologyPaths.length == 0) {
        topologyPaths = new String[hosts.length];
        for (int i = 0; i < topologyPaths.length; i++) {
          topologyPaths[i] = (new NodeBase(hosts[i],
              NetworkTopology.DEFAULT_RACK)).toString();
        }
      }

      this.racks = new String[topologyPaths.length];
      for (int i = 0; i < topologyPaths.length; i++) {
        this.racks[i] = (new NodeBase(topologyPaths[i])).getNetworkLocation();
      }
    }
  }

  public static class MultiPathFilter implements PathFilter {
    private List<PathFilter> filters;

    public MultiPathFilter() {
      this.filters = new ArrayList<PathFilter>();
    }

    public MultiPathFilter(List<PathFilter> filters) {
      this.filters = filters;
    }

    public void add(PathFilter one) {
      filters.add(one);
    }

    public boolean accept(Path path) {
      for (PathFilter filter : filters) {
        if (filter.accept(path)) {
          return true;
        }
      }
      return false;
    }

    public String toString() {
      StringBuffer buf = new StringBuffer();
      buf.append("[");
      for (PathFilter f : filters) {
        buf.append(f);
        buf.append(",");
      }
      buf.append("]");
      return buf.toString();
    }
  }

  public InputSplit[] getSplits1(JobConf job, int numSplits) throws IOException {
    LOG.info("getSplits1");
    return super.getSplits(job, numSplits);
  }
}
