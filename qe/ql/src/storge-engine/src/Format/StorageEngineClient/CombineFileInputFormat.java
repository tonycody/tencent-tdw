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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.net.NetworkTopology;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RecordReader;

import Tool.NullGzFileException;

@SuppressWarnings("deprecation")
public abstract class CombineFileInputFormat<K, V> extends
    FileInputFormat<K, V> implements JobConfigurable {
  public static final Log LOG = LogFactory.getLog(CombineFileInputFormat.class);

  public LogHelper console = new LogHelper(LOG);

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

  public CombineFileInputFormat() {
  }

  public String getFileName(Path p){
    return p.toString();
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

    FileStatus[] fsStatus = listStatus(job);
    Path[] paths = FileUtil.stat2Paths(fsStatus);
    Map<String, FileStatus> fileNameToStatus = new HashMap<String, FileStatus>();
    int arraySize = fsStatus.length;
    for(int i = 0; i < arraySize; i++){
      fileNameToStatus.put(getFileName(paths[i]), fsStatus[i]);
    }
    
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
      getMoreSplitsWithStatus(job, myPaths.toArray(new Path[myPaths.size()]), fileNameToStatus,  maxSize,
          minSizeNode, minSizeRack, splits);
    }

    ArrayList<Path> myPaths = new ArrayList<Path>();
    for (int i = 0; i < paths.length; i++) {
      if (paths[i] == null) {
        continue;
      }
      myPaths.add(paths[i]);
    }
    LOG.debug("myPaths size:\t" + myPaths.size());
    try {
      getMoreSplitsWithStatus(job, myPaths.toArray(new Path[myPaths.size()]), fileNameToStatus , maxSize,
          minSizeNode, minSizeRack, splits);
    } catch (NullGzFileException e) {
      throw new IOException(e);
    }
    LOG.debug("splits #:\t" + splits.size());

    return splits.toArray(new CombineFileSplit[splits.size()]);
  }

  private void getMoreSplits(JobConf job, Path[] paths1, long maxSize,
      long minSizeNode, long minSizeRack, List<CombineFileSplit> splits)
      throws IOException, NullGzFileException {
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

    for (Iterator<Map.Entry<String, List<OneBlockInfo>>> iter = nodeToBlocks
        .entrySet().iterator(); iter.hasNext();) {

      Map.Entry<String, List<OneBlockInfo>> onenode = iter.next();
      this.processsplit(job, onenode, blockToNodes, maxSize, minSizeNode,
          minSizeRack, splits, "node");
    }

    for (Iterator<Map.Entry<String, List<OneBlockInfo>>> iter = rackToBlocks
        .entrySet().iterator(); iter.hasNext();) {

      Map.Entry<String, List<OneBlockInfo>> onerack = iter.next();
      this.processsplit(job, onerack, blockToNodes, maxSize, minSizeNode,
          minSizeRack, splits, "rack");
    }

    this.processsplit(job, null, blockToNodes, maxSize, minSizeNode,
        minSizeRack, splits, "all");

    int maxFileNumPerSplit = job.getInt(
        "hive.merge.inputfiles.maxFileNumPerSplit", 1000);

    HashSet<OneBlockInfo> hs = new HashSet<OneBlockInfo>();
    while (blockToNodes.size() > 0) {
      ArrayList<OneBlockInfo> validBlocks = new ArrayList<OneBlockInfo>();
      List<String> nodes = new ArrayList<String>();
      int filenum = 0;
      hs.clear();
      for (OneBlockInfo blockInfo : blockToNodes.keySet()) {
        validBlocks.add(blockInfo);
        filenum++;
        for (String host : blockInfo.hosts) {
          nodes.add(host);
        }
        hs.add(blockInfo);
        if (filenum >= maxFileNumPerSplit) {
          break;
        }
      }
      for (OneBlockInfo blockInfo : hs) {
        blockToNodes.remove(blockInfo);
      }
      this.addCreatedSplit(job, splits, nodes, validBlocks);
    }

    if (unsplitable.size() != 0) {

      HashMap<OneBlockInfo, String[]> fileToNodes = new HashMap<OneBlockInfo, String[]>();

      for (Path path : unsplitable) {
        FileSystem fs = path.getFileSystem(job);
        FileStatus stat = fs.getFileStatus(path);
        long len = fs.getFileStatus(path).getLen();
        BlockLocation[] locations = path.getFileSystem(job)
            .getFileBlockLocations(stat, 0, len);
        if (locations.length == 0) {
          console.printError("The file " + path.toUri().toString()
              + " maybe is empty, please check it!");
          throw new NullGzFileException("The file " + path.toUri().toString()
              + " maybe is empty, please check it!");
        }

        LOG.info("unsplitable file:" + path.toUri().toString() + " length:"
            + len);

        OneBlockInfo oneblock = new OneBlockInfo(path, 0, len,
            locations[0].getHosts(), locations[0].getTopologyPaths());
        fileToNodes.put(oneblock, locations[0].getHosts());
      }

      this.processsplitForUnsplit(job, null, fileToNodes, maxSize, minSizeNode,
          minSizeRack, splits, "all");
    }
  }

  private void getMoreSplitsWithStatus(JobConf job, Path[] paths1, Map<String, FileStatus> fileNameToStatus,
      long maxSize,long minSizeNode, long minSizeRack, List<CombineFileSplit> splits)
      throws IOException, NullGzFileException {
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
      files[i] = new OneFileInfo(paths[i], fileNameToStatus.get(paths[i].toString()) , job, rackToBlocks, blockToNodes,
          nodeToBlocks);
      totLength += files[i].getLength();
    }

    for (Iterator<Map.Entry<String, List<OneBlockInfo>>> iter = nodeToBlocks
        .entrySet().iterator(); iter.hasNext();) {

      Map.Entry<String, List<OneBlockInfo>> onenode = iter.next();
      this.processsplit(job, onenode, blockToNodes, maxSize, minSizeNode,
          minSizeRack, splits, "node");
    }

    for (Iterator<Map.Entry<String, List<OneBlockInfo>>> iter = rackToBlocks
        .entrySet().iterator(); iter.hasNext();) {

      Map.Entry<String, List<OneBlockInfo>> onerack = iter.next();
      this.processsplit(job, onerack, blockToNodes, maxSize, minSizeNode,
          minSizeRack, splits, "rack");
    }

    this.processsplit(job, null, blockToNodes, maxSize, minSizeNode,
        minSizeRack, splits, "all");

    int maxFileNumPerSplit = job.getInt(
        "hive.merge.inputfiles.maxFileNumPerSplit", 1000);

    HashSet<OneBlockInfo> hs = new HashSet<OneBlockInfo>();
    while (blockToNodes.size() > 0) {
      ArrayList<OneBlockInfo> validBlocks = new ArrayList<OneBlockInfo>();
      List<String> nodes = new ArrayList<String>();
      int filenum = 0;
      hs.clear();
      for (OneBlockInfo blockInfo : blockToNodes.keySet()) {
        validBlocks.add(blockInfo);
        filenum++;
        for (String host : blockInfo.hosts) {
          nodes.add(host);
        }
        hs.add(blockInfo);
        if (filenum >= maxFileNumPerSplit) {
          break;
        }
      }
      for (OneBlockInfo blockInfo : hs) {
        blockToNodes.remove(blockInfo);
      }
      this.addCreatedSplit(job, splits, nodes, validBlocks);
    }

    if (unsplitable.size() != 0) {

      HashMap<OneBlockInfo, String[]> fileToNodes = new HashMap<OneBlockInfo, String[]>();

      for (Path path : unsplitable) {
        FileSystem fs = path.getFileSystem(job);
        FileStatus stat = fileNameToStatus.get(path.toString());//fs.getFileStatus(path);
        long len = stat.getLen();
        BlockLocation[] locations = fs.getFileBlockLocations(stat, 0, len);
        if (locations.length == 0) {
          console.printError("The file " + path.toUri().toString()
              + " maybe is empty, please check it!");
          throw new NullGzFileException("The file " + path.toUri().toString()
              + " maybe is empty, please check it!");
        }

        LOG.info("unsplitable file:" + path.toUri().toString() + " length:"
            + len);

        OneBlockInfo oneblock = new OneBlockInfo(path, 0, len,
            locations[0].getHosts(), locations[0].getTopologyPaths());
        fileToNodes.put(oneblock, locations[0].getHosts());
      }

      this.processsplitForUnsplit(job, null, fileToNodes, maxSize, minSizeNode,
          minSizeRack, splits, "all");
    }
  }

  
  private void processsplit(JobConf job,
      Map.Entry<String, List<OneBlockInfo>> one,
      HashMap<OneBlockInfo, String[]> blockToNodes, long maxSize,
      long minSizeNode, long minSizeRack, List<CombineFileSplit> splits,
      String type) {
    ArrayList<OneBlockInfo> validBlocks = new ArrayList<OneBlockInfo>();
    ArrayList<String> nodes = new ArrayList<String>();
    long curSplitSize = 0;
    if (type.equals("node"))
      nodes.add(one.getKey());

    List<OneBlockInfo> blocks = null;
    if (!type.equals("all")) {
      blocks = one.getValue();
    } else {
      blocks = new ArrayList<OneBlockInfo>();
      blocks.addAll(blockToNodes.keySet());
    }

    OneBlockInfo[] blocksInNodeArr = blocks.toArray(new OneBlockInfo[blocks
        .size()]);
    if (job.getBoolean("hive.merge.inputfiles.sort", true)) {
      Arrays.sort(blocksInNodeArr, new Comparator<OneBlockInfo>() {
        @Override
        public int compare(OneBlockInfo o1, OneBlockInfo o2) {
          return (int) (o2.length - o1.length);
        }
      });
    }

    if (job.getBoolean("hive.merge.inputfiles.rerange", false)) {

      Random r = new Random(123456);
      OneBlockInfo tmp = null;
      for (int i = 0; i < blocksInNodeArr.length; i++) {
        int idx = r.nextInt(blocksInNodeArr.length);
        tmp = blocksInNodeArr[i];
        blocksInNodeArr[i] = blocksInNodeArr[idx];
        blocksInNodeArr[idx] = tmp;
      }
    }

    int maxFileNumPerSplit = job.getInt(
        "hive.merge.inputfiles.maxFileNumPerSplit", 1000);

    for (int i = 0; i < blocksInNodeArr.length; i++) {
      if (blockToNodes.containsKey(blocksInNodeArr[i])) {
        if (!type.equals("node")) {
          nodes.clear();
        }

        curSplitSize = blocksInNodeArr[i].length;
        validBlocks.clear();
        validBlocks.add(blocksInNodeArr[i]);
        blockToNodes.remove(blocksInNodeArr[i]);
        if (maxSize != 0 && curSplitSize >= maxSize) {
          addCreatedSplit(job, splits, nodes, validBlocks);
        } else {
          int filenum = 1;
          for (int j = i + 1; j < blocksInNodeArr.length; j++) {
            if (blockToNodes.containsKey(blocksInNodeArr[j])) {
              long size1 = blocksInNodeArr[j].length;
              if (maxSize != 0 && curSplitSize + size1 <= maxSize) {
                curSplitSize += size1;
                filenum++;
                validBlocks.add(blocksInNodeArr[j]);
                blockToNodes.remove(blocksInNodeArr[j]);
                if (!type.equals("node"))
                  for (int k = 0; k < blocksInNodeArr[j].hosts.length; k++) {
                    nodes.add(blocksInNodeArr[j].hosts[k]);
                  }
              }
              if (filenum >= maxFileNumPerSplit) {
                break;
              }
            }
          }
          if (minSizeNode != 0 && curSplitSize >= minSizeNode) {
            addCreatedSplit(job, splits, nodes, validBlocks);
          } else {
            for (OneBlockInfo oneblock : validBlocks) {
              blockToNodes.put(oneblock, oneblock.hosts);
            }
            break;
          }
        }
      }
    }
  }

  private void processsplitForUnsplit(JobConf job,
      Map.Entry<String, List<OneBlockInfo>> one,
      HashMap<OneBlockInfo, String[]> blockToNodes, long maxSize,
      long minSizeNode, long minSizeRack, List<CombineFileSplit> splits,
      String type) {
    ArrayList<OneBlockInfo> validBlocks = new ArrayList<OneBlockInfo>();
    ArrayList<String> nodes = new ArrayList<String>();
    long curSplitSize = 0;
    if (type.equals("node"))
      nodes.add(one.getKey());

    List<OneBlockInfo> blocks = null;
    if (!type.equals("all")) {
      blocks = one.getValue();
    } else {
      blocks = new ArrayList<OneBlockInfo>();
      blocks.addAll(blockToNodes.keySet());
    }

    OneBlockInfo[] blocksInNodeArr = blocks.toArray(new OneBlockInfo[blocks
        .size()]);
    if (job.getBoolean("hive.merge.inputfiles.sort", true)) {
      Arrays.sort(blocksInNodeArr, new Comparator<OneBlockInfo>() {
        @Override
        public int compare(OneBlockInfo o1, OneBlockInfo o2) {
          long comparereuslt = o2.length - o1.length;
          int result = 0;
          if (comparereuslt > 0)
            result = 1;

          if (comparereuslt < 0)
            result = -1;

          return result;
        }
      });
    }

    if (job.getBoolean("hive.merge.inputfiles.rerange", false)) {
      Random r = new Random(123456);
      OneBlockInfo tmp = null;
      for (int i = 0; i < blocksInNodeArr.length; i++) {
        int idx = r.nextInt(blocksInNodeArr.length);
        tmp = blocksInNodeArr[i];
        blocksInNodeArr[i] = blocksInNodeArr[idx];
        blocksInNodeArr[idx] = tmp;
      }
    }

    int maxFileNumPerSplit = job.getInt(
        "hive.merge.inputfiles.maxFileNumPerSplit", 1000);

    for (int i = 0; i < blocksInNodeArr.length; i++) {
      if (blockToNodes.containsKey(blocksInNodeArr[i])) {
        if (!type.equals("node")) {
          nodes.clear();
        }

        curSplitSize = blocksInNodeArr[i].length;
        validBlocks.clear();
        validBlocks.add(blocksInNodeArr[i]);
        blockToNodes.remove(blocksInNodeArr[i]);
        if (maxSize != 0 && curSplitSize >= maxSize) {
          if (!type.equals("node")) {
            for (int k = 0; k < blocksInNodeArr[i].hosts.length; k++) {
              nodes.add(blocksInNodeArr[i].hosts[k]);
            }
          }
          addCreatedSplit(job, splits, nodes, validBlocks);
        } else {
          int filenum = 1;
          for (int j = i + 1; j < blocksInNodeArr.length; j++) {
            if (blockToNodes.containsKey(blocksInNodeArr[j])) {
              long size1 = blocksInNodeArr[j].length;
              if (maxSize != 0 && curSplitSize < maxSize) {
                curSplitSize += size1;
                filenum++;
                validBlocks.add(blocksInNodeArr[j]);
                blockToNodes.remove(blocksInNodeArr[j]);
              }
              if (filenum >= maxFileNumPerSplit) {
                break;
              }

              if (curSplitSize >= maxSize) {
                break;
              }
            }
          }
          if (minSizeNode != 0 && curSplitSize >= minSizeNode) {
            if (!type.equals("node")) {
              generateNodesInfo(validBlocks, nodes);
            }

            addCreatedSplit(job, splits, nodes, validBlocks);
          } else {
            for (OneBlockInfo oneblock : validBlocks) {
              blockToNodes.put(oneblock, oneblock.hosts);
            }
            break;
          }
        }
      }
    }

    HashSet<OneBlockInfo> hs = new HashSet<OneBlockInfo>();
    while (blockToNodes.size() > 0) {
      validBlocks = new ArrayList<OneBlockInfo>();
      nodes = new ArrayList<String>();
      int filenum = 0;
      hs.clear();
      for (OneBlockInfo blockInfo : blockToNodes.keySet()) {
        filenum++;
        validBlocks.add(blockInfo);

        hs.add(blockInfo);
        if (filenum >= maxFileNumPerSplit) {
          break;
        }
      }
      for (OneBlockInfo blockInfo : hs) {
        blockToNodes.remove(blockInfo);
      }

      generateNodesInfo(validBlocks, nodes);

      this.addCreatedSplit(job, splits, nodes, validBlocks);
    }
  }

  private void generateNodesInfo(ArrayList<OneBlockInfo> validBlocks,
      ArrayList<String> nodes) {
    try {
      HashMap<String, Long> nodeToLen = new HashMap<String, Long>();
      for (OneBlockInfo oneBI : validBlocks) {
        for (int k = 0; k < oneBI.hosts.length; k++) {
          if (nodeToLen.containsKey(oneBI.hosts[k])) {
            long len = nodeToLen.get(oneBI.hosts[k]);
            nodeToLen.put(oneBI.hosts[k], len + oneBI.length);
          } else {
            nodeToLen.put(oneBI.hosts[k], oneBI.length);
          }
        }
      }

      List<Map.Entry<String, Long>> nodeTotLens = new ArrayList<Map.Entry<String, Long>>(
          nodeToLen.entrySet());
      Collections.sort(nodeTotLens, new Comparator<Map.Entry<String, Long>>() {
        public int compare(Map.Entry<String, Long> o1,
            Map.Entry<String, Long> o2) {
          long comparereuslt = o2.getValue().longValue()
              - o1.getValue().longValue();
          int result = 0;
          if (comparereuslt > 0)
            result = 1;

          if (comparereuslt < 0)
            result = -1;

          return result;
        }
      });
      int nodeslen = nodeTotLens.size() > 3 ? 3 : nodeTotLens.size();
      for (int k = 0; k < nodeslen; k++) {
        nodes.add(nodeTotLens.get(k).getKey());
      }
    } catch (Exception e) {
      LOG.info("parsing nodes error:" + e.getMessage());
    }
  }

  public static HashMap<OneBlockInfo, String[]> constructBlockToNodes(
      long[] filelength, ArrayList<String[]> hostss) {
    String[] topologyPaths = new String[0];

    HashMap<OneBlockInfo, String[]> blockToNodes = new HashMap<OneBlockInfo, String[]>();
    for (int i = 0; i < filelength.length; i++) {
      blockToNodes.put(new OneBlockInfo(null, 0, filelength[i], hostss.get(i),
          topologyPaths), hostss.get(i));
    }

    return blockToNodes;
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

    LOG.debug("new split: " + fl.length + " bolcks ");

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

    OneFileInfo(Path path, FileStatus stat, JobConf job,
        HashMap<String, List<OneBlockInfo>> rackToBlocks,
        HashMap<OneBlockInfo, String[]> blockToNodes,
        HashMap<String, List<OneBlockInfo>> nodeToBlocks) throws IOException {
      this.fileSize = 0;

      FileSystem fs = path.getFileSystem(job);
      //FileStatus stat = fs.getFileStatus(path);
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
