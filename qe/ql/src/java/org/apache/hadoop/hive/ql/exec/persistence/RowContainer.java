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

import java.io.File;
import java.io.RandomAccessFile;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

public class RowContainer<Row extends List<Object>> {

  protected Log LOG = LogFactory.getLog(this.getClass().getName());

  private static final int BLOCKSIZE = 25000;

  private static final int TMPFILESIZE = 1 * 1024 * 1024 * 1024;
  private Row[] currentWriteBlock;
  private Row[] currentReadBlock;
  private Row[] firstReadBlockPointer;
  private int blockSize;
  private int numFlushedBlocks;
  private int size;
  private File tmpFile;
  Path tempOutPath = null;
  private File parentFile;
  private int itrCursor;
  private int readBlockSize;
  private int addCursor;
  private int pBlock;
  private SerDe serde;
  private ObjectInspector standardOI;
  private Row dummyRow = null;
  private tableDesc tblDesc;
  private String RCtmppath = null;
  private int splitThreshold = 0;
  private int splitSize = 0;

  private List<Object> keyObject;

  boolean firstCalled = false;
  int acutalSplitNum = 0;
  int currentSplitPointer = 0;
  FileSinkOperator.RecordWriter rw = null;

  org.apache.hadoop.mapred.RecordReader rr = null;
  InputFormat<WritableComparable, Writable> inputFormat = null;
  InputSplit[] inputSplits = null;

  Writable val = null;

  Configuration jc = null;
  JobConf jobCloneUsingLocalFs = null;
  private LocalFileSystem localFs;

  public RowContainer(Configuration jc) throws HiveException {
    this(BLOCKSIZE, jc);
  }

  public RowContainer(int blockSize, Configuration jc) throws HiveException {
    int bms = jc.getInt("rowcontainer.block.minsize", 100);
    this.blockSize = blockSize <= bms ? bms : blockSize;
    this.size = 0;
    this.itrCursor = 0;
    this.addCursor = 0;
    this.numFlushedBlocks = 0;
    this.tmpFile = null;
    this.currentWriteBlock = (Row[]) new ArrayList[this.blockSize];
    this.currentReadBlock = this.currentWriteBlock;
    this.firstReadBlockPointer = currentReadBlock;
    this.serde = null;
    this.standardOI = null;
    this.jc = jc;
    
    
  }

  private void getLocalFSJobConfClone(Configuration jc) {
    if (this.jobCloneUsingLocalFs == null) {
      this.jobCloneUsingLocalFs = new JobConf(jc);
      HiveConf.setVar(jobCloneUsingLocalFs, HiveConf.ConfVars.HADOOPFS,
          Utilities.HADOOP_LOCAL_FS);
    }
  }

  public RowContainer(int blockSize, SerDe sd, ObjectInspector oi,
      Configuration jc) throws HiveException {
    this(blockSize, jc);
    setSerDe(sd, oi);
  }

  public void setSerDe(SerDe sd, ObjectInspector oi) {
    this.serde = sd;
    this.standardOI = oi;
  }

  public void add(Row t) throws HiveException {
    if (this.tblDesc != null) {
      if (addCursor >= blockSize) {
        spillBlock(currentWriteBlock, addCursor);
        addCursor = 0;
        if (numFlushedBlocks == 1)
          currentWriteBlock = (Row[]) new ArrayList[blockSize];
      }
      currentWriteBlock[addCursor++] = t;
    } else if (t != null) {
      this.dummyRow = t;
    }
    ++size;
  }

  public Row first() throws HiveException {
    if (size == 0)
      return null;

    try {
      firstCalled = true;
      this.itrCursor = 0;
      closeWriter();
      closeReader();

      if (tblDesc == null) {
        this.itrCursor++;
        return dummyRow;

      }
      this.currentReadBlock = this.firstReadBlockPointer;
      if (this.numFlushedBlocks == 0) {
        this.readBlockSize = this.addCursor;
        this.currentReadBlock = this.currentWriteBlock;
      } else {
        getLocalFSJobConfClone(jc);
        if (inputSplits == null) {
          if (this.inputFormat == null)
            inputFormat = (InputFormat<WritableComparable, Writable>) ReflectionUtils
                .newInstance(tblDesc.getInputFileFormatClass(),
                    jobCloneUsingLocalFs);

          HiveConf.setVar(jobCloneUsingLocalFs,
              HiveConf.ConfVars.HADOOPMAPREDINPUTDIR,
              org.apache.hadoop.util.StringUtils.escapeString(parentFile
                  .getAbsolutePath()));
          inputSplits = inputFormat.getSplits(jobCloneUsingLocalFs, 1);
          acutalSplitNum = inputSplits.length;
        }
        currentSplitPointer = 0;
        rr = inputFormat.getRecordReader(inputSplits[currentSplitPointer],
            jobCloneUsingLocalFs, Reporter.NULL);
        currentSplitPointer++;

        nextBlock();
      }
      Row ret = currentReadBlock[itrCursor++];
      removeKeys(ret);
      return ret;
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

  public Row next() throws HiveException {

    if (!firstCalled)
      throw new RuntimeException("Call first() then call next().");

    if (size == 0)
      return null;

    if (tblDesc == null) {
      if (this.itrCursor < size) {
        this.itrCursor++;
        return dummyRow;
      }
      return null;
    }

    Row ret;
    if (itrCursor < this.readBlockSize) {
      ret = this.currentReadBlock[itrCursor++];
      removeKeys(ret);
      return ret;
    } else {
      nextBlock();
      if (this.readBlockSize == 0) {
        if (currentWriteBlock != null && currentReadBlock != currentWriteBlock) {
          this.itrCursor = 0;
          this.readBlockSize = this.addCursor;
          this.firstReadBlockPointer = this.currentReadBlock;
          currentReadBlock = currentWriteBlock;
        } else {
          return null;
        }
      }
      return next();
    }
  }

  private void removeKeys(Row ret) {
    if (this.keyObject != null
        && this.currentReadBlock != this.currentWriteBlock) {
      int len = this.keyObject.size();
      int rowSize = ((ArrayList) ret).size();
      for (int i = 0; i < len; i++) {
        ((ArrayList) ret).remove(rowSize - i - 1);
      }
    }
  }

  ArrayList<Object> row = new ArrayList<Object>(2);

  private void getRctmppath() {
    if (RCtmppath == null) {
      RCtmppath = Utilities.getRandomRctmpDir(jobCloneUsingLocalFs);
      splitThreshold = HiveConf.getIntVar(jobCloneUsingLocalFs,
          HiveConf.ConfVars.HIVERCTMPFILESPLITTHRESHOLD);
      splitSize = HiveConf.getIntVar(jobCloneUsingLocalFs,
          HiveConf.ConfVars.HIVERCTMPFILESPLITSIZE);
    }
  }
  
  private void spillBlock(Row[] block, int length) throws HiveException {

    try {
      if (tmpFile == null) {

        getLocalFSJobConfClone(jc);
        getRctmppath();

        String suffix = ".tmp";
        if (this.keyObject != null)
          suffix = "." + this.keyObject.toString() + suffix;

        String queryId = jc.get("hive.query.id", "");
        while (true) {
          String parentId = "hrc_" + queryId + "_" + Utilities.randGen.nextInt();
          parentFile = new File(RCtmppath + parentId);
          boolean success = parentFile.mkdirs();
          if (success)
            break;
          LOG.debug("retry creating tmp row-container directory...");
        }

        tmpFile = File.createTempFile("RowContainer", suffix, parentFile);
        LOG.info("RowContainer created temp file " + tmpFile.getAbsolutePath());
        parentFile.deleteOnExit();
        tmpFile.deleteOnExit();

        HiveOutputFormat<?, ?> hiveOutputFormat = tblDesc
            .getOutputFileFormatClass().newInstance();
        tempOutPath = new Path(tmpFile.toString());
        rw = HiveFileFormatUtils.getRecordWriter(this.jobCloneUsingLocalFs,
            hiveOutputFormat, serde.getSerializedClass(), false,
            tblDesc.getProperties(), tempOutPath);
      } else if (rw == null) {
        throw new HiveException(
            "RowContainer has already been closed for writing.");
      }

      row.clear();
      row.add(null);
      row.add(null);

      if (this.keyObject != null) {
        row.set(1, this.keyObject);
        for (int i = 0; i < length; ++i) {
          Row currentValRow = block[i];
          row.set(0, currentValRow);
          Writable outVal = serde.serialize(row, standardOI);
          rw.write(outVal);
        }
      } else {
        for (int i = 0; i < length; ++i) {
          Row currentValRow = block[i];
          Writable outVal = serde.serialize(currentValRow, standardOI);
          rw.write(outVal);
        }
      }

      if (block == this.currentWriteBlock)
        this.addCursor = 0;

      this.numFlushedBlocks++;

    } catch (Exception e) {
      clear();
      LOG.error(e.toString(), e);
      throw new HiveException(e);
    }
  }

  private void spillBlocktmp(Row[] block, int length, FileSystem destFs,
      Path destPath) throws HiveException {

    try {
      getLocalFSJobConfClone(jc);
      getRctmppath();

      String suffix = ".tmp";
      if (this.keyObject != null)
        suffix = "." + this.keyObject.toString() + suffix;

      File parentFiletmp = null;
      String queryId = jc.get("hive.query.id", "");
      while (true) {
        String parentId = "hrc_" + queryId + "_" + Utilities.randGen.nextInt();
        parentFiletmp = new File(RCtmppath + parentId);
        boolean success = parentFiletmp.mkdirs();
        if (success)
          break;
        LOG.debug("retry creating tmp row-container directory...");
      }

      File tmpFiletmp = File.createTempFile("RowContainer", suffix, parentFile);
      LOG.info("RowContainer created temp file " + tmpFiletmp.getAbsolutePath());
      parentFiletmp.deleteOnExit();
      tmpFiletmp.deleteOnExit();

      HiveOutputFormat<?, ?> hiveOutputFormat = tblDesc
          .getOutputFileFormatClass().newInstance();
      Path tempOutPathtmp = new Path(tmpFiletmp.toString());
      FileSinkOperator.RecordWriter rwtmp = HiveFileFormatUtils
          .getRecordWriter(this.jobCloneUsingLocalFs, hiveOutputFormat,
              serde.getSerializedClass(), false, tblDesc.getProperties(),
              tempOutPathtmp);

      row.clear();
      row.add(null);
      row.add(null);

      if (this.keyObject != null) {
        row.set(1, this.keyObject);
        for (int i = 0; i < length; ++i) {
          Row currentValRow = block[i];
          row.set(0, currentValRow);
          Writable outVal = serde.serialize(row, standardOI);
          rwtmp.write(outVal);
        }
      } else {
        for (int i = 0; i < length; ++i) {
          Row currentValRow = block[i];
          Writable outVal = serde.serialize(currentValRow, standardOI);
          rwtmp.write(outVal);
        }
      }

      rwtmp.close(false);
      rwtmp = null;

      LOG.info("RowContainer copied temp file " + tmpFiletmp.getAbsolutePath()
          + " to dfs directory " + destPath.toString());
      destFs.copyFromLocalFile(true, tempOutPathtmp, new Path(destPath,
          new Path(tempOutPathtmp.getName())));

      rwtmp = null;
      tmpFiletmp = null;
      deleteLocalFile(parentFiletmp, true);
      parentFiletmp = null;
    } catch (Exception e) {
      clear();
      LOG.error(e.toString(), e);
      throw new HiveException(e);
    }
  }

  public int size() {
    return size;
  }

  private boolean nextBlock() throws HiveException {
    itrCursor = 0;
    this.readBlockSize = 0;
    if (this.numFlushedBlocks == 0)
      return false;

    try {
      if (val == null)
        val = serde.getSerializedClass().newInstance();
      boolean nextSplit = true;
      int i = 0;

      if (rr != null) {
        Object key = rr.createKey();
        while (i < this.currentReadBlock.length && rr.next(key, val)) {
          nextSplit = false;
          this.currentReadBlock[i++] = (Row) ObjectInspectorUtils
              .copyToStandardObject(serde.deserialize(val),
                  serde.getObjectInspector(),
                  ObjectInspectorCopyOption.WRITABLE);
        }
      }

      if (nextSplit && this.currentSplitPointer < this.acutalSplitNum) {
        getLocalFSJobConfClone(jc);
        rr = inputFormat.getRecordReader(inputSplits[currentSplitPointer],
            jobCloneUsingLocalFs, Reporter.NULL);
        currentSplitPointer++;
        return nextBlock();
      }

      this.readBlockSize = i;
      return this.readBlockSize > 0;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      try {
        this.clear();
      } catch (HiveException e1) {
        LOG.error(e.getMessage(), e);
      }
      throw new HiveException(e);
    }
  }

  public void copyToDFSDirecory(FileSystem destFs, Path destPath,
      boolean bigtable) throws IOException, HiveException {
    if (addCursor > 0)
      this.spillBlock(this.currentWriteBlock, addCursor);
    if (tempOutPath == null || tempOutPath.toString().trim().equals(""))
      return;
    this.closeWriter();

    if (bigtable && size > splitThreshold) {
      int rownumcnt = 0;
      Row[] WriteBlock = (Row[]) new ArrayList[splitSize];
      ;
      for (ArrayList<Object> newObj = (ArrayList<Object>) this.first(); newObj != null; newObj = (ArrayList<Object>) this
          .next()) {
        if (rownumcnt >= splitSize) {
          spillBlocktmp(WriteBlock, splitSize, destFs, destPath);
          rownumcnt = 0;
          WriteBlock = (Row[]) new ArrayList[splitSize];
        }
        WriteBlock[rownumcnt++] = (Row) newObj;
      }

      if (rownumcnt > 0) {
        spillBlocktmp(WriteBlock, rownumcnt, destFs, destPath);
      }
    } else {
      LOG.info("RowContainer copied temp file " + tmpFile.getAbsolutePath()
          + " to dfs directory " + destPath.toString());
      destFs.copyFromLocalFile(true, tempOutPath, new Path(destPath, new Path(
          tempOutPath.getName())));
    }

    clear();
  }

  public void clear() throws HiveException {
    itrCursor = 0;
    addCursor = 0;
    numFlushedBlocks = 0;
    this.readBlockSize = 0;
    this.acutalSplitNum = 0;
    this.currentSplitPointer = -1;
    this.firstCalled = false;
    this.inputSplits = null;
    tempOutPath = null;
    addCursor = 0;

    size = 0;
    try {
      if (rw != null)
        rw.close(false);
      if (rr != null)
        rr.close();
    } catch (Exception e) {
      LOG.error(e.toString());
      throw new HiveException(e);
    } finally {
      rw = null;
      rr = null;
      tmpFile = null;
      deleteLocalFile(parentFile, true);
      parentFile = null;
    }
  }

  private void deleteLocalFile(File file, boolean recursive) {
    try {
      if (file != null) {
        if (!file.exists())
          return;
        if (file.isDirectory() && recursive) {
          File[] files = file.listFiles();
          for (int i = 0; i < files.length; i++)
            deleteLocalFile(files[i], true);
        }
        boolean deleteSuccess = file.delete();
        if (!deleteSuccess)
          LOG.error("Error deleting tmp file:" + file.getAbsolutePath());
      }
    } catch (Exception e) {
      LOG.error("Error deleting tmp file:" + file.getAbsolutePath(), e);
    }
  }

  private void closeWriter() throws IOException {
    if (this.rw != null) {
      this.rw.close(false);
      this.rw = null;
    }
  }

  private void closeReader() throws IOException {
    if (this.rr != null) {
      this.rr.close();
      this.rr = null;
    }
  }

  public void setKeyObject(List<Object> dummyKey) {
    this.keyObject = dummyKey;
  }

  public void setTableDesc(tableDesc tblDesc) {
    this.tblDesc = tblDesc;
  }
}
