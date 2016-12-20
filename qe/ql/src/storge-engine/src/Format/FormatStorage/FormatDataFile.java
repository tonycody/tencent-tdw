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
package FormatStorage;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import Comm.ConstVar;
import Comm.SEException;
import Comm.Util;
import FormatStorage.BlockIndex.IndexInfo;
import FormatStorage.BlockIndex.IndexMeta;
import FormatStorage.BlockIndex.OffsetInfo;
import FormatStorage.Unit.Record;

public class FormatDataFile {
  public static final Log LOG = LogFactory.getLog("FormatDataFile");

  public Head head = null;
  private int segmentNum = 0;
  private ArrayList<Segment> segments = new ArrayList<Segment>(100);
  private SegmentIndex segmentIndex = null;

  private long fileLen = -1;
  private long confUnitSize = ConstVar.DefaultUnitSize;
  private long confSegmentSize = ConstVar.DefaultSegmentSize;

  long keyIndexOffset = -1;
  long lineIndexOffset = -1;

  private int recordNum = 0;

  private String fileName = null;

  private FSDataInputStream in = null;
  private FSDataOutputStream out = null;
  private Configuration conf = null;
  private FileSystem fs = null;

  private UnitPoolManager unitPool = null;

  private TableMeta tableMeta = null;

  private Segment currentSegment = null;
  long currentOffset = -1;

  private int currentLine = 0;

  boolean hasLoadAllSegmentDone = false;
  private byte workStatus = ConstVar.WS_Init;

  boolean optimize = false;

  static int counter = 0;

  ArrayList<byte[]> fieldValueBytes = new ArrayList<byte[]>(64);
  DataOutputBuffer chunkOutputBuffer = new DataOutputBuffer();

  public FormatDataFile(Configuration conf) throws Exception {
    this.conf = conf;

    fs = FileSystem.get(conf);
    confUnitSize = conf
        .getLong(ConstVar.ConfUnitSize, ConstVar.DefaultUnitSize);

    this.conf.setInt("io.compression.codec.lzo.buffersize", 128 * 1024);

    if (confUnitSize < 0) {
      throw new SEException.InvalidParameterException("invalid ConfUnitSize:"
          + confUnitSize);
    }

    confSegmentSize = fs.getDefaultBlockSize();

    if (confSegmentSize < 0) {
      throw new SEException.InvalidParameterException(
          "invalid confSegmentSize:" + confSegmentSize);
    }

    if (confUnitSize + ConstVar.LineIndexRecordLen + ConstVar.IndexMetaOffset > confSegmentSize) {
      throw new SEException.InvalidParameterException("unitSize("
          + confUnitSize + ") > segmentSize(" + confSegmentSize + ")");
    }

    long poolSize = conf.getLong(ConstVar.ConfPoolSize,
        ConstVar.DefaultPoolSize);
    unitPool = new UnitPoolManager((int) poolSize, this);

    counter++;
  }

  public void create(String fileName, Head head) throws Exception {
    this.fileName = fileName;

    checkHeadInfo(head);
    this.head = head;

    if (out == null) {
      out = fs.create(new Path(fileName));
    }

    addHead(head);

    workStatus = ConstVar.WS_Write;

    currentOffset = out.getPos();

  }

  public void close() throws Exception {
    LOG.info("fd close:\t" + this.fileName);
    if (workStatus == ConstVar.WS_Write) {
      if (currentSegment != null) {
        Unit unit = null;
        if (currentSegment.canPersistented()) {
          currentSegment.setLastSegment();
        } else {
          unit = currentSegment.currentUnit();
          currentSegment.setCurrentUnitNull();
        }

        addSegment(currentSegment);
        currentSegment = null;

        if (unit != null) {
          IndexInfo indexInfo = new IndexInfo();
          indexInfo.offset = currentOffset;

          currentSegment = new Segment(indexInfo, this);
          currentSegment.setLastSegment();

          unit.transfer(currentOffset);
          currentSegment.addUnit(unit);

          addSegment(currentSegment);
          unit = null;
          currentSegment = null;
        }
      }

      persistentSegmentIndex();

      if (out != null) {
        out.close();
        out = null;
      }
    }

    if (workStatus == ConstVar.WS_Read) {
      if (in != null) {
        in.close();
        in = null;
      }
    }

    if (segments != null) {
      segments.clear();
    }

    if (segmentIndex != null) {
      segmentIndex = null;
    }

    if (head != null) {
      head = null;
    }

    if (unitPool != null) {
      unitPool = null;
    }

    recordNum = 0;
    currentOffset = -1;
    keyIndexOffset = -1;
    lineIndexOffset = -1;
    workStatus = ConstVar.WS_Init;
    hasLoadAllSegmentDone = false;
  }

  public void open(String fileName) throws Exception {
    this.fileName = fileName;
    openHDFSFile(fileName);

    if (head == null) {
      head = new Head();
    }

    loadHead(head);

    workStatus = ConstVar.WS_Read;

    long poolSize = conf.getLong(ConstVar.ConfPoolSize,
        ConstVar.DefaultPoolSize);
    unitPool = new UnitPoolManager((int) poolSize, this);

    fileLen = getFileLen();

    unpersistentSegmentIndex();
  }

  public byte var() {
    return head.var();
  }

  public byte encode() {
    return head.encode();
  }

  public byte encodeStyle() {
    return head.encodeStyle();
  }

  public boolean isVar() {
    return (head.var() == 1);
  }

  public boolean isPrimaryKeyFile() {
    return head.primaryIndex() > ConstVar.NotPrimaryKey;
  }

  public int counter() {
    return counter;
  }

  public boolean optimize() {
    return optimize;
  }

  public void setOptimize(boolean optimize) {
    this.optimize = optimize;
  }

  public boolean isCreated() {
    if (workStatus == ConstVar.WS_Write) {
      return true;
    }

    return false;
  }

  public boolean isOpened() {
    if (workStatus == ConstVar.WS_Read) {
      return true;
    }

    return false;
  }

  public short primaryKeyIndex() {
    return head.primaryIndex();
  }

  public long confSegmentSize() {
    return confSegmentSize;

  }

  public long confUnitSize() {
    return confUnitSize;
  }

  public long getWritePos() throws IOException {
    if (out != null) {
      return out.getPos();
    } else {
      return -1;
    }
  }

  public long getReadPos() throws IOException {
    if (in != null) {
      return in.getPos();
    } else {
      return -1;
    }

  }

  public FSDataInputStream in() {
    return in;
  }

  public FSDataOutputStream out() {
    return out;
  }

  void setIn(FSDataInputStream in) {
    this.in = in;
  }

  void setOut(FSDataOutputStream out) {
    this.out = out;
  }

  Configuration conf() {
    return conf;
  }

  Segment currentSegment() {
    return currentSegment;
  }

  public UnitPoolManager unitPool() {
    return unitPool;
  }

  static public class FDStatus {

  }

  public FDStatus stat() throws IOException {
    return null;

  }

  public long getFileLen() throws IOException {
    FileStatus fileStatus = fs.getFileStatus(new Path(fileName));
    if (fileStatus == null)
      return 0;
    else
      return fileStatus.getLen();
  }

  public int segmentNum() {
    return segmentNum;
  }

  public SegmentIndex segmentIndex() {
    return segmentIndex;
  }

  ArrayList<Segment> segments() {
    return segments;
  }

  public Head head() {
    return head;
  }

  public int recordNum() {
    return recordNum;
  }

  public void incRecordNum() {
    recordNum++;
  }

  public byte workStatus() {
    return workStatus;
  }

  public void setWorkStatus(byte status) {
    if (status == ConstVar.WS_Init || status == ConstVar.WS_Read
        || status == ConstVar.WS_Write) {
      workStatus = status;
    }
  }

  public String fileName() {
    return fileName;
  }

  public void loadHead(Head head) throws Exception {
    int magic = in.readInt();
    if (magic != ConstVar.DataMagic) {
      throw new SEException.ErrorFileFormat("invalid file magic:" + magic);
    }

    head.magic = magic;
    head.unpersistent(in);
  }

  public void openHDFSFile(String file) throws IOException {
    if (in == null) {
      in = fs.open(new Path(fileName));
    }
  }

  public void openHDFS(Path path) throws IOException {
    if (in == null) {
      in = fs.open(path);
    }
  }

  public void addRecord(Record record) throws Exception {
    {
      if (currentSegment == null) {

        IndexInfo indexInfo = new IndexInfo();
        indexInfo.offset = currentOffset;
        currentSegment = new Segment(indexInfo, this);
      }

      try {
        currentSegment.addRecord(record);
      } catch (SEException.SegmentFullException e) {

        Unit unit = currentSegment.currentUnit();

        currentSegment.setCurrentUnitNull();

        addSegment(currentSegment);

        IndexInfo indexInfo = new IndexInfo();
        indexInfo.offset = currentOffset;
        currentSegment = new Segment(indexInfo, this);

        unit.transfer(currentOffset);

        currentSegment.addUnit(unit);
      }
    }
  }

  public Unit.Record[] getRecordByValue(Unit.FieldValue[] values, int width)
      throws Exception {
    if (in == null) {

    }

    if (!isPrimaryKeyFile()) {
      return getRecordByOrder(values, width);
    }

    boolean found = false;
    int key = 0;
    int keyIndex = primaryKeyIndex();
    for (int i = 0; i < width; i++) {
      if (keyIndex == values[i].idx) {
        key = Util.bytes2int(values[i].value, 0, values[i].len);
        found = true;
        break;
      }
    }

    if (!found) {
      return getRecordByOrder(values, width);
    }

    Segment segment[] = getSegmentByKey(key);
    if (segment == null) {
      return null;
    }

    int len = 0;
    int size = segment.length;
    ArrayList<Record[]> resultRecord = new ArrayList<Record[]>(size);
    for (int i = 0; i < size; i++) {
      Record[] record = segment[i].getRecordByValue(key, values, width);
      if (record != null) {
        len += record.length;
        if (len > ConstVar.MaxRecord) {
          throw new SEException.MaxRecordLimitedException(
              "max record limited exceed, num:" + len + "limited:"
                  + ConstVar.MaxRecord);
        }

        resultRecord.add(record);
      }
    }

    int idx = 0;
    Record[] result = new Record[len];
    for (int i = 0; i < size; i++) {
      Record[] tmpRecord = resultRecord.get(i);
      int length = tmpRecord.length;
      for (int j = 0; j < length; j++) {
        result[idx++] = tmpRecord[j];
      }
    }

    return result;
  }

  public Unit.Record getRecordByLine(int line) throws Exception {
    if (in == null || lineIndexOffset == -1) {

      throw new SEException.InvalidParameterException(
          "FormatDataFile object not init");
    }

    if (line < 0 || line >= recordNum) {
      return null;
    }

    Segment segment = getSegmentByLine(line);
    if (segment == null) {
      return null;
    }

    return segment.getRecordByLine(line);
  }

  public Record getRecordByLine(int line, Record record) throws Exception {
    if (in == null || lineIndexOffset == -1) {

      throw new SEException.InvalidParameterException(
          "FormatDataFile object not init");
    }

    if (line < 0 || line >= recordNum) {
      return null;
    }

    Segment segment = getSegmentByLine(line);
    if (segment == null) {
      return null;
    }

    return segment.getRecordByLine(line, record);
  }

  public Unit.Record[] getRecordByOrder(Unit.FieldValue[] values, int width)
      throws Exception {
    if (in == null) {

      throw new SEException.InvalidParameterException(
          "FormatDataFile object not init");
    }

    ArrayList<Record[]> resultRecord = new ArrayList<Record[]>(100);

    int len = 0;
    for (int i = 0; i < segmentNum; i++) {
      Record[] record = segments.get(i).getRecordByOrder(values, width);
      if (record != null) {
        len += record.length;
        if (len > ConstVar.MaxRecord) {
          throw new SEException.MaxRecordLimitedException(
              "max record limited exceed, num:" + len + "limited:"
                  + ConstVar.MaxRecord);
        }

        resultRecord.add(record);
      }
    }

    int size = resultRecord.size();
    if (size == 0) {
      return null;
    }

    int idx = 0;
    Record[] result = new Record[len];
    for (int i = 0; i < size; i++) {
      Record[] tmpRecord = resultRecord.get(i);
      int length = tmpRecord.length;
      for (int j = 0; j < length; j++) {
        result[idx++] = tmpRecord[j];
      }
    }

    return result;
  }

  public boolean hasNext() {
    if (currentLine < recordNum) {
      return true;
    }

    return false;
  }

  public boolean seek(int line) throws Exception {
    if (line < 0 || line > recordNum) {
      return false;
    }
    Segment seg = getSegmentByLine(line);
    if (currentSegment != seg) {
      currentSegment = seg;
    }
    currentSegment.seek(line);
    currentLine = line;
    return true;
  }

  public Record getNextRecord() throws Exception {
    try {
      if (!hasNext()) {
        return null;
      }

      if (currentSegment == null && segments.size() > 0) {
        currentSegment = segments.get(0);
      }

      if (currentSegment == null) {
        return null;
      }

      if (currentLine >= currentSegment.endLine()) {
        currentSegment = nextSegment();
      }

      Record record = currentSegment.getNextRecord();

      if (currentLine++ > recordNum) {
        currentLine = recordNum;
      }

      return record;
    } catch (Exception e) {
      e.printStackTrace();
      LOG.info("next get exception:" + e.getMessage());
      throw e;
    }
  }

  public Record getNextRecord(Record record) throws Exception {
    try {
      if (!hasNext()) {
        return null;
      }

      if (currentSegment == null && segments.size() > 0) {
        currentSegment = segments.get(0);
      }

      if (currentSegment == null) {
        return null;
      }

      if (currentLine >= currentSegment.endLine()) {
        currentSegment = nextSegment();
      }

      record = currentSegment.getNextRecord(record);

      if (currentLine++ > recordNum) {
        currentLine = recordNum;
      }

      return record;
    } catch (Exception e) {
      LOG.info("next get exception:" + e.getMessage());
      throw e;

    }
  }

  private Segment nextSegment() {
    int idx = currentSegment.index() + 1;
    if (idx < segments.size()) {
      return segments.get(idx);
    } else {

      currentLine = recordNum;
      return currentSegment;
    }
  }

  private boolean checkHeadInfo(Head head) {
    return true;
  }

  private void addHead(Head head) throws IOException {

    head.persistent(out);
  }

  void unpersistentIndexMeta(FSDataInputStream in) throws IOException {
    IndexMeta indexMeta = new IndexMeta();
    indexMeta.unpersistent(in);
    recordNum = indexMeta.recordNum();
    segmentNum = indexMeta.unitNum();
    keyIndexOffset = indexMeta.keyIndexOffset();
    lineIndexOffset = indexMeta.lineIndexOffset();
  }

  private void unpersistentSegmentIndex() throws Exception {
    in.seek(fileLen - ConstVar.IndexMetaOffset);
    unpersistentIndexMeta(in);

    if (keyIndexOffset != -1) {
      in.seek(keyIndexOffset);
      unpersistentKeySegmentIndex();
    }

    if (lineIndexOffset == -1 && recordNum > 0) {
      throw new SEException.InnerException(
          "line index offset error when load seg index:" + lineIndexOffset);
    }

    if (recordNum > 0) {
      in.seek(lineIndexOffset);
    }
    unpersistentLineSegmentIndex();
  }

  private void unpersistentKeySegmentIndex() throws IOException {
    if (segmentIndex == null) {
      segmentIndex = new SegmentIndex();
    }

    segmentIndex.addIndexMode(ConstVar.KeyMode);

    for (int i = 0; i < segmentNum; i++) {
      IndexInfo indexInfo = new IndexInfo();
      indexInfo.unpersistentKeyIndexInfo(in);

      segmentIndex.addIndexInfo(indexInfo, ConstVar.KeyMode);
    }
  }

  private void unpersistentLineSegmentIndex() throws IOException {
    if (segmentIndex == null) {
      segmentIndex = new SegmentIndex();
    }
    segmentIndex.addIndexMode(ConstVar.LineMode);

    if (!hasLoadAllSegmentDone) {
      segments.clear();
    }

    for (int i = 0; i < segmentNum; i++) {
      IndexInfo indexInfo = new IndexInfo();
      indexInfo.unpersistentLineIndexInfo(in);

      segmentIndex.addIndexInfo(indexInfo, ConstVar.LineMode);

      if (!hasLoadAllSegmentDone) {

        Segment segment = new Segment(indexInfo, this);

        segments.add(segment);
      }
    }

    if (segments.size() == segmentNum) {
      hasLoadAllSegmentDone = true;
    }
  }

  private Segment[] getSegmentByKey(int key) throws Exception {
    BlockIndex.IndexInfo[] indexInfo = segmentIndex.getIndexInfoByKey(key);
    if (indexInfo == null) {
      return null;
    }

    int size = indexInfo.length;
    Segment[] segment = new Segment[size];
    for (int i = 0; i < size; i++) {
      segment[i] = segments.get(indexInfo[i].idx);

      if (segment[i] == null) {

        segment[i] = new Segment(indexInfo[i], this);
      }
    }

    return segment;
  }

  private Segment getSegmentByLine(int line) {
    IndexInfo indexInfo = segmentIndex.getIndexInfoByLine(line);
    if (indexInfo != null) {
      return segments.get(indexInfo.idx);
    }
    return null;
  }

  private void addSegment(Segment segment) throws Exception {
    segment.persistent(out);

    segments.add(segment);

    IndexInfo indexInfo = new IndexInfo();

    if (segmentIndex == null) {
      segmentIndex = new SegmentIndex();
    }

    if (isPrimaryKeyFile()) {
      indexInfo.beginKey = segment.beginKey();
      indexInfo.endKey = segment.endKey();

      segmentIndex.addIndexInfo(indexInfo, ConstVar.KeyMode);
    }

    indexInfo.beginLine = segment.beginLine();
    indexInfo.endLine = segment.endLine();
    indexInfo.offset = segment.offset();
    indexInfo.idx = segmentNum;
    indexInfo.len = segment.len();
    segmentIndex.addIndexInfo(indexInfo, ConstVar.LineMode);

    segmentNum++;

    currentOffset = getWritePos();

  }

  private void persistentSegmentIndex() throws IOException {

    if (segmentIndex == null) {
      segmentIndex = new SegmentIndex();
    }

    OffsetInfo offsetInfo = segmentIndex.persistent(out);

    keyIndexOffset = offsetInfo.keyIndexOffset;
    lineIndexOffset = offsetInfo.lineIndexOffset;

    out.writeInt(recordNum);
    out.writeInt(segmentNum);

    out.writeLong(keyIndexOffset);
    out.writeLong(lineIndexOffset);
  }

  public long getCurrentSegmentOffset() {
    return segmentNum * confSegmentSize();
  }

  public int currentLine() {
    return currentLine;
  }
}
