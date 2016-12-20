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
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import Comm.ConstVar;
import Comm.SEException;
import FormatStorage.BlockIndex.IndexInfo;
import FormatStorage.BlockIndex.IndexMeta;
import FormatStorage.BlockIndex.KeyRange;
import FormatStorage.BlockIndex.OffsetInfo;
import FormatStorage.Unit.Record;
import FormatStorage.Unit.RowRecord;

public class Segment {
  public static final Log LOG = LogFactory.getLog("Segment");

  private long offset = -1;
  private long len = 0;
  private int index = 0;
  private long currentOffset = -1;
  private long remain = 0;
  private long total = 0;

  private long keyIndexOffset = -1;
  private long lineIndexOffset = -1;

  private int beginKey;
  private int endKey;
  private int beginLine;
  private int endLine;
  int recordNum = 0;

  FormatDataFile formatData = null;

  private int unitNum = 0;
  private ArrayList<Unit> units = new ArrayList<Unit>(30);
  private UnitIndex unitIndex = null;

  private boolean hasLoadAllUnitDone = false;
  private Unit currentUnit = null;
  private int currentLine = 0;
  private byte[] gap = null;

  private boolean full = false;
  private boolean last = false;

  int nn = 0;
  long logtime = 0;

  public Segment(IndexInfo indexInfo, FormatDataFile formatData)
      throws IOException {

    this.beginKey = indexInfo.beginKey;
    this.endKey = indexInfo.endKey;
    this.beginLine = indexInfo.beginLine;
    this.endLine = indexInfo.endLine;

    this.offset = indexInfo.offset;
    this.len = indexInfo.len;
    this.index = indexInfo.idx;
    this.formatData = formatData;

    currentOffset = offset;
    currentLine = beginLine;

    total = formatData.confSegmentSize();
    remain = total;

    if (formatData.workStatus() == ConstVar.WS_Write) {
      long tmpOffset = offset - formatData.getCurrentSegmentOffset();
      if (tmpOffset > 0) {
        total -= tmpOffset;
      }

      this.len = ConstVar.IndexMetaOffset;
      remain = total - this.len;

      long pos = formatData.out() == null ? 0 : formatData.getWritePos();

    }
  }

  public long offset() {
    return offset;
  }

  public long currentOffset() {
    return currentOffset;
  }

  public long len() {
    return len;
  }

  public int index() {
    return index;
  }

  public long remain() {
    return remain;
  }

  public long total() {
    return total;
  }

  public int beginLine() {
    return beginLine;
  }

  void setBeginLine(int beginLine) {
    this.beginLine = beginLine;
  }

  void setEndLine(int endLine) {
    this.endLine = endLine;
  }

  public int endLine() {
    return endLine;
  }

  public int beginKey() {
    return beginKey;
  }

  public int endKey() {
    return endKey;
  }

  public int recordNum() {
    return recordNum;
  }

  public int unitNum() {
    return unitNum;
  }

  void setUnitNum(int unitNum) {
    this.unitNum = unitNum;
  }

  public long keyIndexOffset() {
    return keyIndexOffset;
  }

  public long lineIndexOffset() {
    return lineIndexOffset;
  }

  public Unit currentUnit() {
    return currentUnit;
  }

  void setCurrentUnit(Unit unit) {
    currentUnit = unit;
  }

  void setCurrentUnitNull() {
    currentUnit = null;
  }

  ArrayList<Unit> units() {
    return units;
  }

  UnitIndex unitIndex() {
    return unitIndex;
  }

  void setUnitIndex(UnitIndex unitIndex) {
    this.unitIndex = unitIndex;
  }

  void setLastSegment() {
    last = true;
  }

  int currentLine() {
    return currentLine;
  }

  public Unit.Record[] getRecordByValue(int key, Unit.FieldValue[] values,
      int width) throws Exception {
    if (offset < 0 || len == 0) {

      throw new SEException.InvalidParameterException("invalid parameter");
    }

    if (unitIndex == null) {
      unitIndex = new UnitIndex();

      unpersistentUnitIndex(formatData.in());
    }

    Unit[] unit = getUnitByKey(key);
    if (unit == null) {
      return null;
    }

    int len = 0;
    int size = unit.length;

    ArrayList<Record[]> resultRecord = new ArrayList<Record[]>(size);
    for (int i = 0; i < size; i++) {
      Record[] record = unit[i].getRecordByValue(values, width,
          ConstVar.OP_GetSpecial);
      if (record != null) {
        len += record.length;
        if (len > ConstVar.MaxRecord) {

          throw new SEException.MaxRecordLimitedException("max limited exceed");
        }

        resultRecord.add(record);
      }
    }

    int idx = 0;
    Record[] result = new Record[len];
    for (int i = 0; i < resultRecord.size(); i++) {
      Record[] tmpRecord = resultRecord.get(i);
      int length = tmpRecord.length;
      for (int j = 0; j < length; j++) {
        result[idx++] = tmpRecord[j];
      }
    }

    return result;
  }

  public Record getRecordByLine(int line) throws Exception {
    if (offset < 0 || len == 0) {
      throw new SEException.InnerException("invalid offset or len");
    }

    if (unitIndex == null) {
      unitIndex = new UnitIndex();

      unpersistentUnitIndex(formatData.in());
    }

    if (line < beginLine || line >= endLine) {
      return null;
    }

    Unit unit = getUnitByLine(line);
    if (unit == null) {
      return null;
    }

    return unit.getRecordByLine(line);
  }

  public Record getRecordByLine(int line, Record record) throws Exception {
    if (offset < 0 || len == 0) {
      throw new SEException.InnerException("invalid offset or len");
    }

    if (unitIndex == null) {
      unitIndex = new UnitIndex();

      unpersistentUnitIndex(formatData.in());
    }

    if (line < beginLine || line >= endLine) {
      return null;
    }

    Unit unit = getUnitByLine(line);
    if (unit == null) {
      return null;
    }

    return unit.getRecordByLine(line, record);
  }

  public RowRecord getRowByLine(int line) throws Exception {
    if (offset < 0 || len == 0) {
      throw new SEException.InnerException("invalid offset or len");
    }

    if (unitIndex == null) {
      unitIndex = new UnitIndex();

      unpersistentUnitIndex(formatData.in());
    }

    if (line < beginLine || line >= endLine) {
      return null;
    }

    Unit unit = getUnitByLine(line);
    if (unit == null) {
      return null;
    }

    return unit.getRowByLine(line);
  }

  public Unit.Record[] getRecordByOrder(Unit.FieldValue[] values, int width)
      throws Exception {

    ArrayList<Record[]> resultRecord = new ArrayList<Record[]>(unitNum);
    byte op = 0;
    if (values == null) {
      op = ConstVar.OP_GetAll;
    } else {
      op = ConstVar.OP_GetSpecial;
    }

    if (unitIndex == null) {

      unitIndex = new UnitIndex();

      unpersistentUnitIndex(formatData.in());
    }

    int len = 0;
    for (int i = 0; i < unitNum; i++) {
      Record[] record = units.get(i).getRecordByValue(values, width, op);

      if (record != null) {
        len += record.length;
        if (len > ConstVar.MaxRecord) {

          throw new SEException.MaxRecordLimitedException("max limited exceed");
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

  public boolean seek(int line) throws Exception {
    if (line < 0 || line > recordNum) {
      return false;
    }
    if (unitIndex == null) {
      unitIndex = new UnitIndex();
      unpersistentUnitIndex(formatData.in());
    }

    Unit u = getUnitByLine(line);
    if (currentUnit != u) {
      currentUnit = u;
    }
    this.currentLine = line;
    currentUnit.seek(line);

    return true;

  }

  public Record getNextRecord() throws Exception {
    if (unitIndex == null) {
      unitIndex = new UnitIndex();

      unpersistentUnitIndex(formatData.in());
    }

    if (currentUnit == null && units.size() > 0) {
      currentUnit = units.get(0);
    }

    if (currentUnit == null) {
      return null;
    }

    if (currentLine >= currentUnit.endLine()) {
      currentUnit = nextUnit();
    }

    Record record = currentUnit.getNextRecord();
    if (currentLine++ > endLine) {
      currentLine = endLine;
    }

    return record;
  }

  public Record getNextRecord(Record record) throws Exception {
    if (currentLine >= endLine) {
      return null;
    }

    if (unitIndex == null) {
      unitIndex = new UnitIndex();

      unpersistentUnitIndex(formatData.in());
    }

    if (currentUnit == null && units.size() > 0) {
      currentUnit = units.get(0);
    }

    if (currentUnit == null) {
      return null;
    }

    if (currentLine >= currentUnit.endLine()) {
      currentUnit = nextUnit();
    }

    record = currentUnit.getNextRecord(record);
    if (currentLine++ > endLine) {
      currentLine = endLine;
    }

    return record;
  }

  private Unit nextUnit() {
    int idx = currentUnit.index() + 1;
    if (idx >= units.size()) {
      return currentUnit;
    }

    return units.get(idx);
  }

  public void addRecord(Record record) throws Exception {
    if (currentUnit == null) {
      IndexInfo indexInfo = new IndexInfo();
      indexInfo.offset = currentOffset;
      currentUnit = new Unit(indexInfo, this);
    }

    try {
      currentUnit.addRecord(record);
    } catch (SEException.UnitFullException e) {

      addUnit(currentUnit);
      currentUnit = null;
    }
  }

  public void fillGap() {
    if (gap != null) {
      gap = null;
    }

    remain = total - len;

    if (remain > 0) {
      gap = new byte[(int) remain];
    }

  }

  public void persistent(FSDataOutputStream out) throws Exception {

    if (currentUnit != null) {

      addUnit(currentUnit);

      currentUnit = null;
    }

    persistentUnitIndex(out);
    persistentUnitIndexMeta(out);

    persistentDummy(out);
  }

  public boolean canPersistented() throws Exception {
    if (currentUnit == null) {
      return true;
    }

    long need = currentUnit.len() + ConstVar.LineIndexRecordLen;
    if (formatData.isPrimaryKeyFile()) {
      need += ConstVar.KeyIndexRecordLen;
    }

    if (remain < need) {
      return false;
    }

    return true;
  }

  public void setKeyRange(KeyRange keyRange) {
    beginKey = keyRange.begin;
    endKey = keyRange.end;
  }

  private Unit[] getUnitByKey(int key) throws Exception {
    IndexInfo[] indexInfo = unitIndex.getIndexInfoByKey(key);
    if (indexInfo == null) {
      return null;
    }

    int size = indexInfo.length;
    Unit[] unit = new Unit[size];

    for (int i = 0; i < size; i++) {
      unit[i] = units.get(indexInfo[i].idx);
      if (unit[i] == null) {
        unit[i] = new Unit(indexInfo[i], this);
      }
    }

    return unit;
  }

  private Unit getUnitByLine(int line) throws IOException {
    IndexInfo indexInfo = unitIndex.getIndexInfoByLine(line);
    if (indexInfo == null) {
      return null;
    }

    Unit unit = units.get(indexInfo.idx);
    if (unit == null) {
      unit = new Unit(indexInfo, this);
    }

    return unit;
  }

  void unpersistentIndexMeta(FSDataInputStream in) throws IOException {
    IndexMeta indexMeta = new IndexMeta();
    indexMeta.unpersistent(in);

    recordNum = indexMeta.recordNum();
    unitNum = indexMeta.unitNum();
    keyIndexOffset = indexMeta.keyIndexOffset();
    lineIndexOffset = indexMeta.lineIndexOffset();
  }

  public void unpersistentUnitIndex(FSDataInputStream in) throws Exception {
    if (unitIndex == null) {
      unitIndex = new UnitIndex();
    }

    long unitIndexMetaOffset = -1;

    unitIndexMetaOffset = offset + len - ConstVar.IndexMetaOffset;

    in.seek(unitIndexMetaOffset);

    unpersistentIndexMeta(in);

    if (keyIndexOffset != -1) {
      in.seek(keyIndexOffset);
      unpersistentKeyUnitIndex(in);
    }

    if (lineIndexOffset < 0) {
      throw new SEException.InnerException(
          "line index offset error when load unitIndex:" + lineIndexOffset);
    }
    in.seek(lineIndexOffset);
    unpersistentLineUnitIndex(in);

  }

  void unpersistentKeyUnitIndex(FSDataInputStream in) throws IOException {
    unitIndex.addIndexMode(ConstVar.KeyMode);

    for (int i = 0; i < unitNum; i++) {
      IndexInfo indexInfo = new IndexInfo();
      indexInfo.unpersistentKeyIndexInfo(in);

      unitIndex.addIndexInfo(indexInfo, ConstVar.KeyMode);
    }
  }

  void unpersistentLineUnitIndex(FSDataInputStream in) throws IOException {
    unitIndex.addIndexMode(ConstVar.LineMode);

    if (!hasLoadAllUnitDone) {
      units.clear();
    }

    for (int i = 0; i < unitNum; i++) {
      IndexInfo indexInfo = new IndexInfo();
      indexInfo.unpersistentLineIndexInfo(in);

      unitIndex.addIndexInfo(indexInfo, ConstVar.LineMode);

      if (!hasLoadAllUnitDone) {
        Unit unit = new Unit(indexInfo, this);
        units.add(unit);

      }
    }

    if (units.size() == unitNum) {
      hasLoadAllUnitDone = true;
    }
  }

  public void addUnit(Unit unit) throws Exception {
    if (unitIndex == null) {
      unitIndex = new UnitIndex();

    }

    boolean primaryKey = formatData.isPrimaryKeyFile();

    long unitLen = unit.len();
    long need = unitLen + ConstVar.LineIndexRecordLen;
    if (primaryKey) {
      need += ConstVar.KeyIndexRecordLen;
    }

    if (remain < need) {
      full = true;

      throw new SEException.SegmentFullException("segment full, need:" + need
          + "remain:" + remain);
    }

    unit.persistent(formatData.out());

    if (unitNum == 0) {
      beginLine = unit.beginLine();
      beginKey = unit.beginKey();
    }

    if (beginLine > unit.beginLine()) {
      beginLine = unit.beginLine();
    }
    if (endLine < unit.endLine()) {
      endLine = unit.endLine();
    }

    if (beginKey > unit.beginKey()) {
      beginKey = unit.beginKey();
    }
    if (endKey < unit.endKey()) {
      endKey = unit.endKey();
    }
    recordNum += unit.recordNum;

    IndexInfo indexInfo = new IndexInfo();
    if (primaryKey) {
      indexInfo.beginKey = unit.beginKey;
      indexInfo.endKey = unit.endKey;

      unitIndex.addIndexInfo(indexInfo, ConstVar.KeyMode);

      len += ConstVar.KeyIndexRecordLen;
    }

    indexInfo.beginLine = unit.beginLine;
    indexInfo.endLine = unit.endLine;

    indexInfo.offset = unit.offset();
    indexInfo.idx = unitNum;
    indexInfo.len = unitLen;
    unitIndex.addIndexInfo(indexInfo, ConstVar.LineMode);

    len += ConstVar.LineIndexRecordLen;

    len += unitLen;

    unitNum++;
    currentOffset += unitLen;

    remain -= need;

    remain = total - len;

  }

  void persistentUnitIndex(FSDataOutputStream out) throws IOException {
    OffsetInfo offsetInfo = unitIndex.persistent(out);

    keyIndexOffset = offsetInfo.keyIndexOffset;
    lineIndexOffset = offsetInfo.lineIndexOffset;
  }

  void persistentDummy(FSDataOutputStream out) throws IOException {
    fillGap();
    if (out == null) {
      LOG.info("out is null");
    }

    if (!last && gap != null) {
      out.write(gap);
    }

    gap = null;
  }

  void persistentUnitIndexMeta(FSDataOutputStream out) throws Exception {
    int trecordNum = endLine - beginLine;
    if (trecordNum != recordNum) {
      throw new SEException.InnerException("recordNum != endline - beginline");
    }

    out.writeInt(endLine - beginLine);
    out.writeInt(unitNum);

    out.writeLong(keyIndexOffset);
    out.writeLong(lineIndexOffset);

  }
}
