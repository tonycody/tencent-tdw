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
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import Comm.ConstVar;
import Comm.SEException;

public class BlockIndex {
  public static final Log LOG = LogFactory.getLog("BlockIndex");

  protected int indexMode = 0x10;

  private int len = 0;

  static public class KeyRange {
    int begin;
    int end;

    public KeyRange() {

    }
  }

  static public class IndexMeta {
    private int recordNum = 0;
    private int unitNum = 0;
    private long keyIndexOffset = -1;
    private long lineIndexOffset = -1;

    void unpersistent(FSDataInputStream in) throws IOException {
      recordNum = in.readInt();
      unitNum = in.readInt();
      keyIndexOffset = in.readLong();
      lineIndexOffset = in.readLong();
    }

    int recordNum() {
      return recordNum;
    }

    int unitNum() {
      return unitNum;
    }

    long keyIndexOffset() {
      return keyIndexOffset;
    }

    long lineIndexOffset() {
      return lineIndexOffset;
    }
  }

  static public class IndexInfo {
    int beginLine = 0;
    int endLine = 0;
    int beginKey = Integer.MIN_VALUE;;
    int endKey = Integer.MIN_VALUE;;
    int idx = 0;
    long offset = -1;
    long len = 0;

    public long offset() {
      return offset;
    }

    public void setOffset(long offset) {
      this.offset = offset;
    }

    public long len() {
      return len;
    }

    public void setLen(long len) {
      this.len = len;
    }

    public int idx() {
      return idx;
    }

    public int beginLine() {
      return beginLine;
    }

    public void setBeginLine(int beginLine) {
      this.beginLine = beginLine;
    }

    public int endLine() {
      return endLine;
    }

    public void setEndLine(int endLine) {
      this.endLine = endLine;
    }

    public int beginKey() {
      return beginKey;
    }

    public void setBeginKey(int beginKey) {
      this.beginKey = beginKey;
    }

    public int endKey() {
      return endKey;
    }

    public void setEndKey(int endKey) {
      this.endKey = endKey;
    }

    public void unpersistentLineIndexInfo(FSDataInputStream in)
        throws IOException {
      beginLine = in.readInt();
      endLine = in.readInt();
      offset = in.readLong();
      len = in.readLong();
      idx = in.readInt();
    }

    public void unpersistentKeyIndexInfo(FSDataInputStream in)
        throws IOException {
      beginKey = in.readInt();
      endKey = in.readInt();
    }

    public void persistentLineIndexInfo(FSDataOutputStream out)
        throws IOException {
      out.writeInt(beginLine);
      out.writeInt(endLine);
      out.writeLong(offset);

      out.writeLong(len);
      out.writeInt(idx);

    }

    public void persistentKeyIndexInfo(FSDataOutputStream out)
        throws IOException {
      out.writeInt(beginKey);
      out.writeInt(endKey);
    }
  }

  static public class OffsetInfo {
    long keyIndexOffset;
    long lineIndexOffset;

    public OffsetInfo() {
      keyIndexOffset = -1;
      lineIndexOffset = -1;
    }
  }

  private ArrayList<IndexInfo> keyIndexInfos = new ArrayList<IndexInfo>();
  private ArrayList<IndexInfo> lineIndexInfos = new ArrayList<IndexInfo>();

  public BlockIndex() {
    indexMode = ConstVar.LineMode;
  }

  public int len() {
    return len;
  }

  public void setIndexMode(int mode) throws Exception {
    if (mode != ConstVar.LineMode && mode != ConstVar.KeyMode) {
      throw new SEException.InvalidParameterException("invalid index mode:"
          + mode);
    }

    indexMode = mode;
  }

  public void addIndexMode(int mode) {
    if (mode != ConstVar.LineMode && mode != ConstVar.KeyMode) {
    }

    indexMode |= mode;
  }

  public IndexInfo[] getIndexInfoByKey(int value) throws Exception {
    int size = keyIndexInfos.size();
    int lineSize = lineIndexInfos.size();
    if (size != lineSize) {
      throw new SEException.InnerException(
          "key indexInfo size != line indexInfo size");
    }

    ArrayList<IndexInfo> info = new ArrayList<IndexInfo>(10);

    for (int i = 0; i < size; i++) {
      IndexInfo indexInfo = keyIndexInfos.get(i);
      if (value >= indexInfo.beginKey && value <= indexInfo.endKey) {
        IndexInfo lineInfo = lineIndexInfos.get(i);

        IndexInfo tmpInfo = new IndexInfo();
        tmpInfo.beginKey = indexInfo.beginKey;
        tmpInfo.endKey = indexInfo.endKey;

        tmpInfo.beginLine = lineInfo.beginLine;
        tmpInfo.endLine = lineInfo.endLine;
        tmpInfo.offset = lineInfo.offset;
        tmpInfo.len = lineInfo.len;
        tmpInfo.idx = lineInfo.idx;

        info.add(tmpInfo);
      }
    }

    size = info.size();
    if (size == 0) {
      return null;
    }

    IndexInfo[] result = new IndexInfo[size];
    for (int i = 0; i < size; i++) {
      result[i] = info.get(i);
    }

    info.clear();
    info = null;

    return result;
  }

  public IndexInfo getIndexInfoByLine(int value) {
    IndexInfo info = null;

    int size = lineIndexInfos.size();
    for (int i = 0; i < size; i++) {
      IndexInfo indexInfo = lineIndexInfos.get(i);
      if (value >= indexInfo.beginLine && value < indexInfo.endLine) {
        return indexInfo;
      }
    }

    return info;
  }

  public void addIndexInfo(IndexInfo indexInfo, int flag) {
    if (flag == ConstVar.LineMode) {
      lineIndexInfos.add(indexInfo);
      len += ConstVar.LineIndexRecordLen;
      return;
    }

    else if (flag == ConstVar.KeyMode) {
      keyIndexInfos.add(indexInfo);
      len += ConstVar.KeyIndexRecordLen;
      return;
    } else {
    }
  }

  public OffsetInfo persistent(FSDataOutputStream out) throws IOException {
    OffsetInfo offsetInfo = new OffsetInfo();

    if (keyIndexInfos.size() != 0) {
      offsetInfo.keyIndexOffset = out.getPos();
      persistentKeyIndex(out);
    }

    if (lineIndexInfos.size() != 0) {
      offsetInfo.lineIndexOffset = out.getPos();
      persistentLineIndex(out);
    }

    lineIndexInfos.clear();
    lineIndexInfos = null;

    keyIndexInfos.clear();
    keyIndexInfos = null;

    return offsetInfo;
  }

  public IndexInfo getIndexInfo(int idx) throws IOException {
    int size = lineIndexInfos.size();
    if (idx >= size || idx < 0) {
      return null;
    }

    return lineIndexInfos.get(idx);
  }

  private void persistentKeyIndex(FSDataOutputStream out) throws IOException {
    int size = keyIndexInfos.size();
    for (int i = 0; i < size; i++) {
      keyIndexInfos.get(i).persistentKeyIndexInfo(out);
    }
  }

  private void persistentLineIndex(FSDataOutputStream out) throws IOException {
    int size = lineIndexInfos.size();
    for (int i = 0; i < size; i++) {
      lineIndexInfos.get(i).persistentLineIndexInfo(out);
    }
  }

  public ArrayList<IndexInfo> indexInfo() {
    ArrayList<IndexInfo> infos = new ArrayList<IndexInfo>(10);

    int ksize = keyIndexInfos.size();
    int size = lineIndexInfos.size();
    for (int i = 0; i < size; i++) {
      IndexInfo indexInfo = new IndexInfo();

      if (ksize != 0) {
        IndexInfo info1 = keyIndexInfos.get(i);
        indexInfo.beginKey = info1.beginKey;
        indexInfo.endKey = info1.endKey;
      }

      IndexInfo info2 = lineIndexInfos.get(i);
      indexInfo.beginLine = info2.beginLine;
      indexInfo.endLine = info2.endLine;
      indexInfo.offset = info2.offset;
      indexInfo.len = info2.len;
      indexInfo.idx = info2.idx;

      infos.add(indexInfo);
    }
    return infos;
  }

  public ArrayList<IndexInfo> lineIndexInfos() {
    return lineIndexInfos;
  }

  public ArrayList<IndexInfo> keyIndexInfos() {
    return keyIndexInfos;
  }

  public void show() {
    ArrayList<IndexInfo> infos = indexInfo();
    showIndexInfo(infos);
  }

  private void showIndexInfo(ArrayList<IndexInfo> vec) {
    int size = vec.size();
    for (int i = 0; i < size; i++) {
      IndexInfo indexInfo = vec.get(i);
      System.out.println("beginKey:" + indexInfo.beginKey + "endKey:"
          + indexInfo.endKey + "beginLine:" + indexInfo.beginLine + "endLine:"
          + indexInfo.endLine);
      System.out.println("offset:" + indexInfo.offset + "len:" + indexInfo.len
          + "idx:" + indexInfo.idx);
    }
  }
}
