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

import org.apache.hadoop.conf.Configuration;

import FormatStorage.BlockIndex.IndexInfo;

public class FormatFileStatus {
  static FormatDataFile fd = null;

  static public void main(String[] argv) throws Exception {
    int argc = argv.length;
    if (argc == 0) {
      usage();
      return;
    }

    String file = argv[0];

    Configuration conf = new Configuration();
    fd = new FormatDataFile(conf);
    fd.open(file);

    if (argc == 1) {
      showFileInfo();
      return;
    } else if (argc == 2) {
      int segmentNo = Integer.valueOf(argv[1]);

      showSegmentInfo(segmentNo);
      return;
    } else if (argc == 3) {
      int segmentNo = Integer.valueOf(argv[1]);
      int unitNo = Integer.valueOf(argv[2]);

      showUnitInfo(segmentNo, unitNo);
      return;
    } else {
      usage();
      return;
    }
  }

  static public void usage() {
    System.out.println("Usage: cmd fileName [segmentNo] [unitNo]\n");
  }

  static public void showFileInfo() throws Exception {
    long fileLen = fd.getFileLen();
    int segNum = fd.segmentNum();
    int recordNum = fd.recordNum();
    System.out.println("FileLen:" + fileLen + ",RecordNum:" + recordNum
        + ",SegmentNum:" + segNum + "\n");

    SegmentIndex segmentIndex = fd.segmentIndex();
    if (segmentIndex == null) {
      System.out.println("Segment Index load fail \n");
      return;
    }

    segmentIndex.show();
  }

  static public void showSegmentInfo(int segNo) throws Exception {
    IndexInfo indexInfo = fd.segmentIndex().getIndexInfo(segNo);
    if (indexInfo != null)
      System.out.println("SegmentNo:" + segNo + " info: \n" + "offset:"
          + indexInfo.offset + "len:" + indexInfo.len + "idx:" + indexInfo.idx);
    else {
      System.out.println("getIndexInfo null \n");
    }

    Segment segment = new Segment(indexInfo, fd);
    segment.unpersistentUnitIndex(fd.in());

    System.out.println("UnitNum:" + segment.unitNum() + "RecordNum:"
        + segment.recordNum() + "Begin:" + segment.beginLine() + "End:"
        + segment.endLine());
  }

  static public void showUnitInfo(int segNo, int unitNo) throws Exception {
    IndexInfo indexInfo = fd.segmentIndex().getIndexInfo(segNo);
    if (indexInfo == null) {
      System.out.println("getIndexInfo null \n");
      throw new Exception("index info is null");
    }

    System.out.println("SegmentNo:" + segNo + " info: \n" + "offset:"
        + indexInfo.offset + "len:" + indexInfo.len + "idx:" + indexInfo.idx);

    Segment segment = new Segment(indexInfo, fd);
    segment.unpersistentUnitIndex(fd.in());

    UnitIndex unitIndex = segment.unitIndex();
    if (unitIndex == null) {
      System.out.println("load unit index fail");
      throw new Exception("unit index info is null");
    }

    IndexInfo info = unitIndex.getIndexInfo(unitNo);
    if (info == null) {
      System.out.println("getIndexInfo null \n");
      return;
    } else {
      System.out.println("UnitNo:" + unitNo + " info: \n" + "offset:"
          + info.offset + "len:" + info.len + "idx:" + info.idx);
    }

    Unit unit = new Unit(info, segment);
    unit.loadDataMeta(fd.in());

    System.out.println("RecordNum:" + unit.recordNum() + ",chunkLen:"
        + unit.chunkLen() + ",metaOffset:" + unit.metaOffset());
  }
}
