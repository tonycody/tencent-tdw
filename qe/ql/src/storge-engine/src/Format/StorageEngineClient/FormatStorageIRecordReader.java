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
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import Comm.ConstVar;
import FormatStorage1.IFormatDataFile;
import FormatStorage1.IRecord;
import FormatStorage1.ISegmentIndex;
import StorageEngineClient.MyLineRecordReader.LineReader;

public class FormatStorageIRecordReader implements
    RecordReader<LongWritable, IRecord> {
  public static final Log LOG = LogFactory
      .getLog(FormatStorageIRecordReader.class);
  Configuration conf;

  private IFormatDataFile ifdf;
  int recnum = 0;
  int currentrec = 0;
  long beginline;
  String file = null;
  private boolean isGZ = false;
  private CompressionCodecFactory compressionCodecs = null;
  private LineReader in;
  int maxLineLength = Integer.MAX_VALUE;
  private HashMap<Integer, IRecord.IFType> fieldtypes = new HashMap<Integer, IRecord.IFType>();
  private Text tValue = new Text();

  public FormatStorageIRecordReader(CombineFileSplit split, Configuration conf,
      Reporter report, Integer idx) throws IOException {
    int id = idx.intValue();
    this.conf = conf;
    Path p = split.getPath(id);
    file = p.toString();
    if (file.toLowerCase().endsWith(".gz")) {
      int index = file.lastIndexOf("_");
      String sub = file.substring(index + 1, file.length() - 3);
      this.recnum = Integer.valueOf(sub);
      isGZ = true;
      compressionCodecs = new CompressionCodecFactory(conf);
      final CompressionCodec codec = compressionCodecs.getCodec(p);
      FileSystem fs = new Path(file).getFileSystem(conf);
      FSDataInputStream fileIn = fs.open(p);
      in = new LineReader(codec.createInputStream(fileIn), conf);
      Text t = new Text();
      in.readLine(t);
      StringTokenizer stk = new StringTokenizer(t.toString(), new String(
          new char[] { '\01' }));
      int k = 0;
      while (stk.hasMoreTokens()) {
        String str = stk.nextToken();
        byte b = Byte.valueOf(str);
        IRecord.IFType type = new IRecord.IFType(b, k);
        fieldtypes.put(k, type);
        k++;
      }
      maxLineLength = Integer.MAX_VALUE;
      currentrec = 0;
    } else {
      ifdf = new IFormatDataFile(conf);
      ifdf.open(file);

      ISegmentIndex isi = ifdf.segIndex();
      if (isi.getSegnum() == 0) {
        this.recnum = 0;
      } else {
        long offset = split.getOffset(id);
        long len = split.getLength(id);
        int[] segids = isi.getsigidsinoffsetrange(offset, (int) len);
        System.out.println("fsplit:\toffset:  " + offset + "  len:  " + len
            + "  segids[0]:  " + segids[0] + "  segids[1]:  " + segids[1]);
        if (segids[0] >= 0 && segids[0] < isi.getSegnum()
            && segids[1] <= isi.getSegnum() && segids[1] > segids[0]) {
          int line = isi.getILineIndex(segids[0]).beginline();
          this.beginline = line;
          ifdf.seek(line);
          this.recnum = 0;
          for (int i = segids[0]; i < segids[1]; i++) {
            this.recnum += isi.getILineIndex(i).recnum();
          }
        } else {
          this.recnum = 0;
        }
      }
    }
  }

  public FormatStorageIRecordReader(Configuration conf,
      FormatStorageInputSplit split) throws IOException {
    this.conf = conf;
    String file = split.getPath().toString();
    ifdf = new IFormatDataFile(conf);
    ifdf.open(file);
    if (split.wholefileASasplit) {
      this.recnum = ifdf.segIndex().recnum();
      ifdf.seek(0);
    } else {
      this.recnum = split.recnum;
      ifdf.seek(split.beginline);
    }
  }

  @Override
  public boolean next(LongWritable key, IRecord value) throws IOException {
    if (currentrec >= recnum)
      return false;
    key.set(currentrec);
    if (!isGZ) {
      if (!ifdf.next(value)) {
        String err = "FSIR error read:\t" + this.file + ":\tcurrentrec\t"
            + currentrec + "\trecnum\t" + recnum + "\tbeginline\t"
            + this.beginline + "\r\nvalue" + value.showstr();
        throw new IOException(err);
      }
    } else {
      try {
        int newSize = in.readLine(tValue);
        if (newSize == 0) {
          return false;
        }
      } catch (Exception e) {
        return false;
      }
      StringTokenizer stk = new StringTokenizer(tValue.toString(), new String(
          new char[] { '\01' }));
      int j = 0;
      while (stk.hasMoreTokens()) {
        IRecord.IFType type = this.fieldtypes.get(j);
        String str = stk.nextToken();
        if (!str.equals("\\N")) {
          if (type.type() == ConstVar.FieldType_Byte)
            value.addFieldValue(new IRecord.IFValue(Byte.valueOf(str), j));
          else if (type.type() == ConstVar.FieldType_Short)
            value.addFieldValue(new IRecord.IFValue(Short.valueOf(str), j));
          else if (type.type() == ConstVar.FieldType_Int)
            value.addFieldValue(new IRecord.IFValue(Integer.valueOf(str), j));
          else if (type.type() == ConstVar.FieldType_Long)
            value.addFieldValue(new IRecord.IFValue(Long.valueOf(str), j));
          else if (type.type() == ConstVar.FieldType_Float)
            value.addFieldValue(new IRecord.IFValue(Float.valueOf(str), j));
          else if (type.type() == ConstVar.FieldType_Double)
            value.addFieldValue(new IRecord.IFValue(Double.valueOf(str), j));
          else if (type.type() == ConstVar.FieldType_String) {
            if (str.equals("\\NN")) {
              value.addFieldValue(new IRecord.IFValue("", j));

            } else {
              value.addFieldValue(new IRecord.IFValue(str, j));
            }
          }
        } else {
          value.setNull(j);
        }
        j++;
      }

    }
    currentrec++;
    return true;
  }

  @Override
  public void close() throws IOException {

    if (isGZ) {
      in.close();
    } else {
      if (ifdf != null) {
        ifdf.close();
      }
    }
  }

  @Override
  public LongWritable createKey() {
    return new LongWritable(0);
  }

  @Override
  public IRecord createValue() {
    if (!isGZ)
      return ifdf.getIRecordObj();
    else
      return new IRecord(fieldtypes);
  }

  @Override
  public long getPos() throws IOException {
    return currentrec;
  }

  @Override
  public float getProgress() throws IOException {
    return (float) currentrec / recnum;
  }

  public void reset(IRecord rec) {
    if (isGZ)
      rec.reset(this.fieldtypes);
    else
      rec.reset(ifdf.fileInfo().head().fieldMap().fieldtypes());
  }
}
