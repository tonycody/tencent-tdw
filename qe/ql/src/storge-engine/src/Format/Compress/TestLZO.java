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
package Compress;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;

import Comm.Util;

import com.hadoop.compression.lzo.LzoCodec;

public class TestLZO {
  private static char[] numbersAndLetters = ("0123456789abcdefghijklmnopqrstuvwxyz"
      + "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ").toCharArray();

  private static Random randGen = new Random();

  public static void main(String[] argv) throws IOException {
    System.out.println(System.getProperty("java.library.path"));

    Configuration conf = new Configuration();

    conf.setInt("io.compression.codec.lzo.buffersize", 64 * 1024);

    LzoCodec codec = new LzoCodec();
    codec.setConf(conf);

    OutputStream out = new DataOutputBuffer();
    CompressionOutputStream out2 = codec.createOutputStream(out);

    byte[] str2 = new byte[20];

    int num = 10000;
    int before = 0;
    String part = "hello konten hello konten";
    for (long i = 0; i < num; i++) {
      Util.long2bytes(str2, i);
      out2.write(str2, 0, 8);

    }
    out2.finish();

    byte[] buffer = ((DataOutputBuffer) out).getData();

    System.out.println("org len:" + num * 8 + ", compressed len:"
        + ((DataOutputBuffer) out).getLength());

    InputStream in = new DataInputBuffer();
    ((DataInputBuffer) in).reset(((DataOutputBuffer) out).getData(), 0,
        ((DataOutputBuffer) out).getLength());

    CompressionInputStream in2 = codec.createInputStream(in);

    byte[] buf = new byte[100];
    for (long i = 0; i < num; i++) {
      int count = 0;
      count = in2.read(buf, 0, 8);
      if (count > 0) {
        long value = Util.bytes2long(buf, 0, 8);
        if (value != i) {
          System.out.println(i + ",count:" + count + ",value:" + value);
        } else if (i > (num - 20)) {
          System.out.println(i + ",value:" + value);
        }

      } else {
        System.out.println("count:" + count + ", string " + i);
        break;
      }
    }

    in2.close();

    System.out.println("test compress array...");

    OutputStream out3 = new DataOutputBuffer();
    CompressionOutputStream out4 = codec.createOutputStream(out3);

    DataOutputBuffer tout3 = new DataOutputBuffer();

    for (long i = 0; i < num; i++) {
      Util.long2bytes(str2, i);
      out4.write(str2, 0, 8);
    }
    out4.finish();

    buffer = ((DataOutputBuffer) out3).getData();

    System.out.println("org len:" + num * 8 + ", compressed len:"
        + ((DataOutputBuffer) out3).getLength());

    InputStream in3 = new DataInputBuffer();
    ((DataInputBuffer) in3).reset(((DataOutputBuffer) out3).getData(), 0,
        ((DataOutputBuffer) out3).getLength());

    CompressionInputStream in4 = codec.createInputStream(in3);

    for (long i = 0; i < num; i++) {
      int count = 0;
      count = in4.read(buf, 0, 8);
      if (count > 0) {
        long value = Util.bytes2long(buf, 0, 8);
        if (value != i) {
          System.out.println(i + ",count:" + count + ",value:" + value);
        }

        if (i > (num - 20)) {
          System.out.println(i + ",value:" + value);
        }

      } else {
        System.out.println("count:" + count + ", string " + i);
        break;
      }
    }

    in2.close();

  }

  static int rawReadInt(InputStream in) throws IOException {
    int b1 = in.read();
    int b2 = in.read();
    int b3 = in.read();
    int b4 = in.read();
    if ((b1 | b2 | b3 | b4) < 0)
      throw new EOFException();
    return ((b1 << 24) + (b2 << 16) + (b3 << 8) + (b4 << 0));
  }

  public static String randomString(int length) {

    if (length < 1) {
      return null;
    }
    char[] randBuffer = new char[length];
    for (int i = 0; i < randBuffer.length; i++) {
      randBuffer[i] = numbersAndLetters[randGen.nextInt(71)];
    }
    return new String(randBuffer);
  }
}
