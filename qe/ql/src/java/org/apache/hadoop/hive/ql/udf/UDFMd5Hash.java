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

package org.apache.hadoop.hive.ql.udf;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class UDFMd5Hash extends UDF {

  private static  Log l4j = LogFactory.getLog(HiveConf.class);
  private static final String[] hexDigits = { "0", "1", "2", "3", "4", "5",
      "6", "7", "8", "9", "a", "b", "c", "d", "e", "f" };

  public String evaluate(LongWritable in, Text seed) {
    if (in == null || seed == null) {
      return null;
    }
    String toEncrypt = null;
    if(seed.getLength() == 0){
      toEncrypt = in.toString() + seed;
      return getMd5(toEncrypt);
    }
    else{
      byte [] inBytes = in.toString().getBytes();
      byte [] toEncryptB = new byte[inBytes.length + seed.getLength()];
      System.arraycopy(inBytes, 0, toEncryptB, 0,  inBytes.length);
      System.arraycopy(seed.getBytes(), 0, toEncryptB, inBytes.length, seed.getLength());
      
      return getMd5(toEncryptB);
    }    
  }

  public String evaluate(DoubleWritable in, Text seed) {
    if (in == null || seed == null) {
      return null;
    }
    String toEncrypt = null;
    if(seed.getLength() == 0){
      toEncrypt = in.toString() + seed;
      return getMd5(toEncrypt);
    }
    else{
      byte [] inBytes = in.toString().getBytes();
      byte [] toEncryptB = new byte[inBytes.length + seed.getLength()];
      System.arraycopy(inBytes, 0, toEncryptB, 0,  inBytes.length);
      System.arraycopy(seed.getBytes(), 0, toEncryptB, inBytes.length, seed.getLength());
      
      return getMd5(toEncryptB);
    }   
  }

  public String evaluate(IntWritable in, Text seed) {
    if (in == null || seed == null) {
      return null;
    }
    String toEncrypt = null;
    if(seed.getLength() == 0){
      toEncrypt = in.toString() + seed;
      return getMd5(toEncrypt);
    }
    else{
      byte [] inBytes = in.toString().getBytes();
      byte [] toEncryptB = new byte[inBytes.length + seed.getLength()];
      System.arraycopy(inBytes, 0, toEncryptB, 0,  inBytes.length);
      System.arraycopy(seed.getBytes(), 0, toEncryptB, inBytes.length, seed.getLength());
      
      return getMd5(toEncryptB);
    } 
  }

  public String evaluate(Text in, Text seed) {
    if (in == null || seed == null) {
      return null;
    }
    
    byte [] toEncrypt = new byte[in.getLength() + seed.getLength()];
    System.arraycopy(in.getBytes(), 0, toEncrypt, 0,  in.getLength());
    System.arraycopy(seed.getBytes(), 0, toEncrypt, in.getLength(), seed.getLength());
    
    return getMd5(toEncrypt);
  }

  private static String byteArrayToHexString(byte[] b) {
    StringBuffer resultSb = new StringBuffer();
    for (int i = 0; i < b.length; i++) {
      resultSb.append(byteToHexString(b[i]));
    }
    return resultSb.toString();
  }

  private static String byteToHexString(byte b) {
    int n = b;
    if (n < 0)
      n = 256 + n;
    int d1 = n / 16;
    int d2 = n % 16;
    return hexDigits[d1] + hexDigits[d2];
  }

  private static String getMd5(String toEncrypt) {
    try {     
      byte[] results = MessageDigest.getInstance("MD5").digest(
          toEncrypt.getBytes());
      String resultString = byteArrayToHexString(results);
      return resultString;
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
      return null;
    }
  }
  
  private static String getMd5(byte [] toEncrypt) {
    try {      
      byte[] results = MessageDigest.getInstance("MD5").digest(
          toEncrypt);
      String resultString = byteArrayToHexString(results);
      return resultString;
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
      return null;
    }
  }

  public static void main(String[] args) throws NoSuchAlgorithmException {

    UDFMd5Hash md5test = new UDFMd5Hash();
    LongWritable in1 = new LongWritable(12345678);
    String seed = "helloworld";
    DoubleWritable in2 = new DoubleWritable(12345678.0);
    String in3 = "12345678";
    //System.out.println(md5test.evaluate(in1, seed));
    //System.out.println(md5test.evaluate(in2, seed));
    //System.out.println(md5test.evaluate(in3, seed));
  }
}
