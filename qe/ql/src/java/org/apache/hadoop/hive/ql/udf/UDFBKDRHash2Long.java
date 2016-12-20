package org.apache.hadoop.hive.ql.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class UDFBKDRHash2Long extends UDF {
  private LongWritable result = new LongWritable();
  private String str;
  
  public LongWritable evaluate(Text text) {
    if(text == null) {
      result.set(0);
    } else {
      str = text.toString();
      result.set(BKDRHash(str));
    }
	return result;
  }
  
  private long BKDRHash(String str)
  {
     long seed = 31; // 31 131 1313 13131 131313 etc..
     long hash = 0;

     for(int i = 0; i < str.length(); i++)
     {
        hash = (hash * seed) + str.charAt(i);
     }

     return hash;
  }

}
