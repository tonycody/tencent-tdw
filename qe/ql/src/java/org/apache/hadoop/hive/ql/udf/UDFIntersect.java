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

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class UDFIntersect extends UDF{
  
  IntWritable ret = new IntWritable();
  public UDFIntersect(){
    
  }

  public IntWritable evaluate(Text leftStr, Text rightStr){
    if(leftStr == null || rightStr == null){
      ret.set(0);
      return ret;
    }
    
    int retValue = 0;
    
    String [] leftArray = leftStr.toString().split(",");
    String [] rightArray = rightStr.toString().split(",");
    long lv = 0;
    long rv = 0;
    int pos = 0;
    
    for(int i = 0 ; i < leftArray.length; i++){
      for(; pos < rightArray.length; ){
        lv = Long.valueOf(leftArray[i]);
        rv = Long.valueOf(rightArray[pos]);
        
        if(lv == rv){
          retValue++;
          pos++;
          break;
        }
        else if(lv > rv){
          pos++;
        }
        else{
          break;
        }
      }
      
      if(pos >= rightArray.length){
        break;
      }
    }
    
    ret.set(retValue);
    
    return ret;
  }
}
