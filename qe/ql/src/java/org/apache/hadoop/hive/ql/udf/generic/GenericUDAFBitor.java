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
package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableStringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;


public class GenericUDAFBitor implements GenericUDAFResolver2{
  
  public static int m = 1;
  public static int max = 1000;
  private static final Log LOG = LogFactory.getLog("org.apache.hadoop.hive.ql.udf.generic.GenericUDAFBitor");
  
  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException {
    // TODO Auto-generated method stub
    return new GenericUDAFBitorEvaluator();
  }

  @Override
  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo paramInfo)
      throws SemanticException {
    // TODO Auto-generated method stub

    TypeInfo[] parameters = paramInfo.getParameters();
    
    //LOG.info("##################is distinct " + paramInfo.isDistinct());
    //LOG.info("##################is all columns " + paramInfo.isAllColumns());
    
    //LOG.info("##################parameters len is " + parameters);
    //for(int i = 0; i < parameters.length; i++)
    //{
    //  LOG.info("##################parameters index = " + i);
    //  LOG.info("##################parameters type name = " + parameters[i].getTypeName());
    //}
    
    if(paramInfo.isAllColumns()){
      throw new UDFArgumentException("bitor does not support '*' parameter ");
    }

    if (parameters.length != 1) {
      throw new UDFArgumentException("Argument expected:1 String");
    } else {
      if(!"string".equalsIgnoreCase(parameters[0].getTypeName())){
        throw new UDFArgumentException("bitor only support 1 string parameter ");
      }
    }

    return new GenericUDAFBitorEvaluator();
  }

  public static class GenericUDAFBitorEvaluator extends GenericUDAFEvaluator {
    private WritableStringObjectInspector bufferElementOI;
    //private StandardListObjectInspector bufferListOI;
    
    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
        throws HiveException {
      super.init(m, parameters);
      bufferElementOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
      return bufferElementOI;
    }
    
    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      BitorAgg result = new BitorAgg();
      reset(result);
      return result;
    }
    
    static class BitorAgg implements AggregationBuffer {
      public byte [] bitArray = new byte[m];
      public boolean isAllNull = true;
      public boolean isAllEmpty = true;
    }
    
    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
      int size = ((BitorAgg)agg).bitArray.length;
      ((BitorAgg)agg).isAllNull = true;
      ((BitorAgg)agg).isAllEmpty = true;
      if(size > m){
        byte [] newBuffer = new byte[m];
        for(int i = 0; i < m; i++){
          newBuffer[i] = '0';
        }
        ((BitorAgg)agg).bitArray = newBuffer;
      }
      else{
        for(int i = 0; i < size; i++){
          ((BitorAgg)agg).bitArray[i] = '0';
        }
      }
    }
    
    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
        throws HiveException {
      if (parameters == null) {
        return;
      }
    
      assert parameters.length == 1;
      boolean countThisRow = true;
    
      countThisRow = parameters[0] != null ? true : false;
    
      if (countThisRow) {
        ((BitorAgg)agg).isAllNull = false;
        byte [] item = (parameters[0].toString()).getBytes();
        int size = item.length;
        if(size > 0){
          ((BitorAgg)agg).isAllEmpty = false;
        }
        int oldSize = ((BitorAgg)agg).bitArray.length;
        int newSize = oldSize;

        if(oldSize < size && oldSize < max){
          newSize = Math.min(size, max);
          LOG.info("#######resize buffer, from " + oldSize + " to " + newSize);
          System.out.println("#######resize buffer, from " + oldSize + " to " + newSize);
          byte [] newBuffer = new byte[newSize];           
          System.arraycopy(((BitorAgg)agg).bitArray, 0, newBuffer, 0, oldSize);
          for(int i = oldSize; i < newSize; i++){
            newBuffer[i] = '0';
          }
          ((BitorAgg)agg).bitArray = newBuffer;   

        }
        
        //LOG.info("#######size = " + size);
        //LOG.info("#######newSize = " + newSize);
        //LOG.info("#######((BitorAgg)agg).bitArray size = " + ((BitorAgg)agg).bitArray.length);
        
        size = Math.min(size, newSize);
        
        for(int i = 0; i < size; i++){
           if(((BitorAgg)agg).bitArray[i] == '0' && item[i] != '0' ){
             ((BitorAgg)agg).bitArray[i] = '1';
           }
        }
      }
    }
    
    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      if(((BitorAgg)agg).isAllNull)
        return null;
      else if(((BitorAgg)agg).isAllEmpty)
        return new Text("");
      else
        return new Text(((BitorAgg)agg).bitArray);
    }
    
    @Override
    public void merge(AggregationBuffer agg, Object partial)
        throws HiveException {
      if (partial != null) {
        ((BitorAgg)agg).isAllNull = false;
        byte [] item = (partial.toString()).getBytes();
        int size = item.length;
        
        if(size > 0){
          ((BitorAgg)agg).isAllEmpty = false;
        }
        
        int oldSize = ((BitorAgg)agg).bitArray.length;
        int newSize = oldSize;
        
        if(oldSize < size && oldSize < max){
          newSize = Math.min(size, max);
          LOG.info("#######resize buffer, from " + oldSize + " to " + newSize);
          System.out.println("#######resize buffer, from " + oldSize + " to " + newSize);
          byte [] newBuffer = new byte[newSize];           
          System.arraycopy(((BitorAgg)agg).bitArray, 0, newBuffer, 0, oldSize);
          for(int i = oldSize; i < newSize; i++){
            newBuffer[i] = '0';
          }
          ((BitorAgg)agg).bitArray = newBuffer;  
          
          //size = newSize;
        }
       
        size = Math.min(size, newSize);
        
        for(int i = 0; i < size; i++){
           if(((BitorAgg)agg).bitArray[i] == '0' && item[i] != '0' ){
             ((BitorAgg)agg).bitArray[i] = '1';
           }
        }
      }
    }
    
    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      if(((BitorAgg)agg).isAllNull)
        return null;
      else if(((BitorAgg)agg).isAllEmpty)
        return new Text("");
      else
        return new Text(((BitorAgg)agg).bitArray);
    }
  
  }
  
}

