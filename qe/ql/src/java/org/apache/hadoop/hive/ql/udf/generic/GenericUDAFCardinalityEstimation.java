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
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCount.GenericUDAFCountEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ByteArrayObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

public class GenericUDAFCardinalityEstimation implements GenericUDAFResolver2{
  public static int log2OfBucketSize = 16;
  public static int bucketSize = (int) Math.pow(2, log2OfBucketSize);
  public static double alphaMM = (0.7213 / (1 + 1.079 / bucketSize)) * bucketSize * bucketSize;
  private static final Log LOG = LogFactory.getLog("org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCardinalityEstimation_New");
  public static int log2OfbuffSize = 8;
  public static int buffSize = (int) Math.pow(2, log2OfbuffSize);;
  public static int buffNum = bucketSize / buffSize;
  
  public static long totalEstDistBlockNum = 0;
  
  public static void initParams(int bucketSizeLog, int buffSizeLog){
    log2OfBucketSize = bucketSizeLog;
    bucketSize = (int) Math.pow(2, log2OfBucketSize);
    alphaMM = (0.7213 / (1 + 1.079 / bucketSize)) * bucketSize * bucketSize;
    log2OfbuffSize = buffSizeLog;
    buffSize = (int) Math.pow(2, log2OfbuffSize);
    buffNum = bucketSize / buffSize;
  }
  
  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
      throws SemanticException 
  {
    // TODO Auto-generated method stub
    return new GenericUDAFCountEvaluator();
  }

  @Override
  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo paramInfo)
      throws SemanticException 
  {
    // TODO Auto-generated method stub
      TypeInfo[] parameters = paramInfo.getParameters();

      if (parameters.length == 0) {
        if (!paramInfo.isAllColumns()) {
          throw new UDFArgumentException("Argument expected");
        }
        assert !paramInfo.isDistinct() : "DISTINCT not supported with *";
      } else {
        if (parameters.length > 1 && !paramInfo.isDistinct()) {
          throw new UDFArgumentException("DISTINCT keyword must be specified");
        }
        assert !paramInfo.isAllColumns() : "* not supported in expression list";
      }

      return new GenericUDAFCardinalityEstimationEvaluator().setCountAllColumns(
          paramInfo.isAllColumns());
  }
  
  public static class CardinalityEstimationAgg implements AggregationBuffer 
  {
    byte[][] bufferArray = new byte[buffNum][];
    int filledNum = 0;
  }
    
  public static class GenericUDAFCardinalityEstimationEvaluator extends GenericUDAFEvaluator
  {
      private ByteArrayObjectInspector bufferElementOI;
      private StandardListObjectInspector bufferListOI;
      LongWritable result; 
      private boolean countAllColumns = false;
      
      @Override
      public ObjectInspector init(Mode m, ObjectInspector[] parameters)throws HiveException 
      {       
        super.init(m, parameters);        
        //bufferElementOI = PrimitiveObjectInspectorFactory.writableByteObjectInspector;        
        result = new LongWritable(0); 
        //bufferListOI = ObjectInspectorFactory.getStandardByteArrayObjectInspector();
        
        bufferElementOI = ObjectInspectorFactory.getStandardByteArrayObjectInspector();
        bufferListOI = ObjectInspectorFactory.getStandardListObjectInspector(bufferElementOI);
        switch(m)
        {
        case PARTIAL1:
          return bufferListOI;
        case COMPLETE:
          return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        case FINAL:
          return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        case PARTIAL2:
          return bufferListOI;
        }
        return bufferListOI;//PrimitiveObjectInspectorFactory.writableLongObjectInspector;
      }
      
      private GenericUDAFCardinalityEstimationEvaluator setCountAllColumns(boolean countAllCols) 
      {
          countAllColumns = countAllCols;
          return this;        
      }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException 
    {
      // TODO Auto-generated method stub          
      CardinalityEstimationAgg result = new CardinalityEstimationAgg();         
      reset(result);          
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException 
    {
      // TODO Auto-generated method stub
      //((CardinalityEstimationAgg) agg).bufferArray = null;
      for(int i = 0 ; i < buffNum ; i++){
        ((CardinalityEstimationAgg) agg).bufferArray[i] = null;
      }
      
      ((CardinalityEstimationAgg) agg).filledNum = 0;
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters)
        throws HiveException 
    {
      // TODO Auto-generated method stub          
      if (parameters == null) 
      {             
        return;           
      }
              
      assert parameters.length == 1;              
      boolean countThisRow = true;
      
      countThisRow = parameters[0] != null ? true : false;
             
      if (countThisRow)
      { 
        long hashedValue = MurmurHash.hash64(parameters[0].toString());
        final int p = (int) (hashedValue >>> (Long.SIZE - log2OfBucketSize));
        final byte v = (byte) (Long
            .numberOfLeadingZeros((hashedValue << log2OfBucketSize)
                | (1 << (log2OfBucketSize - 1)) + 1) + 1);
        
        int x = p / buffSize;
        int y = p % buffSize;
        
        if(((CardinalityEstimationAgg) agg).bufferArray[x] == null){
          ((CardinalityEstimationAgg) agg).bufferArray[x] = new byte[buffSize];
          ((CardinalityEstimationAgg) agg).filledNum++;
          totalEstDistBlockNum++;
        }
        
        if(((CardinalityEstimationAgg) agg).bufferArray[x][y] < v){
          ((CardinalityEstimationAgg) agg).bufferArray[x][y] = v;
        }         
      }           
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg)
        throws HiveException 
    {
      totalEstDistBlockNum -= ((CardinalityEstimationAgg) agg).filledNum;
      ((CardinalityEstimationAgg) agg).filledNum = 0;
      return ((CardinalityEstimationAgg) agg).bufferArray;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial)
        throws HiveException 
    {
      // TODO Auto-generated method stub         
      
      if (partial != null) 
      {            
        byte [] p = null;  
        byte [] aggElem = null;
                        
        for(int i = 0; i < buffNum; i++)           
        {  
          p = bufferElementOI.getBytes(bufferListOI.getListElement(partial, i)); 
          aggElem = ((CardinalityEstimationAgg) agg).bufferArray[i];
          
          if(p == null && aggElem == null){
            continue;
          }
          else if (aggElem == null && p != null){
            ((CardinalityEstimationAgg) agg).bufferArray[i] = p;
          }
          else if(aggElem != null && p != null){
            for(int j = 0 ; j < buffSize; j++){
              if(p[j] > aggElem[j]){
                aggElem[j] = p[j];
              }
            }
          }
        }         
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException 
    {      
      long res = 0;
      double registerSum = 0;
      double zeros = 0.0;

      for(int i = 0 ; i < buffNum; i++){
        if(((CardinalityEstimationAgg) agg).bufferArray[i] == null){
          registerSum += buffSize * 1.0;
          zeros += buffSize;
        }
        else{
          for(int j = 0 ; j < buffSize; j++){
            registerSum += 1.0 / (1 << (((CardinalityEstimationAgg) agg).bufferArray[i][j]));
            
            if ((((CardinalityEstimationAgg) agg).bufferArray[i][j]) == (byte) 0) 
            {
              zeros++;
            }
          }
        }
      }
      
      double estimate = alphaMM * (1 / registerSum);

      if (estimate <= (5.0 / 2.0) * bucketSize) 
      { 
        // Small Range Estimate
        res = Math.round(bucketSize * Math.log(bucketSize / zeros));
      } 
      else 
      {
        res = Math.round(estimate);
      }
      
      result.set(res);
      return result;
    }
    
  }
}
