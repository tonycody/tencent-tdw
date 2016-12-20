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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import Comm.ConstVar;
import Comm.Util;
import FormatStorage.FieldMap;
import FormatStorage.FormatDataFile;
import FormatStorage.Head;

import FormatStorage.FieldMap.Field;
import FormatStorage.Unit.FieldValue;
import FormatStorage.Unit.Record;


public class FormatStroageStabilityTest 
{
    static class Error
    {
        String msg;
    }
    
	public static void main(String[] argv) 
	{
		if(argv.length != 4)
		{
			System.out.println("usage: FormatStroageStabilityTest cmd[write | readSeq | readRand] Count Var Compress\n");
			return;
		}
		
		String writeCmd = "write";
		String readSeq = "readSeq";
		String readRand = "readRand";
		
		String cmd = argv[0];
		int count = Integer.valueOf(argv[1]);
		boolean var = Boolean.valueOf(argv[2]);
		byte compress = Byte.valueOf(argv[3]);
		
		System.out.println("cmd:"+cmd+",count:"+count+",var:"+var+",compress:"+compress);
		
		if(cmd.equals(writeCmd))
		{		   
		    {
		        doWrite(count, var, compress);
		    }
		}
		else if(cmd.equals(readSeq))
		{		   
		    while(true)
		    {
		        doReadSeq(count,var, compress);
		    }
		}
		else
		{		   
		    while(true)
		    {    		
		        doReadRand(count,var);
		    }
		}
	}
	
	public static void doWrite(int count, boolean var, byte compress)
	{
		try
		{
			long begin = System.currentTimeMillis();
			
			FieldMap fieldMap = new FieldMap();
			fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
			fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
			fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
			fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
			fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
			fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
			
			if(var)
			{
			    fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short)6));
			}
			
			Head head = new Head();
			head.setFieldMap(fieldMap);	
			
			if(compress == 1)
			{
    			head.setCompress((byte) 1);
    			head.setCompressStyle(ConstVar.LZOCompress);
			}
			
			String fileName = "MR_input/testMassRecord";
			if(var)
			{
			    fileName += "_var";
			}
			
			Configuration conf = new Configuration();
			conf.setInt("dfs.replication", 1);
			FormatDataFile fd = new FormatDataFile(conf);
			
			fd.create(fileName, head);
			
			File file1 = new File("testWriteMassRecord.result");
			FileOutputStream out1 = new FileOutputStream(file1);
			
			short fieldNum = 6;
			if(var)
			{
			    fieldNum = 7;
			}
			
			for(int i = 0; i < count; i++)
			{			    
				Record record = new Record(fieldNum);
				
				record.addValue(new FieldValue((byte)(1+i), (short)0));
				record.addValue(new FieldValue((short)(2+i), (short)1));
				record.addValue(new FieldValue((int)(3+i), (short)2));
				record.addValue(new FieldValue((long)(4+i), (short)3));
				record.addValue(new FieldValue((float)(5.5+i), (short)4));
				record.addValue(new FieldValue((double)(6.6+i), (short)5));
				
				/*
				record.addValue(new FieldValue((byte)(1), (short)0));
                record.addValue(new FieldValue((short)(2), (short)1));
                record.addValue(new FieldValue((int)(3), (short)2));
                record.addValue(new FieldValue((long)(4), (short)3));
                record.addValue(new FieldValue((float)(5.5), (short)4));
                record.addValue(new FieldValue((double)(6.6), (short)5));
                */
				if(var)
				{
				    record.addValue(new FieldValue("hello konten"+i, (short)6));
				}
				
				fd.addRecord(record);
				
				if(i % (1000*10000) == 0)
				{
					String string = "write " + i +" record, delay:" + ((System.currentTimeMillis() - begin) / 1000) + " s . file size:"+ fd.getFileLen() +"\n" ;
					out1.write(string.getBytes());
				}
			}
			
			fd.close();
			
			long end = System.currentTimeMillis();
			
			
			String string = "write " + count + " record over, delay: " + ((end - begin)/1000) + " s . file size:"+ fd.getFileLen() +"\n" ;
			out1.write(string.getBytes());
			out1.close();
		}
		catch(IOException e)
		{
			e.printStackTrace();
			System.out.println("get IOException:"+e.getMessage());				
		}		
		catch(Exception e)
		{
			e.printStackTrace();
			System.out.println("get exception:"+e.getMessage());
		}
	}
	
	static void doReadRand(int count, boolean var)
	{
		try 
		{		    
		    String fileName = "MR_input/testMassRecord";
		    if(var)
		    {
		        fileName += "_var";
		    }
			
		    Configuration conf = new Configuration();
			FormatDataFile fd = new FormatDataFile(conf);
			fd.setOptimize(true);
			fd.open(fileName);
			
			File file = new File("testReadMassRecord.result");
			FileOutputStream out = new FileOutputStream(file);
	        
			long begin = System.currentTimeMillis();
			
			int totalCount = 100000000;
			Record valueRecord = new Record();
            for(int i = 0; i < count; i++)
			{
				int rand = (int)(Math.random() * totalCount);
				
				
				Record record = fd.getRecordByLine(rand, valueRecord);
				if(record == null)
				{
					String string = "record no:"+rand+" return null";
					out.write(string.getBytes());
					
					continue;
				}
				
				Error error = new Error();
				/*if(!judgeRecord(record, i, error))
				{
					String string = "record no:"+rand + " value error:"+error.msg+"\n";
					out.write(string.getBytes());
				}*/
				
				if(i % (10*10000) == 0)
                {
                    String string = "read rand " + i +" record, delay:" + ((System.currentTimeMillis() - begin) / 1000) + " s \n" ;
                    out.write(string.getBytes());
                }
			}
            long end = System.currentTimeMillis();
            String string = "Read Rand over, count:"+count+",totalCount:"+totalCount+", delay:"+(long)((end - begin)/1000) + " s";
            out.write(string.getBytes());
            
            out.close();
            System.out.println(string);
		}
		catch(IOException e)
		{
			e.printStackTrace();
			System.out.println("get IOException:"+e.getMessage());				
		}		
		catch(Exception e)
		{
			e.printStackTrace();
			System.out.println("get exception:"+e.getMessage());
		}
	}
	
	static void doReadSeq(int count, boolean var, byte compress)
	{
	    int i = 0;
		try
        {
		    String fileName = "MR_input/testMassRecord";
		    if(var)
            {
		        fileName += "_var";
            }
		    
		    Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);
            fd.setOptimize(true);
            fd.open(fileName);

            File file = new File("testReadMassRecord.result");
            FileOutputStream out = new FileOutputStream(file);
   
            long begin = System.currentTimeMillis();
            
            Record record = new Record();
            for(i = 0; i < count; i++)
            {
                if(compress == 1)
                {
                    record = fd.getNextRecord(record);
                }
                else
                {
                    record = fd.getRecordByLine(i, record);
                }
                
                if (record == null)
                {
                    String string = "record no:" + i + " return null";
                    out.write(string.getBytes());
                }
                
                /*
                Error error = new Error();
                if (!judgeRecord(record, i, error))
                {
                    String string = "record no:" + i + " value error:"+error.msg +"\n";
                    out.write(string.getBytes());
                }
                */
               
                if(i % (10*10000) == 0)
                {
                    String string = "read seq " + i +" record, delay:" + ((System.currentTimeMillis() - begin) / 1000) + " s \n" ;
                    out.write(string.getBytes());
                }
            }
            
            long end = System.currentTimeMillis();
            String string = "Read Seq over, count:"+count+", delay:"+(long)((end - begin)/1000) + " s";
            out.write(string.getBytes());
            
            System.out.println(string);
            out.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
            System.out.println("get IOException:" + e.getMessage()+",i:"+i);
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.out.println("get exception:" + e.getMessage()+",i:"+i);
        }
		
	}
	static boolean judgeRecord(Record record, int line, Error error) throws IOException
	{		
		int index = 0;
		byte[] buf = record.fieldValues().get(index).value(); 
		if(buf == null)
		{
		    error.msg = "buf = null, index = 0";
			return false;			
		}
		if(buf[0] != (byte)(1+line))
		{
		    error.msg = "buf[0] != 1"+"buf[0]:"+buf[0];
			return false;	
		}							
		byte type = record.fieldValues().get(index).type();
		int len = record.fieldValues().get(index).len();
		short idx = record.fieldValues().get(index).idx();
		if(len != ConstVar.Sizeof_Byte)
		{
		    error.msg = "error len:"+len+",index 0";
			return false;	
		}
		if(type != ConstVar.FieldType_Byte)
		{
		    error.msg = "error type:"+type+",index 0";
			return false;	
		}
		if(idx != 0)
		{
		    error.msg = "error idx:"+idx+",index 0";
			return false;	
		}
		
		
		index = 1;
		buf = record.fieldValues().get(index).value(); 
		if(buf == null)
		{
		    error.msg = "index 1, buf = null";
			return false;	
		}
		short sval = Util.bytes2short(buf, 0, ConstVar.Sizeof_Short);
		if(sval != (short)(2+line))
		{
		    error.msg = "2, sval:"+sval;
			return false;	
		}								
		type = record.fieldValues().get(index).type();
		len = record.fieldValues().get(index).len();
		idx = record.fieldValues().get(index).idx();
		if(len != ConstVar.Sizeof_Short)
		{
		    error.msg = "3, len:"+len;
			return false;	
		}
		if(type != ConstVar.FieldType_Short)
		{
		    error.msg = "4, type:"+type;
			return false;	
		}
		if(idx != 1)
		{
		    error.msg = "5, idx:"+idx;
			return false;	
		}
		
		index = 2;
		buf = record.fieldValues().get(index).value(); 
		if(buf == null)
		{
		    error.msg = "6, buf = null";
			return false;	
		}
		int ival = Util.bytes2int(buf, 0, ConstVar.Sizeof_Int);
		if(ival != (3+line))
		{
		    error.msg = "7, ival:"+ival;
			return false;	
		}								
		type = record.fieldValues().get(index).type();
		len = record.fieldValues().get(index).len();
		idx = record.fieldValues().get(index).idx();
		if(len != ConstVar.Sizeof_Int)
		{
		    error.msg = "8, len:"+len;
			return false;	
		}
		if(type != ConstVar.FieldType_Int)
		{
		    error.msg = "9, type:"+type;
			return false;	
		}
		if(idx != 2)
		{
		    error.msg = "10, idx:"+idx;
			return false;	
		}
		
		index = 3;
		buf = record.fieldValues().get(index).value(); 
		if(buf == null)
		{
		    error.msg = "11, buf = null";
			return false;	
		}
		long lval = Util.bytes2long(buf, 0, ConstVar.Sizeof_Long);
		if(lval != (4+line))
		{
		    error.msg = "12, lval:"+lval;
			return false;	
		}								
		type = record.fieldValues().get(index).type();
		len = record.fieldValues().get(index).len();
		idx = record.fieldValues().get(index).idx();
		if(len != ConstVar.Sizeof_Long)
		{
		    error.msg = "13, len:"+len;
			return false;	
		}
		if(type != ConstVar.FieldType_Long)
		{
		    error.msg = "14, type:"+type;
			return false;	
		}
		if(idx != 3)
		{
		    error.msg = "15, idx:"+idx;
			return false;	
		}
		
		index = 4;
		buf = record.fieldValues().get(index).value(); 
		if(buf == null)
		{
		    error.msg = "16, buf = null";
			return false;	
		}
		float fval = Util.bytes2float(buf, 0);
		if(fval != (float)(5.5+line))
		{
		    error.msg = "17, fval:"+fval;
			return false;	
		}								
		type = record.fieldValues().get(index).type();
		len = record.fieldValues().get(index).len();
		idx = record.fieldValues().get(index).idx();
		if(len != ConstVar.Sizeof_Float)
		{
		    error.msg = "18, len:"+len;
			return false;	
		}
		if(type != ConstVar.FieldType_Float)
		{
		    error.msg = "19,type:"+type;
			return false;	
		}
		if(idx != 4)
		{
		    error.msg = "20,idx:"+idx;
			return false;	
		}
		
		index = 5;
		buf = record.fieldValues().get(index).value(); 
		if(buf == null)
		{
		    error.msg = "21,buf null";
			return false;	
		}
		double dval = Util.bytes2double(buf, 0);
		if(dval != (double)(6.6+line))
		{
		    error.msg = "22, dval:"+dval;
			return false;	
		}								
		type = record.fieldValues().get(index).type();
		len = record.fieldValues().get(index).len();
		idx = record.fieldValues().get(index).idx();
		if(len != ConstVar.Sizeof_Double)
		{
		    error.msg = "23, len:"+len;
			return false;	
		}
		if(type != ConstVar.FieldType_Double)
		{
		    error.msg = "24,type:"+type;
			return false;	
		}
		if(idx != 5)
		{
		    error.msg = "25,idx:"+idx;
			return false;	
		}
		
		if(record.fieldNum() == 7)
		{
    		index = 6;
    		buf = record.fieldValues().get(index).value(); 
    		if(buf == null)
    		{
    		    error.msg = "26,buf null";
    			return false;	
    		}				
    		
    		type = record.fieldValues().get(index).type();
    		len = record.fieldValues().get(index).len();
    		idx = record.fieldValues().get(index).idx(); 
    		String str = new String(buf, 0, len);
    		if(len != 12)
    		{
    		    error.msg = "28,len:"+len;
    			return false;	
    		}
    		
            if(!str.equals(new String("hello konten"+line)))
            {
                error.msg = "27,str:"+str;
                return false;   
            }
    		if(type != ConstVar.FieldType_String)
    		{
    		    error.msg = "29,type:"+type;
    			return false;	
    		}
    		if(idx != 6)
    		{
    		    error.msg = "30,idx:"+idx;
    			return false;	
    		}
		}
		return true;
	}
}
