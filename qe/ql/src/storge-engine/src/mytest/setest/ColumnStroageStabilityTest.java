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
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import ColumnStorage.ColumnStorageClient;
import Comm.ConstVar;
import Comm.Util;
import FormatStorage.FieldMap;
import FormatStorage.FormatDataFile;
import FormatStorage.Head;
import FormatStorage.FieldMap.Field;
import FormatStorage.Unit.FieldValue;
import FormatStorage.Unit.Record;

public class ColumnStroageStabilityTest
{
    static class Error
    {
        String msg;
    }
    
    static String prefix = "se_test/cs/stab/"; 
    
    static String fullPrefix = "/user/tdwadmin/se_test/cs/stab/";
    static String byteFileName = fullPrefix + "Column_Byte";
    static String shortFileName = fullPrefix + "Column_Short";
    static String intFileName = fullPrefix + "Column_Int";
    static String longFileName = fullPrefix + "Column_Long";
    static String floatFileName = fullPrefix + "Column_Float";
    static String doubleFileName = fullPrefix + "Column_Double";
    static String stringFileName = fullPrefix + "Column_String";
    static String multiFileNameString = fullPrefix + "Column_Short_Short_Short";
    
    static int type = 1;
    static String[] idxs = null;
    public static void main(String[] argv) throws Exception 
    {
		if(argv.length != 3)
		{
			System.out.println("usage: ColumnStroageStabilityTest cmd[write | readSeq | readRand] Count Idxs\n");
			return;
		}
            
        String writeCmd = "write";
        String readSeq = "readSeq";
        String readRand = "readRand";
        String cmd = argv[0];  
        
		int count = Integer.valueOf(argv[1]);
		idxs = argv[2].split(",");
		
		System.out.println("cmd:"+cmd+",count:"+count+",idxs:"+argv[2]);
		
		if(cmd.equals(writeCmd))
		{		   
			doWrite(count);
		}
		else if(cmd.equals(readSeq))
		{		   
		    while(true)
		    {
		        doReadSeq(count);
		    }
		}
		else
		{		   
		    while(true)
		    {    		
		        doReadRand(count);
		    }
		}
    }
    
    public static void doWrite(int count) throws Exception
    {
        Configuration conf = new Configuration();
        
        System.out.println("write byte file:"+byteFileName);
        FieldMap byteFieldMap = new FieldMap();
        byteFieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
        
        Head byteHead = new Head();
        byteHead.setFieldMap(byteFieldMap);  
        
        FormatDataFile byteFD = new FormatDataFile(conf);
        byteFD.create(byteFileName, byteHead);   
        
        System.out.println("write short file:" + shortFileName);
        FieldMap shortFieldMap = new FieldMap();
        shortFieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
        
        Head shortHead = new Head();
        shortHead.setFieldMap(shortFieldMap);  
        
        FormatDataFile shortFD = new FormatDataFile(conf);
        shortFD.create(shortFileName, shortHead);  
        
        System.out.println("write int file:" + intFileName);
        FieldMap intFieldMap = new FieldMap();
        intFieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
        
        Head intHead = new Head();
        intHead.setFieldMap(intFieldMap);  
        
        FormatDataFile intFD = new FormatDataFile(conf);
        intFD.create(intFileName, intHead);   
        
        System.out.println("write long file:" + longFileName);
        FieldMap longFieldMap = new FieldMap();
        longFieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
        
        Head longHead = new Head();
        longHead.setFieldMap(longFieldMap);  
        
        FormatDataFile longFD = new FormatDataFile(conf);
        longFD.create(longFileName, longHead);   
        
        System.out.println("write float file:" + floatFileName);
        FieldMap floatFieldMap = new FieldMap();
        floatFieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
        
        Head floatHead = new Head();
        floatHead.setFieldMap(floatFieldMap);  
        
        FormatDataFile floatFD = new FormatDataFile(conf);
        floatFD.create(floatFileName, floatHead);   
        
        System.out.println("write double file:" + doubleFileName);
        FieldMap doubleFieldMap = new FieldMap();
        doubleFieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
        
        Head doubleHead = new Head();
        doubleHead.setFieldMap(doubleFieldMap);  
        
        FormatDataFile doubleFD = new FormatDataFile(conf);
        doubleFD.create(doubleFileName, doubleHead);   
        
        System.out.println("write int file:" + stringFileName);
        FieldMap strFieldMap = new FieldMap();
        strFieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short)6));
        
        Head strHead = new Head();
        strHead.setFieldMap(strFieldMap);  
        
        FormatDataFile strFD = new FormatDataFile(conf);
        strFD.create(stringFileName, strHead);   
        
        System.out.println("write int file:" + multiFileNameString);
        FieldMap multiFieldMap = new FieldMap();
        multiFieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)7));
        multiFieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)8));
        multiFieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)9));
        
        Head multiHead = new Head();
        multiHead.setFieldMap(multiFieldMap);  
        
        FormatDataFile multiFD = new FormatDataFile(conf);
        multiFD.create(multiFileNameString, multiHead);   
        
        
        long begin = System.currentTimeMillis();
        for(int i = 0; i < count; i++)
        {            
            createProjectByte(byteFD);
            createProjectShort(shortFD);
            createProjectInt(intFD);
            createProjectLong(longFD);
            createProjectFloat(floatFD);
            createProjectDouble(doubleFD);
            
            
            if(i % 1000000 == 0)
            {
                long end = System.currentTimeMillis();
                System.out.println(i + " record ok, delay:"+(end - begin) / 1000 + " s");
            }
              
        }
        
        byteFD.close();
        shortFD.close();
        intFD.close();
        longFD.close();
        floatFD.close();
        doubleFD.close();
        strFD.close();
        multiFD.close();
        
        long end = System.currentTimeMillis();
        System.out.println(count + " record over(column), delay:"+(end - begin) / 1000 + " s");
       
    }
    
    static void doReadRand(int count)
    {
        try
        {
            
            Path path = new Path(fullPrefix);
            ArrayList<Short> vector = new ArrayList<Short>(10);
            vector.add((short)3);
            vector.add((short)6);
            vector.add((short)4);
            vector.add((short)2);
            vector.add((short)0);
            vector.add((short)5);
            vector.add((short)1);
            
            Configuration conf = new Configuration();
            ColumnStorageClient client = new ColumnStorageClient(path, vector, conf);
            long begin = System.currentTimeMillis();
            
            for(int i = 0; i < count; i++)
            {
                int rand = (int)(Math.random() * count);
                Record record = client.getRecordByLine(rand);
               /*
                if (record == null)
                {
                    String string = "record no:" + i + " return null";
                    out.write(string.getBytes());
                }
                

                if (!judgeRecord(record))
                {
                    String string = "record no:" + i + " value error";
                    out.write(string.getBytes());
                }
                */
                if(i % (100*10000) == 0)
                {
                    String string = "read seq " + i +" record, delay:" + ((System.currentTimeMillis() - begin) / 1000) + " s \n" ;
                    System.out.println(string);
                }
            }
            
            long end = System.currentTimeMillis();
            String string = "Read Seq over, count:"+count+", delay:"+(long)((end - begin)/1000) + " s";
            System.out.println(string);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            System.out.println("get IOException:" + e.getMessage());
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.out.println("get exception:" + e.getMessage());
        }
        
    }
    
    static void doReadSeq(int count)
    {
        try
        {
            
            Path path = new Path(prefix);
            ArrayList<Short> vector = new ArrayList<Short>(10);
            
            File file = new File("testReadMassRecord.result");
            FileOutputStream out = new FileOutputStream(file);
                       
            for(int i = 0; i < idxs.length; i++)
            {
                vector.add(Short.valueOf(idxs[i]));
            }            
            
            Configuration conf = new Configuration();
            ColumnStorageClient client = new ColumnStorageClient(path, vector, conf);
            long begin = System.currentTimeMillis();
            
            for(int i = 0; i < count; i++)
            {
               Record record = client.getRecordByLine(i);
               
                if (record == null)
                {
                    String string = "record no:" + i + " return null \n" ;
                    out.write(string.getBytes());
                }
                

                Error error = new Error();
                if (!judgeRecord(record, error))
                {
                    String string = "record no:" + i + ": " + error.msg + "\n";
                    out.write(string.getBytes());
                }
              
                if(i % (100*10000) == 0)
                {
                    String string = "read seq " + i +" record, delay:" + ((System.currentTimeMillis() - begin) / 1000) + " s " ;
                    out.write(string.getBytes());
                    out.write(new String("\n").getBytes());
                }
            }
            
            client.close();
            
            long end = System.currentTimeMillis();
            String string = "Read Seq over, count:"+count+", delay:"+(long)((end - begin)/1000) + " s \n";
            out.write(string.getBytes());
            out.close();
            System.out.println(string);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            System.out.println("get IOException:" + e.getMessage());
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.out.println("get exception:" + e.getMessage());
        }
        
    }    
   
    public static void createProjectByte(FormatDataFile fd) throws Exception
    {
        Record record = new Record((short) 1);
        record.addValue(new FieldValue((byte)1, (short)0));        
        fd.addRecord(record); 
    }
    
    public static void createProjectShort(FormatDataFile fd) throws Exception
    {
        Record record = new Record((short) 1);
        record.addValue(new FieldValue((short)2, (short)1));      
        fd.addRecord(record); 
    }
    
    public static void createProjectInt(FormatDataFile fd) throws Exception
    {
        Record record = new Record((short) 1);
        record.addValue(new FieldValue((int)3, (short)2));  
        fd.addRecord(record); 
    }
    public static void createProjectLong(FormatDataFile fd) throws Exception
    {
        Record record = new Record((short) 1);
        record.addValue(new FieldValue((long)4, (short)3));        
        fd.addRecord(record); 
    }
    public static void createProjectFloat(FormatDataFile fd) throws Exception
    {
        Record record = new Record((short) 1);
        record.addValue(new FieldValue((float)5.5, (short)4));       
        fd.addRecord(record); 
    }
    public static void createProjectDouble(FormatDataFile fd) throws Exception
    {
        Record record = new Record((short) 1);
        record.addValue(new FieldValue((double)6.6, (short)5));      
        fd.addRecord(record); 
    }
    public static void createProjectString(FormatDataFile fd) throws Exception
    {
        Record record = new Record((short) 1);
        record.addValue(new FieldValue("hello konten", (short)6));        
        fd.addRecord(record); 
    }
    public static void createProjectMulti(FormatDataFile fd) throws Exception
    {       
        Record record = new Record((short) 3);
        record.addValue(new FieldValue((short)7, (short)7));
        record.addValue(new FieldValue((short)8, (short)8));
        record.addValue(new FieldValue((short)9, (short)9));
               
        fd.addRecord(record); 
    }
    
    static boolean judgeRecord(Record record, Error error) throws IOException
    {
        for(int i = 0; i < record.fieldNum(); i++)
        {            
            FieldValue fieldValue = record.fieldValues().get(i);
            int index = record.getIndex((short) i);         
            if(index == 0)
            {
                byte[] buf = fieldValue.value(); 
                if(buf == null)
                {
                    error.msg = "1, buf null";
                    return false;           
                }
                if(buf[0] != 1)
                {
                    error.msg = "2, error value:"+buf[0];
                    return false;   
                }                           
                
                byte type = fieldValue.type();
                int len = fieldValue.len();
                short idx = fieldValue.idx();
                if(len != ConstVar.Sizeof_Byte)
                {
                    error.msg = "3, error len:"+len;
                    return false;   
                }
                if(type != ConstVar.FieldType_Byte)
                {
                    error.msg = "4, error type:"+type;
                    return false;   
                }
                
                if(idx != 0)
                {
                    error.msg = "5, error idx:"+idx;
                    return false;   
                }
            }
            else if(index == 1)
            {
                byte[] buf = fieldValue.value(); 
                if(buf == null)
                {
                    error.msg = "6, buf null";
                    return false;           
                }
                short val = Util.bytes2short(buf, 0, ConstVar.Sizeof_Short);
                if(val != 2)
                {
                    error.msg = "7, error val:"+val;
                    return false;
                }
                byte type = fieldValue.type();
                int len = fieldValue.len();
                short idx = fieldValue.idx();
                if(len != ConstVar.Sizeof_Short)
                {
                    error.msg = "8, error len:"+len;
                    return false;   
                }
                if(type != ConstVar.FieldType_Short)
                {
                    error.msg = "9, error type:"+type;
                    return false;   
                }
                
                if(idx != 1)
                {
                    error.msg = "10, error idx:"+idx;
                    return false;   
                }
            }
            else if(index == 2)
            {
                byte[] buf = fieldValue.value(); 
                if(buf == null)
                {
                    error.msg = "11, buf null";
                    return false;           
                }
                int val = Util.bytes2int(buf, 0, ConstVar.Sizeof_Int);
                if(val != 3)
                {
                    error.msg = "12, error val:"+val;
                    return false;
                }
                byte type = fieldValue.type();
                int len = fieldValue.len();
                short idx = fieldValue.idx();
                if(len != ConstVar.Sizeof_Int)
                {
                    error.msg = "13, error len:"+len;
                    return false;   
                }
                if(type != ConstVar.FieldType_Int)
                {
                    error.msg = "14, error type:"+type;
                    return false;   
                }
                
                if(idx != 2)
                {
                    error.msg = "15, error idx:"+idx;
                    return false;   
                }
            }
            else if(index == 3)
            {
                byte[] buf = fieldValue.value(); 
                if(buf == null)
                {
                    error.msg = "16, buf null";
                    return false;           
                }
                long val = Util.bytes2long(buf, 0, ConstVar.Sizeof_Long);
                if(val != 4)
                {
                    error.msg = "17, error val:"+val;
                    return false;
                }
                byte type = fieldValue.type();
                int len = fieldValue.len();
                short idx = fieldValue.idx();
                if(len != ConstVar.Sizeof_Long)
                {
                    error.msg = "18, error len:"+len;
                    return false;   
                }
                if(type != ConstVar.FieldType_Long)
                {
                    error.msg = "19, error type:"+type;
                    return false;   
                }
                
                if(idx != 3)
                {
                    error.msg = "20, error idx:"+idx;
                    return false;   
                } 
            }
            else if(index == 4)
            {
                byte[] buf = fieldValue.value(); 
                if(buf == null)
                {
                    error.msg = "21, buf null";
                    return false;           
                }
                float val = Util.bytes2float(buf, 0);
                if(val != 5.5)
                {
                    error.msg = "22, error val:"+val;
                    return false;
                }
                byte type = fieldValue.type();
                int len = fieldValue.len();
                short idx = fieldValue.idx();
                if(len != ConstVar.Sizeof_Float)
                {
                    error.msg = "23, error len:"+len;
                    return false;   
                }
                if(type != ConstVar.FieldType_Float)
                {
                    error.msg = "24, error type:"+type;
                    return false;   
                }
                
                if(idx != 4)
                {
                    error.msg = "25, error idx:"+idx;
                    return false;   
                } 
            }
            else if(index == 5)
            {
                byte[] buf = fieldValue.value(); 
                if(buf == null)
                {
                    error.msg = "26, buf null";
                    return false;           
                }
                double val = Util.bytes2double(buf, 0);
                if(val != 6.6)
                {
                    error.msg = "27, error val:"+val;
                    return false;
                }
                byte type = fieldValue.type();
                int len = fieldValue.len();
                short idx = fieldValue.idx();
                if(len != ConstVar.Sizeof_Double)
                {
                    error.msg = "28, error len:"+len;
                    return false;   
                }
                if(type != ConstVar.FieldType_Double)
                {
                    error.msg = "29, error type:"+type;
                    return false;   
                }
                
                if(idx != 5)
                {
                    error.msg = "30, error idx:"+idx;
                    return false;   
                } 
            }
            else if(index == 6)
            {
                byte[] buf = fieldValue.value(); 
                if(buf == null)
                {
                    error.msg = "31, buf null";
                    return false;           
                }
                
                String dummy = "hello konten";
                byte type = fieldValue.type();
                int len = fieldValue.len();
                short idx = fieldValue.idx();
                if(len !=dummy.length())
                {
                    error.msg = "32, error len:"+len;
                    return false;   
                }
                if(type != ConstVar.FieldType_String)
                {
                    error.msg = "33, error type:"+type;
                    return false;   
                }
                String val = new String(buf, 0, len);
                if(!val.equals(dummy))
                {
                    error.msg = "34, error val:"+val;
                    return false;
                }
                if(idx != 6)
                {
                    error.msg = "35, error idx:"+idx;
                    return false;   
                }
            }
            else if(index == 7)
            {
                byte[] buf = fieldValue.value(); 
                if(buf == null)
                {
                    error.msg = "36, buf null";
                    return false;           
                }
                short val = Util.bytes2short(buf, 0, ConstVar.Sizeof_Short);
                if(val != 7)
                {
                    error.msg = "37, error val:"+val;
                    return false;
                }
                byte type = fieldValue.type();
                int len = fieldValue.len();
                short idx = fieldValue.idx();
                if(len != ConstVar.Sizeof_Short)
                {
                    error.msg = "38, error len:"+len;
                    return false;   
                }
                if(type != ConstVar.FieldType_Short)
                {
                    error.msg = "39, error type:"+type;
                    return false;   
                }                
                if(idx != 7)
                {
                    error.msg = "40, error idx:"+idx;
                    return false;   
                }
            }
            else if(index == 8)
            {
                byte[] buf = fieldValue.value(); 
                if(buf == null)
                {
                    error.msg = "41, buf null";
                    return false;           
                }
                short val = Util.bytes2short(buf, 0, ConstVar.Sizeof_Short);
                if(val != 8)
                {
                    error.msg = "42, error val:"+val;
                    return false;
                }
                byte type = fieldValue.type();
                int len = fieldValue.len();
                short idx = fieldValue.idx();
                if(len != ConstVar.Sizeof_Short)
                {
                    error.msg = "43, error len:"+len;
                    return false;   
                }
                if(type != ConstVar.FieldType_Short)
                {
                    error.msg = "44, error type:"+type;
                    return false;   
                }                
                if(idx != 8)
                {
                    error.msg = "45, error idx:"+idx;
                    return false;   
                }
            }
            else if(index == 9)
            {
                byte[] buf = fieldValue.value(); 
                if(buf == null)
                {
                    error.msg = "46, buf null";
                    return false;           
                }
                short val = Util.bytes2short(buf, 0, ConstVar.Sizeof_Short);
                if(val != 9)
                {
                    error.msg = "47, error val:"+val;
                    return false;
                }
                byte type = fieldValue.type();
                int len = fieldValue.len();
                short idx = fieldValue.idx();
                if(len != ConstVar.Sizeof_Short)
                {
                    error.msg = "48, error len:"+len;
                    return false;   
                }
                if(type != ConstVar.FieldType_Short)
                {
                    error.msg = "49, error type:"+type;
                    return false;   
                }                
                if(idx != 9)
                {
                    error.msg = "50, error idx:"+idx;
                    return false;   
                }
            }
        }
        return true;
        
        /*
        String result = null;
        int index = 0;
        byte[] buf = record.fieldValues().get(index).value; 
        if(buf == null)
        {
            return false;           
        }
        if(buf[0] != 1)
        {
            return false;   
        }                           
        byte type = record.fieldValues().get(index).type;
        int len = record.fieldValues().get(index).len;
        short idx = record.fieldValues().get(index).idx;
        if(len != ConstVar.Sizeof_Byte)
        {
            return false;   
        }
        if(type != ConstVar.FieldType_Byte)
        {
            return false;   
        }
        if(idx != 1)
        {
            return false;   
        }
        
        
        index = 1;
        buf = record.fieldValues().get(index).value; 
        if(buf == null)
        {
            return false;   
        }
        short sval = Comm.bytes2short(buf, 0, ConstVar.Sizeof_Short);
        if(sval != (short)2)
        {
            return false;   
        }                               
        type = record.fieldValues().get(index).type;
        len = record.fieldValues().get(index).len;
        idx = record.fieldValues().get(index).idx;
        if(len != ConstVar.Sizeof_Short)
        {
            return false;   
        }
        if(type != ConstVar.FieldType_Short)
        {
            return false;   
        }
        if(idx != 3)
        {
            return false;   
        }
        
        index = 2;
        buf = record.fieldValues().get(index).value; 
        if(buf == null)
        {
            return false;   
        }
        int ival = Comm.bytes2int(buf, 0, ConstVar.Sizeof_Int);
        if(ival != 3)
        {
            return false;   
        }                               
        type = record.fieldValues().get(index).type;
        len = record.fieldValues().get(index).len;
        idx = record.fieldValues().get(index).idx;
        if(len != ConstVar.Sizeof_Int)
        {
            return false;   
        }
        if(type != ConstVar.FieldType_Int)
        {
            return false;   
        }
        if(idx != 5)
        {
            return false;   
        }
        
        index = 3;
        buf = record.fieldValues().get(index).value; 
        if(buf == null)
        {
            return false;   
        }
        long lval = Comm.bytes2long(buf, 0, ConstVar.Sizeof_Long);
        if(lval != 4)
        {
            return false;   
        }                               
        type = record.fieldValues().get(index).type;
        len = record.fieldValues().get(index).len;
        idx = record.fieldValues().get(index).idx;
        if(len != ConstVar.Sizeof_Long)
        {
            return false;   
        }
        if(type != ConstVar.FieldType_Long)
        {
            return false;   
        }
        if(idx != 7)
        {
            return false;   
        }
        
        index = 4;
        buf = record.fieldValues().get(index).value; 
        if(buf == null)
        {
            return false;   
        }
        float fval = Comm.bytes2float(buf, 0);
        if(fval != (float)5.5)
        {
            return false;   
        }                               
        type = record.fieldValues().get(index).type;
        len = record.fieldValues().get(index).len;
        idx = record.fieldValues().get(index).idx;
        if(len != ConstVar.Sizeof_Float)
        {
            return false;   
        }
        if(type != ConstVar.FieldType_Float)
        {
            return false;   
        }
        if(idx != 9)
        {
            return false;   
        }
        
        index = 5;
        buf = record.fieldValues().get(index).value; 
        if(buf == null)
        {
            return false;   
        }
        double dval = Comm.bytes2double(buf, 0);
        if(dval != (double)6.6)
        {
            return false;   
        }                               
        type = record.fieldValues().get(index).type;
        len = record.fieldValues().get(index).len;
        idx = record.fieldValues().get(index).idx;
        if(len != ConstVar.Sizeof_Double)
        {
            return false;   
        }
        if(type != ConstVar.FieldType_Double)
        {
            return false;   
        }
        if(idx != 11)
        {
            return false;   
        }
        
        index = 6;
        buf = record.fieldValues().get(index).value; 
        if(buf == null)
        {
            return false;   
        }               
        String str = new String(buf);
        if(!str.equals(new String("hello konten")))
        {
            return false;   
        }
        type = record.fieldValues().get(index).type;
        len = record.fieldValues().get(index).len;
        idx = record.fieldValues().get(index).idx;
        if(len != str.length())
        {
            return false;   
        }
        if(type != ConstVar.FieldType_String)
        {
            return false;   
        }
        if(idx != 13)
        {
            return false;   
        }
        */
    }
}
