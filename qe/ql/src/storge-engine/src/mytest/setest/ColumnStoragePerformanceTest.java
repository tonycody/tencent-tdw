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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader; 
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.RecordReader;

import ColumnStorage.ColumnStorageClient;
import Comm.ConstVar;
import FormatStorage.FieldMap;
import FormatStorage.FormatDataFile;
import FormatStorage.Head;
import FormatStorage.Unit;
import FormatStorage.FieldMap.Field;
import FormatStorage.Unit.FieldValue;
import FormatStorage.Unit.Record;

public class ColumnStoragePerformanceTest
{
    static File file = new File("ColumnStoragePerformanceTest.result");
    static FileOutputStream output = null;
    
    static String textPrefix = "/user/tdwadmin/se_test/cs/";    
    static String columnPrefix = "/user/tdwadmin/se_test/cs/perf/";
    static String formatPrefix = "/user/tdwadmin/se_test/cs/";
    static String textFilename = textPrefix + "Text";
    static String formatStorageFilename = formatPrefix + "formatStorageFile" ;
    
    static String byteFileName = columnPrefix + "Column_Byte";
    static String shortFileName = columnPrefix + "Column_Short";
    static String intFileName = columnPrefix + "Column_Int";
    static String longFileName = columnPrefix + "Column_Long";
    static String floatFileName = columnPrefix + "Column_Float";
    static String doubleFileName = columnPrefix + "Column_Double";
    static String stringFileName = columnPrefix + "Column_String";
    static String multiFileNameString = columnPrefix + "Column_Multi";
    
    static int count = 1000*10000;
    static Configuration conf = null;
    
    static FormatDataFile[] fds = new FormatDataFile[35];
    static String[] names = {byteFileName, shortFileName, intFileName, longFileName, floatFileName, doubleFileName, stringFileName};
    static byte[] types = {ConstVar.FieldType_Byte, ConstVar.FieldType_Short, ConstVar.FieldType_Int, ConstVar.FieldType_Long, ConstVar.FieldType_Float, ConstVar.FieldType_Double, ConstVar.FieldType_String};
    static int [] lens = {ConstVar.Sizeof_Byte, ConstVar.Sizeof_Short, ConstVar.Sizeof_Int, ConstVar.Sizeof_Long, ConstVar.Sizeof_Float, ConstVar.Sizeof_Double, 0};
    static Object[] values = {(byte)1, (short)2, (int)3, (long)4, (float)5.5, (double)6.6, (String)"hello konten"};
    
    static byte compress = 1;
    
    public static void main(String[] argv) 
    {
        try
        {
            if(argv.length != 3)
            {
                System.out.println("usage: ColumnStroageStabilityTest cmd[write | readSeq | readRand] Count Compress\n");
                return;
            }
                  
            String cmd = argv[0];
            count = Integer.valueOf(argv[1]);
            compress = Byte.valueOf(argv[2]);
            
            System.out.println("cmd:"+cmd+",count:"+count);
            
            output = new FileOutputStream(file);  
            
            String writeCmd = "write";
            String readSeq = "readSeq";
            String readRand = "readRand";
            
            conf = new Configuration();
            conf.setInt("dfs.replication", 1);
            
            if(cmd.equals(writeCmd))
            {
                doInitFile();
            }
            else if(cmd.equals(readRand))
            {
                doReadRand();
            }
            else
            {
                doReadSeq();
            }
            
            output.close();
        }
        catch(Exception e)
        {
            e.printStackTrace();
            System.out.println("get exception:"+e.getMessage());
        }
    }
    
    static void doInitFile() throws Exception
    {
        doInitColumnStorageFile();        
    }
    
    static void doInitTextFile()
    {
        try
        {            
            Path path = new Path(textFilename);
            FileSystem fs = FileSystem.get(conf);
            FSDataOutputStream out = fs.create(path);
            
            OutputStream stream =  new BufferedOutputStream( out);
            BufferedWriter writer   = new BufferedWriter(new OutputStreamWriter(stream));
            
            String value = "111,22222,33333333,444444444444,5555555.5555,6666666666.666666,hello konten,111,22222,33333333,444444444444,5555555.5555,6666666666.666666,hello konten,111,22222,33333333,444444444444,5555555.5555,6666666666.666666,hello konten,111,22222,33333333,444444444444,5555555.5555,6666666666.666666,hello konten,111,22222,33333333,444444444444,5555555.5555,6666666666.666666,hello konten\n" ;
            
            long begin = System.currentTimeMillis();
                        
            for(int i = 0; i < count; i++)
            {                
                writer.write(value);
                
                if(i % 1000000 == 0)
                {
                    String string = "write " + i + " record, delay: " + ((System.currentTimeMillis() - begin)/1000) + " s \n" ;
                    output.write(string.getBytes());  
                }
            }
            writer.close();
            out.close();
            
            long end = System.currentTimeMillis();
            
            String string = "write " + count + " record over(text), delay: " + ((end - begin)/1000) + " s \n" ;
            output.write(string.getBytes()); 
            System.out.println(string);
        }
        catch (Exception e)
        {
            e.printStackTrace();  
            System.out.println(e.getMessage());
        }       
    }
    
    static void initOtherFD() throws Exception
    {
        for(int i = 1; i < 5; i++)
        {
            for(int k = 0; k < 7; k++)
            {
                String fileName = names[k]+(i*7 + k);
                System.out.println("write single file:" + fileName);
                FieldMap fieldMap = new FieldMap();
                fieldMap.addField(new Field(types[k], lens[k], (short)(i*7+k)));
                
                Head head = new Head();
                head.setFieldMap(fieldMap);
                head.setCompress(compress);
                
                fds[i*7 + k] = new FormatDataFile(conf);
                fds[i*7 + k].create(fileName, head);  
            }
        }
    }
    
    static void closeOtherFD() throws Exception
    {
        for(int i = 0; i < 35; i++)
        {
            if(fds[i] != null)
            {
                fds[i].close();
            }
        }
    }
    
    public static void doInitColumnStorageFile() throws Exception
    { 
        System.out.println("write multi file:"+multiFileNameString);    
        
        
        FieldMap fieldMap06 = new FieldMap();
        fieldMap06.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));        
        fieldMap06.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
        fieldMap06.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
        fieldMap06.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
        fieldMap06.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
        fieldMap06.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));        
        fieldMap06.addField(new Field(ConstVar.FieldType_String, 0, (short)6)); 
        
        Head head06 = new Head();
        head06.setFieldMap(fieldMap06);  
        head06.setCompress(compress);
        
        FormatDataFile fd06 = new FormatDataFile(conf);
        fd06.create(multiFileNameString, head06);  
        
        initOtherFD();    
        
        long begin = System.currentTimeMillis();
        for(int i = 0; i < count; i++)
        {            
            createProject0_6(fd06, i); 
            createProject7_34(i);            
            
            if(i % 1000000 == 0)
            {
                long end = System.currentTimeMillis();
                String string = i + " record ok, delay:"+(end - begin) / 1000 + " s \n";
                output.write(string.getBytes());
            }              
        }        
        
        fd06.close();
        closeOtherFD();
        
        long end = System.currentTimeMillis();
        String string = count + " record over(column), delay:"+(end - begin) / 1000 + " s \n";
        System.out.println(string);
        output.write(string.getBytes());
       
    }
    
    public static void doInitFormatStorageFile() throws Exception
    {
        FieldMap fieldMap = new FieldMap();
        for(short i = 0; i < 5; i++)
        {
            fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)(i*7)));
            fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short) (i*7+1)));
            fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short) (i*7+2)));
            fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short) (i*7+3)));
            fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short) (i*7+4)));
            fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short) (i*7+5)));
            fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short) (i*7+6)));
        }
        Head head = new Head();
        head.setFieldMap(fieldMap);      
        
        System.out.println("write file:"+formatStorageFilename);
        FormatDataFile fd = new FormatDataFile(new Configuration());
        fd.create(formatStorageFilename, head);
        
        long begin = System.currentTimeMillis();
        for(int i = 0; i < count; i ++)
        {
            Record record = new Record((short) 35);
            
            for(short j = 0; j < 5; j++)
            {
                record.addValue(new FieldValue((byte)1, (short) (j*7+0)));
                record.addValue(new FieldValue((short)2, (short)(j*7+1)));
                record.addValue(new FieldValue((int)3, (short)(j*7+2)));
                record.addValue(new FieldValue((long)4, (short)(j*7+3)));
                record.addValue(new FieldValue((float)5.5, (short)(j*7+4)));
                record.addValue(new FieldValue((double)6.6, (short)(j*7+5)));
                record.addValue(new FieldValue("hello konten", (short)(j*7+6)));
            }
            fd.addRecord(record);
            
            if(i % 1000000 == 0)
            {
                long end = System.currentTimeMillis();
                String string = i + " record ok(format), delay:"+(end - begin) / 1000 + " s \n";  
                output.write(string.getBytes());
            }
        }
        
        fd.close();
        long end = System.currentTimeMillis();
        String string = count + " record over(formatStorage), delay:"+(end - begin) / 1000 + " s \n";
        output.write(string.getBytes());
        System.out.println(string);
        
    }
    
    public static void createProject0_6(FormatDataFile fd, int line) throws Exception
    {
        Record record = new Record((short) 7);
        record.addValue(new FieldValue((byte)(1+line), (short)0));
        record.addValue(new FieldValue((short)(2+line), (short)1));
        record.addValue(new FieldValue((int)(3+line), (short)2));
        record.addValue(new FieldValue((long)(4+line), (short)3));
        record.addValue(new FieldValue((float)(5.5+line), (short)4));
        record.addValue(new FieldValue((double)(6.6+line), (short)5));
        record.addValue(new FieldValue("hello konten"+line, (short)6));      
              
        fd.addRecord(record); 
    }
    
    public static void createProject7_34(int line) throws Exception
    {
        for(int i = 1; i < 5; i++)
        {
            for(int k = 0; k < 7; k++)
            {
                Record record = new Record((short) 1); 
                if(k == 0)
                {
                    record.addValue(new FieldValue((byte)(1+line), (short)(i*7 + k)));
                }
                else if(k == 1)
                {
                    record.addValue(new FieldValue((short)(2+line), (short)(i*7 + k)));
                }
                else if(k == 2)
                {
                    record.addValue(new FieldValue((int)(3+line), (short)(i*7 + k)));
                }
                else if(k == 3)
                {
                    record.addValue(new FieldValue((long)(4+line), (short)(i*7 + k)));
                }
                else if(k == 4)
                {
                    record.addValue(new FieldValue((float)(5.5+line), (short)(i*7 + k)));
                }
                else if(k == 5)
                {
                    record.addValue(new FieldValue((double)(6.6+line), (short)(i*7 + k)));
                }
                else
                {
                    record.addValue(new FieldValue("hello konten"+line, (short)(i*7 + k)));
                }
                
                fds[i * 7 + k].addRecord(record); 
            }
        }        
    }
    
    static void doReadRand()
    {        
        while(1 != 0)
        {
            doFormatReadRand(count);
        }
    }
    
    static void doReadSeq() throws Exception
    {
        boolean compressed = (compress == 1);
        {
            
            ArrayList<Short> idx = new ArrayList<Short>(10);
           
            
            idx.add((short) 0);
            idx.add((short) 1);
            idx.add((short) 2);
            idx.add((short) 3);
            idx.add((short) 4);
            idx.add((short) 5);
            idx.add((short) 6);
           
            long begin = System.currentTimeMillis();
            doColumnReadSeq(idx, count, compressed);
            long end = System.currentTimeMillis();
            System.out.println("Read Column Seq over(7 field same file) , count:"+count+", delay:"+(long)((end - begin)/1000) + " s");
            
            idx.clear();
            idx.add((short) 7);
            idx.add((short) 8);
            idx.add((short) 9);
            idx.add((short) 10);
            idx.add((short) 11);
            idx.add((short) 12);
            idx.add((short) 13);  
            idx.add((short) 14);
            idx.add((short) 15);
            idx.add((short) 16);
            idx.add((short) 17);
            begin = System.currentTimeMillis();
            doColumnReadSeq(idx, count, compressed);
            end = System.currentTimeMillis();
            System.out.println("Read Column Seq over(11 field diff file) , count:"+count+", delay:"+(long)((end - begin)/1000) + " s");
            
            idx.clear();
            idx.add((short) 7);
            idx.add((short) 8);
            idx.add((short) 9);
            idx.add((short) 10);
            idx.add((short) 11);
            idx.add((short) 12);
            idx.add((short) 13);   
            begin = System.currentTimeMillis();
            doColumnReadSeq(idx, count, compressed);
            end = System.currentTimeMillis();
            System.out.println("Read Column Seq over(7 field diff file) , count:"+count+", delay:"+(long)((end - begin)/1000) + " s");
            
            idx.clear();
            idx.add((short) 9);
            idx.add((short) 10);
            idx.add((short) 13);
            begin = System.currentTimeMillis();
            doColumnReadSeq(idx, count, compressed);
            end = System.currentTimeMillis();
            System.out.println("Read Column Seq over(3 field diff file) , count:"+count+", delay:"+(long)((end - begin)/1000) + " s");
            
            idx.clear();
            idx.add((short) 9);
            idx.add((short) 16);
            idx.add((short) 23);
            begin = System.currentTimeMillis();
            doColumnReadSeq(idx, count, compressed);
            end = System.currentTimeMillis();
            System.out.println("Read Column Seq over(3 field diff file, not var) , count:"+count+", delay:"+(long)((end - begin)/1000) + " s");
            
            idx.clear();
            idx.add((short) 13);
            idx.add((short) 20);
            idx.add((short) 27);
            begin = System.currentTimeMillis();
            doColumnReadSeq(idx, count, compressed);
            end = System.currentTimeMillis();
            System.out.println("Read Column Seq over(3 field diff file, var) , count:"+count+", delay:"+(long)((end - begin)/1000) + " s");
            
            
            idx.clear();
            for(short i = 0; i < 35; i++)
            {
                idx.add(i);
            }
            begin = System.currentTimeMillis();
            doColumnReadSeq(idx, count, compressed);
            end = System.currentTimeMillis();
            System.out.println("Read Column Seq over(35 field) , count:"+count+", delay:"+(long)((end - begin)/1000) + " s");
            
        }
    }

    static void doColumnReadSeq(ArrayList<Short> idx, int count, boolean compress) throws Exception
    {                
        Path path = new Path(columnPrefix);
        ColumnStorageClient client = new ColumnStorageClient(path, idx, conf);
        
        
        for(int i = 0; i < count; i++)
        {
            try
            {
                if(!compress)
                {
                    Record record = client.getRecordByLine(i);
                }
                else
                {
                    Record record = client.getNextRecord();
                }
                /*if (record == null)
                {
                    String string = "record no:" + i + " return null";
                    output.write(string.getBytes());
                }
               
                if(i % (1*1000000) == 0)
                {
                    String string = "read format seq " + i +" record, delay:" + ((System.currentTimeMillis() - begin) / 1000) + " s \n" ;
                    output.write(string.getBytes());                   
                }*/
                
            }
            catch(Exception e)
            {
                System.out.println("get exception, line:"+i);
                System.exit(i);
                break;
            }
        }
        client.close();
        
        
    }
    
    static void doFormatReadRand(int count)
    {
        try 
        {           
            String fileName = "MR_input/testMassRecord";   
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);
            fd.open(fileName);            
         
            long begin = System.currentTimeMillis();
            for(int i = 0; i < count; i++)
            {
                int rand = (int)(Math.random() * count);
                
                Record record = fd.getRecordByLine(rand);
                if(record == null)
                {
                    String string = "record no:"+rand+" return null";
                    output.write(string.getBytes());
                }                
              
                if(i % (1*10000) == 0)
                {
                    String string = "read format rand " + i +" record, delay:" + ((System.currentTimeMillis() - begin) / 1000) + " s \n" ;
                    output.write(string.getBytes());          
                }
            }
            long end = System.currentTimeMillis();
            String string = "Read Foramt Rand over, count:"+count+", delay:"+(long)((end - begin)/1000) + " s";
            output.write(string.getBytes());
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
    
    static void doFormatReadSeq(int count)
    {
        try
        {           
            FormatDataFile fd = new FormatDataFile(conf);
            fd.open(formatStorageFilename);
   
            long begin = System.currentTimeMillis();
            
            Record record = new Record((short)35);
            for(int i = 0; i < count; i++)
            {
                record.clear();
                record = fd.getRecordByLine(i);
                
                /*if (record == null)
                {
                }
                
                if(i % (1*1000000) == 0)
                {
                    String string = "read format seq " + i +" record, delay:" + ((System.currentTimeMillis() - begin) / 1000) + " s \n" ;
                    output.write(string.getBytes());                   
                }
                */
            }
            
            long end = System.currentTimeMillis();
            String string = "Read Foramt Seq over, count:"+count+", delay:"+(long)((end - begin)/1000) + " s";
            output.write(string.getBytes());
            System.out.println(string);
            fd.close();
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
    
    static void doFormatReadSeq2(int count)
    {
        try
        {
            String fileName = "MR_input/testMassRecord";
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);
            fd.open(fileName);
   
            long begin = System.currentTimeMillis();
            
            int ct = 0;
            {
                Record record = null;
                if (record == null)
                {
                    String string = "seq 2 record no:" + ct + " return null";
                    output.write(string.getBytes());
                }
                
                if(ct % (1*10000) == 0)
                {
                    String string = "read format seq2 " + ct +" record, delay:" + ((System.currentTimeMillis() - begin) / 1000) + " s \n" ;
                    output.write(string.getBytes());                   
                }
            }
            
            long end = System.currentTimeMillis();
            String string = "Read Foramt Seq2 over, count:"+ct+", delay:"+(long)((end - begin)/1000) + " s";
            output.write(string.getBytes());
            System.out.println(string);
            fd.close();
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
    static void doTextReadRand(int count)
    {
        try
        {            
            String textFile = "MR_input_text/testPerformanceReadText";
            Path path = new Path(textFile);
            FileSystem fs = FileSystem.get(new Configuration());
            FSDataInputStream in = fs.open(path);
            
            InputStream stream =  new BufferedInputStream(in);
            BufferedReader reader   = new BufferedReader(new InputStreamReader(stream));
            
            
            long begin = System.currentTimeMillis();            
            count = 1*1000;
            for(int i = 0; i < count; i++)
            {
                int line = (int) (Math.random() * count);
                for(int j = 0; j < line; j++)
                {
                    String value = reader.readLine();
                    value = null;
                }
            }
            reader.close(); 
            
            long end = System.currentTimeMillis();          
            String string = "text read seq, count:"+count+", delay:"+(long)((end - begin)/1000) + " s";
            output.write(string.getBytes());
            System.out.println(string);
         
            
              
        }
        catch (Exception e)
        {
            e.printStackTrace();   
            System.out.println(e.getMessage());
        }
    }
    
    
    static void doTextReadSeq(int count)
    {
        try
        {
            ArrayList<Integer> meta = new ArrayList<Integer>(10);
            for(int i = 0; i < 7; i++)
            {
                meta.add(i);
            }
            
            Path path = new Path(textFilename);
            FileSystem fs = FileSystem.get(new Configuration());
            FSDataInputStream in = fs.open(path);
            
            InputStream stream =  new BufferedInputStream(in);
            BufferedReader reader   = new BufferedReader(new InputStreamReader(stream));
            
            
            long begin = System.currentTimeMillis();            
            for(int i = 0; i < count; i++)
            {
                String value = reader.readLine();   
                
                String[] fields = value.split(",");
                
                ByteArrayInputStream bin = new ByteArrayInputStream(value.getBytes());
                
                meta.get(0); byte[] bb= new byte[4]; bin.read(bb); 
                meta.get(1); byte[] sb= new byte[6]; bin.read(sb);  
                meta.get(2); byte[] ib= new byte[9]; bin.read(ib);  
                meta.get(3); byte[] lb= new byte[13]; bin.read(lb); 
                meta.get(4); byte[] fb= new byte[13]; bin.read(fb);  
                meta.get(5); byte[] db= new byte[18]; bin.read(db);  
                meta.get(6);
                value = null;                
            }
            reader.close();
            
            long end = System.currentTimeMillis();
            
            String string = "text read seq " + count + " record over, delay: " + ((end - begin)/1000) + " s \n" ;
            System.out.println(string);            
        }
        catch (Exception e)
        {
            e.printStackTrace();   
            System.out.println(e.getMessage());
        }       
    }  
}
