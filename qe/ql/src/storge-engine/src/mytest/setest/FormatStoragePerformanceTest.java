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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import Comm.ConstVar;
import FormatStorage.FormatDataFile;
import FormatStorage.Unit.Record;

public class FormatStoragePerformanceTest
{
    static File file = new File("FormatStoragePerformanceTest.result");
    static FileOutputStream output = null;
    
    public static void main(String[] argv) throws Exception
    {
        if(argv.length != 3)
        {
            System.out.println("usage:FormatStoragePerformanceTest cmd[write | readSeq | readRand ] count var");
            return;
        }
        
        output = new FileOutputStream(file);
        
        String cmd = argv[0];
        
        String initFile = "write";                
        String readSeq = "readSeq";
        String readRand = "readRand";
        
        int count = Integer.valueOf(argv[1]);
        boolean var = Boolean.valueOf(argv[2]);
        System.out.println("Cmd:"+cmd+",count:"+count+",var:"+var);
        
        if(cmd.equals(initFile))
        {
            doInitFile(count, var);
        }
        else if(cmd.equals(readRand))
        {
            doReadRand(count, var);
        }
        else
        {
            doReadSeq(count, var);
        }
        
        output.close();
    }
    
    static void doInitFile(int count, boolean var)
    {
        try
        {
            String textFile = "MR_input_text/testPerformanceReadText";
            if(var)
            {
                textFile += "_var";
            }
            Path path = new Path(textFile);
            FileSystem fs = FileSystem.get(new Configuration());
            FSDataOutputStream out = fs.create(path);
            
            OutputStream stream =  new BufferedOutputStream( out);
            BufferedWriter writer   = new BufferedWriter(new OutputStreamWriter(stream));
            
            String value = null;
            if(var)
            {
                value = "111,22222,33333333,444444444444,5555555.5555,6666666666.666666,hello konten\n" ;
            }
            else
            {
                value = "111,22222,33333333,444444444444,5555555.5555,6666666666.666666\n" ;
            }
            
            long begin = System.currentTimeMillis();            
            
            for(int i = 0; i < count; i++)
            {                
                writer.write(value);
                
                if(i % 10000000 == 0)
                {
                    String string = "write " + i + " record, delay: " + ((System.currentTimeMillis() - begin)/1000) + " s \n" ;
                    output.write(string.getBytes());  
                }
            }
            writer.close();
            
            long end = System.currentTimeMillis();
            
            String string = "write " + count + " record over, delay: " + ((end - begin)/1000) + " s \n" ;
            output.write(string.getBytes());            
        }
        catch (Exception e)
        {
            e.printStackTrace();  
            System.out.println(e.getMessage());
        }       
    }
    
    static void doReadRand(int count, boolean var)
    {     
        while(1 != 0)
        {
            doFormatReadRand(count, var);
        }
    }
    
    static void doReadSeq(int count, boolean var)
    {   
        while(1!=0)
        {
            doFormatReadSeq(count, var);
            doTextReadSeq(count, var);
        }
      
       
    }

    static void doFormatReadRand(int count, boolean var)
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
    
    static void doFormatReadSeq(int count, boolean var)
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
            fd.open(fileName);
   
            boolean compressed = (fd.head().compress() == ConstVar.Compressed);
            long begin = System.currentTimeMillis();
            
            for(int i = 0; i < count; i++)
            {
                Record valueRecord = new Record();
                valueRecord.clear();
                if(compressed)
                {
                    Record record = fd.getNextRecord(valueRecord);
                    if (record == null)
                    {
                    }
                }
                else
                {
                    Record record = fd.getRecordByLine(i);
                    if (record == null)
                    {
                    }
                }
                
                if(i % (1*1000000) == 0)
                {
                    String string = "read format seq " + i +" record, delay:" + ((System.currentTimeMillis() - begin) / 1000) + " s \n" ;
                    output.write(string.getBytes());                   
                }
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
    
    
    static void doTextReadSeq(int count, boolean var)
    {
        try
        {
            ArrayList<Integer> meta = new ArrayList<Integer>(10);
            for(int i = 0; i < 7; i++)
            {
                meta.add(i);
            }
            
            String textFile = "MR_input_text/testPerformanceReadText";
            if(var)
            {
                textFile += "_var";
            }
            Path path = new Path(textFile);
            FileSystem fs = FileSystem.get(new Configuration());
            FSDataInputStream in = fs.open(path);
            
            InputStream stream =  new BufferedInputStream(in);
            BufferedReader reader   = new BufferedReader(new InputStreamReader(stream));
            
            
            long begin = System.currentTimeMillis();            
            for(int i = 0; i < count; i++)
            {
                String value = reader.readLine();   
                
                String[] fields = value.split(",");
                
               /*
                ByteArrayInputStream bin = new ByteArrayInputStream(value.getBytes());
                
                meta.get(0); byte[] bb= new byte[4]; bin.read(bb); 
                meta.get(1); byte[] sb= new byte[6]; bin.read(sb);  
                meta.get(2); byte[] ib= new byte[9]; bin.read(ib);  
                meta.get(3); byte[] lb= new byte[13]; bin.read(lb); 
                meta.get(4); byte[] fb= new byte[13]; bin.read(fb);  
                meta.get(5); byte[] db= new byte[18]; bin.read(db);  
                meta.get(6);
                value = null;
                */
                Byte.valueOf(fields[0]);
                Short.valueOf(fields[1]);
                Integer.valueOf(fields[2]);
                Long.valueOf(fields[3]);
                Float.valueOf(fields[4]);
                Double.valueOf(fields[5]);
                if(var)
                {
                    String.valueOf(fields[6]);
                }
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
