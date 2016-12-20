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

import java.io.IOException;

import java.util.Properties;


import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.thrift.meta_data.FieldValueMetaData;

import Comm.ConstVar;
import FormatStorage.FieldMap;
import FormatStorage.Head;
import FormatStorage.Unit;
import FormatStorage.FieldMap.Field;
import FormatStorage.Unit.FieldValue;
import FormatStorage.Unit.Record;
import StorageEngineClient.ColumnStorageOutputFormat;

import StorageEngineClient.ColumnStorageWriter;
import StorageEngineClient.FormatStorageOutputFormat;
import StorageEngineClient.FormatStorageSerDe;



public class TestColumnStorageOutputFormat
{
    public static class MyColumnOutputFormat
    {
        ColumnStorageWriter[] columnStorageWriters = null;
        
        MyColumnOutputFormat(Head head, JobConf job, Path outputFilePath) throws Exception
        {
            int fieldNum = head.fieldMap().fieldNum();
            columnStorageWriters = new ColumnStorageWriter[fieldNum];
            
            for(int i = 0; i < fieldNum; i++)
            {
                String fileName = outputFilePath.toString() + "_idx" + head.fieldMap().getField((short) i).index();
                                                
                FieldMap fieldMap = new FieldMap();
                fieldMap.addField(head.fieldMap().getField((short) i));
                
                Head tmpHead = new Head(head);
                tmpHead.setFieldMap(fieldMap);
                
                String projection = "";
                columnStorageWriters[i] = new ColumnStorageWriter(job, tmpHead, projection, fileName);
            }
        }
        
        void doWrite(Record record) throws IOException
        {
            for(int i = 0; i < columnStorageWriters.length; i++)
            {
                columnStorageWriters[i].append((Writable)null, (Writable)record);
            }
        }
    }
    public static void main(String[] argv) throws IOException
    {
        try
        {
            if (argv.length != 2)
            {
                System.out.println("TestColumnStorageOutputFormat <output> <count>");
                System.exit(-1);
            }
    
            JobConf conf = new JobConf(TestColumnStorageOutputFormat.class);
    
            
            conf.setJobName("TestColumnStorageOutputFormat");
    
            conf.setNumMapTasks(1);
            conf.setNumReduceTasks(1);
            
            conf.setOutputKeyClass(LongWritable.class);
            conf.setOutputValueClass(Unit.Record.class);
    
    
            conf.setOutputFormat(ColumnStorageOutputFormat.class);
            conf.set("mapred.output.compress", "flase");
    
            conf.set("mapred.output.dir", argv[0]);
            
            Head head = new Head();
            initHead(head);
            
            head.toJobConf(conf);
            

            Path outputPath = new Path(argv[0]);
            FileOutputFormat.setOutputPath(conf, outputPath);  
            
            FileSystem fs = FileSystem.get(conf);
            MyColumnOutputFormat output = new MyColumnOutputFormat(head, conf, outputPath);
          
            
            
            long begin = System.currentTimeMillis();
            int count = Integer.valueOf(argv[1]);
            String string = "hello konten";
            for(int i = 0; i < count; i ++)
            {   
                Record record = new Record((short) 210);
               
                for(short j = 0; j < 30; j++)
                {                    
                    record.addValue(new FieldValue((byte)1, (short) (j*7+0)));
                    record.addValue(new FieldValue((short)2,(short)(j*7+1)));
                    record.addValue(new FieldValue((int)3, (short)(j*7+2)));
                    record.addValue(new FieldValue((long)4, (short)(j*7+3)));
                    record.addValue(new FieldValue((float)5.5, (short)(j*7+4)));
                    record.addValue(new FieldValue((double)6.6, (short)(j*7+5)));
                    record.addValue(new FieldValue((double)7.7, (short)(j*7+6)));
                 
                }
                output.doWrite(record);
                
                if(i % 100000 == 0)
                {
                    long end = System.currentTimeMillis();
                    System.out.println(i + "record write, delay:"+(end - begin)/1000 + "s");
                }
            }
            
            long end = System.currentTimeMillis();
            System.out.println(count + "record write over, delay:"+(end - begin)/1000 + "s");
        }
        catch(Exception e)
        {
            e.printStackTrace();
            System.out.println("get exception:"+e.getMessage());
        }
    }
    
    public static void initHead(Head head)
    {       
        short fieldNum = 70;
        FieldMap fieldMap = new FieldMap();
        
        for(int i = 0; i < 30; i++)
        {
            fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short) (i * 7 + 0)));
            fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short) (i * 7 + 1)));
            fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short) (i * 7 + 2)));
            fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short) (i * 7 + 3)));
            fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short) (i * 7 + 4)));
            fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short) (i * 7 + 5)));
            fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short) (i * 7 + 6)));
        }
        
        head.setFieldMap(fieldMap);
        
        head.setVar(ConstVar.VarFlag);
    }
}
