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
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Delayed;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import Comm.ConstVar;
import FormatStorage.FieldMap;
import FormatStorage.Head;
import FormatStorage.Unit;
import FormatStorage.FieldMap.Field;
import FormatStorage.Unit.FieldValue;
import FormatStorage.Unit.Record;
import StorageEngineClient.ColumnStorageInputFormat;
import StorageEngineClient.ColumnStorageSplit;
import StorageEngineClient.FormatStorageInputFormat;
import StorageEngineClient.FormatStorageOutputFormat;
import StorageEngineClient.FormatStorageSerDe;
import StorageEngineClient.FormatStorageSplit;


public class TestColumnStorageInputFormat
{
    public static void main(String[] argv) throws IOException, SerDeException
    {
        try
        {
            if (argv.length != 2)
            {
                System.out.println("TestColumnStorageInputFormat <input> idx");
                System.exit(-1);
            }
    
            JobConf conf = new JobConf(TestColumnStorageInputFormat.class);
    
            
            conf.setJobName("TestColumnStorageInputFormat");
    
            conf.setNumMapTasks(1);
            conf.setNumReduceTasks(1);
            
            conf.setOutputKeyClass(LongWritable.class);
            conf.setOutputValueClass(Unit.Record.class);
    
    
            conf.setInputFormat(TextInputFormat.class);
            conf.set("mapred.output.compress", "flase");
    
            conf.set("mapred.input.dir", argv[0]);
            
            conf.set("hive.io.file.readcolumn.ids", argv[1]);            
          
            
            
            FormatStorageSerDe serDe = initSerDe(conf);
            StandardStructObjectInspector oi = (StandardStructObjectInspector)serDe.getObjectInspector();
            List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
            
            FileInputFormat.setInputPaths(conf, argv[0]);
            Path outputPath = new Path(argv[1]);
            FileOutputFormat.setOutputPath(conf, outputPath);    
    
            InputFormat inputFormat = new ColumnStorageInputFormat();
            long begin = System.currentTimeMillis();
            InputSplit[] inputSplits = inputFormat.getSplits(conf, 1);
            long end = System.currentTimeMillis();
            System.out.println("getsplit delay "+ (end - begin) +" ms");
            
            if (inputSplits.length == 0)
            {
                System.out.println("inputSplits is empty");
                return ;
            }
            else
            {
                System.out.println("get Splits:"+inputSplits.length);
            }
           
            int size = inputSplits.length;
            System.out.println("getSplits return size:"+size);
            for(int i = 0; i < size; i++)
            {
                ColumnStorageSplit split = (ColumnStorageSplit) inputSplits[i];
                System.out.printf("split:"+i+" offset:"+split.getStart() + "len:"+split.getLength()+"path:"+split.getPath().toString() +"beginLine:"+split.getBeginLine()+"endLine:"+split.getEndLine());
                if(split.getFileName() != null)
                {
                    System.out.println("fileName:"+split.getFileName());
                }
                else
                {
                    System.out.println("fileName null");
                }
                if(split.fileList() != null)
                {
                    System.out.println("fileList.num:"+split.fileList().size());
                    for(int j= 0; j < split.fileList().size(); j++)
                    {
                        System.out.println("filelist " + j+":"+split.fileList().get(j));
                    }
                }
            }
            
          
            
            while(true)
            {
                int totalDelay = 0;
                RecordReader<WritableComparable, Writable> currRecReader = null;
                for(int i = 0; i < inputSplits.length; i++)
                {
                    currRecReader = inputFormat.getRecordReader(inputSplits[i],
                                                                conf,
                                                                Reporter.NULL);
            
                    WritableComparable key;
                    Writable value;
            
                    key = currRecReader.createKey();
                    value = currRecReader.createValue();
            
                    begin = System.currentTimeMillis();
                    int count = 0;
                    while (currRecReader.next(key, value))
                    {
                        
                        Record record = (Record)value;
                        
                        Object row = serDe.deserialize(record);
                        count++;
                        
                    }
                    end = System.currentTimeMillis();
                    
                    long delay = (end - begin) / 1000;
                    totalDelay += delay;
                    System.out.println(count + " record read over, delay "+ delay +" s");
                }
                
                System.out.println("total delay:"+totalDelay+"\n");
            }
           
        }
        catch(Exception e)
        {
            e.printStackTrace();
            System.out.println("get exception:"+e.getMessage());
        }
    }
    
    public static void initHead(Head head)
    {       
        short fieldNum = 7;
        FieldMap fieldMap = new FieldMap();
        fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
        fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
        fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
        fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
        fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
        fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
        fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short)6));
        
        head.setFieldMap(fieldMap);
        
        head.setVar(ConstVar.VarFlag);
    }
    
    public static FormatStorageSerDe initSerDe(Configuration conf) throws SerDeException
    {
        FormatStorageSerDe serDe = new FormatStorageSerDe();
        Properties tbl = createProperties();
        
        serDe.initialize(conf, tbl);

/*        Record record = new Record((short) 7);
        record.addValue(new FieldValue((byte)1, (short)0));
        record.addValue(new FieldValue((short)2, (short)1));
        record.addValue(new FieldValue((int)3, (short)2));
        record.addValue(new FieldValue((long)4, (short)3));
        record.addValue(new FieldValue((float)5.5, (short)4));
        record.addValue(new FieldValue((double)6.6, (short)5));
        record.addValue(new FieldValue("hello konten", (short)6));
        
        
        Object[] refer = {new Byte((byte) 1),new Short((short) 2), new Integer(3), new Long(4),
                          new Float(5.5), new Double(6.6), new String("hello konten")};
        
  */      
        return serDe;
    }
    
    public static Properties createProperties()
    {
        Properties tbl = new Properties();

        /*
        tbl.setProperty(Constants.SERIALIZATION_FORMAT, "9");
        
        tbl.setProperty("columns","abyte,ashort,aint,along,afloat,adouble,astring");
        
        tbl.setProperty("columns.types","tinyint:smallint:int:bigint:float:double:string");
        tbl.setProperty(Constants.SERIALIZATION_NULL_FORMAT, "NULL");
        */
        
        tbl.setProperty(Constants.SERIALIZATION_FORMAT, "9");        
        
        tbl.setProperty("columns","insertdate,flag,pindao,qq_num,pv,time_0,time_1,time_2,time_3,time_4,time_5,time_6,time_7,time_8,time_9,time_10,time_11,time_12,time_13,time_14,time_15,time_16,time_17,time_18,time_19,time_20,time_21,time_22,time_23,day_1,day_2,day_3,day_4,day_5,day_6,day_7,day_8,day_9,day_10 ,day_11 ,day_12 ,day_13 ,day_14 ,day_15 ,day_16 ,day_17 ,day_18 ,day_19 ,day_20 ,day_21 ,day_22 ,day_23 ,day_24 ,day_25 ,day_26 ,day_27 ,day_28,day_29,day_30,day_31,ref_minisite_pv,ref_tips_pvref_mail_pv,ref_soso_pv,ref_minigame_pv,ref_own_pv");
        
        tbl.setProperty("columns.types","string,string,string,int,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint");
        tbl.setProperty(Constants.SERIALIZATION_NULL_FORMAT, "NULL");
        return tbl;
    }

}
