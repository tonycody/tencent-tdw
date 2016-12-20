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
import StorageEngineClient.FormatStorageInputFormat;
import StorageEngineClient.FormatStorageOutputFormat;
import StorageEngineClient.FormatStorageSerDe;
import StorageEngineClient.FormatStorageSplit;


public class TestFormatStorageInputFormat
{
    public static void main(String[] argv) throws IOException, SerDeException
    {
        try
        {
            if (argv.length != 2)
            {
                System.out.println("TestFormatStorageInputFormat <input> <output>");
                System.exit(-1);
            }
    
            JobConf conf = new JobConf(TestFormatStorageInputFormat.class);
    
            
            conf.setJobName("TestFormatStorageInputFormat");
    
            conf.setNumMapTasks(1);
            conf.setNumReduceTasks(1);
            
            conf.setOutputKeyClass(LongWritable.class);
            conf.setOutputValueClass(Unit.Record.class);
    
    
            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(FormatStorageOutputFormat.class);
            conf.set("mapred.output.compress", "flase");
    
            conf.set("mapred.input.dir", argv[0]);
            
            Head head = new Head();
            initHead(head);
            
            head.toJobConf(conf);
            
            FormatStorageSerDe serDe = initSerDe(conf);
            StandardStructObjectInspector oi = (StandardStructObjectInspector)serDe.getObjectInspector();
            List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
            
            FileInputFormat.setInputPaths(conf, argv[0]);
            Path outputPath = new Path(argv[1]);
            FileOutputFormat.setOutputPath(conf, outputPath);    
    
            InputFormat inputFormat = new FormatStorageInputFormat();
            InputSplit[] inputSplits = inputFormat.getSplits(conf, 1);
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
                FormatStorageSplit split = (FormatStorageSplit) inputSplits[i];
                System.out.printf("split:"+i+"offset:"+split.getStart() + "len:"+split.getLength()+"path:"+conf.get(ConstVar.InputPath) +"beginLine:"+split.getBeginLine()+"endLine:"+split.getEndLine() + "\n");           
            }
            
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
            
                    long begin = System.currentTimeMillis();
                    int count = 0;
                    while (currRecReader.next(key, value))
                    {
                        Record record = (Record)value;
                        
                        Object row = serDe.deserialize(record);
                        count++;
                    }
                    long end = System.currentTimeMillis();
                    
                    long delay = (end - begin) / 1000;
                    totalDelay += delay;
                    System.out.println(count + " record read over, delay "+ delay +" s");
                }
                
                System.out.println("total delay:"+totalDelay);
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

        tbl.setProperty(Constants.SERIALIZATION_FORMAT, "9");
        
        tbl.setProperty("columns","abyte,ashort,aint,along,afloat,adouble,astring");
        
        tbl.setProperty("columns.types","tinyint:smallint:int:bigint:float:double:string");
        tbl.setProperty(Constants.SERIALIZATION_NULL_FORMAT, "NULL");
        return tbl;
    }

}
