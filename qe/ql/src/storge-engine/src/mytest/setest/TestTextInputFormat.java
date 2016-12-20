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
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

public class TestTextInputFormat
{
    public static void main(String[] argv) throws IOException, SerDeException
    {
        try
        {
            if (argv.length != 2)
            {
                System.out.println("TestTextInputFormat <input> <output>");
                System.exit(-1);
            }
    
            JobConf conf = new JobConf(TestTextInputFormat.class);
    
            
            conf.setJobName("TestTextInputFormat");
    
            conf.setNumMapTasks(1);
            conf.setNumReduceTasks(1);
            
            conf.setOutputKeyClass(LongWritable.class);
            conf.setOutputValueClass(Unit.Record.class);
    
    
            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(FormatStorageOutputFormat.class);
            conf.set("mapred.output.compress", "flase");
    
            conf.set("mapred.input.dir", argv[0]);
       
            LazySimpleSerDe serDe = initSerDe(conf);
            LazySimpleStructObjectInspector oi = (LazySimpleStructObjectInspector)serDe.getObjectInspector();
            List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
            
            FileInputFormat.setInputPaths(conf, argv[0]);
            Path outputPath = new Path(argv[1]);
            FileOutputFormat.setOutputPath(conf, outputPath);    
    
            InputFormat inputFormat = new TextInputFormat();
            ((TextInputFormat)inputFormat).configure(conf);
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
                    
                    Object row = serDe.deserialize((Text)value);
                    oi.getStructFieldsDataAsList(row);
                                   
                    count++;
                }
                long end = System.currentTimeMillis();
                
                long delay = (end - begin) / 1000;
                totalDelay += delay;
                System.out.println(count + " record read over, delay "+ delay +" s");
            }
            
            System.out.println("total delay:"+totalDelay);
            
            return;
        }
        catch(Exception e)
        {
            e.printStackTrace();
            System.out.println("get exception:"+e.getMessage());
        }
    }    
   
    
    public static LazySimpleSerDe initSerDe(Configuration conf) throws SerDeException
    {
        LazySimpleSerDe serDe = new LazySimpleSerDe();
        Properties tbl = createProperties();
        
        serDe.initialize(conf, tbl);
   
        return serDe;
    }
    
    public static Properties createProperties()
    {
        Properties tbl = new Properties();

        tbl.setProperty(Constants.SERIALIZATION_FORMAT, "9");
        
        tbl.setProperty("columns","abyte,ashort,aint,along,afloat,adouble,astring");
        
        tbl.setProperty("columns.types","tinyint:smallint:int:bigint:float:double:string");
        tbl.setProperty(Constants.FIELD_DELIM, ",");
        return tbl;
    }

}
