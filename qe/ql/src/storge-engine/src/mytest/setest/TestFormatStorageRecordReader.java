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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;


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
import org.apache.thrift.meta_data.FieldValueMetaData;

import Comm.ConstVar;
import FormatStorage.FieldMap;
import FormatStorage.FormatDataFile;
import FormatStorage.Head;
import FormatStorage.Unit;
import FormatStorage.FieldMap.Field;
import FormatStorage.Unit.FieldValue;
import FormatStorage.Unit.Record;
import StorageEngineClient.FormatStorageInputFormat;
import StorageEngineClient.FormatStorageOutputFormat;
import StorageEngineClient.FormatStorageSerDe;
import StorageEngineClient.FormatStorageSplit;


public class TestFormatStorageRecordReader
{
    public static void main(String[] argv) throws IOException
    {
        try
        {           
            String path1 = "se_test/fs/basic/f1/kt/";
            String path2 = "se_test/fs/basic/f2/";
            
            initFormatData();
            
            JobConf conf1 = new JobConf(TestFormatStorageRecordReader.class);   
            JobConf conf2 = new JobConf(TestFormatStorageRecordReader.class);
            
            FormatStorageSerDe serDe1 = initSerDe(conf1);
            FormatStorageSerDe serDe2 = initSerDe(conf2);
            
            StandardStructObjectInspector oi1 = (StandardStructObjectInspector)serDe1.getObjectInspector();
            List<? extends StructField> fieldRefs1 = oi1.getAllStructFieldRefs();
            
            StandardStructObjectInspector oi2 = (StandardStructObjectInspector)serDe2.getObjectInspector();
            List<? extends StructField> fieldRefs2 = oi2.getAllStructFieldRefs();
            
            
            InputFormat inputFormat = new FormatStorageInputFormat();
            RecordReader<WritableComparable, Writable> currRecReader1 = getRecReader(conf1, path1);
            WritableComparable key;
            Writable value;
    
            key = currRecReader1.createKey();
            value = currRecReader1.createValue();
            System.out.println("currRecReader1. output....");
            while (currRecReader1.next(key, value))
            {                
                ((Record)value).show();
                System.out.println("end value.show");
                Object row = serDe1.deserialize((Record)value);
                Record record = (Record) serDe1.serialize(row, oi1);
                record.show();
                
            }
            /*
            RecordReader<WritableComparable, Writable> currRecReader2 = getRecReader(conf2, path2);                
            key = currRecReader2.createKey();
            value = currRecReader2.createValue();
            System.out.println("currRecReader2. output....");
            while (currRecReader2.next(key, value))
            {
                ((Record)value).show();
            }
            
            RecordReader<WritableComparable, Writable> currRecReader3 = getRecReader(conf1, path1);            
            key = currRecReader3.createKey();
            value = currRecReader3.createValue();
            System.out.println("currRecReader3. output....");
            while (currRecReader3.next(key, value))
            {
                ((Record)value).show();
            }
            */
        }
        catch(Exception e)
        {
            e.printStackTrace();
            System.out.println("get exception:"+e.getMessage());
        }
    }
    
    private static void initFormatData() throws Exception
    {        
        FieldMap fieldMap = new FieldMap();        
        fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)(0)));
        fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)(1)));
        fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)(2)));
        fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)(3)));
        fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)(4)));
        fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)(5)));
        fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short)(6)));            
        
        Head head = new Head();
        head.setFieldMap(fieldMap);
        
        String fileName = "se_test/fs/basic/f1/kt/testGetRecordByLineFDReadManyTimes1";
        
        Configuration conf = new Configuration();
        FormatDataFile fd = new FormatDataFile(conf);
        fd.create(fileName, head); 
        
        int recordNum = 10;
        for(int i = 0; i < recordNum; i++)
        {
            Record record = new Record((short) 7);
            
            int j = 0;
            record.addValue(new FieldValue((byte)(1+i), (short)(j*7+0)));
            record.addValue(new FieldValue((short)(2+i), (short)(j*7+1)));
            record.addValue(new FieldValue(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, null, (short)2));
            record.addValue(new FieldValue((long)(4+i), (short)(j*7+3)));
            record.addValue(new FieldValue((float)(5.5+i), (short)(j*7+4)));
            record.addValue(new FieldValue((double)(6.6+i), (short)(j*7+5)));
            record.addValue(new FieldValue("hello konten"+i, (short)(j*7+6)));            
            fd.addRecord(record); 
        }            
        fd.close();        
    }

    static RecordReader<WritableComparable, Writable> getRecReader(JobConf conf, String path) throws IOException
    {
        conf.set("mapred.input.dir", path);
    
        FileInputFormat.setInputPaths(conf, path);
       
        InputFormat inputFormat = new FormatStorageInputFormat();
        InputSplit[] inputSplits = inputFormat.getSplits(conf, 1);
        
        System.out.println("get Splits:"+inputSplits.length);
    
        return inputFormat.getRecordReader(inputSplits[0], conf,  Reporter.NULL);

    }
    
    static FormatStorageSerDe initSerDe(JobConf conf) throws SerDeException
    {               
        FormatStorageSerDe serDe = new FormatStorageSerDe();
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
        tbl.setProperty(Constants.SERIALIZATION_NULL_FORMAT, "NULL");
        return tbl;
    } 
}
