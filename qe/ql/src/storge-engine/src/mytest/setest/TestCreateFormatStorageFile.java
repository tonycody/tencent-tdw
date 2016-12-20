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

import org.apache.hadoop.conf.Configuration;

import Comm.ConstVar;
import FormatStorage.FieldMap;
import FormatStorage.FormatDataFile;
import FormatStorage.Head;
import FormatStorage.FieldMap.Field;
import FormatStorage.Unit.FieldValue;
import FormatStorage.Unit.Record;

public class TestCreateFormatStorageFile
{
    public static void main(String[] argv)
    {
        if(argv.length != 2)
        {
            System.out.println("TestCreateFormatStorageFile count compress");
            return;
        }
        
        byte compress = Byte.valueOf(argv[1]);
        
        try
        { 
            Configuration conf = new Configuration();
        
            FieldMap fieldMap = new FieldMap();
            fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)0));
            
            Head head = new Head();
            head.setCompress(compress);
            head.setFieldMap(fieldMap);
            
            String fileName = "MR_input/TestCreateFormatStorageFile";
           
            FormatDataFile fd = new FormatDataFile(conf);
            
            fd.create(fileName, head); 
            
            FieldMap fieldMap2 = new FieldMap();
            fieldMap2.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)0));
            Head head2 = new Head();
            head2.setCompress(compress);
            head2.setFieldMap(fieldMap2);            
            String fileName2 = "MR_input/TestCreateFormatStorageFile_2";
            FormatDataFile fd2 = new FormatDataFile(conf);
            
            fd2.create(fileName2, head2); 
            
            long begin = System.currentTimeMillis();
            int count = Integer.valueOf(argv[0]);
            for(int i = 0; i < count; i++)
            {
                Record record = new Record((short) 1);
                record.addValue(new FieldValue((long)(4+i), (short)0));
                
                fd.addRecord(record);  
                
                Record record2 = new Record((short) 1);            
                record2.addValue(new FieldValue((long)(100+i), (short)0));
                fd2.addRecord(record2);
            }
            
            fd.close();      
            fd2.close();
            
            long end = System.currentTimeMillis();
            String string = "write " + count + " record over, delay: " + ((end - begin)/1000) + " s . file size:"+ fd.getFileLen() +"\n" ;
            System.out.println(string);
        }
        catch(Exception e)
        {
            e.printStackTrace();        
            System.out.println("get exception:"+e.getMessage());
        }
    }
}
