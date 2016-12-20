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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.parse.HiveParser.fromClause_return;

import ColumnStorage.ColumnProject;
import ColumnStorage.ColumnStorageClient;
import ColumnStorage.ColumnProject.ColumnInfo;
import Comm.ConstVar;
import Comm.SEException;
import Comm.Util;
import FormatStorage.FieldMap;
import FormatStorage.FormatDataFile;
import FormatStorage.Head;
import FormatStorage.FieldMap.Field;
import FormatStorage.Unit.FieldValue;
import FormatStorage.Unit.Record;

import junit.framework.TestCase;


public class ColumnStorageBasicTest extends TestCase
{    
    String prefix = "se_test/cs/"; 
    
    String fullPrefix = "hdfs://172.25.38.253:54310/user/tdwadmin/se_test/cs/";
    String byteFileName = fullPrefix + "Column_Byte";
    String shortFileName = fullPrefix + "Column_Short";
    String intFileName = fullPrefix + "Column_Int";
    String longFileName = fullPrefix + "Column_Long";
    String floatFileName = fullPrefix + "Column_Float";
    String doubleFileName = fullPrefix + "Column_Double";
    String stringFileName = fullPrefix + "Column_String";
    String multiFileNameString = fullPrefix + "Column_Short_Short_Short";
    
    Map<String, Integer> file2idxMap = new HashMap<String, Integer>(10);

    public ColumnStorageBasicTest() throws Exception 
    {
        file2idxMap.put(byteFileName, 0);
        file2idxMap.put(shortFileName, 1);
        file2idxMap.put(intFileName, 2);
        file2idxMap.put(longFileName, 3);
        file2idxMap.put(floatFileName, 4);
        file2idxMap.put(doubleFileName, 5);
        file2idxMap.put(stringFileName, 6);
    }
    
   
    public void createAllSingleProject(FileSystem fs) throws Exception
    {
        Configuration conf = new Configuration();
        conf.setInt("dfs.replication", 1);;
        
        FormatDataFile[] fd = new FormatDataFile[7]; 
        for(int i = 0; i < 7; i++)
        {
            fd[i] = new FormatDataFile(conf);
        }
        
        FieldMap fieldMap = new FieldMap();
        fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
        
        Head head = new Head();
        head.setFieldMap(fieldMap);       
        fd[0].create(byteFileName, head);
        
        fieldMap = new FieldMap();
        fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
        
        head = new Head();
        head.setFieldMap(fieldMap);       
        fd[1].create(shortFileName, head);
        
        fieldMap = new FieldMap();
        fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
        
        head = new Head();
        head.setFieldMap(fieldMap);       
        fd[2].create(intFileName, head);
                
        fieldMap = new FieldMap();
        fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
        
        head = new Head();
        head.setFieldMap(fieldMap);       
        fd[3].create(longFileName, head);
        
        fieldMap = new FieldMap();
        fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
        
        head = new Head();
        head.setFieldMap(fieldMap);       
        fd[4].create(floatFileName, head);
        
        fieldMap = new FieldMap();
        fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
        
        head = new Head();
        head.setFieldMap(fieldMap);       
        fd[5].create(doubleFileName, head);
        
        fieldMap = new FieldMap();
        fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short)6));
        
        head = new Head();
        head.setFieldMap(fieldMap);       
        fd[6].create(stringFileName, head);
        
        long begin = System.currentTimeMillis();
        int count = 10;
        for(int i = 0; i < count; i ++)
        {
            Record record = new Record(1);
            record.addValue(new FieldValue((byte)i, (short)0));
            fd[0].addRecord(record);
            
            record = new Record(1);
            record.addValue(new FieldValue((short)i, (short)1));
            fd[1].addRecord(record);
            
            record = new Record(1);
            record.addValue(new FieldValue((int)i, (short)2));
            fd[2].addRecord(record);
            
            record = new Record(1);
            record.addValue(new FieldValue((long)i, (short)3));
            fd[3].addRecord(record);
            
            record = new Record(1);
            record.addValue(new FieldValue((float)i, (short)4));
            fd[4].addRecord(record);
            
            record = new Record(1);
            record.addValue(new FieldValue((double)i, (short)5));
            fd[5].addRecord(record);
            
            record = new Record(1);
            record.addValue(new FieldValue("hello konten"+i, (short)6));
            fd[6].addRecord(record);
        }
        
        for(int i = 0; i < 7; i++)
        {
            fd[i].close();
        }
        /*
        createProjectByte(fs);
        createProjectShort(fs);
        createProjectInt(fs);
        createProjectLong(fs);
        createProjectFloat(fs);
        createProjectDouble(fs);
        createProjectString(fs);
        
        */
        long end = System.currentTimeMillis();
        
        System.out.println("createAllProject delay:"+ (end - begin)/1000);
    }
    public void createProjectByte(FileSystem fs) throws Exception
    {
        FieldMap fieldMap = new FieldMap();
        fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
        
        Head head = new Head();
        head.setFieldMap(fieldMap);       
        
        Configuration conf = new Configuration();
        conf.setInt("dfs.replication", 1);;
        
        FormatDataFile fd = new FormatDataFile(conf);
        
        int count = 10;
        for(int i = 0; i < count; i ++)
        {
            Record record = new Record(1);
            record.addValue(new FieldValue((byte)i, (short)0));
            
            fd.addRecord(record);
        }
        
        fd.close();
    }
    
    public void createProjectShort(FileSystem fs) throws Exception
    {
        FieldMap fieldMap = new FieldMap();
        fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
        
        Head head = new Head();
        head.setFieldMap(fieldMap);
        
        String fileName = "Column_Short";
        FormatDataFile fd = new FormatDataFile(new Configuration());
        fd.create(shortFileName, head);
        
        int count = 10;
        for(int i = 0; i < count; i ++)
        {
            Record record = new Record(1);
            record.addValue(new FieldValue((short)i, (short)1));
            
            fd.addRecord(record);
        }
        
        fd.close();
    }
    
    public void createProjectInt(FileSystem fs) throws Exception
    {
        FieldMap fieldMap = new FieldMap();
        fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
        
        Head head = new Head();
        head.setFieldMap(fieldMap);
        
        String fileName = "Column_Int";
        FormatDataFile fd = new FormatDataFile(new Configuration());
        fd.create(intFileName, head);
        
        int count = 10;
        for(int i = 0; i < count; i ++)
        {
            Record record = new Record(1);
            record.addValue(new FieldValue((int)i, (short)2));
            
            fd.addRecord(record);
        }
        
        fd.close();
    }
    
    public void createProjectLong(FileSystem fs) throws Exception
    {
        FieldMap fieldMap = new FieldMap();
        fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
        
        Head head = new Head();
        head.setFieldMap(fieldMap);
        
        String fileName = "Column_Long";
        FormatDataFile fd = new FormatDataFile(new Configuration());
        fd.create(longFileName, head);
        
        int count = 10;
        for(int i = 0; i < count; i ++)
        {
            Record record = new Record(1);
            record.addValue(new FieldValue((long)i, (short)3));
            
            fd.addRecord(record);
        }
        
        fd.close();
    }
    
    public void createProjectFloat(FileSystem fs) throws Exception
    {
        FieldMap fieldMap = new FieldMap();
        fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
        
        Head head = new Head();
        head.setFieldMap(fieldMap);
        
        String fileName = "Column_Float";
        FormatDataFile fd = new FormatDataFile(new Configuration());
        fd.create(floatFileName, head);
        
        int count = 10;
        for(int i = 0; i < count; i ++)
        {
            Record record = new Record(1);
            record.addValue(new FieldValue((float)i, (short)4));
            
            fd.addRecord(record);
        }
        
        fd.close();
    }
    public void createProjectDouble(FileSystem fs) throws Exception
    {
        FieldMap fieldMap = new FieldMap();
        fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
        
        Head head = new Head();
        head.setFieldMap(fieldMap);
        
        String fileName = "Column_Double";
        FormatDataFile fd = new FormatDataFile(new Configuration());
        fd.create(doubleFileName, head);
        
        int count = 10;
        for(int i = 0; i < count; i ++)
        {
            Record record = new Record(1);
            record.addValue(new FieldValue((double)i, (short)5));
            
            fd.addRecord(record);
        }
        
        fd.close();
    }
    public void createProjectString(FileSystem fs) throws Exception
    {
        FieldMap fieldMap = new FieldMap();
        fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short)6));
        
        Head head = new Head();
        head.setFieldMap(fieldMap);
        
        String fileName = "Column_String";
        FormatDataFile fd = new FormatDataFile(new Configuration());
        fd.create(stringFileName, head);
        
        int count = 10;
        for(int i = 0; i < count; i ++)
        {
            Record record = new Record(1);
            record.addValue(new FieldValue(new String("hello konten") + i, (short)6));
            
            fd.addRecord(record);
        }
        
        fd.close();
    }
    
    public void createMultiProject(FileSystem fs) throws Exception
    {
        FieldMap fieldMap = new FieldMap();
        fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)7));
        fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)8));
        fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)9));
        
        Head head = new Head();
        head.setFieldMap(fieldMap);
        
        String fileName = "Column_Short_Short_Short";
        FormatDataFile fd = new FormatDataFile(new Configuration());
        fd.create(multiFileNameString, head);
        
        int count = 10;
        for(int i = 0; i < count; i ++)
        {
            Record record = new Record(3);
            record.addValue(new FieldValue((short)i, (short)7));
            record.addValue(new FieldValue((short)i, (short)8));
            record.addValue(new FieldValue((short)i, (short)9));
            
            fd.addRecord(record);
        }
        
        fd.close();
    }
    
    void checkAllColumnInfo(ArrayList<ColumnInfo> infos)
    {
        if(infos.size() != 7)
        {
            fail("error infos.size:"+infos.size());
        }
        
        for(int i = 0; i < infos.size(); i++)
        {
            ColumnInfo info = infos.get(i);            
            
            if(info.beginLine != 0) 
            {
            }
            if(info.endLine != 10)
            {
            }
            if(info.idxs.size() != 1)
            {
                fail("error idxs.size:"+info.idxs.size());
            }
            if(info.name == null)
            {
                fail("null name");
            }
            
            String file = info.name;
           
            int idx = file2idxMap.get(file);
            Iterator<Short> iterator = info.idxs.iterator();
            while(iterator.hasNext())
            {
                short idx2 = iterator.next();
                if(idx2 != idx)
                {
                    fail("error idx:"+idx2+", file:"+file);
                }
            }
        }
    }
  
    
    public void testLoadNaviFromHead()
    {
        try
        {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            Path path = new Path(prefix);
            
            fs.delete(path, true);            
            createAllSingleProject(fs);
            
            ColumnProject cp = new ColumnProject(conf);
            cp.loadColmnInfoFromHeadInfo(fs, path);
            
            checkAllColumnInfo(cp.infos());
        }
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
    }
       
    /*
    public void testSaveNavigator()
    {
        try
        {
            Configuration conf = new Configuration();
            ColumnProject cp = new ColumnProject(conf);            
            
            FileSystem fs = FileSystem.get(conf);                        
            
            Path dataPath = new Path(prefix);
            cp.loadColmnInfoFromHeadInfo(fs, dataPath);
            
            String navigator = prefix + "navigator";
            Path navifilePath = new Path(navigator);
            cp.saveNavigator(fs, navifilePath);
            
            FSDataInputStream in = fs.open(navifilePath);
            int magic = in.readInt();
            if(magic != ConstVar.NaviMagic)
            {
                fail("error navi magic:"+magic);
            }
            short num = in.readShort();
            if(num != 7)
            {
                fail("error num:"+num);
            }      
            
            ArrayList<ColumnInfo> infos = new ArrayList<ColumnInfo>(10);
            for(int i = 0; i < num; i++)
            {
                ColumnInfo info = new ColumnInfo();
                short fileLen = in.readShort();
                if(fileLen == 0)
                {
                    fail("error fileLen:"+fileLen);
                }
                byte[] buf = new byte[fileLen];
                in.read(buf, 0, fileLen);
                
                info.name = new String(buf);
                
                short num2 = in.readShort();
                for(int j = 0; j < num2; j++)
                {                    
                    info.idxs.add(in.readShort());
                }
                
                info.beginKey = in.readInt();
                info.endKey = in.readInt();
                info.beginLine = in.readInt();
                info.endLine = in.readInt();                
                
                infos.add(info);
            }
            
            checkAllColumnInfo(infos);
        }
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
    }        
    
    public void testLoadNaviInfoFromNaviFile()
    {
        try
        {
            Configuration conf = new Configuration();
            ColumnProject cp = new ColumnProject(conf);           
            
            FileSystem fs = FileSystem.get(conf);  
            
            String navigator = prefix + "navigator";
            Path path = new Path(navigator);
            
            cp.loadColmnInfoFromNavigator(fs, path);
            checkAllColumnInfo(cp.infos());
        }
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
    }
    
    public void testInvaildNaviFile()
    {
        try
        { 
            String navigator = prefix + "invalidNavigator";
            Path path = new Path(navigator);
            
            Configuration conf = new Configuration();            
            FileSystem fs = FileSystem.get(conf);
                        
            FSDataOutputStream out = fs.create(path);
            String str = "hello konten";
            out.write(str.getBytes());
            out.close();
            
            ColumnProject cp = new ColumnProject(conf);
            cp.loadColmnInfoFromNavigator(fs, path);
            
            fail("error, should get exception");
        }
        catch(SEException.ErrorFileFormat e)
        {
        }
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
    }
    */
   
    public void testGetFileNameByFieldIndex()
    {
        try
        { 
            String navigator = prefix;
            Path path = new Path(navigator);  
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
                        
            ColumnProject cp = new ColumnProject(conf);
            cp.loadColmnInfoFromHeadInfo(fs, path);
            
            ArrayList<String> fileList = null;
            fileList = cp.getFileNameByIndex(null);
            if(fileList != null)
            {
                fail("should null");
            }
            
            ArrayList<Short> idxs = new ArrayList<Short>(10);
            idxs.add((short)10);
            fileList = cp.getFileNameByIndex(idxs);
            if(fileList != null)
            {
                fail("should null");
            }
            
            idxs.clear();
            idxs.add((short)0);
            fileList = cp.getFileNameByIndex(idxs);
            if(fileList == null)
            {
                fail("should not null");
            }
            if(fileList.size() != 1)
            {
                fail("error fileList:"+fileList.size());
            }
            if(!fileList.get(0).equals(byteFileName))
            {
                fail("error file name:"+fileList.get(0));
            }
            
            idxs.clear();
            idxs.add((short)0);
            idxs.add((short)5);
            fileList = cp.getFileNameByIndex(idxs);
            if(fileList == null)
            {
                fail("should not null");
            }
            if(fileList.size() != 2)
            {
                fail("error fileList:"+fileList.size());
            }
            if(!fileList.get(0).equals(byteFileName))
            {
                fail("error file name1:"+fileList.get(0));
            }
            if(!fileList.get(1).equals(doubleFileName))
            {
                fail("error file name2:"+fileList.get(1));
            }            
            
            idxs.clear();
            idxs.add((short)0);
            idxs.add((short)5);
            idxs.add((short)10);
            fileList = cp.getFileNameByIndex(idxs);
            if(fileList != null)
            {
                fail("should null");
            }   
        }       
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
    }
    
    public void testConstructorNullParamter()
    {
        try
        { 
            String navigator = prefix + "navigator";
            Path path = new Path(navigator);  
            Configuration conf = new Configuration();
                        
            ArrayList<Short> idxs = new ArrayList<Short>(10);
            
            try
            {            
                ColumnStorageClient client = new ColumnStorageClient(null,idxs, conf);
                fail("error should get exception");
            }
            catch(SEException.InvalidParameterException e)
            {
                
            }
            
            try
            {            
                ColumnStorageClient client = new ColumnStorageClient(path,null, conf);
                fail("error should get exception");
            }
            catch(SEException.InvalidParameterException e)
            {
                
            }
            
            try
            {            
                ColumnStorageClient client = new ColumnStorageClient(path,idxs, null);
                fail("error should get exception");
            }
            catch(SEException.InvalidParameterException e)
            {
                
            }
        }       
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
    }
    
    public void testConstructorFieldNoExist()
    {
        try
        {     
            Configuration conf = new Configuration();
            Path path = new Path(prefix);              
            
            FileSystem fs = FileSystem.get(conf);
            fs.delete(path, true);
            
            createAllSingleProject(fs);
            createMultiProject(fs);
            
            ArrayList<Short> idxs = new ArrayList<Short>(10);            
            idxs.add((short)10);        
            ColumnStorageClient client = new ColumnStorageClient(path,idxs, conf);
         
            fail("should get exception");                     
        }       
        catch(SEException.InvalidParameterException e)
        {
            
        }
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
    }
    
    public void testConstructorFieldInSameFile()
    {
        try
        {     
            Configuration conf = new Configuration();
            Path path = new Path(prefix);              
            FileSystem fs = FileSystem.get(conf);
            
            ArrayList<Short> idxs = new ArrayList<Short>(10);
            idxs.add((short)7);
            idxs.add((short)9);
            
            ColumnStorageClient client = new ColumnStorageClient(path,idxs, conf);
         
            if(client.cp == null)
            {
                fail("cp null");
            }
            
            if(client.list.size() != 1)
            {
                fail("error list size:"+client.list.size());
            }
            
            if(!client.list.get(0).equals(multiFileNameString))
            {
                fail("error filename:"+client.list.get(0));
            }            
        }
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
    }
    
    public void testConstructorFieldInDiffFile()
    {
        try
        {     
            Configuration conf = new Configuration();
            Path path = new Path(prefix);              
            FileSystem fs = FileSystem.get(conf);           
           
            ArrayList<Short> idxs = new ArrayList<Short>(10);
            idxs.add((short)0);
            idxs.add((short)7);
            idxs.add((short)4);
            
            ColumnStorageClient client = new ColumnStorageClient(path,idxs, conf);
         
            if(client.cp == null)
            {
                fail("cp null");
            }
            if(client.fds.length  != 3)
            {
                fail("error fds.len:"+client.fds.length);
            }
            for(int i = 0; i < client.fds.length; i++)
            {
                if(client.fds[i] == null)
                {
                    fail("null fd:"+i);
                }
            }
            if(client.list.size() != 3)
            {
                fail("error list size:"+client.list.size());
            }
            
            if(!client.list.get(0).equals(byteFileName))
            {
                fail("error filename:"+client.list.get(0));
            }            
            if(!client.list.get(1).equals(multiFileNameString))
            {
                fail("error filename:"+client.list.get(1));
            }
            if(!client.list.get(2).equals(floatFileName))
            {
                fail("error filename:"+client.list.get(2));
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
    }
  
    public void testGetRecordByLine()
    {
        try
        {     
            Configuration conf = new Configuration();
            Path path = new Path(prefix);              
            FileSystem fs = FileSystem.get(conf);
            
            ArrayList<Short> idxs = new ArrayList<Short>(10);
            idxs.add((short)0); 
            idxs.add((short)7); 
            idxs.add((short)4); 
            
            ColumnStorageClient client = new ColumnStorageClient(path,idxs, conf);
         
            Record record = client.getRecordByLine(-1);
            if(record != null)
            {
                fail("should return null record 1");
            }
            
            record = client.getRecordByLine(10);
            if(record != null)
            {
                fail("should return null record 2");
            }
            
            for(int i = 0; i < 10; i++)
            {
                record = client.getRecordByLine(i);
                if(record == null)
                {
                    fail("should not return null record");
                }
                
                if(record.fieldValues().size() != 5)
                {
                    fail("error field num:"+record.fieldValues().size());
                }
                
                record.show();
                
                judgeNofixRecord(record, i);      
                
            }            
        }
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
    }
    
    public void judgeNofixRecord(Record record, int line)
    {
        short index = 0;
        byte type = record.getType(index);
        int len = record.getLen(index);
        short idx = record.getIndex(index);
        if(len != ConstVar.Sizeof_Byte)
        {
            fail("error len:"+len);
        }
        if(type != ConstVar.FieldType_Byte)
        {
            fail("error fieldType:"+type);
        }
        if(idx != 0)
        {
            fail("error idx:"+idx);
        }
        byte[] buf = record.getValue(index);
        if(buf == null)
        {
            fail("error null buffer");
        }
        
        byte bv = buf[0];
        if(bv != (byte)line)
        {
            fail("error bv:"+bv+"line:"+line);
        }       
        
        index = 1;                                 
        type = record.getType(index);
        len = record.getLen(index);
        idx = record.getIndex(index);
        if(len != ConstVar.Sizeof_Short)
        {
            fail("error len:"+len);
        }
        if(type != ConstVar.FieldType_Short)
        {
            fail("error fieldType:"+type);
        }
        if(idx != 7)
        {
            fail("error idx:"+idx);
        }            
        buf = record.getValue(index);
        if(buf == null)
        {
            fail("error null buffer");
        }
        
        short sval = Util.bytes2short(buf, 0, ConstVar.Sizeof_Short);
        if(sval != (short)line)
        {
            fail("error value:"+sval+"line:"+line);
        }   
        
        index = 2; 
        type = record.getType(index);
        len = record.getLen(index);
        idx = record.getIndex(index);
        if(len != ConstVar.Sizeof_Short)
        {
            fail("error len:"+len);
        }
        if(type != ConstVar.FieldType_Short)
        {
            fail("error fieldType:"+type);
        }
        if(idx != 8)
        {
            fail("error idx:"+idx);
        }            
        buf = record.getValue(index);
        if(buf == null)
        {
            fail("error null buffer");
        }            
        sval = Util.bytes2short(buf, 0, ConstVar.Sizeof_Short);
        if(sval != (short)line)
        {
            fail("error value:"+sval+"line:"+line);
        }     
        
        index = 3; 
        type = record.getType(index);
        len = record.getLen(index);
        idx = record.getIndex(index);
        if(len != ConstVar.Sizeof_Short)
        {
            fail("error len:"+len);
        }
        if(type != ConstVar.FieldType_Short)
        {
            fail("error fieldType:"+type);
        }
        if(idx != 9)
        {
            fail("error idx:"+idx);
        }            
        buf = record.getValue(index);
        if(buf == null)
        {
            fail("error null buffer");
        }            
        sval = Util.bytes2short(buf, 0, ConstVar.Sizeof_Short);
        if(sval != (short)line)
        {
            fail("error value:"+sval+"line:"+line);
        }   
        
        index = 4;
        type = record.getType(index);
        len = record.getLen(index);
        idx = record.getIndex(index);  
        if(len != ConstVar.Sizeof_Float)
        {
            fail("error len:"+len);
        }
        if(type != ConstVar.FieldType_Float)
        {
            fail("error fieldType:"+type);
        }
        if(idx != 4)
        {
            fail("error idx:"+idx);
        }
        buf = record.getValue(index);            
        if(buf == null)
        {
            fail("value should not null");
        }
        float fv = Util.bytes2float(buf, 0);
        if(fv != (float)line)
        {
            fail("error value:"+fv+"line:"+line);
        }           
    }
   
}
