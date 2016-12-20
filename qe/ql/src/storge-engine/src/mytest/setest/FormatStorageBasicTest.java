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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.mapred.JobConf;

import Comm.Util;
import Comm.ConstVar;
import Comm.SEException;
import FormatStorage.BlockIndex;
import FormatStorage.FieldMap;
import FormatStorage.FormatDataFile;
import FormatStorage.Head;
import FormatStorage.Segment;
import FormatStorage.SegmentIndex;
import FormatStorage.Unit;
import FormatStorage.UnitIndex;
import FormatStorage.BlockIndex.IndexInfo;

import FormatStorage.FieldMap.Field;
import FormatStorage.Unit.DataChunk;
import FormatStorage.Unit.FieldValue;
import FormatStorage.Unit.FixedBitSet;
import FormatStorage.Unit.Record;

import junit.framework.TestCase;


public class FormatStorageBasicTest extends TestCase
{   
 
        String prefix = "se_test/fs/basic/"; 
        int full7chunkLen = 42;
        int full6chunkLen = 28;
        public void testGetFieldLen()
        {
            try
            {
                if(Util.type2len(ConstVar.FieldType_Byte) != ConstVar.Sizeof_Byte)
                {
                    fail("byte len fail");
                }
                
                if(Util.type2len(ConstVar.FieldType_Char) != ConstVar.Sizeof_Char)
                {
                    fail("byte len fail");
                }
                
                if(Util.type2len(ConstVar.FieldType_Short) != ConstVar.Sizeof_Short)
                {
                    fail("byte len fail");
                }
                
                if(Util.type2len(ConstVar.FieldType_Int) != ConstVar.Sizeof_Int)
                {
                    fail("byte len fail");
                }
                
                if(Util.type2len(ConstVar.FieldType_Long) != ConstVar.Sizeof_Long)
                {
                    fail("byte len fail");
                }
                
                if(Util.type2len(ConstVar.FieldType_Float) != ConstVar.Sizeof_Float)
                {
                    fail("byte len fail");
                }
                
                if(Util.type2len(ConstVar.FieldType_Double) != ConstVar.Sizeof_Double)
                {
                    fail("byte len fail");
                }
                try
                {
                    if(Util.type2len(ConstVar.FieldType_String) != 0)
                    {
                        fail("byte len fail");
                    }
                }
                catch(Exception e)
                {
                    
                }
                
                try
                {
                    if(Util.type2len(ConstVar.FieldType_User) != 0)
                    {
                        fail("byte len fail");
                    }
                }
                catch(Exception e)
                {
                    
                }
            }
            catch (Exception e)
            {
                fail(e.getMessage());
            }       
        }
        
        
        public void testFixedBitSet()
        {
            try
            {
                FixedBitSet fixedBitSet = new FixedBitSet(2);
                if(fixedBitSet.size() != 1)
                {
                    fail("error size:"+fixedBitSet.size());
                }
                if(fixedBitSet.length() != 8)
                {
                    fail("error len:"+fixedBitSet.length());
                }
                if(fixedBitSet.needByte(2) != 1)
                {
                    fail("error needByte:"+fixedBitSet.needByte(2));
                }
                if(fixedBitSet.bytes().length != 1)
                {
                    fail("error bytes.len:"+fixedBitSet.bytes().length);
                }
                
                fixedBitSet.set(1);
                
                if(fixedBitSet.get(0))
                {
                    fail("0 should false");
                }
                if(!fixedBitSet.get(1))
                {
                    fail("1 should true");
                }
                
                fixedBitSet.set(7);
                if(!fixedBitSet.get(7))
                {
                    fail("7 should true");                        
                }
                
                try
                {
                    fixedBitSet.set(10);
                    fail("should exception");
                }
                catch(Exception e)
                {
                    
                }
                
                fixedBitSet.needByte(20);
                if(fixedBitSet.size() != 3)
                {
                    fail("error size:"+fixedBitSet.size());
                }
                if(fixedBitSet.length() != 24)
                {
                    fail("error len:"+fixedBitSet.length());
                }
                if(fixedBitSet.bytes().length != 3)
                {
                    fail("error bytes.len:"+fixedBitSet.bytes().length);
                }
                
                
                fixedBitSet.needByte(5);
                if(fixedBitSet.size() != 3)
                {
                    fail("error size:"+fixedBitSet.size());
                }
                if(fixedBitSet.length() != 24)
                {
                    fail("error len:"+fixedBitSet.length());
                }
                if(fixedBitSet.bytes().length != 3)
                {
                    fail("error bytes.len:"+fixedBitSet.bytes().length);
                }
                
                fixedBitSet.clear();
                
                fixedBitSet.set(4);
                if(!fixedBitSet.get(4))
                {
                    fail("error 4 should true");
                }
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get exception:"+e.getMessage());
            }
        }
        
        public void testBoolean2bytes()
        {
            boolean v = false;
            byte[] bytes = Util.boolean2bytes(v);
            
            if(bytes.length != 1)
            {
                fail("boolean2bytes fail, len != 1");
            }
            
            if(bytes[0] != 0)
            {
                fail("boolean2bytes fail, value != 0");         
            }
            
            boolean v2 = Util.bytes2boolean(bytes);
            if(v2)
            {
                fail("bytes2boolean fail, value != false");
            }
        }
        
        public void testShort2bytes()
        {
            short v = 17;
            byte[] bytes = new byte[ConstVar.Sizeof_Short];     
            Util.short2bytes(bytes, v);
            
            short v2 = Util.bytes2short(bytes, 0, ConstVar.Sizeof_Short);
            
            if(v2 != v)
            {
                fail("bytes2short fail, s:"+v+"s2:"+v2);
            }           
        }
        
        public void testInt2bytes()
        {
            int v = 17;
            byte[] bytes = new byte[ConstVar.Sizeof_Int];       
            Util.int2bytes(bytes, v);
            
            int v2 = Util.bytes2int(bytes, 0, ConstVar.Sizeof_Int);
            
            if(v2 != v)
            {
                fail("bytes2int fail, s:"+v+"s2:"+v2);
            }           
        }
        
        public void testLong2bytes()
        {
            int v = 17;
            byte[] bytes = new byte[ConstVar.Sizeof_Long];      
            Util.long2bytes(bytes, v);
            
            long v2 = Util.bytes2long(bytes, 0, ConstVar.Sizeof_Long);
            
            if(v2 != v)
            {
                fail("bytes2long fail, s:"+v+"s2:"+v2);
            }           
        }
        
        public void testFloat2bytes()
        {
            float v = (float) 17.12;
            byte[] bytes = new byte[ConstVar.Sizeof_Float];     
            Util.float2bytes(bytes, v);
            
            float v2 = Util.bytes2float(bytes, 0);
            
            if(v2 != v)
            {
                fail("bytes2float fail, s:"+v+"s2:"+v2);
            }           
        }
        
        public void testDouble2bytes()
        {
            double v = 17.12;
            byte[] bytes = new byte[ConstVar.Sizeof_Double];        
            Util.double2bytes(bytes, v);
            
            double v2 = Util.bytes2double(bytes, 0);
            
            if(v2 != v)
            {
                fail("bytes2double fail, s:"+v+"s2:"+v2);
            }           
        }
        
        public void testGetVarType()
        {       
            if(Util.isVarType(ConstVar.FieldType_Unknown))
            {
                fail("isVarType wrong type1");
            }
            if(!Util.isVarType(ConstVar.FieldType_User))
            {
                fail("isVarType wrong type2");
            }
            if(Util.isVarType(ConstVar.FieldType_Boolean))
            {
                fail("isVarType wrong type3");
            }       
            if(Util.isVarType(ConstVar.FieldType_Char))
            {
                fail("isVarType wrong type4");
            }       
            if(Util.isVarType(ConstVar.FieldType_Byte))
            {
                fail("isVarType wrong type5");
            }
            if(Util.isVarType(ConstVar.FieldType_Short))
            {
                fail("isVarType wrong type6");
            }
            if(Util.isVarType(ConstVar.FieldType_Int))
            {
                fail("isVarType wrong type7");
            }
            if(Util.isVarType(ConstVar.FieldType_Float))
            {
                fail("isVarType wrong type8");
            }
            if(Util.isVarType(ConstVar.FieldType_Long))
            {
                fail("isVarType wrong type9");
            }
            if(Util.isVarType(ConstVar.FieldType_Double))
            {
                fail("isVarType wrong type10");
            }
            if(!Util.isVarType(ConstVar.FieldType_String))
            {
                fail("isVarType wrong type11");
            }
        }
        
        public void testGetValidType()
        {
            if(Util.isValidType(ConstVar.FieldType_Unknown))
            {
                fail("isValidType wrong type1");
            }
            if(Util.isValidType(ConstVar.FieldType_User))
            {
                fail("isValidType wrong type2");
            }
            if(!Util.isValidType(ConstVar.FieldType_Boolean))
            {
                fail("isValidType wrong type3");
            }       
            if(!Util.isValidType(ConstVar.FieldType_Char))
            {
                fail("isValidType wrong type4");
            }       
            if(!Util.isValidType(ConstVar.FieldType_Byte))
            {
                fail("isValidType wrong type5");
            }
            if(!Util.isValidType(ConstVar.FieldType_Short))
            {
                fail("isValidType wrong type6");
            }
            if(!Util.isValidType(ConstVar.FieldType_Int))
            {
                fail("isValidType wrong type7");
            }
            if(!Util.isValidType(ConstVar.FieldType_Float))
            {
                fail("isValidType wrong type8");
            }
            if(!Util.isValidType(ConstVar.FieldType_Long))
            {
                fail("isValidType wrong type9");
            }
            if(!Util.isValidType(ConstVar.FieldType_Double))
            {
                fail("isValidType wrong type10");
            }
            if(!Util.isValidType(ConstVar.FieldType_String))
            {
                fail("isValidType wrong type11");
            }
        }
        public void testGetFieldValue()
        {
            try
            {
                byte[] bb = new byte[1];
                bb[0] = 11;
                byte b2 = (byte) Util.getValue(ConstVar.FieldType_Byte, bb);
                if(b2 != 11)
                {
                    fail("getValue fail, 11");
                }
            }   
            catch(Exception e)
            {
                fail(e.getMessage());
            }
            
            try
            {
                byte[] bs = new byte[ConstVar.Sizeof_Short];
                short s = 12;
                Util.short2bytes(bs, s);            
                short s2 = (short) Util.getValue(ConstVar.FieldType_Short, bs);
                if(s2 != 12)
                {
                    fail("getValue fail, 12");
                }
            }   
            catch(Exception e)
            {
                fail(e.getMessage());
            }
            
            try
            {
                byte[] is = new byte[ConstVar.Sizeof_Int];
                int i = 13;
                Util.int2bytes(is, i);          
                int i2 = (int) Util.getValue(ConstVar.FieldType_Int, is);
                if(i2 != 13)
                {
                    fail("getValue fail, 13");
                }           
            }   
            catch(Exception e)
            {
                fail(e.getMessage());
            }
            
            try
            {
                byte[] ls = new byte[ConstVar.Sizeof_Long];
                long l = 14;
                Util.long2bytes(ls, l);         
                long l2 = Util.getValue(ConstVar.FieldType_Long, ls);
                if(l2 != 14)
                {
                    fail("getValue fail, 14");
                }           
            }   
            catch(Exception e)
            {
                fail(e.getMessage());
            }
            
            
            try
            {
                byte[] fs = new byte[ConstVar.Sizeof_Float];
                            
                long v = Util.getValue(ConstVar.FieldType_String, fs);
                if(v != 0)
                {
                    fail("getValue fail, 17");
                }
                        
            }   
            catch(Exception e)
            {       
                fail(e.getMessage());
            }
        }
        
    

        public void testAddLineIndexInfo()
        {
            BlockIndex lineIndex = new BlockIndex();            
            IndexInfo indexInfo = new IndexInfo();
            
            lineIndex.addIndexInfo(indexInfo, ConstVar.LineMode);
            
            if(lineIndex.lineIndexInfos().size() != 1)
            {
                fail("add line index fail, size != 1");
            }
            
            if(lineIndex.len() != ConstVar.LineIndexRecordLen)
            {
                fail("add line index fail, len:"+lineIndex.len()+":"+ConstVar.LineIndexRecordLen);
            }               
        }
        
        public void testAddKeyIndexInfo()
        {
            BlockIndex keyIndex = new BlockIndex();
            IndexInfo indexInfo = new IndexInfo();
            
            keyIndex.addIndexInfo(indexInfo, ConstVar.KeyMode);
            if(keyIndex.keyIndexInfos().size() != 1)
            {
                fail("add key index fail, size != 1");
            }
            
            if(keyIndex.len() != ConstVar.KeyIndexRecordLen)
            {
                fail("add key index fail, len:"+keyIndex.len()+":"+ConstVar.LineIndexRecordLen);
            }       
        }
        
        public void testGetIndexInfoNull()
        {
            BlockIndex index = new BlockIndex();
            
            try
            {
                IndexInfo info = index.getIndexInfo(0);
                if(info != null)
                {
                    fail("should get null");
                }
            }
            catch(IOException e)
            {
                fail("should not exception");
            }
            
        }
        
        public void testGetIndexInfo()
        {
            BlockIndex index1 = new BlockIndex();
            IndexInfo indexInfo = new IndexInfo();
            
            index1.addIndexInfo(indexInfo, ConstVar.LineMode);
            try
            {
                IndexInfo info = index1.getIndexInfo(0);
                if(info == null)
                {
                    fail("should not null");
                }
            }
            catch(IOException e)
            {
                fail("should not exception");
            }
            
            BlockIndex index2 = new BlockIndex();   
            
            index2.addIndexInfo(indexInfo, ConstVar.KeyMode);
            try
            {
                IndexInfo info = index2.getIndexInfo(0);
                if(info != null)
                {
                    fail("should get null");
                }
            }
            catch(IOException e)
            {
                fail("should not exception:"+e.getMessage());
            }           
        }
        
        public void testGetIndexInfoByLine()
        {
            BlockIndex index1 = new BlockIndex();
            IndexInfo indexInfo = new IndexInfo();
            indexInfo.beginLine = 0;
            indexInfo.endLine = 10;
            indexInfo.offset = 11;
            indexInfo.len = 12;
            indexInfo.idx = 13;
            
            index1.addIndexInfo(indexInfo, ConstVar.LineMode);
            try
            {
                IndexInfo info = index1.getIndexInfoByLine(0);
                if(info == null)
                {
                    fail("testGetIndexInfoByLine0 should not null");
                }
                else 
                {
                    if(info.offset != 11)
                    {
                        fail("offset fail");
                    }
                    if(info.len != 12)
                    {
                        fail("len fail");
                    }
                    if(info.idx != 13)
                    {
                        fail("idx fail");
                    }
                }
                
                info = index1.getIndexInfoByLine(8);
                if(info == null)
                {
                    fail("testGetIndexInfoByLine8 should not null");
                }
                else 
                {
                    if(info.offset != 11)
                    {
                        fail("offset fail");
                    }
                    if(info.len != 12)
                    {
                        fail("len fail");
                    }
                    if(info.idx != 13)
                    {
                        fail("idx fail");
                    }
                }
                
                info = index1.getIndexInfoByLine(10);
                if(info != null)
                {
                    fail("testGetIndexInfoByLine10 should  null");
                }
            }
            catch(Exception e)
            {
                fail("testGetIndexInfoByLine should not exception");
            }           
        }
        
        public void testGetIndexInfoByKey() 
        {
            BlockIndex index = new BlockIndex();
            IndexInfo indexInfo = new IndexInfo();
            indexInfo.beginKey = 0;
            indexInfo.endKey = 10;
            
            index.addIndexInfo(indexInfo, ConstVar.KeyMode);
            try
            {
                IndexInfo[] info = index.getIndexInfoByKey(0);
                if(info != null)
                {
                    fail("testGetIndexInfoByKey0 should null");
                }
                
            }
            catch(SEException.InnerException e)
            {
                
            }
            catch(Exception e)
            {
                fail("testGetIndexInfoByKey0 should not others exception");
            }   
            
            try
            {
                indexInfo.beginLine = 20;
                indexInfo.endKey = 30;
                indexInfo.offset = 11;
                indexInfo.len = 12;
                indexInfo.idx = 13;
                index.addIndexInfo(indexInfo, ConstVar.LineMode);
                
                IndexInfo[] info = index.getIndexInfoByKey(0);
                if(info == null)
                {
                    fail("getIndexInfoByKey0 should not null");
                }
                else if(info.length != 1)
                {
                    fail("getIndexInfoByKey0 should return 1 item");
                }
                else
                {
                    if(info[0].offset != 11)
                    {
                        fail("offset fail");
                    }
                    if(info[0].len != 12)
                    {
                        fail("len fail");
                    }
                    if(info[0].idx != 13)
                    {
                        fail("idx fail");
                    }
                }
                
                info = index.getIndexInfoByKey(10);
                if(info == null)
                {
                    fail("getIndexInfoByKey10 should not null");
                }
                else if(info.length != 1)
                {
                    fail("getIndexInfoByKey10 should return 1 item");
                }
                else
                {
                    if(info[0].offset != 11)
                    {
                        fail("offset fail");
                    }
                    if(info[0].len != 12)
                    {
                        fail("len fail");
                    }
                    if(info[0].idx != 13)
                    {
                        fail("idx fail");
                    }
                }
            }
            
            catch(Exception e)
            {
                fail("getIndexInfoByKey:"+e.getMessage());
            }
                
            try
            {
                IndexInfo indexInfo2 = new IndexInfo();
                indexInfo2.beginKey = 1;
                indexInfo2.endKey = 11;             
                indexInfo2.beginLine = 40;
                indexInfo2.endLine = 50;
                indexInfo2.offset = 21;
                indexInfo2.len = 22;
                indexInfo2.idx = 23;
                index.addIndexInfo(indexInfo2, ConstVar.KeyMode);
                index.addIndexInfo(indexInfo2, ConstVar.LineMode);
                
                IndexInfo[] info = index.getIndexInfoByKey(8);
                if(info == null)
                {
                    fail("getIndexInfoByKey8 should not null");
                }
                else if(info.length != 2)
                {
                    fail("getIndexInfoByKey10 should return 2 item");
                }
                else
                {
                    if(info[0].offset != 11 || info[1].offset != 21)
                    {
                        fail("offset fail:"+info[0].offset+","+info[1].offset);
                    }
                    if(info[0].len != 12  || info[1].len != 22)
                    {
                        fail("len fail:"+info[0].len+","+info[1].len);
                    }
                    if(info[0].idx != 13 || info[1].idx != 23)
                    {
                        fail("idx fail:"+info[0].idx+","+info[1].idx);
                    }
                }
                
            }
            catch(Exception e)
            {
                fail("testGetIndexInfoByKey8 should not exception");
            }           
        }
        
        public void testGetIndexLen()
        {
            BlockIndex index = new BlockIndex();
            IndexInfo info = new IndexInfo();
            
            index.addIndexInfo(info, ConstVar.LineMode);
            
            if(index.len() != ConstVar.LineIndexRecordLen)
            {
                fail("fail line index.len():"+index.len());
            }
            
            index.addIndexInfo(info, ConstVar.KeyMode);
            if(index.len() != ConstVar.LineIndexRecordLen + ConstVar.KeyIndexRecordLen)
            {
                fail("fail line&key index.len():"+index.len());
            }
            
            BlockIndex index2 = new BlockIndex();           
            index2.addIndexInfo(info, ConstVar.KeyMode);
            
            if(index2.len() != ConstVar.KeyIndexRecordLen)
            {
                fail("fail key index.len():"+index2.len());
            }
        }
        
        public void testSetIndexMode()
        {
            try
            {
                BlockIndex index = new BlockIndex();
                if(index.indexMode != ConstVar.LineMode)
                {
                    fail("fail init LineIndexMode");
                }
                
                index.setIndexMode(ConstVar.LineMode);          
                if(index.indexMode != ConstVar.LineMode)
                {
                    fail("fail set LineIndexMode");
                }
                
                index.setIndexMode(ConstVar.KeyMode);           
                if(index.indexMode != ConstVar.KeyMode)
                {
                    fail("fail set KeyIndexMode");
                }
                
                try
                {
                    index.setIndexMode(89);
                }
                catch(SEException.InvalidParameterException e)
                {
                    
                }
                
            }
            catch(Exception e)
            {
                fail("testSetIndexMode should not exception:"+e.getMessage());
            }
        }
        
        public void testAddIndexMode()
        {
            BlockIndex index = new BlockIndex();
                        
            index.addIndexMode(ConstVar.LineMode);
            if((index.indexMode & ConstVar.LineMode) == 0)
            {
                fail("fail addLineMode");
            }
            if((index.indexMode & ConstVar.KeyMode) != 0)
            {
                fail("fail addLineMode");
            }
            
            index.addIndexMode(ConstVar.KeyMode);
            if((index.indexMode & ConstVar.LineMode) == 0)
            {
                fail("fail addKeyMode");
            }
            if((index.indexMode & ConstVar.KeyMode) == 0)
            {
                fail("fail addKeyMode");
            }
        }
        
        public void testPersistentLineIndexInfo() 
        {
            try
            {
                String fileName = prefix + "testPersistentLineIndexInfo";
                Path path = new Path(fileName);
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataOutputStream out = fs.create(path);
                
                IndexInfo info = new IndexInfo();
                info.beginLine = 11;
                info.endLine = 22;
                info.offset = 33;
                info.len = 44;
                info.idx = 55;
                
                info.persistentLineIndexInfo(out);
                out.close();
                
                
                FSDataInputStream in = fs.open(path);
                
                int beginLine = in.readInt();
                int endLine = in.readInt();
                long offset = in.readLong();
                long len = in.readLong();
                int idx = in.readInt();
                in.close();
                
                if(beginLine != 11)                 
                {
                    fail("beginLine fail:"+beginLine);
                }               
                if(endLine != 22)                   
                {
                    fail("endLine fail:"+endLine);
                }
                if(offset != 33)                    
                {
                    fail("offset fail:"+offset);
                }
                if(len != 44)                   
                {
                    fail("len fail:"+len);
                }
                if(idx != 55)                   
                {
                    fail("idx fail:"+idx);
                }
                
            }
            catch(IOException  e)
            {
                fail(e.getMessage());
            }
            
        }
        
        public void testPersistentKeyIndexInfo()
        {
            try
            {
                String fileName = prefix + "testPersistentKeyIndexInfo";
                Path path = new Path(fileName);
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataOutputStream out = fs.create(path);
                
                IndexInfo info = new IndexInfo();
                info.beginKey = 111;
                info.endKey = 222;
                
                info.persistentKeyIndexInfo(out);
                out.close();
                
                FSDataInputStream in = fs.open(path);
                
                int beginKey = in.readInt();
                int endKey = in.readInt();                  
                in.close();
                
                if(beginKey != 111)                 
                {
                    fail("beginKey fail:"+beginKey);
                }               
                if(endKey != 222)                   
                {
                    fail("beginKey fail:"+beginKey);
                }
                
            }
            catch(IOException e)
            {
                fail(e.getMessage());
            }
            
        }
        
        public void testUnpersistentLineIndexInfo()
        {
            try
            {
                String fileName = prefix + "testPersistentLineIndexInfo";
                Path path = new Path(fileName);
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataInputStream in = fs.open(path);
                
                IndexInfo info = new IndexInfo();
                info.unpersistentLineIndexInfo(in);             
                in.close();
                
                if(info.beginLine != 11)                    
                {
                    fail("beginLine fail:"+info.beginLine);
                }               
                if(info.endLine != 22)                  
                {
                    fail("endLine fail:"+info.endLine);
                }
                if(info.offset != 33)                   
                {
                    fail("offset fail:"+info.offset);
                }
                if(info.len != 44)                  
                {
                    fail("len fail:"+info.len);
                }
                if(info.idx != 55)                  
                {
                    fail("idx fail:"+info.idx);
                }
                
                fs.close();
            }
            catch(IOException e)
            {
                fail(e.getMessage());
            }
            
        }
        
        public void testUnpersistentKeyIndexInfo()
        {
            try
            {
                String fileName = prefix + "testPersistentKeyIndexInfo";
                Path path = new Path(fileName);
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataInputStream in = fs.open(path);
                
                IndexInfo info = new IndexInfo();
                info.unpersistentKeyIndexInfo(in);
                in.close();
                
                if(info.beginKey != 111)                    
                {
                    fail("beginKey fail:"+info.beginKey);
                }               
                if(info.endKey != 222)                  
                {
                    fail("beginKey fail:"+info.beginKey);
                }
                
                fs.close();
            }
            catch(IOException e)
            {
                fail(e.getMessage());
            }           
        }
        
        public void testPersistentLineIndex()
        {
            try
            {
                BlockIndex index = new BlockIndex();
                
                for(int i = 0; i < 10; i++)
                {
                    IndexInfo info = new IndexInfo();
                    info.beginLine = i;
                    info.endLine = i + 10;
                    info.offset = i;
                    info.len = i + 20;
                    info.idx = i;
                    
                    index.addIndexInfo(info, ConstVar.LineMode);                
                }
                
                String fileName = prefix + "testPersistentLineIndex";
                Path path = new Path(fileName);
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataOutputStream out = fs.create(path);           
                
                index.persistent(out);
                out.close();
                
                
                FSDataInputStream in = fs.open(path);
                for(int i = 0; i < 10; i++)
                {
                    int beginLine = in.readInt();
                    int endLine = in.readInt();
                    long offset = in.readLong();
                    long len = in.readLong();
                    int idx = in.readInt();                 
                    
                    if(beginLine != i)                  
                    {
                        fail("beginLine fail:"+beginLine);
                    }               
                    if(endLine != i + 10)                   
                    {
                        fail("endLine fail:"+endLine);
                    }
                    if(offset != i)                 
                    {
                        fail("offset fail:"+offset);
                    }
                    if(len != i + 20)                   
                    {
                        fail("len fail:"+len);
                    }
                    if(idx != i)                    
                    {
                        fail("idx fail:"+idx);
                    }                   
                }
            }
            catch(IOException e)
            {
                fail(e.getMessage());
            }           
        }
        
        public void testPersistentKeyIndex()
        {
            try
            {
                BlockIndex index = new BlockIndex();
                
                for(int i = 0; i < 10; i++)
                {
                    IndexInfo info = new IndexInfo();
                    info.beginKey = i;
                    info.endKey = i + 10;
                    
                    index.addIndexInfo(info, ConstVar.KeyMode);             
                }
                
                String fileName = prefix + "testPersistentKeyIndex";
                Path path = new Path(fileName);
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataOutputStream out = fs.create(path);           
                
                index.persistent(out);
                out.close();
                
                
                FSDataInputStream in = fs.open(path);
                for(int i = 0; i < 10; i++)
                {
                    int beginKey = in.readInt();
                    int endKey = in.readInt();
                    
                    if(beginKey != i)                   
                    {
                        fail("beginKey fail:"+beginKey);
                    }               
                    if(endKey != i + 10)                    
                    {
                        fail("endKey fail:"+endKey);
                    }               
                }
            }
            catch(IOException e)
            {
                fail(e.getMessage());
            }       
        }       
        
    
    
        public void testAddField()
        {
            try
            {
                FieldMap fm1 = new FieldMap();
                if(fm1.len() != ConstVar.Sizeof_Short)
                {
                    fail("fail fm1.len:"+fm1.len());
                }
                if(fm1.fieldNum() != 0)
                {
                    fail("fail fm1.fieldNum:"+fm1.fieldNum());
                }
                       
                FieldMap fm2 = new FieldMap();
                Field f = new Field();  
                fm2.addField(f);            
                if(fm2.fieldNum() != 1)
                {
                    fail("fail fm1.fieldNum2:"+fm2.fieldNum());
                }
                
            }
            catch(Exception e)
            {
                fail("addField:"+e.getMessage());
            }
            
            try
            {
                FieldMap fm3 = new FieldMap();              
                for(int i = 0; i < 200000; i++)
                {
                    Field f = new Field();
                    fm3.addField(f);
                }
                if(fm3.fieldNum() != Short.MAX_VALUE)
                {
                    fail("fail fm3.fieldNum3:"+fm3.fieldNum()+"size:"+fm3.fields.size());
                }
            }
            catch(Exception e)
            {
                fail("addField:"+e.getMessage());
            }      
            
            try
            {
                FieldMap fm4 = new FieldMap();
                Field f = new Field(ConstVar.FieldType_String, 0, (short) 0);
                fm4.addField(f);
                
                if(!fm4.var())
                {
                    fail("fm4 should var");
                }
            }
            catch(Exception e)
            {
                fail("addField:"+e.getMessage());
            }      
        }
        
        public void testGetFieldNum()
        {
            try
            {
                FieldMap fm = new FieldMap();
                if(fm.fieldNum() != 0)
                {
                    fail("fail fieldNum:"+fm.fieldNum());
                }
                                
                for(int i = 0; i < 111; i++)
                {
                    Field f = new Field();
                    fm.addField(f);
                }
                if(fm.fieldNum() != 111)
                {
                    fail("fail fieldNum:"+fm.fieldNum());
                }
            }
            catch(Exception e)
            {
                fail("getFieldNum fail:"+e.getMessage());
            }
        }
        
        public void testGetFieldMapLen()
        {
            try
            {
                FieldMap fm = new FieldMap();
                if(fm.len() != ConstVar.Sizeof_Short)
                {
                    fail("fail fieldMapLen1:"+fm.len());
                }
                
                Field f = new Field();
                fm.addField(f);
                if(fm.len() != ConstVar.Sizeof_Short+7)
                {
                    fail("fail fieldMapLen2:"+fm.len());
                }                   
            }
            catch(Exception e)
            {
                fail("fieldMapLen fail:"+e.getMessage());
            }
        }
        
        public void testGetFieldType()
        {
            try
            {
                FieldMap fm1 = new FieldMap();
                fm1.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short) 1));
                fm1.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short) 3));
                fm1.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short) 5));
                fm1.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short) 7));
                
                byte type = fm1.getFieldType((short) 3);
                if(type != ConstVar.FieldType_Short)
                {
                    fail("getFieldType1:"+ type);
                }
                
                type = fm1.getFieldType((short) 5);
                if(type != ConstVar.FieldType_Int)
                {
                    fail("getFieldType2:"+ type);
                }
                
                try
                {
                    type = fm1.getFieldType((short) 4);
                    fail("should exception");
                }
                catch(SEException.InvalidParameterException e)
                {
                }
                catch(Exception e)
                {
                    fail("should not exception:"+e.getMessage());
                }
            }
            catch(Exception e)
            {
                fail("getFieldType3 fail:"+e.getMessage());
            }
        }
        
        public void testPersistentField() throws IOException
        {   
            {           
                String file = prefix + "testPersistentField";
                Path path = new Path(file);
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataOutputStream out = fs.create(path);
                
                Field field = new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)1);
                field.persistent(out);
                if(out.getPos() != 7)
                {
                    fail("error out.pos:"+out.getPos());
                }
                out.close();
                
                FSDataInputStream in = fs.open(path);
                byte type = in.readByte();
                int len = in.readInt();
                short idx = in.readShort();
                
                if(type != ConstVar.FieldType_Byte)
                {
                    fail("fail type:"+type);
                }
                if(len != ConstVar.Sizeof_Byte)
                {
                    fail("fail len:"+len);
                }
                if(idx != 1)
                {
                    fail("fail idx:"+idx);
                }
                
            }
            {
            }
            {
            }
        }
        
        public void testPersistentFieldMap()
        {
            try
            {
                FileSystem fs = FileSystem.get(new Configuration());
                
                String file1 = prefix + "testPersistentFieldMap1";
                Path path = new Path(file1);                
                FSDataOutputStream out1 = fs.create(path);
                
                FieldMap fm1 = new FieldMap();
                fm1.persistent(out1);
                if(out1.getPos() != 2)
                {
                    fail("persisitent null fieldmap fail, pos:"+out1.getPos());
                }
                out1.close();
                
                FSDataInputStream in1 = fs.open(path);
                short fieldNum = in1.readShort();
                if(fieldNum != 0)
                {
                    fail("persistent null fieldmap fail, fieldNum:"+fieldNum);
                }
                
                
                String file2 = prefix + "testPersistentFieldMap2";
                path = new Path(file2);
                FSDataOutputStream out2 = fs.create(path);
                
                FieldMap fm2 = new FieldMap();
                fm2.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short) 1));
                fm2.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short) 3));
                fm2.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short) 5));
                fm2.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short) 7));
                fm2.persistent(out2);
                if(out2.getPos() != (2 + 4*7))
                {
                    fail("persistent 4 field fail, pos:"+out2.getPos());
                }
                out2.close();
                
                FSDataInputStream in2 = fs.open(path);
                fieldNum = in2.readShort();
                if(fieldNum != 4)
                {
                    fail("persistent 4 field fail, fieldNum:"+fieldNum);
                }
                
                byte type = in2.readByte();
                int len = in2.readInt();
                short idx = in2.readShort();
                if(type != ConstVar.FieldType_Byte)
                {
                    fail("fail type:"+type);
                }
                if(len != ConstVar.Sizeof_Byte)
                {
                    fail("fail len:"+len);
                }
                if(idx != 1)
                {
                    fail("fail idx:"+idx);
                }
                
                type = in2.readByte();
                len = in2.readInt();
                idx = in2.readShort();
                if(type != ConstVar.FieldType_Short)
                {
                    fail("fail type:"+type);
                }
                if(len != ConstVar.Sizeof_Short)
                {
                    fail("fail len:"+len);
                }
                if(idx != 3)
                {
                    fail("fail idx:"+idx);
                }
                
                type = in2.readByte();
                len = in2.readInt();
                idx = in2.readShort();
                if(type != ConstVar.FieldType_Int)
                {
                    fail("fail type:"+type);
                }
                if(len != ConstVar.Sizeof_Int)
                {
                    fail("fail len:"+len);
                }
                if(idx != 5)
                {
                    fail("fail idx:"+idx);
                }
                
                type = in2.readByte();
                len = in2.readInt();
                idx = in2.readShort();
                if(type != ConstVar.FieldType_Long)
                {
                    fail("fail type:"+type);
                }
                if(len != ConstVar.Sizeof_Long)
                {
                    fail("fail len:"+len);
                }
                if(idx != 7)
                {
                    fail("fail idx:"+idx);
                }
            }
            catch(IOException e)
            {
                fail("testPersistentField fail1:"+e.getMessage());
            }
            catch(Exception e)
            {
                fail("testPersistentField fail2:"+e.getMessage());
            }
        }
        
        public void testUnpersistentFieldMap()
        {
            try
            {
                FileSystem fs = FileSystem.get(new Configuration());
                
                String file1 = prefix + "testPersistentFieldMap1";
                Path path = new Path(file1);                
                FSDataInputStream in1 = fs.open(path);
                
                FieldMap fm1 = new FieldMap();
                fm1.unpersistent(in1);
                in1.close();
                
                if(fm1.len() != 2)
                {
                    fail("unpersistent fieldMap fail, len:"+fm1.len());
                }
                if(fm1.fieldNum() != 0)
                {
                    fail("unpersistent fieldMap fail, fieldNum:"+fm1.fieldNum());
                }
                
                String file2 = prefix + "testPersistentFieldMap2";
                path = new Path(file2);
                FSDataInputStream in2 = fs.open(path);
                
                FieldMap fm2 = new FieldMap();
                fm2.unpersistent(in2);
                if(fm2.len() != 2+4*7)
                {
                    fail("unpersistent fieldMap fail, len:"+fm2.len());
                }
                if(fm2.fieldNum() != 4)
                {
                    fail("unpersistent fieldMap fail, fieldNum:"+fm2.fieldNum());
                }
                
                Field field = fm2.fields.get((short)1);
                if(field.type != ConstVar.FieldType_Byte)
                {
                    fail("unpersistent fail type:"+field.type);
                }
                if(field.len != ConstVar.Sizeof_Byte)
                {
                    fail("unpersistent fail len:"+field.len);
                }
                if(field.index != 1)
                {
                    fail("unpersistent fail idx:"+field.index);
                }
                
                field = fm2.fields.get((short)3);
                if(field.type != ConstVar.FieldType_Short)
                {
                    fail("unpersistent fail type:"+field.type);
                }
                if(field.len != ConstVar.Sizeof_Short)
                {
                    fail("unpersistent fail len:"+field.len);
                }
                if(field.index != 3)
                {
                    fail("unpersistent fail idx:"+field.index);
                }
                
                field = fm2.fields.get((short)5);
                if(field.type != ConstVar.FieldType_Int)
                {
                    fail("unpersistent fail type:"+field.type);
                }
                if(field.len != ConstVar.Sizeof_Int)
                {
                    fail("unpersistent fail len:"+field.len);
                }
                if(field.index != 5)
                {
                    fail("unpersistent fail idx:"+field.index);
                }
                
                field = fm2.fields.get((short)7);
                if(field.type != ConstVar.FieldType_Long)
                {
                    fail("unpersistent fail type:"+field.type);
                }
                if(field.len != ConstVar.Sizeof_Long)
                {
                    fail("unpersistent fail len:"+field.len);
                }
                if(field.index != 7)
                {
                    fail("unpersistent fail idx:"+field.index);
                }
            }           
            catch(IOException e)
            {
                fail("unpersistent fieldMap fail:"+e.getMessage());
            }
            catch(Exception e)
            {
                fail("unpersistent failMap fail:"+e.getMessage());
            }
        }
        
    
    
    
        public void testInitHead()
        {
            Head head  = new Head();
            if(head.compress() != 0)
            {
                fail("compress not 0");
            }
            if(head.encode() != 0)
            {
                fail("encode != 0");                
            }
            if(head.var() != 0)
            {
                fail("var != 0");
            }
            if(head.primaryIndex() != -1)
            {
                fail("primary index != -1");
            }
            if(head.magic() != ConstVar.DataMagic)
            {
                fail("invalid init magic");
            }
            if(head.ver() != ConstVar.Ver)
            {
                fail("invalid init ver");
            }
        }
        
        public void testGetHeadLen()
        {
            try
            {
                Head head = new Head();
                if(head.len() != 15) 
                {
                    fail("invalid head init len:"+head.len());
                }
                
                FieldMap fieldMap = new FieldMap();
                fieldMap.addField(new Field());
                
                head.setFieldMap(fieldMap);
                
                if(head.len() != 24) 
                {
                    fail("error head.len:"+head.len());
                }
                
                String key = "hello konten";
                head.setKey(key);
                if(head.len() != 22 + key.length() + 2)
                {
                    fail("error head.len:"+head.len());
                }
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get exception:"+e.getMessage());
            }
        }
        
        public void testPersistentHead() 
        {
        
            try
            {
                String fileName = prefix + "testPersistentHead";
                Path path = new Path(fileName);
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataOutputStream out = fs.create(path);
                
                Head head = new Head();
                head.persistent(out);
                if(out.getPos() != 17) 
                {
                    fail("persistent error pos:"+out.getPos());
                }
                out.close();
                
                FSDataInputStream in = fs.open(path);           
                
                int magic = in.readInt();               
                short ver = in.readShort();
                byte var = in.readByte();
                byte compress = in.readByte();
                byte compressStyle = in.readByte();
                short primaryIndex = in.readShort();
                byte encode = in.readByte();
                byte encodeStyle  = in.readByte();
                short keyLen = in.readShort();
                short fieldNum = in.readShort();
                
                if(magic != head.magic())
                {
                    fail("error magic:"+magic);
                }
                if(ver != head.ver())
                {
                    fail("error ver:"+ver);
                }
                if(var != 0)
                {
                    fail("error var:"+var);
                }
                if(compress != head.compress())
                {
                    fail("error compress:"+compress);
                }
                if(compressStyle != head.compressStyle())
                {
                    fail("error compressStyle:"+compressStyle);
                }
                if(primaryIndex != head.primaryIndex())
                {
                    fail("error primaryIndex:"+primaryIndex);
                }
                if(encode != head.encode())
                {
                    fail("error encode:"+encode);
                }
                if(encodeStyle != head.encodeStyle())
                {
                    fail("error encodeStyle:"+encodeStyle);
                }
                if(keyLen != 0)
                {
                    fail("error keyLen:"+keyLen);
                }
                if(fieldNum != 0)
                {
                    fail("error fieldNum:"+fieldNum);
                }
                
                fileName = prefix + "testPersistentHead2";
                path = new Path(fileName);              
                FSDataOutputStream out2 = fs.create(path);
                
                Head head2 = new Head();
                String key = "hello konten";
                
                FieldMap fieldMap = new FieldMap();
                fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
                fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
                head2.setFieldMap(fieldMap);
                head2.setKey(key);
                
                head2.persistent(out2);
                if(out2.getPos() != 13 + 2 + key.length() + fieldMap.len())
                {
                    fail("persistent error pos:"+out.getPos());
                }
                out2.close();
                
                FSDataInputStream in2 = fs.open(path);
            
                magic = in2.readInt();              
                ver = in2.readShort();
                var = in2.readByte();
                compress = in2.readByte();
                compressStyle = in2.readByte();
                primaryIndex = in2.readShort();
                encode = in2.readByte();
                encodeStyle  = in2.readByte();
                
                keyLen = in2.readShort();
                if(keyLen == 0)
                {
                    fail("error keyLen:"+keyLen);
                }
                byte[] buf = new byte[keyLen];
                in2.readFully(buf);
                String keykey = new String(buf);
                if(!key.equals(keykey))
                {
                    fail("error key:"+keykey);
                }
                
                FieldMap fieldMap22 = new FieldMap();
                fieldMap22.unpersistent(in2);
                if(fieldMap22.fieldNum() != 2)
                {
                    fail("error fieldNum:"+fieldMap22.fieldNum());
                }
                Field field = fieldMap22.getField((short) 0);
                if(field.type() != ConstVar.FieldType_Byte)
                {
                    fail("fail field type:"+field.type());
                }
                if(field.len() != ConstVar.Sizeof_Byte)
                {
                    fail("fail field len:"+field.len());
                }
                if(field.index() != 0)
                {
                    fail("fail index:"+field.index());
                }
                
                field = fieldMap22.getField((short) 1);
                if(field.type() != ConstVar.FieldType_Short)
                {
                    fail("fail field type:"+field.type());
                }
                if(field.len() != ConstVar.Sizeof_Short)
                {
                    fail("fail field len:"+field.len());
                }
                if(field.index() != 1)
                {
                    fail("fail index:"+field.index());
                }
                
            }
            catch(Exception e)
            {
                fail("get exception:"+e.getMessage());
            }
        }
        
        public void testUnpersistentHead()
        {
            try
            {
                String fileName = prefix + "testPersistentHead2";
                Path path = new Path(fileName);
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataInputStream in = fs.open(path);
                
                Head head = new Head();
                
                head.setMagic(in.readInt());                
                head.unpersistent(in);
                
                if(head.compress() != 0)
                {
                    fail("compress not 0");
                }
                if(head.encode() != 0)
                {
                    fail("encode != 0");                
                }
                if(head.var() != 0)
                {
                    fail("var != 0");
                }
                if(head.primaryIndex() != -1)
                {
                    fail("primary index != -1");
                }
                if(head.magic() != ConstVar.DataMagic)
                {
                    fail("invalid init magic");
                }
                if(head.ver() != ConstVar.Ver)
                {
                    fail("invalid init ver");
                }
                
                String key = "hello konten";
                if(!head.key.equals(key))
                {
                    fail("error key:"+head.key);
                }
                
                FieldMap fieldMap = head.fieldMap();                
                if(fieldMap.fieldNum() != 2)
                {
                    fail("error fieldNum:"+fieldMap.fieldNum());
                }
                Field field = fieldMap.getField((short) 0);
                if(field.type() != ConstVar.FieldType_Byte)
                {
                    fail("fail field type:"+field.type());
                }
                if(field.len() != ConstVar.Sizeof_Byte)
                {
                    fail("fail field len:"+field.len());
                }
                if(field.index() != 0)
                {
                    fail("fail index:"+field.index());
                }
                
                field = fieldMap.getField((short) 1);
                if(field.type() != ConstVar.FieldType_Short)
                {
                    fail("fail field type:"+field.type());
                }
                if(field.len() != ConstVar.Sizeof_Short)
                {
                    fail("fail field len:"+field.len());
                }
                if(field.index() != 1)
                {
                    fail("fail index:"+field.index());
                }
                
            }
            catch(Exception e)
            {
                fail("get exception:"+e.getMessage());
            }           
        }
        
        public void testToJobConf()
        {
            try
            {
                Head head = new Head();
                String key = "hello konten";
                head.setKey(key);
                
                FieldMap fieldMap = new FieldMap();
                fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
                fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
                head.setFieldMap(fieldMap);
                
                JobConf conf = new JobConf();
                head.toJobConf(conf);
            
                Head head2 = new Head();
                head2.fromJobConf(conf);
                
                if(head2.magic != head.magic)
                {
                    fail("error magic:"+head2.magic);
                }
                if(head2.compress != head.compress)
                {
                    fail("error compress:"+head2.compress);
                }
                if(head2.compressStyle != head.compressStyle)
                {
                    fail("error compressStyle:"+head2.compressStyle);
                }
                if(head2.encode != head.encode)
                {
                    fail("error encode:"+head2.encode);
                }
                if(head2.encodeStyle != head.encodeStyle)
                {
                    fail("error encodeStyle:"+head2.encodeStyle);
                }
                if(!head2.key.equals(head.key))
                {
                    fail("error key:"+head2.key);
                }
                if(head2.primaryIndex != head.primaryIndex)
                {
                    fail("error primary index:"+head2.primaryIndex);
                }
                if(head2.var != head.var)
                {
                    fail("error var:"+head2.var);
                }
                if(head2.ver != head.ver)
                {
                    fail("error ver:"+head2.ver);
                }
                
                if(head2.fieldMap.fieldNum() != head.fieldMap.fieldNum())
                {
                    fail("error fieldNum:"+head2.fieldMap.fieldNum());
                }
                
                Field f1 = head.fieldMap.getField((short) 0);
                Field f2 = head2.fieldMap.getField((short) 0);
                if(f1.type() != f2.type())
                {
                    fail("error type:"+f2.type());
                }
                if(f1.len() != f2.len())
                {
                    fail("error len:"+f2.len());
                }
                if(f1.index() != f2.index())
                {
                    fail("error index:"+f2.index());
                }
                
                f1 = head.fieldMap.getField((short) 1);
                f2 = head2.fieldMap.getField((short) 1);
                if(f1.type() != f2.type())
                {
                    fail("error type:"+f2.type());
                }
                if(f1.len() != f2.len())
                {
                    fail("error len:"+f2.len());
                }
                if(f1.index() != f2.index())
                {
                    fail("error index:"+f2.index());
                }
            }
            catch(Exception e)
            {
                fail("get exception:"+e.getMessage());
            }
        }   
        
    
    
        public void testAddFieldValue()
        {
            try
            {
                Record record = new Record( 1);              
                
                FieldValue fieldValue = new FieldValue(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, null, (short) 0);
                record.addValue(fieldValue);                
                if(record.fieldValues().get(0).value != null)
                {
                    fail("value should null");
                }
                if(record.fieldValues().get(0).len != ConstVar.Sizeof_Byte)
                {
                    fail("error len:"+record.fieldValues().get(0).len);
                }
                if(record.fieldValues().get(0).type != ConstVar.FieldType_Byte)
                {
                    fail("error fieldType:"+record.fieldValues().get(0).type);
                }
                
                try
                {
                    FieldValue f2 = new FieldValue();
                    record.addValue(f2);
                    fail("error fieldNum");
                }
                catch(SEException.FieldValueFull e)
                {
                    
                } 
                
                try
                {
                    FieldValue f2 = new FieldValue(ConstVar.FieldType_Int, ConstVar.Sizeof_Short, null, (short)1);
                    fail("shoud exception");
                }
                catch(SEException.InvalidParameterException e)
                {
                    
                }
                
                try
                {
                    FieldValue f3 = new FieldValue(ConstVar.FieldType_String, ConstVar.Sizeof_Short, null, (short)1);                   
                }
                catch(Exception e)
                {
                    fail("shoud not exception");    
                }
                
                short fieldNum = 7;
                Record record2 = new Record(fieldNum);
                
                byte[] bb = new byte[ConstVar.Sizeof_Byte];
                bb[0] = 1;
                FieldValue fieldValue1 = new FieldValue(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, bb, (short) 10);
                record2.addValue(fieldValue1);
                
                byte[] sb = new byte[ConstVar.Sizeof_Short];
                short s = 2;
                Util.short2bytes(sb, s);                
                FieldValue fieldValue2 = new FieldValue(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, sb, (short) 11);
                record2.addValue(fieldValue2);
                
                byte[] ib = new byte[ConstVar.Sizeof_Int];
                int i = 3;
                Util.int2bytes(ib, i);              
                FieldValue fieldValue3 = new FieldValue(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, ib, (short) 12);
                record2.addValue(fieldValue3);
            
                byte[] lb = new byte[ConstVar.Sizeof_Long];
                long l = 4;
                Util.long2bytes(lb, l);             
                FieldValue fieldValue4 = new FieldValue(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, lb, (short) 13);
                record2.addValue(fieldValue4);
                
                byte[] fb = new byte[ConstVar.Sizeof_Float];
                float f = (float) 5.5;
                Util.float2bytes(fb, f);                
                FieldValue fieldValue5 = new FieldValue(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, fb, (short) 14);
                record2.addValue(fieldValue5);
                
                byte[] db = new byte[ConstVar.Sizeof_Double];
                double d = 6.6;
                Util.double2bytes(db, d);               
                FieldValue fieldValue6 = new FieldValue(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, db, (short) 15);
                record2.addValue(fieldValue6);
                
                String str = "hello konten";                            
                FieldValue fieldValue7 = new FieldValue(ConstVar.FieldType_String, (short)str.length(), str.getBytes(), (short) 16);
                record2.addValue(fieldValue7);
                
                
                int index = 0;
                byte[] buf = record2.fieldValues().get(index).value; 
                if(buf == null)
                {
                    fail("value should not null");
                }
                if(buf[0] != 1)
                {
                    fail("error value:"+buf[0]);
                }                           
                byte type = record2.fieldValues().get(index).type;
                int len = record2.fieldValues().get(index).len;
                short idx = record2.fieldValues().get(index).idx;
                if(len != ConstVar.Sizeof_Byte)
                {
                    fail("error len:"+len);
                }
                if(type != ConstVar.FieldType_Byte)
                {
                    fail("error fieldType:"+type);
                }
                if(idx != 10)
                {
                    fail("error idx:"+idx);
                }
                
                
                index = 1;
                buf = record2.fieldValues().get(index).value; 
                if(buf == null)
                {
                    fail("value should not null");
                }
                short sval = Util.bytes2short(buf, 0, ConstVar.Sizeof_Short);
                if(sval != 2)
                {
                    fail("error value:"+sval);
                }                               
                type = record2.fieldValues().get(index).type;
                len = record2.fieldValues().get(index).len;
                idx = record2.fieldValues().get(index).idx;
                if(len != ConstVar.Sizeof_Short)
                {
                    fail("error len:"+len);
                }
                if(type != ConstVar.FieldType_Short)
                {
                    fail("error fieldType:"+type);
                }
                if(idx != 11)
                {
                    fail("error idx:"+idx);
                }
                
                index = 2;
                buf = record2.fieldValues().get(index).value; 
                if(buf == null)
                {
                    fail("value should not null");
                }
                int ival = Util.bytes2int(buf, 0, ConstVar.Sizeof_Int);
                if(ival != 3)
                {
                    fail("error value:"+ival);
                }                               
                type = record2.fieldValues().get(index).type;
                len = record2.fieldValues().get(index).len;
                idx = record2.fieldValues().get(index).idx;
                if(len != ConstVar.Sizeof_Int)
                {
                    fail("error len:"+len);
                }
                if(type != ConstVar.FieldType_Int)
                {
                    fail("error fieldType:"+type);
                }
                if(idx != 12)
                {
                    fail("error idx:"+idx);
                }
                
                index = 3;
                buf = record2.fieldValues().get(index).value; 
                if(buf == null)
                {
                    fail("value should not null");
                }
                long lval = Util.bytes2long(buf, 0, ConstVar.Sizeof_Long);
                if(lval != 4)
                {
                    fail("error value:"+lval);
                }                               
                type = record2.fieldValues().get(index).type;
                len = record2.fieldValues().get(index).len;
                idx = record2.fieldValues().get(index).idx;
                if(len != ConstVar.Sizeof_Long)
                {
                    fail("error len:"+len);
                }
                if(type != ConstVar.FieldType_Long)
                {
                    fail("error fieldType:"+type);
                }
                if(idx != 13)
                {
                    fail("error idx:"+idx);
                }
                
                index = 4;
                buf = record2.fieldValues().get(index).value; 
                if(buf == null)
                {
                    fail("value should not null");
                }
                float fval = Util.bytes2float(buf, 0);
                if(fval != 5.5)
                {
                    fail("error value:"+fval);
                }                               
                type = record2.fieldValues().get(index).type;
                len = record2.fieldValues().get(index).len;
                idx = record2.fieldValues().get(index).idx;
                if(len != ConstVar.Sizeof_Float)
                {
                    fail("error len:"+len);
                }
                if(type != ConstVar.FieldType_Float)
                {
                    fail("error fieldType:"+type);
                }
                if(idx != 14)
                {
                    fail("error idx:"+idx);
                }
                
                index = 5;
                buf = record2.fieldValues().get(index).value; 
                if(buf == null)
                {
                    fail("value should not null");
                }
                double dval = Util.bytes2double(buf, 0);
                if(dval != 6.6)
                {
                    fail("error value:"+dval);
                }                               
                type = record2.fieldValues().get(index).type;
                len = record2.fieldValues().get(index).len;
                idx = record2.fieldValues().get(index).idx;
                if(len != ConstVar.Sizeof_Double)
                {
                    fail("error len:"+len);
                }
                if(type != ConstVar.FieldType_Double)
                {
                    fail("error fieldType:"+type);
                }
                if(idx != 15)
                {
                    fail("error idx:"+idx);
                }
                
                index = 6;
                buf = record2.fieldValues().get(index).value; 
                if(buf == null)
                {
                    fail("value should not null");
                }               
                str = new String(buf);
                if(!str.equals(new String("hello konten")))
                {
                    fail("error val:"+str);
                }
                type = record2.fieldValues().get(index).type;
                len = record2.fieldValues().get(index).len;
                idx = record2.fieldValues().get(index).idx;
                if(len != str.length())
                {
                    fail("error len:"+len);
                }
                if(type != ConstVar.FieldType_String)
                {
                    fail("error fieldType:"+type);
                }
                if(idx != 16)
                {
                    fail("error idx:"+idx);
                }
                
            }
            catch(SEException.InvalidParameterException e)
            {
                fail("get seexception:"+e.getMessage());
            }
            catch(IOException e)
            {
                fail("get ioexception:"+e.getMessage());
            }
            catch(Exception e)
            {
                fail("get exception:"+e.getMessage());
            }
        }
        
        public void testValidFieldType()
        {
            try
            {
                FieldValue f = new FieldValue(ConstVar.FieldType_Unknown, (short)0, null, (short)0);
                fail("should exception");
            }
            catch(SEException.InvalidParameterException e)
            {
                
            }
            catch(Exception e)
            {
                fail("should not this exception:"+e.getMessage());
            }
            
            try
            {
                FieldValue f = new FieldValue(ConstVar.FieldType_User, (short)0, null, (short)0);
                fail("should exception");
            }
            catch(SEException.InvalidParameterException e)
            {
                
            }
            catch(Exception e)
            {
                fail("should not this exception:"+e.getMessage());
            }
        }
        
        public void testAddMuchFieldValue()
        {
            int size = 128;
            
            try
            {
                Record record = new Record( size);
                for(int i = 0; i < size; i++)
                {
                    record.addValue(new FieldValue(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, null, (short)i));
                }
            }
            catch(Exception e)
            {
                fail("should not exception:"+e.getMessage());
            }
            
            size = 32767;            
            try
            {
                Record record = new Record( size);
                for(int i = 0; i < size; i++)
                {
                    record.addValue(new FieldValue(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, null, (short)i));
                }
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("should not exception:"+e.getMessage());
            }
            
            size = 32768;            
            try
            {
                Record record = new Record( size);
                for(int i = 0; i < size; i++)
                {
                    record.addValue(new FieldValue(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, null, (short)i));
                }
                fail("should exception");
            }
            catch(SEException.InvalidParameterException e)
            {
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("should not this exception:"+e.getMessage());
            }
        }
        
        public void testValueTrunk()
        {
            try
            {
                String str1 = "hello konten";
                FieldValue f1 = new FieldValue(ConstVar.FieldType_String, (short)100, str1.getBytes(), (short)0);
                fail("should exception");
            }
            catch(SEException.InvalidParameterException e)
            {
                
            }
            catch(Exception e)
            {
                fail("should not this exception");
            }
            
            try
            {
                String str1 = "hello konten";
                FieldValue f1 = new FieldValue(ConstVar.FieldType_String, (short)3, str1.getBytes(), (short)0);
                fail("should exception");
            }
            catch(SEException.InvalidParameterException e)
            {
                
            }
            catch(Exception e)
            {
                fail("should not this exception");
            }
            
            try
            {               
                FieldValue f1 = new FieldValue(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, new byte[10], (short)0);
                fail("should exception");
            }
            catch(SEException.InvalidParameterException e)
            {
                
            }
            catch(Exception e)
            {
                fail("should not this exception");
            }
        }
        
        public void testRecordMerage()
        {
            try
            {
                Record record1 = new Record( 2);
                record1.addValue(new FieldValue((byte)1, (short)1));
                record1.addValue(new FieldValue((short)2, (short)3));
                
                Record record2 = new Record( 2);
                record2.addValue(new FieldValue((int)3, (short)5));
                record2.addValue(new FieldValue((long)4, (short)7));
                                
                Record record = new Record();
                record.merge(record1);
                if(record.fieldNum != 2)
                {
                    fail("error fieldNum:"+record.fieldNum);
                }
                
                record.merge(record2);
                if(record.fieldNum != 4)
                {
                    fail("error fieldNum2:"+record.fieldNum);
                }
                    
                judgeMeragedRecord(record);
                
                record1.merge(record2);
                if(record1.fieldNum != 4)
                {
                    fail("error fieldNum3:"+record1.fieldNum);
                }
                    
                judgeMeragedRecord(record1);
                
                record1.merge(record2); 
                if(record1.fieldNum != 6)
                {
                    fail("error fieldNum:"+record1.fieldNum);                    
                }
                judgeMeragedRecord(record1);
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get exception:"+e.getMessage());
            }
        }
        
        void judgeMeragedRecord(Record record)
        {
            short index = 0;
            byte type = record.fieldValues().get(index).type;
            int len = record.fieldValues().get(index).len;
            short idx = record.fieldValues().get(index).idx;
            byte[] value = record.fieldValues().get(index).value; 
            
            if(type != ConstVar.FieldType_Byte)
            {
                fail("error type:"+type);
            }
            if(len != ConstVar.Sizeof_Byte)
            {
                fail("error len:"+len);
            }
            if(idx != 1)
            {
                fail("error idx:"+idx);
            }                
            byte bv = value[0];
            if(bv != 1)
            {
                fail("error value:"+bv);
            }
            
            index = 1;
            type = record.fieldValues().get(index).type;
            len = record.fieldValues().get(index).len;
            idx = record.fieldValues().get(index).idx;
            value = record.fieldValues().get(index).value; 
            
            if(type != ConstVar.FieldType_Short)
            {
                fail("error type:"+type);
            }
            if(len != ConstVar.Sizeof_Short)
            {
                fail("error len:"+len);
            }
            if(idx != 3)
            {
                fail("error idx:"+idx);
            }                
            short sv = Util.bytes2short(value, 0, 2);
            if(sv != 2)
            {
                fail("error value:"+sv);
            }
                
            index = 2;
            type = record.fieldValues().get(index).type;
            len = record.fieldValues().get(index).len;
            idx = record.fieldValues().get(index).idx;
            value = record.fieldValues().get(index).value; 
            
            if(type != ConstVar.FieldType_Int)
            {
                fail("error type:"+type);
            }
            if(len != ConstVar.Sizeof_Int)
            {
                fail("error len:"+len);
            }
            if(idx != 5)
            {
                fail("error idx:"+idx);
            }                
            int iv = Util.bytes2int(value, 0, 4);
            if(iv != 3)
            {
                fail("error value:"+iv);
            }
            
            index = 3;
            type = record.fieldValues().get(index).type;
            len = record.fieldValues().get(index).len;
            idx = record.fieldValues().get(index).idx;
            value = record.fieldValues().get(index).value; 
            
            if(type != ConstVar.FieldType_Long)
            {
                fail("error type:"+type);
            }
            if(len != ConstVar.Sizeof_Long)
            {
                fail("error len:"+len);
            }
            if(idx != 7)
            {
                fail("error idx:"+idx);
            }                
            long lv = Util.bytes2long(value, 0, 8);
            if(lv != 4)
            {
                fail("error value:"+lv);
            }
            
        }
        public void testRecordToList()
        {
            
        }
        
        public void testRecordTrim()
        {
            
        }
        
        
    
    
        public void testInitChunk()
        {
            try
            {
                short fieldNum = 3;
                Record record = new Record(fieldNum);
                
                byte[] lb = new byte[ConstVar.Sizeof_Long];
                long l = 4;
                Util.long2bytes(lb, l);             
                FieldValue fieldValue4 = new FieldValue(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, lb, (short) 13);
                record.addValue(fieldValue4);
                
                byte[] fb = new byte[ConstVar.Sizeof_Float];
                float f = (float) 5.5;
                Util.float2bytes(fb, f);                
                FieldValue fieldValue5 = new FieldValue(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, fb, (short) 14);
                record.addValue(fieldValue5);
                
                            
                String str = "hello konten";                            
                FieldValue fieldValue7 = new FieldValue(ConstVar.FieldType_String, (short)str.length(), str.getBytes(), (short) 16);
                record.addValue(fieldValue7);
                
                DataChunk chunk = new DataChunk(record);
                if(chunk.fieldNum != fieldNum)
                {
                    fail("error fieldNum:"+chunk.fieldNum);
                }
                if(chunk.len != (1 + 8+4 + 2+str.length()))
                {
                    fail("error len:"+chunk.len);
                }
                               
                /*  
                BitSet bitSet = chunk.bitset;
                
                if(bitSet.length() != fieldNum)
                {
                    fail("error bitSet length:"+bitSet.length());
                }
                for(int i = 0; i < bitSet.length(); i++)
                {
                    if(!bitSet.get(i))
                    {
                        fail("bitSet null, i:"+i);
                    }
                }
                */              
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("should not exception:"+e.getMessage());
            }
            
            try
            {
                short fieldNum = 3;
                Record record = new Record(fieldNum);
                
                byte[] lb = new byte[ConstVar.Sizeof_Long];
                long l = 4;
                Util.long2bytes(lb, l);             
                FieldValue fieldValue4 = new FieldValue(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, lb, (short) 13);
                record.addValue(fieldValue4);
                                
                FieldValue fieldValue5 = new FieldValue(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, null, (short) 14);
                record.addValue(fieldValue5);
                
                            
                String str = "hello konten";    
                
                FieldValue fieldValue7 = new FieldValue(ConstVar.FieldType_String, str.length(), str.getBytes(), (short) 16);
                record.addValue(fieldValue7);                
                
                DataChunk chunk = new DataChunk(record);                
                if(chunk.fieldNum != fieldNum)
                {
                    fail("error fieldNum:"+chunk.fieldNum);
                }
                if(chunk.len != (1 + 8+4 + 2+str.length()))
                {
                    fail("error len:"+chunk.len);
                }                          
            }
            catch(Exception e)
            {
                fail("should not exception:"+e.getMessage());
            }
        }
        
        public void testChunkToRecord() 
        {
            try
            {
                String fileName = prefix + "testChunkToRecord";              
                Path path = new Path(fileName);
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataOutputStream out = fs.create(path);
                
                short fieldNum = 3;
                Record record = new Record(fieldNum);
                
                byte[] lb = new byte[ConstVar.Sizeof_Long];
                long l = 4;
                Util.long2bytes(lb, l);             
                FieldValue fieldValue4 = new FieldValue(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, lb, (short) 13);
                record.addValue(fieldValue4);
                
                byte[] fb = new byte[ConstVar.Sizeof_Float];
                float f = (float) 5.5;
                Util.float2bytes(fb, f);                
                FieldValue fieldValue5 = new FieldValue(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, fb, (short) 14);
                record.addValue(fieldValue5);               
                            
                String str = "hello konten";                            
                FieldValue fieldValue7 = new FieldValue(ConstVar.FieldType_String, (short)str.length(), str.getBytes(), (short) 16);
                record.addValue(fieldValue7);
                
                DataChunk chunk = new DataChunk(record);
                
                out.write(chunk.values, 0, (int) chunk.len);
                                
                if(out.getPos() != chunk.len)
                {
                    fail("error pos:"+out.getPos()+"chunk.len:"+chunk.len);                 
                }
                out.close();
                
                FSDataInputStream in = fs.open(path);           
               
                FixedBitSet bitSet = new FixedBitSet(fieldNum);
                in.read(bitSet.bytes(), 0, bitSet.size());
                for(int i = 0; i < fieldNum; i++)
                {
                    if(!bitSet.get(i))
                    {
                        fail("should set:"+i);
                    }
                }
                
                byte[] value = new byte[8];
                in.readFully(value);
                long lv = Util.bytes2long(value, 0, 8); 
                if(lv != 4)
                {
                    fail("error long value:"+lv);
                }
                
                value = new byte[4];
                in.readFully(value);
                float fv = Util.bytes2float(value, 0); 
                if(fv != 5.5)
                {
                    fail("error float value:"+fv);
                }
                
                short strLen = in.readShort();
                if(strLen != str.length())
                {
                    fail("error strLen:"+strLen);
                }
                value = new byte[strLen];
                in.readFully(value);
                String strv = new String(value);
                if(!strv.equals(str))
                {
                    fail("error strv:"+strv);
                }
                
                
                FieldMap fieldMap = new FieldMap();
                fieldMap.addField(new Field(ConstVar.FieldType_Long, 8, (short) 13));
                fieldMap.addField(new Field(ConstVar.FieldType_Float, 4, (short) 14));
                fieldMap.addField(new Field(ConstVar.FieldType_String, 8, (short) 16));
                    
                in.seek(0);
                int valuelen = 1+8+4+2+12;
                DataChunk chunk2 = new DataChunk(fieldNum);
                
                ArrayList<byte[]> arrayList = new ArrayList<byte[]>(64);
                DataInputBuffer inputBuffer = new DataInputBuffer();               
                byte[] buf = new byte[valuelen];
                in.read(buf, 0, valuelen);
                inputBuffer.reset(buf, 0, valuelen);                
                chunk2.unpersistent(0, valuelen, inputBuffer);
                Record record2 = chunk2.toRecord(fieldMap, true, arrayList);
                
                bitSet = chunk2.fixedBitSet;
                if(bitSet.length() != (fieldNum/8+1)*8)
                {
                    fail("bitSet.len:"+bitSet.length());
                }
                
                for(int i = 0; i < fieldNum; i++)
                {
                    if(!bitSet.get(i))
                    {
                        fail("bitSet should set:"+i);
                    }
                }
                record = record2;
                
                int index = 0;
                byte type = record2.fieldValues().get(index).type;
                int len = record2.fieldValues().get(index).len;
                short idx = record2.fieldValues().get(index).idx;
                value = record2.fieldValues().get(index).value; 
                if(len != ConstVar.Sizeof_Long)
                {
                    fail("error len:"+len);
                }
                if(type != ConstVar.FieldType_Long)
                {
                    fail("error fieldType:"+type);
                }
                if(idx != 13)
                {
                    fail("error idx:"+idx);
                }
                if(value == null)
                {
                    fail("error value null");
                }
                
                {
                }
                lv = Util.bytes2long(value, 0, len);
                if(lv != 4)
                {
                    fail("error long value:"+lv);
                }
                
                index = 1;
                type = record.fieldValues().get(index).type;
                len = record.fieldValues().get(index).len;
                idx = record.fieldValues().get(index).idx;
                value = record.fieldValues().get(index).value; 
                
                if(len != ConstVar.Sizeof_Float)
                {
                    fail("error len:"+len);
                }
                if(type != ConstVar.FieldType_Float)
                {
                    fail("error fieldType:"+type);
                }
                if(idx != 14)
                {
                    fail("error idx:"+idx);
                }
                if(value == null)
                {
                    fail("error value null");
                }
                {
                }
                fv = Util.bytes2float(value, 0);
                if(fv != 5.5)
                {
                    fail("error float value:"+fv);
                }
                
                index = 2;
                type = record.fieldValues().get(index).type;
                len = record.fieldValues().get(index).len;
                idx = record.fieldValues().get(index).idx;
                value = record.fieldValues().get(index).value; 
                
                str = "hello konten";
                if(len != str.length())
                {
                    fail("error len:"+len);
                }
                if(type != ConstVar.FieldType_String)
                {
                    fail("error fieldType:"+type);
                }
                if(idx != 16)
                {
                    fail("error idx:"+idx);
                }
                if(value == null)
                {
                    fail("error value null");
                }
                {
                }
                String sv = new String(value, 0, len);
                if(!str.equals(sv))
                {
                    fail("error string value:"+sv);
                }               
                
            }
            catch(Exception e)
            {
                fail("should not exception:"+e.getMessage());
            }
        }
        
        public void testChunkToRecordNull()
        {
            try
            {
                String fileName = prefix + "testChunkToRecord2";             
                Path path = new Path(fileName);
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataOutputStream out = fs.create(path);
                
                short fieldNum = 3;
                Record record = new Record(fieldNum);
                
                byte[] lb = new byte[ConstVar.Sizeof_Long];
                long l = 4;
                Util.long2bytes(lb, l);             
                FieldValue fieldValue4 = new FieldValue(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, lb, (short) 13);
                record.addValue(fieldValue4);               
    
                FieldValue fieldValue5 = new FieldValue(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, null, (short) 14);
                record.addValue(fieldValue5);               
                            
                String str = "hello konten";                            
                FieldValue fieldValue7 = new FieldValue(ConstVar.FieldType_String, (short)str.length(), str.getBytes(), (short) 16);
                record.addValue(fieldValue7);
                
                DataChunk chunk = new DataChunk(record);
                
                out.write(chunk.values, 0, (int) chunk.len);                
               
                if(out.getPos() != chunk.len)
                {
                    fail("error pos:"+out.getPos()+"chunk.len:"+chunk.len);                 
                }
                out.close();
                
                FSDataInputStream in = fs.open(path);                 
                
                FixedBitSet bitSet = new FixedBitSet(fieldNum);
                in.read(bitSet.bytes(), 0, bitSet.size());
                
                for(int i = 0; i < fieldNum; i++)
                {
                    if(bitSet.get(1))
                    {
                        fail("shoud not set");
                    }
                    
                    if(!bitSet.get(i) && i != 1)
                    {
                        fail("should set:"+i);
                    }
                }
                
                byte[] value = new byte[8];
                in.readFully(value);
                long lv = Util.bytes2long(value, 0, 8); 
                if(lv != 4)
                {
                    fail("error long value:"+lv);
                }
                
                in.readFloat();  
                
                short strLen = in.readShort();
                if(strLen != str.length())
                {
                    fail("error strLen:"+strLen);
                }
                value = new byte[strLen];
                in.readFully(value);
                String strv = new String(value, 0, strLen);
                if(!strv.equals(str))
                {
                    fail("error strv:"+strv);
                }
                
                FieldMap fieldMap = new FieldMap();
                fieldMap.addField(new Field(ConstVar.FieldType_Long, 8, (short) 13));
                fieldMap.addField(new Field(ConstVar.FieldType_Float, 4, (short) 14));
                fieldMap.addField(new Field(ConstVar.FieldType_String, 8, (short) 16));
                    
                in.seek(0);
                int valuelen = 1+8+4+2+12;
                DataChunk chunk2 = new DataChunk(fieldNum);
                
                ArrayList<byte[]> arrayList = new ArrayList<byte[]>(64);
                
                DataInputBuffer inputBuffer = new DataInputBuffer();               
                byte[] buf = new byte[valuelen];
                in.read(buf, 0, valuelen);
                inputBuffer.reset(buf, 0, valuelen);                   
                chunk2.unpersistent(0, valuelen, inputBuffer);
                Record record2 = chunk2.toRecord(fieldMap, true, arrayList);
                
                bitSet = chunk2.fixedBitSet;                
                
                for(int i = 0; i < fieldNum; i++)
                {
                    if(bitSet.get(1))
                    {
                        fail("shoud not set");
                    }
                    
                    if(!bitSet.get(i) && i != 1)
                    {
                        fail("should set:"+i);
                    }
                }
                record = record2;
                
                int index = 0;
                byte type = record2.fieldValues().get(index).type;
                int len = record2.fieldValues().get(index).len;
                short idx = record2.fieldValues().get(index).idx;
                value = record2.fieldValues().get(index).value; 
                if(len != ConstVar.Sizeof_Long)
                {
                    fail("error len:"+len);
                }
                if(type != ConstVar.FieldType_Long)
                {
                    fail("error fieldType:"+type);
                }
                if(idx != 13)
                {
                    fail("error idx:"+idx);
                }
                if(value == null)
                {
                    fail("error value null");
                }
                {
                }
                lv = Util.bytes2long(value, 0, 8);
                if(lv != 4)
                {
                    fail("error long value:"+lv);
                }
                
                index = 1;
                type = record.fieldValues().get(index).type;
                len = record.fieldValues().get(index).len;
                idx = record.fieldValues().get(index).idx;
                value = record.fieldValues().get(index).value; 
                
                if(len != ConstVar.Sizeof_Float)
                {
                    fail("error len:"+len);
                }
                if(type != ConstVar.FieldType_Float)
                {
                    fail("error fieldType:"+type);
                }
                if(idx != 14)
                {
                    fail("error idx:"+idx);
                }
                if(value != null)
                {
                    fail("error value not null");
                }               
                
                index = 2;
                type = record.fieldValues().get(index).type;
                len = record.fieldValues().get(index).len;
                idx = record.fieldValues().get(index).idx;
                value = record.fieldValues().get(index).value; 
                
                str = "hello konten";
                if(len != str.length())
                {
                    fail("error len:"+len);
                }
                if(type != ConstVar.FieldType_String)
                {
                    fail("error fieldType:"+type);
                }
                if(idx != 16)
                {
                    fail("error idx:"+idx);
                }
                if(value == null)
                {
                    fail("error value null");
                }
                {
                }
                String sv = new String(value, 0, len);
                if(!str.equals(sv))
                {
                    fail("error string value:"+sv);
                }               
                
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("should not exception:"+e.getMessage());
            }
        }           
    
    
    
        public void testInitUnit()
        {
            try
            {                               
                IndexInfo info = new IndexInfo();
                info.beginKey = 1;
                info.endKey = 11;
                info.beginLine = 2;
                info.endLine = 22;
                info.offset = 300;
                info.len = 100;
                info.idx = 7;               
                
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.head = new Head();
                Segment seg = new Segment(info, fd);
                
                Unit unit2 = new Unit(info, seg);
                if(unit2.offset() != 300)
                {
                    fail("error offset:"+unit2.offset());
                }               
                if(unit2.len() != 100 + (0 * 8) + ConstVar.DataChunkMetaOffset)
                {
                    fail("error len:"+unit2.len());
                }
                if(unit2.beginKey() != 1)
                {
                    fail("error beginkey:"+unit2.beginKey());
                }
                if(unit2.endKey() != 11)
                {
                    fail("error endKey:"+unit2.endKey());
                }
                if(unit2.beginLine() != 2)
                {
                    fail("error beginLine:"+unit2.beginLine());
                }
                if(unit2.endLine() != 22)
                {
                    fail("error endLine:"+unit2.endLine());
                }
                
                Head head = new Head();
                fd.create(prefix + "testUnitInit", head);
                for(int i = 0; i < 67; i++)
                {
                    fd.incRecordNum();
                }
                
                Unit unit3 = new Unit(info, seg);
                if(unit3.beginLine() != 67)
                {
                    fail("error beginLine:"+unit3.beginLine());
                }
                if(unit3.endLine() != 67)
                {
                    fail("error endLine:"+unit3.endLine());
                }
            }
            catch(IOException e)
            {
                fail("get IOexception:"+e.getMessage());
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get exception:"+e.getMessage());
            }
        }
        
        public void testAddOneRecordUnit()
        {
            try
            {
                Head head = new Head();
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.create(prefix + "testUnitAddOneRecord", head); 
                
                IndexInfo info = new IndexInfo();
                info.offset = 0;
                Segment seg = new Segment(info, fd);
                Unit unit = new Unit(info, seg);
                
                Record record = new Record( 7);
                record.addValue(new FieldValue((byte)1, (short)0));
                record.addValue(new FieldValue((short)2, (short)1));
                record.addValue(new FieldValue((int)3, (short)2));
                record.addValue(new FieldValue((long)4, (short)3));
                record.addValue(new FieldValue((float)5.5, (short)4));
                record.addValue(new FieldValue((double)6.6, (short)5));
                record.addValue(new FieldValue("hello konten", (short)6));
                
                unit.addRecord(record);
                
                if(unit.offset() != 0)
                {
                    fail("error offset:"+unit.offset());                    
                }
                if(unit.len() != full7chunkLen + ConstVar.DataChunkMetaOffset)
                {
                    fail("error len:"+unit.len());
                }
                if(unit.recordNum() != 1)
                {
                    fail("error recordNum:"+unit.recordNum());
                }
                if(unit.beginLine() != 0)
                {
                    fail("error beginLine:"+unit.beginLine());                  
                }
                if(unit.endLine() != 1)
                {
                    fail("error endLine:"+unit.endLine());
                }
                if(unit.beginKey() != 0)
                {
                    fail("error beginKey:"+unit.beginKey());
                }
                if(unit.endKey() != 0)
                {
                    fail("error endKey:"+unit.endKey());
                }
            }
            catch(IOException e)
            {
                fail("get IOException:"+e.getMessage());
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get exception:"+e.getMessage());
            }
        }
        
        public void testAddMuchRecordUnit()
        {
            try
            {
                Head head = new Head();
                head.setVar((byte) 1);
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.create(prefix + "testUnitAddMuchRecord", head); 
                
                IndexInfo info = new IndexInfo();
                info.offset = 0;
                Segment seg = new Segment(info, fd);
                Unit unit = new Unit(info, seg);
                
                Record record = new Record( 7);
                record.addValue(new FieldValue((byte)1, (short)0));
                record.addValue(new FieldValue((short)2, (short)1));
                record.addValue(new FieldValue((int)3, (short)2));
                record.addValue(new FieldValue((long)4, (short)3));
                record.addValue(new FieldValue((float)5.5, (short)4));
                record.addValue(new FieldValue((double)6.6, (short)5));
                record.addValue(new FieldValue("hello konten", (short)6));
                
                int count = 0;
                try
                {
                    for(count = 1; ; count++)
                    {
                        unit.addRecord(record);
                    }
                }
                catch(SEException.UnitFullException e)
                {
                    
                }
                
                if(unit.offset() != 0)
                {
                    fail("error offset:"+unit.offset());                    
                }
                if(unit.len() != full7chunkLen * count + count*8 + ConstVar.DataChunkMetaOffset)
                {
                    fail("error len:"+unit.len()+"count:"+count);
                }
                if(unit.recordNum() != count)
                {
                    fail("error recordNum:"+unit.recordNum());
                }
                if(unit.beginLine() != 0)
                {
                    fail("error beginLine:"+unit.beginLine());                  
                }
                if(unit.endLine() != count)
                {
                    fail("error endLine:"+unit.endLine());
                }
                if(unit.beginKey() != 0)
                {
                    fail("error beginKey:"+unit.beginKey());
                }
                if(unit.endKey() != 0)
                {
                    fail("error endKey:"+unit.endKey());
                }
            }
            catch(IOException e)
            {
                fail("get IOException:"+e.getMessage());
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get exception:"+e.getMessage());
            }
        }
                
        
        public void testPersistentUnitVar()
        {
            try
            {
                Head head = new Head();
                head.setVar((byte) 1);
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.create(prefix + "testPersistentUnitVar_tmp", head); 
                            
                IndexInfo info = new IndexInfo();
                info.offset = 0;
                Segment seg = new Segment(info, fd);
                Unit unit = new Unit(info, seg);
                
                Record record = new Record( 7);
                record.addValue(new FieldValue((byte)1, (short)0));
                record.addValue(new FieldValue((short)2, (short)1));
                record.addValue(new FieldValue((int)3, (short)2));
                record.addValue(new FieldValue((long)4, (short)3));
                record.addValue(new FieldValue((float)5.5, (short)4));
                record.addValue(new FieldValue((double)6.6, (short)5));
                record.addValue(new FieldValue("hello konten", (short)6));
                
                int count = 100;
                for(int i = 0; i < count; i++)
                {
                    unit.addRecord(record);
                }
                
                String file = prefix + "testPersistentUnitVar";
                Path path = new Path(file);
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataOutputStream out = fs.create(path);
                
                unit.persistent(out);
                long pos = out.getPos();
                if(pos != full7chunkLen * count +count*8+ConstVar.DataChunkMetaOffset)
                {
                    fail("error pos:"+pos);
                }
                out.close();
                
                
                long len = unit.len();
                if(len != count * full7chunkLen + count * 8 + ConstVar.DataChunkMetaOffset)
                {
                    fail("error unit.len"+len);
                }
                
                FSDataInputStream in = fs.open(path);
                in.seek(len - 8 - 4);
                long metaOffset = in.readLong();
                if(metaOffset != full7chunkLen*count)
                {
                    fail("error metaOffset:"+metaOffset);
                }
                
                in.seek(len - 8 - 4 - 4);
                int recordNum = in.readInt();
                if(recordNum != count)
                {
                    fail("error recordNum:"+recordNum);
                }
                
                in.seek(metaOffset);
                for(int i = 0; i < recordNum; i++)
                {
                    long offset = in.readLong();
                    if(offset != full7chunkLen * i)
                    {
                        fail("error offset:"+offset+"i:"+i);
                    }
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
                fail("get IOException:"+e.getMessage());
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get Exception:"+e.getMessage());
            }
        } 
        
        public void testPersistentUnitNotVar()
        {
            try
            {
                Head head = new Head();
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.create(prefix + "testPersistentUnitNotVar_tmp", head); 
                            
                IndexInfo info = new IndexInfo();
                info.offset = 0;
                Segment seg = new Segment(info, fd);
                Unit unit = new Unit(info, seg);
                
                Record record = new Record( 6);
                record.addValue(new FieldValue((byte)1, (short)0));
                record.addValue(new FieldValue((short)2, (short)1));
                record.addValue(new FieldValue((int)3, (short)2));
                record.addValue(new FieldValue((long)4, (short)3));
                record.addValue(new FieldValue((float)5.5, (short)4));
                record.addValue(new FieldValue((double)6.6, (short)5));
                
                int count = 100;
                for(int i = 0; i < count; i++)
                {
                    unit.addRecord(record);
                }
                
                String file = prefix + "testPersistentUnitNotVar";
                Path path = new Path(file);
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataOutputStream out = fs.create(path);
                
                unit.persistent(out);
                long pos = out.getPos();
                if(pos != full6chunkLen * count +ConstVar.DataChunkMetaOffset)
                {
                    fail("error pos:"+pos);
                }
                out.close();
                
                
                long len = unit.len();
                if(len != count * full6chunkLen + ConstVar.DataChunkMetaOffset)
                {
                    fail("error unit.len"+len);
                }
                
                FSDataInputStream in = fs.open(path);
                in.seek(len - 8 - 4);
                long metaOffset = in.readLong();
                if(metaOffset != full6chunkLen*count)
                {
                    fail("error metaOffset:"+metaOffset);
                }
                
                in.seek(len - 8 - 4 - 4);
                int recordNum = in.readInt();
                if(recordNum != count)
                {
                    fail("error recordNum:"+recordNum);
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
                fail("get IOException:"+e.getMessage());
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get Exception:"+e.getMessage());
            }
        } 
        
        public void testUnpersistenUnitVar()
        {
            try
            {
                FieldMap fieldMap = new FieldMap();
                fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
                fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
                fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
                fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
                fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
                fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
                fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short)6));
                
                
                Head head = new Head();
                head.setFieldMap(fieldMap);
                
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.setWorkStatus(ConstVar.WS_Read);
                fd.head = head;
                
                IndexInfo info = new IndexInfo();
                info.offset = 0;
                info.len = 100 * full7chunkLen + 100 * 8 + ConstVar.DataChunkMetaOffset;
                Segment seg = new Segment(info, fd);
                Unit unit = new Unit(info, seg);                
            
                String file = prefix + "testPersistentUnitVar";
                Path path = new Path(file);
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataInputStream in = fs.open(path);
                
                byte[] buffer = unit.loadUnitBuffer(in);
                
                unit.loadDataMeta(buffer, true);
                
                if(unit.recordNum() != 100)
                {
                    fail("error recordNum:"+unit.recordNum());
                }
                
                if(unit.offsetArray() == null)
                {
                    fail("error offsetArray, null");
                }
                
                if(unit.offsetArray().length != 100)
                {
                    fail("error offsetArray len:"+unit.offsetArray().length);
                }
                
                ArrayList<byte[]> arrayList = new ArrayList<byte[]>(64);
                
                ByteArrayInputStream stream1 = new ByteArrayInputStream(buffer);
                DataInputStream stream = new DataInputStream(stream1);
                DataInputBuffer inputBuffer = new DataInputBuffer();
                inputBuffer.reset(buffer, 0, buffer.length);
                for(int i = 0; i < unit.offsetArray().length; i++)
                {
                    if(unit.offsetArray()[i] != full7chunkLen * i)
                    {
                        fail("error meta offset:"+unit.offsetArray()[i]+"i:"+i);
                    }
                    
                    DataChunk chunk = new DataChunk((short) 7);
                    chunk.unpersistent(unit.offsetArray()[i], full7chunkLen, inputBuffer);
                    
                    Record record = chunk.toRecord(fieldMap, true, arrayList);
                    judgeFixedRecord(record);
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
                fail("get IOException:"+e.getMessage());
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get Exception:"+e.getMessage());
            }
        }       
        
        public void testUnpersistenUnitNotVar()
        {
            try
            {
                FieldMap fieldMap = new FieldMap();
                fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
                fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
                fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
                fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
                fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
                fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
                
                
                Head head = new Head();
                head.setFieldMap(fieldMap);
                
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.setWorkStatus(ConstVar.WS_Read);
                fd.head = head;
                
                IndexInfo info = new IndexInfo();
                info.offset = 0;
                info.len = 100 * full6chunkLen + ConstVar.DataChunkMetaOffset;
                Segment seg = new Segment(info, fd);
                Unit unit = new Unit(info, seg);                
            
                String file = prefix + "testPersistentUnitNotVar";
                Path path = new Path(file);
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataInputStream in = fs.open(path);
                
                byte[] buffer = unit.loadUnitBuffer(in);
                
                unit.loadDataMeta(buffer, false);
                
                if(unit.recordNum() != 100)
                {
                    fail("error recordNum:"+unit.recordNum());
                }
                
                ArrayList<byte[]> arrayList = new ArrayList<byte[]>(64);
                
                ByteArrayInputStream stream1 = new ByteArrayInputStream(buffer);
                DataInputStream stream = new DataInputStream(stream1);
                DataInputBuffer inputBuffer = new DataInputBuffer();
                inputBuffer.reset(buffer, 0, buffer.length);
                for(int i = 0; i < 100; i++)
                {
                    DataChunk chunk = new DataChunk((short) 6);
                    chunk.unpersistent(i*29, full6chunkLen, inputBuffer);
                    
                    Record record = chunk.toRecord(fieldMap, true, arrayList);
                    judgeFixedRecord(record);
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
                fail("get IOException:"+e.getMessage());
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get Exception:"+e.getMessage());
            }
        }       
        
        public void testTransferUnit()
        {
            try
            {
                Head head = new Head();
                head.setVar((byte) 1);
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.create(prefix + "testTransferUnitOneRecord_tmp", head); 
                            
                IndexInfo info = new IndexInfo();
                info.offset = 123;
                Segment seg = new Segment(info, fd);
                Unit unit = new Unit(info, seg);
                
                Record record = new Record( 7);
                record.addValue(new FieldValue((byte)1, (short)0));
                record.addValue(new FieldValue((short)2, (short)1));
                record.addValue(new FieldValue((int)3, (short)2));
                record.addValue(new FieldValue((long)4, (short)3));
                record.addValue(new FieldValue((float)5.5, (short)4));
                record.addValue(new FieldValue((double)6.6, (short)5));
                record.addValue(new FieldValue("hello konten", (short)6));
                
                for(int i = 0; i < 100; i++)
                {
                    unit.addRecord(record);
                }
                
                if(unit.offset() != 123)
                {
                    fail("error offset1:"+unit.offset());
                }
                
                
                DataInputBuffer inputBuffer = new DataInputBuffer();
                inputBuffer.reset(((DataOutputBuffer)unit.metasBuffer).getData(), 0, ((DataOutputBuffer)unit.metasBuffer).getLength());
                for(int i = 0; i < 100; i++)
                {         
                    long value = inputBuffer.readLong(); 
                    if(value != 123 + i*full7chunkLen)
                    {
                        fail("error data offset1:"+value+"i:"+i);
                    }
                }
                
                if(unit.metaOffset() != 123 + full7chunkLen*100)
                {
                    fail("error metaOffset1:"+unit.metaOffset());
                }
                
                unit.transfer(2000);
                
                if(unit.offset() != 2000)
                {
                    fail("error offset2:"+unit.offset());
                }
                
                
                inputBuffer.reset(((DataOutputBuffer)unit.metasBuffer).getData(), 0, ((DataOutputBuffer)unit.metasBuffer).getLength());
                for(int i = 0; i < 100; i++)
                {
                    long value = inputBuffer.readLong();
                    if(value != 2000 + i*full7chunkLen)
                    {
                        fail("error data offset2:"+value+"i:"+i);
                    }
                }
                if(unit.metaOffset() != 2000 + full7chunkLen*100)
                {
                    fail("error metaOffset2:"+unit.metaOffset());
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
                fail("get IOException:"+e.getMessage());
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get Exception:"+e.getMessage());
            }
        }       
                
    
        public void testGetRecordByLineUnit()
        {
            try
            {               
                Head head = new Head();
                head.setVar((byte) 1);
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.create(prefix + "testUnitGetRecordByLine_tmp", head); 
                            
                String fileName = prefix + "testUnitGetRecordByLine";
                Path path = new Path(fileName);
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataOutputStream out = fs.create(path);
                
                IndexInfo info = new IndexInfo();
                info.offset = 123;
                Segment seg = new Segment(info, fd);
                Unit unit = new Unit(info, seg);
                for(int i = 0; i < 100; i++)
                {
                    Record record = new Record( 7);
                    record.addValue(new FieldValue((byte)(0+i), (short)0));
                    record.addValue(new FieldValue((short)(1+i), (short)1));
                    record.addValue(new FieldValue((int)(2+i), (short)2));
                    record.addValue(new FieldValue((long)(3+i), (short)3));
                    record.addValue(new FieldValue((float)(4.4+i), (short)4));
                    record.addValue(new FieldValue((double)(5.55+i), (short)5));
                    record.addValue(new FieldValue("hello konten"+i, (short)6));
                                
                    unit.addRecord(record);
                    
                }
                
                if(unit.beginLine() != 0)
                {
                    fail("error beginLine:"+unit.beginLine());
                }
                if(unit.endLine() != 100)
                {
                    fail("error endLine:"+unit.endLine());
                }
                byte[] buf = new byte[(int) unit.offset()];
                out.write(buf);
                
                unit.persistent(out);                   
                out.close();
                
                info.len = unit.len();  
                info.beginLine = unit.beginLine();
                info.endLine = unit.endLine();
                
                FSDataInputStream in = fs.open(path);
                
                FieldMap fieldMap = new FieldMap();
                fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
                fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
                fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
                fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
                fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
                fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
                fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short)6));
                head.setFieldMap(fieldMap);
                                
                FormatDataFile fd2 = new FormatDataFile(conf);
                fd2.head = head;
                fd2.setIn(in);
                Segment seg2 = new Segment(info, fd2);
                Unit unit2 = new Unit(info, seg2);
                
                if(unit2.beginLine() != 0)
                {
                    fail("error begin line:"+unit2.beginLine());
                }
                if(unit2.endLine() != 100)
                {
                    fail("error end line :"+unit2.endLine());
                }
                
                
                
                try
                {
                    Record record = unit2.getRecordByLine(-1);
                    if(record != null)
                    {
                        fail("should get null");
                    }
                }               
                catch(Exception e)
                {
                    fail("get exception:"+e.getMessage());
                }
                try
                {
                    Record record = unit2.getRecordByLine(120);
                    if(record != null)
                    {
                        fail("should get null");
                    }
                }               
                catch(Exception e)
                {
                    fail("get exception:"+e.getMessage());
                }
                
                for(int i = 0; i < 100; i++)
                {
                    try
                    {
                        Record record = unit2.getRecordByLine(i);
                    
                        short index = 0;                    
                        byte type = record.getType(index);
                        int len = record.getLen(index);
                        byte[] value = record.getValue(index);
                        short idx = record.getIndex(index);
                        if(type != ConstVar.FieldType_Byte)
                        {
                            fail("fail type:"+type);
                        }
                        if(idx != 0)
                        {
                            fail("error idx:"+idx);
                        }
                        if(len != ConstVar.Sizeof_Byte)
                        {
                            fail("fail len:"+len);                      
                        }
                        if(value == null)
                        {
                            fail("error value null");                       
                        }
                        {
                        }
                        byte bv = value[0];
                        if(bv != i+index)
                        {
                            fail("error value:"+bv);
                        }
                        
                        index = 1;                  
                        type = record.getType(index);
                        len = record.getLen(index);
                        value = record.getValue(index);
                        idx = record.getIndex(index);
                        if(type != ConstVar.FieldType_Short)
                        {
                            fail("fail type:"+type);
                        }
                        if(idx != 1)
                        {
                            fail("error idx:"+idx);
                        }
                        if(len != ConstVar.Sizeof_Short)
                        {
                            fail("fail len:"+len);                      
                        }
                        if(value == null)
                        {
                            fail("error value null");                       
                        }
                        {
                        }
                        short sv = Util.bytes2short(value, 0, 2);
                        if(sv != i+index)
                        {
                            fail("error value:"+sv);
                        }
                        
                        index = 2;                  
                        type = record.getType(index);
                        len = record.getLen(index);
                        value = record.getValue(index);
                        idx = record.getIndex(index);
                        if(type != ConstVar.FieldType_Int)
                        {
                            fail("fail type:"+type);
                        }
                        if(idx != 2)
                        {
                            fail("error idx:"+idx);
                        }
                        if(len != ConstVar.Sizeof_Int)
                        {
                            fail("fail len:"+len);                      
                        }
                        if(value == null)
                        {
                            fail("error value null");                       
                        }
                        {
                        }
                        int iv = Util.bytes2int(value, 0, 4);
                        if(iv != i+index)
                        {
                            fail("error value:"+iv);
                        }
                        
                        index = 3;                  
                        type = record.getType(index);
                        len = record.getLen(index);
                        value = record.getValue(index);
                        idx = record.getIndex(index);
                        if(type != ConstVar.FieldType_Long)
                        {
                            fail("fail type:"+type);
                        }
                        if(idx != 3)
                        {
                            fail("fail idx:"+idx);
                        }
                        if(len != ConstVar.Sizeof_Long)
                        {
                            fail("fail len:"+len);                      
                        }
                        if(value == null)
                        {
                            fail("error value null");                       
                        }
                        {
                        }
                        long lv = Util.bytes2long(value, 0, 8);
                        if(lv != i+index)
                        {
                            fail("error value:"+lv);
                        }
                        
                        index = 4;                  
                        type = record.getType(index);
                        len = record.getLen(index);
                        value = record.getValue(index);
                        idx = record.getIndex(index);
                        if(type != ConstVar.FieldType_Float)
                        {
                            fail("fail type:"+type);
                        }
                        if(idx != 4)
                        {
                            fail("error idx:"+idx);                     
                        }
                        if(len != ConstVar.Sizeof_Float)
                        {
                            fail("fail len:"+len);                      
                        }
                        if(value == null)
                        {
                            fail("error value null");                       
                        }
                        {
                        }
                        float fv = Util.bytes2float(value, 0);
                        if(fv != (float)(4.4+i))
                        {
                            fail("error value:"+fv);
                        }
                        
                        index = 5;                  
                        type = record.getType(index);
                        len = record.getLen(index);
                        value = record.getValue(index);
                        idx = record.getIndex(index);
                        if(type != ConstVar.FieldType_Double)
                        {
                            fail("fail type:"+type);
                        }
                        if(idx != 5)
                        {
                            fail("error idx:"+idx);                     
                        }
                        if(len != ConstVar.Sizeof_Double)
                        {
                            fail("fail len:"+len);                      
                        }
                        if(value == null)
                        {
                            fail("error value null");                       
                        }
                        {
                        }
                        double dv = Util.bytes2double(value, 0);
                        if(dv != (double)(5.55+i))
                        {
                            fail("error value:"+dv);
                        }
                        
                        index = 6;                  
                        type = record.getType(index);
                        len = record.getLen(index);
                        value = record.getValue(index);
                        idx = record.getIndex(index);
                        if(type != ConstVar.FieldType_String)
                        {
                            fail("fail type:"+type);
                        }
                        if(idx != 6)
                        {
                            fail("error idx:"+idx);                     
                        }
                        String str = "hello konten"+i;
                        if(len != str.length())
                        {
                            fail("fail len:"+len);                      
                        }
                        if(value == null)
                        {
                            fail("error value null");                       
                        }
                        {
                        }
                        
                        String strv = new String(value, 0, len);
                        if(!str.equals(strv))
                        {
                            fail("error value:"+strv);
                        }
                    }
                    catch(Exception e)
                    {
                        e.printStackTrace();
                        fail("get exception:"+e.getMessage());
                    }
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
                fail("get IOException:"+e.getMessage());
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get Exception:"+e.getMessage());
            }
        }
        
        public void testGetRecordByOrderUnit()
        {
            try
            {               
                Head head = new Head();
                head.setVar((byte) 1);
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.create(prefix + "testUnitGetRecordByOrder_tmp", head); 
                            
                String fileName = prefix + "testUnitGetRecordByOrder";
                Path path = new Path(fileName);
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataOutputStream out = fs.create(path);
                
                IndexInfo info = new IndexInfo();
                info.offset = 123;
                Segment seg = new Segment(info, fd);
                Unit unit = new Unit(info, seg);
                for(int i = 0; i < 100; i++)
                {
                    Record record = new Record( 7);
                    record.addValue(new FieldValue((byte)(0+i), (short)0));
                    record.addValue(new FieldValue((short)(1+i), (short)1));
                    record.addValue(new FieldValue((int)(2+i), (short)2));
                    record.addValue(new FieldValue((long)(3+i), (short)3));
                    record.addValue(new FieldValue((float)(4.4+i), (short)4));
                    record.addValue(new FieldValue((double)(5.55+i), (short)5));
                    record.addValue(new FieldValue("hello konten"+i, (short)6));
                                
                    unit.addRecord(record);
                    
                }
                
                if(unit.beginLine() != 0)
                {
                    fail("error beginLine:"+unit.beginLine());
                }
                if(unit.endLine() != 100)
                {
                    fail("error endLine:"+unit.endLine());
                }
                byte[] buf = new byte[(int) unit.offset()];
                out.write(buf);
                
                unit.persistent(out);                   
                out.close();
                
                info.len = unit.len();  
                info.beginLine = unit.beginLine();
                info.endLine = unit.endLine();
                
                FSDataInputStream in = fs.open(path);
                
                FieldMap fieldMap = new FieldMap();
                fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
                fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
                fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
                fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
                fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
                fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
                fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short)6));
                head.setFieldMap(fieldMap);
      
                FormatDataFile fd2 = new FormatDataFile(conf);
                fd2.head = head;
                fd2.setIn(in);
                Segment seg2 = new Segment(info, fd2);
                Unit unit2 = new Unit(info, seg2);
                
                if(unit2.beginLine() != 0)
                {
                    fail("error begin line:"+unit2.beginLine());
                }
                if(unit2.endLine() != 100)
                {
                    fail("error end line :"+unit2.endLine());
                }
                
                
                    
                Record[] records = unit2.getRecordByValue(null, 0, ConstVar.OP_GetAll);
                if(records.length != 100)
                {
                    fail("error record.len:"+records.length);
                }
                
                for(int i = 0; i < 100; i++)
                {
                    Record record = records[i];
                    try
                    {
                        short index = 0;                    
                        byte type = record.getType(index);
                        int len = record.getLen(index);
                        byte[] value = record.getValue(index);
                        short idx = record.getIndex(index);
                        if(type != ConstVar.FieldType_Byte)
                        {
                            fail("fail type:"+type);
                        }
                        if(idx != 0)
                        {
                            fail("error idx:"+idx);
                        }
                        if(len != ConstVar.Sizeof_Byte)
                        {
                            fail("fail len:"+len);                      
                        }
                        if(value == null)
                        {
                            fail("error value null");                       
                        }
                        {
                        }
                        byte bv = value[0];
                        if(bv != (byte)(i+index))
                        {
                            fail("error value:"+bv);
                        }
                        
                        index = 1;                  
                        type = record.getType(index);
                        len = record.getLen(index);
                        value = record.getValue(index);
                        idx = record.getIndex(index);
                        if(type != ConstVar.FieldType_Short)
                        {
                            fail("fail type:"+type);
                        }
                        if(idx != 1)
                        {
                            fail("error idx:"+idx);
                        }
                        if(len != ConstVar.Sizeof_Short)
                        {
                            fail("fail len:"+len);                      
                        }
                        if(value == null)
                        {
                            fail("error value null");                       
                        }
                        {
                        }
                        short sv = Util.bytes2short(value, 0, 2);
                        if(sv != i+index)
                        {
                            fail("error value:"+sv);
                        }
                        
                        index = 2;                  
                        type = record.getType(index);
                        len = record.getLen(index);
                        value = record.getValue(index);
                        idx = record.getIndex(index);
                        if(type != ConstVar.FieldType_Int)
                        {
                            fail("fail type:"+type);
                        }
                        if(idx != 2)
                        {
                            fail("error idx:"+idx);
                        }
                        if(len != ConstVar.Sizeof_Int)
                        {
                            fail("fail len:"+len);                      
                        }
                        if(value == null)
                        {
                            fail("error value null");                       
                        }
                        {
                        }
                        int iv = Util.bytes2int(value, 0, 4);
                        if(iv != i+index)
                        {
                            fail("error value:"+iv);
                        }
                        
                        index = 3;                  
                        type = record.getType(index);
                        len = record.getLen(index);
                        value = record.getValue(index);
                        idx = record.getIndex(index);
                        if(type != ConstVar.FieldType_Long)
                        {
                            fail("fail type:"+type);
                        }
                        if(idx != 3)
                        {
                            fail("fail idx:"+idx);
                        }
                        if(len != ConstVar.Sizeof_Long)
                        {
                            fail("fail len:"+len);                      
                        }
                        if(value == null)
                        {
                            fail("error value null");                       
                        }
                        {
                        }
                        long lv = Util.bytes2long(value, 0, 8);
                        if(lv != i+index)
                        {
                            fail("error value:"+sv);
                        }
                        
                        index = 4;                  
                        type = record.getType(index);
                        len = record.getLen(index);
                        value = record.getValue(index);
                        idx = record.getIndex(index);
                        if(type != ConstVar.FieldType_Float)
                        {
                            fail("fail type:"+type);
                        }
                        if(idx != 4)
                        {
                            fail("error idx:"+idx);                     
                        }
                        if(len != ConstVar.Sizeof_Float)
                        {
                            fail("fail len:"+len);                      
                        }
                        if(value == null)
                        {
                            fail("error value null");                       
                        }
                        {
                        }
                        float fv = Util.bytes2float(value, 0);
                        if(fv != (float)(4.4+i))
                        {
                            fail("error value:"+fv);
                        }
                        
                        index = 5;                  
                        type = record.getType(index);
                        len = record.getLen(index);
                        value = record.getValue(index);
                        idx = record.getIndex(index);
                        if(type != ConstVar.FieldType_Double)
                        {
                            fail("fail type:"+type);
                        }
                        if(idx != 5)
                        {
                            fail("error idx:"+idx);                     
                        }
                        if(len != ConstVar.Sizeof_Double)
                        {
                            fail("fail len:"+len);                      
                        }
                        if(value == null)
                        {
                            fail("error value null");                       
                        }
                        {
                        }
                        double dv = Util.bytes2double(value, 0);
                        if(dv != (double)(5.55+i))
                        {
                            fail("error value:"+dv);
                        }
                        
                        index = 6;                  
                        type = record.getType(index);
                        len = record.getLen(index);
                        value = record.getValue(index);
                        idx = record.getIndex(index);
                        if(type != ConstVar.FieldType_String)
                        {
                            fail("fail type:"+type);
                        }
                        if(idx != 6)
                        {
                            fail("error idx:"+idx);                     
                        }
                        String str = "hello konten"+i;
                        if(len != str.length())
                        {
                            fail("fail len:"+len);                      
                        }
                        if(value == null)
                        {
                            fail("error value null");                       
                        }
                        {
                        }
                        
                        String strv = new String(value);
                        if(!str.equals(strv))
                        {
                            fail("error value:"+strv);
                        }
                    }
                    catch(Exception e)
                    {
                        e.printStackTrace();
                        fail("get exception:"+e.getMessage());
                    }
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
                fail("get IOException:"+e.getMessage());
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get Exception:"+e.getMessage());
            }
        }
        
        public void testGetRecordByValueUnit()
        {
            try
            {               
                Head head = new Head();
                head.setVar((byte) 1);
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.create(prefix + "testUnitGetRecordByValue_tmp", head); 
                            
                String fileName = prefix + "testUnitGetRecordByValue";
                Path path = new Path(fileName);
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataOutputStream out = fs.create(path);
                
                IndexInfo info = new IndexInfo();
                info.offset = 123;
                Segment seg = new Segment(info, fd);
                Unit unit = new Unit(info, seg);
                for(int i = 0; i < 100; i++)
                {
                    Record record = new Record( 7);
                    record.addValue(new FieldValue((byte)(0+i), (short)0));
                    record.addValue(new FieldValue((short)(1+i), (short)1));
                    record.addValue(new FieldValue((int)(2+i), (short)2));
                    record.addValue(new FieldValue((long)(3+i), (short)3));
                    record.addValue(new FieldValue((float)(4.4+i), (short)4));
                    record.addValue(new FieldValue((double)(5.55+i), (short)5));
                    record.addValue(new FieldValue("hello konten"+i, (short)6));
                                
                    unit.addRecord(record);
                    
                }
                
                if(unit.beginLine() != 0)
                {
                    fail("error beginLine:"+unit.beginLine());
                }
                if(unit.endLine() != 100)
                {
                    fail("error endLine:"+unit.endLine());
                }
                byte[] buf = new byte[(int) unit.offset()];
                out.write(buf);
                
                unit.persistent(out);                   
                out.close();
                
                info.len = unit.len();  
                info.beginLine = unit.beginLine();
                info.endLine = unit.endLine();
                
                FSDataInputStream in = fs.open(path);
                
                FieldMap fieldMap = new FieldMap();
                fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
                fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
                fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
                fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
                fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
                fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
                fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short)6));
                head.setFieldMap(fieldMap);
                
                FormatDataFile fd2 = new FormatDataFile(conf);
                fd2.head = head;
                fd2.setIn(in);
                Segment seg2 = new Segment(info, fd2);
                Unit unit2 = new Unit(info, seg2);
                
                if(unit2.beginLine() != 0)
                {
                    fail("error begin line:"+unit2.beginLine());
                }
                if(unit2.endLine() != 100)
                {
                    fail("error end line :"+unit2.endLine());
                }
                
                
                
                FieldValue[] values1 = new FieldValue[2];
                values1[0] = new FieldValue((short)(3), (short)3);
                values1[1] = new FieldValue((int)(3), (short)5);
                Record[] records1 = unit2.getRecordByValue(values1, values1.length, ConstVar.OP_GetSpecial);
                if(records1 != null)
                {
                    fail("should return null");
                }
                
                seg2.units().add(unit2);
                
                for(int i = 0; i < 100; i++)
                {
                    int base = 0;
                    FieldValue[] values = new FieldValue[2];
                    values[0] = new FieldValue((short)(1+i), (short)1);
                    values[1] = new FieldValue((int)(2+i), (short)2);
                    Record[] records = unit2.getRecordByValue(values, values.length, ConstVar.OP_GetSpecial);
                    if( i < 100)
                    {
                        if(records == null)
                        {
                            fail("records null:"+i);
                        }
                        
                        if(records.length != 1)
                        {
                            fail("error record.len:"+records.length+"i:"+i);
                        }
                    }
                    else
                    {
                        if(records != null)
                        {
                            fail("should return null:"+i);
                        }                       
                    }
                    
                    if(records == null)
                    {
                        continue;
                    }
                    
                    Record record = records[0];
                    try
                    {
                        short index = 0;                    
                        byte type = record.getType(index);
                        int len = record.getLen(index);
                        byte[] value = record.getValue(index);
                        short idx = record.getIndex(index);
                        if(type != ConstVar.FieldType_Byte)
                        {
                            fail("fail type:"+type);
                        }
                        if(idx != 0)
                        {
                            fail("error idx:"+idx);
                        }
                        if(len != ConstVar.Sizeof_Byte)
                        {
                            fail("fail len:"+len);                      
                        }
                        if(value == null)
                        {
                            fail("error value null");                       
                        }
                        {
                        }
                        byte bv = value[0];
                        if(bv != (byte)(i+index + base))
                        {
                            fail("error value:"+bv);
                        }
                        
                        index = 1;                  
                        type = record.getType(index);
                        len = record.getLen(index);
                        value = record.getValue(index);
                        idx = record.getIndex(index);
                        if(type != ConstVar.FieldType_Short)
                        {
                            fail("fail type:"+type);
                        }
                        if(idx != 1)
                        {
                            fail("error idx:"+idx);
                        }
                        if(len != ConstVar.Sizeof_Short)
                        {
                            fail("fail len:"+len);                      
                        }
                        if(value == null)
                        {
                            fail("error value null");                       
                        }
                        {
                        }
                        short sv = Util.bytes2short(value, 0, 2);
                        if(sv != i+index+ base)
                        {
                            fail("error value:"+sv);
                        }
                        
                        index = 2;                  
                        type = record.getType(index);
                        len = record.getLen(index);
                        value = record.getValue(index);
                        idx = record.getIndex(index);
                        if(type != ConstVar.FieldType_Int)
                        {
                            fail("fail type:"+type);
                        }
                        if(idx != 2)
                        {
                            fail("error idx:"+idx);
                        }
                        if(len != ConstVar.Sizeof_Int)
                        {
                            fail("fail len:"+len);                      
                        }
                        if(value == null)
                        {
                            fail("error value null");                       
                        }
                        {
                        }
                        int iv = Util.bytes2int(value, 0, 4);
                        if(iv != i+index+ base)
                        {
                            fail("error value:"+iv);
                        }
                        
                        index = 3;                  
                        type = record.getType(index);
                        len = record.getLen(index);
                        value = record.getValue(index);
                        idx = record.getIndex(index);
                        if(type != ConstVar.FieldType_Long)
                        {
                            fail("fail type:"+type);
                        }
                        if(idx != 3)
                        {
                            fail("fail idx:"+idx);
                        }
                        if(len != ConstVar.Sizeof_Long)
                        {
                            fail("fail len:"+len);                      
                        }
                        if(value == null)
                        {
                            fail("error value null");                       
                        }
                        {
                        }
                        long lv = Util.bytes2long(value, 0, 8);
                        if(lv != i+index+ base)
                        {
                            fail("error value:"+sv);
                        }
                        
                        index = 4;                  
                        type = record.getType(index);
                        len = record.getLen(index);
                        value = record.getValue(index);
                        idx = record.getIndex(index);
                        if(type != ConstVar.FieldType_Float)
                        {
                            fail("fail type:"+type);
                        }
                        if(idx != 4)
                        {
                            fail("error idx:"+idx);                     
                        }
                        if(len != ConstVar.Sizeof_Float)
                        {
                            fail("fail len:"+len);                      
                        }
                        if(value == null)
                        {
                            fail("error value null");                       
                        }
                        {
                        }
                        float fv = Util.bytes2float(value, 0);
                        if(fv != (float)(4.4+i+ base))
                        {
                            fail("error value:"+fv);
                        }
                        
                        index = 5;                  
                        type = record.getType(index);
                        len = record.getLen(index);
                        value = record.getValue(index);
                        idx = record.getIndex(index);
                        if(type != ConstVar.FieldType_Double)
                        {
                            fail("fail type:"+type);
                        }
                        if(idx != 5)
                        {
                            fail("error idx:"+idx);                     
                        }
                        if(len != ConstVar.Sizeof_Double)
                        {
                            fail("fail len:"+len);                      
                        }
                        if(value == null)
                        {
                            fail("error value null");                       
                        }
                        {
                        }
                        double dv = Util.bytes2double(value, 0);
                        if(dv != (double)(5.55+i+ base))
                        {
                            fail("error value:"+dv);
                        }
                        
                        index = 6;                  
                        type = record.getType(index);
                        len = record.getLen(index);
                        value = record.getValue(index);
                        idx = record.getIndex(index);
                        if(type != ConstVar.FieldType_String)
                        {
                            fail("fail type:"+type);
                        }
                        if(idx != 6)
                        {
                            fail("error idx:"+idx);                     
                        }
                        String str = "hello konten"+(i+ base);
                        if(len != str.length())
                        {
                            fail("fail len:"+len);                      
                        }
                        if(value == null)
                        {
                            fail("error value null");                       
                        }
                        {
                        }
                        
                        String strv = new String(value);
                        if(!str.equals(strv))
                        {
                            fail("error value:"+strv);
                        }
                    }
                    catch(Exception e)
                    {
                        e.printStackTrace();
                        fail("get exception:"+e.getMessage());
                    }
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
                fail("get IOException:"+e.getMessage());
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get Exception:"+e.getMessage());
            }
        }
    
    
    
        
        
        public void testInitSegment()
        {
            try
            {
                IndexInfo info = new IndexInfo();
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                Segment segment = new Segment(info, fd);
                
                if(segment.beginKey() != 0)
                {
                    fail("error begin key:"+segment.beginKey());
                }
                if(segment.endKey() != 0)
                {
                    fail("error end key:"+segment.endKey());
                }
                if(segment.beginLine() != 0)
                {
                    fail("error begin line:"+segment.beginLine());
                }
                if(segment.endLine() != 0)
                {
                    fail("error endLine:"+segment.endLine());
                }
                
                if(segment.offset() != -1)
                {
                    fail("error offset:"+segment.offset());
                }
                if(segment.currentOffset() != -1)
                {
                    fail("error current offset:"+segment.currentOffset());
                }
                if(segment.recordNum() != 0)
                {
                    fail("error record num :"+segment.recordNum());
                }
                if(segment.total() != 64*1024*1024) 
                {
                    fail("error total:"+segment.total());
                }
                if(segment.remain() != segment.total())
                {
                    fail("error remain:"+segment.remain());
                }
                if(segment.unitNum() != 0)
                {
                    fail("error unitNum:"+segment.unitNum());
                }
                if(segment.units().size() != 0)
                {
                    fail("error units.size:"+segment.units().size());
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
                fail("get IOException:"+e.getMessage());
                
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get exception:"+e.getMessage());
            }
            
            try
            {
                IndexInfo info = new IndexInfo();
                info.offset = 12;
                
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.create(prefix + "testInitSegment_tmp1", new Head());
                
                Segment segment = new Segment(info, fd);
                
                if(segment.beginKey() != 0)
                {
                    fail("error begin key:"+segment.beginKey());
                }
                if(segment.endKey() != 0)
                {
                    fail("error end key:"+segment.endKey());
                }
                if(segment.beginLine() != 0)
                {
                    fail("error begin line:"+segment.beginLine());
                }
                if(segment.endLine() != 0)
                {
                    fail("error endLine:"+segment.endLine());
                }
                
                if(segment.offset() != 12)
                {
                    fail("error offset:"+segment.offset());
                }
                if(segment.currentOffset() != 12)
                {
                    fail("error current offset:"+segment.currentOffset());
                }
                if(segment.recordNum() != 0)
                {
                    fail("error record num :"+segment.recordNum());
                }
                if(segment.total() != 64*1024*1024 - 12) 
                {
                    fail("error total:"+segment.total());
                }
                if(segment.remain() != segment.total() - 24)
                {
                    fail("error remain:"+segment.remain());
                }
                if(segment.unitNum() != 0)
                {
                    fail("error unitNum:"+segment.unitNum());
                }
                if(segment.units().size() != 0)
                {
                    fail("error units.size:"+segment.units().size());
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
                fail("get IOException:"+e.getMessage());
                
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get exception:"+e.getMessage());
            }
        }
        
        public void testAddRecordSegment()
        {
            try
            {
                IndexInfo info = new IndexInfo();
                info.offset = 12;
                
                Head head = new Head();
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.create(prefix + "testAddRecordSegment_tmp", head);
                
                Segment segment = new Segment(info, fd);
                
                for(int i = 0; i < 100; i++)
                {
                    Record record = new Record( 7);
                    record.addValue(new FieldValue((byte)1, (short)0));
                    record.addValue(new FieldValue((short)2, (short)1));
                    record.addValue(new FieldValue((int)3, (short)2));
                    record.addValue(new FieldValue((long)4, (short)3));
                    record.addValue(new FieldValue((float)5.5, (short)4));
                    record.addValue(new FieldValue((double)6.6, (short)5));
                    record.addValue(new FieldValue("hello konten", (short)6));
                    
                    segment.addRecord(record);
                }
                
                if(segment.beginKey() != 0)
                {
                    fail("error begin key:"+segment.beginKey());
                }
                if(segment.endKey() != 0)
                {
                    fail("error end key:"+segment.endKey());
                }
                if(segment.beginLine() != 0)
                {
                    fail("error begin line:"+segment.beginLine());
                }
                if(segment.endLine() != 0)
                {
                    fail("error endLine:"+segment.endLine());
                }
                
                if(segment.offset() != 12)
                {
                    fail("error offset:"+segment.offset());
                }
                if(segment.currentOffset() != 12) 
                {
                    fail("error current offset:"+segment.currentOffset());
                }
                if(segment.recordNum() != 0) 
                {
                    fail("error record num :"+segment.recordNum());
                }
                if(segment.total() != 64*1024*1024 - 12) 
                {
                    fail("error total:"+segment.total());
                }
                if(segment.remain() != segment.total() - 24 ) 
                {
                    fail("error remain:"+segment.remain());
                }
                if(segment.unitNum() != 0)
                {
                    fail("error unitNum:"+segment.unitNum());
                }
                if(segment.units().size() != 0)
                {
                    fail("error units.size:"+segment.units().size());
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
                fail("get IOException:"+e.getMessage());                
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get exception:"+e.getMessage());
            }
        }
        
        public void testAddRecordSegmentFull()
        {
            int count = 1; 
            Segment segment = null;
            try
            {
                IndexInfo info = new IndexInfo();
                info.offset = 12;
                
                Head head = new Head();
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.create(prefix + "testAddRecordSegmentFull_tmp", head);
                
                segment = new Segment(info, fd);                
                
                for(int i = 0; ; i++, count++)
                {
                    Record record = new Record( 7);
                    record.addValue(new FieldValue((byte)1, (short)0));
                    record.addValue(new FieldValue((short)2, (short)1));
                    record.addValue(new FieldValue((int)3, (short)2));
                    record.addValue(new FieldValue((long)4, (short)3));
                    record.addValue(new FieldValue((float)5.5, (short)4));
                    record.addValue(new FieldValue((double)6.6, (short)5));
                    record.addValue(new FieldValue("hello konten", (short)6));
                    
                    segment.addRecord(record);
                    record = null;
                    
                    if(count == Integer.MAX_VALUE)
                        fail("should seg full exception");
                }               
            }
            catch(IOException e)
            {
                e.printStackTrace();
                fail("get IOException:"+e.getMessage());                
            }
            catch(SEException.SegmentFullException e)
            {               
                if(segment.recordNum() != count - segment.currentUnit().recordNum())
                {
                    fail("error record num:"+segment.recordNum());
                }       
                
                try
                {
                    if(segment.remain() > segment.currentUnit().len())
                    {
                        fail("should add, remain:"+segment.remain()+",unit.len:"+segment.currentUnit().len());
                    }
                }
                catch (Exception e1)
                {
                    e1.printStackTrace();
                    fail("get exception len:"+e1.getMessage());
                }
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get exception:"+e.getMessage());
            }
        }
        
        public void testAddUnitSegment()
        {
            try
            {
                IndexInfo info = new IndexInfo();
                info.offset = 12;
                
                Head head = new Head();
                head.setVar((byte) 1);
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.create(prefix + "testAddUnitSegment_tmp", head);
                
                Segment segment = new Segment(info, fd);
                    
                int unitSize = 10;
                for(int i = 0; i < unitSize; i++)
                {
                    IndexInfo indexInfo = new IndexInfo();
                    indexInfo.offset = i * 100;
                    indexInfo.len = 77;
                    indexInfo.beginLine = (i+1) * 100;
                    indexInfo.endLine = (i+2)*100;
                    indexInfo.idx = i;
                    
                    Unit unit = new Unit(indexInfo, segment);
                    addRecord2Unit(unit, 100);
                    unit.beginLine = (i+1) * 100;
                    unit.endLine = (i+2) * 100;
                    segment.addUnit(unit);  
                    if(unit.len() != (100*full7chunkLen+8*100+ConstVar.DataChunkMetaOffset))
                    {
                        fail("error unit.len:"+unit.len());
                    }
                }
                
                int unitlen = (100*full7chunkLen+8*100+ConstVar.DataChunkMetaOffset);
                                                
                if(segment.beginLine() != 100)
                {
                    fail("error begin line:"+segment.beginLine());
                }
                if(segment.endLine() != 1100)
                {
                    fail("error end line:"+segment.endLine());
                }
                if(segment.beginKey() != 0)
                {
                    fail("error begin key:"+segment.beginKey());
                }
                if(segment.endKey() != 0)
                {
                    fail("error end key:"+segment.endKey());
                }
                if(segment.offset() != 12)
                {
                    fail("error offset:"+segment.offset());
                }
                if(segment.currentOffset() != 12 + unitlen * 10)
                {
                    fail("error current offset:"+segment.currentOffset());
                }
                if(segment.unitNum() != unitSize)
                {
                    fail("error unitNum:"+segment.unitNum());
                }
                
                if(segment.unitIndex().len() != unitSize* ConstVar.LineIndexRecordLen)
                {
                    fail("error unit index len:"+segment.unitIndex().len());
                }
                
                long total = fd.confSegmentSize() - 12;
                if(segment.remain() != total - unitlen * unitSize - unitSize* ConstVar.LineIndexRecordLen - ConstVar.IndexMetaOffset)
                {
                    fail("error remain:"+segment.remain());
                }               
            }
            catch(IOException e)
            {
                e.printStackTrace();
                fail("get IOException:"+e.getMessage());                
            }           
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get exception:"+e.getMessage());
            }
        }
        
        public void testAddUnitSegmentFull()
        {
            int count = 0; 
            Segment segment = null;
            try
            {
                IndexInfo info = new IndexInfo();
                info.offset = 12;
                
                Head head = new Head();
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.create(prefix + "testAddUnitSegmentFull_tmp", head);
                
                segment = new Segment(info, fd);
                                
                for(int i = 0; ; i++, count++)
                {
                    IndexInfo indexInfo = new IndexInfo();
                    indexInfo.offset = i * 100;
                    indexInfo.len = 77;
                    indexInfo.beginLine = (i+1) * 100;
                    indexInfo.endLine = (i+2)*100;
                    indexInfo.idx = i;
                    
                    Unit unit = new Unit(indexInfo, segment);
                    unit.setMetaOffset(indexInfo.offset);
                    segment.addUnit(unit);                  
                }                                               
                
            }
            catch(IOException e)
            {
                e.printStackTrace();
                fail("get IOException:"+e.getMessage());                
            }   
            catch(SEException.SegmentFullException e)
            {
                if(segment.unitNum() != count)
                {
                    fail("error unit num:"+segment.unitNum());
                }
                                
                if(segment.remain() > 77 + ConstVar.LineIndexRecordLen)
                {
                    fail("should add, remain:"+segment.remain()+",need len:"+(77 + ConstVar.LineIndexRecordLen));
                }
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get exception:"+e.getMessage());
            }
        }
        
        public void testAddUnitLineIndex()
        {
            try
            {
                IndexInfo info = new IndexInfo();
                info.offset = 12;
                
                Head head = new Head();
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.create(prefix + "testAddUnitLineIndex_tmp", head);
                
                Segment segment = new Segment(info, fd);
                    
                int unitSize = 10;
                for(int i = 0; i < unitSize; i++)
                {
                    IndexInfo indexInfo = new IndexInfo();
                    indexInfo.offset = i * 100;
                    indexInfo.len = 77;
                    indexInfo.beginLine = (i+1) * 100;
                    indexInfo.endLine = (i+2)*100;
                    indexInfo.idx = i;
                    
                    Unit unit = new Unit(indexInfo, segment);
                    unit.beginLine = (i+1) * 100;
                    unit.endLine = (i+2) * 100;
                    unit.setFull();
                    unit.setMetaOffset(indexInfo.offset);
                    segment.addUnit(unit);  
                }
                    
                if(segment.unitIndex().len() != ConstVar.LineIndexRecordLen * unitSize)
                {
                    fail("error unitIndex len:"+segment.unitIndex().len());
                }
                        
                if(segment.unitIndex().lineIndexInfos().size() != unitSize)
                {
                    fail("error line index size:"+segment.unitIndex().lineIndexInfos().size());
                }
                if(segment.unitIndex().keyIndexInfos().size() != 0)
                {
                    fail("error key index size:"+segment.unitIndex().keyIndexInfos().size());
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
                fail("get IOException:"+e.getMessage());                
            }           
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get exception:"+e.getMessage());
            }
        }
        
        public void testAddUnitKeyIndex()
        {
            
        }
        
        
        
        public void testGetSegmentLen()
        {
            try
            {
                IndexInfo info = new IndexInfo();
                info.offset = 12;
                
                Head head = new Head();
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.create(prefix + "testGetSegmentLen_tmp1", head);
                
                Segment segment = new Segment(info, fd);
                
                for(int i = 0; i < 100; i++)
                {
                    Record record = new Record( 7);
                    record.addValue(new FieldValue((byte)1, (short)0));
                    record.addValue(new FieldValue((short)2, (short)1));
                    record.addValue(new FieldValue((int)3, (short)2));
                    record.addValue(new FieldValue((long)4, (short)3));
                    record.addValue(new FieldValue((float)5.5, (short)4));
                    record.addValue(new FieldValue((double)6.6, (short)5));
                    record.addValue(new FieldValue("hello konten", (short)6));
                    
                    segment.addRecord(record);
                }
                
                if(segment.recordNum() != 0)
                {
                    fail("error record num:"+segment.recordNum());
                }
                if(segment.len() != ConstVar.IndexMetaOffset)
                {
                    fail("error len:"+segment.len());
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
                fail("get IOException:"+e.getMessage());
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get Exception:e"+e.getMessage());             
            }
            
            try
            {
                IndexInfo info = new IndexInfo();
                info.offset = 12;
                
                Head head = new Head();
                head.setVar((byte)1);
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.create(prefix + "testGetSegmentLen_tmp2", head);
                
                Segment segment = new Segment(info, fd);
                    
                int unitSize = 10;
                for(int i = 0; i < unitSize; i++)
                {
                    IndexInfo indexInfo = new IndexInfo();
                    indexInfo.offset = i * 100;
                    indexInfo.len = 77;
                    indexInfo.beginLine = (i+1) * 100;
                    indexInfo.endLine = (i+2)*100;
                    indexInfo.idx = i;
                    
                    Unit unit = new Unit(indexInfo, segment);
                    unit.beginLine = (i+1) * 100;
                    unit.endLine = (i+2) * 100;
                    addRecord2Unit(unit, 100);
                    segment.addUnit(unit);
                    if(unit.len() != 100*full7chunkLen+8*100+ConstVar.DataChunkMetaOffset)
                    {
                        fail("error unitlen:"+unit.len());
                    }
                }
                
                int unitlen = 100*full7chunkLen+8*100+ConstVar.DataChunkMetaOffset;
                if(segment.len() != unitSize * unitlen + ConstVar.LineIndexRecordLen * unitSize + ConstVar.IndexMetaOffset)
                {
                    fail("error segment.len:"+segment.len());
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
                fail("get IOException:"+e.getMessage());
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get Exception:"+e.getMessage());
            }                       
        }
        
        public void testGetDummyLen()
        {
            try
            {
                IndexInfo info = new IndexInfo();
                info.offset = 12;
                
                Head head = new Head();
                head.setVar((byte) 1);
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.create(prefix + "testGetDummyLen_tmp", head);
                
                Segment segment = new Segment(info, fd);
                    
                int unitlen = 100*full7chunkLen+8*100+ConstVar.DataChunkMetaOffset;
                int unitSize = 10;
                for(int i = 0; i < unitSize; i++)
                {
                    IndexInfo indexInfo = new IndexInfo();
                    indexInfo.offset = i * 100;
                    indexInfo.len = 77;
                    indexInfo.beginLine = (i+1) * 100;
                    indexInfo.endLine = (i+2)*100;
                    indexInfo.idx = i;
                    
                    Unit unit = new Unit(indexInfo, segment);
                    unit.beginLine = (i+1) * 100;
                    unit.endLine = (i+2) * 100;
                    addRecord2Unit(unit, 100);
                    
                    segment.addUnit(unit);
                    if(unit.len() != unitlen)
                    {
                        fail("error unit.len:"+unit.len());
                    }
                    
                }
                
                long total = fd.confSegmentSize() - 12;
                if(segment.remain() != total - unitSize * unitlen - ConstVar.LineIndexRecordLen * unitSize - ConstVar.IndexMetaOffset)
                {
                    fail("error segment.remain:"+segment.remain());
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
                fail("get IOException:"+e.getMessage());
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get Exception:"+e.getMessage());
            }           
        }
        
        public void testPersistentLineUnitIndex()
        {           
            try
            {
                IndexInfo info = new IndexInfo();
                info.offset = 12;
                
                Head head = new Head();
                head.setVar((byte) 1);
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.create(prefix + "testPersistentLineUnitIndex_tmp", head);             
                
                Segment segment = new Segment(info, fd);
                    
                int unitSize = 100;
                for(int i = 0; i < unitSize; i++)
                {
                    IndexInfo indexInfo = new IndexInfo();
                    indexInfo.offset = i * 100;
                    indexInfo.len = 77;
                    indexInfo.beginLine = (i+1) * 100;
                    indexInfo.endLine = (i+2)*100;
                    indexInfo.idx = i;
                    
                    Unit unit = new Unit(indexInfo, segment);
                    addRecord2Unit(unit, 100);
                    
                    unit.beginLine = (i+1) * 100;
                    unit.endLine = (i+2) * 100;
                    
                    segment.addUnit(unit);
                }
                
                int unitlen = full7chunkLen*100+8*100+ConstVar.DataChunkMetaOffset;
                String fileName = prefix + "testPersistentLineUnitIndex";
                Path path = new Path(fileName);
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataOutputStream out = fs.create(path);
                
                segment.persistentUnitIndex(out);
                if(out.getPos() != unitSize * ConstVar.LineIndexRecordLen)
                {
                    fail("error pos:"+out.getPos());
                }
                out.close();
                
                if(segment.lineIndexOffset() != 0)
                {
                    fail("error line index offset:"+segment.lineIndexOffset());
                }
                if(segment.keyIndexOffset() != -1)
                {
                    fail("error key index offset:"+segment.keyIndexOffset());
                }
                
                FSDataInputStream in = fs.open(path);
                
                for(int i = 0; i < unitSize; i++)
                {
                    int beginLine = in.readInt();
                    int endLine = in.readInt();
                    long offset = in.readLong();
                    long len = in.readLong();
                    int idx = in.readInt();
                    
                    if(beginLine != (i+1) * 100)
                    {
                        fail("error begin line:"+beginLine+" i:"+i);
                    }
                    if(endLine != (i+2) * 100)
                    {
                        fail("error end line:"+endLine+" i:"+i);
                    }
                    if(offset != i * 100)
                    {
                        fail("error offset:"+offset+" i:"+i);
                    }
                    if(len != unitlen)
                    {
                        fail("error len:"+len+" i:"+i);
                    }
                    if(idx != i)
                    {
                        fail("error idx:"+idx+" i:"+i);
                    }
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
                fail("get IOException:"+e.getMessage());
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get Exception:"+e.getMessage());
            }       
        }
        
        public void testPersistentKeyUnitIndex()
        {
            
        }       
        
        public void testPersistentUnitIndexMeta()
        {
            try
            {
                IndexInfo info = new IndexInfo();
                info.offset = 12;
                
                Head head = new Head();
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.create(prefix + "testPersistentUnitIndexMeta_tmp", head);             
                
                Segment segment = new Segment(info, fd);
                    
                int unitSize = 100;
                for(int i = 0; i < unitSize; i++)
                {
                    IndexInfo indexInfo = new IndexInfo();
                    indexInfo.offset = i * 100;
                    indexInfo.len = 77;
                    indexInfo.beginLine = (i+1) * 100;
                    indexInfo.endLine = (i+2)*100;
                    indexInfo.idx = i;
                    
                    Unit unit = new Unit(indexInfo, segment);
                    unit.beginLine = (i+1) * 100;
                    unit.endLine = (i+2) * 100;
                    unit.setFull();
                    unit.setMetaOffset(indexInfo.offset);
                    segment.addUnit(unit);
                }               
                
                String fileName = prefix + "testPersistentUnitIndexMeta";
                Path path = new Path(fileName);
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataOutputStream out = fs.create(path);
                
                segment.recordNum = 234;
                segment.setBeginLine(1);
                segment.setEndLine(235);
                
                segment.persistentUnitIndexMeta(out);
                if(out.getPos() != ConstVar.IndexMetaOffset)
                {
                    fail("error pos:"+out.getPos());
                }
                out.close();
                
                FSDataInputStream in = fs.open(path);
                
                int recordNum = in.readInt();
                int unitNum = in.readInt();
                long keyIndexOffset = in.readLong();
                long lineIndexOffset = in.readLong();
                                    
                if(recordNum != 234)
                {
                    fail("error recordnum:"+recordNum);
                }
                if(unitNum != unitSize)
                {
                    fail("error unitNum:"+unitNum);
                }   
                if(keyIndexOffset != -1)
                {
                    fail("error key index offset:"+keyIndexOffset);                 
                }
                if(lineIndexOffset != -1)  
                {
                    fail("error line inded offset:"+lineIndexOffset);
                }               
            }
            catch(IOException e)
            {
                e.printStackTrace();
                fail("get IOException:"+e.getMessage());
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get Exception:"+e.getMessage());
            }       
        }
        
        public void testPersistentDummy()
        {
            try
            {
                IndexInfo info = new IndexInfo();
                info.offset = 12;
                
                Head head = new Head();
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.create(prefix + "testPersistentDummy_tmp", head);             
                
                Segment segment = new Segment(info, fd);
                    
                int unitSize = 100;
                for(int i = 0; i < unitSize; i++)
                {
                    IndexInfo indexInfo = new IndexInfo();
                    indexInfo.offset = i * 100;
                    indexInfo.len = 77;
                    indexInfo.beginLine = (i+1) * 100;
                    indexInfo.endLine = (i+2)*100;
                    indexInfo.idx = i;
                    
                    Unit unit = new Unit(indexInfo, segment);
                    unit.beginLine = (i+1) * 100;
                    unit.endLine = (i+2) * 100;
                    unit.setFull();
                    unit.setMetaOffset(indexInfo.offset);
                    segment.addUnit(unit);
                }               
                
                String fileName = prefix + "testPersistentDummy";
                Path path = new Path(fileName);
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataOutputStream out = fs.create(path);
                
                segment.persistentUnitIndex(out);
                
                segment.recordNum = 234;
                segment.setBeginLine(1);
                segment.setEndLine(235);
                segment.persistentUnitIndexMeta(out);
                
                segment.persistentDummy(out);               
                if(out.getPos() != segment.unitIndex().len() + ConstVar.IndexMetaOffset + segment.remain())
                {
                    fail("error pos:"+out.getPos());
                }
                out.close();                
            }
            catch(IOException e)
            {
                e.printStackTrace();
                fail("get IOException:"+e.getMessage());
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get Exception:"+e.getMessage());
            }       
        }
        
        public void testUnpersistentUnitIndexMeta()
        {
            try
            {
                IndexInfo info = new IndexInfo();
                info.offset = 12;
                
                Head head = new Head();
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.create(prefix + "testUnpersistentUnitIndexMeta_tmp", head);               
                
                Segment segment = new Segment(info, fd);
                
                String fileName = prefix + "testPersistentUnitIndexMeta";
                Path path = new Path(fileName);
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataInputStream in = fs.open(path);
                
                int unitSize = 100;
                
                                
                segment.unpersistentIndexMeta(in);
                
                if(segment.recordNum() != 234)
                {
                    fail("error record num:"+segment.recordNum());
                }
                if(segment.unitNum() != unitSize)
                {
                    fail("error unit num:"+segment.unitNum());
                }
                if(segment.keyIndexOffset() != -1)
                {
                    fail("error keyIndex offset:"+segment.keyIndexOffset());
                }
                if(segment.lineIndexOffset() != -1) 
                {
                    fail("error lineIndexOffset:"+segment.lineIndexOffset());
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
                fail("get ioexception:"+e.getMessage());
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get exception:"+e.getMessage());
            }
        }
        
        public void testUnpersistentLineUnitIndex()
        {
            try
            {
                IndexInfo info = new IndexInfo();
                info.offset = 12;
                
                Head head = new Head();
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.create(prefix + "testUnpersistentLineUnitIndex_tmp", head);               
                
                Segment segment = new Segment(info, fd);
                
                String fileName = prefix + "testPersistentLineUnitIndex";
                Path path = new Path(fileName);
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataInputStream in = fs.open(path);
                
                int unitSize = 100;
                            
                segment.setUnitIndex(new UnitIndex());
                segment.setUnitNum(unitSize);
                segment.unpersistentLineUnitIndex(in);
                
                if(segment.unitIndex().len() != ConstVar.LineIndexRecordLen * unitSize)
                {
                    fail("error unitIndex len:"+segment.unitIndex().len());
                }
                        
                if(segment.unitIndex().lineIndexInfos().size() != unitSize)
                {
                    fail("error line index size:"+segment.unitIndex().lineIndexInfos().size());
                }
                if(segment.unitIndex().keyIndexInfos().size() != 0)
                {
                    fail("error key index size:"+segment.unitIndex().keyIndexInfos().size());
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
                fail("get ioexception:"+e.getMessage());
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get exception:"+e.getMessage());
            }
        }
        
        public void testUnpersistentKeyUnitIndex()
        {
            
        }
        
        public void testCanPersistentSegment()
        {
            try
            {
                IndexInfo info = new IndexInfo();
                info.offset = 0;
                
                Head head = new Head();
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.create(prefix + "testCanPersistentSegment_tmp", head);                
                
                Segment segment = new Segment(info, fd);
                
                info.len = 64 * 1024 * 1024 - ConstVar.LineIndexRecordLen - ConstVar.IndexMetaOffset;
                Unit unit = new Unit(info, segment);
                unit.setLen(info.len);
                unit.setFull();
                segment.setCurrentUnit(unit);
                
                if(!segment.canPersistented())
                {
                    fail("shoud can persistent, remain:"+segment.remain()+"need:"+(unit.len()+ConstVar.LineIndexRecordLen));
                }
                
                info.len = 64 * 1024 * 1024 - ConstVar.LineIndexRecordLen - ConstVar.IndexMetaOffset + 1;
                Unit unit2 = new Unit(info, segment);
                unit2.setLen(info.len);
                unit2.setFull();
                segment.setCurrentUnit(unit2);
                
                if(segment.canPersistented())
                {
                    fail("should not persistent, remain:"+segment.remain()+"need:"+(unit.len()+ ConstVar.LineIndexRecordLen));
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
                fail("get ioexception:"+e.getMessage());
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get exception:"+e.getMessage());
            }
        }
        
        private void addRecord2Unit(Unit unit, int count) throws Exception
        {
            for(int i = 0; i < count; i++)
            {
                Record record = new Record( 7);
                record.addValue(new FieldValue((byte)1, (short)0));
                record.addValue(new FieldValue((short)2, (short)1));
                record.addValue(new FieldValue((int)3, (short)2));
                record.addValue(new FieldValue((long)4, (short)3));
                record.addValue(new FieldValue((float)5.5, (short)4));
                record.addValue(new FieldValue((double)6.6, (short)5));
                record.addValue(new FieldValue("hello konten", (short)6));
                unit.addRecord(record);
            }
            
        }
        
        public void testPersistentSegment()
        {
            try
            {
                IndexInfo info = new IndexInfo();
                info.offset = 0;
                
                Head head = new Head();
                head.setVar((byte) 1);
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.create(prefix + "testPersistentSegment_tmp", head);               
                
                String fileName = prefix + "testPersistentSegment";
                Path path = new Path(fileName);
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataOutputStream out = fs.create(path);
                
                fd.setOut(out);
                Segment segment = new Segment(info, fd);
                    
                int unitSize = 100;
                for(int i = 0; i < unitSize; i++)
                {
                    IndexInfo indexInfo = new IndexInfo();
                    indexInfo.offset = i * 100;
                    indexInfo.len = 77;
                    indexInfo.beginLine = (i+1) * 100;
                    indexInfo.endLine = (i+2)*100;
                    indexInfo.idx = i;
                    
                    Unit unit = new Unit(indexInfo, segment);
                    addRecord2Unit(unit, 100);
                    unit.beginLine = (i+1) * 100;
                    unit.endLine = (i+2) * 100;
                    segment.addUnit(unit);
                    if(unit.len() != 100 * full7chunkLen + 100 * 8 + ConstVar.DataChunkMetaOffset)
                    {
                        fail("error unit.len:"+unit.len());
                    }                    
                }       
                
                segment.recordNum = 234;
                segment.setBeginLine(1);
                segment.setEndLine(235);
                
                segment.persistent(out);                
                
                if(out.getPos() != fd.confSegmentSize()) 
                {
                    System.out.println("seg.len:"+segment.len()+"seg.remain:"+segment.remain()+"index.len"+segment.unitIndex().len());
                    fail("error pos:"+out.getPos());
                }
                out.close();
                
                int unitlen = full7chunkLen*100+8*100+ConstVar.DataChunkMetaOffset;
                FSDataInputStream in = fs.open(path);
                
                
                in.seek(segment.lineIndexOffset());
                
                info.offset = 0;
                info.len = segment.len();
                fd.setWorkStatus(ConstVar.WS_Read);
                Segment segment2 = new Segment(info, fd);
                segment2.unpersistentUnitIndex(in);             
                if(segment2.recordNum() != 234)
                {
                    fail("error recordnum:"+segment2.recordNum());
                }
                if(segment2.unitNum() != unitSize)
                {
                    fail("error unitNum:"+segment2.unitNum());
                }   
                if(segment2.keyIndexOffset() != -1)
                {
                    fail("error key index offset:"+segment2.keyIndexOffset());                  
                }
                if(segment2.lineIndexOffset() != unitlen * unitSize)
                {
                    fail("error line index offset:"+segment2.lineIndexOffset());
                }           
                if(segment2.units().size() != unitSize)
                {
                    fail("error units.size:"+segment2.units().size());
                }
                
                UnitIndex index = segment2.unitIndex();
                if(index.lineIndexInfos().size() != unitSize)
                {
                    fail("error line unit index size:"+index.lineIndexInfos().size());
                }
                if(index.keyIndexInfos().size() != 0)
                {
                    fail("error key unit index size:"+index.keyIndexInfos().size());
                }
                
                for(int i = 0; i < unitSize; i++)
                {
                    IndexInfo ii = index.lineIndexInfos().get(i);
                    if(ii.beginLine() != (1+i)*100)
                    {
                        fail("error beginline:"+ii.beginLine()+"i:"+i);
                    }
                    if(ii.endLine() != (2+i)*100)
                    {
                        fail("error end line:"+ii.endLine()+"i:"+i);
                    }
                    if(ii.offset() != i * 100)
                    {
                        fail("error offset:"+ii.offset()+"i:"+i);
                    }
                    if(ii.len != unitlen)
                    {
                        fail("error len:"+ii.len()+"i:"+i);
                    }
                    if(ii.idx() != i)
                    {
                        fail("error idx:"+ii.idx()+"i:"+i);
                    }
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
                fail("get IOException:"+e.getMessage());
            }
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get Exception:"+e.getMessage());
            }       
        }               
                
        public void testGetRecordByLineSegment()
        {                       
            Segment segment = null;
            try
            {
                FieldMap fieldMap = new FieldMap();
                fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
                fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
                fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
                fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
                fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
                fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
                fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short)6));
                
                
                Head head = new Head();
                head.setFieldMap(fieldMap);
                
                String fileName = prefix + "testGetRecordByLineSegment";
                Path path = new Path(fileName);
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataOutputStream out = fs.create(path);
                
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.setWorkStatus(ConstVar.WS_Write);
                fd.head = head;
                fd.setOut(out);
                
                IndexInfo info = new IndexInfo();
                info.offset = 0;                
                
                segment = new Segment(info, fd);                
                
                int recordNum = 150000;
                for(int i = 0; i < recordNum; i++)
                {
                    Record record = new Record( 7);
                    record.addValue(new FieldValue((byte)1, (short)0));
                    record.addValue(new FieldValue((short)2, (short)1));
                    record.addValue(new FieldValue((int)3, (short)2));
                    record.addValue(new FieldValue((long)4, (short)3));
                    record.addValue(new FieldValue((float)5.5, (short)4));
                    record.addValue(new FieldValue((double)6.6, (short)5));
                    record.addValue(new FieldValue("hello konten", (short)6));
                    
                    segment.addRecord(record);
                    record = null;                  
                }       
                
                segment.persistent(out);
                out.close();                                
                
                FSDataInputStream in = fs.open(path);
                
                fd.setIn(in);
                fd.setWorkStatus(ConstVar.WS_Read);
                
                info.offset = 0;
                info.len = segment.len();
                info.beginLine = 0;
                info.endLine = 1500000;
                Segment segment2 = new Segment(info, fd);
                Record record = segment2.getRecordByLine(-1);
                if(record != null)
                {
                    fail("should get null");
                }
                
                record = segment2.getRecordByLine(150000);
                if(record != null)
                {
                    fail("should get null");
                }
                record = segment2.getRecordByLine(150001);
                if(record != null)
                {
                    fail("should get null");
                }
                
                record = segment2.getRecordByLine(0);
                if(record == null)
                {
                    fail("should not get null");
                }
                
                judgeFixedRecord(record);
                
                
                int line = 150000 - 1;
                record = segment2.getRecordByLine(line);
                if(record == null)
                {
                    fail("should not get null");
                }
                
                judgeFixedRecord(record);               
                
            }
            catch(IOException e)
            {
                e.printStackTrace();
                fail("get IOException:"+e.getMessage());                
            }           
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get exception:"+e.getMessage());
            }           
        }
        
        public void testGetRecordByValueSegment()
        {
        }
        
        public void testGetRecordByOrderSegment()
        {
            Segment segment = null;
            try
            {
                FieldMap fieldMap = new FieldMap();
                fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
                fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
                fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
                fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
                fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
                fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
                fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short)6));
                
                
                Head head = new Head();
                head.setFieldMap(fieldMap);
                
                String fileName = prefix + "testGetRecordByValueSegment";
                Path path = new Path(fileName);
                FileSystem fs = FileSystem.get(new Configuration());
                FSDataOutputStream out = fs.create(path);
                
                Configuration conf = new Configuration();
                FormatDataFile fd = new FormatDataFile(conf);
                fd.setWorkStatus(ConstVar.WS_Write);
                fd.head = head;
                fd.setOut(out);
                
                IndexInfo info = new IndexInfo();
                info.offset = 0;                
                
                segment = new Segment(info, fd);                
                
                int recordNum = 150000;
                for(int i = 0; i < recordNum; i++)
                {
                    Record record = new Record( 7);
                    record.addValue(new FieldValue((byte)1, (short)0));
                    record.addValue(new FieldValue((short)2, (short)1));
                    record.addValue(new FieldValue((int)3, (short)2));
                    record.addValue(new FieldValue((long)4, (short)3));
                    record.addValue(new FieldValue((float)5.5, (short)4));
                    record.addValue(new FieldValue((double)6.6, (short)5));
                    record.addValue(new FieldValue("hello konten", (short)6));
                    
                    segment.addRecord(record);
                    record = null;                  
                }       
                
                segment.persistent(out);
                out.close();                                
                
                FSDataInputStream in = fs.open(path);
                
                fd.setIn(in);
                fd.setWorkStatus(ConstVar.WS_Read);
                
                info.offset = 0;
                info.len = segment.len();
                
                Segment segment2 = new Segment(info, fd);
                
                FieldValue[] values = new FieldValue[2];
                values[0] = new FieldValue((byte)1, (short)2);
                values[1] = new FieldValue((short)2, (short)3);             
                Record[] records = segment2.getRecordByOrder(values, values.length);
                if(records != null)
                {
                    fail("should get null, index error, records.len:"+records.length);
                }
                
                values[0] = new FieldValue((byte)1, (short)0);
                values[1] = new FieldValue((short)3, (short)1);             
                records = segment2.getRecordByOrder(values, values.length);
                if(records != null)
                {
                    fail("should get null, value error");
                }
                
                values[0] = new FieldValue((byte)1, (short)0);
                values[1] = new FieldValue((short)2, (short)1);             
                records = segment2.getRecordByOrder(values, values.length);
                if(records == null)
                {
                    fail("should not get null");
                }
                if(records.length != 150000)
                {
                    fail("error result size:"+records.length);
                }               
                
                for(int i = 0; i < 150000; i++)
                {
                    judgeFixedRecord(records[i]);                   
                }
                                            
                records = segment2.getRecordByOrder(null, 0);
                if(records == null)
                {
                    fail("should not get null");
                }
                if(records.length != 150000)
                {
                    fail("error result size:"+records.length);
                }               
                
                for(int i = 0; i < 150000; i++)
                {
                    judgeFixedRecord(records[i]);
                }
            }
            catch(IOException e)
            {
                e.printStackTrace();
                fail("get IOException:"+e.getMessage());                
            }           
            catch(Exception e)
            {
                e.printStackTrace();
                fail("get exception:"+e.getMessage());
            }       
        }   
    

        
    
    public void testSetNoPreFileName() 
    {
        try
        {
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);
            Head head = new Head();
            
            String fileName = prefix + "testSetNoPreFileName";
            fd.create(fileName, head);
            
            Path path = new Path(fileName);
            FileSystem fs = FileSystem.get(new Configuration());
            if(!fs.isFile(path))
            {
                fail("create file fail");
            }
        }
        catch(IOException e)
        {
            e.printStackTrace();
            fail("get ioexception:"+e.getMessage());
        }
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
        
    }

    public void testSetPreFileName()
    {
        try
        {
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);
            Head head = new Head();
            
            //String fileName = "hdfs://tdw-172-25-38-246:54310/user/tdwadmin/testSetPreFileName";
            String fileName = "/user/tdwadmin/se_test/fs/basic/testSetPreFileName";
            fd.create(fileName, head);
            fd.close();
            
            Path path = new Path(fileName);
            FileSystem fs = FileSystem.get(new Configuration());
            if(!fs.isFile(path))
            {
                fail("create file fail");
            }
        }
        catch(IOException e)
        {
            fail(e.getMessage());
        }
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
    }
    
    public void testSetInvalidFileName()
    {
        try
        {
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);
            Head head = new Head();
            
            String fileName = "hdfs://tdw-172-25-38-246:54310/user/tdwadmin/se_test/testSetInvalidFileName";
            fd.create(fileName, head);  
            
            fail("invalid file name");
        }
        catch(IOException e)
        {
            
        }
        catch(Exception e)
        {
        }
    }
    
    public void testGetUnitSize()
    {
        try
        {
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);
            
            long unitSize = fd.confUnitSize();
            if(unitSize == 0)
            {
                fail("get unit size fail:"+unitSize);
            }
        }
        catch(IOException e)
        {
            fail(e.getMessage());
        }
        catch(Exception e)
        {
            fail(e.getMessage());
        }
    }
    
    public void testGetSegmentSize()
    {
        try
        {
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);
            
            long segSize = fd.confSegmentSize();
            if(segSize == 0)
            {
                fail("get seg size fail:"+segSize);
            }
        }
        catch(IOException e)
        {
            fail(e.getMessage());
        }
        catch(Exception e)
        {
            fail(e.getMessage());
        }
    }
    
    public void testUnitSizeGreatThanSegmentSize()
    {
        try
        {
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);               
        }
        catch(IOException e)
        {
            fail(e.getMessage());
        }
        catch(Exception e)
        {
            fail(e.getMessage());
        }
    }
    
    public void testWriteHead()
    {
        try
        {
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);       
            
            String fileName = prefix + "testWriteHead";
            
            Head head = new Head();
            
            fd.create(fileName, head);      
            fd.close();
            
            long fileLen = fd.getFileLen();
            long realLen = head.len()+ConstVar.IndexMetaOffset;
            if(fileLen != realLen)
            {
                fail("fileLen != realLen, fileLen:"+fileLen+"realLen:"+realLen);
            }
        }
        catch(IOException e)
        {
            fail(e.getMessage());
        }
        catch(Exception e)
        {
            fail(e.getMessage());
        }
    }   
    
    
    public void testAddRecord()
    {
        try
        {
            Head head = new Head();
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);
            fd.create(prefix + "testAddRecord", head); 
            
            int size = 100;
            for(int i = 0; i < size; i++)
            {
                Record record = new Record( 7);
                record.addValue(new FieldValue((byte)1, (short)0));
                record.addValue(new FieldValue((short)2, (short)1));
                record.addValue(new FieldValue((int)3, (short)2));
                record.addValue(new FieldValue((long)4, (short)3));
                record.addValue(new FieldValue((float)5.5, (short)4));
                record.addValue(new FieldValue((double)6.6, (short)5));
                record.addValue(new FieldValue("hello konten", (short)6));
                fd.addRecord(record);
            }
            
            if(fd.recordNum() != size)
            {
                fail("error record num:"+fd.recordNum());
            }
            if(fd.currentSegment().currentUnit() == null)
            {
                fail("null current unit");
            }
            if(fd.currentSegment() == null)
            {
                fail("null current seg");
            }
        }
        catch(IOException e)
        {
            e.printStackTrace();
            fail("get ioexception:"+e.getMessage());
        }
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
    }

    public void testAddRecordMoreSegment()
    {
        try
        {           
            FieldMap fieldMap = new FieldMap();
            fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
            fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
            fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
            fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
            fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
            fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
            fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short)6));
                    
            Head head = new Head();
            head.setFieldMap(fieldMap);
            
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);
            fd.create(prefix + "testAddRecordMoreSegment", head); 
            
            int size = 200*10000;
            for(int i = 0; i < size; i++)
            {
                Record record = new Record( 7);
                record.addValue(new FieldValue((byte)1, (short)0));
                record.addValue(new FieldValue((short)2, (short)1));
                record.addValue(new FieldValue((int)3, (short)2));
                record.addValue(new FieldValue((long)4, (short)3));
                record.addValue(new FieldValue((float)5.5, (short)4));
                record.addValue(new FieldValue((double)6.6, (short)5));
                record.addValue(new FieldValue("hello konten", (short)6));
                fd.addRecord(record);
            }
            
            if(fd.recordNum() != size)
            {
                fail("error record num:"+fd.recordNum());
            }
            if(fd.currentSegment().currentUnit() == null)
            {
                fail("null current unit");
            }
            if(fd.currentSegment() == null)
            {
                fail("null current seg");
            }
            if(fd.segmentNum() != 1) 
            {
                fail("error segment num:"+fd.segmentNum());
            }
            
            fd.close();
        }
        catch(IOException e)
        {
            e.printStackTrace();
            fail("get ioexception:"+e.getMessage());
        }
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
    }
    
    public void testClose()
    {
        try
        {
            Head head = new Head();
            head.setVar((byte) 1);
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);
            fd.create(prefix + "testClose", head); 
            
            int size = 100*10000;
            
            for(int i = 0; i < size; i++)
            {
                Record record = new Record( 7);
                record.addValue(new FieldValue((byte)1, (short)0));
                record.addValue(new FieldValue((short)2, (short)1));
                record.addValue(new FieldValue((int)3, (short)2));
                record.addValue(new FieldValue((long)4, (short)3));
                record.addValue(new FieldValue((float)5.5, (short)4));
                record.addValue(new FieldValue((double)6.6, (short)5));
                record.addValue(new FieldValue("hello konten", (short)6));
                fd.addRecord(record);
            }
            
            if(fd.recordNum() != size)
            {
                fail("error record num:"+fd.recordNum());
            }
            if(fd.currentSegment().currentUnit() == null)
            {
                fail("null current unit");
            }
            if(fd.currentSegment() == null)
            {
                fail("null current seg");
            }
            if(fd.segmentNum() != 0)
            {
                fail("error segment num:"+fd.segmentNum());
            }
            
            
            int headLen = head.len();
            long currentUnitLen = fd.currentSegment().currentUnit().len();
            long segmentLen = fd.currentSegment().len() + currentUnitLen + ConstVar.LineIndexRecordLen;
            long remain = fd.currentSegment().remain();
            int unitNum = fd.currentSegment().unitNum();
            fd.close();
            
            int indexLen = ConstVar.LineIndexRecordLen * fd.segmentNum();
            int metaLen = ConstVar.IndexMetaOffset;
            
            long fileLen = fd.getFileLen();
            
            if(fileLen != headLen + segmentLen + indexLen + metaLen)
            {
                fail("error file len:"+fileLen);
            }
            
            if(fd.in() != null)
            {
                fail("in should set null");
            }
            if(fd.out() != null)
            {
                fail("out should set null");
            }
            if(fd.recordNum() != 0)
            {
                fail("record num should set 0");
            }
            if(fd.keyIndexOffset != -1)
            {
                fail("key index offset not -1");
            }
            if(fd.lineIndexOffset != -1)
            {
                fail("line index offset not -1");
            }
            if(fd.currentOffset != -1)
            {
                fail("current offset not -1");
            }
            if(fd.hasLoadAllSegmentDone)
            {
                fail("has load all segment Done not false");
            }
            
            String fileName = prefix + "testClose";
            Path path = new Path(fileName);
            FileSystem fs = FileSystem.get(new Configuration());
            FSDataInputStream in = fs.open(path);
            
            long metaOffset = fileLen - ConstVar.IndexMetaOffset;
            in.seek(metaOffset);
    
            int recordNum = in.readInt();
            int segNum = in.readInt();
            long keyIndexOffset = in.readLong();
            long lineIndexOffset = in.readLong();
            
            if(recordNum != size)
            {
                fail("error record num:"+recordNum);
            }
            if(segNum != 1)
            {
                fail("error segNum:"+segNum);
            }
            if(keyIndexOffset != -1)
            {
                fail("error key index offset:"+keyIndexOffset);
            }
            if(lineIndexOffset != (headLen + segmentLen))
            {
                fail("error line index offset:"+lineIndexOffset);
            }
            
            in.seek(lineIndexOffset);
            for(int i = 0; i < segNum; i++)
            {
                int beginLine = in.readInt();
                int endLine = in.readInt();
                long offset = in.readLong();
                long len = in.readLong();
                int idx = in.readInt();
                
                if(beginLine != 0)
                {
                    fail("error beginLine:"+beginLine);                 
                }
                if(endLine != size)
                {
                    fail("error end line:"+endLine);
                }
                if(offset != head.len())
                {
                    fail("error offset:"+offset);
                }
                long tlen = size * full7chunkLen + size * 8 +  ConstVar.DataChunkMetaOffset * (unitNum + 1) + 28 * (unitNum + 1) + 24;
                if(len != tlen)
                {
                    fail("error len:"+len);
                }
            }           
        }
        catch(IOException e)
        {
            e.printStackTrace();
            fail("get ioexception:"+e.getMessage());
        }
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
    }
    
    public void testNoFillLastSegment()
    {
        try
        {
            String fileName = prefix + "testNoFillLastSegment";        
            Head head = new Head();
            
            
            FieldMap fieldMap = new FieldMap();
            fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
            fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
            fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
            fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
            fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
            fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
            fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short)6));
            
            head.setFieldMap(fieldMap);
            
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);
            fd.create(fileName, head);
            
            Record record = new Record( 7);
            record.addValue(new FieldValue((byte)1, (short)0));
            record.addValue(new FieldValue((short)2, (short)1));
            record.addValue(new FieldValue((int)3, (short)2));
            record.addValue(new FieldValue((long)4, (short)3));
            record.addValue(new FieldValue((float)5.5, (short)4));
            record.addValue(new FieldValue((double)6.6, (short)5));
            record.addValue(new FieldValue("hello konten", (short)6));
            
            fd.addRecord(record);
            
            fd.close();
        
            FileSystem fs = FileSystem.get(conf);
            long fileLen = fs.getFileStatus(new Path(fileName)).getLen();
            
            int tlen = head.len() + full7chunkLen +  8 + ConstVar.DataChunkMetaOffset+ ConstVar.LineIndexRecordLen + ConstVar.IndexMetaOffset + ConstVar.LineIndexRecordLen + ConstVar.IndexMetaOffset;
            if(fileLen != tlen)
            {
                fail("error file len:"+fileLen);
            }
            
            FormatDataFile fd2 = new FormatDataFile(new Configuration());
            fd2.open(fileName);
            if(fd2.recordNum() != 1)
            {
                fail("error record num:"+fd2.recordNum());
            }
            if(fd2.segmentNum() != 1)
            {
                fail("error segment num:"+fd2.segmentNum());
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
    }
    
    public void testOpenNoRecord()
    {
        try
        {
            String fileName = prefix + "testOpenNoRecord";        
            Head head = new Head();
            FormatDataFile fd = new FormatDataFile(new Configuration());
            fd.create(fileName, head);
            fd.close();
        
            FileSystem fs = FileSystem.get(new Configuration());
            long fileLen = fs.getFileStatus(new Path(fileName)).getLen();
            if(fileLen != head.len() + ConstVar.IndexMetaOffset)
            {
                fail("error file len:"+fileLen);
            }
            
            FormatDataFile fd2 = new FormatDataFile(new Configuration());
            fd2.open(fileName);
            if(fd2.recordNum() != 0)
            {
                fail("error record num:"+fd2.recordNum());
            }
            if(fd2.segmentNum() != 0)
            {
                fail("error segment num:"+fd2.segmentNum());
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
    }
    public void testOpen()
    {
        try
        {           
            String fileName = prefix + "testAddRecordMoreSegment";
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);
            fd.open(fileName);
            int recordNum = 200*10000;
            if(fd.segmentNum() != 2)
            {
                fail("error segment num:"+fd.segmentNum());
            }
            if(fd.recordNum() != recordNum)
            {
                fail("error record num:"+recordNum);
            }
            if(fd.segmentIndex().len() != ConstVar.LineIndexRecordLen * 2)
            {
                fail("error index len:"+fd.segmentIndex().len());
            }
            if(fd.segmentIndex().lineIndexInfos().size() != 2)
            {
                fail("error unit index size:"+fd.segmentIndex().lineIndexInfos().size());
            }
            if(fd.segmentIndex().keyIndexInfos().size() != 0)
            {
                fail("error key index size:"+fd.segmentIndex().keyIndexInfos().size());
            }
        }
        catch(IOException e)
        {
            fail(e.getMessage());
        }
        catch(Exception e)
        {
            fail(e.getMessage());
        }
    }
    
    public void testGetRecordByLineFD()
    {
        Segment segment = null;
        try
        {
            FieldMap fieldMap = new FieldMap();
            fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
            fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
            fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
            fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
            fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
            fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
            fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short)6));
            
            
            Head head = new Head();
            head.setFieldMap(fieldMap);
            
            String fileName = prefix + "testAddRecordMoreSegment";
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);
            fd.open(fileName);          
            
            int recordNum = 200*10000;
            
            Record record = fd.getRecordByLine(-1);
            if(record != null)
            {
                fail("should get null");
            }
            record = fd.getRecordByLine(0);
            if(record == null)
            {
                fail("should not get null");                
            }
            
            judgeFixedRecord(record);
            
            record = fd.getRecordByLine(recordNum - 1);
            if(record == null)
            {
                fail("should not get null");
            }
            judgeFixedRecord(record);
            
            record = fd.getRecordByLine(recordNum);
            if(record != null)
            {
                fail("should get null");
            }
            
            record = fd.getRecordByLine(recordNum + 1);
            if(record != null)
            {
                fail("should get null");
            }
            
            record = fd.getRecordByLine(45687);
            if(record == null)
            {
                fail("should not get null");
            }
            judgeFixedRecord(record);  
            
            fd.close();
        }
        catch(IOException e)
        {
            e.printStackTrace();
            fail("get IOException:"+e.getMessage());                
        }           
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }           
    }
        
    public void testGetRecordByLineFDNotVar()
    {
        Segment segment = null;
        try
        {
            FieldMap fieldMap = new FieldMap();
            fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
            fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
            fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
            fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
            fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
            fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
            
            
            Head head = new Head();
            head.setFieldMap(fieldMap);
            
            String fileName = prefix + "testGetRecordByLineFDNotVar";
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);            
            fd.create(fileName, head);          
            
            int recordNum = 200*10000;            
            for(int i = 0; i < recordNum; i++)
            {
                Record record = new Record( 6);
                record.addValue(new FieldValue((byte)(1+i), (short)0));
                record.addValue(new FieldValue((short)(2+i), (short)1));
                record.addValue(new FieldValue((int)(3+i), (short)2));
                record.addValue(new FieldValue((long)(4+i), (short)3));
                record.addValue(new FieldValue((float)(5.5+i), (short)4));
                record.addValue(new FieldValue((double)(6.6+i), (short)5));
                
                fd.addRecord(record);
            }
            
            fd.close();
            
            
            FormatDataFile fd2 = new FormatDataFile(conf);
            fd2.open(fileName);
            Record record = fd2.getRecordByLine(-1);
            if(record != null)
            {
                fail("should get null");
            }
            record = fd2.getRecordByLine(0);
            if(record == null)
            {
                fail("should not get null");                
            }
            
            judgeNotFixedRecord(record, 0);
            
            record = fd2.getRecordByLine(recordNum - 1);
            if(record == null)
            {
                fail("should not get null");
            }
            judgeNotFixedRecord(record, recordNum -1);
            
            record = fd2.getRecordByLine(recordNum);
            if(record != null)
            {
                fail("should get null");
            }
            
            record = fd2.getRecordByLine(recordNum + 1);
            if(record != null)
            {
                fail("should get null");
            }
            
            record = fd2.getRecordByLine(45687);
            if(record == null)
            {
                fail("should not get null");
            }
            judgeNotFixedRecord(record,45687);      
            
            fd.close();
        }
        catch(IOException e)
        {
            e.printStackTrace();
            fail("get IOException:"+e.getMessage());                
        }           
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }           
    }
    
    public void testGetRecordByOrderFD()
    {
        Segment segment = null;
        try
        {
            FieldMap fieldMap = new FieldMap();
            fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
            fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
            fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
            fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
            fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
            fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
            fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short)6));
            
            
            Head head = new Head();
            head.setFieldMap(fieldMap);
            
            String fileName = prefix + "testAddRecordMoreSegment";
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);
            fd.open(fileName);          
            
            int recordNum = 200*10000;
            
            FieldValue[] values = new FieldValue[2];
            values[0] = new FieldValue((byte)1, (short)2);
            values[1] = new FieldValue((short)2, (short)3);
            
            Record[] records = fd.getRecordByOrder(values, values.length);
            if(records != null)
            {
                fail("should get null");
            }
            
            values[0] = new FieldValue((byte)2, (short)1);
            values[1] = new FieldValue((short)2, (short)3);
            
            records = fd.getRecordByOrder(values, values.length);           
            if(records != null)
            {
                fail("should get null");
            }
            
            values[0] = new FieldValue((byte)1, (short)0);
            values[1] = new FieldValue((short)2, (short)1);
            
            records = fd.getRecordByOrder(values, values.length);           
            if(records == null)
            {
                fail("should not get null");
            }
            if(records.length != recordNum)
            {
                fail("error record len:"+records.length);
            }
            for(int i = 0; i < recordNum; i++)
            {
                judgeFixedRecord(records[i]);
            }                            
            
            fd.close();
        }
        catch(IOException e)
        {
            e.printStackTrace();
            fail("get IOException:"+e.getMessage());                
        }           
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }           
    }
    
    
    public void testGetRecordByLineFD2()
    { 
        try
        {
            FieldMap fieldMap = new FieldMap();
            fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
            fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
            fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
            fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
            fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
            fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
            fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short)6));
            
            
            Head head = new Head();
            head.setFieldMap(fieldMap);
            
            String fileName = prefix + "testGetRecordByLineFD2";
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);
            fd.create(fileName, head);          
            
            int recordNum = 200*10000;
            for(int i = 0; i < recordNum; i++)
            {
                Record record = new Record( 7);
                record.addValue(new FieldValue((byte)(1+i), (short)0));
                record.addValue(new FieldValue((short)(2+i), (short)1));
                record.addValue(new FieldValue((int)(3+i), (short)2));
                record.addValue(new FieldValue((long)(4+i), (short)3));
                record.addValue(new FieldValue((float)(5.5+i), (short)4));
                record.addValue(new FieldValue((double)(6.6+i), (short)5));
                record.addValue(new FieldValue("hello konten"+i, (short)6));
                
                fd.addRecord(record);
            }
            
            fd.close();
            
           
            FormatDataFile fd2 = new FormatDataFile(conf);
            fd2.open(fileName);
            Record record = fd2.getRecordByLine(-1);
            if(record != null)
            {
                fail("should get null");
            }
            record = fd2.getRecordByLine(0);
            if(record == null)
            {
                fail("should not get null");                
            }
            
            judgeNotFixedRecord(record, 0);
            
            record = fd2.getRecordByLine(recordNum - 1);
            if(record == null)
            {
                fail("should not get null");
            }
            judgeNotFixedRecord(record, recordNum - 1);
            
            record = fd2.getRecordByLine(recordNum);
            if(record != null)
            {
                fail("should get null");
            }
            
            record = fd2.getRecordByLine(recordNum + 1);
            if(record != null)
            {
                fail("should get null");
            }
            
            record = fd2.getRecordByLine(45687);
            if(record == null)
            {
                fail("should not get null");
            }
            judgeNotFixedRecord(record, 45687);  
            
            fd.close();
        }
        catch(IOException e)
        {
            e.printStackTrace();
            fail("get IOException:"+e.getMessage());                
        }           
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }           
    }
    
    public void testGetRecordByOrderFD2()
    {
        try
        {
            FieldMap fieldMap = new FieldMap();
            fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
            fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
            fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
            fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
            fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
            fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
            fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short)6));
            
            
            Head head = new Head();
            head.setFieldMap(fieldMap);
            
            String fileName = prefix + "testGetRecordByLineFD2";
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);           
            fd.open(fileName);            
            
            FieldValue[] values = new FieldValue[2];
            values[0] = new FieldValue((byte)2, (short)1);
            values[1] = new FieldValue((short)2, (short)3);
            
            Record[] records = fd.getRecordByOrder(values, values.length);
            if(records != null)
            {
                fail("should get null");
            }
               
            for(int i = 0; i < 1; i++)
            {
                values[0] = new FieldValue((byte)(1+i), (short)0);
                values[1] = new FieldValue((short)(2+i), (short)1);
                
                records = fd.getRecordByOrder(values, values.length);           
                if(records == null)
                {
                    fail("should not get null");
                }
                if(records.length != 31)  
                {
                    fail("error record len:"+records.length);
                }       
                
                judgeNotFixedRecord(records[0], i);
            }                            
            
            fd.close();
        }
        catch(IOException e)
        {
            e.printStackTrace();
            fail("get IOException:"+e.getMessage());                
        }           
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }           
    }
    
    public void testGetRecordByLineFD3()
    { 
        try
        {
            FieldMap fieldMap = new FieldMap();
            fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
            fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
            fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
            fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
            fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
            fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
            fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short)6));
            
            
            Head head = new Head();
            head.setFieldMap(fieldMap);
            
            String fileName = prefix + "testGetRecordByLineFD3";
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);
            fd.create(fileName, head);          
            
            int recordNum = 200*10000;
            for(int i = 0; i < recordNum; i++)
            {
                Record record = new Record( 7);
                record.addValue(new FieldValue((byte)(1+i), (short)0));
                record.addValue(new FieldValue((short)(2+i), (short)1));
                record.addValue(new FieldValue((int)(3+i), (short)2));
                record.addValue(new FieldValue((long)(4+i), (short)3));
                record.addValue(new FieldValue((float)(5.5+i), (short)4));
                record.addValue(new FieldValue((double)(6.6+i), (short)5));
                record.addValue(new FieldValue("hello konten"+i, (short)6));
                
                fd.addRecord(record);
            }
            
            fd.close();
            
           
            FormatDataFile fd2 = new FormatDataFile(conf);
            fd2.open(fileName);
            
            Record value = new Record();
            Record record = fd2.getRecordByLine(-1, value);
            if(record != null)
            {
                fail("should get null");
            }
            record = fd2.getRecordByLine(0, value);
            if(record == null)
            {
                fail("should not get null");                
            }
            
            judgeNotFixedRecord(value, 0);
            
            record = fd2.getRecordByLine(recordNum - 1, value);
            if(record == null)
            {
                fail("should not get null");
            }
            judgeNotFixedRecord(value, recordNum - 1);
            
            record = fd2.getRecordByLine(recordNum, value);
            if(record != null)
            {
                fail("should get null");
            }
            
            record = fd2.getRecordByLine(recordNum + 1);
            if(record != null)
            {
                fail("should get null");
            }
            
            record = fd2.getRecordByLine(45687, value);
            if(record == null)
            {
                fail("should not get null");
            }
            judgeNotFixedRecord(value, 45687);    
            
            fd.close();
        }
        catch(IOException e)
        {
            e.printStackTrace();
            fail("get IOException:"+e.getMessage());                
        }           
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }           
    }      
    
    public void testGetRecordByLineFD3NotVar()
    { 
        try
        {
            FieldMap fieldMap = new FieldMap();
            fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
            fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
            fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
            fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
            fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
            fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
            
            
            Head head = new Head();
            head.setFieldMap(fieldMap);
            
            String fileName = prefix + "testGetRecordByLineFD3NotVar";
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);
            fd.create(fileName, head);          
            
            int recordNum = 200*10000;
            for(int i = 0; i < recordNum; i++)
            {
                Record record = new Record( 6);
                record.addValue(new FieldValue((byte)(1+i), (short)0));
                record.addValue(new FieldValue((short)(2+i), (short)1));
                record.addValue(new FieldValue((int)(3+i), (short)2));
                record.addValue(new FieldValue((long)(4+i), (short)3));
                record.addValue(new FieldValue((float)(5.5+i), (short)4));
                record.addValue(new FieldValue((double)(6.6+i), (short)5));
                
                fd.addRecord(record);
            }
            
            fd.close();
            
           
            FormatDataFile fd2 = new FormatDataFile(conf);
            fd2.open(fileName);
            
            Record value = new Record();
            Record record = fd2.getRecordByLine(-1, value);
            if(record != null)
            {
                fail("should get null");
            }
            record = fd2.getRecordByLine(0, value);
            if(record == null)
            {
                fail("should not get null");                
            }
            
            judgeNotFixedRecord(value, 0);
            
            record = fd2.getRecordByLine(recordNum - 1, value);
            if(record == null)
            {
                fail("should not get null");
            }
            judgeNotFixedRecord(value, recordNum - 1);
            
            record = fd2.getRecordByLine(recordNum, value);
            if(record != null)
            {
                fail("should get null");
            }
            
            record = fd2.getRecordByLine(recordNum + 1);
            if(record != null)
            {
                fail("should get null");
            }
            
            record = fd2.getRecordByLine(45687, value);
            if(record == null)
            {
                fail("should not get null");
            }
            judgeNotFixedRecord(value, 45687);     
            
            fd.close();
        }
        catch(IOException e)
        {
            e.printStackTrace();
            fail("get IOException:"+e.getMessage());                
        }           
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }           
    }      
    
    public void testGetRecordByLineFDReadManyTimes()
    { 
        try
        {
            FieldMap fieldMap = new FieldMap();
            for(int i = 0; i < 10; i++)
            {
                fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)(i*7+0)));
                fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)(i*7+1)));
                fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)(i*7+2)));
                fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)(i*7+3)));
                fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)(i*7+4)));
                fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)(i*7+5)));
                fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short)(i*7+6)));            
            }
            Head head = new Head();
            head.setFieldMap(fieldMap);
            
            String fileName = prefix + "testGetRecordByLineFDReadManyTimes1";
            
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);
            fd.create(fileName, head); 
            
            int recordNum = 10;
            for(int i = 0; i < recordNum; i++)
            {
                Record record = new Record( 70);
                for(int j = 0; j < 10; j++)
                {
                    record.addValue(new FieldValue((byte)(1+i), (short)(j*7+0)));
                    record.addValue(new FieldValue((short)(2+i), (short)(j*7+1)));
                    record.addValue(new FieldValue((int)(3+i), (short)(j*7+2)));
                    record.addValue(new FieldValue((long)(4+i), (short)(j*7+3)));
                    record.addValue(new FieldValue((float)(5.5+i), (short)(j*7+4)));
                    record.addValue(new FieldValue((double)(6.6+i), (short)(j*7+5)));
                    record.addValue(new FieldValue("hello konten"+i, (short)(j*7+6)));
                }
                fd.addRecord(record); 
            }            
            fd.close();
            
            FieldMap fieldMap2 = new FieldMap();           
            fieldMap2.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)(0)));
            fieldMap2.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)(1)));
            fieldMap2.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)(2)));
            fieldMap2.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)(3)));
            fieldMap2.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)(4)));
            fieldMap2.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)(5)));
            fieldMap2.addField(new Field(ConstVar.FieldType_String, 0, (short)(6)));            
            
            Head head2 = new Head();
            head2.setFieldMap(fieldMap2);
            String fileName2 = prefix + "testGetRecordByLineFDReadManyTimes2";
            FormatDataFile fd2 = new FormatDataFile(conf);
            fd2.create(fileName2, head2);            
            for(int i = 0; i < recordNum; i++)
            {
                Record record = new Record( 7);
                record.addValue(new FieldValue((byte)(1+i), (short)0));
                record.addValue(new FieldValue((short)(2+i), (short)1));
                record.addValue(new FieldValue((int)(3+i), (short)2));
                record.addValue(new FieldValue((long)(4+i), (short)3));
                record.addValue(new FieldValue((float)(5.5+i), (short)4));
                record.addValue(new FieldValue((double)(6.6+i), (short)5));
                record.addValue(new FieldValue("hello konten"+i, (short)6));
                
                fd2.addRecord(record); 
            }            
           
            fd2.close();
           
            FormatDataFile fd3 = new FormatDataFile(conf);
            fd3.open(fileName);
            
            Record value = new Record();            
            for(int i = 0; i < recordNum; i++)
            {
                Record record = fd3.getRecordByLine(i, value);
                judgeNotFixedRecordMuchFields(record, i);
            }
            
            FormatDataFile fd4 = new FormatDataFile(conf);
            fd4.open(fileName2);            
            Record value2 = new Record();            
            for(int i = 0; i < recordNum; i++)
            {
                Record record = fd4.getRecordByLine(i, value2);
                judgeNotFixedRecord(record, i);
            }
            
            Record value3 = new Record();            
            for(int i = 0; i < recordNum; i++)
            {
                Record record = fd3.getRecordByLine(i, value3);
                judgeNotFixedRecordMuchFields(record, i);
            }
            
            fd3.close();
            fd4.close();
        }
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
    }
    
    public void testGetRecordByLineFDNullField()
    { 
        try
        {
            FieldMap fieldMap = new FieldMap();
           
            fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)(1)));
            fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)(3)));
            fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)(5)));
            fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)(7)));
            fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)(9)));
            fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)(11)));
            fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short)(13)));            
           
            Head head = new Head();
            head.setFieldMap(fieldMap);
            
            String fileName = prefix + "testGetRecordByLineFDNullField";
            
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);
            fd.create(fileName, head); 
            
            int recordNum = 10;
            for(int i = 0; i < recordNum; i++)
            {
                int j = 0;
                Record record = new Record( 7);
                record.addValue(new FieldValue((byte)(1+i), (short)(j*7+1)));
                record.addValue(new FieldValue((short)(2+i), (short)(j*7+3)));
                record.addValue(new FieldValue(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, null, (short)5));
                record.addValue(new FieldValue((long)(4+i), (short)(j*7+7)));
                record.addValue(new FieldValue((float)(5.5+i), (short)(j*7+9)));
                record.addValue(new FieldValue((double)(6.6+i), (short)(j*7+11)));
                record.addValue(new FieldValue("hello konten"+i, (short)(j*7+13)));
               
                fd.addRecord(record); 
            }            
            fd.close();           
           
            FormatDataFile fd3 = new FormatDataFile(conf);
            fd3.open(fileName);
            
            Record value = new Record();            
            for(int i = 0; i < recordNum; i++)
            {
                Record record = fd3.getRecordByLine(i, value);
                judgeNotFixedRecordNullField(record, i);
                
            }            
            fd3.close();          
        }
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
    }
    
    public void testNotRecordFormatDataFile()
    {
        try
        {   
            FieldMap fieldMap = new FieldMap();
            fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
            fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
            fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
            fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
            fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
            fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
            fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short)6));
            
            
            Head head = new Head();
            head.setFieldMap(fieldMap);
            
            String fileName = prefix + "testNotRecordFormatDataFile";
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);   
            fd.create(fileName, head);
            
            fd.close();
            
            fd.open(fileName);
            SegmentIndex segmentIndex = fd.segmentIndex();
            if(segmentIndex == null)
            {
                fail("segmentIndex should not null");
            }
            
            ArrayList<IndexInfo> infos = segmentIndex.indexInfo();
            if(infos == null)
            {
                fail("infos should not null");
            }
            
            if(infos.size() != 0)
            {
                fail("infos.size() should 0");
            }
            
            FormatDataFile fd1 = new FormatDataFile(conf); 
            fd1.open(fileName);
            
            segmentIndex = fd1.segmentIndex();
            if(segmentIndex == null)
            {
                fail("segmentIndex should not null 2");
            }
            
            infos = segmentIndex.indexInfo();
            if(infos == null)
            {
                fail("infos should not null 2");
            }
            
            if(infos.size() != 0)
            {
                fail("infos.size() should 0 2");
            }
            
            fd1.close();
        }
        catch(IOException e)
        {
            e.printStackTrace();
            fail("get ioexception:"+e.getMessage());
        }
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }        
    }
    
    public void testGetRecordByLineFDCompress()
    {         
        try
        {
            FieldMap fieldMap = new FieldMap();
            fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
            fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
            fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
            fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
            fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
            fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
            fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short)6));
            
            
            Head head = new Head();
            head.setFieldMap(fieldMap);
            head.setCompress((byte) 1);
            head.setCompressStyle(ConstVar.LZOCompress);
            
            String fileName = prefix + "testGetRecordByLineFDCompress";
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);
            fd.create(fileName, head);               
            
            int recordNum = 1000*10000;
            for(int i = 0; i < recordNum; i++)
            {
                Record record = new Record(7);
                record.addValue(new FieldValue((byte)(1+i), (short)0));
                record.addValue(new FieldValue((short)(2+i), (short)1));
                record.addValue(new FieldValue((int)(3+i), (short)2));
                record.addValue(new FieldValue((long)(4+i), (short)3));
                record.addValue(new FieldValue((float)(5.5+i), (short)4));
                record.addValue(new FieldValue((double)(6.6+i), (short)5));
                record.addValue(new FieldValue("hello konten"+i, (short)6));
                
                fd.addRecord(record);
            }
            
            fd.close();
            
          
            
            fd.open(fileName);
            Record record = fd.getRecordByLine(-1);
            if(record != null)
            {
                fail("should get null");
            }
            
            for(int i = 0; i < 10000; i++)
            {
                record = fd.getRecordByLine(i);
                if(record == null)
                {
                    fail("should not get null:"+i);                
                }
                
                judgeNotFixedRecord(record, i);
            }            
            record = fd.getRecordByLine(recordNum);
            if(record != null)
            {
                fail("should get null");
            }
            
            record = fd.getRecordByLine(recordNum + 1);
            if(record != null)
            {
                fail("should get null");
            }  
            
            fd.close();
        }
        catch(IOException e)
        {
            e.printStackTrace();
            fail("get IOException:"+e.getMessage());                
        }           
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }           
    }        
    
    public void testGetRecordByLineFDCompressNotVar()
    {       
        try
        {
            FieldMap fieldMap = new FieldMap();
            fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
            fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
            fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
            fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
            fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
            fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
            
            Head head = new Head();
            head.setFieldMap(fieldMap);
            head.setCompress((byte) 1);
            head.setCompressStyle(ConstVar.LZOCompress);
            
            String fileName = prefix + "testGetRecordByLineFDCompressNotVar";
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);
            fd.create(fileName, head);               
            
            int recordNum = 1000*10000;
            for(int i = 0; i < recordNum; i++)
            {
                Record record = new Record(6);
                record.addValue(new FieldValue((byte)(1+i), (short)0));
                record.addValue(new FieldValue((short)(2+i), (short)1));
                record.addValue(new FieldValue((int)(3+i), (short)2));
                record.addValue(new FieldValue((long)(4+i), (short)3));
                record.addValue(new FieldValue((float)(5.5+i), (short)4));
                record.addValue(new FieldValue((double)(6.6+i), (short)5));
                               
                fd.addRecord(record);
            }
            
            fd.close();
            
            fd.open(fileName);
            Record record = fd.getRecordByLine(-1);
            if(record != null)
            {
                fail("should get null");
            }
            
            for(int i = 0; i < 10000; i++)
            {
                record = fd.getRecordByLine(i);
                if(record == null)
                {
                    fail("should not get null:"+i);                
                }
                
                judgeNotFixedRecord(record, i);
            }            
            record = fd.getRecordByLine(recordNum);
            if(record != null)
            {
                fail("should get null");
            }
            
            record = fd.getRecordByLine(recordNum + 1);
            if(record != null)
            {
                fail("should get null");
            }  
            
            fd.close();
        }
        catch(IOException e)
        {
            e.printStackTrace();
            fail("get IOException:"+e.getMessage());                
        }           
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }           
    }        
    
    public void testGetRecordByOrderFDCompress()
    {
        System.out.println("getRecordByOrderFDCompress not support!");
    }
    
    public void testGetRecordByLineFDCompressMR()
    {      
        try
        {
            FieldMap fieldMap = new FieldMap();
            fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
            fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
            fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
            fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
            fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
            fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
            fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short)6));
            
            
            Head head = new Head();
            head.setFieldMap(fieldMap);
            head.setCompress((byte) 1);
            head.setCompressStyle(ConstVar.LZOCompress);
            
            String fileName = prefix + "testGetRecordByLineFDCompress";
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);
         
                     
            fd.open(fileName);
            Record record = fd.getRecordByLine(-1);
            if(record != null)
            {
                fail("should get null");
            }
            
            Record valueRecord = new Record();
            int recordNum = 1000*10000;      
            for(int i = 0; i < 10000; i++)
            {
                record = fd.getRecordByLine(i, valueRecord);
                if(record == null)
                {
                    fail("should not get null:"+i);                
                }
                
                judgeNotFixedRecord(record, i);
            }            
            record = fd.getRecordByLine(recordNum, valueRecord);
            if(record != null)
            {
                fail("should get null");
            }
            
            record = fd.getRecordByLine(recordNum + 1);
            if(record != null)
            {
                fail("should get null");
            }  
            
            fd.close();
        }
        catch(IOException e)
        {
            e.printStackTrace();
            fail("get IOException:"+e.getMessage());                
        }           
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }           
    }       
    
    public void testGetNextRecordFDCompress()
    {      
        try
        {
            FieldMap fieldMap = new FieldMap();
            fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
            fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
            fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
            fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
            fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
            fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
            fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short)6));
            
            
            Head head = new Head();
            head.setFieldMap(fieldMap);
            head.setCompress((byte) 1);
            head.setCompressStyle(ConstVar.LZOCompress);
            
            String fileName = prefix + "testGetRecordByLineFDCompress";
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);
            
            fd.open(fileName);
            
            int recordNum = 1000*10000;
            Record record = fd.getRecordByLine(-1);
            if(record != null)
            {
                fail("should get null");
            }
            
            for(int i = 0; i < recordNum; i++)
            {
                record = fd.getNextRecord();
                if(record == null)
                {
                    fail("should not get null:"+i);                
                }
                
                judgeNotFixedRecord(record, i);
            }       
          
            record = fd.getRecordByLine(recordNum);
            if(record != null)
            {
                fail("should get null");
            }
            
            record = fd.getRecordByLine(recordNum + 1);
            if(record != null)
            {
                fail("should get null");
            }  
            
            fd.close();
        }
        catch(IOException e)
        {
            e.printStackTrace();
            fail("get IOException:"+e.getMessage());                
        }           
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }           
    }        
    
    public void testGetNextRecordFDCompressNotVar()
    {      
        try
        {
            FieldMap fieldMap = new FieldMap();
            fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
            fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
            fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
            fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
            fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
            fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
            
            Head head = new Head();
            head.setFieldMap(fieldMap);
            head.setCompress((byte) 1);
            head.setCompressStyle(ConstVar.LZOCompress);
            
            String fileName = prefix + "testGetRecordByLineFDCompressNotVar";
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);
            
            fd.open(fileName);
            
            int recordNum = 1000*10000;
            Record record = fd.getRecordByLine(-1);
            if(record != null)
            {
                fail("should get null");
            }
            
            for(int i = 0; i < recordNum; i++)
            {
                record = fd.getNextRecord();
                if(record == null)
                {
                    fail("should not get null:"+i);                
                }
                
                judgeNotFixedRecord(record, i);
            }       
         
            record = fd.getRecordByLine(recordNum);
            if(record != null)
            {
                fail("should get null");
            }
            
            record = fd.getRecordByLine(recordNum + 1);
            if(record != null)
            {
                fail("should get null");
            }  
            
            fd.close();
        }
        catch(IOException e)
        {
            e.printStackTrace();
            fail("get IOException:"+e.getMessage());                
        }           
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }           
    }    
    
    public void testGetNextRecordFDCompressMR()
    {      
        try
        {
            FieldMap fieldMap = new FieldMap();
            fieldMap.addField(new Field(ConstVar.FieldType_Byte, ConstVar.Sizeof_Byte, (short)0));
            fieldMap.addField(new Field(ConstVar.FieldType_Short, ConstVar.Sizeof_Short, (short)1));
            fieldMap.addField(new Field(ConstVar.FieldType_Int, ConstVar.Sizeof_Int, (short)2));
            fieldMap.addField(new Field(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, (short)3));
            fieldMap.addField(new Field(ConstVar.FieldType_Float, ConstVar.Sizeof_Float, (short)4));
            fieldMap.addField(new Field(ConstVar.FieldType_Double, ConstVar.Sizeof_Double, (short)5));
            fieldMap.addField(new Field(ConstVar.FieldType_String, 0, (short)6));
            
            
            Head head = new Head();
            head.setFieldMap(fieldMap);
            head.setCompress((byte) 1);
            head.setCompressStyle(ConstVar.LZOCompress);
            
            String fileName = prefix + "testGetRecordByLineFDCompress";
            Configuration conf = new Configuration();
            FormatDataFile fd = new FormatDataFile(conf);
            
            fd.open(fileName);
            
            int recordNum = 1000*10000;
            Record record = fd.getRecordByLine(-1);
            if(record != null)
            {
                fail("should get null");
            }
            
            Record valueRecord = new Record();
            for(int i = 0; i < recordNum; i++)
            {
                record = fd.getNextRecord(valueRecord);
                if(record == null)
                {
                    fail("should not get null:"+i);                
                }
                
                judgeNotFixedRecord(valueRecord, i);
            }       
       
            record = fd.getRecordByLine(recordNum);
            if(record != null)
            {
                fail("should get null");
            }
            
            record = fd.getRecordByLine(recordNum + 1);
            if(record != null)
            {
                fail("should get null");
            }  
            
            fd.close();
        }
        catch(IOException e)
        {
            e.printStackTrace();
            fail("get IOException:"+e.getMessage());                
        }           
        catch(Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }           
    }        
       
    void judgeNotFixedRecordNullField(Record record, int line)
    {
        int index = 0;
        byte[] buf = record.fieldValues().get(index).value; 
        if(buf == null)
        {
            fail("value should not null");
        }
        if(buf[0] != (byte)(1+line))
        {
            fail("error value:"+buf[0]);
        }                           
        byte type = record.fieldValues().get(index).type;
        int len = record.fieldValues().get(index).len;
        short idx = record.fieldValues().get(index).idx;
        if(len != ConstVar.Sizeof_Byte)
        {
            fail("error len:"+len);
        }
        if(type != ConstVar.FieldType_Byte)
        {
            fail("error fieldType:"+type);
        }
        if(idx != 1)
        {
            fail("error idx:"+idx);
        }
        
        
        index = 1;
        buf = record.fieldValues().get(index).value; 
        if(buf == null)
        {
            fail("value should not null");
        }
        short sval = Util.bytes2short(buf, 0, ConstVar.Sizeof_Short);
        if(sval != (short)(2+line))
        {
            fail("error value:"+sval);
        }                               
        type = record.fieldValues().get(index).type;
        len = record.fieldValues().get(index).len;
        idx = record.fieldValues().get(index).idx;
        if(len != ConstVar.Sizeof_Short)
        {
            fail("error len:"+len);
        }
        if(type != ConstVar.FieldType_Short)
        {
            fail("error fieldType:"+type);
        }
        if(idx != 3)
        {
            fail("error idx:"+idx);
        }
        
        index = 2;
        buf = record.fieldValues().get(index).value; 
        if(buf != null)
        {
            fail("value should null");
        }
        
        type = record.fieldValues().get(index).type;
        len = record.fieldValues().get(index).len;
        idx = record.fieldValues().get(index).idx;
        if(len != ConstVar.Sizeof_Int)
        {
            fail("error len:"+len);
        }
        if(type != ConstVar.FieldType_Int)
        {
            fail("error fieldType:"+type);
        }
        if(idx != 5)
        {
            fail("error idx:"+idx);
        }
        
        index = 3;
        buf = record.fieldValues().get(index).value; 
        if(buf == null)
        {
            fail("value should not null");
        }
        long lval = Util.bytes2long(buf, 0, ConstVar.Sizeof_Long);
        if(lval != 4+line)
        {
            fail("error value:"+lval);
        }                               
        type = record.fieldValues().get(index).type;
        len = record.fieldValues().get(index).len;
        idx = record.fieldValues().get(index).idx;
        if(len != ConstVar.Sizeof_Long)
        {
            fail("error len:"+len);
        }
        if(type != ConstVar.FieldType_Long)
        {
            fail("error fieldType:"+type);
        }
        if(idx != 7)
        {
            fail("error idx:"+idx);
        }
        
        index = 4;
        buf = record.fieldValues().get(index).value; 
        if(buf == null)
        {
            fail("value should not null");
        }
        float fval = Util.bytes2float(buf, 0);
        if(fval != (float)(5.5+line))
        {
            fail("error value:"+fval);
        }                               
        type = record.fieldValues().get(index).type;
        len = record.fieldValues().get(index).len;
        idx = record.fieldValues().get(index).idx;
        if(len != ConstVar.Sizeof_Float)
        {
            fail("error len:"+len);
        }
        if(type != ConstVar.FieldType_Float)
        {
            fail("error fieldType:"+type);
        }
        if(idx != 9)
        {
            fail("error idx:"+idx);
        }
        
        index = 5;
        buf = record.fieldValues().get(index).value; 
        if(buf == null)
        {
            fail("value should not null");
        }
        double dval = Util.bytes2double(buf, 0);
        if(dval != (double)(6.6+line))
        {
            fail("error value:"+dval);
        }                               
        type = record.fieldValues().get(index).type;
        len = record.fieldValues().get(index).len;
        idx = record.fieldValues().get(index).idx;
        if(len != ConstVar.Sizeof_Double)
        {
            fail("error len:"+len);
        }
        if(type != ConstVar.FieldType_Double)
        {
            fail("error fieldType:"+type);
        }
        if(idx != 11)
        {
            fail("error idx:"+idx);
        }
        
        if(record.fieldNum == 7)
        {
            index = 6;
            buf = record.fieldValues().get(index).value; 
            if(buf == null)
            {
                fail("value should not null");
            }               
            
            String tstr = "hello konten"+line;
            type = record.fieldValues().get(index).type;
            len = record.fieldValues().get(index).len;
            idx = record.fieldValues().get(index).idx;
            if(len != tstr.length())
            {
                fail("error len:"+len);
            }
            if(type != ConstVar.FieldType_String)
            {
                fail("error fieldType:"+type);
            }
            if(idx != 13)
            {
                fail("error idx:"+idx);
            }
            String str = new String(buf, 0, len);        
            if(!str.equals(tstr))
            {
                fail("error val:"+str);
            }
        }
    }
    void judgeNotFixedRecord(Record record, int line)
    {
        int index = 0;
        byte[] buf = record.fieldValues().get(index).value; 
        if(buf == null)
        {
            fail("value should not null");
        }
        if(buf[0] != (byte)(1+line))
        {
            fail("error value:"+buf[0]+", line:"+line);
        }                           
        byte type = record.fieldValues().get(index).type;
        int len = record.fieldValues().get(index).len;
        short idx = record.fieldValues().get(index).idx;
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
        
        
        index = 1;
        buf = record.fieldValues().get(index).value; 
        if(buf == null)
        {
            fail("value should not null");
        }
        short sval = Util.bytes2short(buf, 0, ConstVar.Sizeof_Short);
        if(sval != (short)(2+line))
        {
            fail("error value:"+sval);
        }                               
        type = record.fieldValues().get(index).type;
        len = record.fieldValues().get(index).len;
        idx = record.fieldValues().get(index).idx;
        if(len != ConstVar.Sizeof_Short)
        {
            fail("error len:"+len);
        }
        if(type != ConstVar.FieldType_Short)
        {
            fail("error fieldType:"+type);
        }
        if(idx != 1)
        {
            fail("error idx:"+idx);
        }
        
        index = 2;
        buf = record.fieldValues().get(index).value; 
        if(buf == null)
        {
            fail("value should not null");
        }
        int ival = Util.bytes2int(buf, 0, ConstVar.Sizeof_Int);
        if(ival != 3+line)
        {
            fail("error value:"+ival);
        }                               
        type = record.fieldValues().get(index).type;
        len = record.fieldValues().get(index).len;
        idx = record.fieldValues().get(index).idx;
        if(len != ConstVar.Sizeof_Int)
        {
            fail("error len:"+len);
        }
        if(type != ConstVar.FieldType_Int)
        {
            fail("error fieldType:"+type);
        }
        if(idx != 2)
        {
            fail("error idx:"+idx);
        }
        
        index = 3;
        buf = record.fieldValues().get(index).value; 
        if(buf == null)
        {
            fail("value should not null");
        }
        long lval = Util.bytes2long(buf, 0, ConstVar.Sizeof_Long);
        if(lval != 4+line)
        {
            fail("error value:"+lval);
        }                               
        type = record.fieldValues().get(index).type;
        len = record.fieldValues().get(index).len;
        idx = record.fieldValues().get(index).idx;
        if(len != ConstVar.Sizeof_Long)
        {
            fail("error len:"+len);
        }
        if(type != ConstVar.FieldType_Long)
        {
            fail("error fieldType:"+type);
        }
        if(idx != 3)
        {
            fail("error idx:"+idx);
        }
        
        index = 4;
        buf = record.fieldValues().get(index).value; 
        if(buf == null)
        {
            fail("value should not null");
        }
        float fval = Util.bytes2float(buf, 0);
        if(fval != (float)(5.5+line))
        {
            fail("error value:"+fval);
        }                               
        type = record.fieldValues().get(index).type;
        len = record.fieldValues().get(index).len;
        idx = record.fieldValues().get(index).idx;
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
        
        index = 5;
        buf = record.fieldValues().get(index).value; 
        if(buf == null)
        {
            fail("value should not null");
        }
        double dval = Util.bytes2double(buf, 0);
        if(dval != (double)(6.6+line))
        {
            fail("error value:"+dval);
        }                               
        type = record.fieldValues().get(index).type;
        len = record.fieldValues().get(index).len;
        idx = record.fieldValues().get(index).idx;
        if(len != ConstVar.Sizeof_Double)
        {
            fail("error len:"+len);
        }
        if(type != ConstVar.FieldType_Double)
        {
            fail("error fieldType:"+type);
        }
        if(idx != 5)
        {
            fail("error idx:"+idx);
        }
        
        if(record.fieldNum == 7)
        {
            index = 6;
            buf = record.fieldValues().get(index).value; 
            if(buf == null)
            {
                fail("value should not null");
            }               
            
            String tstr = "hello konten"+line;
            type = record.fieldValues().get(index).type;
            len = record.fieldValues().get(index).len;
            idx = record.fieldValues().get(index).idx;
            if(len != tstr.length())
            {
                fail("error len:"+len);
            }
            if(type != ConstVar.FieldType_String)
            {
                fail("error fieldType:"+type);
            }
            if(idx != 6)
            {
                fail("error idx:"+idx);
            }
            String str = new String(buf, 0, len);        
            if(!str.equals(tstr))
            {
                fail("error val:"+str+",line:"+line);
            }
        }
    }
    
    void judgeNotFixedRecordMuchFields(Record record, int line)
    {
        for(int i = 0; i < 10; i++)
        {
            int index = (i*7+0);
            byte[] buf = record.fieldValues().get(index).value; 
            if(buf == null)
            {
                fail("value should not null");
            }
            if(buf[0] != (byte)(1+line))
            {
                fail("error value:"+buf[0]);
            }                           
            byte type = record.fieldValues().get(index).type;
            int len = record.fieldValues().get(index).len;
            short idx = record.fieldValues().get(index).idx;
            if(len != ConstVar.Sizeof_Byte)
            {
                fail("error len:"+len);
            }
            if(type != ConstVar.FieldType_Byte)
            {
                fail("error fieldType:"+type);
            }            
            
            index = (i*7+1);
            buf = record.fieldValues().get(index).value; 
            if(buf == null)
            {
                fail("value should not null");
            }
            short sval = Util.bytes2short(buf, 0, ConstVar.Sizeof_Short);
            if(sval != (short)(2+line))
            {
                fail("error value:"+sval);
            }                               
            type = record.fieldValues().get(index).type;
            len = record.fieldValues().get(index).len;
            idx = record.fieldValues().get(index).idx;
            if(len != ConstVar.Sizeof_Short)
            {
                fail("error len:"+len);
            }
            if(type != ConstVar.FieldType_Short)
            {
                fail("error fieldType:"+type);
            }
           
            index = (i*7+2);
            buf = record.fieldValues().get(index).value; 
            if(buf == null)
            {
                fail("value should not null");
            }
            int ival = Util.bytes2int(buf, 0, ConstVar.Sizeof_Int);
            if(ival != 3+line)
            {
                fail("error value:"+ival);
            }                               
            type = record.fieldValues().get(index).type;
            len = record.fieldValues().get(index).len;
            idx = record.fieldValues().get(index).idx;
            if(len != ConstVar.Sizeof_Int)
            {
                fail("error len:"+len);
            }
            if(type != ConstVar.FieldType_Int)
            {
                fail("error fieldType:"+type);
            }
                       
            index = (i*7+3);
            buf = record.fieldValues().get(index).value; 
            if(buf == null)
            {
                fail("value should not null");
            }
            long lval = Util.bytes2long(buf, 0, ConstVar.Sizeof_Long);
            if(lval != 4+line)
            {
                fail("error value:"+lval);
            }                               
            type = record.fieldValues().get(index).type;
            len = record.fieldValues().get(index).len;
            idx = record.fieldValues().get(index).idx;
            if(len != ConstVar.Sizeof_Long)
            {
                fail("error len:"+len);
            }
            if(type != ConstVar.FieldType_Long)
            {
                fail("error fieldType:"+type);
            }
                       
            index = (i*7+4);
            buf = record.fieldValues().get(index).value; 
            if(buf == null)
            {
                fail("value should not null");
            }
            float fval = Util.bytes2float(buf, 0);
            if(fval != (float)(5.5+line))
            {
                fail("error value:"+fval);
            }                               
            type = record.fieldValues().get(index).type;
            len = record.fieldValues().get(index).len;
            idx = record.fieldValues().get(index).idx;
            if(len != ConstVar.Sizeof_Float)
            {
                fail("error len:"+len);
            }
            if(type != ConstVar.FieldType_Float)
            {
                fail("error fieldType:"+type);
            }           
            
            index = (i*7+5);
            buf = record.fieldValues().get(index).value; 
            if(buf == null)
            {
                fail("value should not null");
            }
            double dval = Util.bytes2double(buf, 0);
            if(dval != (double)(6.6+line))
            {
                fail("error value:"+dval);
            }                               
            type = record.fieldValues().get(index).type;
            len = record.fieldValues().get(index).len;
            idx = record.fieldValues().get(index).idx;
            if(len != ConstVar.Sizeof_Double)
            {
                fail("error len:"+len);
            }
            if(type != ConstVar.FieldType_Double)
            {
                fail("error fieldType:"+type);
            }
          
            
            index = (i*7+6);
            buf = record.fieldValues().get(index).value; 
            if(buf == null)
            {
                fail("value should not null");
            }               
            
            String tstr = "hello konten"+line;
            type = record.fieldValues().get(index).type;
            len = record.fieldValues().get(index).len;
            idx = record.fieldValues().get(index).idx;
            if(len != tstr.length())
            {
                fail("error len:"+len);
            }
            if(type != ConstVar.FieldType_String)
            {
                fail("error fieldType:"+type);
            }
            
            String str = new String(buf, 0, len);        
            if(!str.equals(tstr))
            {
                fail("error val:"+str);
            }          
        }
    }
    
    void judgeFixedRecord(Record record)
    {
        int index = 0;
        byte[] buf = record.fieldValues().get(index).value; 
        if(buf == null)
        {
            fail("value should not null");
        }
        if(buf[0] != 1)
        {
            fail("error value:"+buf[0]);
        }                           
        byte type = record.fieldValues().get(index).type;
        int len = record.fieldValues().get(index).len;
        short idx = record.fieldValues().get(index).idx;
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
        
        
        index = 1;
        buf = record.fieldValues().get(index).value; 
        if(buf == null)
        {
            fail("value should not null");
        }
        short sval = Util.bytes2short(buf, 0, ConstVar.Sizeof_Short);
        if(sval != (short)2)
        {
            fail("error value:"+sval);
        }                               
        type = record.fieldValues().get(index).type;
        len = record.fieldValues().get(index).len;
        idx = record.fieldValues().get(index).idx;
        if(len != ConstVar.Sizeof_Short)
        {
            fail("error len:"+len);
        }
        if(type != ConstVar.FieldType_Short)
        {
            fail("error fieldType:"+type);
        }
        if(idx != 1)
        {
            fail("error idx:"+idx);
        }
        
        index = 2;
        buf = record.fieldValues().get(index).value; 
        if(buf == null)
        {
            fail("value should not null");
        }
        int ival = Util.bytes2int(buf, 0, ConstVar.Sizeof_Int);
        if(ival != 3)
        {
            fail("error value:"+ival);
        }                               
        type = record.fieldValues().get(index).type;
        len = record.fieldValues().get(index).len;
        idx = record.fieldValues().get(index).idx;
        if(len != ConstVar.Sizeof_Int)
        {
            fail("error len:"+len);
        }
        if(type != ConstVar.FieldType_Int)
        {
            fail("error fieldType:"+type);
        }
        if(idx != 2)
        {
            fail("error idx:"+idx);
        }
        
        index = 3;
        buf = record.fieldValues().get(index).value; 
        if(buf == null)
        {
            fail("value should not null");
        }
        long lval = Util.bytes2long(buf, 0, ConstVar.Sizeof_Long);
        if(lval != 4)
        {
            fail("error value:"+lval);
        }                               
        type = record.fieldValues().get(index).type;
        len = record.fieldValues().get(index).len;
        idx = record.fieldValues().get(index).idx;
        if(len != ConstVar.Sizeof_Long)
        {
            fail("error len:"+len);
        }
        if(type != ConstVar.FieldType_Long)
        {
            fail("error fieldType:"+type);
        }
        if(idx != 3)
        {
            fail("error idx:"+idx);
        }
        
        index = 4;
        buf = record.fieldValues().get(index).value; 
        if(buf == null)
        {
            fail("value should not null");
        }
        float fval = Util.bytes2float(buf, 0);
        if(fval != (float)5.5)
        {
            fail("error value:"+fval);
        }                               
        type = record.fieldValues().get(index).type;
        len = record.fieldValues().get(index).len;
        idx = record.fieldValues().get(index).idx;
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
        
        index = 5;
        buf = record.fieldValues().get(index).value; 
        if(buf == null)
        {
            fail("value should not null");
        }
        double dval = Util.bytes2double(buf, 0);
        if(dval != (double)6.6)
        {
            fail("error value:"+dval);
        }                               
        type = record.fieldValues().get(index).type;
        len = record.fieldValues().get(index).len;
        idx = record.fieldValues().get(index).idx;
        if(len != ConstVar.Sizeof_Double)
        {
            fail("error len:"+len);
        }
        if(type != ConstVar.FieldType_Double)
        {
            fail("error fieldType:"+type);
        }
        if(idx != 5)
        {
            fail("error idx:"+idx);
        }
        
        if(record.fieldNum == 7)
        {
            index = 6;
            String valueString = "hello konten";
            type = record.fieldValues().get(index).type;
            len = record.fieldValues().get(index).len;
            idx = record.fieldValues().get(index).idx;
            if(len != valueString.length())
            {
                fail("error len:"+len);
            }
            if(type != ConstVar.FieldType_String)
            {
                fail("error fieldType:"+type);
            }
            if(idx != 6)
            {
                fail("error idx:"+idx);
            }
            buf = record.fieldValues().get(index).value;         
            if(buf == null)
            {
                fail("value should not null");
            }               
            String str = new String(buf, 0,len);
            if(!str.equals(valueString))
            {
                fail("error val:"+str);
            }
        }
    }
    
    public void setUp()
    {
    }
    
    public void tearDown()
    {
    }
    

}
