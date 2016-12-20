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

import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.Constants;

import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;

import Comm.ConstVar;
import Comm.Util;
import FormatStorage.Unit.FieldValue;
import FormatStorage.Unit.Record;
import StorageEngineClient.FormatStorageSerDe;

import junit.framework.TestCase;


public class FormatStorageSerDeTest extends TestCase
{
    public void testFieldValue2Object()
    {
        try
        {
            FieldValue fileValue = new FieldValue((byte)2, (short)0);
            Object obj = fileValue.toObject();
            
            if((Byte)obj != 2)
            {
                fail("error value:"+(Byte)obj);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
       
        try
        {
            FieldValue fileValue = new FieldValue((short)2, (short)0);
            Object obj = fileValue.toObject();
            
            if((Short)obj != 2)
            {
                fail("error value:"+(Short)obj);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
        
        try
        {
            FieldValue fileValue = new FieldValue((int)2, (short)0);
            Object obj = fileValue.toObject();
            
            if((Integer)obj != 2)
            {
                fail("error value:"+(Integer)obj);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
        
        try
        {
            FieldValue fileValue = new FieldValue((long)2, (short)0);
            Object obj = fileValue.toObject();
            
            if((Long)obj != 2)
            {
                fail("error value:"+(Long)obj);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
        
        try
        {
            FieldValue fileValue = new FieldValue((float)2.2, (short)0);
            Object obj = fileValue.toObject();
            
            if((Float)obj != (float)2.2)
            {
                fail("error value:"+(Float)obj);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
        
        try
        {
            FieldValue fileValue = new FieldValue((double)2.2, (short)0);
            Object obj = fileValue.toObject();
            
            if((Double)obj != (double)2.2)
            {
                fail("error value:"+(Double)obj);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
        
        try
        {
            FieldValue fileValue = new FieldValue("hello konten", (short)0);
            Object obj = fileValue.toObject();
            
            if(!((String)obj).equals(new String("hello konten")))
            {
                fail("error value:"+(String)obj);
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }   
    }
    
    public void testFormatStorageSerDe() throws Throwable
    {
        try
        {
            FormatStorageSerDe serDe = new FormatStorageSerDe();
            Configuration conf = new Configuration();
            Properties tbl = createProperties();
            
            serDe.initialize(conf, tbl);

            Record record = new Record(7);
            record.addValue(new FieldValue((byte)1, (short)0));
            record.addValue(new FieldValue((short)2, (short)1));
            record.addValue(new FieldValue((int)3, (short)2));
            record.addValue(new FieldValue((long)4, (short)3));
            record.addValue(new FieldValue((float)5.5, (short)4));
            record.addValue(new FieldValue((double)6.6, (short)5));
            record.addValue(new FieldValue("hello konten", (short)6));
            
            
            Object[] refer = {new Byte((byte) 1),new Short((short) 2), new Integer(3), new Long(4),
                              new Float(5.5), new Double(6.6), new String("hello konten")};
            
            deserializeAndSerialize(serDe, record, refer, 1);

        }
        catch (Throwable e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
    }
    
    public void testFormatStorageSerDeNullField() throws Throwable
    {
        try
        {
            FormatStorageSerDe serDe = new FormatStorageSerDe();
            Configuration conf = new Configuration();
            Properties tbl = createProperties();
            
            serDe.initialize(conf, tbl);

            Record record = new Record(7);
            record.addValue(new FieldValue((byte)1, (short)0));
            record.addValue(new FieldValue((short)2, (short)1));
            record.addValue(new FieldValue((int)3, (short)2));            
            record.addValue(new FieldValue(ConstVar.FieldType_Long, ConstVar.Sizeof_Long, null, (short)3));
            record.addValue(new FieldValue((float)5.5, (short)4));
            record.addValue(new FieldValue((double)6.6, (short)5));
            record.addValue(new FieldValue("hello konten", (short)6));
            
            
            Object[] refer = {new Byte((byte) 1),new Short((short) 2), new Integer(3), new Long(4),
                              new Float(5.5), new Double(6.6), new String("hello konten")};
            
            deserializeAndSerialize(serDe, record, refer, 0);

        }
        catch (Throwable e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
    }
    
    public void testFormatStorageSerDeLessField() throws Throwable
    {
        try
        {
            FormatStorageSerDe serDe = new FormatStorageSerDe();
            Configuration conf = new Configuration();
            Properties tbl = createProperties();
            
            serDe.initialize(conf, tbl);

            Record record = new Record(3);
            record.addValue(new FieldValue((byte)1, (short)0));
            record.addValue(new FieldValue((short)2, (short)1));
            record.addValue(new FieldValue((int)3, (short)2));  
            
            Object[] refer = {new Byte((byte) 1),new Short((short) 2), new Integer(3), new Long(4),
                              new Float(5.5), new Double(6.6), new String("hello konten")};
            
            Object row = serDe.deserialize(record);

        }
        catch (Throwable e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
    }

    private void deserializeAndSerialize(FormatStorageSerDe serDe, Record record, Object[] refer, int flag)
    {
        try
        {
            StandardStructObjectInspector oi = (StandardStructObjectInspector)serDe.getObjectInspector();
            List<? extends StructField> fieldRefs = oi.getAllStructFieldRefs();
            if(fieldRefs.size() != 7)
            {
                fail("error fieldRefs.size:"+fieldRefs.size());
            }
            
            
            Object row = serDe.deserialize(record);
            
            
            /*
            for (int i = 0; i < fieldRefs.size(); i++)
            {
                Object fieldData = oi.getStructFieldData(row, fieldRefs.get(i));
                if (fieldData != null)
                {
                    fieldData = ((PrimitiveCategory)fieldData).getWritableObject();
                }
                assertEquals("Field " + i, refer[i], fieldData);
            }
            assertEquals(Text.class, serDe.getSerializedClass());
            */
            
            Record record2 = (Record) serDe.serialize(row, oi);
            if(flag == 2)
            {
                if(record2.fieldValues().size() != 3)
                {
                    fail("error field num:"+record2.fieldValues().size());
                }
                
                short index = 0;
                byte bv = record2.getValue(index)[0];
                if(bv != 1)
                {
                    fail("error value:"+bv);            
                }
                
                index = 1;
                short sv = Util.bytes2short(record2.getValue(index), 0, 2);
                if(sv != 2)
                {
                    fail("error value:"+sv);            
                }
                
                index = 2;
                int iv = Util.bytes2int(record2.getValue(index), 0, 4);
                if(iv != 3)
                {
                    fail("error value:"+iv);            
                }
            }
            else 
            {
                judgeRecord(record2, flag);
            }
            
            
        }
        catch (Exception e)
        {
            e.printStackTrace();
            fail("get exception:"+e.getMessage());
        }
    }
  
    private void judgeRecord(Record record, int flag)
    {
        short index = 0;
        byte bv = record.getValue(index)[0];
        if(bv != 1)
        {
            fail("error value:"+bv);            
        }
        
        index = 1;
        short sv = Util.bytes2short(record.getValue(index), 0, 2);
        if(sv != 2)
        {
            fail("error value:"+sv);            
        }
        
        index = 2;
        int iv = Util.bytes2int(record.getValue(index), 0, 4);
        if(iv != 3)
        {
            fail("error value:"+iv);            
        }
        
        index = 3;
        byte[] value = record.getValue(index);
        
        if(flag == 0) 
        {
            if(value != null)
            {
                fail("should null value");
            }
        }
        else
        {
            long lv = Util.bytes2long(record.getValue(index), 0, 8);
            if(lv != 4)
            {
                fail("error value:"+lv);            
            }
        }
       
        index = 4;
        float fv = Util.bytes2float(record.getValue(index), 0);
        if(fv != (float)5.5)
        {
            fail("error value:"+fv);            
        }
        
        index = 5;
        double dv = Util.bytes2double(record.getValue(index), 0);
        if(dv != (double)6.6)
        {
            fail("error value:"+dv);            
        }
       
        index = 6;
        String strv = new String(record.getValue(index));
        if(!strv.equals("hello konten"))
        {
            fail("error value:"+strv);            
        }
        int len = record.getLen(index);
        if(len != 12)
        {
            fail("error len:"+len);
        }
    }
    private Properties createProperties()
    {
        Properties tbl = new Properties();

        tbl.setProperty(Constants.SERIALIZATION_FORMAT, "9");
        
        tbl.setProperty("columns","abyte,ashort,aint,along,afloat,adouble,astring");
        
        tbl.setProperty("columns.types","tinyint:smallint:int:bigint:float:double:string");
        tbl.setProperty(Constants.SERIALIZATION_NULL_FORMAT, "NULL");
        return tbl;
    }

    
    public void testLazySimpleSerDeLastColumnTakesRest() throws Throwable
    {
        /*
        try
        {
            FormatStorageSerDe serDe = new FormatStorageSerDe();
            Configuration conf = new Configuration();
            Properties tbl = createProperties();
            tbl.setProperty(Constants.SERIALIZATION_LAST_COLUMN_TAKES_REST,
                            "true");
            serDe.initialize(conf, tbl);

            Text t = new Text(
                            "123\t456\t789\t1000\t5.3\thive and hadoop\t1.\ta\tb\t");
            String s = "123\t456\t789\t1000\t5.3\thive and hadoop\tNULL\ta\tb\t";
            Object[] expectedFieldsData = { new ByteWritable((byte) 123),
                            new ShortWritable((short) 456),
                            new IntWritable(789), new LongWritable(1000),
                            new DoubleWritable(5.3),
                            new Text("hive and hadoop"), null,
                            new Text("a\tb\t") };

            deserializeAndSerialize(serDe, t, s, expectedFieldsData);
        }
        catch (Throwable e)
        {
            e.printStackTrace();
            throw e;
        }
        */
    }

    
    public void testLazySimpleSerDeExtraColumns() throws Throwable
    {
        /*
        try
        {
            FormatStorageSerDe serDe = new FormatStorageSerDe();
            Configuration conf = new Configuration();
            Properties tbl = createProperties();
            serDe.initialize(conf, tbl);

            Text t = new Text(
                            "123\t456\t789\t1000\t5.3\thive and hadoop\t1.\ta\tb\t");
            String s = "123\t456\t789\t1000\t5.3\thive and hadoop\tNULL\ta";
            Object[] expectedFieldsData = { new ByteWritable((byte) 123),
                            new ShortWritable((short) 456),
                            new IntWritable(789), new LongWritable(1000),
                            new DoubleWritable(5.3),
                            new Text("hive and hadoop"), null, new Text("a") };

            deserializeAndSerialize(serDe, t, s, expectedFieldsData);

        }
        catch (Throwable e)
        {
            e.printStackTrace();
            throw e;
        }
        */
    }

    
    public void testLazySimpleSerDeMissingColumns() throws Throwable
    {
        /*
        try
        {
            FormatStorageSerDe serDe = new FormatStorageSerDe();
            Configuration conf = new Configuration();
            Properties tbl = createProperties();
            serDe.initialize(conf, tbl);

            Text t = new Text("123\t456\t789\t1000\t5.3\t");
            String s = "123\t456\t789\t1000\t5.3\t\tNULL\tNULL";
            Object[] expectedFieldsData = { new ByteWritable((byte) 123),
                            new ShortWritable((short) 456),
                            new IntWritable(789), new LongWritable(1000),
                            new DoubleWritable(5.3), new Text(""), null, null };

            deserializeAndSerialize(serDe, t, s, expectedFieldsData);

        }
        catch (Throwable e)
        {
            e.printStackTrace();
            throw e;
        }
        
        */
    }

}
