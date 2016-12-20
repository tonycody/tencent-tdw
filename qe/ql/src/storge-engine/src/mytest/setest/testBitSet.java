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

import java.util.BitSet;

public class testBitSet
{
    public static BitSet fromByteArray(byte[] bytes) 
    {
        BitSet bits = new BitSet();
        for (int i=0; i<bytes.length*8; i++) 
        {
            if ((bytes[bytes.length-i/8-1]&(1<<(i%8))) > 0)
            {
                bits.set(i);
            }
        }
        return bits;
    }
    
    public static byte[] toByteArray(BitSet bits) 
    {
        byte[] bytes = new byte[bits.length()/8+1];
        for (int i=0; i<bits.length(); i++)
        {
            if (bits.get(i))
            {
                bytes[bytes.length-i/8-1] |= 1<<(i%8);
            }
        }
        return bytes;
    }
    
    public static class MyBitSet
    {
        static byte[] bytes = null;
        static int len = 0;
        MyBitSet(int len)
        {
            int need = len/8+1;
            bytes = new byte[need];
            
            this.len = len;
        }
        
        public MyBitSet(MyBitSet bitSet)
        {
            this.bytes = bitSet.bytes;
            this.len = bitSet.len;
        }
        
        static void set(int i) throws Exception
        {
            if(i > bytes.length * 8)
            {
                throw new Exception("out of index:"+i);
            }
            
            bytes[bytes.length-i/8-1] |= 1<<(i%8);
        }
        
        static boolean get(int i) throws Exception
        {
            if(i > bytes.length * 8)
            {
                throw new Exception("out of index:"+i);
            }
            
            if((bytes[bytes.length - i/8 - 1] & (1 << (i % 8))) > 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        static void clear()
        {
            for(int i = 0; i < bytes.length * 8; i++)
            {
                bytes[bytes.length-i/8-1] &= 0;
            }
        }
    }

    public static void main(String argv[]) throws Exception
    {
        /*
        BitSet bit = new BitSet ();
        
        System.out.println("init bitset len:"+bit.length()+"size:"+bit.size());
        bit.set(1);
        bit.set(10);
        System.out.println("set 2,  bitset len:"+bit.length()+"size:"+bit.size());
       
        System.out.println("bit.length"+bit.length());
        
        for(int i = 0; i < bit.length(); i++)
        {
        	System.out.println("bit " + i + ", value " + bit.get(i));
        }
        
        System.out.println("bit 900:"+bit.get(900));
        
        byte[] bytes = new byte[10];
        byte nbyte = Util.bitset2bytes(bit, bytes);
        System.out.println("bytes.len  aaa"+nbyte);
        
        BitSet bit2 = new BitSet();
        Util.bytes2bitset(bytes, nbyte, bit2);
        
        System.out.println("bit2.length"+bit2.length());
        
        for(int i = 0; i < bit2.length(); i++)
        {
        	System.out.println("bit " + i + ", value " + bit2.get(i));
        }
        
        System.out.println("bit2 900:"+bit2.get(900));    
        */
        
        /*
        BitSet bsBitSet = new BitSet();
        for(short i = 0; i < 3; i++)
        {
            if((i % 2) != 0)
            {
                continue;                   
            }
            else
            {
                bsBitSet.set(i);      
            }
        }
        
        for(short i = 0; i < 3; i++)
        {
            System.out.println("bsBitSet " + i + " index, value:"+bsBitSet.get(i));
        }
        
        int pos = 0;
       
        byte needByte = (byte) (bsBitSet.length()/8 + 1);
        
        System.out.println("needByte:"+needByte);
        
        byte[] bytes = new byte[10];
        if(bytes == null || bytes.length < needByte)
        {
            bytes = new byte[needByte];
        }
        
        byte nbyte = Util.bitset2bytes(bsBitSet, bytes);  
        System.out.println("nbyte:"+nbyte);
        
        
        ByteArrayOutputStream stream1 = new ByteArrayOutputStream();  
        DataOutputStream stream = new DataOutputStream(stream1);     
        
        stream.writeByte(nbyte);     pos += 1;      
        stream.write(bytes, 0, nbyte);        pos+= nbyte;      
        
        byte[] buf2 = stream1.toByteArray();
        
        ByteArrayInputStream in1 = new ByteArrayInputStream(buf2);  
        DataInputStream in = new DataInputStream(in1);     
        
        byte n1 = in.readByte();
        in.read(bytes, 0, n1);
        
           
        BitSet bSet2 = new BitSet();
        Util.bytes2bitset(bytes, n1, bSet2);
        
        for(short i = 0; i < 3; i++)
        {
            System.out.println("bSet2 " + i + " index, value:"+bSet2.get(i));
        }        
        */
       
        /*
        BitSet bit = new BitSet();
        System.out.println("bit.len:"+bit.length());
        
        for(int i = 0; i < 10; i++)
        {
            bit.set(i);
        }
        
        byte[] bytes = new byte[10];
        byte nbyte = Util.bitset2bytes(bit, bytes);
        System.out.println("bit.len:"+bit.length()+"nbyte:"+nbyte);
        
        bit.clear(9);
        bit.clear(8);
        bit.clear(7);
        nbyte = Util.bitset2bytes(bit, bytes);
        System.out.println("bit.len:"+bit.length()+"nbyte:"+nbyte);
        */
        
        /*
        byte[] bytes = new byte[10];
        byte nbyte = Util.bitset2bytes(bit, bytes);
        System.out.println("bytes.len "+nbyte);
        
        BitSet bit2 = new BitSet();
        Util.bytes2bitset(bytes, nbyte, bit2);
        
        System.out.println("bit2.length"+bit2.length());
        */
        
        try
        {
            int len = 10;
            MyBitSet myBitSet = new MyBitSet(len);
            for(int i = 0; i < len; i++)
            {
                System.out.println(i+":"+myBitSet.get(i));
            }
            
            myBitSet.set(5);
            myBitSet.set(7);
            
            for(int i = 0; i < len; i++)
            {
                System.out.println(i+":"+myBitSet.get(i));
            }
            
            MyBitSet bitSet2 = new MyBitSet(myBitSet);
            for(int i = 0; i < len; i++)
            {
                System.out.println("bitset2 " + i+":"+bitSet2.get(i));
            }
            
            myBitSet.clear();
            for(int i = 0; i < len; i++)
            {
                System.out.println("after clear(),"+i+":"+myBitSet.get(i));
            }
            for(int i = 0; i < len; i++)
            {
                System.out.println("after clear(),"+"bitset2 " + i+":"+bitSet2.get(i));
            }
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }
}
