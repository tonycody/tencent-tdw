/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



package org.apache.hadoop.hive.ql.util.jdbm.helper;



public class Conversion
{

    
    public static byte[] convertToByteArray( String s )
    {
        try {
            return s.getBytes( "UTF8" );
        } catch ( java.io.UnsupportedEncodingException uee ) {
            uee.printStackTrace();
            throw new Error( "Platform doesn't support UTF8 encoding" );
        }
    }


    
    public static byte[] convertToByteArray( byte n )
    {
        n = (byte)( n ^ ( (byte) 0x80 ) ); 
        return new byte[] { n };
    }


    
    public static byte[] convertToByteArray( short n )
    {
        n = (short) ( n ^ ( (short) 0x8000 ) ); 
        byte[] key = new byte[ 2 ];
        pack2( key, 0, n );
        return key;
    }


    
    public static byte[] convertToByteArray( int n )
    {
        n = (n ^ 0x80000000); 
        byte[] key = new byte[4];
        pack4(key, 0, n);
        return key;
    }


    
    public static byte[] convertToByteArray( long n )
    {
        n = (n ^ 0x8000000000000000L); 
        byte[] key = new byte[8];
        pack8( key, 0, n );
        return key;
    }


    
    public static String convertToString( byte[] buf )
    {
        try {
            return new String( buf, "UTF8" );
        } catch ( java.io.UnsupportedEncodingException uee ) {
            uee.printStackTrace();
            throw new Error( "Platform doesn't support UTF8 encoding" );
        }
    }


    
    public static int convertToInt( byte[] buf )
    {
        int value = unpack4( buf, 0 );
        value = ( value ^ 0x80000000 ); 
        return value;
    }


    
    public static long convertToLong( byte[] buf )
    {
        long value = ( (long) unpack4( buf, 0 ) << 32  )
                     + ( unpack4( buf, 4 ) & 0xFFFFFFFFL );
        value = ( value ^ 0x8000000000000000L ); 
        return value;
    }




    static int unpack4( byte[] buf, int offset )
    {
        int value = ( buf[ offset ] << 24 )
            | ( ( buf[ offset+1 ] << 16 ) & 0x00FF0000 )
            | ( ( buf[ offset+2 ] << 8 ) & 0x0000FF00 )
            | ( ( buf[ offset+3 ] << 0 ) & 0x000000FF );

        return value;
    }


    static final void pack2( byte[] data, int offs, int val )
    {
        data[offs++] = (byte) ( val >> 8 );
        data[offs++] = (byte) val;
    }


    static final void pack4( byte[] data, int offs, int val )
    {
        data[offs++] = (byte) ( val >> 24 );
        data[offs++] = (byte) ( val >> 16 );
        data[offs++] = (byte) ( val >> 8 );
        data[offs++] = (byte) val;
    }


    static final void pack8( byte[] data, int offs, long val )
    {
        pack4( data, 0, (int) ( val >> 32 ) );
        pack4( data, 4, (int) val );
    }


    
    public static void main( String[] args )
    {
        byte[] buf;

        buf = convertToByteArray( (int) 5 );
        System.out.println( "int value of 5 is: " + convertToInt( buf ) );

        buf = convertToByteArray( (int) -1 );
        System.out.println( "int value of -1 is: " + convertToInt( buf ) );

        buf = convertToByteArray( (int) 22111000 );
        System.out.println( "int value of 22111000 is: " + convertToInt( buf ) );


        buf = convertToByteArray( (long) 5L );
        System.out.println( "long value of 5 is: " + convertToLong( buf ) );

        buf = convertToByteArray( (long) -1L );
        System.out.println( "long value of -1 is: " + convertToLong( buf ) );

        buf = convertToByteArray( (long) 1112223334445556667L );
        System.out.println( "long value of 1112223334445556667 is: " + convertToLong( buf ) );
    }

}
