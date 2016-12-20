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
package Comm;

import java.util.BitSet;

public class Util {
  public static int type2len(byte type) throws Exception {
    int fieldLen = 0;
    if (type == ConstVar.FieldType_Byte) {
      fieldLen = ConstVar.Sizeof_Byte;
    } else if (type == ConstVar.FieldType_Char) {
      fieldLen = ConstVar.Sizeof_Char;
    } else if (type == ConstVar.FieldType_Short) {
      fieldLen = ConstVar.Sizeof_Short;
    } else if (type == ConstVar.FieldType_Int) {
      fieldLen = ConstVar.Sizeof_Int;
    } else if (type == ConstVar.FieldType_Float) {
      fieldLen = ConstVar.Sizeof_Float;
    } else if (type == ConstVar.FieldType_Long) {
      fieldLen = ConstVar.Sizeof_Long;
    } else if (type == ConstVar.FieldType_Double) {
      fieldLen = ConstVar.Sizeof_Double;
    } else {
      throw new SEException.InvalidParameterException("invlid type:" + type);
    }

    return fieldLen;
  }

  public static String type2name(byte type) {
    String name = ConstVar.fieldName_Unknown;
    if (type == ConstVar.FieldType_Byte) {
      name = ConstVar.fieldName_Byte;
    } else if (type == ConstVar.FieldType_Char) {
      name = ConstVar.fieldName_Char;
    } else if (type == ConstVar.FieldType_Short) {
      name = ConstVar.fieldName_Short;
    } else if (type == ConstVar.FieldType_Int) {
      name = ConstVar.fieldName_Int;
    } else if (type == ConstVar.FieldType_Float) {
      name = ConstVar.fieldName_Float;
    } else if (type == ConstVar.FieldType_Long) {
      name = ConstVar.fieldName_Long;
    } else if (type == ConstVar.FieldType_Double) {
      name = ConstVar.fieldName_Double;
    } else if (type == ConstVar.FieldType_String) {
      name = ConstVar.fieldName_String;
    }

    return name;
  }

  public static byte bitset2bytes(BitSet bits, byte[] bytes) {
    byte nbyte = (byte) (bits.length() / 8 + 1);
    if (bytes.length < nbyte) {
      bytes = new byte[nbyte];
    }

    for (int i = 0; i < bits.length(); i++) {
      if (bits.get(i)) {
        bytes[nbyte - i / 8 - 1] |= 1 << (i % 8);
      }
    }
    return nbyte;
  }

  public static BitSet bytes2bitset(byte[] bytes, byte nbyte, BitSet bits) {

    if (bytes == null) {
      return bits;
    }

    for (int i = 0; i < nbyte * 8; i++) {
      if ((bytes[nbyte - i / 8 - 1] & (1 << (i % 8))) > 0) {
        bits.set(i);
      }
    }
    return bits;
  }

  public static boolean isVarType(byte type) {
    if (type == ConstVar.FieldType_User) {
      return true;
    } else if (type == ConstVar.FieldType_String) {
      return true;
    } else {
      return false;
    }
  }

  public static boolean isValidType(byte type) {
    if (type < ConstVar.FieldType_Boolean || type > ConstVar.FieldType_String) {
      return false;
    }

    return true;
  }

  public static int long2bytes(byte[] bytes, long val) {
    for (int i = 7; i > 0; i--) {
      bytes[i] = (byte) (val);
      val >>>= 8;
    }

    bytes[0] = (byte) (val);
    return ConstVar.Sizeof_Long;
  }

  public static long bytes2long(byte[] bytes, int offset, final int length) {
    if (bytes == null || length != ConstVar.Sizeof_Long
        || (offset + length > bytes.length)) {
      return -1L;
    }
    long l = 0;
    for (int i = offset; i < (offset + length); i++) {
      l <<= 8;
      l ^= (long) bytes[i] & 0xFF;
    }
    return l;
  }

  public static byte[] boolean2bytes(final boolean b) {
    byte[] bb = new byte[1];
    bb[0] = b ? (byte) -1 : (byte) 0;
    return bb;
  }

  public static boolean bytes2boolean(final byte[] b) {
    if (b == null || b.length > 1) {
      throw new IllegalArgumentException("Array is wrong size");
    }
    return b[0] != (byte) 0;
  }

  public static int bytes2int(byte[] bytes, int offset, int length) {
    if (bytes == null || length != ConstVar.Sizeof_Int
        || (offset + length > bytes.length)) {
      return -1;
    }

    int n = 0;
    for (int i = offset; i < (offset + length); i++) {
      n <<= 8;
      n ^= bytes[i] & 0xFF;
    }
    return n;
  }

  public static int int2bytes(byte[] bytes, int val) {
    int offset = 0;
    for (int i = offset + 3; i > offset; i--) {
      bytes[i] = (byte) (val);
      val >>>= 8;
    }

    bytes[offset] = (byte) (val);
    return offset + ConstVar.Sizeof_Int;
  }

  public static void short2bytes(byte[] bytes, short val) {
    bytes[1] = (byte) (val);
    val >>>= 8;
    bytes[0] = (byte) (val);
  }

  public static short bytes2short(byte[] bytes, int offset, final int length) {
    if (bytes == null || length != ConstVar.Sizeof_Short
        || (offset + length > bytes.length)) {
      return -1;
    }

    short n = 0;
    n ^= bytes[offset] & 0xFF;
    n <<= 8;
    n ^= bytes[offset + 1] & 0xFF;
    return n;
  }

  public static int float2bytes(byte[] bytes, float f) {
    int i = Float.floatToRawIntBits(f);
    return int2bytes(bytes, i);
  }

  public static float bytes2float(byte[] bytes, int offset) {
    int i = bytes2int(bytes, offset, ConstVar.Sizeof_Int);
    return Float.intBitsToFloat(i);
  }

  public static int double2bytes(byte[] bytes, double d) {
    long l = Double.doubleToLongBits(d);
    return long2bytes(bytes, l);
  }

  public static double bytes2double(final byte[] bytes, final int offset) {
    long l = bytes2long(bytes, offset, ConstVar.Sizeof_Long);
    return Double.longBitsToDouble(l);
  }

  public static long getValue(byte type, byte[] value) throws Exception {
    if (type == ConstVar.FieldType_Byte) {
      return value[0];
    } else if (type == ConstVar.FieldType_Short) {
      return bytes2short(value, 0, ConstVar.Sizeof_Short);
    } else if (type == ConstVar.FieldType_Int) {
      return bytes2int(value, 0, ConstVar.Sizeof_Int);
    } else if (type == ConstVar.FieldType_Long) {
      return bytes2long(value, 0, ConstVar.Sizeof_Long);
    } else if (type == ConstVar.FieldType_Float) {
      System.out.println("float type, value:" + bytes2float(value, 0));
      return (long) bytes2float(value, 0);
    } else if (type == ConstVar.FieldType_Double) {
      System.out.println("double type, value:" + bytes2double(value, 0));
      return (long) bytes2double(value, 0);
    } else if (type == ConstVar.FieldType_String) {
      return 0;
    } else {
      throw new SEException.InvalidParameterException("invalid field Type:"
          + type);
    }

  }

}
