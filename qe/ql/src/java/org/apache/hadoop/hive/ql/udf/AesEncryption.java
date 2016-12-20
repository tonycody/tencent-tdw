package org.apache.hadoop.hive.ql.udf;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import javax.crypto.spec.IvParameterSpec;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.io.Text;

import sun.misc.BASE64Encoder;


//UDF是作用于单个数据行，产生一个数据行
//用户必须要继承UDF，且必须至少实现一个evalute方法，该方法并不在UDF中
//但是Hive会检查用户的UDF是否拥有一个evalute方法

@description(name = "AesEncryption", value = "_FUNC_(str) - returns the AesEncryption of str", extended = "Returns NULL if the argument is NULL.\n"
    + "Example:\n"
    + "  > SELECT _FUNC_('abc') FROM src LIMIT 1;\n"
    + "  ''")
public class AesEncryption extends UDF {
  private Text result = new Text();
  private static byte[] DefaultAesKey = "Aefse0a\0\0\0\0\0\0\0\0\0".getBytes();
  private static IvParameterSpec iv = new IvParameterSpec(DefaultAesKey);// 使用CBC模式，需要一个向量iv，可增加加密算法的强度
  private static SecretKeySpec skeySpec = new SecretKeySpec(DefaultAesKey, "AES");

  // 加密
  public static String Encrypt(String sSrc, byte[] raw) {
    /*
     * if (sKey == null) { System.out.print("Key为空null"); return null; } //
     * 判断Key是否为16位 if (sKey.length() != 16) {
     * System.out.print("Key长度不是16位"); return null; } byte[] raw =
     * sKey.getBytes();
     */
    try {
      
      Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");// "算法/模式/补码方式"
      cipher.init(Cipher.ENCRYPT_MODE, skeySpec, iv);
      byte[] encrypted = cipher.doFinal(sSrc.getBytes());
      return new BASE64Encoder().encode(encrypted);// 此处使用BASE64做转码功能，同时能起到2次加密的作用。
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    } catch (NoSuchPaddingException e) {
      e.printStackTrace();
    } catch (InvalidKeyException e) {
      e.printStackTrace();
    } catch (IllegalBlockSizeException e) {
      e.printStackTrace();
    } catch (BadPaddingException e) {
      e.printStackTrace();
    } catch (InvalidAlgorithmParameterException e){
      e.printStackTrace();
    }
    return null;

  }

  // 自定义方法
  public Text evaluate(Text str) {
    if (str == null)
      return null;
    String tmp = Encrypt(str.toString(), DefaultAesKey);

    if (tmp == null)
      return null;
    else
      result.set(tmp);
    return result;
  }
  
  public Text evaluate(Text str, Text key) {
    if (str == null)
      return null;
    String tmp = Encrypt(str.toString(), key.getBytes());

    if (tmp == null)
      return null;
    else
      result.set(tmp);
    return result;
  }
}
