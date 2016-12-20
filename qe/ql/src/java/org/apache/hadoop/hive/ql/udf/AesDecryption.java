package org.apache.hadoop.hive.ql.udf;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.io.Text;

import sun.misc.BASE64Decoder;

//UDF是作用于单个数据行，产生一个数据行
//用户必须要继承UDF，且必须至少实现一个evalute方法，该方法并不在UDF中
//但是Hive会检查用户的UDF是否拥有一个evalute方法

@description(name = "AesDecryption", value = "_FUNC_(str) - returns the AesDecryption of str", extended = "Returns NULL if the argument is NULL.\n"
    + "Example:\n"
    + "  > SELECT _FUNC_('abc') FROM src LIMIT 1;\n"
    + "  ''")
public class AesDecryption extends UDF {
  private Text result = new Text();
  private static byte[] DefaultAesKey = "Aefse0a\0\0\0\0\0\0\0\0\0".getBytes();
  private static IvParameterSpec iv = new IvParameterSpec(DefaultAesKey);
  private static SecretKeySpec skeySpec = new SecretKeySpec(DefaultAesKey, "AES");

  // 解密
    public static String Decrypt(String sSrc, byte[] raw){
        try {
          Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            
            cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv);
            byte[] encrypted1 = (new BASE64Decoder()).decodeBuffer(sSrc);//先用base64解密
            try {
                byte[] original = cipher.doFinal(encrypted1);
                String originalString = new String(original);
                return originalString;
            } catch (Exception e) {
                System.out.println(e.toString());
                return null;
            }
        } catch (Exception ex) {
            System.out.println(ex.toString());
            return null;
        }
    }
  // 自定义方法
  public Text evaluate(Text str) {
    if (str == null)
      return null;

    String tmp=Decrypt(str.toString(), DefaultAesKey);
    if (tmp == null)
      return null;
    else
      result.set(tmp);
    return result;
    
  }
}
