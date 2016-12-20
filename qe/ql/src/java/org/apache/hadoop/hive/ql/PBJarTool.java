package org.apache.hadoop.hive.ql;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.Constants;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;

public class PBJarTool {
  private static final Log LOG = LogFactory.getLog("hive.ql.PBJarTool");
  public static final String protobufPackageName = "tdw";
  
  public static void closeStatement(Statement stmt){
    try{
      stmt.close();
    }
    catch(Exception x){
      
    }
  }
  
  public static void closeConnection(Connection con){
    try{
      con.close();
    }
    catch(Exception x){
      
    }
  }
  
  public static boolean checkIsPbTable(Table tTable){
    String pbTablePro = tTable.getParameters().get("PB_TABLE");
    if(pbTablePro != null && pbTablePro.equalsIgnoreCase("true")){
      return true;
    }
    return false;
  }
  
  public static String downloadjar(String DBName, String tableName, HiveConf conf) throws SemanticException {
    LOG.info("download jar of " + DBName + "::" + tableName);
    String newPath = null;
    String modified_time = null;
    String jarName = null;
    
    String url = conf.get("hive.metastore.pbjar.url",
    "jdbc:postgresql://192.168.1.1:5432/pbjar");
    String user = conf.get("hive.metastore.user", "tdw");
    String passwd = conf.get("hive.metastore.passwd", "tdw");
    String protoVersion = HiveConf.getVar(conf,HiveConf.ConfVars.HIVEPROTOBUFVERSION);
    
    LOG.info("####################################protobuf version = " + protoVersion);
    
    String driver = "org.postgresql.Driver";
    ResultSet rs = null;
    Connection connect = null;
    PreparedStatement ps = null;
    try {
      Class.forName(driver).newInstance();
    } catch (Exception e2) {
      e2.printStackTrace();
      throw new SemanticException(e2.getMessage());
    } 
    
    try {
      connect = DriverManager.getConnection(url, user, passwd);
    
      String processName = java.lang.management.ManagementFactory
          .getRuntimeMXBean().getName();
      String processID = processName.substring(0, processName.indexOf('@'));
      String appinfo = "downloadjar_" + processID + "_"
          + SessionState.get().getSessionName();
      connect.setClientInfo("ApplicationName", appinfo);
     
      ps = connect
        .prepareStatement("SELECT to_char(modified_time,'yyyymmddhh24miss') as modified_time, "
            + "jar_file FROM pb_proto_jar WHERE db_name=? and tbl_name=? and protobuf_version=? order by modified_time desc limit 1");
      ps.setString(1, DBName.toLowerCase());
      ps.setString(2, tableName.toLowerCase());
      ps.setString(3, protoVersion);
      
      rs = ps.executeQuery();
      
      byte[] fileBytes = new byte[16 * 1024 * 1024];
      if (rs.next()) {
        modified_time = rs.getString("modified_time");
        jarName = DBName.toLowerCase() + "_" + tableName.toLowerCase() + "_"
            + modified_time + ".jar";
        newPath = "./auxlib/" + jarName;
        File file = new File(newPath);
       
        if (!file.exists()) {
          //in case of auxlib do not exist,e.g test env
          file.getParentFile().mkdirs();
          if (!file.createNewFile()){
            LOG.error("Try to create file " + newPath + " failed.");
            throw new SemanticException("Try to create file " + newPath + " failed.");
          }
             
          FileOutputStream outputStream = null;
          try{
            outputStream = new FileOutputStream(file);
            InputStream iStream = rs.getBinaryStream("jar_file");
            int size = 0;
    
            while ((size = iStream.read(fileBytes)) != -1) {
              System.out.println(size);
              outputStream.write(fileBytes, 0, size);
            }
          }
          finally{
            outputStream.close();
          }         
        } else {
          LOG.info("The jar file of " + jarName + " is the latest version.");
        }
      } else {
        LOG.error("Can not find the jar file of " + jarName + " in the tPG.");
        throw new SemanticException("Can not find the jar file of " + jarName + " in the tPG.");
      }     
    } catch (Exception e2) {
      e2.printStackTrace();
      throw new SemanticException(e2.getMessage());
    }
    finally{
      closeStatement(ps);
      closeConnection(connect);
    }
    
    return modified_time;
    }
}
