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
package org.apache.hadoop.hive.contrib.uploadjar;
import java.net.*; 
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.io.*; 

public class uploadjar 
{
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
  public static void closeFileInputStream(FileInputStream fis){
    try{
      fis.close();
    }
    catch(Exception x){
      
    }
  }
  
  public static boolean upload_with_version(String url, String user, String passwd, String dbName,
      String tblName,String upload_user,String modified_time, String version)throws Exception
  {
    System.out.println("upload jar new ");
    boolean res = true; 
  	String driver = "org.postgresql.Driver";
    //assume the pwd is $HIVE_HOME
    String protoName = tblName.toLowerCase() + ".proto";
    String protoPath = "./protobuf/gen/" + dbName + "/" + protoName;
    String jarName = dbName.toLowerCase() + "_" + tblName.toLowerCase() + "_" + modified_time + ".jar";
    String jarPath = "./auxlib/" + jarName;
    
    Connection connect = null;
    PreparedStatement insert = null;
    FileInputStream protofis = null;
    File protofile = new File(protoPath);
    FileInputStream jarfis = null;
    File jarfile = new File(jarPath);
    
    System.out.println("###url = " + url);
    System.out.println("###jarName = " + jarName);
    System.out.println("###jarPath = " + jarPath);
    
    if(!protofile.exists()){
    	res = false;
    	throw new Exception("Can't find proto file "+ protoPath + ". upload jar failed."
    			+ " check if it has been upload by user");
    }
    if(!jarfile.exists()){
    	res = false;
    	throw new Exception("Can't find jar file "+ jarPath + ". upload jar failed."
    			+ " check if it has been created by makejar");
    }
  

    try{
      Class.forName(driver).newInstance();
      connect = DriverManager.getConnection(url, user, passwd);

      protofis = new FileInputStream(protofile);  
      jarfis = new FileInputStream(jarfile);

      insert = connect.prepareStatement("INSERT INTO pb_proto_jar(db_name, tbl_name, proto_name, "
          + "proto_file, jar_name, jar_file, user_name, modified_time, protobuf_version) "
          + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");

      insert.setString(1, dbName);
      insert.setString(2, tblName);
      insert.setString(3, protoName);
      insert.setBinaryStream(4, protofis, (int)protofile.length());
      insert.setString(5, jarName);
      insert.setBinaryStream(6, jarfis, (int)jarfile.length());
      insert.setString(7, upload_user);

      SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
      Timestamp ts = new Timestamp(df.parse(modified_time).getTime());
      insert.setTimestamp(8, ts);
      insert.setString(9, version);

      insert.executeUpdate();
      res = true;
      
      System.out.println("###insert pb table success  ");
    }
    catch(Exception x){
      res = false;
      x.printStackTrace();
      System.out.println("###exception = " + x.getMessage());
      throw x;
    }
    finally{
      closeFileInputStream(protofis);
      closeFileInputStream(jarfis);
      closeStatement(insert);
      closeConnection(connect);
    }
  	return res; 	  
  }
  
  public static boolean upload(String url, String user, String passwd, String dbName,
      String tblName,String upload_user,String modified_time)throws Exception
  {
    boolean res = true; 
    String driver = "org.postgresql.Driver";
    String protoName = tblName.toLowerCase() + ".proto";
    //assume the pwd is $HIVE_HOME
    String protoPath = "./protobuf/gen/" + dbName + "/" + protoName;
    String jarName = dbName.toLowerCase() + "_" + tblName.toLowerCase() + "_" + modified_time + ".jar";
    String jarPath = "./auxlib/" + jarName;
    
    Connection connect = null;
    PreparedStatement insert = null;
    FileInputStream protofis = null;
    File protofile = new File(protoPath);
    FileInputStream jarfis = null;
    File jarfile = new File(jarPath);
    if(!protofile.exists()){
      res = false;
      throw new Exception("Can't find proto file "+ protoPath + ". upload jar failed."
          + " check if it has been upload by user");
    }
    if(!jarfile.exists()){
      res = false;
      throw new Exception("Can't find jar file "+ jarPath + ". upload jar failed."
          + " check if it has been created by makejar");
    }
  

    try{
      Class.forName(driver).newInstance();
      connect = DriverManager.getConnection(url, user, passwd);

      protofis = new FileInputStream(protofile);  
      jarfis = new FileInputStream(jarfile);

      insert = connect.prepareStatement("INSERT INTO pb_proto_jar(db_name, tbl_name, proto_name, "
          + "proto_file, jar_name, jar_file, user_name, modified_time) "
          + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

      insert.setString(1, dbName);
      insert.setString(2, tblName);
      insert.setString(3, protoName);
      insert.setBinaryStream(4, protofis, (int)protofile.length());
      insert.setString(5, jarName);
      insert.setBinaryStream(6, jarfis, (int)jarfile.length());
      insert.setString(7, upload_user);

      SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
      Timestamp ts = new Timestamp(df.parse(modified_time).getTime());
      insert.setTimestamp(8, ts);
      //insert.setString(9, version);

      insert.executeUpdate();
      res = true;
    }
    catch(Exception x){
      res = false;
      x.printStackTrace();
      throw x;
    }
    finally{
      closeFileInputStream(protofis);
      closeFileInputStream(jarfis);
      closeStatement(insert);
      closeConnection(connect);
    }
    return res;     
  }
  
  public static void main(String[] args) 
  { 
    String url = null;
    String user = null;
    String passwd = null;
    String dbName = null;
    String tblName = null;
    String upload_user = null;
    String modified_time = null;
    String version = "2.3.0";
    boolean containVersion = false;
    if(args.length == 7){
      url = args[0];
      user = args[1];
      passwd = args[2];
      dbName = args[3];
      tblName = args[4];
      upload_user = args[5];
      modified_time = args[6];
      containVersion = false;
    }
    else if(args.length == 8){
      url = args[0];
      user = args[1];
      passwd = args[2];
      dbName = args[3];
      tblName = args[4];
      upload_user = args[5];
      modified_time = args[6];
      version = args[7];
      containVersion = true;
    }
    
    try {
      if(containVersion){
        upload_with_version(url, user, passwd, dbName, tblName, upload_user, modified_time, version);
      }
      else{
        upload(url, user, passwd, dbName, tblName, upload_user, modified_time);
      }
  	} catch (Exception e) {
  		e.printStackTrace();
  		System.exit(1);
  	}
  } 
} 
