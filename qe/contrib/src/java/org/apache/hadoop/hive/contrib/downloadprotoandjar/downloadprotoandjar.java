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
package org.apache.hadoop.hive.contrib.downloadprotoadnjar; 
import java.lang.reflect.Method;
import java.net.*; 
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.io.*; 

public class downloadprotoandjar {
	public static boolean downloadprotoandjar(String url, String user, String passwd, String dbName, String tblName,String modified_time,String path)throws Exception
    {
    boolean res = true;
	String driver = "org.postgresql.Driver";
	
	ResultSet rs = null;
	Connection connect = null;
	PreparedStatement ps = null;
	try {
		Class.forName(driver).newInstance();
	} catch (InstantiationException e2) {
		e2.printStackTrace();
	} catch (IllegalAccessException e2) {
		e2.printStackTrace();
	} catch (ClassNotFoundException e2) {
		e2.printStackTrace();
	}
	try {
		connect = DriverManager.getConnection(url, user, passwd);
	} catch (SQLException e2) {
		e2.printStackTrace();
	}
	try {
		ps = connect.prepareStatement("SELECT proto_name,proto_file,jar_name,jar_file FROM pb_proto_jar WHERE db_name=? and tbl_name=? and to_char(modified_time,'yyyymmddhh24miss')=?");
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
	try {
		ps.setString(1, dbName.toLowerCase());
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
	try {
		ps.setString(2, tblName.toLowerCase());
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
	try {
		ps.setString(3, modified_time);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
	try {
		rs = ps.executeQuery();
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
	try {
		byte[] protoBytes = new byte[16*1024*1024];
		byte[] jarBytes = new byte[16*1024*1024];
		if(rs.next()){ 
			String protoName = rs.getString("proto_name");
			String protoPath = path + "/" + protoName;
			String jarName = rs.getString("jar_name");
			String jarPath = path + "/" + jarName;
			
			File proto = new File(protoPath);
			File jar = new File(jarPath);
			
			if(proto.exists())
				proto.delete();
			if(jar.exists())
				jar.delete();

			try {
	    		if (!proto.createNewFile()) 
	    			throw new Exception("Can't create proto file "+ protoPath);
	    	} catch (IOException e2) {
	    		e2.printStackTrace();
	    	}
			
			try {
	    		if (!jar.createNewFile()) 
	    			throw new Exception("Can't create jar file "+ protoPath);
	    	} catch (IOException e2) {
	    		e2.printStackTrace();
	    	}
			
			FileOutputStream protoOStream = null;
			FileOutputStream jarOStream = null;
	    	try {
	    		protoOStream = new FileOutputStream(proto);
	    	} catch (FileNotFoundException e) {
	    		e.printStackTrace();
	    	}  
	    	try {
	    		jarOStream = new FileOutputStream(jar);
	    	} catch (FileNotFoundException e) {
	    		e.printStackTrace();
	    	} 
	    		
	    	InputStream protoIStream = rs.getBinaryStream("proto_file");
	    	InputStream jarIStream = rs.getBinaryStream("jar_file");
	    	int size=0;  
	    		
	    	try {
	    		while((size=protoIStream.read(protoBytes))!=-1){  
	    			System.out.println(size);  
	    			protoOStream.write(jarBytes,0,size);  
	    		}
	    	} catch (IOException e) {
	    		e.printStackTrace();
	    	} 
	    	
	    	try {
	    		while((size=jarIStream.read(jarBytes))!=-1){  
	    			System.out.println(size);  
	    			jarOStream.write(jarBytes,0,size);  
	    		}
	    	} catch (IOException e) {
	    		e.printStackTrace();
	    	}  
		}
		else{
			throw new Exception("Can not find the proto or jar file in the tPG.");
		}
	}
	catch (SQLException e1) {
		e1.printStackTrace();
	}
	try {
		rs.close();
	} catch (SQLException e) {
		e.printStackTrace();
	}
	try {
		ps.close();
	} catch (SQLException e) {
		e.printStackTrace();
	}
	return res; 	  
    }

	public static void main(String[] args) 
	{ 
		String url = args[0];
		String user = args[1];
		String passwd = args[2];
		String dbName = args[3];
		String tblName = args[4];
		String modified_time = args[5];
		String path = args[6];
	
		try {
			downloadprotoandjar(url, user, passwd, dbName, tblName, modified_time, path);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
