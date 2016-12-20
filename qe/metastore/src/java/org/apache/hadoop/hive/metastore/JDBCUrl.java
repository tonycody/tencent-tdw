package org.apache.hadoop.hive.metastore;

public class JDBCUrl {
  private String driver;
  private String host;
  private int port;
  private String db;
  
  public JDBCUrl(String url){
    String [] urlArray = url.split(":|/");
    if(urlArray.length < 7){
      throw new IllegalArgumentException("url is unvalid, please check it:" + url);
    }
    
    driver = urlArray[1];
    host = urlArray[4];
    port = Integer.valueOf(urlArray[5]);
    db = urlArray[6];    
  }
  
  public String getDriver(){
    return driver;
  }
  
  public String getHost(){
    return host;
  }
  
  public int getPort(){
    return port;
  }
  
  public String getDB(){
    return db;
  }
}
