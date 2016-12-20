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
package org.apache.hadoop.hive.shims;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import java.io.IOException;
import org.apache.hadoop.security.UserGroupInformation;
import javax.security.auth.login.LoginException;

public class Hadoop20Shims implements HadoopShims {
  public boolean usesJobShell() {
    return false;
  }

  public boolean fileSystemDeleteOnExit(FileSystem fs, Path path)
      throws IOException {

    return fs.deleteOnExit(path);
  }

  public void inputFormatValidateInput(InputFormat fmt, JobConf conf)
      throws IOException {
  }

  public void setTmpFiles(String prop, String files) {
  }

  public HadoopShims.MiniDFSShim getMiniDfs(Configuration conf,
      int numDataNodes, boolean format, String[] racks) throws IOException {
    return new MiniDFSShim(
        new MiniDFSCluster(conf, numDataNodes, format, racks));
  }

  public class MiniDFSShim implements HadoopShims.MiniDFSShim {
    private MiniDFSCluster cluster;

    public MiniDFSShim(MiniDFSCluster cluster) {
      this.cluster = cluster;
    }

    public FileSystem getFileSystem() throws IOException {
      return cluster.getFileSystem();
    }

    public void shutdown() {
      cluster.shutdown();
    }
  }

  public int compareText(Text a, Text b) {
    return a.compareTo(b);
  }

  @Override
  public void closeAllForUGI(UserGroupInformation ugi) {
    // TODO Auto-generated method stub
    
}

  @Override
  public UserGroupInformation getUGIForConf(Configuration conf)
      throws LoginException, IOException {
    UserGroupInformation ugi = UserGroupInformation.readFrom(conf);
    if(ugi == null) {
      ugi = UserGroupInformation.login(conf);
    }
    return ugi;
  }

  @Override
  public String getJobLauncherHttpAddress(Configuration conf) {
    return conf.get("mapred.job.tracker.http.address");
  }

  @Override
  public String getJobLauncherRpcAddress(Configuration conf){
    return conf.get("mapred.job.tracker");
  }

  @Override
  public boolean isLocalMode(Configuration conf) {
    return "local".equals(getJobLauncherRpcAddress(conf));
  }
  
  @Override
  public String getJobTrackerConf(Configuration conf) {
    return "mapred.job.tracker="+conf.get("mapred.job.tracker");
  }

}
