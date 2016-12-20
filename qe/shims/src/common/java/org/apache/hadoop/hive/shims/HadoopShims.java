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
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import java.io.IOException;
import javax.security.auth.login.LoginException;
import org.apache.hadoop.security.UserGroupInformation;

public interface HadoopShims {

  public boolean usesJobShell();

  public boolean fileSystemDeleteOnExit(FileSystem fs, Path path)
      throws IOException;

  public void inputFormatValidateInput(InputFormat fmt, JobConf conf)
      throws IOException;

  public void setTmpFiles(String prop, String files);

  public MiniDFSShim getMiniDfs(Configuration conf, int numDataNodes,
      boolean format, String[] racks) throws IOException;

  public interface MiniDFSShim {
    public FileSystem getFileSystem() throws IOException;

    public void shutdown() throws IOException;
  }

  public int compareText(Text a, Text b);

  
  /**
   * Get the UGI that the given job configuration will run as.
   *
   * In secure versions of Hadoop, this simply returns the current
   * access control context's user, ignoring the configuration.
   */

  public void closeAllForUGI(UserGroupInformation ugi);

  public UserGroupInformation getUGIForConf(Configuration conf) throws LoginException, IOException;
  
  public String getJobLauncherHttpAddress(Configuration conf);

  public String getJobLauncherRpcAddress(Configuration conf);
  
  public boolean isLocalMode(Configuration conf);
  
  public String getJobTrackerConf(Configuration conf);
}
