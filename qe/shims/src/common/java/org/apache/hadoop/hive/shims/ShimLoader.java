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

import org.apache.hadoop.util.VersionInfo;
import java.util.Map;
import java.util.HashMap;

public abstract class ShimLoader {
  private static HadoopShims hadoopShims;
  private static JettyShims jettyShims;

  private static HashMap<String, String> HADOOP_SHIM_CLASSES = new HashMap<String, String>();

  static {
    HADOOP_SHIM_CLASSES.put("0.17",
        "org.apache.hadoop.hive.shims.Hadoop17Shims");
    HADOOP_SHIM_CLASSES.put("0.18",
        "org.apache.hadoop.hive.shims.Hadoop18Shims");
    HADOOP_SHIM_CLASSES.put("0.19",
        "org.apache.hadoop.hive.shims.Hadoop19Shims");
    HADOOP_SHIM_CLASSES.put("0.20",
        "org.apache.hadoop.hive.shims.Hadoop20Shims");
    HADOOP_SHIM_CLASSES.put("2.1", "org.apache.hadoop.hive.shims.Hadoop21Shims");
    HADOOP_SHIM_CLASSES.put("2.2", "org.apache.hadoop.hive.shims.Hadoop22Shims");
    HADOOP_SHIM_CLASSES.put("2.4", "org.apache.hadoop.hive.shims.Hadoop22Shims");
  }

  private static HashMap<String, String> JETTY_SHIM_CLASSES = new HashMap<String, String>();

  static {
    JETTY_SHIM_CLASSES.put("0.17", "org.apache.hadoop.hive.shims.Jetty17Shims");
    JETTY_SHIM_CLASSES.put("0.18", "org.apache.hadoop.hive.shims.Jetty18Shims");
    JETTY_SHIM_CLASSES.put("0.19", "org.apache.hadoop.hive.shims.Jetty19Shims");
    JETTY_SHIM_CLASSES.put("0.20", "org.apache.hadoop.hive.shims.Jetty20Shims");
    JETTY_SHIM_CLASSES.put("2.1", "org.apache.hadoop.hive.shims.Jetty21Shims");
    JETTY_SHIM_CLASSES.put("2.2", "org.apache.hadoop.hive.shims.Jetty22Shims");
    JETTY_SHIM_CLASSES.put("2.4", "org.apache.hadoop.hive.shims.Jetty22Shims");
  }

  public synchronized static HadoopShims getHadoopShims() {
    if (hadoopShims == null) {
      hadoopShims = loadShims(HADOOP_SHIM_CLASSES, HadoopShims.class);
    }
    return hadoopShims;
  }

  public synchronized static JettyShims getJettyShims() {
    if (jettyShims == null) {
      jettyShims = loadShims(JETTY_SHIM_CLASSES, JettyShims.class);
    }
    return jettyShims;
  }

  @SuppressWarnings("unchecked")
  private static <T> T loadShims(Map<String, String> classMap, Class<T> xface) {
    String vers = getMajorVersion();
    String className = classMap.get(vers);
    try {
      Class clazz = Class.forName(className);
      return xface.cast(clazz.newInstance());
    } catch (Exception e) {
      throw new RuntimeException("Could not load shims in class " + className,
          e);
    }
  }

  private static String getMajorVersion() {
    String vers = VersionInfo.getVersion();

    String parts[] = vers.split("\\.");
    if (parts.length < 2) {
      throw new RuntimeException("Illegal Hadoop Version: " + vers
          + " (expected A.B.* format)");
    }
    return parts[0] + "." + parts[1];
  }
}
