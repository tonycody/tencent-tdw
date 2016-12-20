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

package org.apache.hadoop.hive.ql.util.jdbm;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public final class RecordManagerFactory {

  public static RecordManager createRecordManager(String name)
      throws IOException {
    return createRecordManager(name, new Properties());
  }

  public static RecordManager createRecordManager(String name,
      Properties options) throws IOException {
    RecordManagerProvider factory = getFactory(options);
    return factory.createRecordManager(name, options);
  }

  public static RecordManager createRecordManager(File file, Properties options)
      throws IOException {
    RecordManagerProvider factory = getFactory(options);
    return factory.createRecordManager(file, options);
  }

  private static RecordManagerProvider getFactory(Properties options) {
    String provider;
    Class clazz;
    RecordManagerProvider factory;

    provider = options.getProperty(RecordManagerOptions.PROVIDER_FACTORY,
        "org.apache.hadoop.hive.ql.util.jdbm.recman.Provider");

    try {
      clazz = Class.forName(provider);
      factory = (RecordManagerProvider) clazz.newInstance();
    } catch (Exception except) {
      throw new IllegalArgumentException("Invalid record manager provider: "
          + provider + "\n[" + except.getClass().getName() + ": "
          + except.getMessage() + "]");
    }
    return factory;
  }

}
