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

public class RecordManagerOptions {

  public static final String PROVIDER_FACTORY = "jdbm.provider";

  public static final String THREAD_SAFE = "jdbm.threadSafe";

  public static final String AUTO_COMMIT = "jdbm.autoCommit";

  public static final String DISABLE_TRANSACTIONS = "jdbm.disableTransactions";

  public static final String CACHE_TYPE = "jdbm.cache.type";

  public static final String CACHE_SIZE = "jdbm.cache.size";

  public static final String NORMAL_CACHE = "normal";

  public static final String SOFT_REF_CACHE = "soft";

  public static final String WEAK_REF_CACHE = "weak";

  public static final String NO_CACHE = "nocache";

}
