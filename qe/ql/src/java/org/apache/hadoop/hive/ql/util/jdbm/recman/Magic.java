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

package org.apache.hadoop.hive.ql.util.jdbm.recman;

public interface Magic {

  public short FILE_HEADER = 0x1350;

  public short BLOCK = 0x1351;

  short FREE_PAGE = 0;
  short USED_PAGE = 1;
  short TRANSLATION_PAGE = 2;
  short FREELOGIDS_PAGE = 3;
  short FREEPHYSIDS_PAGE = 4;

  public short NLISTS = 5;

  long MAX_BLOCKS = 0x7FFFFFFFFFFFL;

  short LOGFILE_HEADER = 0x1360;

  public short SZ_BYTE = 1;

  public short SZ_SHORT = 2;

  public short SZ_INT = 4;

  public short SZ_LONG = 8;
}
