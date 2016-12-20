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
package org.apache.hadoop.hive.ql.dataImport;

import java.sql.Types;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class HiveTypes {

  public static final Log LOG = LogFactory.getLog(HiveTypes.class.getName());

  private HiveTypes() {
  }

  public static String toHiveType(int sqlType) {

    switch (sqlType) {
    case Types.INTEGER:
    case Types.SMALLINT:
      return "INT";
    case Types.VARCHAR:
    case Types.CHAR:
    case Types.LONGVARCHAR:
    case Types.NVARCHAR:
    case Types.NCHAR:
    case Types.LONGNVARCHAR:
    case Types.DATE:
    case Types.TIME:
    case Types.TIMESTAMP:
    case Types.CLOB:
    case Types.OTHER:
      return "STRING";
    case Types.NUMERIC:
    case Types.DECIMAL:
    case Types.FLOAT:
    case Types.DOUBLE:
    case Types.REAL:
      return "DOUBLE";
    case Types.BIT:
    case Types.BOOLEAN:
      return "BOOLEAN";
    case Types.TINYINT:
      return "TINYINT";
    case Types.BIGINT:
      return "BIGINT";
    default:
      return null;
    }
  }

  public static boolean isHiveTypeImprovised(int sqlType) {
    return sqlType == Types.DATE || sqlType == Types.TIME
        || sqlType == Types.TIMESTAMP || sqlType == Types.DECIMAL
        || sqlType == Types.NUMERIC;
  }
}
