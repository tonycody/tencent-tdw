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
package org.apache.hadoop.hive.ql.sql;

public class SQLTransfer {

  public static boolean isInsertSelectStar(String sql) {
    String[] array = sql.split("[\\s]");
    if (array[0].trim().equalsIgnoreCase("insert")) {
      for (int i = 0; i < array.length; i++) {
        if (array[i].trim().equalsIgnoreCase("select")) {
          if (array[i + 1].trim().equalsIgnoreCase("*")
              && array[i + 2].trim().equalsIgnoreCase("from")
              && i + 4 == array.length) {
            return true;

          }
          return false;
        }

      }
    }
    return false;
  }

  public static void main(String[] args) {
    String str = "insert table test select * from aa";
    if (isInsertSelectStar(str))
      System.out.println("is select *");
  }
}
