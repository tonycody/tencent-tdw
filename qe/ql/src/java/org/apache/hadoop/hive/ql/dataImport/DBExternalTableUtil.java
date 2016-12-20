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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.Constants;

public class DBExternalTableUtil {
  public static boolean isDBExternalTable(QB qb, Hive db)
      throws SemanticException {
    for (String alias : qb.getTabAliases()) {
      String tab_name = qb.getTabNameForAlias(alias);
      String db_name = qb.getTableRef(alias).getDbName();
      Table tab = null;
      try {
        db.getDatabase(db_name);
        if (!db.hasAuth(SessionState.get().getUserName(),
            Hive.Privilege.SELECT_PRIV, db_name, tab_name)) {
          throw new SemanticException("user : "
              + SessionState.get().getUserName()
              + " do not have SELECT privilege on table : " + db_name + "::"
              + tab_name);
        }

        tab = db.getTable(db_name, tab_name);
        String tableServer = tab.getSerdeParam(Constants.TABLE_SERVER);
        if (tableServer != null && !tableServer.isEmpty()) {
          return true;
        }

      } catch (Exception e) {
        throw new SemanticException("get database : " + db_name
            + " error,make sure it exists!");
      }
    }
    return false;
  }

  public static boolean isDBExternal(Hive db, String db_name, String tab_name) {

    try {
      Table tab = null;
      if (!db.hasAuth(SessionState.get().getUserName(),
          Hive.Privilege.SELECT_PRIV, db_name, tab_name)) {
        throw new SemanticException("user : "
            + SessionState.get().getUserName()
            + " do not have SELECT privilege on table : " + db_name + "::"
            + tab_name);
      }
      tab = db.getTable(db_name, tab_name);
      String tableServer = tab.getSerdeParam(Constants.TABLE_SERVER);
      if (tableServer != null && !tableServer.isEmpty()) {
        return true;
      }
      return false;
    } catch (SemanticException e) {
      return false;
    } catch (HiveException e) {
      return false;
    }
  }

  public static boolean isDBExternalCanInsert(Hive db, String dbName,
      String tableName) {
    try {
      Table tab = null;
      if (!db.hasAuth(SessionState.get().getUserName(),
          Hive.Privilege.SELECT_PRIV, dbName, tableName)) {
        throw new SemanticException("user : "
            + SessionState.get().getUserName()
            + " do not have SELECT privilege on table : " + dbName + "::"
            + tableName);
      }
      tab = db.getTable(dbName, tableName);
      String tableServer = tab.getSerdeParam(Constants.TABLE_SERVER);
      String table = tab.getSerdeParam(Constants.TABLE_NAME);
      if (tableServer != null && !tableServer.isEmpty() && table != null
          && !table.isEmpty()) {
        return true;
      }
      return false;
    } catch (SemanticException e) {
      e.printStackTrace();
      return false;
    } catch (HiveException e) {
      e.printStackTrace();
      return false;
    }
  }

  public static boolean isDBExternalCanInsert(Table tab) {
    String tableServer = tab.getSerdeParam(Constants.TABLE_SERVER);
    String table = tab.getSerdeParam(Constants.TABLE_NAME);

    if (tableServer != null && !tableServer.isEmpty() && table != null
        && !table.isEmpty()) {
      return true;
    }

    return false;
  }

  public static boolean isDBExternalTable(QB qb, Hive db,
      Map<String, Table> tbls, int optimizeLevel) throws SemanticException {
    for (String alias : qb.getTabAliases()) {
      String tab_name = qb.getTabNameForAlias(alias);
      String db_name = qb.getTableRef(alias).getDbName();
      Table tab = null;

      switch (optimizeLevel) {
      case 0:
        try {
          db.getDatabase(db_name);
          if (!db.hasAuth(SessionState.get().getUserName(),
              Hive.Privilege.SELECT_PRIV, db_name, tab_name)) {
            throw new SemanticException("user : "
                + SessionState.get().getUserName()
                + " do not have SELECT privilege on table : " + db_name + "::"
                + tab_name);
          }

          tab = db.getTable(db_name, tab_name);

          String tableServer = tab.getSerdeParam(Constants.TABLE_SERVER);
          if (tableServer != null && !tableServer.isEmpty()) {
            return true;
          }
        } catch (Exception e) {
          throw new SemanticException("get database : " + db_name
              + " error,make sure it exists!");
        }
        break;

      case 1:
      case 2:
      default:

        if (tbls != null && !tbls.isEmpty()) {
          tab = tbls.get(db_name + "/" + tab_name);
        }

        try {
          if (tab == null) {
            db.getDatabase(db_name);
            if (!db.hasAuth(SessionState.get().getUserName(),
                Hive.Privilege.SELECT_PRIV, db_name, tab_name)) {
              throw new SemanticException("user : "
                  + SessionState.get().getUserName()
                  + " do not have SELECT privilege on table : " + db_name
                  + "::" + tab_name);
            }

            tab = db.getTable(db_name, tab_name);

            if (tbls != null) {
              tbls.put(db_name + "/" + tab_name, tab);
            }
          }

          String tableServer = tab.getSerdeParam(Constants.TABLE_SERVER);
          if (tableServer != null && !tableServer.isEmpty()) {
            return true;
          }
        } catch (Exception e) {
          throw new SemanticException("get database : " + db_name
              + " error,make sure it exists!");
        }
        break;
      }
    }
    return false;
  }

  public static boolean isDBExternal(Table tab) {
    String tableServer = tab.getSerdeParam(Constants.TABLE_SERVER);
    if (tableServer != null && !tableServer.isEmpty()) {
      return true;
    }

    return false;
  }

  public static boolean isDBExternal(Hive db, String db_name, String tab_name,
      Map<String, Table> tbls, int optimizeLevel) {
    try {
      Table tab = null;

      switch (optimizeLevel) {
      case 0:
        tab = db.getTable(db_name, tab_name);
        break;

      case 1:
      case 2:
      default:
        if (tbls != null && !tbls.isEmpty()) {
          tab = tbls.get(db_name + "/" + tab_name);
        }

        if (tab == null) {
          tab = db.getTable(db_name, tab_name);
          if (tbls != null) {
            tbls.put(db_name + "/" + tab_name, tab);
          }
        }
        break;
      }

      String tableServer = tab.getSerdeParam(Constants.TABLE_SERVER);
      if (tableServer != null && !tableServer.isEmpty()) {
        return true;
      }
      return false;
    } catch (SemanticException e) {
      return false;
    } catch (HiveException e) {
      return false;
    }
  }

  public static boolean isDBExternalCanInsert(Hive db, String dbName,
      String tableName, Map<String, Table> tbls, int optimizeLevel) {
    try {
      Table tab = null;

      switch (optimizeLevel) {
      case 0:
        tab = db.getTable(dbName, tableName);
        break;

      case 1:
      case 2:
      default:
        if (tbls != null && !tbls.isEmpty()) {
          tab = tbls.get(dbName + "/" + tableName);
        }

        if (tab == null) {
          tab = db.getTable(dbName, tableName);
          if (tbls != null) {
            tbls.put(dbName + "/" + tableName, tab);
          }
        }
        break;
      }

      String tableServer = tab.getSerdeParam(Constants.TABLE_SERVER);
      String table = tab.getSerdeParam(Constants.TABLE_NAME);
      if (tableServer != null && !tableServer.isEmpty() && table != null
          && !table.isEmpty()) {
        return true;
      }
      return false;

    } catch (SemanticException e) {
      e.printStackTrace();
      return false;
    } catch (HiveException e) {
      e.printStackTrace();
      return false;
    }
  }
}
