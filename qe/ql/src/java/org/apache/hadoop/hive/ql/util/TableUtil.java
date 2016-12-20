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
package org.apache.hadoop.hive.ql.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.RangePartitionItem;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.TablePartition;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde.Constants;

public class TableUtil {
  private static final Log LOG = LogFactory.getLog("TableUtil");
  public static boolean isTableFormatChanged(QB qb, Hive db)
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
        FileSystem fs = FileSystem.get(tab.getDataLocation(), db.getConf());
        FileStatus[] states = fs.listStatus(new Path(tab.getDataLocation()
            .toString()));
        for (FileStatus s : states) {
          if (checkDir(fs, s))
            return true;
        }

      } catch (Exception e) {
        throw new SemanticException("get database : " + db_name
            + " error,make sure it exists!");
      }
    }
    return false;
  }

  public static class PartNameComparator implements Comparator<String>{

    @Override
    public int compare(String arg0, String arg1) {
      // TODO Auto-generated method stub
      if(arg0 != null && arg1 != null){
        if(arg0.equalsIgnoreCase("default")){
          return 0;
        }
        if(arg1.equalsIgnoreCase("default")){
          return -1;
        }
        return arg1.compareTo(arg0);
      }
      else if(arg0 != null && arg1 == null){
        return 1;
      }
      else if(arg0 == null && arg1 != null){
        return -1;
      }
      else {
        return 0;
      }
    }
    
  }
  
  public static boolean isTableFormatChanged(QB qb, Hive db,
      Map<String, Table> tbls, int optimizeLevel) throws SemanticException {
    
    try{
      HashMap<String, TablePartition> tblMap = qb.getMetaData().getAliasToTable();
      HiveConf conf = SessionState.get().getConf();
      int maxCheckPartNum = conf.getInt(HiveConf.ConfVars.HIVE_SELECT_STAR_CHECK_PARTITION_MAX.varname, 
          HiveConf.ConfVars.HIVE_SELECT_STAR_CHECK_PARTITION_MAX.defaultIntVal);

      for (String alias : qb.getTabAliases()) {
        TablePartition tp = tblMap.get(alias);
        String priPartName = tp.getPriPartName();
        String subPartName = tp.getSubPartName();
        
        FileSystem fs = FileSystem.get(tp.getTbl().getDataLocation(), db.getConf());
        
        if(priPartName == null && subPartName == null){         
          Partition priPart = tp.getTbl().getTTable().getPriPartition();
          Partition subPart = tp.getTbl().getTTable().getSubPartition();
     
          if(priPart != null && subPart == null){
            List<String> needCheckPartions = new ArrayList<String>();
            Set<String> partNameSet = priPart.getParSpaces().keySet();
            if(priPart.getParSpacesSize() > maxCheckPartNum){             
              for(String name:partNameSet){
                needCheckPartions.add(name);
              }
              Collections.sort(needCheckPartions, new PartNameComparator());
              
              for(int index = 0; index < maxCheckPartNum; index++){
                FileStatus[] states = fs.listStatus(new Path(tp.getTbl().getDataLocation().toString()
                    + "/" + needCheckPartions.get(index)));
                for (FileStatus s : states) {
                  if (checkDir(fs, s))
                    return true;
                }
              }
                        
              tp.setPaths(Warehouse.getPartitionPaths(tp.getTbl().getPath(), needCheckPartions, null));
            }
            else{
              for(String name:partNameSet){
                FileStatus[] states = fs.listStatus(new Path(tp.getTbl().getDataLocation().toString()
                    + "/" + name));
                for (FileStatus s : states) {
                  if (checkDir(fs, s))
                    return true;
                }
              }
            }
          }
          else if(priPart != null && subPart != null){          
            int subPartSize = subPart.getParSpaces().size();
            int needScanPartNum = (int)(maxCheckPartNum / subPartSize) + 1;
            List<String> needCheckPartions = new ArrayList<String>();
            List<String> needCheckSubPartions = new ArrayList<String>();
           
            if(priPart.getParSpacesSize() > needScanPartNum){
              
              Set<String> partNameSet = priPart.getParSpaces().keySet();
              Set<String> subPartNameSet = subPart.getParSpaces().keySet();
              for(String name:partNameSet){
                needCheckPartions.add(name);
              }
              Collections.sort(needCheckPartions, new PartNameComparator());
              
              for(String name:subPartNameSet){
                needCheckSubPartions.add(name);
              }
              Collections.sort(needCheckSubPartions, new PartNameComparator());
              
              for(int index = 0; index < needScanPartNum; index++){       
                FileStatus[] states = fs.listStatus(new Path(tp.getTbl().getDataLocation().toString()
                    + "/" + needCheckPartions.get(index)));
                for (FileStatus s : states) {
                  if (checkDir(fs, s))
                    return true;
                }
              }
              
              tp.setPaths(Warehouse.getPartitionPaths(tp.getTbl().getPath(), needCheckPartions, needCheckSubPartions));
            }
          }
          else{
            FileStatus[] states = fs.listStatus(tp.getPath());
            for (FileStatus s : states) {
              if (checkDir(fs, s))
                return true;
            }
          }
        }
        else if(priPartName != null && subPartName == null){
          Partition subPart = tp.getTbl().getTTable().getSubPartition();
          if(subPart != null){
            List<String> needCheckSubPartions = new ArrayList<String>();
            if(subPart.getParSpacesSize() > maxCheckPartNum){
              Set<String> partNameSet = subPart.getParSpaces().keySet();
              for(String name:partNameSet){
                needCheckSubPartions.add(name);
              }
              Collections.sort(needCheckSubPartions, new PartNameComparator());

              for(int index = 0; index < maxCheckPartNum; index++){
                FileStatus[] states = fs.listStatus(new Path(tp.getTbl().getDataLocation().toString()
                    + "/" + priPartName + "/" + needCheckSubPartions.get(index)));
                for (FileStatus s : states) {
                  if (checkDir(fs, s))
                    return true;
                }
              }
              
              tp.setPaths(Warehouse.getPriPartitionPaths(tp.getPath(), priPartName, needCheckSubPartions));
            }
          }
          else{
            FileStatus[] states = fs.listStatus(new Path(tp.getTbl().getDataLocation().toString()
                + "/" + priPartName));
            for (FileStatus s : states) {
              if (checkDir(fs, s))
                return true;
            }
          }          
        }
        else if(priPartName != null && subPartName != null){
          FileStatus[] states = fs.listStatus(new Path(tp.getTbl().getDataLocation().toString()
              + "/" + priPartName + "/" + subPartName));
          for (FileStatus s : states) {
            if (checkDir(fs, s))
              return true;
          }
        }        
      }
    }
    catch(Exception x){
      throw new SemanticException(x.getMessage());
    }

    return false;
  }

  private static boolean checkDir(FileSystem fs, FileStatus f)
      throws IOException {
    if (f.isDir()) {
      FileStatus[] states = fs.listStatus(f.getPath());
      for (FileStatus fileStatus : states) {
        if (checkDir(fs, fileStatus))
          return true;
      }
    } else {
      if (f.getPath().toString().toLowerCase().endsWith(".gz"))
        return true;
    }
    return false;
  }
}
