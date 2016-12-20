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
package org.apache.hadoop.hive.ql.exec;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.FileSinkCount;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.InsertPartDesc;
import org.apache.hadoop.hive.ql.parse.QB.PartRefType;
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory;
import org.apache.hadoop.hive.ql.plan.PartValuesList;
import org.apache.hadoop.hive.ql.plan.RangePartitionExprTree;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.fileSinkDesc;
import org.apache.hadoop.hive.ql.plan.partitionSinkDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.LongWritable;

public class PartitionerOperator extends TerminalOperator<partitionSinkDesc>
    implements Serializable {

  private static final long serialVersionUID = 1L;

  transient protected ExprNodeEvaluator[] partKeyFields;
  transient protected ObjectInspector[] partKeyObjectInspectors;
  transient protected Object[] partKeyObjects;

  transient protected Partitioner[] partitioners;
  transient protected int[] targetPartitions;
  transient protected int[] numPartitionsEachLevel;
  transient protected int numPartKeys;

  transient protected String[][] partNames;
  transient protected FileSinkOperator[][] fsOps;

  transient protected Configuration config;
  transient protected partitionSinkDesc partSinkDesc;

  transient protected InsertPartDesc insertPartDesc;
  transient protected int errorNum;
  transient protected int errorLimit;
  transient protected List<String> partNameList;
  transient protected List<String> subPartNameList;
  transient protected PartRefType partType;
  transient boolean isInsertPart;
  transient Map<String, Integer> partErrorMap;
  transient private LongWritable partition_filesink_success_count = new LongWritable();

  public static enum PartitionFileSinkCount {
    PARTITIONER_SUCCESS_COUNT
  }

  @Override
  protected void initializeOp(Configuration hconf) throws HiveException {
    this.config = hconf;

    partSinkDesc = (partitionSinkDesc) conf;

    assert (inputObjInspectors.length == 1);
    ObjectInspector rowInspector = inputObjInspectors[0];

    System.out.println("row inspector is " + rowInspector);

    statsMap.put(PartitionFileSinkCount.PARTITIONER_SUCCESS_COUNT,
        partition_filesink_success_count);

    numPartKeys = partSinkDesc.getPartKeys().size();
    partKeyFields = new ExprNodeEvaluator[numPartKeys];
    partKeyObjectInspectors = new ObjectInspector[numPartKeys];
    partKeyObjects = new Object[numPartKeys];
    partNames = new String[numPartKeys][];
    partitioners = new Partitioner[numPartKeys];
    numPartitionsEachLevel = new int[numPartKeys];
    
    ArrayList<RangePartitionExprTree> exprTrees = genRangePartitionExprTree();
    partSinkDesc.setExprTrees(exprTrees);
    

    insertPartDesc = partSinkDesc.getInsertPartDesc();
    if (insertPartDesc != null) {
      errorNum = 0;
      errorLimit = insertPartDesc.getErrorLimit();
      if (errorLimit < 0) {
        errorLimit = Integer.MAX_VALUE;
      }
      partNameList = insertPartDesc.getPartList();
      subPartNameList = insertPartDesc.getSubPartList();
      partType = insertPartDesc.getPartType();
      isInsertPart = insertPartDesc.isInsertPart();
      partErrorMap = new HashMap<String, Integer>();
    } else {
      errorNum = 0;
      errorLimit = 0;
      partNameList = null;
      subPartNameList = null;
      partType = null;
      isInsertPart = false;
      partErrorMap = null;
    }

    System.out.println("Num partition keys is " + numPartKeys + ", "
        + partSinkDesc.getPartTypes());
    System.out.println("File Sink descriptor : " + partSinkDesc);

    for (int i = 0; i < numPartKeys; i++) {
      partKeyFields[i] = ExprNodeEvaluatorFactory.get(partSinkDesc
          .getPartKeys().get(i));
      partKeyObjectInspectors[i] = partKeyFields[i].initialize(rowInspector);
      partKeyObjects[i] = null;

      numPartitionsEachLevel[i] = partSinkDesc.getPartSpaces().get(i)
          .getPartSpace().size();

      System.out.println("Partiton level " + i + " has "
          + numPartitionsEachLevel[i] + " partitons.");

      partNames[i] = new String[numPartitionsEachLevel[i]];
      int j = 0;
      for (Entry<String, PartValuesList> entry : partSinkDesc.getPartSpaces()
          .get(i).getPartSpace().entrySet()) {
        partNames[i][j] = entry.getKey();
        ++j;
      }

      String partitionType = partSinkDesc.getPartTypes().get(i);
      if (partitionType.equalsIgnoreCase("LIST")) {
        partitioners[i] = getListPartitioner(i);
      } else if (partitionType.equalsIgnoreCase("RANGE")) {
        partitioners[i] = getRangePartitioner(i);
      } else if (partitionType.equalsIgnoreCase("HASH")) {
        partitioners[i] = getHashPartitioner(i);
      } else {
        throw new HiveException("Unknow partition type.");
      }
    }

    fsOps = new FileSinkOperator[numPartitionsEachLevel[0]][];
    int partitionsLevel2 = numPartKeys == 1 ? 1 : numPartitionsEachLevel[1];
    for (int i = 0; i < numPartitionsEachLevel[0]; i++) {
      fsOps[i] = new FileSinkOperator[partitionsLevel2];
    }

    targetPartitions = new int[2];
    Arrays.fill(targetPartitions, 0);
    initializeChildren(hconf);
  }
  
  private RangePartitionExprTree getRangePartitionExprTree(
      String partKeyTypeName, exprNodeDesc partKey,
      Map<String, PartValuesList> partSpace) {
    TypeInfo partKeyType = TypeInfoFactory
        .getPrimitiveTypeInfo(partKeyTypeName);

    ObjectInspector stringOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING);
    ObjectInspector valueOI = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableObjectInspector(((PrimitiveTypeInfo) partKeyType)
            .getPrimitiveCategory());

    ArrayList<exprNodeDesc> exprTree = new ArrayList<exprNodeDesc>();
    for (Entry<String, PartValuesList> entry : partSpace.entrySet()) {
      String partName = entry.getKey();
      PartValuesList partValues = entry.getValue();

      if (partName.equalsIgnoreCase("default")) {
        exprTree.add(null);
      } else {
        ObjectInspectorConverters.Converter converter = ObjectInspectorConverters
            .getConverter(stringOI, valueOI);
        Object pv = converter.convert(partValues.getValues().get(0));
        pv = ((PrimitiveObjectInspector) valueOI).getPrimitiveJavaObject(pv);

        exprNodeDesc partValueDesc = new exprNodeConstantDesc(partKeyType, pv);
        exprNodeDesc compareDesc = TypeCheckProcFactory.DefaultExprProcessor
            .getFuncExprNodeDesc("comparison", partValueDesc, partKey);

        exprTree.add(compareDesc);
      }
    }

    return new RangePartitionExprTree(exprTree);
  }

  private ArrayList<RangePartitionExprTree> genRangePartitionExprTree(){
    int partTotalLevel = numPartKeys;
    ArrayList<RangePartitionExprTree> exprTrees = new ArrayList<RangePartitionExprTree>();
    
    for(int level = 0; level < partTotalLevel; level++){
      String partType = partSinkDesc.getPartTypes().get(level);
      //String partKeyType = partSinkDesc.getPartKeys().get(level).getTypeInfo().getTypeName();
      //String partKeyName = partSinkDesc.getPartKeys().get(level).getExprString();
      Map<String,PartValuesList> partSpace = partSinkDesc.getPartSpaces().get(level).getPartSpace();
      exprNodeDesc partKeyDesc = partSinkDesc.getPartKeys().get(level);
      String partKeyType = partKeyDesc.getTypeInfo().getTypeName();
      
      if (partType.equalsIgnoreCase("RANGE")) {
        exprTrees.add(getRangePartitionExprTree(partKeyType,
            partKeyDesc, partSpace));
      } else {
        exprTrees.add(null);
      }
    }
    return exprTrees;
  }
  
  private Partitioner getRangePartitioner(int level) throws HiveException {

    ExprNodeEvaluator[] partEvaluators = new ExprNodeEvaluator[partSinkDesc
        .getPartSpaces().get(level).getPartSpace().size()];

    int defaultPart = -1, partition = 0;
    Map<String, PartValuesList> partSpace = partSinkDesc.getPartSpaces()
        .get(level).getPartSpace();
    ArrayList<exprNodeDesc> exprTree = partSinkDesc.getExprTrees().get(level)
        .getExprs();   

    for (String partName : partSpace.keySet()) {
      if (partName.equalsIgnoreCase("default")) {
        defaultPart = partition;
        ++partition;
        continue;
      }

      exprNodeDesc partKeyDesc = exprTree.get(partition);
      if (partKeyDesc == null) {
        throw new HiveException("No partition values defined in partition "
            + partName);
      }
      partEvaluators[partition] = ExprNodeEvaluatorFactory.get(exprTree
          .get(partition));
      partEvaluators[partition].initialize(inputObjInspectors[0]);

      ++partition;
    }

    System.out.println("Total partiton is " + partition);
    
    int maxCacheSize = 10000;
    if(config != null){
      maxCacheSize = config.getInt(HiveConf.ConfVars.HIVE_EXEC_RANGEPART_CACHE_MAXSIZE.varname, 
          HiveConf.ConfVars.HIVE_EXEC_RANGEPART_CACHE_MAXSIZE.defaultIntVal);
    }

    return new RangePartitioner(partKeyObjectInspectors[level],
        partKeyFields[level], partEvaluators, defaultPart, maxCacheSize);
  }

  private Partitioner getListPartitioner(int level) throws HiveException {
    TypeInfo partKeyType = TypeInfoFactory.getPrimitiveTypeInfo(partSinkDesc
        .getPartTypeInfos().get(level));
    if (partKeyType.getCategory() != Category.PRIMITIVE) {
      throw new HiveException(
          "Current just accept primitive type as partition key.");
    }

    Map<Object, Integer> partitionValueSpaces = new HashMap<Object, Integer>();

    ObjectInspector stringOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING);

    ObjectInspector valueOI = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableObjectInspector(((PrimitiveTypeInfo) partKeyType)
            .getPrimitiveCategory());

    int partition = 0, defaultPart = -1;
    Map<String, PartValuesList> partSpace = partSinkDesc.getPartSpaces()
        .get(level).getPartSpace();
    for (Map.Entry<String, PartValuesList> entry : partSpace.entrySet()) {
      String partName = entry.getKey();
      List<String> partValues = entry.getValue().getValues();

      if (partName.equalsIgnoreCase("default"))
        defaultPart = partition;

      for (String value : partValues) {
        ObjectInspectorConverters.Converter converter = ObjectInspectorConverters
            .getConverter(stringOI, valueOI);

        Object pv = converter.convert(value);
        partitionValueSpaces.put(pv, partition);
      }

      ++partition;
    }

    return new ListPartitioner(partKeyObjectInspectors[level],
        partKeyFields[level], partitionValueSpaces, defaultPart);
  }

  private Partitioner getHashPartitioner(int level) throws HiveException {
    Map<String, PartValuesList> partSpace = partSinkDesc.getPartSpaces()
        .get(level).getPartSpace();

    return new HashPartitioner(partKeyObjectInspectors[level],
        partKeyFields[level], partSpace.size());
  }

  @Override
  public void process(Object row, int tag) throws HiveException {
    for (int i = 0; i < numPartKeys; i++) {
      targetPartitions[i] = partitioners[i].getPartition(row);
    }

    FileSinkOperator op = fsOps[targetPartitions[0]][targetPartitions[1]];
    if (op == null) {
      int partId = getPartitionId(targetPartitions[0], targetPartitions[1]);
      String dirName = conf.getDirName() + "/"
          + partNames[0][targetPartitions[0]];

      if (isInsertPart) {
        boolean isFind = false;
        switch (partType) {
        case PRI:
          for (String partName : partNameList) {
            if (partName.equalsIgnoreCase(partNames[0][targetPartitions[0]])) {
              isFind = true;
              break;
            }
          }
          break;
        case COMP:
          for (String partName : partNameList) {
            if (partName.equalsIgnoreCase(partNames[0][targetPartitions[0]])) {
              for (String subPartName : subPartNameList) {
                if (subPartName
                    .equalsIgnoreCase(partNames[1][targetPartitions[1]])) {
                  isFind = true;
                  break;
                }
              }
              if (isFind)
                break;
            }
          }
          break;

        case SUB:
          break;
        }

        if (!isFind) {
          Integer errorNumTmp = null;
          String partName = null;
          switch (partType) {
          case PRI:
            partName = partNames[0][targetPartitions[0]];
            errorNumTmp = partErrorMap.get(partName);
            if (errorNumTmp == null) {
              errorNumTmp = 1;
              partErrorMap.put(partName, errorNumTmp);
            } else {
              errorNumTmp++;
              partErrorMap.put(partName, errorNumTmp);
            }
            break;
          case COMP:

            partName = partNames[0][targetPartitions[0]] + "/"
                + partNames[1][targetPartitions[1]];

            errorNumTmp = partErrorMap.get(partName);
            if (errorNumTmp == null) {
              errorNumTmp = 1;
              partErrorMap.put(partName, errorNumTmp);
            } else {
              errorNumTmp++;
              partErrorMap.put(partName, errorNumTmp);
            }

            break;
          case SUB:
            break;
          }

          errorNum++;

          if (errorNum >= errorLimit)
            throw new HiveException(
                "too many data lines that can not insert dest partition!! "
                    + "\n" + "error information " + getLog());

          return;
        }
      }

      if (numPartKeys == 2) {
        dirName += "/" + partNames[1][targetPartitions[1]];
      }

      fileSinkDesc fsDesc = new fileSinkDesc(dirName, conf.getTableInfo(),
          false, partId);
      op = (FileSinkOperator) OperatorFactory.get(fsDesc);

      LOG.info("Init a FileSink Operator for partition path " + dirName);
      op.initialize(config, inputObjInspectors);
      fsOps[targetPartitions[0]][targetPartitions[1]] = op;
    }

    op.process(row, tag);

    partition_filesink_success_count
        .set(partition_filesink_success_count.get() + 1);
  }

  private int getPartitionId(int level1, int level2) {
    return level1 << 16 | level2;
  }

  @Override
  public void closeOp(boolean abort) throws HiveException {
    for (FileSinkOperator[] ops : fsOps) {
      for (FileSinkOperator op : ops) {
        if (op != null)
          op.closeOp(abort);
      }
    }

    if (isInsertPart) {
      LOG.info("insert partition error data information " + getLog());
    }
  }

  @Override
  public void jobClose(Configuration hconf, boolean success)
      throws HiveException {
    try {
      if (conf != null) {
        String specTablePath = conf.getDirName();
        numPartKeys = conf.getPartKeys().size();
        /*partNames = new String[numPartKeys][];
        for (int i = 0; i < numPartKeys; i++) {
          partNames[i] = new String[conf.getPartSpaces().get(i).getPartSpace()
              .size()];
          int j = 0;
          for (Entry<String, PartValuesList> entry : conf.getPartSpaces()
              .get(i).getPartSpace().entrySet()) {
            partNames[i][j] = entry.getKey();
            ++j;
          }
        }*/
        
        
        FileSystem fs = (new Path(specTablePath)).getFileSystem(hconf);
        FileStatus [] fsStats = null;
        try{
          fsStats = fs.listStatus(new Path(specTablePath));
        }
        catch(FileNotFoundException fx){
          LOG.info("can not find any partition path in " + specTablePath);
          super.jobClose(hconf, success);
          return;
        }
        
        if(fsStats == null || fsStats.length == 0){
          LOG.info("can not find any partition path in " + specTablePath);
          super.jobClose(hconf, success);
          return;
        }
               
        Path[] listPaths = FileUtil.stat2Paths(fsStats);
        if(listPaths == null || listPaths.length == 0){
          super.jobClose(hconf, success);
          return;
        }
        
        for(Path p:listPaths){           
          String partName = null;
          if(numPartKeys == 2){
            partName = p.getName();
          }
          else if(numPartKeys == 1){
            partName = Utilities.getPartNameFromTempPath(p.getName());
            if(partName == null){
              continue;
            }
          }        

          if (numPartKeys == 2) {
            FileStatus [] subFsStats = fs.listStatus(p);
            
            if(subFsStats == null || subFsStats.length == 0){
              LOG.info("can not find any subpartition path in " + p.getParent() + "/" + p.getName());
              continue;
            }
            Path[] subListPaths = FileUtil.stat2Paths(subFsStats);
            if(subListPaths == null || subListPaths.length == 0){
              continue;
            }
            
            for(Path subp:subListPaths){              
              String subPartName = Utilities.getPartNameFromTempPath(subp.getName());
              if(subPartName == null){
                continue;
              }             
              partitionCloseNew(specTablePath + "/" + partName + "/" + subPartName, subp, success, hconf);
            }
          }
          else{
            partitionCloseNew(specTablePath + "/" + partName, p, success, hconf);
          }
          
        }
        
        /*for (int i = 0; i < partNames[0].length; i++) {
          String specPartPath = specTablePath + "/" + partNames[0][i];
          if (numPartKeys == 2) {
            for (int j = 0; j < partNames[1].length; j++) {
              partitionClose(specPartPath + "/" + partNames[1][j], success,
                  hconf);
            }
          } else {
            partitionClose(specPartPath, success, hconf);
          }
        }*/
      }
    } catch (IOException e) {
      throw new HiveException(e);
    }
    super.jobClose(hconf, success);
  }

  
  private void partitionClose(String specPath, boolean success,
      Configuration hconf) throws IOException, HiveException {
    FileSystem fs = (new Path(specPath)).getFileSystem(hconf);
    Path tmpPath = Utilities.toTempPath(specPath);
    Path intermediatePath = new Path(tmpPath.getParent(), tmpPath.getName()
        + ".intermediate");
    Path finalPath = new Path(specPath);
    if (success) {
    	LOG.info("is tmp file exists: " + tmpPath);
      if (fs.exists(tmpPath)) {
        LOG.info("Moving tmp dir: " + tmpPath + " to intermediate: " + intermediatePath);
        Utilities.rename(fs, tmpPath, intermediatePath);
        Utilities.removeTempOrDuplicateFiles(fs, intermediatePath);
        LOG.info("Moving intermediate dir: " + intermediatePath + " to final: " + finalPath);
        Utilities.renameOrMoveFiles(fs, intermediatePath, finalPath);
        LOG.info("Move intermediate dir: " + intermediatePath + " to final: " + finalPath + " OK");
      }
    } else {
      fs.delete(tmpPath, true);
    }
  }
  
  private void partitionCloseNew(String specPath, Path tmpPath, boolean success,
      Configuration hconf) throws IOException, HiveException {
    FileSystem fs = tmpPath.getFileSystem(hconf);
    //Path tmpPath = Utilities.toTempPath(specPath);
    Path intermediatePath = new Path(tmpPath.getParent(), tmpPath.getName()
        + ".intermediate");
    Path finalPath = new Path(specPath);
    if (success) {
      LOG.info("is tmp file exists: " + tmpPath);
      //if (fs.exists(tmpPath)) {
        LOG.info("Moving tmp dir: " + tmpPath + " to intermediate: " + intermediatePath);
        Utilities.rename(fs, tmpPath, intermediatePath);
        Utilities.removeTempOrDuplicateFiles(fs, intermediatePath);
        LOG.info("Moving intermediate dir: " + intermediatePath + " to final: " + finalPath);
        Utilities.renameOrMoveFiles(fs, intermediatePath, finalPath);
        LOG.info("Move intermediate dir: " + intermediatePath + " to final: " + finalPath + " OK");
      //}
    } else {
      fs.delete(tmpPath, true);
    }
  }

  public String getName() {
    return new String("FS");
  }

  public String getLog() {
    if (partErrorMap == null || partErrorMap.isEmpty()) {
      return null;
    }
    StringBuilder sb = new StringBuilder();

    Set<String> keySet = partErrorMap.keySet();

    sb.append("error lines per map is set :" + errorLimit + ",");
    sb.append("total error lines :" + errorNum + "\n");

    for (String key : keySet) {
      sb.append("error lines for partition " + key + " is "
          + partErrorMap.get(key) + "\n");
    }

    return sb.toString();
  }

}
