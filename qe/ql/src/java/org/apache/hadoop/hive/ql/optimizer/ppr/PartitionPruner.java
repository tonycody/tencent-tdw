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

package org.apache.hadoop.hive.ql.optimizer.ppr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.TablePartition;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.UDFOPAnd;
import org.apache.hadoop.hive.ql.udf.UDFOPEqual;
import org.apache.hadoop.hive.ql.udf.UDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.UDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.UDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.UDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.UDFOPNot;
import org.apache.hadoop.hive.ql.udf.UDFOPOr;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;

public class PartitionPruner implements Transform {

  private static final Log LOG = LogFactory
      .getLog("hive.ql.optimizer.ppr.PartitionPruner");

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    OpWalkerCtx opWalkerCtx = new OpWalkerCtx(pctx.getOpToPartPruner());

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", "(TS%FIL%)|(TS%FIL%FIL%)"),
        OpProcFactory.getFilterProc());

    Dispatcher disp = new DefaultRuleDispatcher(OpProcFactory.getDefaultProc(),
        opRules, opWalkerCtx);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    pctx.setHasNonPartCols(opWalkerCtx.getHasNonPartCols());

    return pctx;
  }

  private static class PartitionPrunerContext {
    ObjectInspector partKeysInpsector;
    int numPartitionLevels;
    List<String> partKeyNames;
    Map<String, ObjectInspectorConverters.Converter> partKeyConverters;
    Map<String, Set<String>> partKeyToTotalPartitions;
    Map<String, org.apache.hadoop.hive.metastore.api.Partition> partitionMap;

    boolean supportPPrForIn = false;
    boolean supportPPrForInRangePartFunc = false;
    boolean useNewRangePartPPr = false;
    boolean addFirstPartAlways = false;
    boolean checkDateFormat = false;
  }

  static public class RangePartInfo {
    public String name;
    public String value;
  }
  
  public static exprNodeDesc changeInExprToOrExpr(exprNodeDesc inExpr) throws HiveException{
    List<exprNodeDesc> childList = inExpr.getChildren();
    List<exprNodeDesc> orExprList = new ArrayList<exprNodeDesc>();
    int size = childList.size();
    exprNodeDesc leftExpr = childList.get(0);
    try{
      for(int index = 1 ; index < size; index++){
        List<exprNodeDesc> tempExprList = new ArrayList<exprNodeDesc>();
        tempExprList.add(leftExpr);
        tempExprList.add(childList.get(index));
        exprNodeDesc orItemExpr = (exprNodeGenericFuncDesc) TypeCheckProcFactory.getDefaultExprProcessor().getFuncExprNodeDesc("=",tempExprList);
        orExprList.add(orItemExpr);
      }      
      exprNodeGenericFuncDesc ret = (exprNodeGenericFuncDesc) TypeCheckProcFactory.getDefaultExprProcessor().getFuncExprNodeDesc("or", orExprList); 
      return ret;
    }
    catch(Exception x){
      throw new HiveException(x);
    }
  }

  public static String findRangPart(
      ObjectInspectorConverters.Converter converter1,
      ObjectInspectorConverters.Converter converter2, String value,
      boolean isIncrease, List<RangePartInfo> rangPartInfoList) {
    int start = 0;
    int end = rangPartInfoList.size() - 1;
    int mid = end / 2;
    String compareValue = null;
    int ret = 0;
    int retPre = 0;
    String compareValuePre = null;
    int len = rangPartInfoList.size();

    if (len == 0) {
      return null;
    }

    if (isIncrease) {
      for (;;) {
        compareValue = rangPartInfoList.get(mid).value;
        ret = ((Comparable) converter1.convert(value))
            .compareTo((Comparable) converter2.convert(compareValue));

        if (ret < 0) {
          if (mid == 0) {
            return rangPartInfoList.get(mid).name;
          } else if (mid - 1 >= 0) {
            compareValuePre = rangPartInfoList.get(mid - 1).value;
            retPre = ((Comparable) converter1.convert(value))
                .compareTo((Comparable) converter2.convert(compareValuePre));

            if (retPre >= 0) {
              return rangPartInfoList.get(mid).name;
            }
          }

          end = mid - 1;
          mid = (start + end) / 2;
          if (start > end || end < 0 || start > len - 1) {
            return null;
          }
        } else if (ret == 0) {
          if (mid + 1 < len) {
            return rangPartInfoList.get(mid + 1).name;
          } else {
            return null;
          }
        } else {
          start = mid + 1;
          mid = (start + end) / 2;
          if (start > end || end < 0 || start > len - 1) {
            return null;
          }
        }
      }
    } else {
      for (;;) {
        compareValue = rangPartInfoList.get(mid).value;
        ret = ((Comparable) converter1.convert(value))
            .compareTo((Comparable) converter2.convert(compareValue));

        if (ret > 0) {
          if (mid == 0) {
            return rangPartInfoList.get(mid).name;
          } else if (mid - 1 >= 0) {
            compareValuePre = rangPartInfoList.get(mid - 1).value;
            retPre = ((Comparable) converter1.convert(value))
                .compareTo((Comparable) converter2.convert(compareValuePre));
            if (retPre <= 0) {
              return rangPartInfoList.get(mid).name;
            }
          }

          end = mid - 1;
          mid = (start + end) / 2;
          if (start > end || end < 0 || start > len - 1) {
            return null;
          }
        } else if (ret == 0) {
          if (mid + 1 < len) {
            return rangPartInfoList.get(mid + 1).name;
          } else {
            return null;
          }
        } else {
          start = mid + 1;
          mid = (start + end) / 2;
          if (start > end || end < 0 || start > len - 1) {
            return null;
          }
        }
      }
    }
  }

  public static boolean isContainRunningComputeFunction(exprNodeDesc funcDesc) {
    if (funcDesc == null) {
      return false;
    }

    if (FunctionRegistry.isFuncRunningCompute(funcDesc)) {
      return true;
    }

    List<exprNodeDesc> children = funcDesc.getChildren();

    if (children == null || children.isEmpty()) {
      return false;
    }

    for (exprNodeDesc child : children) {
      if (isContainRunningComputeFunction(child)) {
        return true;
      }
    }

    return false;
  }

  public static List<exprNodeDesc> evaluatePrunedExpressionForIn(
      exprNodeDesc prunerExpr, PartitionPrunerContext ppc,
      Map<String, Set<String>> partColToTargetPartitions) throws HiveException

  {
    boolean isFunction = prunerExpr instanceof exprNodeGenericFuncDesc;
    if (!isFunction) {
      if (prunerExpr instanceof exprNodeColumnDesc) {
        List<exprNodeDesc> partExprList = new ArrayList<exprNodeDesc>();
        partExprList.add(prunerExpr);
        return partExprList;
      } else {
        return null;
      }
    }

    List<exprNodeDesc> children = prunerExpr.getChildren();
    List<exprNodeDesc> returnPartDescs = new ArrayList<exprNodeDesc>();

    for (exprNodeDesc child : children) {
      Map<String, Set<String>> partMap = new HashMap<String, Set<String>>();
      List<exprNodeDesc> results = evaluatePrunedExpressionForIn(child, ppc,
          partMap);

      if (results != null) {
        addAndDeduplicate(returnPartDescs, results);
      }
    }
    return returnPartDescs;
  }

  public static List<String> generateRange(String start, String end)
      throws HiveException {
    List<String> ret = new ArrayList<String>();

    try {
      long startNum = Long.valueOf(start);
      long endNum = Long.valueOf(end);

      for (long i = startNum; i < endNum; i++) {
        ret.add(String.valueOf(i));
      }
    } catch (Exception x) {
      throw new HiveException(x);
    }

    return ret;
  }

  public List<Long> generateRange(long start, long end) {
    List<Long> ret = new ArrayList<Long>();
    for (long i = start; i < end; i++) {
      ret.add(i);
    }

    return ret;
  }

  private static List<exprNodeDesc> evaluatePrunedExpression(
      exprNodeDesc prunerExpr, PartitionPrunerContext ppc,
      Map<String, Set<String>> partColToTargetPartitions) throws HiveException {
    boolean isFunction = prunerExpr instanceof exprNodeGenericFuncDesc;
    if (!isFunction) {
      if (prunerExpr instanceof exprNodeColumnDesc) {
        List<exprNodeDesc> partExprList = new ArrayList<exprNodeDesc>();
        partExprList.add(prunerExpr);
        return partExprList;
      } else {
        return null;
      }
    }

    List<exprNodeDesc> children = prunerExpr.getChildren();
    List<Map<String, Set<String>>> childrenPartitions = new ArrayList<Map<String, Set<String>>>();
    List<List<exprNodeDesc>> partDescsFromChildren = new ArrayList<List<exprNodeDesc>>();
    List<exprNodeDesc> returnPartDescs = new ArrayList<exprNodeDesc>();

    for (exprNodeDesc child : children) {
      Map<String, Set<String>> partMap = new HashMap<String, Set<String>>();
      List<exprNodeDesc> results = evaluatePrunedExpression(child, ppc, partMap);
      partDescsFromChildren.add(results);
      if (results != null) {
        addAndDeduplicate(returnPartDescs, results);
      }
      childrenPartitions.add(partMap);
    }

    int numPartExprs = returnPartDescs.size();
    
    if (ppc.supportPPrForIn && FunctionRegistry.isOpIn(prunerExpr)) {
      // get left expression for "in", it maybe a parition column or a function
      boolean isAFunction = false;
      List<exprNodeDesc> inChild = prunerExpr.getChildren();
      exprNodeDesc leftNode = inChild.get(0);
      int childSize = inChild.size();
      
      Map<String, Set<String>> partMap = new HashMap<String, Set<String>>();
      List<exprNodeDesc> results = evaluatePrunedExpressionForIn(leftNode, ppc, partMap);

      partDescsFromChildren.add(results);

      if (results != null) {
        addAndDeduplicate(returnPartDescs, results);
      }

      if(returnPartDescs == null || returnPartDescs.isEmpty()){
        return null;
      }
      exprNodeColumnDesc partColDesc = (exprNodeColumnDesc) returnPartDescs
          .get(0);
      String partColName = partColDesc.getColumn();
      ObjectInspectorConverters.Converter converter = ppc.partKeyConverters
          .get(partColName);
      org.apache.hadoop.hive.metastore.api.Partition part = ppc.partitionMap
          .get(partColName);
      boolean isRangePartition = part.getParType().equalsIgnoreCase("RANGE");
      boolean isHashPartition = part.getParType().equalsIgnoreCase("HASH");
      Set<String> tempPartitions = null;

      if(isRangePartition){
        tempPartitions = evaluateRangePartitionForIn(ppc, prunerExpr,
            ppc.partKeysInpsector, ppc.partKeyConverters.get(partColName),
            part.getParSpaces(), part.getLevel(), ppc.numPartitionLevels);
        
        if (tempPartitions.isEmpty()
            && part.getParSpaces().containsKey("default")) {
          tempPartitions.add("default");
        }
      }
      else if(isHashPartition){
        tempPartitions = evaluateHashPartition(prunerExpr, ppc.partKeysInpsector,
            ppc.partKeyConverters.get(partColName), part.getParSpaces(),
            part.getLevel(), ppc.numPartitionLevels);
      }
      else{
        tempPartitions = evaluateListPartitionForIn(prunerExpr,
            ppc.partKeysInpsector, ppc.partKeyConverters.get(partColName),
            part.getParSpaces(), part.getLevel(), ppc.numPartitionLevels);
        
        if (tempPartitions.isEmpty()
            && part.getParSpaces().containsKey("default")) {
          tempPartitions.add("default");
        }
      }
           
      Set<String> targetPartitions = partColToTargetPartitions.get(partColDesc
          .getColumn());
      if (targetPartitions == null) {
        targetPartitions = tempPartitions;
        partColToTargetPartitions
            .put(partColDesc.getColumn(), targetPartitions);
      } else {
        targetPartitions.addAll(tempPartitions);
      }     
    }
    
    if (FunctionRegistry.isOpOr(prunerExpr)) {
      for (String partKeyName : ppc.partKeyNames) {
        Set<String> leftParts = childrenPartitions.get(0).get(partKeyName);
        Set<String> rightParts = childrenPartitions.get(1).get(partKeyName);
        if (leftParts != null && rightParts == null) {
          rightParts = ppc.partKeyToTotalPartitions.get(partKeyName);
        } else if (rightParts != null && leftParts == null) {
          leftParts = rightParts;
          rightParts = ppc.partKeyToTotalPartitions.get(partKeyName);
        } else if (rightParts == null && leftParts == null) {
          continue;
        }
        leftParts.addAll(rightParts);
        partColToTargetPartitions.put(partKeyName, leftParts);
      }
    } else if (FunctionRegistry.isOpAnd(prunerExpr)) {
      for (Map<String, Set<String>> childPartMap : childrenPartitions) {
        for (Entry<String, Set<String>> entry : childPartMap.entrySet()) {
          Set<String> partitions = partColToTargetPartitions
              .get(entry.getKey());
          if (partitions == null) {
            partitions = entry.getValue();
            partColToTargetPartitions.put(entry.getKey(), partitions);
          } else
            partitions.retainAll(entry.getValue());
        }
      }
    } else if (FunctionRegistry.isOpNot(prunerExpr)) {
      assert (childrenPartitions.size() < 2);
      if (childrenPartitions.size() == 1) {
        Map<String, Set<String>> partMap = childrenPartitions.get(0);
        for (Entry<String, Set<String>> entry : partMap.entrySet()) {
          Set<String> targetPartitions = new TreeSet<String>();
          Set<String> partitions = entry.getValue();
          Set<String> totalPartitions = ppc.partKeyToTotalPartitions.get(entry
              .getKey());
          for (String i : totalPartitions) {
            if (!partitions.contains(i))
              targetPartitions.add(i);
          }

          partColToTargetPartitions.put(entry.getKey(), targetPartitions);
        }
      }
    }

    boolean isEqualUDF = FunctionRegistry.isOpEqual(prunerExpr);
    if ((isEqualUDF || FunctionRegistry.isOpEqualOrGreaterThan(prunerExpr)
        || FunctionRegistry.isOpEqualOrLessThan(prunerExpr)
        || FunctionRegistry.isOpGreaterThan(prunerExpr) || FunctionRegistry
        .isOpLessThan(prunerExpr)) && numPartExprs == 1) {
      assert (partDescsFromChildren.size() == 2);
      exprNodeGenericFuncDesc funcDesc = (exprNodeGenericFuncDesc) prunerExpr;

      if (partDescsFromChildren.get(0) == null
          || partDescsFromChildren.get(0).size() == 0) {
        exprNodeDesc temp = funcDesc.getChildExprs().get(0);
        funcDesc.getChildExprs().set(0, funcDesc.getChildExprs().get(1));
        funcDesc.getChildExprs().set(1, temp);

        String newMethodName = null;
        if (FunctionRegistry.isOpEqualOrGreaterThan(prunerExpr)) {
          newMethodName = "<=";
        } else if (FunctionRegistry.isOpEqualOrLessThan(prunerExpr)) {
          newMethodName = ">=";
        } else if (FunctionRegistry.isOpGreaterThan(prunerExpr)) {
          newMethodName = "<";
        } else if (FunctionRegistry.isOpLessThan(prunerExpr)) {
          newMethodName = ">";
        }

        if (newMethodName != null) {
          ArrayList<TypeInfo> argumentTypeInfos = new ArrayList<TypeInfo>(
              funcDesc.getChildExprs().size());
          for (int i = 0; i < children.size(); i++) {
            exprNodeDesc child = funcDesc.getChildExprs().get(i);
            argumentTypeInfos.add(child.getTypeInfo());
          }
          funcDesc.setGenericUDF(FunctionRegistry
              .getFunctionInfo(newMethodName).getGenericUDF());
        }
      }

      boolean isContainFunc = isContainFunc(funcDesc);
      LOG.info("XXisContainFunc=" + isContainFunc);
      LOG.info("XXfuncDesc=" + funcDesc.getExprString());
      
      exprNodeColumnDesc partColDesc = (exprNodeColumnDesc) returnPartDescs
          .get(0);
      String partColName = partColDesc.getColumn();
      org.apache.hadoop.hive.metastore.api.Partition part = ppc.partitionMap
          .get(partColName);
      boolean isRangePartition = part.getParType().equalsIgnoreCase("RANGE");
      boolean isHashPartition = part.getParType().equalsIgnoreCase("HASH");

      boolean exprRewritten = false;
      boolean containEqual = false;
      Set<String> tempPartitions;
      if (isRangePartition) {
        if (ppc.useNewRangePartPPr && isContainFunc && (part.getParKey().getType().equalsIgnoreCase("bigint")
            || part.getParKey().getType().equalsIgnoreCase("string")
            || part.getParKey().getType().equalsIgnoreCase("int")
            || part.getParKey().getType().equalsIgnoreCase("smallint")
            || part.getParKey().getType().equalsIgnoreCase("tinyint"))) {

          tempPartitions = evaluateNewRangePartition(funcDesc,
              ppc.partKeysInpsector, ppc.partKeyConverters.get(partColName),
              part.getParSpaces(), part.getLevel(), ppc.numPartitionLevels, ppc.addFirstPartAlways,
              ppc.checkDateFormat);

        } else {
          exprNodeGenericFuncDesc boundaryCheckerDesc = null;
          if (FunctionRegistry.isOpEqualOrGreaterThan(prunerExpr)
              || FunctionRegistry.isOpGreaterThan(prunerExpr)) {
            boundaryCheckerDesc = (exprNodeGenericFuncDesc) TypeCheckProcFactory
                .getDefaultExprProcessor().getFuncExprNodeDesc("=",
                    funcDesc.getChildExprs());

            List<exprNodeDesc> argDescs = new ArrayList<exprNodeDesc>();
            argDescs.add(funcDesc);
            funcDesc = (exprNodeGenericFuncDesc) TypeCheckProcFactory
                .getDefaultExprProcessor().getFuncExprNodeDesc("not", argDescs);
            exprRewritten = true;
            if (FunctionRegistry.isOpGreaterThan(prunerExpr))
              containEqual = true;
          } else if (FunctionRegistry.isOpEqual(prunerExpr)) {
            funcDesc = (exprNodeGenericFuncDesc) TypeCheckProcFactory
                .getDefaultExprProcessor().getFuncExprNodeDesc("<=",
                    funcDesc.getChildExprs());
          }

          tempPartitions = evaluateRangePartition(funcDesc,
              boundaryCheckerDesc, ppc.partKeysInpsector,
              ppc.partKeyConverters.get(partColName), part.getParSpaces(),
              part.getLevel(), ppc.numPartitionLevels, isEqualUDF,
              exprRewritten, containEqual);
        }

        if (tempPartitions.isEmpty()
            && part.getParSpaces().containsKey("default")) {
          tempPartitions.add("default");
        }
      } else if (isHashPartition) {

        tempPartitions = evaluateHashPartition(funcDesc, ppc.partKeysInpsector,
            ppc.partKeyConverters.get(partColName), part.getParSpaces(),
            part.getLevel(), ppc.numPartitionLevels);
      } else {
        tempPartitions = evaluateListPartition(funcDesc, ppc.partKeysInpsector,
            ppc.partKeyConverters.get(partColName), part.getParSpaces(),
            part.getLevel(), ppc.numPartitionLevels);

        if (tempPartitions.isEmpty()
            && part.getParSpaces().containsKey("default")) {
          tempPartitions.add("default");
        }
      }

      Set<String> targetPartitions = partColToTargetPartitions.get(partColDesc
          .getColumn());
      if (targetPartitions == null) {
        targetPartitions = tempPartitions;
        partColToTargetPartitions
            .put(partColDesc.getColumn(), targetPartitions);
      } else {
        targetPartitions.addAll(tempPartitions);
      }

    }

    return returnPartDescs;
  }
  
  
  public static boolean isContainFunc(exprNodeGenericFuncDesc funcDesc){
    List<exprNodeDesc>  childs = funcDesc.getChildExprs();
    int childSize = childs.size();
    boolean containFunc = false;
    boolean containCol = false;
    for(int index = 0; index < childSize; index++){
      containFunc = checkContainFunc(childs.get(index));
      containCol = checkContainCol(childs.get(index));
      if(containFunc && containCol){
        return true;
      }
    }
    return false;
  }
  
  public static boolean checkContainFunc(exprNodeDesc expr){
    if(expr == null){
      return false;
    }
    LOG.info("XXXXXcheckContainFunc=" + expr.getExprString());
    if(expr instanceof exprNodeGenericFuncDesc){
      return true;
    }
    else{
      List<exprNodeDesc> childs = expr.getChildren();
      boolean ret = false;
      if(childs != null && !childs.isEmpty()){
        for(exprNodeDesc desc:childs){
          ret = checkContainFunc(desc);
          if(ret){
            return ret;
          }
        }        
      }
      return false;
    }
  }
  
  public static boolean checkContainCol(exprNodeDesc expr){
    if(expr == null){
      return false;
    }
    
    LOG.info("XXXXXcheckContainCol=" + expr.getExprString());
    
    if(expr instanceof exprNodeColumnDesc){
      return true;
    }
    else{
      List<exprNodeDesc> childs = expr.getChildren();
      boolean ret = false;
      if(childs != null && !childs.isEmpty()){
        for(exprNodeDesc desc:childs){
          ret = checkContainCol(desc);
          if(ret){
            return ret;
          }
        }        
      }
      return false;
    }
  }

  private static void addAndDeduplicate(List<exprNodeDesc> target,
      List<exprNodeDesc> source) {
    if (target.size() == 0) {
      target.addAll(source);
      return;
    }
    for (exprNodeDesc srcDesc : source) {
      boolean added = false;
      for (exprNodeDesc targetDesc : source) {
        if (srcDesc.isSame(targetDesc)) {
          added = true;
          break;
        }
      }
      if (added)
        target.add(srcDesc);
    }
  }

  private static Set<String> evaluateRangePartition(exprNodeDesc desc,
      exprNodeDesc boundaryCheckerDesc, ObjectInspector partKeysInspector,
      ObjectInspectorConverters.Converter converter,
      Map<String, List<String>> partSpace, int level, int numPartitionLevel,
      boolean isEqualFunc, boolean exprRewritten, boolean containsEqual)
      throws HiveException {
    ArrayList<Object> partObjects = new ArrayList<Object>(numPartitionLevel);
    for (int i = 0; i < numPartitionLevel; i++) {
      partObjects.add(null);
    }
    
    ExprNodeEvaluator evaluator = ExprNodeEvaluatorFactory.get(desc);
    ObjectInspector evaluateResultOI = evaluator.initialize(partKeysInspector);

    Set<String> targetPartitions = new LinkedHashSet<String>();

    for (Entry<String, List<String>> entry : partSpace.entrySet()) {
      List<String> partValues = entry.getValue();

      if (partValues == null || partValues.size() == 0) {
        assert (entry.getKey().equalsIgnoreCase("default"));
        targetPartitions.add(entry.getKey());
        break;
      }

      Object pv = converter.convert(partValues.get(0));
      partObjects.set(level, pv);
      Object evaluateResultO = evaluator.evaluate(partObjects);
      Boolean r = (Boolean) ((PrimitiveObjectInspector) evaluateResultOI)
          .getPrimitiveJavaObject(evaluateResultO);
      if (r == null) {
        targetPartitions.clear();
        break;
      } else {
        if (Boolean.TRUE.equals(r)) {
          if (!isEqualFunc)
            targetPartitions.add(entry.getKey());
        } else if (Boolean.FALSE.equals(r)) {
          if (boundaryCheckerDesc != null) {
            ExprNodeEvaluator boundaryChecker = ExprNodeEvaluatorFactory
                .get(boundaryCheckerDesc);
            ObjectInspector boundaryCheckerOI = boundaryChecker
                .initialize(partKeysInspector);
            Boolean isBoundary = (Boolean) ((PrimitiveObjectInspector) boundaryCheckerOI)
                .getPrimitiveJavaObject(boundaryChecker.evaluate(partObjects));
            if (isBoundary == null) {
              targetPartitions.clear();
              break;
            } else {
              if (Boolean.FALSE.equals(isBoundary)) {
                break;
              }
            }
          }
          if (!(exprRewritten && containsEqual)) {
            targetPartitions.add(entry.getKey());
          }
          break;
        }
      }
    }

    if (exprRewritten) {
      Set<String> oldPartitions = targetPartitions;
      targetPartitions = new TreeSet<String>();
      targetPartitions.addAll(partSpace.keySet());
      Iterator<String> iter = targetPartitions.iterator();
      while (iter.hasNext()) {
        if (oldPartitions.contains(iter.next()))
          iter.remove();
      }
    }

    targetPartitions.remove("default");
    return targetPartitions;
  }

  private static Set<String> evaluateListPartition(exprNodeDesc desc,
      ObjectInspector partKeysInpsector,
      ObjectInspectorConverters.Converter converter,
      Map<String, List<String>> partSpace, int level, int numPartitionLevels)
      throws HiveException {
    ExprNodeEvaluator evaluator = ExprNodeEvaluatorFactory.get(desc);
    ObjectInspector evaluateResultOI = evaluator.initialize(partKeysInpsector);

    Set<String> targetPartitions = new TreeSet<String>();

    ArrayList<Object> partObjects = new ArrayList<Object>(numPartitionLevels);
    for (int i = 0; i < numPartitionLevels; i++) {
      partObjects.add(null);
    }
    for (Entry<String, List<String>> entry : partSpace.entrySet()) {
      List<String> partValues = entry.getValue();

      for (String partVal : partValues) {
        Object pv = converter.convert(partVal);
        partObjects.set(level, pv);
        Object evaluateResultO = evaluator.evaluate(partObjects);
        Boolean r = (Boolean) ((PrimitiveObjectInspector) evaluateResultOI)
            .getPrimitiveJavaObject(evaluateResultO);
        if (r == null) {
          return new TreeSet<String>();
        } else {
          if (Boolean.TRUE.equals(r)) {
            targetPartitions.add(entry.getKey());
          }
        }
      }
    }

    return targetPartitions;
  }

  private static Set<String> evaluateNewRangePartition(exprNodeDesc desc,
      ObjectInspector partKeysInpsector,
      ObjectInspectorConverters.Converter converter,
      Map<String, List<String>> partSpace, int level, 
      int numPartitionLevels, boolean addFirst, boolean checkDateStr)
      throws HiveException {

    int startPos = -1;
    int endPos = -1;
    List<Integer> indexList = new ArrayList<Integer>();
    int index = 0;

    ExprNodeEvaluator evaluator = ExprNodeEvaluatorFactory.get(desc);
    ObjectInspector evaluateResultOI = evaluator.initialize(partKeysInpsector);

    Set<String> targetPartitions = new TreeSet<String>();

    ArrayList<Object> partObjects = new ArrayList<Object>(numPartitionLevels);
    for (int i = 0; i < numPartitionLevels; i++) {
      partObjects.add(null);
    }

    String startValue = null;
    String endValue = null;
    String startKey = null;
    boolean containDefaultPart = false;
    for (Entry<String, List<String>> entry : partSpace.entrySet()) {
      List<String> partValues = entry.getValue();

      if (partValues == null || partValues.isEmpty()) {
        if(entry.getKey().equalsIgnoreCase("default")){
          containDefaultPart = true;
        }
        continue;
      }

      endValue = partValues.get(0);
      List<String> rangeValueList = null;
      if (startValue == null) {
        startValue = endValue;
        index++;
        startKey = entry.getKey();
        continue;
      } else {
        rangeValueList = generateRange(startValue, endValue);
      }

      for (String partVal : rangeValueList) {
        if(checkDateStr){
          boolean isValidDateStr = checkPartValDateFormat(partVal);
          if(!isValidDateStr){
            continue;
          }
        }
        Object pv = converter.convert(partVal);
        partObjects.set(level, pv);
        Object evaluateResultO = evaluator.evaluate(partObjects);
        Boolean r = (Boolean) ((PrimitiveObjectInspector) evaluateResultOI)
            .getPrimitiveJavaObject(evaluateResultO);
        if (r == null) {
          Set<String> ret = new TreeSet<String>();
          if(addFirst){
            ret.add(startKey);
          }
          return ret;
        } else {
          if (Boolean.TRUE.equals(r)) {
            targetPartitions.add(entry.getKey());
            indexList.add(index);
            if (startPos == -1) {
              startPos = index;
            }

            endPos = index;
            break;
          }
        }
      }

      startValue = endValue;
      index++;
    }

    if (startPos == 1 || startPos == -1) {
      targetPartitions.add(startKey);
      if(startPos == -1 && containDefaultPart){
        targetPartitions.add("default");
      }
    }

    if (!checkContinues((ArrayList<Integer>) indexList)) {
      targetPartitions.add(startKey);
    }

    if(addFirst){
      targetPartitions.add(startKey);
    }
    return targetPartitions;
  }
  
  private static boolean checkDateDay(int year, int month, int day){
    switch(month){
      case 1:
      case 3:
      case 5:
      case 7:
      case 8:
      case 10:
      case 12:
        if(day >= 1 && day <= 31){
          return true;
        }
        else{
          return false;
        }
      
      case 4:
      case 6:
      case 9:
      case 11:
        if(day >= 1 && day <= 30){
          return true;
        }
        else{
          return false;
        }
        
      case 2:{
        boolean isLeapyear = ((year % 4 == 0) && (year % 100 != 0)) || ((year % 100 == 0) && (year % 400 == 0));
        if(isLeapyear && day >=1 && day <= 29){
          return true;
        }
        else if(!isLeapyear && day >=1 && day <= 28){
          return true;
        }
        else{
          return false;
        }    
      }  
    }
    
    return false;
  }
  
  private static boolean checkPartValDateFormat(String partVal) {
    // TODO Auto-generated method stub
    if(partVal == null || partVal.isEmpty()){
      return false;
    }
    int partValLen = partVal.length();
    switch(partValLen){
    case 4:{
      int year= Integer.valueOf(partVal);
      if(year > 0){
        return true;
      }
      else{
        return false;
      }
    }

    case 6:{
      int year= Integer.valueOf(partVal.substring(0,4));
      int month = Integer.valueOf(partVal.substring(4,6));
      if(year > 0 && month > 0 && month <= 12){
        return true;
      }
      else{
        return false;
      }
    }

    case 8:{
      int year= Integer.valueOf(partVal.substring(0,4));
      int month = Integer.valueOf(partVal.substring(4,6));
      int day = Integer.valueOf(partVal.substring(6,8));
      if(year > 0 && month > 0 && month <= 12){
        return checkDateDay(year, month, day);
      }
      else{
        return false;
      }
    }
    
    case 10:{
      int year= Integer.valueOf(partVal.substring(0,4));
      int month = Integer.valueOf(partVal.substring(4,6));
      int day = Integer.valueOf(partVal.substring(6,8));
      int hour = Integer.valueOf(partVal.substring(8,10));
      if(year > 0 && month > 0 && month <= 12 && hour >= 0 && hour <= 23){
        return checkDateDay(year, month, day);
      }
      else{
        return false;
      }
    }

    case 12:{
      int year= Integer.valueOf(partVal.substring(0,4));
      int month = Integer.valueOf(partVal.substring(4,6));
      int day = Integer.valueOf(partVal.substring(6,8));
      int hour = Integer.valueOf(partVal.substring(8,10));
      int min = Integer.valueOf(partVal.substring(10,12));
      if(year > 0 && month > 0 && month <= 12 && hour >= 0 && hour <= 23 && min >= 0 && min <= 59){
        return checkDateDay(year, month, day);
      }
      else{
        return false;
      }
    }
      
    default:
      return false;
    }
  }

  private static Set<String> evaluateListPartitionForIn(exprNodeDesc desc,
      ObjectInspector partKeysInpsector,
      ObjectInspectorConverters.Converter converter,
      Map<String, List<String>> partSpace, int level, int numPartitionLevels)
      throws HiveException {
    
    List<exprNodeDesc> childExprs = desc.getChildren();
    exprNodeDesc leftExpr = childExprs.get(0);
    int size = childExprs.size();
    Set<String> targetPartitions = new TreeSet<String>();
    
    for(int index = 1; index < size; index++){
      List<exprNodeDesc> exprList = new ArrayList<exprNodeDesc>();
      exprList.add(leftExpr);
      exprList.add(childExprs.get(index));
      exprNodeDesc equalExpr = (exprNodeGenericFuncDesc) TypeCheckProcFactory
                .getDefaultExprProcessor().getFuncExprNodeDesc("=",exprList);
      
      LOG.info("XXXXXXXXXXXXXXXXXXXXXXXXXequalExpr=" + equalExpr.getExprString());
      
      ExprNodeEvaluator evaluator = ExprNodeEvaluatorFactory.get(equalExpr);
      ObjectInspector evaluateResultOI = evaluator.initialize(partKeysInpsector);

      ArrayList<Object> partObjects = new ArrayList<Object>(numPartitionLevels);
      for (int i = 0; i < numPartitionLevels; i++) {
        partObjects.add(null);
      }
      for (Entry<String, List<String>> entry : partSpace.entrySet()) {
        List<String> partValues = entry.getValue();

        for (String partVal : partValues) {
          Object pv = converter.convert(partVal);
          partObjects.set(level, pv);
          Object evaluateResultO = evaluator.evaluate(partObjects);
          Boolean r = (Boolean) ((PrimitiveObjectInspector) evaluateResultOI)
              .getPrimitiveJavaObject(evaluateResultO);
          if (r == null) {
            return new TreeSet<String>();
          } else {
            if (Boolean.TRUE.equals(r)) {
              targetPartitions.add(entry.getKey());
            }
          }
        }
      }
    }
    
    return targetPartitions;
  }
  
  private static Set<String> evaluateRangePartitionForIn(PartitionPrunerContext ppc, 
      exprNodeDesc desc,
      ObjectInspector partKeysInpsector,
      ObjectInspectorConverters.Converter converter,
      Map<String, List<String>> partSpace, int level, int numPartitionLevels)
      throws HiveException {
    
    List<exprNodeDesc> childExprs = desc.getChildren();
    exprNodeDesc leftExpr = childExprs.get(0);
    int size = childExprs.size();
    Set<String> targetPartitions = new TreeSet<String>();
    boolean isContainFunc = leftExpr instanceof exprNodeGenericFuncDesc;
    
    for(int index = 1; index < size; index++){
      List<exprNodeDesc> exprList = new ArrayList<exprNodeDesc>();
      exprList.add(leftExpr);
      exprList.add(childExprs.get(index));
      Set<String> retParts = null;
      
      if(isContainFunc && ppc.useNewRangePartPPr){
        exprNodeDesc equalExpr = (exprNodeGenericFuncDesc) TypeCheckProcFactory
              .getDefaultExprProcessor().getFuncExprNodeDesc("=",exprList);
        
        LOG.info("XXXNEW equalExpr=" + equalExpr.getExprString());
        
        retParts = evaluateNewRangePartition(equalExpr, partKeysInpsector, converter, 
            partSpace, level, numPartitionLevels, ppc.addFirstPartAlways, ppc.checkDateFormat);      
      }
      else{
        exprNodeDesc equalExpr = (exprNodeGenericFuncDesc) TypeCheckProcFactory
              .getDefaultExprProcessor().getFuncExprNodeDesc("<=",exprList);

        LOG.info("XXXequalExpr=" + equalExpr.getExprString());
              
        retParts = evaluateRangePartition(equalExpr, null, partKeysInpsector, converter, 
          partSpace, level, numPartitionLevels, true, false, true);
      }
      
      targetPartitions.addAll(retParts);
    }
    
    return targetPartitions;
  }

  static boolean checkContinues(ArrayList<Integer> indexList) {
    boolean isContinues = true;
    int len = indexList.size();
    int lastValue = -1;
    int value = -1;

    for (int i = 0; i < len; i++) {
      if (i == 0) {
        continue;
      } else {
        lastValue = indexList.get(i - 1);
        value = indexList.get(i);
        if (value > lastValue + 1) {
          isContinues = false;
          break;
        }
      }
    }

    return isContinues;
  }

  private static Set<String> evaluateHashPartition(exprNodeDesc desc,
      ObjectInspector partKeysInpsector,
      ObjectInspectorConverters.Converter converter,
      Map<String, List<String>> partSpace, int level, int numPartitionLevels)
      throws HiveException {

    Set<String> targetPartitions = new TreeSet<String>();
    for (Entry<String, List<String>> entry : partSpace.entrySet()) {
      targetPartitions.add(entry.getKey());
    }

    return targetPartitions;
  }

  public static PrunedPartitionList prune(TablePartition tab,
      exprNodeDesc prunerExpr, HiveConf conf, String alias)
      throws HiveException {
    LOG.trace("Started pruning partiton");
    LOG.trace("tabname = " + tab.getName());
    LOG.trace("prune Expression = " + prunerExpr);
    
    boolean supportPPrForIn = false;
    boolean supportPPrForInRangePartFunc = false;
    boolean useNewRangePartPPr = false;
    boolean addFirstPartAlways = false;
    boolean checkDateFormat = false;
    if(conf != null)
    {
      //supportPPrForIn = conf.getBoolean("hive.optimize.ppr.in", false);
      supportPPrForIn = conf.getBoolean(HiveConf.ConfVars.HIVE_OPT_PPR_IN.varname, 
          HiveConf.ConfVars.HIVE_OPT_PPR_IN.defaultBoolVal);
      //supportPPrForInRangePartFunc = conf.getBoolean("hive.optimize.ppr.rangepart.func", false);
      supportPPrForInRangePartFunc = conf.getBoolean(HiveConf.ConfVars.HIVE_OPT_PPR_IN_RANGEPART_FUNC_SUPPORT.varname,
          HiveConf.ConfVars.HIVE_OPT_PPR_IN_RANGEPART_FUNC_SUPPORT.defaultBoolVal);
      useNewRangePartPPr = conf.getBoolean(HiveConf.ConfVars.HIVE_OPT_PPR_RANGEPART_NEW.varname,
          HiveConf.ConfVars.HIVE_OPT_PPR_RANGEPART_NEW.defaultBoolVal);
      addFirstPartAlways = conf.getBoolean(HiveConf.ConfVars.HIVE_OPT_PPR_RANGEPART_NEW_ADDFIRSTPART_ALWAYS.varname,
          HiveConf.ConfVars.HIVE_OPT_PPR_RANGEPART_NEW_ADDFIRSTPART_ALWAYS.defaultBoolVal);
      checkDateFormat = conf.getBoolean(HiveConf.ConfVars.HIVE_OPT_PPR_RANGEPART_NEW_CHECK_DATEFORMAT.varname,
          HiveConf.ConfVars.HIVE_OPT_PPR_RANGEPART_NEW_CHECK_DATEFORMAT.defaultBoolVal);
    }
    

    Set<String> targetPartitionPaths = new TreeSet<String>();

    if (tab.isPartitioned()) {
      PartitionPrunerContext ppc = new PartitionPrunerContext();
      
      ppc.supportPPrForIn = supportPPrForIn;
      ppc.supportPPrForInRangePartFunc = supportPPrForInRangePartFunc;
      ppc.useNewRangePartPPr = useNewRangePartPPr;
      ppc.addFirstPartAlways = addFirstPartAlways;
      ppc.checkDateFormat = checkDateFormat;
    

      ppc.partKeyConverters = new HashMap<String, ObjectInspectorConverters.Converter>();
      ppc.partKeyToTotalPartitions = new HashMap<String, Set<String>>();
      ppc.partitionMap = new HashMap<String, org.apache.hadoop.hive.metastore.api.Partition>();
      org.apache.hadoop.hive.metastore.api.Partition partition = tab
          .getTTable().getPriPartition();
      ppc.partitionMap.put(partition.getParKey().getName(), partition);
      partition = tab.getTTable().getSubPartition();
      if (partition != null) {
        ppc.partitionMap.put(partition.getParKey().getName(), partition);
      }
      ppc.numPartitionLevels = ppc.partitionMap.size();

      ppc.partKeyNames = new ArrayList<String>();
      ArrayList<ObjectInspector> partObjectInspectors = new ArrayList<ObjectInspector>();
      for (int i = 0; i < ppc.numPartitionLevels; i++) {
        ppc.partKeyNames.add(null);
        partObjectInspectors.add(null);
      }
      for (org.apache.hadoop.hive.metastore.api.Partition part : ppc.partitionMap
          .values()) {
        ppc.partKeyNames.set(part.getLevel(), part.getParKey().getName());
        ObjectInspector partOI = PrimitiveObjectInspectorFactory
            .getPrimitiveWritableObjectInspector(((PrimitiveTypeInfo) TypeInfoFactory
                .getPrimitiveTypeInfo(part.getParKey().getType()))
                .getPrimitiveCategory());
        partObjectInspectors.set(part.getLevel(), partOI);

        ObjectInspector partStringOI = PrimitiveObjectInspectorFactory
            .getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING);
        ppc.partKeyConverters.put(part.getParKey().getName(),
            ObjectInspectorConverters.getConverter(partStringOI, partOI));
        ppc.partKeyToTotalPartitions.put(part.getParKey().getName(), part
            .getParSpaces().keySet());
      }
      ppc.partKeysInpsector = ObjectInspectorFactory
          .getStandardStructObjectInspector(ppc.partKeyNames,
              partObjectInspectors);

      Map<String, Set<String>> partColToPartitionNames = new HashMap<String, Set<String>>();
      evaluatePrunedExpression(prunerExpr, ppc, partColToPartitionNames);
      String priPartColName = ppc.partKeyNames.get(0);
      Set<String> priPartNames = partColToPartitionNames.get(priPartColName);
      if (priPartNames == null)
        priPartNames = ppc.partKeyToTotalPartitions.get(priPartColName);
      String subPartColName = null;
      Set<String> subPartNames = null;
      if (ppc.partKeyNames.size() > 1) {
        subPartColName = ppc.partKeyNames.get(1);
        subPartNames = partColToPartitionNames.get(subPartColName);
        if (subPartNames == null)
          subPartNames = ppc.partKeyToTotalPartitions.get(subPartColName);
      }

      for (String priPartName : priPartNames) {
        if (subPartNames != null) {
          for (String subPartName : subPartNames) {
            targetPartitionPaths.add(new Path(tab.getPath(), priPartName + "/"
                + subPartName).toString());
          }
        } else {
          targetPartitionPaths.add(new Path(tab.getPath(), priPartName)
              .toString());
        }
      }
    } else {
      targetPartitionPaths.add(tab.getPath().toString());
    }

    return new PrunedPartitionList(targetPartitionPaths);

  }

  public static boolean hasColumnExpr(exprNodeDesc desc) {
    if (desc == null) {
      return false;
    }
    if (desc instanceof exprNodeColumnDesc) {
      return true;
    }
    List<exprNodeDesc> children = desc.getChildren();
    if (children != null) {
      for (int i = 0; i < children.size(); i++) {
        if (hasColumnExpr(children.get(i))) {
          return true;
        }
      }
    }
    return false;
  }

}
