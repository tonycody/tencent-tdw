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

package org.apache.hadoop.hive.ql.parse;

import java.util.*;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.TablePartition;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeFieldDesc;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.plan.exprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeNullDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFOPAnd;
import org.apache.hadoop.hive.ql.udf.UDFOPNot;
import org.apache.hadoop.hive.ql.udf.UDFOPOr;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ASTPartitionPruner {

  @SuppressWarnings("nls")
  private static final Log LOG = LogFactory
      .getLog("hive.ql.parse.PartitionPruner");

  private String tableAlias;

  private QBMetaData metaData;

  private TablePartition tab;

  private exprNodeDesc prunerExpr;

  private HiveConf conf;
  private QB qb;

  private boolean onlyContainsPartCols;

  private boolean ambiguousTableOrColunm = false;
  
  // true if condition 'in' is about subQAlias column
  private boolean isSubQAlias = false;

  public ASTPartitionPruner() {
  }

  public ASTPartitionPruner(String tableAlias, QB qb/* QBMetaData metaData */,
      HiveConf conf) {
    this.qb = qb;
    this.tableAlias = tableAlias;
    this.metaData = qb.getMetaData();
    this.tab = metaData.getTableForAlias(tableAlias);
    this.prunerExpr = null;
    this.conf = conf;
    onlyContainsPartCols = true;
  }

  public boolean onlyContainsPartitionCols() {
    return onlyContainsPartCols;
  }

  public exprNodeDesc getPrunerExpr() {
    return prunerExpr;
  }

  public void setPrunerExpr(exprNodeDesc prunerExpr) {
    this.prunerExpr = prunerExpr;
  }

  static class ExprNodeTempDesc {

    public ExprNodeTempDesc(exprNodeDesc desc) {
      isTableName = false;
      this.desc = desc;
    }

    public ExprNodeTempDesc(String tableName) {
      isTableName = true;
      this.tableName = tableName;
    }

    public boolean getIsTableName() {
      return isTableName;
    }

    public exprNodeDesc getDesc() {
      return desc;
    }

    public String getTableName() {
      return tableName;
    }

    boolean isTableName;
    exprNodeDesc desc;
    String tableName;

    public String toString() {
      if (isTableName) {
        return "Table:" + tableName;
      } else {
        return "Desc: " + desc;
      }
    }
  };

  static ExprNodeTempDesc genSimpleExprNodeDesc(ASTNode expr)
      throws SemanticException {
    exprNodeDesc desc = null;
    switch (expr.getType()) {
    case HiveParser.TOK_NULL:
      desc = new exprNodeNullDesc();
      break;
    case HiveParser.Identifier:
      desc = new exprNodeConstantDesc(TypeInfoFactory.stringTypeInfo,
          SemanticAnalyzer.unescapeIdentifier(expr.getText()));
      break;
    case HiveParser.Number:
      Number v = null;
      try {
        v = Double.valueOf(expr.getText());
        v = Long.valueOf(expr.getText());
        v = Integer.valueOf(expr.getText());
      } catch (NumberFormatException e) {
      }
      if (v == null) {
        throw new SemanticException(
            ErrorMsg.INVALID_NUMERICAL_CONSTANT.getMsg(expr));
      }
      desc = new exprNodeConstantDesc(v);
      break;
    case HiveParser.StringLiteral:
      desc = new exprNodeConstantDesc(TypeInfoFactory.stringTypeInfo,
          BaseSemanticAnalyzer.unescapeSQLString(expr.getText()));
      break;
    case HiveParser.TOK_CHARSETLITERAL:
      desc = new exprNodeConstantDesc(BaseSemanticAnalyzer.charSetString(expr
          .getChild(0).getText(), expr.getChild(1).getText()));
      break;
    case HiveParser.KW_TRUE:
      desc = new exprNodeConstantDesc(Boolean.TRUE);
      break;
    case HiveParser.KW_FALSE:
      desc = new exprNodeConstantDesc(Boolean.FALSE);
      break;
    }
    return desc == null ? null : new ExprNodeTempDesc(desc);
  }

  @SuppressWarnings("nls")
  private ExprNodeTempDesc genExprNodeDesc(ASTNode expr,
      UnparseTranslator unparseTranslator) throws SemanticException {

    ExprNodeTempDesc tempDesc = genSimpleExprNodeDesc(expr);
    if (tempDesc != null) {
      return tempDesc;
    }

    int tokType = expr.getType();
    switch (tokType) {
    case HiveParser.TOK_TABLE_OR_COL: {
      String tableOrCol = BaseSemanticAnalyzer.unescapeIdentifier(
          expr.getChild(0).getText()).toLowerCase();

      String db = null;
      boolean setDb = false;
      String fullAlias = null;
      if (expr.getChildCount() == 2) {
        db = BaseSemanticAnalyzer
            .unescapeIdentifier(expr.getChild(1).getText());
        setDb = true;
      }

      if (db == null)
        db = SessionState.get().getDbName();

      if (setDb) {
        fullAlias = db + "/" + tableOrCol;

        if (null != qb.getUserAliasFromDBTB(fullAlias)) {
          if (qb.getUserAliasFromDBTB(fullAlias).size() > 1) {
            throw new SemanticException("table : " + fullAlias
                + "  has more than one alias : "
                + qb.getUserAliasFromDBTB(fullAlias).get(0) + " and "
                + qb.getUserAliasFromDBTB(fullAlias).get(0) + " ......");
          }
          fullAlias = fullAlias + "#"
              + qb.getUserAliasFromDBTB(fullAlias).get(0);
        }

        LOG.debug("setDb....fullAlias is : " + fullAlias);
      } else {
        if (qb.existsUserAlias(tableOrCol.toLowerCase())) {
          LOG.debug("do not setDb....fullAlias is : " + fullAlias);
          if (qb.getSubqAliases().contains(tableOrCol.toLowerCase())) {
            fullAlias = tableOrCol;
            LOG.debug("a sub alias....fullAlias is : " + fullAlias);
          } else {
            fullAlias = qb.getTableRefFromUserAlias(tableOrCol).getDbName()
                + "/" + qb.getTableRefFromUserAlias(tableOrCol).getTblName()
                + "#" + tableOrCol;
            LOG.debug("a user set alias....fullAlias is : " + fullAlias);
            if (qb.getTabAliases().contains(fullAlias.toLowerCase())) {
              ;
              LOG.debug("a user alias....fullAlias is : " + fullAlias);
            } else {
              fullAlias = null;
            }
          }
        }
        LOG.debug("internal ...fullAlias is : " + fullAlias);
        if (qb.exisitsDBTB(db + "/" + tableOrCol)) {
          if (fullAlias != null) {
            if (!(db + "/" + tableOrCol + "#" + tableOrCol)
                .equalsIgnoreCase(fullAlias)) {
              LOG.debug("TDW can't jude the : " + tableOrCol
                  + " in current session! "
                  + ErrorMsg.INVALID_TABLE_OR_COLUMN.getMsg(expr.getChild(0)));
              throw new SemanticException("TDW can't jude the : " + tableOrCol
                  + " in current session! "
                  + ErrorMsg.INVALID_TABLE_OR_COLUMN.getMsg(expr.getChild(0)));

            } else
              ;
          } else {
            fullAlias = db + "/" + tableOrCol;
            if (null != qb.getUserAliasFromDBTB(fullAlias)) {
              if (qb.getUserAliasFromDBTB(fullAlias).size() > 1) {
                LOG.debug("table : " + fullAlias
                    + "  has more than one alias : "
                    + qb.getUserAliasFromDBTB(fullAlias).get(0) + " and "
                    + qb.getUserAliasFromDBTB(fullAlias).get(0) + " ......");
                throw new SemanticException("table : " + fullAlias
                    + "  has more than one alias : "
                    + qb.getUserAliasFromDBTB(fullAlias).get(0) + " and "
                    + qb.getUserAliasFromDBTB(fullAlias).get(0) + " ......");

              }
              fullAlias = fullAlias + "#"
                  + qb.getUserAliasFromDBTB(fullAlias).get(0);
            }

            LOG.debug("a table in default db....fullAlias is : " + fullAlias);
          }
        }

      }

      LOG.debug("fullAlias - : " + fullAlias);

      if (fullAlias == null) {
        fullAlias = tableOrCol;
      }

      LOG.debug("fullAlias +: " + fullAlias);

      if (metaData.getAliasToTable().get(fullAlias.toLowerCase()) != null) {

        TablePartition tab = qb.getMetaData().getSrcForAlias(fullAlias);
        try {
          StructObjectInspector rowObjectInspector = (StructObjectInspector) tab
              .getDeserializer().getObjectInspector();
          List<? extends StructField> fields_tmp = rowObjectInspector
              .getAllStructFieldRefs();
          for (int i = 0; i < fields_tmp.size(); i++) {
            String fieldName = fields_tmp.get(i).getFieldName();
            if (fieldName.equalsIgnoreCase(tableOrCol)) {
              ambiguousTableOrColunm = true;
            }
          }
        } catch (SerDeException e) {
          throw new RuntimeException(e);
        }

        tempDesc = new ExprNodeTempDesc(fullAlias);
      } else if (qb.getSubqAliases().contains(tableOrCol.toLowerCase())) {
        tempDesc = new ExprNodeTempDesc(tableOrCol);
      } else {
        String colName = tableOrCol;
        String tabAlias = SemanticAnalyzer.getTabAliasForCol(this.metaData,
            colName, (ASTNode) expr.getChild(0));
        LOG.debug("getTableColumnDesc(" + tabAlias + ", " + colName);
        tempDesc = getTableColumnDesc(tabAlias, colName);
      }
      break;
    }

    default: {

      boolean isFunction = (expr.getType() == HiveParser.TOK_FUNCTION)
          || (expr.getType() == HiveParser.TOK_FUNCTIONOVER)
          || (expr.getType() == HiveParser.TOK_FUNCTIONOVERDI);

      int childrenBegin = (isFunction ? 1 : 0);
      ArrayList<ExprNodeTempDesc> tempChildren = new ArrayList<ExprNodeTempDesc>(
          expr.getChildCount() - childrenBegin);
      for (int ci = childrenBegin; ci < expr.getChildCount(); ci++) {
        ExprNodeTempDesc child = genExprNodeDesc((ASTNode) expr.getChild(ci),
            unparseTranslator);

        if (ambiguousTableOrColunm == true) {
          if (expr.getType() != HiveParser.DOT) {
            String colName = expr.getChild(ci).getChild(0).getText();
            String tabAlias = SemanticAnalyzer.getTabAliasForCol(this.metaData,
                colName, (ASTNode) expr.getChild(ci).getChild(0));
            if (tabAlias != null) {
              child = getTableColumnDesc(tabAlias, colName);
            }
          }
          ambiguousTableOrColunm = false;
        }

        tempChildren.add(child);
      }

      if (expr.getType() == HiveParser.DOT
          && tempChildren.get(0).getIsTableName()) {
        String tabAlias = tempChildren.get(0).getTableName();
        String colName = ((exprNodeConstantDesc) tempChildren.get(1).getDesc())
            .getValue().toString();
        if (qb.getSubqAliases().contains(tabAlias.toLowerCase())) {
          tempDesc = new ExprNodeTempDesc((exprNodeConstantDesc) tempChildren
              .get(1).getDesc());
          isSubQAlias = true;
        } else {
          tempDesc = getTableColumnDesc(tabAlias, colName);
        }

      } else {
        ArrayList<exprNodeDesc> children = new ArrayList<exprNodeDesc>(
            expr.getChildCount() - childrenBegin);
        for (int ci = 0; ci < tempChildren.size(); ci++) {
          children.add(tempChildren.get(ci).getDesc());
        }

        exprNodeDesc desc = null;
        try {
          if (isSubQAlias) {
            String funcText = TypeCheckProcFactory.DefaultExprProcessor.getFunctionText(expr, isFunction);
            if (funcText != null && funcText.equalsIgnoreCase("in")) {
              assert children.get(0) != null;
              TypeInfo firstTypeInfo = children.get(0).getTypeInfo();
              for (int i = 1; i < children.size(); i++) {
                children.get(i).setTypeInfo(firstTypeInfo);;
              }
            }
            isSubQAlias = false;
          }
          desc = TypeCheckProcFactory.DefaultExprProcessor
              .getXpathOrFuncExprNodeDesc(expr, isFunction, children,
                  unparseTranslator);
        } catch (UDFArgumentTypeException e) {
          throw new SemanticException(ErrorMsg.INVALID_ARGUMENT_TYPE.getMsg(
              expr.getChild(childrenBegin + e.getArgumentId()), e.getMessage()));
        } catch (UDFArgumentLengthException e) {
          throw new SemanticException(ErrorMsg.INVALID_ARGUMENT_LENGTH.getMsg(
              expr, e.getMessage()));
        } catch (UDFArgumentException e) {
          throw new SemanticException(ErrorMsg.INVALID_ARGUMENT.getMsg(expr,
              e.getMessage()));
        }

        if (FunctionRegistry.isOpAndOrNot(desc)) {
        } else if (mightBeUnknown(desc)
            || ((desc instanceof exprNodeGenericFuncDesc) && !FunctionRegistry
                .isDeterministic(((exprNodeGenericFuncDesc) desc)
                    .getGenericUDF()))) {
          LOG.trace("Pruner function might be unknown: " + expr.toStringTree());
          desc = new exprNodeConstantDesc(desc.getTypeInfo(), null);
        }

        tempDesc = new ExprNodeTempDesc(desc);
      }
      break;
    }
    }
    return tempDesc;
  }

  private ExprNodeTempDesc getTableColumnDesc(String tabAlias, String colName) {
    ExprNodeTempDesc desc;
    try {
      TablePartition t = this.metaData.getTableForAlias(tabAlias);

      TypeInfo typeInfo = TypeInfoUtils
          .getTypeInfoFromObjectInspector(this.metaData
              .getTableForAlias(tabAlias).getDeserializer()
              .getObjectInspector());
      desc = new ExprNodeTempDesc(new exprNodeConstantDesc(
          ((StructTypeInfo) typeInfo).getStructFieldTypeInfo(colName), null));
      onlyContainsPartCols = false;
    } catch (SerDeException e) {
      throw new RuntimeException(e);
    }
    return desc;
  }

  public static boolean mightBeUnknown(exprNodeDesc desc) {
    if (desc instanceof exprNodeConstantDesc) {
      exprNodeConstantDesc d = (exprNodeConstantDesc) desc;
      return d.getValue() == null;
    } else if (desc instanceof exprNodeNullDesc) {
      return false;
    } else if (desc instanceof exprNodeFieldDesc) {
      exprNodeFieldDesc d = (exprNodeFieldDesc) desc;
      return mightBeUnknown(d.getDesc());

    } else if (desc instanceof exprNodeGenericFuncDesc) {
      exprNodeGenericFuncDesc d = (exprNodeGenericFuncDesc) desc;
      for (int i = 0; i < d.getChildren().size(); i++) {
        if (mightBeUnknown(d.getChildExprs().get(i))) {
          return true;
        }
      }
      return false;
    } else if (desc instanceof exprNodeColumnDesc) {
      return false;
    }
    return false;
  }

  public boolean hasPartitionPredicate(ASTNode expr) throws SemanticException {

    int tokType = expr.getType();
    boolean hasPPred = false;
    switch (tokType) {
    case HiveParser.TOK_TABLE_OR_COL: {
      String colName = BaseSemanticAnalyzer.unescapeIdentifier(expr.getChild(0)
          .getText());

      return tab.isPartitionKey(colName);
    }
    case HiveParser.DOT: {
      assert (expr.getChildCount() == 2);
      ASTNode left = (ASTNode) expr.getChild(0);
      ASTNode right = (ASTNode) expr.getChild(1);

      if (left.getType() == HiveParser.TOK_TABLE_OR_COL) {
        String tableOrCol = BaseSemanticAnalyzer.unescapeIdentifier(left
            .getChild(0).getText());

        String db = null;
        boolean setDb = false;
        String fullAlias = null;
        if (expr.getChildCount() == 2) {
          db = BaseSemanticAnalyzer.unescapeIdentifier(expr.getChild(1)
              .getText());
          setDb = true;
        }

        if (db == null)
          db = SessionState.get().getDbName();

        if (setDb) {
          fullAlias = db + "/" + tableOrCol;

          if (null != qb.getUserAliasFromDBTB(fullAlias)) {
            if (qb.getUserAliasFromDBTB(fullAlias).size() > 1) {
              throw new SemanticException("table : " + fullAlias
                  + "  has more than one alias : "
                  + qb.getUserAliasFromDBTB(fullAlias).get(0) + " and "
                  + qb.getUserAliasFromDBTB(fullAlias).get(0) + " ......");
            }
            fullAlias = fullAlias + "#"
                + qb.getUserAliasFromDBTB(fullAlias).get(0);
          }

          LOG.debug("setDb....fullAlias is : " + fullAlias);
        } else {
          if (qb.existsUserAlias(tableOrCol)) {
            LOG.debug("do not setDb....fullAlias is : " + fullAlias);
            if (qb.getSubqAliases().contains(tableOrCol.toLowerCase())) {
              fullAlias = tableOrCol;
              LOG.debug("a sub alias....fullAlias is : " + fullAlias);
            } else {
              fullAlias = qb.getTableRefFromUserAlias(tableOrCol).getDbName()
                  + "/" + qb.getTableRefFromUserAlias(tableOrCol).getTblName()
                  + "#" + tableOrCol;
              LOG.debug("a user set alias....fullAlias is : " + fullAlias);
              if (qb.getTabAliases().contains(fullAlias.toLowerCase())) {
                ;
                LOG.debug("a user alias....fullAlias is : " + fullAlias);
              } else {
                fullAlias = null;
              }
            }
          }
          LOG.debug("internal ...fullAlias is : " + fullAlias);
          if (qb.exisitsDBTB(db + "/" + tableOrCol)) {
            if (fullAlias != null) {
              if (!(db + "/" + tableOrCol + "#" + tableOrCol)
                  .equalsIgnoreCase(fullAlias)) {
                LOG.debug("TDW can't jude the : " + tableOrCol
                    + " in current session! "
                    + ErrorMsg.INVALID_TABLE_OR_COLUMN.getMsg(expr.getChild(0)));
                throw new SemanticException("TDW can't jude the : "
                    + tableOrCol + " in current session! "
                    + ErrorMsg.INVALID_TABLE_OR_COLUMN.getMsg(expr.getChild(0)));

              } else
                ;
            } else {
              fullAlias = db + "/" + tableOrCol;
              if (null != qb.getUserAliasFromDBTB(fullAlias)) {
                if (qb.getUserAliasFromDBTB(fullAlias).size() > 1) {
                  LOG.debug("table : " + fullAlias
                      + "  has more than one alias : "
                      + qb.getUserAliasFromDBTB(fullAlias).get(0) + " and "
                      + qb.getUserAliasFromDBTB(fullAlias).get(0) + " ......");
                  throw new SemanticException("table : " + fullAlias
                      + "  has more than one alias : "
                      + qb.getUserAliasFromDBTB(fullAlias).get(0) + " and "
                      + qb.getUserAliasFromDBTB(fullAlias).get(0) + " ......");

                }
                fullAlias = fullAlias + "#"
                    + qb.getUserAliasFromDBTB(fullAlias).get(0);
              }

              LOG.debug("a table in default db....fullAlias is : " + fullAlias);
            }
          }

        }

        LOG.debug("fullAlias - : " + fullAlias);

        if (fullAlias == null) {
          fullAlias = tableOrCol;
        }

        LOG.debug("fullAlias +: " + fullAlias);

        if (metaData.getAliasToTable().get(fullAlias.toLowerCase()) != null) {
          String colName = BaseSemanticAnalyzer.unescapeIdentifier(right
              .getText());
          return tableAlias.equalsIgnoreCase(fullAlias)
              && tab.isPartitionKey(colName);
        }
      }
    }
    default: {
      boolean isFunction = (expr.getType() == HiveParser.TOK_FUNCTION);

      int childrenBegin = (isFunction ? 1 : 0);
      for (int ci = childrenBegin; ci < expr.getChildCount(); ci++) {
        hasPPred = (hasPPred || hasPartitionPredicate((ASTNode) expr
            .getChild(ci)));
      }
      break;
    }
    }

    return hasPPred;
  }

  @SuppressWarnings("nls")
  public void addExpression(ASTNode expr, UnparseTranslator unparseTranslator)
      throws SemanticException {
    LOG.debug("adding pruning Tree = " + expr.toStringTree());
    ExprNodeTempDesc temp = genExprNodeDesc(expr, unparseTranslator);
    LOG.debug("new pruning Tree = " + temp);
    exprNodeDesc desc = temp.getDesc();
    if (!(desc instanceof exprNodeConstantDesc)
        || ((exprNodeConstantDesc) desc).getValue() != null) {
      LOG.trace("adding pruning expr = " + desc);
      if (this.prunerExpr == null)
        this.prunerExpr = desc;
      else
        this.prunerExpr = TypeCheckProcFactory.DefaultExprProcessor
            .getFuncExprNodeDesc("OR", this.prunerExpr, desc);
    }
  }

  @SuppressWarnings("nls")
  public void addJoinOnExpression(ASTNode expr,
      UnparseTranslator unparseTranslator) throws SemanticException {
    LOG.trace("adding pruning Tree = " + expr.toStringTree());
    exprNodeDesc desc = genExprNodeDesc(expr, unparseTranslator).getDesc();
    if (!(desc instanceof exprNodeConstantDesc)
        || ((exprNodeConstantDesc) desc).getValue() != null) {
      LOG.trace("adding pruning expr = " + desc);
      if (this.prunerExpr == null)
        this.prunerExpr = desc;
      else
        this.prunerExpr = TypeCheckProcFactory.DefaultExprProcessor
            .getFuncExprNodeDesc("AND", this.prunerExpr, desc);
    }
  }

  @SuppressWarnings("nls")
  public PrunedPartitionList prune() throws HiveException {
    return org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner.prune(
        this.tab, this.prunerExpr, conf, this.tableAlias);
  }

  public Table getTable() {
    return this.tab.getTbl();
  }
}
