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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.exprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeNullDesc;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFOPPositive;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.ql.exec.AmbiguousMethodException;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

public class TypeCheckProcFactory {

  protected static final Log LOG = LogFactory.getLog(TypeCheckProcFactory.class
      .getName());

  public static exprNodeDesc processGByExpr(Node nd, Object procCtx)
      throws SemanticException {
    ASTNode expr = (ASTNode) nd;
    TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
    RowResolver input = ctx.getInputRR();
    exprNodeDesc desc = null;

    ColumnInfo colInfo = input.get("", expr.toStringTree());
    if (colInfo != null) {
      desc = new exprNodeColumnDesc(colInfo.getType(),
          colInfo.getInternalName(), colInfo.getTabAlias(),
          colInfo.getIsPartitionCol());
      return desc;
    }
    return desc;
  }

  public static HashMap<Node, Object> genExprNode(ASTNode expr,
      TypeCheckCtx tcCtx, Configuration conf) throws SemanticException {
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();

    opRules
        .put(new RuleRegExp("R3", HiveParser.Identifier + "%|"
            + HiveParser.StringLiteral + "%|" + HiveParser.TOK_CHARSETLITERAL
            + "%|" + HiveParser.KW_IF + "%|" + HiveParser.KW_CASE + "%|"
            + HiveParser.KW_WHEN + "%"),
            TypeCheckProcFactory.getStrExprProcessor());

    opRules.put(new RuleRegExp("R1", HiveParser.TOK_NULL + "%"),
        getNullExprProcessor());
    opRules.put(new RuleRegExp("R2", HiveParser.Number + "%"),
        getNumExprProcessor());

    opRules
        .put(new RuleRegExp("R3", HiveParser.Identifier + "%|"
            + HiveParser.StringLiteral + "%|" + HiveParser.TOK_CHARSETLITERAL
            + "%|" + HiveParser.KW_IF + "%|" + HiveParser.KW_CASE + "%|"
            + HiveParser.KW_WHEN + "%"),
            TypeCheckProcFactory.getStrExprProcessor());
    opRules.put(new RuleRegExp("R4", HiveParser.KW_TRUE + "%|"
        + HiveParser.KW_FALSE + "%"), getBoolExprProcessor());
    opRules.put(new RuleRegExp("R5", HiveParser.TOK_TABLE_OR_COL + "%"),
        getColumnExprProcessor());

    Dispatcher disp = new DefaultRuleDispatcher(getDefaultExprProcessor(conf),
        opRules, tcCtx);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.add(expr);
    HashMap<Node, Object> nodeOutputs = new HashMap<Node, Object>();
    ogw.startWalking(topNodes, nodeOutputs);

    return nodeOutputs;
  }

  public static class NullExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      exprNodeDesc desc = TypeCheckProcFactory.processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }

      return new exprNodeNullDesc();
    }

  }

  public static NullExprProcessor getNullExprProcessor() {
    return new NullExprProcessor();
  }

  public static class NumExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      exprNodeDesc desc = TypeCheckProcFactory.processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }

      Number v = null;
      ASTNode expr = (ASTNode) nd;
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
      return new exprNodeConstantDesc(v);
    }

  }

  public static NumExprProcessor getNumExprProcessor() {
    return new NumExprProcessor();
  }

  public static class StrExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      exprNodeDesc desc = TypeCheckProcFactory.processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }

      ASTNode expr = (ASTNode) nd;
      String str = null;

      switch (expr.getToken().getType()) {
      case HiveParser.StringLiteral:
        str = BaseSemanticAnalyzer.unescapeSQLString(expr.getText());
        break;
      case HiveParser.TOK_CHARSETLITERAL:
        str = BaseSemanticAnalyzer.charSetString(expr.getChild(0).getText(),
            expr.getChild(1).getText());
        break;
      default:
        str = BaseSemanticAnalyzer.unescapeIdentifier(expr.getText());
        break;
      }
      return new exprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, str);
    }

  }

  public static StrExprProcessor getStrExprProcessor() {
    return new StrExprProcessor();
  }

  public static class BoolExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      exprNodeDesc desc = TypeCheckProcFactory.processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }

      ASTNode expr = (ASTNode) nd;
      Boolean bool = null;

      switch (expr.getToken().getType()) {
      case HiveParser.KW_TRUE:
        bool = Boolean.TRUE;
        break;
      case HiveParser.KW_FALSE:
        bool = Boolean.FALSE;
        break;
      default:
        assert false;
      }
      return new exprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, bool);
    }

  }

  public static BoolExprProcessor getBoolExprProcessor() {
    return new BoolExprProcessor();
  }

  public static class ColumnExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      LOG.debug("ColumnExprProcessor");
      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;
      if (ctx.getError() != null) {
        return null;
      }

      exprNodeDesc desc = TypeCheckProcFactory.processGByExpr(nd, procCtx);
      if (desc != null) {
        return desc;
      }

      ASTNode expr = (ASTNode) nd;
      RowResolver input = ctx.getInputRR();

      if (expr.getType() != HiveParser.TOK_TABLE_OR_COL) {
        ctx.setError(ErrorMsg.INVALID_COLUMN.getMsg(expr));
        LOG.debug("ColumnExprProcessor" + ErrorMsg.INVALID_COLUMN.getMsg(expr));
        return null;
      }

      String tableOrCol = BaseSemanticAnalyzer.unescapeIdentifier(expr
          .getChild(0).getText());
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

        if (null != ctx.getQb().getUserAliasFromDBTB(fullAlias)) {
          if (ctx.getQb().getUserAliasFromDBTB(fullAlias).size() > 1) {
            ctx.setError(ErrorMsg.INVALID_TABLE_OR_COLUMN.getMsg(expr.getChild(
                0).getChild(0)));
            LOG.debug("table : " + fullAlias + "  has more than one alias : "
                + ctx.getQb().getUserAliasFromDBTB(fullAlias).get(0) + " and "
                + ctx.getQb().getUserAliasFromDBTB(fullAlias).get(0)
                + " ......");
            return null;
          }
          fullAlias = fullAlias + "#"
              + ctx.getQb().getUserAliasFromDBTB(fullAlias).get(0);
        }

        LOG.debug("setDb....fullAlias is : " + fullAlias);
      } else {
        if (ctx.getQb().existsUserAlias(tableOrCol)) {
          LOG.debug("do not setDb....fullAlias is : " + fullAlias);
          if (ctx.getQb().getSubqAliases().contains(tableOrCol.toLowerCase())) {
            fullAlias = tableOrCol;
            LOG.debug("a sub alias....fullAlias is : " + fullAlias);
          } else {
            if ((ctx.getQb().getTableRefFromUserAlias(tableOrCol)) == null)
              LOG.debug("ctx.getQb().getTableRefFromUserAlias(tableOrCol) is null!");
            LOG.debug("db name : "
                + ctx.getQb().getTableRefFromUserAlias(tableOrCol).getDbName());
            LOG.debug("tble name : "
                + ctx.getQb().getTableRefFromUserAlias(tableOrCol).getTblName());

            fullAlias = ctx.getQb().getTableRefFromUserAlias(tableOrCol)
                .getDbName()
                + "/"
                + ctx.getQb().getTableRefFromUserAlias(tableOrCol).getTblName()
                + "#" + tableOrCol;
            LOG.debug("a user set alias....fullAlias is : " + fullAlias);
            if (ctx.getQb().getTabAliases().contains(fullAlias.toLowerCase())
                && input.hasTableAlias(fullAlias.toLowerCase())) {
              ;
              LOG.debug("a user alias....fullAlias is : " + fullAlias);
            } else if (ctx.getQb().getSubqAliases()
                .contains(fullAlias.toLowerCase())
                && input.hasTableAlias(fullAlias.toLowerCase())) {
              LOG.debug("a user sub view alias....fullAlias is : " + fullAlias);
            } else {
              fullAlias = null;
            }
          }
        }
        LOG.debug("internal ...fullAlias is : " + fullAlias);
        if (ctx.getQb().exisitsDBTB(db + "/" + tableOrCol)) {
          if (fullAlias != null) {
            if (!(db + "/" + tableOrCol + "#" + tableOrCol)
                .equalsIgnoreCase(fullAlias)) {
              ctx.setError(ErrorMsg.INVALID_TABLE_OR_COLUMN.getMsg(expr
                  .getChild(0)));
              LOG.debug("TDW can't jude the : " + tableOrCol
                  + " in current session! "
                  + ErrorMsg.INVALID_TABLE_OR_COLUMN.getMsg(expr.getChild(0)));
              return null;
            } else
              ;
          } else {
            fullAlias = db + "/" + tableOrCol;
            if (null != ctx.getQb().getUserAliasFromDBTB(fullAlias)) {
              if (ctx.getQb().getUserAliasFromDBTB(fullAlias).size() > 1) {
                ctx.setError(ErrorMsg.INVALID_TABLE_OR_COLUMN.getMsg(expr
                    .getChild(0).getChild(0)));
                LOG.debug("table : " + fullAlias
                    + "  has more than one alias : "
                    + ctx.getQb().getUserAliasFromDBTB(fullAlias).get(0)
                    + " and "
                    + ctx.getQb().getUserAliasFromDBTB(fullAlias).get(0)
                    + " ......");
                return null;
              }
              fullAlias = fullAlias + "#"
                  + ctx.getQb().getUserAliasFromDBTB(fullAlias).get(0);
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

      boolean isTableAlias = input.hasTableAlias(fullAlias.toLowerCase());
      ColumnInfo colInfo = input.get(null, fullAlias);

      if (isTableAlias) {
        if (colInfo != null) {
          ctx.setError(ErrorMsg.AMBIGUOUS_TABLE_OR_COLUMN.getMsg(expr));
          return null;
        } else {

          ColumnInfo colInfo_tmp = input.get(fullAlias, tableOrCol);
          if (colInfo_tmp != null) {
            String ambiguous_table_or_colunm = tableOrCol;
            return ambiguous_table_or_colunm;
          }

          return null;
        }
      } else {
        if (colInfo == null) {
          if (input.getIsExprResolver()) {
            ctx.setError(ErrorMsg.NON_KEY_EXPR_IN_GROUPBY.getMsg(expr));
            return null;
          } else {
            ctx.setError(ErrorMsg.INVALID_TABLE_OR_COLUMN.getMsg(expr
                .getChild(0)));
            LOG.debug(ErrorMsg.INVALID_TABLE_OR_COLUMN.toString() + ":"
                + input.toString());
            return null;
          }
        } else {
          return new exprNodeColumnDesc(colInfo.getType(),
              colInfo.getInternalName(), colInfo.getTabAlias(),
              colInfo.getIsPartitionCol());
        }
      }

    }

  }

  public static ColumnExprProcessor getColumnExprProcessor() {
    return new ColumnExprProcessor();
  }

  public static class DefaultExprProcessor implements NodeProcessor {

    static HashMap<Integer, String> specialUnaryOperatorTextHashMap;
    static HashMap<Integer, String> specialFunctionTextHashMap;
    static HashMap<Integer, String> conversionFunctionTextHashMap;
    static {
      specialUnaryOperatorTextHashMap = new HashMap<Integer, String>();
      specialUnaryOperatorTextHashMap.put(HiveParser.PLUS, "positive");
      specialUnaryOperatorTextHashMap.put(HiveParser.MINUS, "negative");
      specialFunctionTextHashMap = new HashMap<Integer, String>();
      specialFunctionTextHashMap.put(HiveParser.TOK_ISNULL, "isnull");
      specialFunctionTextHashMap.put(HiveParser.TOK_ISNOTNULL, "isnotnull");
      conversionFunctionTextHashMap = new HashMap<Integer, String>();
      conversionFunctionTextHashMap.put(HiveParser.TOK_BOOLEAN,
          Constants.BOOLEAN_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_TINYINT,
          Constants.TINYINT_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_SMALLINT,
          Constants.SMALLINT_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_INT,
          Constants.INT_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_BIGINT,
          Constants.BIGINT_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_FLOAT,
          Constants.FLOAT_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_DOUBLE,
          Constants.DOUBLE_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_STRING,
          Constants.STRING_TYPE_NAME);
      conversionFunctionTextHashMap.put(HiveParser.TOK_TIMESTAMP,
          Constants.TIMESTAMP_TYPE_NAME);
    }

    Configuration conf;

    public DefaultExprProcessor(Configuration conf) {
      this.conf = conf;
    }

    public static boolean isRedundantConversionFunction(ASTNode expr,
        boolean isFunction, ArrayList<exprNodeDesc> children) {
      if (!isFunction)
        return false;
      assert (children.size() == expr.getChildCount() - 1);
      if (children.size() != 1)
        return false;
      String funcText = conversionFunctionTextHashMap.get(((ASTNode) expr
          .getChild(0)).getType());
      if (funcText == null)
        return false;
      return ((PrimitiveTypeInfo) children.get(0).getTypeInfo()).getTypeName()
          .equalsIgnoreCase(funcText);
    }

    public static String getFunctionText(ASTNode expr, boolean isFunction) {
      String funcText = null;
      if (!isFunction) {
        if (expr.getChildCount() == 1) {
          funcText = specialUnaryOperatorTextHashMap.get(expr.getType());
        }
        if (funcText == null) {
          funcText = expr.getText();
        }
      } else {
        assert (expr.getChildCount() >= 1);
        int funcType = ((ASTNode) expr.getChild(0)).getType();
        funcText = specialFunctionTextHashMap.get(funcType);
        if (funcText == null) {
          funcText = conversionFunctionTextHashMap.get(funcType);
        }
        if (funcText == null) {
          funcText = ((ASTNode) expr.getChild(0)).getText();
        }
      }
      return BaseSemanticAnalyzer.unescapeIdentifier(funcText);

    }

    public static exprNodeDesc getFuncExprNodeDesc(String name,
        exprNodeDesc... children) {
      ArrayList<exprNodeDesc> c = new ArrayList<exprNodeDesc>(
          Arrays.asList(children));
      try {
        return getFuncExprNodeDesc(name, c);
      } catch (UDFArgumentException e) {
        throw new RuntimeException("Hive 2 internal error", e);
      }
    }

    public static exprNodeDesc getFuncExprNodeDesc1(String name,
        exprNodeDesc... children) {
      try {
        return getFuncExprNodeDesc(name + "1", Arrays.asList(children));
      } catch (UDFArgumentException e) {
        throw new RuntimeException("Hive 2 internal error", e);
      }
    }

    public static exprNodeDesc getFuncExprNodeDesc(String udfName,
        List<exprNodeDesc> children) throws UDFArgumentException {

      FunctionInfo fi = FunctionRegistry.getFunctionInfo(udfName);

      if (fi == null) {
        throw new UDFArgumentException("udf:" + udfName + " not found.");
      }

      GenericUDF genericUDF = fi.getGenericUDF();
      if (genericUDF == null) {
        throw new UDFArgumentException("udf:" + udfName
            + " is an aggregation function.");
      }

      return exprNodeGenericFuncDesc.newInstance(genericUDF, children);

    }

    static exprNodeDesc getXpathOrFuncExprNodeDesc(ASTNode expr,
        boolean isFunction, ArrayList<exprNodeDesc> children, TypeCheckCtx ctx,
        Configuration conf) throws SemanticException, UDFArgumentException {

      if (isRedundantConversionFunction(expr, isFunction, children)) {
        assert (children.size() == 1);
        assert (children.get(0) != null);
        return children.get(0);
      }
      String funcText = getFunctionText(expr, isFunction);
      exprNodeDesc desc;
      if (funcText.equals(".")) {
        assert (children.size() == 2);
        assert (children.get(1) instanceof exprNodeConstantDesc);
        exprNodeDesc object = children.get(0);
        exprNodeConstantDesc fieldName = (exprNodeConstantDesc) children.get(1);
        assert (fieldName.getValue() instanceof String);

        String fieldNameString = (String) fieldName.getValue();
        TypeInfo objectTypeInfo = object.getTypeInfo();

        boolean isList = (object.getTypeInfo().getCategory() == ObjectInspector.Category.LIST);
        if (isList) {
          objectTypeInfo = ((ListTypeInfo) objectTypeInfo)
              .getListElementTypeInfo();
        }
        if (objectTypeInfo.getCategory() != Category.STRUCT) {
          throw new SemanticException(ErrorMsg.INVALID_DOT.getMsg(expr));
        }
        TypeInfo t = ((StructTypeInfo) objectTypeInfo)
            .getStructFieldTypeInfo(fieldNameString);
        if (isList) {
          t = TypeInfoFactory.getListTypeInfo(t);
        }

        desc = new exprNodeFieldDesc(t, children.get(0), fieldNameString,
            isList);

      } else if (funcText.equals("[")) {
        assert (children.size() == 2);

        TypeInfo myt = children.get(0).getTypeInfo();

        if (myt.getCategory() == Category.LIST) {
          if (!(children.get(1) instanceof exprNodeConstantDesc)
              || !(((exprNodeConstantDesc) children.get(1)).getTypeInfo()
                  .equals(TypeInfoFactory.intTypeInfo))) {
            throw new SemanticException(
                ErrorMsg.INVALID_ARRAYINDEX_CONSTANT.getMsg(expr));
          }

          TypeInfo t = ((ListTypeInfo) myt).getListElementTypeInfo();
          desc = new exprNodeGenericFuncDesc(t,
              FunctionRegistry.getGenericUDFForIndex(), children);
        } else if (myt.getCategory() == Category.MAP) {
          if (!(children.get(1) instanceof exprNodeConstantDesc)) {
            throw new SemanticException(
                ErrorMsg.INVALID_MAPINDEX_CONSTANT.getMsg(expr));
          }
          if (!(((exprNodeConstantDesc) children.get(1)).getTypeInfo()
              .equals(((MapTypeInfo) myt).getMapKeyTypeInfo()))) {
            throw new SemanticException(
                ErrorMsg.INVALID_MAPINDEX_TYPE.getMsg(expr));
          }
          TypeInfo t = ((MapTypeInfo) myt).getMapValueTypeInfo();
          desc = new exprNodeGenericFuncDesc(t,
              FunctionRegistry.getGenericUDFForIndex(), children);
        } else {
          throw new SemanticException(ErrorMsg.NON_COLLECTION_TYPE.getMsg(expr,
              myt.getTypeName()));
        }
      } else {
        FunctionInfo fi = FunctionRegistry.getFunctionInfo(funcText);
        if (fi == null) {
          if (isFunction)
            throw new SemanticException(
                ErrorMsg.INVALID_FUNCTION.getMsg((ASTNode) expr.getChild(0)));
          else
            throw new SemanticException(
                ErrorMsg.INVALID_FUNCTION.getMsg((ASTNode) expr));
        }

        if (!fi.isNative()) {
          ctx.getUnparseTranslator().addIdentifierTranslation(
              (ASTNode) expr.getChild(0));
        }

        if (fi.getGenericUDTF() != null) {
          throw new SemanticException(ErrorMsg.UDTF_INVALID_LOCATION.getMsg());
        }

        try {
          desc = getFuncExprNodeDesc(funcText, children);
        } catch (AmbiguousMethodException e) {
          ArrayList<Class<?>> argumentClasses = new ArrayList<Class<?>>(
              children.size());
          for (int i = 0; i < children.size(); i++) {
            argumentClasses.add(((PrimitiveTypeInfo) children.get(i)
                .getTypeInfo()).getPrimitiveWritableClass());
          }

          if (isFunction) {
            String reason = "Looking for UDF \"" + expr.getChild(0).getText()
                + "\" with parameters " + argumentClasses;
            throw new SemanticException(
                ErrorMsg.INVALID_FUNCTION_SIGNATURE.getMsg(
                    (ASTNode) expr.getChild(0), reason), e);
          } else {
            String reason = "Looking for Operator \"" + expr.getText()
                + "\" with parameters " + argumentClasses;
            throw new SemanticException(
                ErrorMsg.INVALID_OPERATOR_SIGNATURE.getMsg(expr, reason), e);
          }
        }
      }

      if (FunctionRegistry.isOpPositive(desc)) {
        assert (desc.getChildren().size() == 1);
        desc = desc.getChildren().get(0);
      }
      assert (desc != null);
      return desc;
    }

    static exprNodeDesc getXpathOrFuncExprNodeDesc(ASTNode expr,
        boolean isFunction, ArrayList<exprNodeDesc> children,
        UnparseTranslator unparseTranslator) throws SemanticException,
        UDFArgumentException {
      if (isRedundantConversionFunction(expr, isFunction, children)) {
        assert (children.size() == 1);
        assert (children.get(0) != null);
        return children.get(0);
      }
      String funcText = getFunctionText(expr, isFunction);

      exprNodeDesc desc;
      if (funcText.equals(".")) {
        assert (children.size() == 2);
        assert (children.get(1) instanceof exprNodeConstantDesc);
        exprNodeDesc object = children.get(0);
        exprNodeConstantDesc fieldName = (exprNodeConstantDesc) children.get(1);
        assert (fieldName.getValue() instanceof String);

        String fieldNameString = (String) fieldName.getValue();
        TypeInfo objectTypeInfo = object.getTypeInfo();

        boolean isList = (object.getTypeInfo().getCategory() == ObjectInspector.Category.LIST);
        if (isList) {
          objectTypeInfo = ((ListTypeInfo) objectTypeInfo)
              .getListElementTypeInfo();
        }
        if (objectTypeInfo.getCategory() != Category.STRUCT) {
          throw new SemanticException(ErrorMsg.INVALID_DOT.getMsg(expr));
        }
        TypeInfo t = ((StructTypeInfo) objectTypeInfo)
            .getStructFieldTypeInfo(fieldNameString);
        if (isList) {
          t = TypeInfoFactory.getListTypeInfo(t);
        }

        desc = new exprNodeFieldDesc(t, children.get(0), fieldNameString,
            isList);

      } else if (funcText.equals("[")) {
        assert (children.size() == 2);

        TypeInfo myt = children.get(0).getTypeInfo();

        if (myt.getCategory() == Category.LIST) {
          if (!(children.get(1) instanceof exprNodeConstantDesc)
              || !(((exprNodeConstantDesc) children.get(1)).getTypeInfo()
                  .equals(TypeInfoFactory.intTypeInfo))) {
            throw new SemanticException(
                ErrorMsg.INVALID_ARRAYINDEX_CONSTANT.getMsg(expr));
          }

          TypeInfo t = ((ListTypeInfo) myt).getListElementTypeInfo();
          desc = new exprNodeGenericFuncDesc(t,
              FunctionRegistry.getGenericUDFForIndex(), children);
        } else if (myt.getCategory() == Category.MAP) {
          if (!(children.get(1) instanceof exprNodeConstantDesc)) {
            throw new SemanticException(
                ErrorMsg.INVALID_MAPINDEX_CONSTANT.getMsg(expr));
          }
          if (!(((exprNodeConstantDesc) children.get(1)).getTypeInfo()
              .equals(((MapTypeInfo) myt).getMapKeyTypeInfo()))) {
            throw new SemanticException(
                ErrorMsg.INVALID_MAPINDEX_TYPE.getMsg(expr));
          }
          TypeInfo t = ((MapTypeInfo) myt).getMapValueTypeInfo();
          desc = new exprNodeGenericFuncDesc(t,
              FunctionRegistry.getGenericUDFForIndex(), children);
        } else {
          throw new SemanticException(ErrorMsg.NON_COLLECTION_TYPE.getMsg(expr,
              myt.getTypeName()));
        }
      } else {
        FunctionInfo fi = FunctionRegistry.getFunctionInfo(funcText);
        if (fi == null) {
          if (isFunction)
            throw new SemanticException(
                ErrorMsg.INVALID_FUNCTION.getMsg((ASTNode) expr.getChild(0)));
          else
            throw new SemanticException(
                ErrorMsg.INVALID_FUNCTION.getMsg((ASTNode) expr));
        }

        if (!fi.isNative()) {
          unparseTranslator
              .addIdentifierTranslation((ASTNode) expr.getChild(0));
        }

        if (fi.getGenericUDTF() != null) {
          throw new SemanticException(ErrorMsg.UDTF_INVALID_LOCATION.getMsg());
        }

        desc = getFuncExprNodeDesc(funcText, children);
        if (desc == null) {
          ArrayList<Class<?>> argumentClasses = new ArrayList<Class<?>>(
              children.size());
          for (int i = 0; i < children.size(); i++) {
            argumentClasses.add(((PrimitiveTypeInfo) children.get(i)
                .getTypeInfo()).getPrimitiveWritableClass());
          }

          if (isFunction) {
            String reason = "Looking for UDF \"" + expr.getChild(0).getText()
                + "\" with parameters " + argumentClasses;
            throw new SemanticException(
                ErrorMsg.INVALID_FUNCTION_SIGNATURE.getMsg(
                    (ASTNode) expr.getChild(0), reason));
          } else {
            String reason = "Looking for Operator \"" + expr.getText()
                + "\" with parameters " + argumentClasses;
            throw new SemanticException(
                ErrorMsg.INVALID_OPERATOR_SIGNATURE.getMsg(expr, reason));
          }
        }
      }

      if (FunctionRegistry.isOpPositive(desc)) {
        assert (desc.getChildren().size() == 1);
        desc = desc.getChildren().get(0);
      }
      assert (desc != null);
      return desc;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      LOG.debug("DefaultExprProcessor");
      TypeCheckCtx ctx = (TypeCheckCtx) procCtx;

      exprNodeDesc desc = TypeCheckProcFactory.processGByExpr(nd, procCtx);
      if (desc != null) {
        ctx.setError(null);
        return desc;
      }

      if (ctx.getError() != null) {
        return null;
      }

      ASTNode expr = (ASTNode) nd;

      if (expr.getType() == HiveParser.TOK_FUNCPARAMETER
          || expr.getType() == HiveParser.TOK_PARTITIONBY
          || expr.getType() == HiveParser.TOK_ORDERBY) {
        return null;
      }

      if (expr.getType() == HiveParser.DOT
          && expr.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
          && nodeOutputs[0] != null && nodeOutputs[0] instanceof String) {
        nodeOutputs[0] = null;
      }

      if (expr.getType() == HiveParser.DOT
          && expr.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
          && nodeOutputs[0] == null) {

        RowResolver input = ctx.getInputRR();
        String tableAlias = SemanticAnalyzer.unescapeIdentifier(expr
            .getChild(0).getChild(0).getText());

        String fullAlias = null;
        String db = null;

        boolean isSetDb = false;
        if (expr.getChild(0).getChildCount() == 2) {
          db = SemanticAnalyzer.unescapeIdentifier(expr.getChild(0).getChild(1)
              .getText());
          isSetDb = true;
        } else {
          db = SessionState.get().getDbName();
        }

        if (isSetDb) {
          fullAlias = db + "/" + tableAlias;
          if (null != ctx.getQb().getUserAliasFromDBTB(fullAlias)) {
            if (ctx.getQb().getUserAliasFromDBTB(fullAlias).size() > 1) {
              ctx.setError(ErrorMsg.INVALID_TABLE_OR_COLUMN.getMsg(expr
                  .getChild(0).getChild(0)));
              LOG.debug("table : " + fullAlias + "  has more than one alias : "
                  + ctx.getQb().getUserAliasFromDBTB(fullAlias).get(0)
                  + " and "
                  + ctx.getQb().getUserAliasFromDBTB(fullAlias).get(0)
                  + " ......");
              return null;
            }
            fullAlias = fullAlias + "#"
                + ctx.getQb().getUserAliasFromDBTB(fullAlias).get(0);
          }

        } else {
          if (ctx.getQb().existsUserAlias(tableAlias)) {
            if (ctx.getQb().getSubqAliases().contains(tableAlias.toLowerCase())
                && input.hasTableAlias(tableAlias)) {
              fullAlias = tableAlias;
            } else {
              fullAlias = ctx.getQb().getTableRefFromUserAlias(tableAlias)
                  .getDbName()
                  + "/"
                  + ctx.getQb().getTableRefFromUserAlias(tableAlias)
                      .getTblName() + "#" + tableAlias;
              if (ctx.getQb().getTabAliases().contains(fullAlias.toLowerCase())
                  && input.hasTableAlias(fullAlias)) {
                ;
              } else {
                fullAlias = null;
              }
            }
          }

          if (ctx.getQb().exisitsDBTB(db + "/" + tableAlias)) {
            if (fullAlias != null) {
              if (!(db + "/" + tableAlias + "#" + tableAlias)
                  .equalsIgnoreCase(fullAlias)) {
                ctx.setError(ErrorMsg.INVALID_TABLE_OR_COLUMN.getMsg(expr
                    .getChild(0).getChild(0)));
                LOG.debug("TDW can't jude the : "
                    + tableAlias
                    + " in current session! "
                    + ErrorMsg.INVALID_TABLE_OR_COLUMN.getMsg(expr.getChild(0)
                        .getChild(0)));
                return null;
              } else
                ;
            } else {
              fullAlias = db + "/" + tableAlias;
              if (null != ctx.getQb().getUserAliasFromDBTB(fullAlias)) {
                if (ctx.getQb().getUserAliasFromDBTB(fullAlias).size() > 1) {
                  ctx.setError(ErrorMsg.INVALID_TABLE_OR_COLUMN.getMsg(expr
                      .getChild(0).getChild(0)));
                  LOG.debug("table : " + fullAlias
                      + "  has more than one alias : "
                      + ctx.getQb().getUserAliasFromDBTB(fullAlias).get(0)
                      + " and "
                      + ctx.getQb().getUserAliasFromDBTB(fullAlias).get(0)
                      + " ......");
                  return null;
                }
                fullAlias = fullAlias + "#"
                    + ctx.getQb().getUserAliasFromDBTB(fullAlias).get(0);
              }

            }

          }

        }

        if (fullAlias == null) {
          boolean gotIt = false;
          for (String t : input.getTableNames()) {
            String[] tmp = t.split("#");
            if (tmp[tmp.length - 1].equalsIgnoreCase(tableAlias)) {
              fullAlias = t;
              break;
            }
          }
        }

        if (fullAlias == null) {
          fullAlias = tableAlias;
        }

        LOG.debug("DefaultExprProcessor ... fullAlias : " + fullAlias);
        ColumnInfo colInfo = input.get(fullAlias,
            ((exprNodeConstantDesc) nodeOutputs[1]).getValue().toString());

        if (colInfo == null) {
          ctx.setError(ErrorMsg.INVALID_COLUMN.getMsg(expr.getChild(1)));
          LOG.debug("DefaultExprProcessor : "
              + ErrorMsg.INVALID_COLUMN.getMsg(expr.getChild(1)));
          return null;
        }
        return new exprNodeColumnDesc(colInfo.getType(),
            colInfo.getInternalName(), colInfo.getTabAlias(),
            colInfo.getIsPartitionCol());
      }

      if (nodeOutputs != null && nodeOutputs[0] != null
          && nodeOutputs[0] instanceof String) {
        String column_name = (String) nodeOutputs[0];
        RowResolver input = ctx.getInputRR();

        ColumnInfo colInfo = input.get(null, column_name);

        if (colInfo != null) {
          exprNodeColumnDesc desc_tmp = new exprNodeColumnDesc(
              colInfo.getType(), colInfo.getInternalName(),
              colInfo.getTabAlias(), colInfo.getIsPartitionCol());

          nodeOutputs[0] = (Object) desc_tmp;
        }
      }

      if (conversionFunctionTextHashMap.keySet().contains(expr.getType())
          || specialFunctionTextHashMap.keySet().contains(expr.getType())
          || expr.getToken().getType() == HiveParser.CharSetName
          || expr.getToken().getType() == HiveParser.CharSetLiteral) {
        return null;
      }

      boolean isFunction = (expr.getType() == HiveParser.TOK_FUNCTION);
      isFunction |= (expr.getType() == HiveParser.TOK_FUNCTIONOVER);

      int childrenBegin = (isFunction ? 1 : 0);
      ArrayList<exprNodeDesc> children = new ArrayList<exprNodeDesc>(
          expr.getChildCount() - childrenBegin);
      for (int ci = childrenBegin; ci < expr.getChildCount(); ci++) {
        children.add((exprNodeDesc) nodeOutputs[ci]);
      }

      if (children.contains(null)) {
        return null;
      }

      try {
        return getXpathOrFuncExprNodeDesc(expr, isFunction, children, ctx, conf);
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
    }

  }

  public static DefaultExprProcessor getDefaultExprProcessor() {
    return new DefaultExprProcessor(new Configuration());
  }

  public static DefaultExprProcessor getDefaultExprProcessor(Configuration conf) {
    return new DefaultExprProcessor(conf);
  }
}
